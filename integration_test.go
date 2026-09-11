package pglogreplsimple

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// ---------------------------------------------------------------- env gating

const testConnInfoEnv = "PGLOGREPLSIMPLE_TEST_CONNINFO"

func requireDB(t *testing.T) string {
	t.Helper()
	connInfo := os.Getenv(testConnInfoEnv)
	if connInfo == "" {
		t.Skipf("set %s to run integration tests", testConnInfoEnv)
	}
	return connInfo
}

// ---------------------------------------------------------------- helpers

const testSlotPrefix = "pglrs_"
const testPlugin = "test_decoding"

// slotName returns a per-test slot name to avoid collisions.
func slotName(t *testing.T) string {
	t.Helper()
	return testSlotPrefix +
		strings.ToLower(strings.ReplaceAll(t.Name(), "/", "_"))
}

// createSlot creates a logical replication slot via SQL and returns a cleanup
// function that drops the slot. It uses pg_create_logical_replication_slot
// (not the replication protocol's CREATE_REPLICATION_SLOT) because the test
// server may not allow all plugins via the replication protocol command.
func createSlot(t *testing.T, connInfo, slot string) func() {
	t.Helper()

	// Drop any leftover slot first. Ignore "doesn't exist" errors.
	tryDropSlot(t, connInfo, slot)

	// Create the slot via SQL.
	execSQL(t, connInfo,
		"SELECT pg_create_logical_replication_slot('"+slot+"', '"+testPlugin+"')")

	return func() {
		tryDropSlot(t, connInfo, slot)
	}
}

// tryDropSlot drops a replication slot if it exists. It is safe to call even
// if the slot does not exist or is still in use.
func tryDropSlot(t *testing.T, connInfo, slot string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgconn.Connect(ctx, connInfo)
	if err != nil {
		return
	}
	defer conn.Close(context.Background())

	// Only drop if the slot actually exists.
	_, err = conn.Exec(ctx,
		"SELECT 1 FROM pg_replication_slots WHERE slot_name = '"+slot+"'",
	).ReadAll()
	if err != nil {
		return
	}
	_, _ = conn.Exec(ctx,
		"SELECT pg_drop_replication_slot('"+slot+"')").ReadAll()
}

// execSQL runs arbitrary SQL on a fresh non-replication connection.
func execSQL(t *testing.T, connInfo, sql string) []*pgconn.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgconn.Connect(ctx, connInfo)
	if err != nil {
		t.Fatalf("connect for exec: %v", err)
	}
	defer conn.Close(context.Background())

	res, err := conn.Exec(ctx, sql).ReadAll()
	if err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}

	return res
}

// setupTable creates or empties the test table
func setupTable(t *testing.T, connInfo string) {
	t.Helper()
	execSQL(t, connInfo,
		`CREATE TABLE IF NOT EXISTS
			 pglogreplsimple_test(id int primary key, val text);
		 DELETE FROM pglogreplsimple_test;`)
}

// newIntegrationReceiver creates a Receiver wired to the given connInfo and
// slot, with test_decoding as the only accepted plugin.
func newIntegrationReceiver(t *testing.T, connInfo, slot string) *Receiver {
	t.Helper()
	return NewReceiver(
		WithParams(&Param{
			Logger:             fakeLogger{},
			ConnInfo:           connInfo,
			SlotName:           slot,
			ErrorRetryInterval: 500 * time.Millisecond,
			FeedbackInterval:   1 * time.Second,
		}),
		WithAcceptedPlugins(map[string][]string{
			"test_decoding": {},
		}),
	)
}

// ---------------------------------------------------------------- tests

func TestIntegrationProduceXLogData(t *testing.T) {
	connInfo := requireDB(t)
	slot := slotName(t)
	cleanup := createSlot(t, connInfo, slot)
	defer cleanup()
	setupTable(t, connInfo)

	r := newIntegrationReceiver(t, connInfo, slot)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	it, err := r.Produce(ctx)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Generate WAL, then give the receiver time to process it before
	// canceling the context.
	go func() {
		execSQL(t, connInfo,
			`INSERT INTO pglogreplsimple_test VALUES (1, 'hello');
			 INSERT INTO pglogreplsimple_test VALUES (2, 'world');`)
		time.Sleep(500 * time.Millisecond)
		cancel()
	}()

	msgCount := 0
	for msg := range it {
		switch dat := msg.(type) {
		case *pglogrepl.XLogData:
			t.Logf("WALData: %v", string(dat.WALData))
			msgCount++
			// test_decoding produces readable text; just check it's non-empty.
			if len(dat.WALData) == 0 {
				t.Error("XLogData.WALData is empty")
			}
			r.AckLSN(dat.WALStart)
		case *pglogrepl.PrimaryKeepaliveMessage:
			t.Logf("PKM: %v", dat)
			r.AckLSN(dat.ServerWALEnd)
		case *pgproto3.NoticeResponse:
			// fine
		}
	}

	if msgCount == 0 {
		t.Error("expected at least one XLogData message, got 0")
	}

	if r.Err() != nil && !errors.Is(r.Err(), context.Canceled) {
		t.Errorf("r.Err() = %v, want nil or context.Canceled", r.Err())
	}
}

func TestIntegrationShutdown(t *testing.T) {
	connInfo := requireDB(t)
	slot := slotName(t)
	cleanup := createSlot(t, connInfo, slot)
	defer cleanup()

	r := newIntegrationReceiver(t, connInfo, slot)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	it, err := r.Produce(ctx)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	shutdownErr := errors.New("test shutdown")

	// Shutdown from another goroutine after a short delay.
	go func() {
		time.Sleep(100 * time.Millisecond)
		r.Shutdown(shutdownErr)
	}()

	for range it {
		// drain until shutdown ends the iterator
	}

	if r.State() != Stop {
		t.Errorf("State() = %v, want Stop", r.State())
	}
	if r.Err() != shutdownErr {
		t.Errorf("Err() = %v, want %v", r.Err(), shutdownErr)
	}
}

func TestIntegrationBreakAndResume(t *testing.T) {
	connInfo := requireDB(t)
	slot := slotName(t)
	cleanup := createSlot(t, connInfo, slot)
	defer cleanup()
	setupTable(t, connInfo)

	r := newIntegrationReceiver(t, connInfo, slot)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// First Produce: break out after receiving one message.
	it1, err := r.Produce(ctx)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Generate a row so there's WAL to receive.
	go func() {
		time.Sleep(100 * time.Millisecond)
		execSQL(t, connInfo,
			`INSERT INTO pglogreplsimple_test VALUES (1, 'first');`)
	}()

	gotMsg := false
	for msg := range it1 {
		gotMsg = true
		switch dat := msg.(type) {
		case *pglogrepl.XLogData:
			t.Logf("WALData(it1): %v", string(dat.WALData))
			r.AckLSN(dat.WALStart)
		case *pglogrepl.PrimaryKeepaliveMessage:
			t.Logf("PKM(it1): %v", dat)
			r.AckLSN(dat.ServerWALEnd)
		}
		break // exit the loop; state should be Break
	}

	if !gotMsg {
		t.Fatal("expected at least one message before break")
	}

	// After break, Produce resets state to Recv (not Break) so that a
	// subsequent Produce call can resume.
	if r.State() != Recv {
		t.Errorf("State() = %v, want Recv (after break, state is reset for resume)", r.State())
	}

	// Resume: a second Produce should work from the Break state.
	it2, err := r.Produce(ctx)
	if err != nil {
		t.Fatalf("second Produce: %v", err)
	}

	// Generate more WAL and then cancel.
	go func() {
		time.Sleep(200 * time.Millisecond)
		execSQL(t, connInfo,
			`INSERT INTO pglogreplsimple_test VALUES (99, 'resumed')
			 ON CONFLICT (id) DO UPDATE SET val = 'resumed';`)
		cancel()
	}()

	didIter := false
	for msg := range it2 {
		didIter = true
		switch dat := msg.(type) {
		case *pglogrepl.XLogData:
			t.Logf("WALData(it2): %v", string(dat.WALData))
			r.AckLSN(dat.WALStart)
		case *pglogrepl.PrimaryKeepaliveMessage:
			t.Logf("PKM(it2): %v", dat)
			r.AckLSN(dat.ServerWALEnd)
		}
	}

	if !didIter {
		t.Error("second Produce iterator yielded no messages")
	}
}

func TestIntegrationReconnect(t *testing.T) {
	connInfo := requireDB(t)
	slot := slotName(t)
	cleanup := createSlot(t, connInfo, slot)
	defer cleanup()
	setupTable(t, connInfo)

	r := newIntegrationReceiver(t, connInfo, slot)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	it, err := r.Produce(ctx)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Phase 1: receive an initial message to confirm the connection works.
	gotFirst := false
	gotAfterReconnect := false
	backendPids := [2]string{}
	for msg := range it {
		switch dat := msg.(type) {
		case *pglogrepl.XLogData:
			x := execSQL(t, connInfo,
				`SELECT active_pid FROM pg_replication_slots
				 WHERE slot_name = '` + slot + `'`,
			)[0].Rows[0][0]
			r.AckLSN(dat.WALStart)
			if !gotFirst {
				backendPids[0] = string(x)
				t.Logf("WALData (%v): %v", backendPids, string(dat.WALData))
				gotFirst = true
				// Force a reconnect by killing the underlying connection.
				// Since this test is in the same package, we can access
				// r.conn directly. Closing it causes the next ReceiveMessage
				// to fail, which makes recvOne return Connect, triggering
				// connInit to reconnect.
				go func() {
					time.Sleep(100 * time.Millisecond)
					if r.conn != nil {
						r.conn.Close(context.Background())
					}
					// Generate WAL after the reconnect so the receiver
					// has something to deliver on the new connection.
					time.Sleep(1 * time.Second)
					execSQL(t, connInfo,
						`INSERT INTO pglogreplsimple_test
							VALUES (50, 'after_reconnect')
						 ON CONFLICT (id) DO UPDATE SET val='after_reconnect';`)
					time.Sleep(500 * time.Millisecond)
					cancel()
				}()
			} else {
				backendPids[1] = string(x)
				t.Logf("WALData (%v): %v", backendPids, string(dat.WALData))
				gotAfterReconnect = true
			}
		case *pglogrepl.PrimaryKeepaliveMessage:
			t.Logf("PKM: %v", dat)
			r.AckLSN(dat.ServerWALEnd)
		}
	}

	if backendPids[0] == backendPids[1] {
		t.Errorf("expecting different backend PIDs. Got: %v", backendPids)
	}

	if !gotFirst {
		t.Fatal("expected at least one XLogData before reconnect")
	}
	if !gotAfterReconnect {
		t.Error("expected XLogData after reconnect; "+
			"receiver did not reconnect or did not deliver new WAL")
	}
}
 
// Local Variables:
// tab-width: 4
// End:
