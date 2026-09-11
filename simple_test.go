package pglogreplsimple

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
)

// ---------------------------------------------------------------- helpers

// fakeLogger is a no-op Logger that satisfies the Logger interface.
type fakeLogger struct{}

func (fakeLogger) Error(string)               {}
func (fakeLogger) Warn(string)                {}
func (fakeLogger) Info(string)                {}
func (fakeLogger) Debug(string)               {}
func (fakeLogger) Debg2(string)               {}
func (fakeLogger) Debg3(string)               {}
func (fakeLogger) Debg4(string)               {}
func (fakeLogger) Debg5(string)               {}
func (fakeLogger) Errorf(string, ...any)      {}
func (fakeLogger) Warnf(string, ...any)       {}
func (fakeLogger) Infof(string, ...any)       {}
func (fakeLogger) Debugf(string, ...any)      {}
func (fakeLogger) Debg2f(string, ...any)      {}
func (fakeLogger) Debg3f(string, ...any)      {}
func (fakeLogger) Debg4f(string, ...any)      {}
func (fakeLogger) Debg5f(string, ...any)      {}

// newTestReceiver builds a Receiver whose fields are populated enough to
// exercise the state-machine methods without a real database connection.
func newTestReceiver() *Receiver {
	return &Receiver{
		p: Param{
			ErrorRetryInterval: DefaultErrorRetryInterval,
			FeedbackInterval:   DefaultFeedbackInterval,
		},
		lg:              fakeLogger{},
		acceptedPlugins: DefaultPlugins,
	}
}

// buildKeepaliveData constructs the payload of a CopyData message carrying a
// PrimaryKeepaliveMessage, matching the wire format expected by
// pglogrepl.ParsePrimaryKeepaliveMessage.
func buildKeepaliveData(serverWALEnd pglogrepl.LSN, replyRequested bool) []byte {
	buf := make([]byte, 0, 18)
	buf = append(buf, byte(pglogrepl.PrimaryKeepaliveMessageByteID))
	tmp := make([]byte, 8)
	binary.BigEndian.PutUint64(tmp, uint64(serverWALEnd))
	buf = append(buf, tmp...)          // ServerWALEnd
	buf = append(buf, make([]byte, 8)...) // ServerTime (zero)
	if replyRequested {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	return buf
}

// buildXLogData constructs the payload of a CopyData message carrying an
// XLogData message, matching the wire format expected by
// pglogrepl.ParseXLogData.
func buildXLogData(walStart, serverWALEnd pglogrepl.LSN, walData []byte) []byte {
	buf := make([]byte, 0, 25+len(walData))
	buf = append(buf, byte(pglogrepl.XLogDataByteID))
	tmp := make([]byte, 8)
	binary.BigEndian.PutUint64(tmp, uint64(walStart))
	buf = append(buf, tmp...)
	binary.BigEndian.PutUint64(tmp, uint64(serverWALEnd))
	buf = append(buf, tmp...)
	buf = append(buf, make([]byte, 8)...) // ServerTime (zero)
	buf = append(buf, walData...)
	return buf
}

// ---------------------------------------------------------------- Next.String

func TestNextString(t *testing.T) {
	cases := []struct {
		state Next
		want  string
	}{
		{Connect, "Connect"},
		{Recv, "Recv"},
		{Break, "Break"},
		{Stop, "Stop"},
	}
	for _, tc := range cases {
		if got := tc.state.String(); got != tc.want {
			t.Errorf("Next(%d).String() = %q, want %q", tc.state, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------- NewReceiver

func TestNewReceiverDefaults(t *testing.T) {
	r := NewReceiver()
	if r.p.ErrorRetryInterval != DefaultErrorRetryInterval {
		t.Errorf("ErrorRetryInterval = %v, want %v",
			r.p.ErrorRetryInterval, DefaultErrorRetryInterval)
	}
	if r.p.FeedbackInterval != DefaultFeedbackInterval {
		t.Errorf("FeedbackInterval = %v, want %v",
			r.p.FeedbackInterval, DefaultFeedbackInterval)
	}
	if r.acceptedPlugins == nil {
		t.Error("acceptedPlugins is nil, want DefaultPlugins")
	}
	if r.startLSN != 0 {
		t.Errorf("startLSN = %v, want 0", r.startLSN)
	}
}

func TestNewReceiverWithOptions(t *testing.T) {
	lg := fakeLogger{}
	params := &Param{
		Logger:             lg,
		ConnInfo:           "host=localhost dbname=test",
		SlotName:           "my_slot",
		ErrorRetryInterval: 3 * time.Second,
		FeedbackInterval:   5 * time.Second,
	}
	r := NewReceiver(
		WithParams(params),
		WithStartLSN(12345),
		WithAcceptedPlugins(map[string][]string{"pgoutput": {`"proto_version" '1'`}}),
	)
	if r.reload_p == nil {
		t.Fatal("reload_p is nil, expected it to hold the initial params")
	}
	if r.reload_p.ConnInfo != "host=localhost dbname=test" {
		t.Errorf("reload_p.ConnInfo = %q, want %q",
			r.reload_p.ConnInfo, "host=localhost dbname=test")
	}
	if r.startLSN != 12345 {
		t.Errorf("startLSN = %v, want 12345", r.startLSN)
	}
	if _, ok := r.acceptedPlugins["pgoutput"]; !ok {
		t.Error("acceptedPlugins missing pgoutput")
	}
	if _, ok := r.acceptedPlugins["wal2json"]; ok {
		t.Error("acceptedPlugins should not contain wal2json")
	}
}

// ---------------------------------------------------------------- AckLSN

func TestAckLSNSingleArg(t *testing.T) {
	r := newTestReceiver()
	r.AckLSN(100)
	if r.recvStat.wpos != 100 || r.recvStat.fpos != 100 || r.recvStat.rpos != 100 {
		t.Errorf("after AckLSN(100): wpos=%d fpos=%d rpos=%d, want all 100",
			r.recvStat.wpos, r.recvStat.fpos, r.recvStat.rpos)
	}
}

func TestAckLSNMonotonic(t *testing.T) {
	r := newTestReceiver()
	r.AckLSN(100)
	r.AckLSN(50) // should not move backwards
	if r.recvStat.wpos != 100 {
		t.Errorf("wpos = %d after lower AckLSN, want 100", r.recvStat.wpos)
	}
}

func TestAckLSNTwoArgs(t *testing.T) {
	r := newTestReceiver()
	r.AckLSN(100, 80)
	if r.recvStat.wpos != 100 {
		t.Errorf("wpos = %d, want 100", r.recvStat.wpos)
	}
	if r.recvStat.fpos != 100 {
		// max(r.recvStat.wpos, other[0]) = max(100,80)=100
		t.Errorf("fpos = %d, want 100", r.recvStat.fpos)
	}
	if r.recvStat.rpos != 100 {
		t.Errorf("rpos = %d, want 100", r.recvStat.rpos)
	}
}

func TestAckLSNThreeArgs(t *testing.T) {
	r := newTestReceiver()
	r.AckLSN(200, 150, 100)
	if r.recvStat.wpos != 200 {
		t.Errorf("wpos = %d, want 200", r.recvStat.wpos)
	}
	if r.recvStat.fpos != 200 {
		// max(r.recvStat.wpos, other[0]) = max(200,150)=200
		t.Errorf("fpos = %d, want 200", r.recvStat.fpos)
	}
	if r.recvStat.rpos != 200 {
		// max(r.recvStat.wpos, other[1]) = max(200,100)=200
		t.Errorf("rpos = %d, want 200", r.recvStat.rpos)
	}
}

func TestAckLSNThreeArgsAdvancing(t *testing.T) {
	r := newTestReceiver()
	// Start from a high wpos so the max with wpos does not mask the other args.
	r.recvStat.wpos = 500
	r.AckLSN(600, 550, 520)
	if r.recvStat.wpos != 600 {
		t.Errorf("wpos = %d, want 600", r.recvStat.wpos)
	}
	// fpos = max(wpos_after_update, other[0]) = max(600, 550) = 600
	if r.recvStat.fpos != 600 {
		t.Errorf("fpos = %d, want 600", r.recvStat.fpos)
	}
	// rpos = max(wpos_after_update, other[1]) = max(600, 520) = 600
	if r.recvStat.rpos != 600 {
		t.Errorf("rpos = %d, want 600", r.recvStat.rpos)
	}
}

// ----------------------------------------------------------- scheduleFeedback

func TestScheduleFeedback(t *testing.T) {
	r := newTestReceiver()
	before := time.Now()
	r.scheduleFeedback()
	want := before.Add(r.p.FeedbackInterval)
	got := r.nextFeedback
	// Allow a few milliseconds of slack.
	if got.Before(want.Add(-50*time.Millisecond)) ||
		got.After(want.Add(50*time.Millisecond)) {
		t.Errorf("nextFeedback = %v, want ~%v", got, want)
	}
}

// ---------------------------------------------------------------- checkStop

func TestCheckStopNoShutdown(t *testing.T) {
	r := newTestReceiver()
	r.shutdownCtx, r.shutdownTrg = context.WithCancelCause(context.Background())
	defer r.shutdownTrg(nil)
	if got := r.checkStop(); got != Recv {
		t.Errorf("checkStop() = %v, want Recv", got)
	}
}

func TestCheckStopShutdown(t *testing.T) {
	r := newTestReceiver()
	r.shutdownCtx, r.shutdownTrg = context.WithCancelCause(context.Background())
	causeErr := context.Canceled
	r.shutdownTrg(causeErr)
	if got := r.checkStop(); got != Stop {
		t.Errorf("checkStop() = %v, want Stop", got)
	}
	if r.lastErr != causeErr {
		t.Errorf("lastErr = %v, want %v", r.lastErr, causeErr)
	}
}

// ---------------------------------------------------------------- errPause

func TestErrPauseTimerReturnsConnect(t *testing.T) {
	r := newTestReceiver()
	r.p.ErrorRetryInterval = 10 * time.Millisecond // very short
	r.shutdownCtx, r.shutdownTrg = context.WithCancelCause(context.Background())
	defer r.shutdownTrg(nil)
	ctx, cancel := context.WithCancelCause(r.shutdownCtx)
	defer cancel(nil)
	got := r.errPause(ctx, cancel, "test error")
	if got != Connect {
		t.Errorf("errPause() = %v, want Connect", got)
	}
}

func TestErrPauseShutdownDuringPause(t *testing.T) {
	r := newTestReceiver()
	r.p.ErrorRetryInterval = 10 * time.Second // long so shutdown wins
	r.shutdownCtx, r.shutdownTrg = context.WithCancelCause(context.Background())
	causeErr := context.Canceled
	r.shutdownTrg(causeErr)
	ctx, cancel := context.WithCancelCause(r.shutdownCtx)
	defer cancel(nil)
	got := r.errPause(ctx, cancel, "test error")
	if got != Stop {
		t.Errorf("errPause() = %v, want Stop", got)
	}
	if r.lastErr != causeErr {
		t.Errorf("lastErr = %v, want %v", r.lastErr, causeErr)
	}
}

// ---------------------------------------------------------------- configure

func TestConfigureNoLogger(t *testing.T) {
	r := newTestReceiver()
	r.lg = nil // remove logger
	got := r.configure(Connect)
	if got != Stop {
		t.Errorf("configure() = %v, want Stop", got)
	}
	if r.lastErr != ErrNoLogger {
		t.Errorf("lastErr = %v, want ErrNoLogger", r.lastErr)
	}
}

func TestConfigureNoReload(t *testing.T) {
	r := newTestReceiver()
	got := r.configure(Recv)
	if got != Recv {
		t.Errorf("configure() = %v, want Recv (unchanged)", got)
	}
}

func TestConfigureIntervalClamping(t *testing.T) {
	r := newTestReceiver()
	r.reload_p = &Param{
		Logger:             fakeLogger{},
		ErrorRetryInterval: 100 * time.Millisecond, // too small
		FeedbackInterval:   200 * time.Millisecond, // too small
	}
	r.configure(Recv)
	if r.p.ErrorRetryInterval != DefaultErrorRetryInterval {
		t.Errorf("ErrorRetryInterval = %v, want %v",
			r.p.ErrorRetryInterval, DefaultErrorRetryInterval)
	}
	if r.p.FeedbackInterval != DefaultFeedbackInterval {
		t.Errorf("FeedbackInterval = %v, want %v",
			r.p.FeedbackInterval, DefaultFeedbackInterval)
	}
}

func TestConfigureConnInfoChangeTriggersConnect(t *testing.T) {
	r := newTestReceiver()
	r.p.ConnInfo = "host=localhost dbname=old"
	r.reload_p = &Param{
		Logger:             fakeLogger{},
		ConnInfo:           "host=localhost dbname=new",
		ErrorRetryInterval: DefaultErrorRetryInterval,
		FeedbackInterval:   DefaultFeedbackInterval,
	}
	got := r.configure(Recv)
	if got != Connect {
		t.Errorf("configure() = %v, want Connect", got)
	}
	if r.p.ConnInfo != "host=localhost dbname=new" {
		t.Errorf("ConnInfo = %q, want %q", r.p.ConnInfo, "host=localhost dbname=new")
	}
}

func TestConfigureSlotNameChangeTriggersConnect(t *testing.T) {
	r := newTestReceiver()
	r.p.SlotName = "old_slot"
	r.reload_p = &Param{
		Logger:             fakeLogger{},
		SlotName:           "new_slot",
		ErrorRetryInterval: DefaultErrorRetryInterval,
		FeedbackInterval:   DefaultFeedbackInterval,
	}
	got := r.configure(Recv)
	if got != Connect {
		t.Errorf("configure() = %v, want Connect", got)
	}
	if r.p.SlotName != "new_slot" {
		t.Errorf("SlotName = %q, want %q", r.p.SlotName, "new_slot")
	}
}

func TestConfigureSameValuesNoReconnect(t *testing.T) {
	r := newTestReceiver()
	r.p.ConnInfo = "host=localhost dbname=same"
	r.p.SlotName = "same_slot"
	r.reload_p = &Param{
		Logger:             fakeLogger{},
		ConnInfo:           "host=localhost dbname=same",
		SlotName:           "same_slot",
		ErrorRetryInterval: DefaultErrorRetryInterval,
		FeedbackInterval:   DefaultFeedbackInterval,
	}
	got := r.configure(Recv)
	if got != Recv {
		t.Errorf("configure() = %v, want Recv", got)
	}
}

func TestConfigureLoggerSwap(t *testing.T) {
	r := newTestReceiver()
	r.lg = nil // start with no logger
	newLogger := fakeLogger{}
	r.reload_p = &Param{
		Logger:             newLogger,
		ErrorRetryInterval: DefaultErrorRetryInterval,
		FeedbackInterval:   DefaultFeedbackInterval,
	}
	r.configure(Recv)
	if r.lg == nil {
		t.Error("lg is nil after configure with new logger")
	}
}

func TestConfigureCloseOnActivation(t *testing.T) {
	r := newTestReceiver()
	ch := make(chan struct{}, 1)
	r.reload_p = &Param{
		Logger:             fakeLogger{},
		CloseOnActivation:  ch,
		ErrorRetryInterval: DefaultErrorRetryInterval,
		FeedbackInterval:   DefaultFeedbackInterval,
	}
	r.configure(Recv)
	select {
	case <-ch:
		// good
	default:
		t.Error("CloseOnActivation channel was not closed")
	}
}

func TestConfigureBreakShortCircuit(t *testing.T) {
	r := newTestReceiver()
	r.state = Break
	r.reload_p = &Param{
		Logger:             fakeLogger{},
		ConnInfo:           "host=localhost dbname=changed",
		ErrorRetryInterval: DefaultErrorRetryInterval,
		FeedbackInterval:   DefaultFeedbackInterval,
	}
	got := r.configure(Break)
	if got != Break {
		t.Errorf("configure(Break) = %v, want Break", got)
	}
	if r.p.ConnInfo != "" {
		t.Errorf("ConnInfo = %q, want unchanged (empty)", r.p.ConnInfo)
	}
}

func TestConfigureStopShortCircuit(t *testing.T) {
	r := newTestReceiver()
	r.state = Stop
	r.reload_p = &Param{
		Logger:             fakeLogger{},
		ConnInfo:           "host=localhost dbname=changed",
		ErrorRetryInterval: DefaultErrorRetryInterval,
		FeedbackInterval:   DefaultFeedbackInterval,
	}
	got := r.configure(Stop)
	if got != Stop {
		t.Errorf("configure(Stop) = %v, want Stop", got)
	}
}

// -------------------------------------------------------------- RequestReload

func TestRequestReload(t *testing.T) {
	r := newTestReceiver()
	newParams := Param{
		Logger:             fakeLogger{},
		ConnInfo:           "host=localhost dbname=reload",
		SlotName:           "reload_slot",
		ErrorRetryInterval: 7 * time.Second,
		FeedbackInterval:   3 * time.Second,
	}
	r.RequestReload(newParams)
	if r.reload_p == nil {
		t.Fatal("reload_p is nil after RequestReload")
	}
	if r.reload_p.ConnInfo != "host=localhost dbname=reload" {
		t.Errorf("reload_p.ConnInfo = %q, want %q",
			r.reload_p.ConnInfo, "host=localhost dbname=reload")
	}
}

func TestRequestReloadInterruptsCurrent(t *testing.T) {
	r := newTestReceiver()
	canceled := false
	r.cancelCurrent = func(cause error) {
		canceled = true
		_ = cause
	}
	r.RequestReload(Param{Logger: fakeLogger{}})
	if !canceled {
		t.Error("cancelCurrent was not called during RequestReload")
	}
}

// -------------------------------------------------------------- processNotice

func TestProcessNoticeYieldTrue(t *testing.T) {
	r := newTestReceiver()
	r.shutdownCtx, r.shutdownTrg = context.WithCancelCause(context.Background())
	defer r.shutdownTrg(nil)
	msg := &pgproto3.NoticeResponse{}
	yielded := false
	yield := func(m MsgItem) bool {
		yielded = true
		if _, ok := m.(*pgproto3.NoticeResponse); !ok {
			t.Errorf("yielded type = %T, want *pgproto3.NoticeResponse", m)
		}
		return true
	}
	got := r.processNotice(msg, yield)
	if got != Recv {
		t.Errorf("processNotice() = %v, want Recv", got)
	}
	if !yielded {
		t.Error("yield was not called")
	}
}

func TestProcessNoticeYieldFalse(t *testing.T) {
	r := newTestReceiver()
	r.shutdownCtx, r.shutdownTrg = context.WithCancelCause(context.Background())
	defer r.shutdownTrg(nil)
	msg := &pgproto3.NoticeResponse{}
	yield := func(m MsgItem) bool {
		return false
	}
	got := r.processNotice(msg, yield)
	if got != Break {
		t.Errorf("processNotice() = %v, want Break", got)
	}
}

// ------------------------------------------------------------ processCopyData

func TestProcessCopyDataXLogData(t *testing.T) {
	r := newTestReceiver()
	r.shutdownCtx, r.shutdownTrg = context.WithCancelCause(context.Background())
	defer r.shutdownTrg(nil)
	data := buildXLogData(0, 100, []byte("test wal data"))
	msg := &pgproto3.CopyData{Data: data}
	yielded := false
	yield := func(m MsgItem) bool {
		yielded = true
		xld, ok := m.(*pglogrepl.XLogData)
		if !ok {
			t.Fatalf("yielded type = %T, want *pglogrepl.XLogData", m)
		}
		if xld.WALStart != 0 {
			t.Errorf("WALStart = %v, want 0", xld.WALStart)
		}
		if xld.ServerWALEnd != 100 {
			t.Errorf("ServerWALEnd = %v, want 100", xld.ServerWALEnd)
		}
		if string(xld.WALData) != "test wal data" {
			t.Errorf("WALData = %q, want %q", xld.WALData, "test wal data")
		}
		return true
	}
	got := r.processCopyData(msg, yield)
	if got != Recv {
		t.Errorf("processCopyData() = %v, want Recv", got)
	}
	if !yielded {
		t.Error("yield was not called")
	}
}

func TestProcessCopyDataXLogDataYieldFalse(t *testing.T) {
	r := newTestReceiver()
	r.shutdownCtx, r.shutdownTrg = context.WithCancelCause(context.Background())
	defer r.shutdownTrg(nil)
	data := buildXLogData(50, 200, []byte("data"))
	msg := &pgproto3.CopyData{Data: data}
	yield := func(m MsgItem) bool { return false }
	got := r.processCopyData(msg, yield)
	if got != Break {
		t.Errorf("processCopyData() = %v, want Break", got)
	}
}

func TestProcessCopyDataKeepaliveNoReply(t *testing.T) {
	r := newTestReceiver()
	r.shutdownCtx, r.shutdownTrg = context.WithCancelCause(context.Background())
	defer r.shutdownTrg(nil)
	data := buildKeepaliveData(42, false)
	msg := &pgproto3.CopyData{Data: data}
	yielded := false
	yield := func(m MsgItem) bool {
		yielded = true
		pkm, ok := m.(*pglogrepl.PrimaryKeepaliveMessage)
		if !ok {
			t.Fatalf(
				"yielded type = %T, want *pglogrepl.PrimaryKeepaliveMessage",
				m,
			)
		}
		if pkm.ServerWALEnd != 42 {
			t.Errorf("ServerWALEnd = %v, want 42", pkm.ServerWALEnd)
		}
		if pkm.ReplyRequested {
			t.Error("ReplyRequested = true, want false")
		}
		return true
	}
	got := r.processCopyData(msg, yield)
	if got != Recv {
		t.Errorf("processCopyData() = %v, want Recv", got)
	}
	if !yielded {
		t.Error("yield was not called")
	}
}

func TestProcessCopyDataKeepaliveYieldFalse(t *testing.T) {
	r := newTestReceiver()
	r.shutdownCtx, r.shutdownTrg = context.WithCancelCause(context.Background())
	defer r.shutdownTrg(nil)
	data := buildKeepaliveData(42, false)
	msg := &pgproto3.CopyData{Data: data}
	yield := func(m MsgItem) bool { return false }
	got := r.processCopyData(msg, yield)
	if got != Break {
		t.Errorf("processCopyData() = %v, want Break", got)
	}
}

func TestProcessCopyDataUnknownByte(t *testing.T) {
	r := newTestReceiver()
	r.shutdownCtx, r.shutdownTrg = context.WithCancelCause(context.Background())
	defer r.shutdownTrg(nil)
	data := []byte{0xFF, 0x01, 0x02}
	msg := &pgproto3.CopyData{Data: data}
	yield := func(m MsgItem) bool {
		t.Error("yield should not be called for unknown CopyData type")
		return true
	}
	got := r.processCopyData(msg, yield)
	if got != Recv {
		t.Errorf("processCopyData() = %v, want Recv", got)
	}
}

func TestProcessCopyDataBadKeepaliveParse(t *testing.T) {
	r := newTestReceiver()
	r.shutdownCtx, r.shutdownTrg = context.WithCancelCause(context.Background())
	defer r.shutdownTrg(nil)
	// PrimaryKeepaliveMessageByteID but wrong length (must be exactly 17 bytes
	// after the type byte).
	data := []byte{byte(pglogrepl.PrimaryKeepaliveMessageByteID), 0x00, 0x01}
	msg := &pgproto3.CopyData{Data: data}
	yield := func(m MsgItem) bool {
		t.Error("yield should not be called for parse failure")
		return true
	}
	got := r.processCopyData(msg, yield)
	if got != Connect {
		t.Errorf("processCopyData() = %v, want Connect", got)
	}
}

func TestProcessCopyDataBadXLogDataParse(t *testing.T) {
	r := newTestReceiver()
	r.shutdownCtx, r.shutdownTrg = context.WithCancelCause(context.Background())
	defer r.shutdownTrg(nil)
	// XLogDataByteID but too short (must be at least 24 bytes after type byte).
	data := []byte{byte(pglogrepl.XLogDataByteID), 0x00, 0x01, 0x02}
	msg := &pgproto3.CopyData{Data: data}
	yield := func(m MsgItem) bool {
		t.Error("yield should not be called for parse failure")
		return true
	}
	got := r.processCopyData(msg, yield)
	if got != Connect {
		t.Errorf("processCopyData() = %v, want Connect", got)
	}
}

// ------------------------------------------------ Produce / Close error paths

func TestProduceLocked(t *testing.T) {
	r := newTestReceiver()
	// Hold the producing lock so a second call gets ErrReceiverLocked.
	if !r.producing.TryLock() {
		t.Fatal("could not acquire producing lock")
	}
	defer r.producing.Unlock()
	_, err := r.Produce(context.Background())
	if err != ErrReceiverLocked {
		t.Errorf("Produce() err = %v, want ErrReceiverLocked", err)
	}
}

func TestProduceStopped(t *testing.T) {
	r := newTestReceiver()
	r.state = Stop
	_, err := r.Produce(context.Background())
	if err != ErrReceiverStopped {
		t.Errorf("Produce() err = %v, want ErrReceiverStopped", err)
	}
}

func TestCloseLocked(t *testing.T) {
	r := newTestReceiver()
	if !r.producing.TryLock() {
		t.Fatal("could not acquire producing lock")
	}
	defer r.producing.Unlock()
	err := r.Close()
	if err != ErrReceiverLocked {
		t.Errorf("Close() err = %v, want ErrReceiverLocked", err)
	}
}

func TestCloseStopped(t *testing.T) {
	r := newTestReceiver()
	r.state = Stop
	err := r.Close()
	if err != ErrReceiverStopped {
		t.Errorf("Close() err = %v, want ErrReceiverStopped", err)
	}
}

func TestCloseClean(t *testing.T) {
	r := newTestReceiver()
	err := r.Close()
	if err != nil {
		t.Errorf("Close() err = %v, want nil", err)
	}
	if r.state != Stop {
		t.Errorf("state = %v, want Stop", r.state)
	}
}

// ---------------------------------------------------------------- Getters

func TestGetters(t *testing.T) {
	r := newTestReceiver()
	r.plugin = "wal2json"
	r.state = Recv
	r.lastErr = ErrPlugin
	if r.Plugin() != "wal2json" {
		t.Errorf("Plugin() = %q, want %q", r.Plugin(), "wal2json")
	}
	if r.State() != Recv {
		t.Errorf("State() = %v, want Recv", r.State())
	}
	if r.Err() != ErrPlugin {
		t.Errorf("Err() = %v, want ErrPlugin", r.Err())
	}
	// Params should return a copy of the current params.
	p := r.Params()
	if p.ErrorRetryInterval != r.p.ErrorRetryInterval {
		t.Errorf("Params().ErrorRetryInterval = %v, want %v",
			p.ErrorRetryInterval, r.p.ErrorRetryInterval)
	}
}

func TestShutdown(t *testing.T) {
	r := newTestReceiver()
	r.shutdownCtx, r.shutdownTrg = context.WithCancelCause(context.Background())
	r.Shutdown(context.Canceled)
	select {
	case <-r.shutdownCtx.Done():
		// good
	default:
		t.Error("shutdownCtx was not canceled after Shutdown()")
	}
}
 
// Local Variables:
// tab-width: 4
// End:
