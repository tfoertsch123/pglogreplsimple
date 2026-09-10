package main

import (
	"os"
	"os/signal"
	"errors"
	"syscall"
	"time"
	"path/filepath"
	"encoding/json"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/tfoertsch123/log"
	cap "github.com/tfoertsch123/pglogreplsimple"
)

var ErrShutdown = errors.New("Shutdown signal")
func main() {
	_, basename := filepath.Split(os.Args[0])
	if len(os.Args) < 3 {
		log.Panicf("%s: CONNINFO SLOTNAME [STARTLSN]", basename)
	}
	log.SetLevel(log.DEBG3)
	params := &cap.Param{
		Logger: log.L(),
		ConnInfo: os.Args[1],
		SlotName: os.Args[2],
	}
	confirmedFlushLsn := pglogrepl.LSN(0)
	if len(os.Args) > 3 {
		if err := (&confirmedFlushLsn).Scan(os.Args[3]); err != nil {
			log.Panicf("invalid start LSN (%v): %v", os.Args[3], err)
		}
	}
	m := cap.NewReceiver(
		cap.WithParams(params),
		cap.WithAcceptedPlugins(map[string][]string{
			"wal2json": cap.DefaultPlugins["wal2json"],
		}),
		cap.WithStartLSN(confirmedFlushLsn),
	)

	shutdownCh := make(chan os.Signal, 1)
    signal.Notify(shutdownCh, syscall.SIGINT, syscall.SIGTERM)

	// signal loop
    go func() {
		defer func() {
			signal.Stop(shutdownCh)
			close(shutdownCh)
		}()
        for {
            select {
            case s := <-shutdownCh:
				log.Noticef("got signal %v", s)
				m.Shutdown(ErrShutdown)
            }
        }
    }()

	it, err := m.Produce(nil)
	if err != nil {
		log.Panicf("Produce: %v", err)
	}

	// We want to advance the confirmed_flush_lsn in the replication slot
	// as closely as possible. We have 2 sources of LSNs. One source is the
	// decoded transaction. A transaction is only decoded when it is committed.
	// At that point the transaction is transmitted with the statements in the
	// right order within the transaction. But a transaction that's committed
	// later might contain statements at a WAL position before the previously
	// committed transaction. The only reliable order is the commit record's
	// "nextlsn" field. A transaction's begin record contains the same
	// "nextlsn" field.
	// So, we acknowledge as processed each "nextlsn" of a commit record.
	//
	// The other source is the primary keepalive message. A DB cluster shares
	// the WAL stream. But logical decoding works per database. So, other DBs
	// within the same cluster might generate WAL while the DB we are
	// monitoring is totally idle. In that case, we want to acknowledge the
	// PKM's LSN. There is also WAL generating activity within our DB that
	// does not trigger logical decoding, for instance VACUUM. So, that
	// problem does not disappear if the cluster only has one active DB.
	//
	// Now, if the logical decoder is decoding a large transaction, it can
	// happen a primary keepalive message is transmitted with a much higher
	// LSN than the current transaction's commit LSN. So, we can't blindly
	// acknowledge every PKM's LSN because it might tell the replication
	// slot we have already processed transactions that have not even reached
	// us. But in case of inactivity in our database we somewhat want to
	// follow the PKM's LSN.
	//
	// Here is the solution. PKMs are sent by the DB every 10 seconds even
	// if there is no activity in the DB. So, each time we read a commit
	// record we record the current local time in lastCommitTime. We ignore
	// PKMs with a time of arrival less than 5 seconds after the most recent
	// commit record or if the PKM has arrived between a begin and a
	// commit record. If a PKM arrives outside of a B/C pair and more than
	// 5 seconds after the C, then we acknowledge its LSN.
	inTxn := false
	lastCommitTime := time.Now()
	for msg := range it {
		switch dat := msg.(type) {
		case *pglogrepl.XLogData:
			log.Infof("XLD>> WALStart=%v, ServerWALEnd=%v, ServerTime=%v",
				dat.WALStart, dat.ServerWALEnd, dat.ServerTime)
			var jdata interface{}
			err := json.Unmarshal(dat.WALData, &jdata)
			if err != nil {
				log.Errorf("Could not parse JSON content: %v", err)
			}

			switch v := jdata.(type) {
			case map[string]interface{}:
				log.Infof("%v", string(dat.WALData))
				switch v["action"] {
				case "B":
					inTxn = true
				case "C":
					// found a commit record: advance replication slot
					var lsn pglogrepl.LSN
					(&lsn).Scan(v["nextlsn"])
					m.AckLSN(lsn)
					inTxn = false
					lastCommitTime = time.Now()
				}
			default:
				log.Errorf("unexpeced JSON type %[1]T: %[1]v", v)
			}

		case *pglogrepl.PrimaryKeepaliveMessage:
			log.Infof("PKM>> ServerWALEnd=%v, ServerTime=%v, ReplyReq=%v",
				dat.ServerWALEnd, dat.ServerTime, dat.ReplyRequested)
			if !inTxn && time.Now().After(lastCommitTime.Add(5*time.Second)) {
				m.AckLSN(dat.ServerWALEnd)
			}

		case *pgproto3.NoticeResponse:
			log.Infof("notice>> %v", dat.Message)
			
		default:
			log.Infof("SHOULD NOT HAPPEN>> %T", msg)
		}
	}
	if err = m.Err(); err != nil {
		log.Infof("lastErr: %v", err)	
	}
	log.Notice("Shutdown complete")
}

// Local Variables:
// tab-width: 4
// End:
