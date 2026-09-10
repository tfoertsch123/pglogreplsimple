package pglogreplsimple

import (
	"time"
	"errors"
	"context"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/tfoertsch123/pgconnstr"
)

// Shutdown signals the Receiver to stop. The provided err is stored as
// the cause of the shutdown and can be retrieved later with [Receiver.Err].
// Shutdown is safe to call from any goroutine and is the preferred way to
// stop a Receiver whose [Receiver.Produce] iterator is currently running.
func (r *Receiver) Shutdown(err error) {
	r.shutdownTrg(err)
}

// Plugin returns the name of the logical-decoding plugin in use by the
// current connection.  It is set after a successful connection and is empty
// before the first successful connection.
func (r *Receiver) Plugin() string {
	return r.plugin
}

// Params returns a copy of the Receiver's current configuration parameters.
// The returned value reflects the most recently applied [Param] (either from
// [WithParams] or [Receiver.RequestReload]).
func (r *Receiver) Params() Param {
	return r.p
}

// Err returns the error that caused the Receiver to shut down, or nil if
// it stopped without an error.  It should be called after the
// [Receiver.Produce] iterator has ended.
func (r *Receiver) Err() error {
	return r.lastErr
}

// State returns the current internal state of the Receiver.  This is
// primarily useful for debugging or monitoring.  See [Next] for the
// possible values.
func (r *Receiver) State() Next {
	return r.state
}

// errPause is similar to checkStop both check for a pending shutdown
// or reload. In addition to that, errPause waits for a while
// and returns Connect if the timer hits. errPause is supposed to be used
// by connInit.
// The passed cancel function should cancel the passed ctx.
func (r *Receiver) errPause(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	f string, p ...interface{},
) Next {
	r.lg.Errorf(f, p...)
	if r.conn != nil {
		r.conn.Close(context.Background())
		r.conn = nil
	}
	timer := time.AfterFunc(r.p.ErrorRetryInterval, func() {
		cancel(context.DeadlineExceeded)
	})
	defer timer.Stop()

	// The passed ctx is a child context of r.shutdownCtx. So, if the latter
	// is canceled, the former is also automatically canceled. In that case
	// both, r.shutdownCtx.Done() and ctx.Done(), are closed. A simple select
	// on both of them might pick ctx.Done() first. In that case we'd return
	// Connect. However, since the parent context is already canceled, that
	// fact should have priority. So, in case ctx.Done() is reported, we also
	// check the parent context and give it higher priority.

	select {
	case <- ctx.Done():
		select {
		case <- r.shutdownCtx.Done():
			if cause := context.Cause(r.shutdownCtx); cause != nil {
				r.lastErr = cause
			}
			return Stop
		default:
		}
		return Connect
	case <- r.shutdownCtx.Done():
		if cause := context.Cause(r.shutdownCtx); cause != nil {
			r.lastErr = cause
		}
		return Stop
	}
}

// checkStop is the equivalent of errPause only to be called by RecvOne
// instead of connInit. It checks for pending shutdown and reload events.
// If none are present, it just logs the error.
func (r *Receiver) checkStop() Next {
	select {
	case <- r.shutdownCtx.Done():
		if cause := context.Cause(r.shutdownCtx); cause != nil {
			r.lastErr = cause
		}
		return Stop
	default:
	}
	return Recv
}


// connInit() tries to connect to the DB and perform all the actions
// needed to start receiving data. On any error it calls errPause()
// and returns its return value.
// That means if the pause timer hits, Connect is returned. If the shutdown
// context was canceled while waiting, Stop is returned. Or, if a reload
// signal was caught, Reinit is returned.
func (r *Receiver) connInit() Next {
	if r.conn != nil {
		r.conn.Close(context.Background())
		r.conn = nil
	}

	// try to read a message
	ctx, cancel := context.WithCancelCause(r.shutdownCtx)
	defer cancel(nil)
	defer r.setCancelCurrent(nil)

	r.setCancelCurrent(cancel)

	ci, err := pgconnstr.Parse(r.p.ConnInfo)
	if err != nil {
		return r.errPause(ctx, cancel, "primary_conninfo: %v", err)
	}
	ci["replication"] = "database"

	conn, err := pgconn.Connect(ctx, ci.URL())
	if err != nil {
		return r.errPause(ctx, cancel, "PG Conn: %v", err)
	}
	r.conn = conn

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return r.errPause(ctx, cancel, "IdentifySystem: %v", err)
	}

	r.lg.Infof(
		"SystemID: %v, Timeline: %v, WALPos: %v, DBName: %v",
		sysident.SystemID, sysident.Timeline, sysident.XLogPos,
		sysident.DBName,
	)

	// check if the slot exists and has the expected plugin
	res, err := conn.Exec(
		ctx,
		`SELECT plugin, confirmed_flush_lsn, `+
			`CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn() `+
			`ELSE pg_current_wal_insert_lsn() END AS end_lsn `+
			`FROM pg_replication_slots `+
			`WHERE slot_name='`+strings.ReplaceAll(r.p.SlotName, `'`, `''`)+`'`,
	).ReadAll()
	if err != nil {
		return r.errPause(ctx, cancel, "Reading replication slot: %v", err)
	}

	if len(res[0].Rows) == 0 {
		return r.errPause(ctx, cancel,
			"Replication slot %v not found", r.p.SlotName)
	}

	if len(res[0].Rows) > 1 {
		return r.errPause(ctx, cancel,
			"Multiple Replication slots: %v", r.p.SlotName)
	}

	plugin := string(res[0].Rows[0][0])
	confirmedFlushLSN := string(res[0].Rows[0][1])
	serverEndLSN := string(res[0].Rows[0][2])
	r.lg.Infof("Plugin of slot %v: %v", r.p.SlotName, plugin)
	r.lg.Infof("Confirmed Flush LSN: %v", confirmedFlushLSN)
	r.lg.Infof("Server End LSN: %v", serverEndLSN)

	plugin_opts := []string{}
	if opt_, ok := r.acceptedPlugins[plugin]; !ok {
		r.shutdownTrg(ErrPlugin)
		return r.errPause(ctx, cancel, "plugin not acceptable: %v", plugin)
	} else {
		r.plugin = plugin
		plugin_opts = opt_
	}

	// check the lsn. If the slot's confirmedFlushLSN is ahead of our
	// LSN, refuse connection. If our LSN is 0, that's a special case,
	// when we connect for the very first time.
	slotlsn, err := pglogrepl.ParseLSN(confirmedFlushLSN)
	if err != nil {
		r.shutdownTrg(err)
		return r.errPause(ctx, cancel, "LSN %v: %v", confirmedFlushLSN, err)
	}

	ourlsn := r.recvStat.wpos
	adjstat := func() {}
	if ourlsn == pglogrepl.LSN(0) {
		ourlsn = r.startLSN
		if ourlsn == pglogrepl.LSN(0) {
			ourlsn = slotlsn
		}
		if ourlsn < slotlsn {
			r.shutdownTrg(ErrConfirmedFlushLSN)
			return r.errPause(ctx, cancel,
				"Confirmed Flush LSN %v is ahead of requested start LSN %v",
				slotlsn, ourlsn,
			)
		}
		// If the user requests a start LSN too far in the future, we
		// refuse to connect. PG would accept it. But we'd send our first
		// feedback message with that LSN as writeLSN. That would ruin
		// the slot if that's given by mistake. On the other hand, if it's
		// not a mistake, we'd just wait until the server is beyond that
		// point. For that reason, this is not an error causing us to
		// stop. Instead, we'd log a message until the server has reached
		// the given start LSN.
		endlsn, err := pglogrepl.ParseLSN(serverEndLSN)
		if err != nil {
			r.shutdownTrg(err)
			return r.errPause(ctx, cancel, "LSN %v: %v", serverEndLSN, err)
		}
		if r.startLSN > endlsn {
			return r.errPause(ctx, cancel,
				"StartLSN (%v) cannot be ahead of latest known LSN (%v)",
				r.startLSN, serverEndLSN)
		}
		
		adjstat = func() {
			r.recvStat.wpos = ourlsn
			r.recvStat.fpos = ourlsn
			r.recvStat.rpos = ourlsn
		}
	}
		
	err = pglogrepl.StartReplication(
		ctx,
		conn,
		r.p.SlotName,
		ourlsn,
		pglogrepl.StartReplicationOptions{PluginArgs: plugin_opts},
	)
	if err != nil {
		return r.errPause(ctx, cancel, "StartReplication: %v", err)
	}
	adjstat()

	r.scheduleFeedback()

	return Recv
}

func (r *Receiver) scheduleFeedback() {
	r.nextFeedback = time.Now().Add(r.p.FeedbackInterval)
}

// AckLSN advances the LSN positions reported to the server in the next
// standby status update.  It should be called from within the
// [Receiver.Produce] loop after processing a message.
//
// With a single argument, the write, flush, and replay positions are all set
// to at least the given LSN.  With two arguments, the second sets both the
// flush and replay positions.  With three arguments, the second sets the flush
// position and the third sets the replay position.  Positions are never moved
// backwards.
func (r *Receiver) AckLSN(write pglogrepl.LSN, other ...pglogrepl.LSN) {
	switch len(other) {
	case 0:
		r.recvStat.wpos = max(r.recvStat.wpos, write)
		r.recvStat.fpos = max(r.recvStat.wpos, write)
		r.recvStat.rpos = max(r.recvStat.wpos, write)
	case 1:
		r.recvStat.wpos = max(r.recvStat.wpos, write)
		r.recvStat.fpos = max(r.recvStat.wpos, other[0])
		r.recvStat.rpos = max(r.recvStat.wpos, other[0])
	default:
		r.recvStat.wpos = max(r.recvStat.wpos, write)
		r.recvStat.fpos = max(r.recvStat.wpos, other[0])
		r.recvStat.rpos = max(r.recvStat.wpos, other[1])
	}
}

func (r *Receiver) sendFeedback() error {
	if r.prevStat != r.recvStat {
		r.lg.Debugf("Sending feedback: write: %v, flush: %v, replay: %v",
			r.recvStat.wpos, r.recvStat.fpos, r.recvStat.rpos)
	}
	// the context parameter here is currently ignored by
	// pglogrepl.SendStandbyStatusUpdate()
	err := pglogrepl.SendStandbyStatusUpdate(
		r.shutdownCtx,
		r.conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: r.recvStat.wpos,
			WALFlushPosition: r.recvStat.fpos,
			WALApplyPosition: r.recvStat.rpos,
		},
	)
	if err != nil {
		return err
	}

	r.prevStat = r.recvStat
	r.scheduleFeedback()
	return nil
}

func (r *Receiver) processNotice(
	msg *pgproto3.NoticeResponse,
	yield func(MsgItem) bool,
) Next {
	if !yield(msg) {
		return Break
	}
	return Recv
}

func (r *Receiver) processCopyData(
	msg *pgproto3.CopyData,
	yield func(MsgItem) bool,
) Next {
	// Right after connecting to the DB, the DB sends a
	// PrimaryKeepaliveMessage with the slot's confirmed_flush_lsn
	// as payload.
	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
		if err != nil {
			r.lg.Errorf("PKAL %v failed to parse: %v", msg.Data[1:], err)
			return Connect
		}
		// m.relg.Debg4f("PKAL: %#v", pkm)
		if pkm.ReplyRequested {
			r.lg.Debg3("PKAL: immediate reply requested")
			r.sendFeedback()
		}
		if !yield(&pkm) {
			return Break
		}

	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
		if err != nil {
			r.lg.Errorf("ParseXLogData failed: %v", err)
			return Connect
		}
		if !yield(&xld) {
			return Break
		}
	default:
		r.lg.Errorf("Unexpected CopyData message type: <%v> -- ignored",
			msg.Data[0])
	}
	return Recv
}

func (r *Receiver) _recvOneMsg(
	tmout time.Duration,
) (pgproto3.BackendMessage, error) {
	ctx, cancel := context.WithCancelCause(r.shutdownCtx)
	timer := time.AfterFunc(tmout, func() {
		cancel(context.DeadlineExceeded)
	})
	defer r.setCancelCurrent(nil)
	defer cancel(nil)
	defer timer.Stop()

	r.setCancelCurrent(cancel)
	return r.conn.ReceiveMessage(ctx)
}
	
func (r *Receiver) recvOne(yield func(MsgItem) bool) Next {
	// check for pending shutdown or reload
	if nxt := r.checkStop(); nxt != Recv {
		return nxt
	}

	timeLeftUntilFeedback := time.Until(r.nextFeedback)
	if timeLeftUntilFeedback < 5*time.Millisecond {
		if err := r.sendFeedback(); err != nil {
			r.lg.Errorf("SendStandbyStatusUpdate failed: %v", err)
			// in case of an error, the safest thing to do is to reconnect.
			return Connect
		}
		return Recv
	}

	rawMsg, err := r._recvOneMsg(timeLeftUntilFeedback)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			// this will call the next RecvOne and the checkStop call above
			// will then return stop after reporting the cause.
			return Recv
		}

		// otherwise, the safest thing to do is to reconnect.
		r.lg.Errorf("ReceiveMessage failed: %v", err)
		return Connect
	}

	// analyze the message
	switch msg := rawMsg.(type) {
	case *pgproto3.ErrorResponse:
		r.lg.Errorf("PG error: %v", msg)
		return Connect
	case *pgproto3.NoticeResponse:
		return r.processNotice(msg, yield)
	case *pgproto3.CopyData:
		return r.processCopyData(msg, yield)
	default:
		r.lg.Errorf("ReceiveMessage: got unexpected message of type %T",
			rawMsg)
		return Recv
	}
}
 
// Local Variables:
// tab-width: 4
// End:
