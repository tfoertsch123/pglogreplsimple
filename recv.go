// Package pglogreplsimple provides a high-level wrapper around the
// github.com/jackc/pglogrepl package for consuming PostgreSQL logical
// replication streams.
//
// It manages the full lifecycle of a logical replication receiver:
// connecting to the database, identifying the system, validating the
// replication slot, starting replication, receiving WAL messages, and
// sending periodic standby status updates (feedback) to advance the
// confirmed-flush LSN.
//
// # Basic usage
//
// Create a Receiver with [NewReceiver], optionally specifying parameters,
// accepted plugins, and a starting LSN via the With* option functions.
// Then iterate over the channel returned by [Receiver.Produce] to receive
// WAL messages as they arrive:
//
//	r := pglogreplsimple.NewReceiver(
//	    pglogreplsimple.WithParams(&pglogreplsimple.Param{
//	        Logger:    log.L(),
//	        ConnInfo:  "host=localhost dbname=mydb",
//	        SlotName:  "my_slot",
//	    }),
//	)
//
//	it, err := r.Produce(ctx)
//	if err != nil {
//		// handle error
//	}
//	for msg := range it {
//	    switch dat := msg.(type) {
//	    case *pglogrepl.XLogData:
//	        // process WAL data ...
//	        r.AckLSN(dat.WALStart)
//	    case *pglogrepl.PrimaryKeepaliveMessage:
//	        r.AckLSN(dat.ServerWALEnd)
//	    case *pgproto3.NoticeResponse:
//	        // process NOTICE from server
//	    }
//	}
//
// A more elaborate example can be found in the "example" directory.
//
// # Accepted plugins
//
// The Receiver only connects to slots using a logical-decoding plugin
// listed in its accepted-plugins map.  [DefaultPlugins] includes sensible
// defaults for the "wal2json" and "pgoutput" plugins.  Override the set
// with [WithAcceptedPlugins].
//
// # Reconnection and error handling
//
// On connection or receive errors the Receiver automatically retries after
// [Param.ErrorRetryInterval] (default 5 s).  If the parent context is
// canceled or [Receiver.Shutdown] is called, the Receiver stops and the
// iterator ends.  The terminal error can be retrieved with [Receiver.Err].
//
// # Reloading parameters at runtime
//
// Parameters can be changed without restarting the process.  Call
// [Receiver.RequestReload] with a new [Param]; the updated values are
// applied at the next loop iteration.  Changing ConnInfo or SlotName
// triggers a reconnection.
//
// # Acknowledging WAL positions
//
// The Receiver sends standby status updates to the server at regular
// intervals (see [Param.FeedbackInterval], default 10 s).  The confirmed-
// flush LSN in those updates determines how much WAL the server retains.
// Use [Receiver.AckLSN] from within the Produce loop to advance the write,
// flush, and replay positions reported to the server.  Positions are
// monotonic and never move backwards.
package pglogreplsimple

import (
	"time"
	"iter"
	"errors"
	"context"

	"sync"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

// MsgItem is a type that is either a *pglogrepl.PrimaryKeepaliveMessage,
// a *pglogrepl.XLogData or a *pgproto3.NoticeResponse.
type MsgItem interface {}

// Logger is an interface type representing the expected logger interface
// The [github.com/tfoertsch123/log] module provides a logger with this
// interface.
type Logger interface {
	Error(string)
	Warn(string)
	Info(string)
	Debug(string)
	Debg2(string)
	Debg3(string)
	Debg4(string)
	Debg5(string)
	Errorf(string, ...interface{})
	Warnf(string,  ...interface{})
	Infof(string,  ...interface{})
	Debugf(string, ...interface{})
	Debg2f(string, ...interface{})
	Debg3f(string, ...interface{})
	Debg4f(string, ...interface{})
	Debg5f(string, ...interface{})
}

// Next represents the internal state-machine state of a Receiver.  It
// determines what the [Receiver.Produce] loop does on its next iteration.
type Next int8

// Next states.  The Receiver transitions between these as it connects,
// receives messages, pauses on error, and shuts down.
const (
	// Connect means the Receiver should (re)establish a connection to the
	// database and start replication.
	Connect Next = iota
	// Recv means the Receiver should wait for and process the next message
	// from the database.
	Recv
	// Break means the iterator's yield function returned false and the
	// Produce loop should exit.  A subsequent Produce call can resume.
	Break
	// Stop means the Receiver has shut down and the Produce loop should
	// exit without the possibility of resuming.
	Stop
)

// String returns the name of the state for logging and debugging.
func (nxt Next) String() string {
	return []string{"Connect", "Recv", "Break", "Stop"}[nxt]
}

// Param holds the configuration parameters for a Receiver.  It is supplied
// via [WithParams] when the Receiver is created and can be updated at runtime
// via [Receiver.RequestReload].
// The CloseOnActivation channel will be closed when these parameters are
// activated. This can be used for instance if the logger is changed as the
// result of a reload request to close the connected log file.
type Param struct {
	CloseOnActivation chan<- struct{}
	ConnInfo string
	SlotName string
	ErrorRetryInterval time.Duration
	FeedbackInterval time.Duration
	Logger Logger
}

type recvStatus struct {
	wpos pglogrepl.LSN
	fpos pglogrepl.LSN
	rpos pglogrepl.LSN
}

type reloadRequest struct{}

func(_ reloadRequest) Error() string {
	return "Reload requested"
}

// Receiver manages a single logical replication connection to a PostgreSQL
// database.  It connects to the server, starts replication on a named slot,
// and yields WAL messages to the caller via the [Receiver.Produce] iterator.
//
// A Receiver is created with [NewReceiver] and used by calling Produce in a
// range loop.  During iteration the caller calls [Receiver.AckLSN] to advance
// the confirmed-flush LSN.  The Receiver handles reconnection, standby
// feedback, and graceful shutdown internally.
type Receiver struct {
	producing sync.Mutex		// prevents multiple Produce() calls
	state Next
	plugin string				// the current plugin set after connect
	acceptedPlugins map[string][]string // plugins + options
	p Param						// the current set of params
	lg Logger
	startLSN pglogrepl.LSN
	conn *pgconn.PgConn
	recvStat recvStatus			// written by m2 as it writes the file
	prevStat recvStatus			// written by SendFeedback()
	nextFeedback time.Time		// when to send the next Feedback

	// will be cancelled when it's time to exit
	shutdownCtx context.Context
	shutdownTrg context.CancelCauseFunc

	// the reason for shutting down
	lastErr error

	// ReceiveMessage is called with a context derived from shutdownCtx.
	// This is the cancel function of that derived context. It is called
	// upon Reload and when the deadline exceeds.
	mu sync.Mutex
	reload_p *Param
	cancelCurrent context.CancelCauseFunc
}

// Package-level error values returned by the Receiver API or stored as a
// shutdown cause retrievable via [Receiver.Err].
var (
	// ErrNoLogger is returned when the Receiver is configured without a
	// logger.  A logger must be set before [Receiver.Produce] is called.
	ErrNoLogger = errors.New("Logger not set")

	// ErrReceiverLocked is returned by [Receiver.Produce] or
	// [Receiver.Close] when another goroutine is already running a Produce
	// iterator on the same Receiver.
	ErrReceiverLocked = errors.New("Receiver is locked by another thread")

	// ErrReceiverStopped is returned by [Receiver.Produce] or
	// [Receiver.Close] when the Receiver has already been stopped and
	// cannot be used again.
	ErrReceiverStopped = errors.New("Receiver was stopped before")

	// ErrPlugin is the shutdown cause set when the replication slot's
	// logical-decoding plugin is not in the Receiver's accepted-plugins
	// map.
	ErrPlugin = errors.New("Wrong decoding plugin")

	// ErrConfirmedFlushLSN is the shutdown cause set when the slot's
	// confirmed_flush_lsn is ahead of the requested start LSN, which
	// would mean skipping WAL data that has not yet been acknowledged.
	ErrConfirmedFlushLSN = errors.New("ConfirmedFlushLSN is in the future")
)

type rOpts struct {
	p *Param
	acceptedPlugins map[string][]string
	startLSN pglogrepl.LSN	
}
// Opt is a configuration option applied to a new Receiver.  Use the provided
// option functions (e.g. [WithParams], [WithAcceptedPlugins], [WithStartLSN])
// to construct an Opt.
type Opt func(*rOpts)
// WithParams sets the initial [Param] values for the Receiver.  The Param is
// copied and applied during the first call to [Receiver.Produce].
func WithParams(x *Param) Opt {
	return func(o *rOpts) {
		o.p = x
	}
}
// WithAcceptedPlugins sets the map of accepted logical-decoding plugin names
// to their plugin-option argument lists.  The slot's plugin must appear as a
// key in this map or the Receiver will refuse to start replication.
// By default the plugins in [DefaultPlugins] are accepted. However, this
// module does not depend on any specific plugin behavior or data format.
// So, your own plugin should work, as well.
func WithAcceptedPlugins(x map[string][]string) Opt {
	return func(o *rOpts) {
		o.acceptedPlugins = x
	}
}
// WithStartLSN sets the starting LSN used when the Receiver connects for the
// first time and no confirmed-flush LSN has been recorded yet.  If zero, the
// slot's confirmed_flush_lsn is used.
func WithStartLSN(x pglogrepl.LSN) Opt {
	return func(o *rOpts) {
		o.startLSN = x
	}
}

// DefaultPlugins is the default set of accepted logical-decoding plugins and
// their option arguments.  It supports the "wal2json" and "pgoutput" plugins.
// Pass it (or a subset) to [WithAcceptedPlugins] to override the defaults.
var DefaultPlugins = map[string][]string{
	"wal2json": []string{
		`"format-version" '2'`,
		`"include-types" 'true'`,
		`"include-xids" 'true'`,
		`"include-timestamp" 'true'`,
		`"include-lsn" 'true'`,
		`"include-pk" 'true'`,
		`"numeric-data-types-as-string" 'true'`,
	},
	"pgoutput": []string{
		`"proto_version" '1'`,
		`"messages" 'true'`,
		`"publication_names" 'all_tables'`,
	},
}

// DefaultErrorRetryInterval is the duration waited before retrying a failed
// connection attempt when no explicit interval is set.
const DefaultErrorRetryInterval = 5*time.Second

// DefaultFeedbackInterval is the default interval at which standby status
// updates (feedback) are sent to the server.
const DefaultFeedbackInterval = 10*time.Second

// NewReceiver creates a new [Receiver] configured with the given options.
// If no [WithParams] option is supplied, default values are used for the
// error-retry and feedback intervals; a logger and connection info must be
// provided via [WithParams] before calling [Receiver.Produce].
//
// Typical usage:
//
//	r := NewReceiver(
//	    WithParams(&Param{Logger: lg, ConnInfo: conninfo, SlotName: slot}),
//	    WithStartLSN(startLSN),
//	)
//	it, err := r.Produce(ctx)
//	if err != nil {
//		// handle error
//	}
//	for msg := range it {
//	    // handle msg ...
//	    r.AckLSN(lsn)
//	}
func NewReceiver(p ...Opt) *Receiver {
	opts := rOpts{
		acceptedPlugins: DefaultPlugins,
	}
	for _, o := range p {
		o(&opts)
	}

	r := &Receiver{
		p: Param{
			ErrorRetryInterval: DefaultErrorRetryInterval,
			FeedbackInterval: DefaultFeedbackInterval,
		},
		reload_p: opts.p,
		startLSN: opts.startLSN,
		acceptedPlugins: opts.acceptedPlugins,
	}

	return r
}

// Close shuts down the Receiver.  It sets the internal state to Stop and
// closes the underlying database connection if one is open.  Close must not
// be called concurrently with an active [Receiver.Produce] iterator; if the
// iterator is running, use [Receiver.Shutdown] instead.
//
// The main use of Close is as follows:
//
//	it, err := r.Produce(ctx)
//	if err != nil {
//		// handle error
//	}
//	for msg := range it {
//		// handle msg ...
//		if some condition {
//			break
//		}
//	}
//	switch r.State() {
//	case Stop:
//		// r is done and cannot be resumed
//	case Break:
//		// the for loop was exited by "break".
//		// at this point another iterator can be created using r.Produce()
//		// or the r object can be shut down using
//		r.Close()
//	}
//
// Close returns [ErrReceiverLocked] if the Receiver is currently producing,
// [ErrReceiverStopped] if it has already been stopped, or any error returned
// by closing the connection.
func (r *Receiver) Close() error {
	if !r.producing.TryLock() {
		return ErrReceiverLocked
	}
	defer r.producing.Unlock()
	if r.state == Stop {
		return ErrReceiverStopped
	}
	r.state = Stop
	if r.conn != nil {
		err := r.conn.Close(context.Background())
		r.conn = nil
		return err
	}
	return nil
}

func (r *Receiver) configure(nxt Next) Next {
	switch r.state {
	case Break, Stop:			// no change and no reload in these states
		return r.state
	}
	r.mu.Lock()
	if r.reload_p == nil {
		r.mu.Unlock()
		if r.lg == nil {
			r.lastErr = ErrNoLogger
			return Stop			// no logger is fatal
		}
		return nxt				// nothing to do
	}
	var p *Param
	p, r.reload_p = r.reload_p, nil
	r.mu.Unlock()

	if p.Logger != nil {
		r.lg = p.Logger			// logger first
	}
	if r.lg == nil {
		r.lastErr = ErrNoLogger
		return Stop				// no logger is fatal
	}

	if p.ErrorRetryInterval <= 500 * time.Millisecond {
		r.lg.Debugf("adjusting ErrorRetryInterval from %v to %v",
			p.ErrorRetryInterval, DefaultErrorRetryInterval)
		p.ErrorRetryInterval = DefaultErrorRetryInterval
	}
	if p.FeedbackInterval <= 500 * time.Millisecond {
		r.lg.Debugf("adjusting FeedbackInterval from %v to %v",
			p.FeedbackInterval, DefaultFeedbackInterval)
		p.FeedbackInterval = DefaultFeedbackInterval
	}

	if p.ConnInfo != r.p.ConnInfo {
		r.p.ConnInfo = p.ConnInfo
		nxt = Connect
	}
	if p.SlotName != r.p.SlotName {
		r.p.SlotName = p.SlotName
		nxt = Connect
	}
	r.p.ErrorRetryInterval = p.ErrorRetryInterval
	r.p.FeedbackInterval = p.FeedbackInterval
	if p.CloseOnActivation != nil {
		close(p.CloseOnActivation)
	}
	return nxt
}

func (r *Receiver) setCancelCurrent(c context.CancelCauseFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cancelCurrent = c
}

// RequestReload queues a parameter update for the Receiver.  The updated
// [Param] is applied at the top of the next [Receiver.Produce] loop iteration.
// If the connection info or slot name changed, the Receiver reconnects; other
// fields (intervals, logger) take effect immediately.  If the Receiver is
// currently blocked waiting for a message, the wait is interrupted so the
// reload is applied without delay.
// The optional [Param.CloseOnActivation] channel is closed when the parameter
// package has been activated. This can be used to close an old logfile for
// instance.
func (r *Receiver) RequestReload(p Param) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lg.Debugf("Reload requested: %#v", p)
	r.reload_p = &p
	if r.cancelCurrent != nil {
		r.cancelCurrent(reloadRequest{})
	}
}

// Produce starts the replication receive loop and returns an iterator over
// WAL messages.  Each yielded item is either
//  - a *pglogrepl.PrimaryKeepaliveMessage or
//  - a *pglogrepl.XLogData or
//  - a *pgproto3.NoticeResponse (see [MsgItem]).
//
// If the "for range" iterator style does not fit your needs, the returned
// [iter.Seq] can easily be converted into a pull-style iterator.
//
// Postgres expects to be called back from time to time. If it does not
// get this feedback message it deems the other end dead and closes the
// connection. See [wal_sender_timeout]. This limits the time one message
// must be processed in.
//
// If the "for range" loop is exited with "break", message consumption can
// be resumed by calling [Receiver.Produce] again and creating a new iterator.
// The [Receiver.State] function can be used to distinguish between this
// situation and a normal shutdown.
//
// A [Receiver] in [Break] state still holds all the resources. If you don't
// want to resume consumption, you need to [Receiver.Close] it.
//
// Produce must not be called concurrently with itself or with [Receiver.Close].
// If the Receiver is already producing, it returns [ErrReceiverLocked]; if the
// Receiver has been stopped, it returns [ErrReceiverStopped].
//
// The provided ctx controls the lifetime of the entire replication session.
// When ctx is canceled the Receiver shuts down and the iterator ends.  Passing
// nil is equivalent to context.Background().
//
// The caller should call [Receiver.AckLSN] from within the loop to advance the
// confirmed-flush LSN sent in standby status updates.
//
// [wal_sender_timeout]: https://www.postgresql.org/docs/current/runtime-config-replication.html#GUC-WAL-SENDER-TIMEOUT
func (r *Receiver) Produce(ctx context.Context) (iter.Seq[MsgItem], error) {
	if !r.producing.TryLock() {
		return nil, ErrReceiverLocked
	}
	if r.state == Stop {
		r.producing.Unlock()
		return nil, ErrReceiverStopped
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.shutdownCtx, r.shutdownTrg = context.WithCancelCause(ctx)
	ret := func(yield func(MsgItem) bool) {
		defer r.producing.Unlock()
		defer r.shutdownTrg(nil)
		r.lastErr = nil
		for {
			// old_state := r.state
			r.state = r.configure(r.state)
			// r.lg.Debugf("configure(): %v ==> %v", old_state, r.state)
			switch r.state {
			case Stop:
				if r.conn != nil {
					r.conn.Close(context.Background())
					r.conn = nil
				}
				return
			case Break:
				// This is the result of yield() returning false
				// yield() is only called when a message is to be delivered
				// to the caller. At that state we are in Recv state.
				// Just in case the caller wants to run another
				// for msg := range r.Produce() {...}
				// loop, we need to adjust the state.
				r.state = Recv
				return
			case Connect:
				r.state = r.connInit()
			case Recv:
				r.state = r.recvOne(yield)
			}
		}
	}
	return ret, nil
}

// Local Variables:
// tab-width: 4
// End:
