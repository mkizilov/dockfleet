// Package election lets several managers share one control plane while only one
// of them at a time leads it.
//
// A manager campaigns for a key in a lease-based store (etcd; see
// internal/etcd). The holder of the key is the leader. A winner then asks the
// shared database for a new epoch, a number that grows with every change of
// leader, and acts under it: the scheduler and the reconciler stamp it on every
// transaction (so a leader that has been replaced cannot write), and agents
// refuse instructions that carry an epoch older than one they have seen.
//
// The package knows nothing of etcd, PostgreSQL or the scheduler: the backend
// is an interface, the allocation of the epoch is a function, and what a leader
// does is a list of duties started when a term begins and stopped when it ends.
// It imports only the standard library.
//
// What an Elector promises, and what it does not:
//
//   - A manager believes it leads only while it has renewed its lease recently
//     by its own clock: validity is counted from before the renewal was sent, and
//     only part of the lease is trusted (see Config.Trust). Epoch answers
//     ErrNotLeader after that, even if the goroutine that renews is stuck.
//   - Whatever a manager believes, the store has the last word: a term's epoch
//     is no longer current once a newer term began, and writes made under it fail.
//   - Duties of one term have all returned before a duty of the next one starts.
package election

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Errors of the election.
var (
	// ErrNotLeader is returned by Epoch while this manager does not lead.
	ErrNotLeader = errors.New("this manager is not the leader")
	// ErrLeaseExpired is what a Backend returns when the lease it is asked to
	// renew no longer exists.
	ErrLeaseExpired = errors.New("the election lease has expired")
)

// Lease is a time-limited claim on the backend. Everything attached to it
// disappears when it expires or is revoked.
type Lease struct {
	ID  string
	TTL time.Duration
}

// Holder is who holds the leadership key.
type Holder struct {
	// ID is the instance ID of the manager that wrote the key.
	ID string
	// Revision is the backend's revision at which the key was created. It only
	// goes into diagnostics: the epoch comes from the shared database.
	Revision int64
}

// Claim is the answer to an attempt to take the key.
type Claim struct {
	// Won is true if this call created the key.
	Won bool
	// Holder is the holder of the key after the call.
	Holder Holder
}

// Backend is the lease-based store the election runs on. Implementations must
// make Claim atomic: of several managers that try at once, one wins.
type Backend interface {
	// Grant creates a lease that expires after about ttl unless it is renewed.
	// The backend may round the ttl; the lease says what it got.
	Grant(ctx context.Context, ttl time.Duration) (Lease, error)
	// KeepAlive renews the lease and returns the time it now has left, or
	// ErrLeaseExpired if it no longer exists.
	KeepAlive(ctx context.Context, l Lease) (time.Duration, error)
	// Revoke ends the lease and deletes what is attached to it. Revoking a
	// lease that is gone is not an error.
	Revoke(ctx context.Context, l Lease) error
	// Claim writes key=id attached to the lease if and only if the key does not
	// exist.
	Claim(ctx context.Context, key, id string, l Lease) (Claim, error)
	// Observe reads who holds the key, if anyone.
	Observe(ctx context.Context, key string) (Holder, bool, error)
}

// Role is what a manager does in the election.
type Role string

// The roles.
const (
	Follower Role = "follower"
	Leader   Role = "leader"
)

// Term is one period of leadership.
type Term struct {
	// Epoch is the fencing token of the term: it is larger than the epoch of
	// every earlier term.
	Epoch int64
	// Leader is the instance ID that holds the term.
	Leader string
	// Since is when the term began, by this manager's clock.
	Since time.Time
	// Previous is the leader of the epoch before this one, and PreviousEpoch
	// that epoch, as the shared database recorded them; Previous is empty for
	// the first.
	Previous      string
	PreviousEpoch int64
}

// Duty is work only the leader does. It is started when a term begins and its
// context is canceled when the term ends; it must return soon after that. It
// should build whatever state it needs afresh: a manager that leads again later
// starts a new duty, not the old one.
type Duty func(ctx context.Context, term Term)

// Change is a change of role, passed to Config.OnChange.
type Change struct {
	From, To Role
	// Term is the term that began (To is Leader) or ended (From is Leader).
	Term Term
	// Leader is the leader as known after the change: this manager if it leads,
	// otherwise the last holder seen ("" if none).
	Leader string
	// Reason says why: "elected", "resigned", "lease expired", "lease could not
	// be renewed in time", "fenced" or "shutdown".
	Reason string
	At     time.Time
}

// Status is a snapshot of the election, for the API and the metrics.
type Status struct {
	ID   string
	Role Role
	// Epoch is the epoch of the current term while leading, 0 otherwise.
	Epoch int64
	// Leader is the leader as this manager knows it: itself while it leads,
	// otherwise the last holder it saw ("" if none or unknown).
	Leader string
	// Since is when the role began.
	Since time.Time
	// ValidUntil is, while leading, the time by this manager's clock after which
	// it stops acting as leader unless it renews the lease before.
	ValidUntil  time.Time
	Transitions uint64
	// Errors counts steps that failed against the backend or the database.
	Errors    uint64
	LastError string
}

// Fence is what a duty (or a request handler) that decides things for the
// cluster needs from the election: the epoch to act under, and a way to say
// that the database showed a newer one. *Elector implements it; a nil Fence
// means a single manager, which acts under no epoch at all.
type Fence interface {
	// Epoch returns the epoch of the term this manager leads, or ErrNotLeader.
	Epoch() (int64, error)
	// Fenced reports that the epoch is no longer the current one.
	Fenced(epoch int64)
}

var _ Fence = (*Elector)(nil)

// Config configures an Elector.
type Config struct {
	Backend Backend
	// ID is this manager's instance ID; it is what the key holds.
	ID string
	// Key is the leadership key.
	Key string
	// TTL is the lease's time to live; zero means 15 seconds. The lease is
	// renewed every Interval (a third of the TTL by default), and only
	// Trust of it is counted on.
	TTL time.Duration
	// Interval is the time between steps of Run; zero means a third of the TTL.
	Interval time.Duration
	// Trust is the part of the lease a leader counts on, from before the last
	// renewal was sent; zero means 0.8. What is left covers a backend whose
	// clock runs a little slower than this manager's.
	Trust float64
	// Begin starts a term in the shared database and returns it. It is called
	// once after the key is won and before any duty runs; if it fails the key
	// is given up. revision is the backend's revision of the key.
	Begin func(ctx context.Context, revision int64) (Term, error)
	// Duties are started when a term begins.
	Duties []Duty
	// OnChange is called after every change of role, outside the lock that
	// guards the status (it must not call Step or Resign).
	OnChange func(Change)
	Logger   *slog.Logger
	// Now defaults to time.Now; tests set it.
	Now func() time.Time
}

// Elector runs one manager's part in the election.
type Elector struct {
	cfg Config

	// stepMu serializes Step, Resign and shutdown, which change the lease and
	// the role; mu guards the fields below, which Epoch and Status read.
	stepMu sync.Mutex
	mu     sync.Mutex

	role        Role
	since       time.Time
	term        Term
	validUntil  time.Time
	leader      string
	transitions uint64
	errs        uint64
	lastErr     string
	fenced      bool
	holdOff     time.Time

	lease Lease // the lease this manager holds, if lease.ID != ""

	cancelDuties context.CancelFunc
	duties       sync.WaitGroup

	wake chan struct{}
}

// New returns an Elector that follows until it wins.
func New(cfg Config) *Elector {
	if cfg.TTL <= 0 {
		cfg.TTL = 15 * time.Second
	}
	if cfg.Interval <= 0 {
		cfg.Interval = cfg.TTL / 3
	}
	if cfg.Trust <= 0 || cfg.Trust > 1 {
		cfg.Trust = 0.8
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(discard{}, nil))
	}
	return &Elector{cfg: cfg, role: Follower, since: cfg.Now(), wake: make(chan struct{}, 1)}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// Epoch is the epoch of the term this manager leads, or ErrNotLeader. Besides
// being the leader it requires the lease to be fresh by this manager's own
// clock, so a manager that has stalled stops claiming leadership by itself.
func (e *Elector) Epoch() (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.role != Leader || !e.cfg.Now().Before(e.validUntil) {
		return 0, ErrNotLeader
	}
	return e.term.Epoch, nil
}

// Fenced tells the election that the shared database shows a newer epoch than
// epoch, so the term that holds it is over. The next step gives up the lease
// and the leadership. It is safe to call from a duty.
func (e *Elector) Fenced(epoch int64) {
	e.mu.Lock()
	if e.role == Leader && e.term.Epoch == epoch {
		e.fenced = true
	}
	e.mu.Unlock()
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// Status returns a snapshot.
func (e *Elector) Status() Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := Status{
		ID: e.cfg.ID, Role: e.role, Leader: e.leader, Since: e.since,
		Transitions: e.transitions, Errors: e.errs, LastError: e.lastErr,
	}
	if e.role == Leader {
		s.Epoch, s.Leader, s.ValidUntil = e.term.Epoch, e.cfg.ID, e.validUntil
	}
	return s
}

// Run campaigns and keeps the lease until ctx ends, then steps down and gives
// the key up so another manager can take over at once.
func (e *Elector) Run(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			e.shutdown()
			return
		case <-timer.C:
		case <-e.wake:
		}
		e.Step(ctx)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(e.cfg.Interval)
	}
}

// Step makes one move: a leader renews its lease, anyone else tries to take the
// key. Run calls it every Interval; tests call it themselves.
func (e *Elector) Step(ctx context.Context) {
	e.stepMu.Lock()
	defer e.stepMu.Unlock()

	if e.takeFenced() {
		e.stepDown(ctx, "fenced", true, e.cfg.TTL)
		return
	}
	if e.Status().Role == Leader {
		e.renew(ctx)
		return
	}
	e.campaign(ctx)
}

// Resign gives up leadership, if held, and stays out of the next election for
// one TTL so that another manager wins it. It does nothing for a follower.
func (e *Elector) Resign(ctx context.Context) {
	e.stepMu.Lock()
	defer e.stepMu.Unlock()
	if e.Status().Role == Leader {
		e.stepDown(ctx, "resigned", true, e.cfg.TTL)
	}
}

func (e *Elector) takeFenced() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	f := e.fenced
	e.fenced = false
	return f && e.role == Leader
}

// shutdown steps down and releases the lease, within a bound of its own because
// the context of Run is already done.
func (e *Elector) shutdown() {
	e.stepMu.Lock()
	defer e.stepMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if e.Status().Role == Leader {
		e.stepDown(ctx, "shutdown", true, 0)
	} else {
		e.revokeLease(ctx)
	}
}

func (e *Elector) recordError(what string, err error) {
	e.mu.Lock()
	e.errs++
	e.lastErr = what + ": " + err.Error()
	e.mu.Unlock()
	e.cfg.Logger.Warn("election step failed", "step", what, "error", err)
}

func (e *Elector) clearError() {
	e.mu.Lock()
	e.lastErr = ""
	e.mu.Unlock()
}

// campaign is a follower's step: keep a lease, try to take the key.
func (e *Elector) campaign(ctx context.Context) {
	e.mu.Lock()
	hold := e.holdOff
	e.mu.Unlock()
	if now := e.cfg.Now(); now.Before(hold) {
		// Out of the race, but still watching who wins it.
		if h, ok, err := e.cfg.Backend.Observe(ctx, e.cfg.Key); err == nil {
			e.mu.Lock()
			e.leader = ""
			if ok {
				e.leader = h.ID
			}
			e.mu.Unlock()
		}
		return
	}

	if e.lease.ID != "" {
		if _, err := e.cfg.Backend.KeepAlive(ctx, e.lease); err != nil {
			if !errors.Is(err, ErrLeaseExpired) {
				e.recordError("renew the lease while following", err)
				return
			}
			e.lease = Lease{}
		}
	}
	if e.lease.ID == "" {
		l, err := e.cfg.Backend.Grant(ctx, e.cfg.TTL)
		if err != nil {
			e.recordError("grant a lease", err)
			return
		}
		e.lease = l
	}

	claim, err := e.cfg.Backend.Claim(ctx, e.cfg.Key, e.cfg.ID, e.lease)
	if err != nil {
		e.recordError("claim the leadership key", err)
		return
	}
	e.clearError()
	if !claim.Won {
		e.mu.Lock()
		e.leader = claim.Holder.ID
		e.mu.Unlock()
		return
	}

	// The key is ours. Start the term in the shared database before anything
	// else; without an epoch there is nothing to lead with.
	beginCtx, cancel := context.WithTimeout(ctx, e.cfg.TTL/2)
	term, err := e.cfg.Begin(beginCtx, claim.Holder.Revision)
	cancel()
	if err != nil {
		e.recordError("begin a term", err)
		e.revokeLease(ctx)
		return
	}
	// The lease must still be alive after the epoch exists: that is what orders
	// this term after every earlier one (a manager that held the key before this
	// one cannot have begun its term later).
	sent := e.cfg.Now()
	left, err := e.cfg.Backend.KeepAlive(ctx, e.lease)
	if err != nil {
		e.recordError("confirm the lease after beginning the term", err)
		e.lease = Lease{}
		return
	}
	e.promote(ctx, term, e.validity(sent, left))
}

// validity is how long after sent a lease with left remaining is trusted.
func (e *Elector) validity(sent time.Time, left time.Duration) time.Time {
	if left <= 0 {
		left = e.lease.TTL
	}
	return sent.Add(time.Duration(float64(left) * e.cfg.Trust))
}

// renew is a leader's step: renew the lease and extend the trust in it.
func (e *Elector) renew(ctx context.Context) {
	sent := e.cfg.Now()
	left, err := e.cfg.Backend.KeepAlive(ctx, e.lease)
	switch {
	case err == nil:
		e.clearError()
		e.mu.Lock()
		e.validUntil = e.validity(sent, left)
		e.mu.Unlock()
	case errors.Is(err, ErrLeaseExpired):
		e.lease = Lease{}
		e.stepDown(ctx, "lease expired", false, 0)
	default:
		e.recordError("renew the lease", err)
		e.mu.Lock()
		expired := !e.cfg.Now().Before(e.validUntil)
		e.mu.Unlock()
		if expired {
			e.stepDown(ctx, "lease could not be renewed in time", true, 0)
		}
	}
}

func (e *Elector) promote(ctx context.Context, term Term, validUntil time.Time) {
	now := e.cfg.Now()
	term.Leader, term.Since = e.cfg.ID, now
	termCtx, cancel := context.WithCancel(ctx)

	e.mu.Lock()
	e.role, e.since, e.term, e.validUntil, e.leader = Leader, now, term, validUntil, e.cfg.ID
	e.cancelDuties = cancel
	e.transitions++
	e.fenced = false
	e.mu.Unlock()

	for _, duty := range e.cfg.Duties {
		e.duties.Add(1)
		go func() {
			defer e.duties.Done()
			duty(termCtx, term)
		}()
	}
	e.cfg.Logger.Info("this manager is now the leader", "epoch", term.Epoch, "previous_leader", term.Previous, "previous_epoch", term.PreviousEpoch)
	e.notify(Change{From: Follower, To: Leader, Term: term, Leader: e.cfg.ID, Reason: "elected", At: now})
}

// stepDown ends the term: the duties are stopped and have returned before this
// does, so the next term never overlaps them. release gives the lease up too;
// holdOff keeps this manager out of the next election for that long.
func (e *Elector) stepDown(ctx context.Context, reason string, release bool, holdOff time.Duration) {
	now := e.cfg.Now()
	e.mu.Lock()
	term := e.term
	cancel := e.cancelDuties
	e.role, e.since, e.validUntil, e.leader = Follower, now, time.Time{}, ""
	e.cancelDuties = nil
	e.transitions++
	e.fenced = false
	if holdOff > 0 {
		e.holdOff = now.Add(holdOff)
	}
	e.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	e.duties.Wait()
	if release {
		e.revokeLease(ctx)
	}
	e.cfg.Logger.Warn("this manager is no longer the leader", "epoch", term.Epoch, "reason", reason)
	e.notify(Change{From: Leader, To: Follower, Term: term, Reason: reason, At: now})
}

func (e *Elector) revokeLease(ctx context.Context) {
	if e.lease.ID == "" {
		return
	}
	if err := e.cfg.Backend.Revoke(ctx, e.lease); err != nil {
		// The lease expires by itself; nothing else can be done here.
		e.cfg.Logger.Warn("could not revoke the election lease; it will expire", "error", err)
	}
	e.lease = Lease{}
}

func (e *Elector) notify(c Change) {
	if e.cfg.OnChange != nil {
		e.cfg.OnChange(c)
	}
}

// String names the election in logs.
func (e *Elector) String() string {
	s := e.Status()
	return fmt.Sprintf("election{%s %s epoch %d}", s.ID, s.Role, s.Epoch)
}
