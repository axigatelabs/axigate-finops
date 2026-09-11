package gateway

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Loop control, slice 3: stop a run, do not just flag it. A run is paused when
// it crosses a call cap or a spend cap, or when detection flags a suspected
// loop and the policy says to pause on that. A paused run's later requests are
// refused until an operator resumes it (pause and review). A global kill switch
// refuses everything. A bypass credential forces one request through a pause.
//
// The caps are cumulative per run and enforced in memory. A call is reserved at
// admit — counted against the run before it is served — so a burst of concurrent
// calls for one run trips the cap at the boundary instead of after ~concurrency
// of overshoot. Each in-flight call is reserved at the run's average cost so
// far, or at ReserveUSDPerCall when that is higher — so a burst that hits a
// cold run (no cost recorded yet, hence no average) is still bounded when the
// operator sets a floor. Without a floor the residual bound is the calls
// already in flight before the run's first cost is recorded. An exact bound
// ACROSS processes is --shared-counter (counter_redis.go): the same rules run
// as one script in a shared store, part of this binary. A request with no run
// id cannot be capped, the same honesty as detection: a run is never inferred.

// ControlPolicy configures enforcement. It is active only when a cap or the
// pause-on-loop switch is set, so it is opt-in.
type ControlPolicy struct {
	MaxCallsPerRun    int
	MaxSpendUSDPerRun float64
	// ReserveUSDPerCall is the least a call is assumed to cost while it is in
	// flight, for the spend-cap check. A run's first burst has no recorded cost
	// to average, so without a floor every call in that burst is admitted; with
	// one, the burst is refused once floor × in-flight would reach the cap. It
	// never changes what is recorded — only what is reserved.
	ReserveUSDPerCall    float64
	PauseOnSuspectedLoop bool
	// AdminToken guards the bypass header and the admin endpoints. Empty means
	// no bypass and no admin channel (caps still work; a paused run then needs
	// a restart to clear).
	AdminToken string
	// AllowRequestCaps honors a caller's X-AxiGate-Max-Spend header as a per-run
	// spend cap set from application code, no server flag or restart needed.
	AllowRequestCaps bool
	MaxRuns          int // cap on runs tracked; 0 = a sane default
	Now              func() time.Time
}

func (p ControlPolicy) active() bool {
	return p.MaxCallsPerRun > 0 || p.MaxSpendUSDPerRun > 0 || p.PauseOnSuspectedLoop || p.AllowRequestCaps
}

type runState struct {
	calls       int
	inflight    int // calls admitted but not yet recorded (the reserve-at-admit count)
	spendUSD    float64
	maxSpendUSD float64 // caller-set per-run cap (X-AxiGate-Max-Spend); 0 = none
	paused      bool
	reason      string
	lastSeen    time.Time
	// Attribution noted from the run's requests, so a stop alert can say WHO was
	// stopped, not just which run id. Metadata only — never prompt text.
	agent, team, model string
}

// StopEvent is what the gateway reports the moment it stops a run: which run,
// whose it was, why, and what it had spent so far. Metadata only.
type StopEvent struct {
	Run      string    `json:"run"`
	Agent    string    `json:"agent"`
	Team     string    `json:"team"`
	Model    string    `json:"model"`
	Reason   string    `json:"reason"`
	Calls    int       `json:"calls"`
	SpendUSD float64   `json:"spend_usd"`
	At       time.Time `json:"at"`
}

// micro is a dollar figure in whole micro-dollars. Cap comparisons happen at
// this precision, in memory and in the shared store alike, so 0.7 + 0.1 meets
// a $0.80 cap on every machine instead of depending on float rounding.
func micro(v float64) int64 { return int64(math.Round(v * 1e6)) }

// spendCapLocked returns the binding spend cap: the strictest of the server-wide
// policy and the caller's inline X-AxiGate-Max-Spend, ignoring the ones left off.
// The caller must hold c.mu.
// money prints a dollar figure to the cent, or to the tenth of a cent when the
// cap was set finer than that — a $0.015 cap must not read as $0.01.
func money(v float64) string {
	s := strconv.FormatFloat(v, 'f', 4, 64)
	s = strings.TrimRight(s, "0")
	if i := strings.IndexByte(s, '.'); i >= 0 && len(s)-i-1 < 2 {
		s += strings.Repeat("0", 2-(len(s)-i-1))
	}
	return "$" + s
}

func (c *controller) spendCapLocked(st *runState) float64 {
	cap := c.policy.MaxSpendUSDPerRun
	if st.maxSpendUSD > 0 && (cap == 0 || st.maxSpendUSD < cap) {
		cap = st.maxSpendUSD
	}
	return cap
}

type controller struct {
	mu     sync.Mutex
	policy ControlPolicy
	killed bool
	runs   map[string]*runState
	// onPause, when set, receives one StopEvent per run-stop transition. It is
	// invoked on its own goroutine, never under the lock and never in a request's
	// path, so a slow or failing alert sink cannot affect a call.
	onPause func(StopEvent)
	// onPauseSync, when set, receives the same event synchronously, after the
	// lock is released and before the call that caused it returns — for
	// bookkeeping that must be in order with what the caller does next.
	onPauseSync func(StopEvent)
	pending     []StopEvent // stop events waiting for dispatch after the lock
}

func newController(p ControlPolicy) *controller {
	if p.Now == nil {
		p.Now = time.Now
	}
	if p.MaxRuns <= 0 {
		p.MaxRuns = 10000
	}
	return &controller{policy: p, runs: map[string]*runState{}}
}

// noteRun records a run's attribution (from the request's X-AxiGate-* headers
// and model) so a stop alert can name who was stopped. Cheap; called per request.
func (c *controller) noteRun(run, agent, team, model string) {
	if run == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.runs[run]
	if st == nil {
		st = &runState{}
		c.runs[run] = st
	}
	if agent != "" {
		st.agent = agent
	}
	if team != "" {
		st.team = team
	}
	if model != "" {
		st.model = model
	}
}

// pauseLocked marks a run paused with a reason and dispatches the stop alert
// exactly once per transition (every caller checks !paused first). The caller
// holds c.mu; the alert is handed to its own goroutine so nothing here blocks.
func (c *controller) pauseLocked(run string, st *runState, reason string) {
	st.paused, st.reason = true, reason
	if c.onPause != nil || c.onPauseSync != nil {
		c.pending = append(c.pending, StopEvent{
			Run: run, Agent: st.agent, Team: st.team, Model: st.model,
			Reason: reason, Calls: st.calls, SpendUSD: st.spendUSD, At: c.policy.Now().UTC(),
		})
	}
}

// dispatch hands out the stop events a call produced, once the lock is
// released: the synchronous hook first, in order, then the alert on its own
// goroutine. Deferred after the unlock in admit and record.
func (c *controller) dispatch() {
	c.mu.Lock()
	evs, sync, async := c.pending, c.onPauseSync, c.onPause
	c.pending = nil
	c.mu.Unlock()
	for _, ev := range evs {
		if sync != nil {
			sync(ev)
		}
		if async != nil {
			go async(ev)
		}
	}
}

// admit decides whether to forward, before the request leaves, and reserves the
// call against its run so concurrent calls see the growing total. bypass
// overrides a per-run pause but never the kill switch. An empty run is always
// admitted: a request the caller did not tag cannot be attributed to a run to
// cap. Every admitted run-tagged call must be balanced by exactly one record
// (served) or release (errored before serving), so the in-flight count stays
// honest.
func (c *controller) admit(run string, bypass bool) (bool, string) {
	c.mu.Lock()
	defer c.dispatch()
	defer c.mu.Unlock()
	if c.killed {
		return false, "kill switch engaged"
	}
	if run == "" {
		return true, ""
	}
	st := c.runs[run]
	if st == nil {
		st = &runState{}
		c.runs[run] = st
	}
	st.lastSeen = c.policy.Now()
	if st.paused {
		if bypass { // a forced-through call is still in flight and will be recorded
			st.inflight++
			return true, ""
		}
		return false, st.reason
	}
	if !bypass {
		// Reserve-at-admit: refuse (and pause) if the run's calls or projected
		// spend, counting those already in flight, reach a cap — so a burst of
		// concurrent calls for one run trips the cap at the boundary.
		if c.policy.MaxCallsPerRun > 0 && st.calls+st.inflight >= c.policy.MaxCallsPerRun {
			c.pauseLocked(run, st, fmt.Sprintf("run reached the call cap of %d", c.policy.MaxCallsPerRun))
			return false, st.reason
		}
		if cap := c.spendCapLocked(st); cap > 0 {
			// Reserve each in-flight call at the run's average so far, or at
			// the operator's floor when that is higher (a cold run has no
			// average; a cheap start would otherwise under-reserve).
			perCall := c.policy.ReserveUSDPerCall
			if st.calls > 0 {
				if avg := st.spendUSD / float64(st.calls); avg > perCall {
					perCall = avg
				}
			}
			if micro(st.spendUSD+perCall*float64(st.inflight)) >= micro(cap) {
				c.pauseLocked(run, st, "run reached the spend cap of "+money(cap))
				return false, st.reason
			}
		}
	}
	st.inflight++
	return true, ""
}

// record tallies a completed request and pauses the run if it crossed a cap or
// tripped the loop signal. The request that crosses a cap has already been
// served; the next one for that run is what gets refused.
func (c *controller) record(run string, costUSD float64, suspectedLoop bool) {
	if run == "" {
		return
	}
	c.mu.Lock()
	defer c.dispatch()
	defer c.mu.Unlock()
	st := c.runs[run]
	if st == nil {
		st = &runState{}
		c.runs[run] = st
	}
	if st.inflight > 0 { // release this call's admit-time reservation
		st.inflight--
	}
	st.calls++
	st.spendUSD += costUSD
	st.lastSeen = c.policy.Now()
	if !st.paused {
		spendCap := c.spendCapLocked(st)
		switch {
		case c.policy.MaxCallsPerRun > 0 && st.calls >= c.policy.MaxCallsPerRun:
			c.pauseLocked(run, st, fmt.Sprintf("run reached the call cap of %d", c.policy.MaxCallsPerRun))
		case spendCap > 0 && micro(st.spendUSD) >= micro(spendCap):
			c.pauseLocked(run, st, "run reached the spend cap of "+money(spendCap))
		case suspectedLoop && c.policy.PauseOnSuspectedLoop:
			c.pauseLocked(run, st, "suspected loop")
		}
	}
	c.evict()
}

// release returns an admitted call's in-flight reservation without recording a
// cost, for a call that never reached the provider (a request-build or dial
// error). It keeps the in-flight count honest so the reserve-at-admit bound does
// not drift toward over-blocking. An empty run, or a run with nothing in flight,
// is a no-op.
func (c *controller) release(run string) {
	if run == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if st := c.runs[run]; st != nil && st.inflight > 0 {
		st.inflight--
	}
}

// setRunCap records a caller-supplied per-run spend cap (the X-AxiGate-Max-Spend
// header). The strictest cap in effect is what record enforces. An empty run is
// ignored: a cap, like a loop, is never attributed to a run the caller did not
// name.
func (c *controller) setRunCap(run string, maxSpendUSD float64) {
	if run == "" || maxSpendUSD <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.runs[run]
	if st == nil {
		st = &runState{}
		c.runs[run] = st
	}
	st.maxSpendUSD = maxSpendUSD
	st.lastSeen = c.policy.Now()
	c.evict()
}

// markPaused records a pause made elsewhere (the shared store, before a gap)
// without alerting again: the fallback then refuses the run as the store did.
func (c *controller) markPaused(run, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.runs[run]
	if st == nil {
		st = &runState{}
		c.runs[run] = st
	}
	st.paused, st.reason = true, reason
}

// resumeRun clears a run's pause after review. Returns whether a paused run was
// found.
func (c *controller) resumeRun(run string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if st := c.runs[run]; st != nil && st.paused {
		// Resume grants a fresh allowance: clear the pause and the tallies that
		// tripped the cap, so the reviewed run continues under a full budget
		// instead of re-pausing on its own history at the next admit.
		st.paused, st.reason = false, ""
		st.calls, st.spendUSD, st.inflight = 0, 0, 0
		return true
	}
	return false
}

func (c *controller) setKilled(on bool) {
	c.mu.Lock()
	c.killed = on
	c.mu.Unlock()
}

// Status is a snapshot for the admin endpoint.
type Status struct {
	Killed     int            `json:"killed"` // 0 or 1, so the JSON is trivially machine-read
	PausedRuns []PausedRun    `json:"paused_runs"`
	Runs       int            `json:"runs_tracked"`
	Policy     PolicyReport   `json:"policy"`
	Counter    *CounterReport `json:"counter,omitempty"` // present when a shared store is configured
}

// PausedRun reports one paused run without leaking anything but its id and why.
type PausedRun struct {
	Run      string  `json:"run"`
	Reason   string  `json:"reason"`
	Calls    int     `json:"calls"`
	SpendUSD float64 `json:"spend_usd"`
}

// PolicyReport echoes the active thresholds.
type PolicyReport struct {
	MaxCallsPerRun       int     `json:"max_calls_per_run"`
	MaxSpendUSDPerRun    float64 `json:"max_spend_usd_per_run"`
	ReserveUSDPerCall    float64 `json:"reserve_usd_per_call,omitempty"`
	PauseOnSuspectedLoop bool    `json:"pause_on_suspected_loop"`
}

func (c *controller) status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := Status{Runs: len(c.runs), Policy: PolicyReport{
		MaxCallsPerRun:       c.policy.MaxCallsPerRun,
		MaxSpendUSDPerRun:    c.policy.MaxSpendUSDPerRun,
		ReserveUSDPerCall:    c.policy.ReserveUSDPerCall,
		PauseOnSuspectedLoop: c.policy.PauseOnSuspectedLoop,
	}}
	if c.killed {
		s.Killed = 1
	}
	for id, st := range c.runs {
		if st.paused {
			s.PausedRuns = append(s.PausedRuns, PausedRun{Run: id, Reason: st.reason, Calls: st.calls, SpendUSD: st.spendUSD})
		}
	}
	sortPaused(s.PausedRuns)
	return s
}

func sortPaused(p []PausedRun) {
	sort.Slice(p, func(i, j int) bool { return p[i].Run < p[j].Run })
}

// evict bounds the map. Paused runs are kept (they carry the reason an operator
// needs); only active runs are dropped, least-recently-seen first.
func (c *controller) evict() {
	if len(c.runs) <= c.policy.MaxRuns {
		return
	}
	type age struct {
		id string
		t  time.Time
	}
	var ages []age
	for id, st := range c.runs {
		if !st.paused {
			ages = append(ages, age{id, st.lastSeen})
		}
	}
	sort.Slice(ages, func(i, j int) bool { return ages[i].t.Before(ages[j].t) })
	over := len(c.runs) - c.policy.MaxRuns
	for i := 0; i < over && i < len(ages); i++ {
		delete(c.runs, ages[i].id)
	}
}
