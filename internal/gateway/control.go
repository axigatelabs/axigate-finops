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
	// MaxSpendUSDPerKeyDay and MaxSpendUSDPerKey cap what one API key may
	// spend in a UTC day and in total, whatever runs it starts — the leaked-key
	// case. A key at its daily cap is refused until midnight UTC; at its total
	// cap until an operator resumes it. Reserved at admit like a run.
	MaxSpendUSDPerKeyDay float64
	MaxSpendUSDPerKey    float64
	// Shadow serves every call but marks the ones a cap would have refused and
	// sends the alert as "would have stopped" — the way to watch a week before
	// enforcing. The tallies, pauses and resumes work exactly as they would;
	// only the refusal is withheld. The kill switch still refuses.
	Shadow bool
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

// shadowNote is what a status reply says about its paused lists in shadow
// mode, where nothing is refused.
func shadowNote(p ControlPolicy, killed bool) string {
	switch {
	case !p.Shadow:
		return ""
	case killed:
		return "shadow mode: paused_runs and paused_keys are marked, not refused; the kill switch is engaged, so every call is refused until it is cleared"
	}
	return "shadow mode: paused_runs and paused_keys are marked, not refused; every call is served"
}

// keyCaps reports whether any per-key ceiling is set; without one the
// counters never track a key, so a store deployed for run caps alone gains
// nothing on upgrade.
func (p ControlPolicy) keyCaps() bool { return p.MaxSpendUSDPerKeyDay > 0 || p.MaxSpendUSDPerKey > 0 }

func (p ControlPolicy) active() bool {
	return p.MaxCallsPerRun > 0 || p.MaxSpendUSDPerRun > 0 || p.PauseOnSuspectedLoop || p.AllowRequestCaps || p.MaxSpendUSDPerKeyDay > 0 || p.MaxSpendUSDPerKey > 0
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

// StopEvent is what the gateway reports the moment it stops a run or a key:
// which one, whose it was, why, and what it had spent so far. Metadata only.
// A key stop has Key set and Run empty.
type StopEvent struct {
	Run      string    `json:"run"`
	Key      string    `json:"key,omitempty"`
	Agent    string    `json:"agent"`
	Team     string    `json:"team"`
	Model    string    `json:"model"`
	Reason   string    `json:"reason"`
	Calls    int       `json:"calls"`
	SpendUSD float64   `json:"spend_usd"`
	At       time.Time `json:"at"`
	Shadow   bool      `json:"shadow,omitempty"` // shadow mode: nothing was refused, the run or key is only marked
	dayPause bool      // a key stop made by the daily cap: Calls and SpendUSD are today's
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

// keyState is one API key's tally: today's and total spend, with the same
// reserve-at-admit in-flight count a run has. A pause made by the daily cap
// clears itself when the UTC day changes.
type keyState struct {
	day                string
	dayCalls           int
	daySpend           float64
	lifeCalls          int
	lifeSpend          float64
	inflight           int
	paused             bool
	reason             string
	dayPause           bool
	lastSeen           time.Time
	agent, team, model string
}

type controller struct {
	mu     sync.Mutex
	policy ControlPolicy
	killed bool
	runs   map[string]*runState
	keys   map[string]*keyState
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
	return &controller{policy: p, runs: map[string]*runState{}, keys: map[string]*keyState{}}
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

// noteKey records a key's attribution (the last call's agent/team/model), so
// a key stop can say whose traffic it was.
func (c *controller) noteKey(key, agent, team, model string) {
	if key == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ks := c.keyLocked(key)
	if agent != "" {
		ks.agent = agent
	}
	if team != "" {
		ks.team = team
	}
	if model != "" {
		ks.model = model
	}
	c.evictKeys()
}

// keyDay is the UTC day the key tallies belong to.
func (c *controller) keyDay() string { return c.policy.Now().UTC().Format("2006-01-02") }

// keyLocked returns a key's state, rolling its daily tally (and a pause the
// daily cap made) over when the UTC day has changed. The caller holds c.mu.
func (c *controller) keyLocked(key string) *keyState {
	ks := c.keys[key]
	if ks == nil {
		ks = &keyState{day: c.keyDay()}
		c.keys[key] = ks
	}
	if today := c.keyDay(); today > ks.day { // the day only moves forward; a lagging clock never resets a tally
		ks.day, ks.dayCalls, ks.daySpend, ks.inflight = today, 0, 0, 0
		if ks.dayPause {
			ks.paused, ks.reason, ks.dayPause = false, "", false
		}
	}
	ks.lastSeen = c.policy.Now()
	return ks
}

// pauseKeyLocked marks a key paused and queues its stop alert once.
func (c *controller) pauseKeyLocked(key string, ks *keyState, reason string, dayPause bool) {
	ks.paused, ks.reason, ks.dayPause = true, reason, dayPause
	if c.onPause != nil || c.onPauseSync != nil {
		calls, spend := ks.lifeCalls, ks.lifeSpend
		if dayPause {
			calls, spend = ks.dayCalls, ks.daySpend
		}
		c.pending = append(c.pending, StopEvent{
			Key: key, Agent: ks.agent, Team: ks.team, Model: ks.model,
			Reason: reason, Calls: calls, SpendUSD: spend, At: c.policy.Now().UTC(), Shadow: c.policy.Shadow, dayPause: dayPause,
		})
	}
}

// keyCapLocked checks a key's ceilings with the same reserve-at-admit rule a
// run uses; it pauses the key and returns the reason when one is met. The
// caller holds c.mu.
func (c *controller) keyCapLocked(key string, ks *keyState) string {
	per := c.policy.ReserveUSDPerCall
	if ks.lifeCalls > 0 {
		if avg := ks.lifeSpend / float64(ks.lifeCalls); avg > per {
			per = avg
		}
	}
	// The total cap is checked first: it is the one that needs a person, and a
	// key past it must not be merely day-paused into resuming at midnight.
	if cap := c.policy.MaxSpendUSDPerKey; cap > 0 && micro(ks.lifeSpend+per*float64(ks.inflight)) >= micro(cap) {
		c.pauseKeyLocked(key, ks, keyLabel(key)+" reached its spend cap of "+money(cap), false)
		return ks.reason
	}
	if cap := c.policy.MaxSpendUSDPerKeyDay; cap > 0 && micro(ks.daySpend+per*float64(ks.inflight)) >= micro(cap) {
		c.pauseKeyLocked(key, ks, keyLabel(key)+" reached its daily spend cap of "+money(cap)+" (resets at midnight UTC)", true)
		return ks.reason
	}
	return ""
}

// pauseLocked marks a run paused with a reason and dispatches the stop alert
// exactly once per transition (every caller checks !paused first). The caller
// holds c.mu; the alert is handed to its own goroutine so nothing here blocks.
func (c *controller) pauseLocked(run string, st *runState, reason string) {
	st.paused, st.reason = true, reason
	if c.onPause != nil || c.onPauseSync != nil {
		c.pending = append(c.pending, StopEvent{
			Run: run, Agent: st.agent, Team: st.team, Model: st.model,
			Reason: reason, Calls: st.calls, SpendUSD: st.spendUSD, At: c.policy.Now().UTC(), Shadow: c.policy.Shadow,
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
	return c.admitKeyed(run, "", bypass)
}

// admitKeyed is admit with the call's API key: the key's ceilings are checked
// after the run's pause and before the run's caps, and an admitted call is
// reserved against both.
func (c *controller) admitKeyed(run, key string, bypass bool) (bool, string) {
	c.mu.Lock()
	defer c.dispatch()
	defer c.mu.Unlock()
	if c.killed {
		return false, "kill switch engaged"
	}
	var ks *keyState
	if key != "" {
		ks = c.keyLocked(key)
	}
	if run == "" && ks == nil {
		return true, ""
	}
	var st *runState
	if run != "" {
		st = c.runs[run]
		if st == nil {
			st = &runState{}
			c.runs[run] = st
		}
		st.lastSeen = c.policy.Now()
	}
	// A served call is reserved on its run and its key, whichever it has. In
	// shadow mode a call a cap would refuse is served (and reserved) all the
	// same, and the reason travels back with it so the row can say so.
	shadow := c.policy.Shadow && !bypass
	reserve := func() {
		if st != nil {
			st.inflight++
		}
		if ks != nil {
			ks.inflight++
			c.evictKeys()
		}
	}
	if st != nil && st.paused {
		if bypass || shadow { // a forced-through call is still in flight and will be recorded
			reserve()
			if shadow {
				return true, st.reason
			}
			return true, ""
		}
		return false, st.reason
	}
	if ks != nil && ks.paused {
		if bypass || shadow {
			reserve()
			if shadow {
				return true, ks.reason
			}
			return true, ""
		}
		return false, ks.reason
	}
	if ks != nil && !bypass {
		if reason := c.keyCapLocked(key, ks); reason != "" {
			if shadow {
				reserve()
				return true, reason
			}
			return false, reason
		}
	}
	if st == nil {
		reserve()
		return true, ""
	}
	if !bypass {
		// Reserve-at-admit: refuse (and pause) if the run's calls or projected
		// spend, counting those already in flight, reach a cap — so a burst of
		// concurrent calls for one run trips the cap at the boundary.
		if c.policy.MaxCallsPerRun > 0 && st.calls+st.inflight >= c.policy.MaxCallsPerRun {
			c.pauseLocked(run, st, fmt.Sprintf("run reached the call cap of %d", c.policy.MaxCallsPerRun))
			if shadow {
				reserve()
				return true, st.reason
			}
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
				if shadow {
					reserve()
					return true, st.reason
				}
				return false, st.reason
			}
		}
	}
	reserve()
	return true, ""
}

// record tallies a completed request and pauses the run if it crossed a cap or
// tripped the loop signal. The request that crosses a cap has already been
// served; the next one for that run is what gets refused.
func (c *controller) record(run string, costUSD float64, suspectedLoop bool) {
	c.recordKeyed(run, "", costUSD, suspectedLoop)
}

// recordKeyed settles a served call against its run and its key.
func (c *controller) recordKeyed(run, key string, costUSD float64, suspectedLoop bool) {
	if run == "" && key == "" {
		return
	}
	c.mu.Lock()
	defer c.dispatch()
	defer c.mu.Unlock()
	if key != "" {
		ks := c.keyLocked(key)
		if ks.inflight > 0 {
			ks.inflight--
		}
		ks.dayCalls++
		ks.lifeCalls++
		ks.daySpend += costUSD
		ks.lifeSpend += costUSD
		if !ks.paused { // settled spend only; reservations are admit's business
			switch {
			case c.policy.MaxSpendUSDPerKey > 0 && micro(ks.lifeSpend) >= micro(c.policy.MaxSpendUSDPerKey):
				c.pauseKeyLocked(key, ks, keyLabel(key)+" reached its spend cap of "+money(c.policy.MaxSpendUSDPerKey), false)
			case c.policy.MaxSpendUSDPerKeyDay > 0 && micro(ks.daySpend) >= micro(c.policy.MaxSpendUSDPerKeyDay):
				c.pauseKeyLocked(key, ks, keyLabel(key)+" reached its daily spend cap of "+money(c.policy.MaxSpendUSDPerKeyDay)+" (resets at midnight UTC)", true)
			}
		}
		c.evictKeys()
	}
	if run == "" {
		return
	}
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
func (c *controller) release(run string) { c.releaseKeyed(run, "") }

// releaseKeyed returns a never-served call's reservation on its run and its key.
func (c *controller) releaseKeyed(run, key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if key != "" {
		if ks := c.keys[key]; ks != nil && ks.inflight > 0 {
			ks.inflight--
		}
	}
	if st := c.runs[run]; run != "" && st != nil && st.inflight > 0 {
		st.inflight--
	}
}

// resumeKey clears a key's pause and grants it a fresh budget for the cap
// that paused it: today's after a daily pause, today's and total after a
// total pause (a routine "let it run today" must not wipe the total the
// operator set). Returns whether a paused key was found.
func (c *controller) resumeKey(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ks := c.keys[key]
	if ks == nil || !ks.paused {
		return false
	}
	if !ks.dayPause {
		ks.lifeCalls, ks.lifeSpend = 0, 0
	}
	ks.paused, ks.reason, ks.dayPause = false, "", false
	ks.dayCalls, ks.daySpend, ks.inflight = 0, 0, 0
	return true
}

// markKeyPaused records a key pause made elsewhere (the shared store) without
// alerting again; a daily one still clears at midnight.
func (c *controller) markKeyPaused(key, reason string, dayPause bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ks := c.keyLocked(key)
	ks.paused, ks.reason, ks.dayPause = true, reason, dayPause
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
	Killed int `json:"killed"` // 0 or 1, so the JSON is trivially machine-read
	// Note is set in shadow mode: the paused lists are the runs and keys a cap
	// has marked, and every one of their calls is still being served.
	Note       string         `json:"note,omitempty"`
	PausedRuns []PausedRun    `json:"paused_runs"`
	PausedKeys []PausedKey    `json:"paused_keys,omitempty"`
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

// PausedKey reports one paused key by its fingerprint or name, and why. The
// reason says which cap paused it; today's figures and the total are named
// as such so nobody reads today's calls as the key's whole history.
type PausedKey struct {
	Key           string  `json:"key"`
	Reason        string  `json:"reason"`
	CallsToday    int     `json:"calls_today"`
	SpendUSDToday float64 `json:"spend_usd_today"`
	TotalUSD      float64 `json:"total_usd"` // all-time spend this counter has seen
}

// PolicyReport echoes the active thresholds.
type PolicyReport struct {
	MaxCallsPerRun       int     `json:"max_calls_per_run"`
	MaxSpendUSDPerRun    float64 `json:"max_spend_usd_per_run"`
	ReserveUSDPerCall    float64 `json:"reserve_usd_per_call,omitempty"`
	PauseOnSuspectedLoop bool    `json:"pause_on_suspected_loop"`
	MaxSpendUSDPerKeyDay float64 `json:"max_spend_usd_per_key_day,omitempty"`
	MaxSpendUSDPerKey    float64 `json:"max_spend_usd_per_key,omitempty"`
	Shadow               bool    `json:"shadow,omitempty"` // caps mark and alert, never refuse
}

func (c *controller) status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := Status{Runs: len(c.runs), Note: shadowNote(c.policy, c.killed), Policy: PolicyReport{
		MaxCallsPerRun:       c.policy.MaxCallsPerRun,
		MaxSpendUSDPerRun:    c.policy.MaxSpendUSDPerRun,
		ReserveUSDPerCall:    c.policy.ReserveUSDPerCall,
		PauseOnSuspectedLoop: c.policy.PauseOnSuspectedLoop,
		MaxSpendUSDPerKeyDay: c.policy.MaxSpendUSDPerKeyDay,
		MaxSpendUSDPerKey:    c.policy.MaxSpendUSDPerKey,
		Shadow:               c.policy.Shadow,
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
	today := c.keyDay()
	for id, ks := range c.keys {
		if ks.paused && !(ks.dayPause && today > ks.day) { // a daily pause from an earlier day is already over
			s.PausedKeys = append(s.PausedKeys, PausedKey{Key: id, Reason: ks.reason, CallsToday: ks.dayCalls, SpendUSDToday: ks.daySpend, TotalUSD: ks.lifeSpend})
		}
	}
	sort.Slice(s.PausedKeys, func(i, j int) bool { return s.PausedKeys[i].Key < s.PausedKeys[j].Key })
	return s
}

// evictKeys bounds the key map the way evict bounds runs: paused keys are
// kept, active ones drop least-recently-seen first. The caller holds c.mu.
func (c *controller) evictKeys() {
	if len(c.keys) <= c.policy.MaxRuns {
		return
	}
	type age struct {
		id string
		t  time.Time
	}
	var ages []age
	for id, ks := range c.keys {
		if !ks.paused {
			ages = append(ages, age{id, ks.lastSeen})
		}
	}
	sort.Slice(ages, func(i, j int) bool { return ages[i].t.Before(ages[j].t) })
	over := len(c.keys) - c.policy.MaxRuns
	for i := 0; i < over && i < len(ages); i++ {
		delete(c.keys, ages[i].id)
	}
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
