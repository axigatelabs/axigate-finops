package gateway

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Loop control, slice 3: stop a run, do not just flag it. A run is paused when
// it crosses a call cap or a spend cap, or when detection flags a suspected
// loop and the policy says to pause on that. A paused run's later requests are
// refused until an operator resumes it (pause and review). A global kill switch
// refuses everything. A bypass credential forces one request through a pause.
//
// The caps are cumulative per run and enforced in memory, so the bound on
// unauthorized spend is the requests already in flight when the cap trips; an
// exact bound under heavy concurrency is the durable reservation store, lifted
// later. A request with no run id cannot be capped, the same honesty as
// detection: a run is never inferred.

// ControlPolicy configures enforcement. It is active only when a cap or the
// pause-on-loop switch is set, so it is opt-in.
type ControlPolicy struct {
	MaxCallsPerRun       int
	MaxSpendUSDPerRun    float64
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
	spendUSD    float64
	maxSpendUSD float64 // caller-set per-run cap (X-AxiGate-Max-Spend); 0 = none
	paused      bool
	reason      string
	lastSeen    time.Time
}

type controller struct {
	mu     sync.Mutex
	policy ControlPolicy
	killed bool
	runs   map[string]*runState
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

// admit decides whether to forward, before the request leaves. bypass overrides
// a per-run pause but never the kill switch. An empty run is always admitted:
// a request the caller did not tag cannot be attributed to a run to cap.
func (c *controller) admit(run string, bypass bool) (bool, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.killed {
		return false, "kill switch engaged"
	}
	if run == "" {
		return true, ""
	}
	if st := c.runs[run]; st != nil && st.paused && !bypass {
		return false, st.reason
	}
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
	defer c.mu.Unlock()
	st := c.runs[run]
	if st == nil {
		st = &runState{}
		c.runs[run] = st
	}
	st.calls++
	st.spendUSD += costUSD
	st.lastSeen = c.policy.Now()
	if !st.paused {
		// The binding spend cap is the strictest of the server-wide policy and
		// the caller's own X-AxiGate-Max-Spend, ignoring the ones left off.
		spendCap := c.policy.MaxSpendUSDPerRun
		if st.maxSpendUSD > 0 && (spendCap == 0 || st.maxSpendUSD < spendCap) {
			spendCap = st.maxSpendUSD
		}
		switch {
		case c.policy.MaxCallsPerRun > 0 && st.calls >= c.policy.MaxCallsPerRun:
			st.paused, st.reason = true, fmt.Sprintf("run reached the call cap of %d", c.policy.MaxCallsPerRun)
		case spendCap > 0 && st.spendUSD >= spendCap:
			st.paused, st.reason = true, fmt.Sprintf("run reached the spend cap of $%.2f", spendCap)
		case suspectedLoop && c.policy.PauseOnSuspectedLoop:
			st.paused, st.reason = true, "suspected loop"
		}
	}
	c.evict()
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

// resumeRun clears a run's pause after review. Returns whether a paused run was
// found.
func (c *controller) resumeRun(run string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if st := c.runs[run]; st != nil && st.paused {
		st.paused, st.reason = false, ""
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
	Killed     int          `json:"killed"` // 0 or 1, so the JSON is trivially machine-read
	PausedRuns []PausedRun  `json:"paused_runs"`
	Runs       int          `json:"runs_tracked"`
	Policy     PolicyReport `json:"policy"`
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
	PauseOnSuspectedLoop bool    `json:"pause_on_suspected_loop"`
}

func (c *controller) status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := Status{Runs: len(c.runs), Policy: PolicyReport{
		MaxCallsPerRun:       c.policy.MaxCallsPerRun,
		MaxSpendUSDPerRun:    c.policy.MaxSpendUSDPerRun,
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
	sort.Slice(s.PausedRuns, func(i, j int) bool { return s.PausedRuns[i].Run < s.PausedRuns[j].Run })
	return s
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
