package gateway

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// Loop detection, slice 2: report only. A runaway agent shows up as one run
// making many calls in a short window, or the same call repeated. Both are
// read from the caller's explicit X-AxiGate-Run id; a loop is never inferred
// for a request that carries no run id, because there is nothing to tie it to.
// This slice annotates the event and logs a warning; it does not block. The
// per-run cap and kill switch build on this decision next.

// LoopPolicy configures detection. Detection is active only when at least one
// of MaxPerRun or MaxRepeat is positive, so it is opt-in.
type LoopPolicy struct {
	Window    time.Duration // how far back a run's requests are counted
	MaxPerRun int           // requests within Window in one run that flag a loop (rate)
	MaxRepeat int           // identical requests within Window in one run that flag a loop
	MaxRuns   int           // cap on runs tracked at once; 0 = a sane default
	Now       func() time.Time
}

// active reports whether the policy asks for any detection.
func (p LoopPolicy) active() bool { return p.MaxPerRun > 0 || p.MaxRepeat > 0 }

// Decision is what the detector concluded for one request.
type Decision struct {
	Suspected bool
	Signal    string // "rate", "repeat", or "rate+repeat"
	Count     int    // requests in the window for this run, including this one
	Repeat    int    // the largest identical-request count in the window
}

type ev struct {
	t   time.Time
	sig string
}

type runWindow struct {
	events   []ev
	lastSeen time.Time
}

type detector struct {
	policy LoopPolicy
	runs   map[string]*runWindow
	mu     sync.Mutex
}

func newDetector(p LoopPolicy) *detector {
	if p.Now == nil {
		p.Now = time.Now
	}
	if p.MaxRuns <= 0 {
		p.MaxRuns = 10000
	}
	if p.Window <= 0 {
		p.Window = time.Minute
	}
	return &detector{policy: p, runs: map[string]*runWindow{}}
}

// observe records one request for run and returns the decision. run must be a
// non-empty caller-supplied run id.
func (d *detector) observe(run, sig string, now time.Time) Decision {
	d.mu.Lock()
	defer d.mu.Unlock()

	cutoff := now.Add(-d.policy.Window)
	w := d.runs[run]
	if w == nil {
		w = &runWindow{}
		d.runs[run] = w
	}
	// Drop this run's requests that fell out of the window, then add this one.
	kept := w.events[:0]
	for _, e := range w.events {
		if e.t.After(cutoff) {
			kept = append(kept, e)
		}
	}
	w.events = append(kept, ev{t: now, sig: sig})
	w.lastSeen = now

	count := len(w.events)
	repeat := 0
	if d.policy.MaxRepeat > 0 {
		bySig := map[string]int{}
		for _, e := range w.events {
			bySig[e.sig]++
			if bySig[e.sig] > repeat {
				repeat = bySig[e.sig]
			}
		}
	}

	d.evict(now, cutoff)

	dec := Decision{Count: count, Repeat: repeat}
	var signals []string
	if d.policy.MaxPerRun > 0 && count >= d.policy.MaxPerRun {
		signals = append(signals, "rate")
	}
	if d.policy.MaxRepeat > 0 && repeat >= d.policy.MaxRepeat {
		signals = append(signals, "repeat")
	}
	if len(signals) > 0 {
		dec.Suspected = true
		dec.Signal = strings.Join(signals, "+")
	}
	return dec
}

// evict drops runs whose most recent request left the window, and if still
// over the cap, drops the least-recently-seen runs. This bounds memory to the
// runs actually active within the window.
func (d *detector) evict(now, cutoff time.Time) {
	for id, w := range d.runs {
		if w.lastSeen.Before(cutoff) || w.lastSeen.Equal(cutoff) {
			delete(d.runs, id)
		}
	}
	if len(d.runs) <= d.policy.MaxRuns {
		return
	}
	type age struct {
		id string
		t  time.Time
	}
	ages := make([]age, 0, len(d.runs))
	for id, w := range d.runs {
		ages = append(ages, age{id, w.lastSeen})
	}
	sort.Slice(ages, func(i, j int) bool { return ages[i].t.Before(ages[j].t) })
	for _, a := range ages[:len(d.runs)-d.policy.MaxRuns] {
		delete(d.runs, a.id)
	}
}
