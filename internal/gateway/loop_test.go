package gateway

import (
	"testing"
	"time"
)

func clockFrom(t0 time.Time) (func() time.Time, func(d time.Duration)) {
	cur := t0
	return func() time.Time { return cur }, func(d time.Duration) { cur = cur.Add(d) }
}

func TestDetectorFlagsRateWithinAWindow(t *testing.T) {
	now, adv := clockFrom(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	d := newDetector(LoopPolicy{Window: time.Minute, MaxPerRun: 5, Now: now})
	var last Decision
	for i := 0; i < 5; i++ {
		last = d.observe("run-1", "sig", now())
		adv(time.Second)
	}
	if !last.Suspected || last.Signal != "rate" || last.Count != 5 {
		t.Fatalf("fifth request in window should flag rate: %+v", last)
	}
	// A request in a different run is independent.
	if dec := d.observe("run-2", "sig", now()); dec.Suspected {
		t.Fatalf("a different run must not inherit run-1's count: %+v", dec)
	}
}

func TestDetectorFlagsRepeatOfIdenticalCalls(t *testing.T) {
	now, adv := clockFrom(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	// A high rate ceiling so only the repeat signal can fire.
	d := newDetector(LoopPolicy{Window: time.Minute, MaxPerRun: 1000, MaxRepeat: 3, Now: now})
	d.observe("r", "same", now())
	adv(time.Second)
	d.observe("r", "different", now())
	adv(time.Second)
	dec := d.observe("r", "same", now())
	adv(time.Second)
	if dec.Suspected {
		t.Fatalf("two identical of three is not yet a loop: %+v", dec)
	}
	dec = d.observe("r", "same", now()) // third "same"
	if !dec.Suspected || dec.Signal != "repeat" || dec.Repeat != 3 {
		t.Fatalf("third identical call should flag repeat: %+v", dec)
	}
}

func TestDetectorForgetsRequestsOutsideTheWindow(t *testing.T) {
	now, adv := clockFrom(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	d := newDetector(LoopPolicy{Window: 10 * time.Second, MaxPerRun: 3, Now: now})
	// Three requests, but spaced so only one is ever inside the 10s window.
	for i := 0; i < 3; i++ {
		if dec := d.observe("r", "sig", now()); dec.Suspected {
			t.Fatalf("spaced-out requests must not flag: %+v", dec)
		}
		adv(20 * time.Second)
	}
	// The run whose only request fell out of the window is evicted.
	d.observe("r", "sig", now())
	if len(d.runs) != 1 {
		t.Fatalf("expected one live run, got %d", len(d.runs))
	}
}

func TestDetectorBoundsTrackedRuns(t *testing.T) {
	now, adv := clockFrom(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	d := newDetector(LoopPolicy{Window: time.Hour, MaxPerRun: 100, MaxRuns: 50, Now: now})
	for i := 0; i < 500; i++ {
		d.observe(string(rune('A'+i%26))+time.Duration(i).String(), "sig", now())
		adv(time.Millisecond)
	}
	if len(d.runs) > 50 {
		t.Fatalf("tracked runs not bounded: %d > 50", len(d.runs))
	}
}

func TestPolicyActiveOnlyWhenAThresholdIsSet(t *testing.T) {
	if (LoopPolicy{}).active() {
		t.Fatal("zero policy must be inactive (opt-in)")
	}
	if !(LoopPolicy{MaxPerRun: 1}).active() || !(LoopPolicy{MaxRepeat: 1}).active() {
		t.Fatal("a positive threshold must activate the policy")
	}
}
