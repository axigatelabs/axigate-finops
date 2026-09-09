package gateway

import (
	"testing"
	"time"
)

func fixed() func() time.Time {
	t := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	return func() time.Time { return t }
}

func TestCallCapPausesTheRunAfterItIsReached(t *testing.T) {
	c := newController(ControlPolicy{MaxCallsPerRun: 3, Now: fixed()})
	for i := 0; i < 3; i++ {
		if ok, _ := c.admit("r", false); !ok {
			t.Fatalf("request %d should be admitted before the cap trips", i+1)
		}
		c.record("r", 0.01, false)
	}
	// The third call reached the cap; the fourth is refused.
	ok, reason := c.admit("r", false)
	if ok || reason == "" {
		t.Fatalf("fourth request should be refused, got ok=%v reason=%q", ok, reason)
	}
	// A different run is unaffected; an untagged request is never capped.
	if ok, _ := c.admit("other", false); !ok {
		t.Fatal("a different run must not be paused")
	}
	if ok, _ := c.admit("", false); !ok {
		t.Fatal("an untagged request must never be capped")
	}
}

func TestSpendCapPausesTheRun(t *testing.T) {
	c := newController(ControlPolicy{MaxSpendUSDPerRun: 1.00, Now: fixed()})
	c.record("r", 0.60, false)
	if ok, _ := c.admit("r", false); !ok {
		t.Fatal("under the spend cap should still be admitted")
	}
	c.record("r", 0.60, false) // now $1.20 >= $1.00
	if ok, reason := c.admit("r", false); ok || reason == "" {
		t.Fatalf("over the spend cap should be refused: ok=%v", ok)
	}
}

func TestPauseOnSuspectedLoop(t *testing.T) {
	c := newController(ControlPolicy{PauseOnSuspectedLoop: true, Now: fixed()})
	c.record("r", 0, false)
	if ok, _ := c.admit("r", false); !ok {
		t.Fatal("no loop yet")
	}
	c.record("r", 0, true) // detection flagged this one
	if ok, reason := c.admit("r", false); ok || reason != "suspected loop" {
		t.Fatalf("a flagged loop should pause the run: ok=%v reason=%q", ok, reason)
	}
}

func TestBypassOverridesPauseButNotKill(t *testing.T) {
	c := newController(ControlPolicy{MaxCallsPerRun: 1, AdminToken: "tok", Now: fixed()})
	c.record("r", 0, false) // reaches the cap of 1
	if ok, _ := c.admit("r", false); ok {
		t.Fatal("run should be paused")
	}
	if ok, _ := c.admit("r", true); !ok {
		t.Fatal("bypass should force the request through a pause")
	}
	// The kill switch is absolute: bypass does not override it.
	c.setKilled(true)
	if ok, _ := c.admit("r", true); ok {
		t.Fatal("bypass must not override the kill switch")
	}
}

func TestResumeClearsAPausedRun(t *testing.T) {
	c := newController(ControlPolicy{MaxCallsPerRun: 1, Now: fixed()})
	c.record("r", 0, false)
	if ok, _ := c.admit("r", false); ok {
		t.Fatal("should be paused")
	}
	if !c.resumeRun("r") {
		t.Fatal("resume should find the paused run")
	}
	if ok, _ := c.admit("r", false); !ok {
		t.Fatal("a resumed run should be admitted again")
	}
	if c.resumeRun("nope") {
		t.Fatal("resuming an unknown run should report nothing to do")
	}
}

func TestKillSwitchRefusesAllRuns(t *testing.T) {
	c := newController(ControlPolicy{MaxCallsPerRun: 100, Now: fixed()})
	c.setKilled(true)
	if ok, reason := c.admit("any", false); ok || reason != "kill switch engaged" {
		t.Fatalf("kill switch should refuse: ok=%v reason=%q", ok, reason)
	}
	c.setKilled(false)
	if ok, _ := c.admit("any", false); !ok {
		t.Fatal("clearing the kill switch should admit again")
	}
}

func TestStatusReportsPausedRunsOnly(t *testing.T) {
	c := newController(ControlPolicy{MaxSpendUSDPerRun: 1.00, Now: fixed()})
	c.record("paused", 1.50, false) // one call over the spend cap
	c.record("active", 0.10, false)
	c.record("active", 0.10, false) // still well under, not paused
	s := c.status()
	if len(s.PausedRuns) != 1 || s.PausedRuns[0].Run != "paused" {
		t.Fatalf("status paused runs: %+v", s.PausedRuns)
	}
	if s.Policy.MaxSpendUSDPerRun != 1.00 || s.Runs != 2 {
		t.Fatalf("status policy/runs: %+v", s)
	}
}

func TestControlActiveOnlyWhenConfigured(t *testing.T) {
	if (ControlPolicy{}).active() {
		t.Fatal("zero control policy must be inactive")
	}
	if !(ControlPolicy{MaxCallsPerRun: 1}).active() ||
		!(ControlPolicy{MaxSpendUSDPerRun: 1}).active() ||
		!(ControlPolicy{PauseOnSuspectedLoop: true}).active() ||
		!(ControlPolicy{AllowRequestCaps: true}).active() {
		t.Fatal("any threshold, or allowing request caps, should activate control")
	}
}

func TestInlineMaxSpendCapPausesTheRun(t *testing.T) {
	// A caller sets its own $1 cap from code (the X-AxiGate-Max-Spend header,
	// applied via setRunCap) with no server-wide cap at all. The run pauses once
	// cumulative spend reaches it.
	c := newController(ControlPolicy{AllowRequestCaps: true, Now: fixed()})
	c.setRunCap("r", 1.00)
	c.record("r", 0.60, false)
	if ok, _ := c.admit("r", false); !ok {
		t.Fatal("under the caller's cap should still be admitted")
	}
	c.record("r", 0.60, false) // now $1.20 >= $1.00
	if ok, reason := c.admit("r", false); ok || reason == "" {
		t.Fatalf("over the caller's inline cap should be refused: ok=%v reason=%q", ok, reason)
	}
	// A run the caller did not cap is unaffected; an untagged request is never
	// capped even if a header value is supplied.
	if ok, _ := c.admit("other", false); !ok {
		t.Fatal("a run with no inline cap must not be paused")
	}
	c.setRunCap("", 0.01)
	if ok, _ := c.admit("", false); !ok {
		t.Fatal("an untagged request must never be capped")
	}
}

func TestStrictestSpendCapWins(t *testing.T) {
	// Server-wide cap $5 and a caller inline cap $1 both in play: the stricter
	// one binds each way.
	c := newController(ControlPolicy{MaxSpendUSDPerRun: 5.00, AllowRequestCaps: true, Now: fixed()})
	// Run "r": inline $1 is stricter than the server's $5, so $1 binds.
	c.setRunCap("r", 1.00)
	c.record("r", 1.20, false) // $1.20 >= $1.00
	if ok, _ := c.admit("r", false); ok {
		t.Fatal("the stricter inline cap ($1) should pause the run")
	}
	// Run "s": inline $10 is looser than the server's $5, so $5 binds.
	c.setRunCap("s", 10.00)
	c.record("s", 6.00, false) // $6 >= $5
	if ok, _ := c.admit("s", false); ok {
		t.Fatal("the stricter server cap ($5) should pause the run")
	}
}
