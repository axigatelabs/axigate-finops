package gateway

import (
	"strings"
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

func TestMoneyPrintsCentsOrFinerWhenTheCapIsFiner(t *testing.T) {
	for v, want := range map[float64]string{1: "$1.00", 0.1: "$0.10", 0.02: "$0.02", 0.015: "$0.015", 5.5: "$5.50", 0.0075: "$0.0075"} {
		if got := money(v); got != want {
			t.Errorf("money(%v) = %q, want %q", v, got, want)
		}
	}
}

// A key's ceilings hold across the runs it starts, reset by the day, and are
// released by an operator — the leaked-key case.
func TestKeyCeilingsHoldAcrossRunsAndResetByDay(t *testing.T) {
	now := time.Date(2026, 9, 11, 23, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	c := newController(ControlPolicy{MaxSpendUSDPerKeyDay: 0.02, MaxSpendUSDPerKey: 0.04, Now: clock})
	var alerts []StopEvent
	c.onPauseSync = func(ev StopEvent) { alerts = append(alerts, ev) }
	c.noteKey("k-0123456789ab-1234", "thief", "", "gpt-4o")
	// Three runs on one key at $0.0075 each: the third crosses the daily cap.
	for i, run := range []string{"r1", "r2", "r3"} {
		if ok, _ := c.admitKeyed(run, "k-0123456789ab-1234", false); !ok {
			t.Fatalf("call %d should be admitted", i+1)
		}
		c.recordKeyed(run, "k-0123456789ab-1234", 0.0075, false)
	}
	if ok, reason := c.admitKeyed("r4", "k-0123456789ab-1234", false); ok || reason != "key …1234 reached its daily spend cap of $0.02 (resets at midnight UTC)" {
		t.Fatalf("a fresh run on a capped key is refused: ok=%v reason=%q", ok, reason)
	}
	if len(alerts) != 1 || alerts[0].Key != "k-0123456789ab-1234" || alerts[0].Run != "" || alerts[0].Agent != "thief" || alerts[0].Calls != 3 {
		t.Fatalf("one key alert with attribution: %+v", alerts)
	}
	// Another key is unaffected; an untagged call on the capped key is not.
	if ok, _ := c.admitKeyed("r5", "k-0123456789ab-9999", false); !ok {
		t.Fatal("another key is not affected")
	}
	if ok, _ := c.admitKeyed("", "k-0123456789ab-1234", false); ok {
		t.Fatal("an untagged call still counts against its key")
	}
	st := c.status()
	if len(st.PausedKeys) != 1 || st.PausedKeys[0].Key != "k-0123456789ab-1234" || st.PausedKeys[0].CallsToday != 3 {
		t.Fatalf("status lists the paused key: %+v", st.PausedKeys)
	}
	// Midnight UTC: the daily pause clears itself; the total keeps counting.
	now = now.Add(2 * time.Hour)
	if ok, _ := c.admitKeyed("r6", "k-0123456789ab-1234", false); !ok {
		t.Fatal("the daily cap resets at midnight UTC")
	}
	c.recordKeyed("r6", "k-0123456789ab-1234", 0.0075, false)
	for i := 0; i < 2; i++ { // $0.045 total: the total cap of $0.04 is crossed on the second, before today's cap
		c.admitKeyed("r7", "k-0123456789ab-1234", false)
		c.recordKeyed("r7", "k-0123456789ab-1234", 0.0075, false)
	}
	if ok, reason := c.admitKeyed("r8", "k-0123456789ab-1234", false); ok || reason != "key …1234 reached its spend cap of $0.04" {
		t.Fatalf("the lifetime cap refuses: ok=%v reason=%q", ok, reason)
	}
	now = now.Add(24 * time.Hour)
	if ok, _ := c.admitKeyed("r9", "k-0123456789ab-1234", false); ok {
		t.Fatal("a lifetime pause does not clear with the day")
	}
	if !c.resumeKey("k-0123456789ab-1234") {
		t.Fatal("resume finds the paused key")
	}
	if ok, _ := c.admitKeyed("r9", "k-0123456789ab-1234", false); !ok {
		t.Fatal("a resumed key gets a fresh budget")
	}
	if len(alerts) != 2 {
		t.Fatalf("one alert per key stop, got %d", len(alerts))
	}
}

func TestKeyCeilingReservesAtAdmitAndReleases(t *testing.T) {
	c := newController(ControlPolicy{MaxSpendUSDPerKeyDay: 0.02, ReserveUSDPerCall: 0.01, Now: fixed()})
	// Two calls reserve the whole daily cap; the third is refused before it leaves.
	c.admitKeyed("a", "k", false)
	c.admitKeyed("b", "k", false)
	if ok, _ := c.admitKeyed("c", "k", false); ok {
		t.Fatal("a cold burst on one key is held at the key's cap")
	}
	c.resumeKey("k")
	c.admitKeyed("d", "k", false)
	c.releaseKeyed("d", "k") // never left: the key's reservation is returned
	c.admitKeyed("e", "k", false)
	if ok, _ := c.admitKeyed("f", "k", false); !ok {
		t.Fatal("after a release two calls still fit")
	}
}

// Record pauses a key on what was actually spent, the way the run path and
// the shared store do; what is still in flight is admit's business, so a
// released call never leaves a key paused with nothing spent.
func TestKeyRecordPausesOnActualSpendOnly(t *testing.T) {
	c := newController(ControlPolicy{MaxSpendUSDPerKey: 0.03, ReserveUSDPerCall: 0.01, Now: fixed()})
	c.admitKeyed("a", "k", false)
	c.admitKeyed("b", "k", false)
	c.admitKeyed("c", "k", false)
	c.recordKeyed("a", "k", 0.011, false)
	c.releaseKeyed("b", "k")
	c.releaseKeyed("c", "k")
	if ok, reason := c.admitKeyed("d", "k", false); !ok {
		t.Fatalf("$0.011 spent of $0.03 with nothing in flight admits: %q", reason)
	}
	c.recordKeyed("d", "k", 0.02, false) // $0.031 actually spent
	if ok, _ := c.admitKeyed("e", "k", false); ok {
		t.Fatal("the total cap holds on settled spend")
	}
}

// A "let it run today" resume after a daily pause keeps the total the
// operator set; a resume after a total pause grants a fresh total.
func TestKeyResumeAfterADailyPauseKeepsTheTotal(t *testing.T) {
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	c := newController(ControlPolicy{MaxSpendUSDPerKeyDay: 0.05, MaxSpendUSDPerKey: 0.06, Now: func() time.Time { return now }})
	spend := func(run string, n int) {
		for i := 0; i < n; i++ {
			if ok, _ := c.admitKeyed(run, "k", false); !ok {
				return
			}
			c.recordKeyed(run, "k", 0.0075, false)
		}
	}
	spend("r1", 7) // $0.0525 today: paused by the daily cap
	st := c.status()
	if len(st.PausedKeys) != 1 || !strings.Contains(st.PausedKeys[0].Reason, "daily") || st.PausedKeys[0].TotalUSD < 0.05 || st.PausedKeys[0].CallsToday != 7 {
		t.Fatalf("status names the cap and today's figures: %+v", st.PausedKeys)
	}
	if !c.resumeKey("k") {
		t.Fatal("resume finds the key")
	}
	spend("r2", 1) // $0.06 total: the total cap fires on the next call, not $0.06 later
	if ok, reason := c.admitKeyed("r3", "k", false); ok || reason != "key k reached its spend cap of $0.06" {
		t.Fatalf("the total survived the daily resume: ok=%v reason=%q", ok, reason)
	}
	c.resumeKey("k")
	spend("r4", 1)
	if ok, _ := c.admitKeyed("r5", "k", false); !ok {
		t.Fatal("a resume after a total pause grants a fresh total")
	}
}

// The key's day only moves forward, so a lagging clock never resets a tally,
// and midnight clears yesterday's reservations along with the tally so a
// crashed replica's phantom cannot refuse a key for good.
func TestKeyDayOnlyMovesForwardAndClearsInflight(t *testing.T) {
	now := time.Date(2026, 9, 11, 23, 30, 0, 0, time.UTC)
	c := newController(ControlPolicy{MaxSpendUSDPerKeyDay: 0.02, ReserveUSDPerCall: 0.01, Now: func() time.Time { return now }})
	c.admitKeyed("a", "k", false)
	c.admitKeyed("b", "k", false) // both in flight and never settled
	if ok, _ := c.admitKeyed("c", "k", false); ok {
		t.Fatal("reserved to the cap")
	}
	now = now.Add(time.Hour) // midnight
	if ok, _ := c.admitKeyed("d", "k", false); !ok {
		t.Fatal("midnight clears yesterday's reservations")
	}
	c.recordKeyed("d", "k", 0.0075, false)
	now = now.Add(-26 * time.Hour) // a clock that lags a day
	c.admitKeyed("e", "k", false)
	c.recordKeyed("e", "k", 0.0075, false)
	c.admitKeyed("f", "k", false)
	c.recordKeyed("f", "k", 0.0075, false) // $0.0225 on the same day as d
	if ok, _ := c.admitKeyed("g", "k", false); ok {
		t.Fatal("a lagging clock must not reset the daily tally")
	}
	now = now.Add(50 * time.Hour) // the day after the pause
	if len(c.status().PausedKeys) != 0 {
		t.Fatal("status does not list a daily pause the day has already cleared")
	}
}

// Shadow mode: a call a cap would refuse is served and reserved all the
// same, the reason travels back with it, the alert fires once, and the kill
// switch still refuses.
func TestShadowModeServesAndMarksWhatACapWouldRefuse(t *testing.T) {
	c := newController(ControlPolicy{MaxCallsPerRun: 2, MaxSpendUSDPerKeyDay: 0.01, Shadow: true, Now: fixed()})
	var alerts []StopEvent
	c.onPauseSync = func(ev StopEvent) { alerts = append(alerts, ev) }
	for i := 0; i < 2; i++ {
		if ok, reason := c.admitKeyed("r1", "", false); !ok || reason != "" {
			t.Fatalf("call %d is under the cap: ok=%v reason=%q", i+1, ok, reason)
		}
		c.recordKeyed("r1", "", 0.001, false)
	}
	ok, reason := c.admitKeyed("r1", "", false)
	if !ok || reason != "run reached the call cap of 2" {
		t.Fatalf("shadow serves with the reason: ok=%v reason=%q", ok, reason)
	}
	c.recordKeyed("r1", "", 0.001, false)
	if ok, reason := c.admitKeyed("r1", "", false); !ok || reason == "" {
		t.Fatalf("still served, still marked: ok=%v reason=%q", ok, reason)
	}
	if len(alerts) != 1 || !alerts[0].Shadow || alerts[0].Run != "r1" {
		t.Fatalf("one shadow alert: %+v", alerts)
	}
	if st := c.status(); len(st.PausedRuns) != 1 || !st.Policy.Shadow {
		t.Fatalf("status shows the marked run and the mode: %+v", st)
	}
	if !c.resumeRun("r1") {
		t.Fatal("resume finds it")
	}
	if ok, reason := c.admitKeyed("r1", "", false); !ok || reason != "" {
		t.Fatalf("a resumed run is clean again: ok=%v reason=%q", ok, reason)
	}
	// A key past its cap is served with the key's reason.
	c.admitKeyed("r2", "k-0123456789ab-7777", false)
	c.recordKeyed("r2", "k-0123456789ab-7777", 0.05, false)
	if ok, reason := c.admitKeyed("r3", "k-0123456789ab-7777", false); !ok || !strings.HasPrefix(reason, "key …7777 reached its daily spend cap") {
		t.Fatalf("shadow serves a capped key with its reason: ok=%v reason=%q", ok, reason)
	}
	if len(alerts) != 2 || !alerts[1].Shadow || alerts[1].Key == "" {
		t.Fatalf("one shadow alert for the key: %+v", alerts)
	}
	// The kill switch is an operator's act and still refuses.
	c.setKilled(true)
	if ok, reason := c.admitKeyed("r4", "", false); ok || reason != "kill switch engaged" {
		t.Fatalf("shadow does not soften the kill switch: ok=%v reason=%q", ok, reason)
	}
}
