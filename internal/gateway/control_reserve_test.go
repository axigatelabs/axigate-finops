package gateway

import "testing"

// A burst of concurrent calls for one run (admitted but not yet recorded) must
// trip the spend cap near the boundary, not after ~concurrency of overshoot.
func TestReserveAtAdmitBoundsConcurrentSpend(t *testing.T) {
	c := newController(ControlPolicy{MaxSpendUSDPerRun: 0.25, Now: fixed()})
	// One completed call establishes the run's average (~$0.03).
	if ok, _ := c.admit("r", false); !ok {
		t.Fatal("first admit")
	}
	c.record("r", 0.03, false)

	admitted := 0
	for i := 0; i < 100; i++ { // a 100-wide burst, none recording yet
		if ok, _ := c.admit("r", false); ok {
			admitted++
		} else {
			break
		}
	}
	// Without reserve-at-admit all 100 would pass (spent stays $0.03 < $0.25).
	// With it, admits stop once $0.03 + $0.03*inflight reaches $0.25 — ~8, not 100.
	if admitted > 10 {
		t.Fatalf("reserve-at-admit should bound the burst near the cap; admitted %d of 100", admitted)
	}
	if admitted < 3 {
		t.Fatalf("should still admit several calls under the cap; admitted %d", admitted)
	}
}

// The call cap admits exactly its budget concurrently, even with nothing recorded.
func TestReserveAtAdmitBoundsConcurrentCalls(t *testing.T) {
	c := newController(ControlPolicy{MaxCallsPerRun: 5, Now: fixed()})
	admitted := 0
	for i := 0; i < 100; i++ {
		if ok, _ := c.admit("r", false); ok {
			admitted++
		} else {
			break
		}
	}
	if admitted != 5 {
		t.Fatalf("call cap should admit exactly 5 concurrent, got %d", admitted)
	}
}

// release frees an in-flight slot for a call that errored before recording, so
// the bound does not drift toward over-blocking.
func TestReleaseFreesInflight(t *testing.T) {
	c := newController(ControlPolicy{MaxCallsPerRun: 3, Now: fixed()})
	if ok, _ := c.admit("r", false); !ok { // inflight 1
		t.Fatal("admit 1")
	}
	if ok, _ := c.admit("r", false); !ok { // inflight 2
		t.Fatal("admit 2")
	}
	c.release("r") // errored before serving
	c.release("r") // inflight back to 0
	// All three slots are free again; without release only one more would fit.
	for i := 0; i < 3; i++ {
		if ok, _ := c.admit("r", false); !ok {
			t.Fatalf("slot %d should be free after release", i+1)
		}
	}
}

// Resume grants a fresh allowance: a reviewed run continues under a full budget
// rather than re-pausing on its own history at the next admit.
func TestResumeGrantsFreshBudget(t *testing.T) {
	c := newController(ControlPolicy{MaxCallsPerRun: 2, Now: fixed()})
	for i := 0; i < 2; i++ {
		if ok, _ := c.admit("r", false); !ok {
			t.Fatalf("admit %d", i+1)
		}
		c.record("r", 0, false)
	}
	if ok, _ := c.admit("r", false); ok {
		t.Fatal("run should be paused after using its budget")
	}
	if !c.resumeRun("r") {
		t.Fatal("resume should find the paused run")
	}
	for i := 0; i < 2; i++ {
		if ok, _ := c.admit("r", false); !ok {
			t.Fatalf("post-resume admit %d should succeed under the fresh budget", i+1)
		}
		c.record("r", 0, false)
	}
	if ok, _ := c.admit("r", false); ok {
		t.Fatal("run should re-pause after the fresh budget is used")
	}
}

// A burst that hits a run before its first cost is recorded has no average to
// reserve against, so without a floor every call in the burst is admitted.
// With --reserve-per-call the burst is bounded at the cap.
func TestReservePerCallBoundsAColdBurst(t *testing.T) {
	// Without a floor: a cold run admits the whole burst.
	c := newController(ControlPolicy{MaxSpendUSDPerRun: 0.10, Now: fixed()})
	for i := 0; i < 5; i++ {
		if ok, _ := c.admit("cold", false); !ok {
			t.Fatalf("without a floor, cold call %d should be admitted (nothing to reserve against)", i+1)
		}
	}
	// With a $0.05 floor and a $0.10 cap: two calls reserve the whole cap; the
	// third is refused before it leaves, and the run is paused.
	c = newController(ControlPolicy{MaxSpendUSDPerRun: 0.10, ReserveUSDPerCall: 0.05, Now: fixed()})
	for i := 0; i < 2; i++ {
		if ok, _ := c.admit("cold", false); !ok {
			t.Fatalf("call %d should be admitted: reserved so far is under the cap", i+1)
		}
	}
	if ok, reason := c.admit("cold", false); ok || reason != "run reached the spend cap of $0.10" {
		t.Fatalf("third call of a cold burst should be refused at the cap: ok=%v reason=%q", ok, reason)
	}
	// Recording the in-flight calls cheaply does not un-pause the run.
	c.record("cold", 0.01, false)
	c.record("cold", 0.01, false)
	if ok, _ := c.admit("cold", false); ok {
		t.Fatal("a paused run stays paused until resumed")
	}
	// The floor is a reservation, not a charge: only the recorded cost lands.
	if s := c.status(); len(s.PausedRuns) != 1 || s.PausedRuns[0].SpendUSD != 0.02 {
		t.Fatalf("recorded spend should be the real $0.02, got %+v", s.PausedRuns)
	}
}

// The floor is "at least": once the run has an average above it, the average
// is what gets reserved; a cheap start never drags the reservation under it.
func TestReservePerCallIsAFloorNotAnOverride(t *testing.T) {
	// A call is admitted when the run's spend plus the reservations of the
	// calls ahead of it is still under the cap (the call that crosses the cap
	// is served; the next one is refused), so count admits accordingly.
	c := newController(ControlPolicy{MaxSpendUSDPerRun: 1.00, ReserveUSDPerCall: 0.10, Now: fixed()})
	c.record("r", 0.40, false) // average $0.40 > floor $0.10
	// 1st: $0.40 + 0 ahead → admitted. 2nd: $0.40 + $0.40 = $0.80 → admitted.
	for i := 0; i < 2; i++ {
		if ok, _ := c.admit("r", false); !ok {
			t.Fatalf("call %d still fits under the cap at the average", i+1)
		}
	}
	// 3rd: $0.40 + 2 × $0.40 = $1.20 ≥ $1.00 → refused. Reserving at the lower
	// floor instead would give $0.60 and wrongly admit it.
	if ok, _ := c.admit("r", false); ok {
		t.Fatal("the average, not the lower floor, must be reserved once it is known")
	}

	// And the other way round: a cheap start does not pull the reservation
	// below the floor.
	c = newController(ControlPolicy{MaxSpendUSDPerRun: 0.10, ReserveUSDPerCall: 0.05, Now: fixed()})
	c.record("r", 0.001, false) // average $0.001 < floor $0.05
	// 1st: $0.001 → admitted. 2nd: $0.001 + $0.05 = $0.051 → admitted.
	for i := 0; i < 2; i++ {
		if ok, _ := c.admit("r", false); !ok {
			t.Fatalf("call %d still fits under the cap at the floor", i+1)
		}
	}
	// 3rd: $0.001 + 2 × $0.05 = $0.101 ≥ $0.10 → refused. Reserving at the
	// cheap average would give $0.003 and admit the whole burst.
	if ok, _ := c.admit("r", false); ok {
		t.Fatal("the floor, not the cheap average, must be reserved")
	}
}
