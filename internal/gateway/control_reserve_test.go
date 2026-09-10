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
