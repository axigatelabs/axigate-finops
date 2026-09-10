package console

import (
	"fmt"
	"testing"
	"time"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
)

// rollEvent builds one served or refused gateway event for a run.
func rollEvent(run, agent, status string, cost float64, blocked bool, seq int) ledger.Event {
	dims := map[string]string{"status": status, "request_id": fmt.Sprintf("%d", seq)}
	if blocked {
		dims["blocked"] = "run reached the spend cap of $0.10"
	}
	e := ledger.Event{
		Source: "gateway", Provider: "openai", Model: "gpt-4o", Dimensions: dims,
		Tags: ledger.Tags{Run: run, Agent: agent}, CostUSD: cost, Confidence: ledger.Estimated,
		StartsAt: time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC), EndsAt: time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC),
	}
	e.DeriveID()
	return e
}

func scopeOver(evs []ledger.Event) scope {
	e, l := boundsOf(evs)
	return scope{evs: evs, earliest: e, latest: l}
}

func near(a, b float64) bool { return a > b-0.0005 && a < b+0.0005 }

func TestByRunRollsUpSpendCallsFailuresAndRefusals(t *testing.T) {
	evs := []ledger.Event{
		rollEvent("run-a", "planner", "200", 0.02, false, 1),
		rollEvent("run-a", "planner", "200", 0.02, false, 2),
		rollEvent("run-b", "reconciler", "200", 0.10, false, 3), // one costly success
		rollEvent("run-b", "reconciler", "500", 0, false, 4),    // dead work: provider errored
		rollEvent("run-b", "reconciler", "429", 0, true, 5),     // refused by the gateway
		rollEvent("run-b", "reconciler", "429", 0, true, 6),
		rollEvent("", "", "200", 0.01, false, 7), // untagged
	}
	sum := summaryOf(scopeOver(evs), 0)

	if len(sum.ByRun) != 3 {
		t.Fatalf("ByRun has %d lines, want 3: %+v", len(sum.ByRun), sum.ByRun)
	}
	b := sum.ByRun[0]
	if b.Run != "run-b" || !near(b.SpentUSD, 0.10) {
		t.Fatalf("top run should be run-b at $0.10, got %+v", b)
	}
	if b.Calls != 2 || b.Failed != 1 || b.Blocked != 2 {
		t.Fatalf("run-b counts: calls=%d failed=%d blocked=%d, want 2/1/2", b.Calls, b.Failed, b.Blocked)
	}
	if !near(b.AvoidedUSD, 0.20) { // 2 refused × the run's own $0.10 average
		t.Fatalf("run-b prevented = %v, want ~0.20", b.AvoidedUSD)
	}
	a := sum.ByRun[1]
	if a.Run != "run-a" || !near(a.SpentUSD, 0.04) || a.Calls != 2 || a.Failed != 0 || a.Blocked != 0 {
		t.Fatalf("run-a line wrong: %+v", a)
	}
	u := sum.ByRun[2]
	if u.Run != "(untagged)" || !near(u.SpentUSD, 0.01) {
		t.Fatalf("untagged line wrong: %+v", u)
	}
}

func TestByRunIsCappedToTopRuns(t *testing.T) {
	var evs []ledger.Event
	for i := 0; i < maxRunLines+5; i++ {
		evs = append(evs, rollEvent(fmt.Sprintf("run-%02d", i), "bot", "200", float64(i)*0.01, false, i))
	}
	sum := summaryOf(scopeOver(evs), 0)
	if len(sum.ByRun) != maxRunLines {
		t.Fatalf("ByRun should cap at %d, got %d", maxRunLines, len(sum.ByRun))
	}
	if sum.ByRun[0].Run != fmt.Sprintf("run-%02d", maxRunLines+4) {
		t.Fatalf("top line should be the biggest spender, got %s", sum.ByRun[0].Run)
	}
}
