package seed

import (
	"testing"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
)

func TestGenerateIsDeterministicAndWellFormed(t *testing.T) {
	a := Generate(Options{Events: 3000, Seed: 42})
	b := Generate(Options{Events: 3000, Seed: 42})
	if len(a) != 3000 || len(b) != 3000 {
		t.Fatalf("count: %d %d", len(a), len(b))
	}
	for i := range a {
		if a[i].ID != b[i].ID || a[i].CostUSD != b[i].CostUSD {
			t.Fatalf("not deterministic at %d", i)
		}
		if err := a[i].Validate(); err != nil {
			t.Fatalf("invalid seeded event: %v", err)
		}
	}
	// Every event id is unique (so a re-import never doubles).
	ids := map[string]bool{}
	for _, e := range a {
		if ids[e.ID] {
			t.Fatalf("duplicate id %s", e.ID)
		}
		ids[e.ID] = true
	}
}

func TestGenerateContainsARunawayLoopWithBlockedCalls(t *testing.T) {
	evs := Generate(Options{Events: 2000, Seed: 1})
	var loop, blocked int
	teams := map[string]bool{}
	for _, e := range evs {
		if e.Tags.Run == "run-planner-RUNAWAY" {
			loop++
			if e.Dimensions["blocked"] != "" {
				blocked++
				if e.CostUSD != 0 {
					t.Fatal("a blocked call must have no cost")
				}
			}
		}
		teams[e.Tags.Team] = true
	}
	if loop == 0 || blocked == 0 {
		t.Fatalf("expected a runaway loop with blocked calls, got loop=%d blocked=%d", loop, blocked)
	}
	if len(teams) < 3 {
		t.Fatalf("expected several teams, got %d", len(teams))
	}
	_ = ledger.Estimated
}
