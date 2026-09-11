package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
)

// These tests cover what the dashboard shows over a ledger that holds both
// metered calls and a provider's bill: the total, the lines, the gap.

// taggedCall is a priced gateway call with a team, an agent and a run.
func taggedCall(team, agent, run string, cost float64, seq int) ledger.Event {
	e := pricedUsageEvent(cost, seq)
	e.Tags = ledger.Tags{Team: team, Agent: agent, Run: run}
	e.DeriveID()
	return e
}

func usdOf(ls []Line, key string) (float64, bool) {
	for _, l := range ls {
		if l.Key == key {
			return l.USD, true
		}
	}
	return 0, false
}

func sumUSD(ls []Line) float64 {
	var s float64
	for _, l := range ls {
		s += l.USD
	}
	return s
}

// A ledger has tagged gateway calls; then the provider's bill for the same
// month lands and says more than the gateway metered. The bill sets the total.
// It must not erase who spent it, become a run, or spike the day chart.
func TestImportedBillSetsTheTotalButNeverErasesAttribution(t *testing.T) {
	calls := []ledger.Event{
		taggedCall("research", "planner", "research-42", 0.06, 1),
		taggedCall("research", "scraper", "research-42", 0.02, 2),
		taggedCall("payments", "refund-reconciler", "refund-9", 0.02, 3),
		rollEvent("refund-9", "refund-reconciler", "429", 0, true, 4), // refused, costs nothing
	}
	before := summaryOf(scopeOver(calls), 0)
	bill := providerCostRow("openai", 0.13, 0) // the provider says $0.13; the gateway metered $0.10
	after := summaryOf(scopeOver(append(calls, bill)), 0)

	if !near(after.TotalUSD, 0.13) || after.TotalState != "provider-reported" {
		t.Fatalf("the bill should set the total: total=%.4f state=%q", after.TotalUSD, after.TotalState)
	}
	if before.TotalState != "estimated" {
		t.Fatalf("without a bill the total is estimated, got %q", before.TotalState)
	}
	// Every team and agent keeps exactly its metered figure…
	for _, pair := range [][2][]Line{{before.ByTeam, after.ByTeam}, {before.ByAgent, after.ByAgent}} {
		for _, l := range pair[0] {
			if got, ok := usdOf(pair[1], l.Key); !ok || !near(got, l.USD) {
				t.Fatalf("%s went from %.4f to %.4f (present=%v) after the bill landed", l.Key, l.USD, got, ok)
			}
		}
	}
	// …the gap is one honest line, and the bars add up to the bill.
	if g, ok := usdOf(after.ByTeam, unmeteredKey); !ok || !near(g, 0.03) {
		t.Fatalf("by-team gap line = %.4f (present=%v), want 0.03: %+v", g, ok, after.ByTeam)
	}
	if g, ok := usdOf(after.ByAgent, unmeteredKey); !ok || !near(g, 0.03) || !near(after.UnmeteredUSD, 0.03) {
		t.Fatalf("by-agent gap line = %.4f (present=%v), unmetered=%.4f", g, ok, after.UnmeteredUSD)
	}
	if !near(sumUSD(after.ByTeam), after.TotalUSD) || !near(sumUSD(after.ByAgent), after.TotalUSD) {
		t.Fatalf("by-team %.4f / by-agent %.4f should add up to the bill %.4f", sumUSD(after.ByTeam), sumUSD(after.ByAgent), after.TotalUSD)
	}
	// Runs are a gateway concept: the bill is not a call and never becomes one.
	if len(after.ByRun) != len(before.ByRun) {
		t.Fatalf("runs changed after the bill: before %+v, after %+v", before.ByRun, after.ByRun)
	}
	for _, r := range after.ByRun {
		if r.Run == "(untagged)" {
			t.Fatalf("the bill showed up as an untagged run: %+v", r)
		}
	}
	// Models come from metered calls; a bill row has none.
	if _, ok := usdOf(after.ByModel, "(not grouped by model)"); ok || len(after.ByModel) != 1 {
		t.Fatalf("by-model should be the metered gpt-4o only: %+v", after.ByModel)
	}
	// The day chart is drawn from metered calls; a monthly bill is not a day.
	if len(after.Daily) != 1 || !near(after.Daily[0].USD, 0.10) || !near(after.PeakDayUSD, 0.10) {
		t.Fatalf("daily should still be the metered $0.10: %+v peak %.4f", after.Daily, after.PeakDayUSD)
	}
	// The headline counts name real teams and agents; the gap line is neither.
	if after.Teams != 2 || after.Agents != 3 {
		t.Fatalf("teams=%d agents=%d, want 2 and 3", after.Teams, after.Agents)
	}
}

// A bill below the gateway's estimate sets the total and adds no line: the
// bars stay the estimate (and so exceed the bill). A negative line would be a
// guess too.
func TestABillBelowTheEstimateAddsNoLine(t *testing.T) {
	calls := []ledger.Event{taggedCall("research", "planner", "research-42", 0.10, 1)}
	sum := summaryOf(scopeOver(append(calls, providerCostRow("openai", 0.08, 0))), 0)
	if !near(sum.TotalUSD, 0.08) || sum.UnmeteredUSD != 0 {
		t.Fatalf("total=%.4f unmetered=%.4f", sum.TotalUSD, sum.UnmeteredUSD)
	}
	if _, ok := usdOf(sum.ByTeam, unmeteredKey); ok {
		t.Fatalf("no gap line when the bill is below the estimate: %+v", sum.ByTeam)
	}
	if got, _ := usdOf(sum.ByTeam, "research"); !near(got, 0.10) {
		t.Fatalf("attribution stays the metered estimate, got %.4f", got)
	}
}

// A ledger fed by imports alone (no gateway) still attributes the bill —
// as unknown, which is the truth — and its total is the bill.
func TestABillAloneIsStillAttributedAsUnknown(t *testing.T) {
	sum := summaryOf(scopeOver([]ledger.Event{providerCostRow("openai", 0.50, 0)}), 0)
	if !near(sum.TotalUSD, 0.50) || sum.TotalState != "provider-reported" || sum.UnmeteredUSD != 0 {
		t.Fatalf("total=%.4f state=%q unmetered=%.4f", sum.TotalUSD, sum.TotalState, sum.UnmeteredUSD)
	}
	if got, _ := usdOf(sum.ByTeam, "(untagged)"); !near(got, 0.50) {
		t.Fatalf("a bill with no gateway behind it is untagged spend: %+v", sum.ByTeam)
	}
	if len(sum.ByRun) != 0 {
		t.Fatalf("a bill is not a run: %+v", sum.ByRun)
	}
	if len(sum.Daily) != 1 || !near(sum.Daily[0].USD, 0.50) {
		t.Fatalf("with nothing metered the bill is the only thing to chart: %+v", sum.Daily)
	}
}

// Last month's bill must not hide this month's live calls.
func TestLastMonthsBillLeavesThisMonthsCallsAlone(t *testing.T) {
	call := taggedCall("research", "planner", "research-42", 0.10, 1) // September 2026
	aug := providerCostRow("openai", 0.40, 0)
	aug.StartsAt, aug.EndsAt = time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)
	aug.DeriveID()
	sum := summaryOf(scopeOver([]ledger.Event{call, aug}), 0)
	if !near(sum.TotalUSD, 0.50) {
		t.Fatalf("total should be August's bill + September's calls, got %.4f", sum.TotalUSD)
	}
	if got, _ := usdOf(sum.ByTeam, "research"); !near(got, 0.10) || sum.UnmeteredUSD != 0 {
		t.Fatalf("September's attribution untouched, no gap: %+v unmetered=%.4f", sum.ByTeam, sum.UnmeteredUSD)
	}
	// August's bill is still on the bars (as untagged: nothing metered that month) and they add up.
	if got, _ := usdOf(sum.ByTeam, "(untagged)"); !near(got, 0.40) || !near(sumUSD(sum.ByTeam), sum.TotalUSD) || !near(sumUSD(sum.ByAgent), sum.TotalUSD) {
		t.Fatalf("August's bill should sit on the bars as untagged: team=%+v agent=%+v total=%.4f", sum.ByTeam, sum.ByAgent, sum.TotalUSD)
	}
	if sum.TotalState != "mixed" {
		t.Fatalf("a billed month plus a live month is a mixed total, got %q", sum.TotalState)
	}
}

// A month of nothing but refusals (a paused run kept retrying) plus its bill:
// the bill is the whole story, and it must still be on the bars.
func TestARefusalOnlyMonthStillShowsItsBill(t *testing.T) {
	refused := rollEvent("refund-9", "refund-reconciler", "429", 0, true, 1)
	sum := summaryOf(scopeOver([]ledger.Event{refused, providerCostRow("openai", 0.50, 0)}), 0)
	if !near(sum.TotalUSD, 0.50) {
		t.Fatalf("total = %.4f", sum.TotalUSD)
	}
	if g, ok := usdOf(sum.ByTeam, unmeteredKey); !ok || !near(g, 0.50) || !near(sumUSD(sum.ByTeam), sum.TotalUSD) || !near(sumUSD(sum.ByAgent), sum.TotalUSD) {
		t.Fatalf("the whole bill should be the gap line: team=%+v agent=%+v", sum.ByTeam, sum.ByAgent)
	}
	if sum.LoopsBlocked != 1 || len(sum.ByRun) != 1 || sum.ByRun[0].Blocked != 1 {
		t.Fatalf("the refusal is still a refusal: blocked=%d runs=%+v", sum.LoopsBlocked, sum.ByRun)
	}
}

// A cost page imported mid-month sets the total for the days it holds and
// nothing more: the gateway's later days stay priced usage, on the bars and
// in the total, and the bill is reconciled only against the days it covers.
func TestAPartialMonthBillCoversOnlyItsOwnDays(t *testing.T) {
	early := taggedCall("research", "planner", "research-42", 0.10, 1) // Sep 1
	late := taggedCall("research", "planner", "research-43", 0.05, 2)
	late.StartsAt, late.EndsAt = time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC), time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	late.DeriveID()
	bill := providerCostRow("openai", 0.13, 0) // covers Sep 1 only
	bill.EndsAt = time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	bill.DeriveID()
	sum := summaryOf(scopeOver([]ledger.Event{early, late, bill}), 0)
	if !near(sum.TotalUSD, 0.18) || sum.TotalState != "mixed" {
		t.Fatalf("total should be the bill for Sep 1 + the priced Sep 20 call: %.4f %q", sum.TotalUSD, sum.TotalState)
	}
	if got, _ := usdOf(sum.ByTeam, "research"); !near(got, 0.15) || !near(sum.UnmeteredUSD, 0.03) || !near(sumUSD(sum.ByTeam), sum.TotalUSD) {
		t.Fatalf("research keeps both calls, the gap is the Sep 1 difference: team=%+v unmetered=%.4f", sum.ByTeam, sum.UnmeteredUSD)
	}
	if len(sum.Daily) != 2 {
		t.Fatalf("both metered days chart, the bill adds no day: %+v", sum.Daily)
	}
}

// An imported usage page is metered spend but not calls: it prices and
// attributes, and never becomes a run.
func TestImportedUsageBucketsAreNotRuns(t *testing.T) {
	bucket := pricedUsageEvent(0.30, 1)
	bucket.Source, bucket.Tags = "openai-usage", ledger.Tags{}
	bucket.DeriveID()
	sum := summaryOf(scopeOver([]ledger.Event{bucket}), 0)
	if !near(sum.TotalUSD, 0.30) || len(sum.ByRun) != 0 {
		t.Fatalf("total=%.4f runs=%+v", sum.TotalUSD, sum.ByRun)
	}
}

// The dashboard says which state the total is in and explains the gap line.
func TestDashboardShowsTheTotalStateAndTheGapLine(t *testing.T) {
	s := New([]ledger.Event{taggedCall("research", "planner", "research-42", 0.10, 1), providerCostRow("openai", 0.13, 0)})
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	body := rec.Body.String()
	for _, want := range []string{"provider-reported</span> state", "team and agent figures", "(billed, not metered)", "never spread across teams or agents", "research"} {
		if !strings.Contains(body, want) {
			t.Fatalf("dashboard should contain %q", want)
		}
	}
}

// pricedUsageEvent is what the gateway records: token usage priced from the
// table — an ESTIMATED figure with tokens behind it.
func pricedUsageEvent(cost float64, seq int) ledger.Event {
	e := rollEvent("run-1", "planner", "200", cost, false, seq)
	e.Usage = ledger.Usage{InputTokens: 4000, OutputTokens: 2000, Requests: 1}
	return e
}

// providerCostRow is what a provider's own cost report contributes: a
// provider-reported dollar figure for a month, with no token usage.
func providerCostRow(provider string, cost float64, seq int) ledger.Event {
	e := ledger.Event{
		Source: provider + "-costs", Provider: provider,
		Dimensions: map[string]string{"line_item": "gpt-4o", "row": string(rune('a' + seq))},
		CostUSD:    cost, Confidence: ledger.ProviderReported,
		StartsAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), EndsAt: time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC),
	}
	e.DeriveID()
	return e
}
