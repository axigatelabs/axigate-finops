package report

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
	"github.com/axigatelabs/axigate-finops/internal/pricing"
)

func day(d int) time.Time { return time.Date(2025, 8, d, 0, 0, 0, 0, time.UTC) }

func usage(provider, model, key string, d int, u ledger.Usage, tags ledger.Tags, usd float64) ledger.Event {
	e := ledger.Event{
		Source: provider + "-usage", Provider: provider, Model: model,
		Dimensions: map[string]string{"api_key_id": key}, Usage: u, Tags: tags,
		CostUSD: usd, Confidence: ledger.Estimated, PriceVersion: "t", StartsAt: day(d), EndsAt: day(d + 1),
	}
	e.DeriveID()
	return e
}

func cost(provider, model string, d int, usd float64) ledger.Event {
	e := ledger.Event{
		Source: provider + "-costs", Provider: provider, Model: model,
		Dimensions: map[string]string{"line_item": model + ", input"},
		CostUSD:    usd, Confidence: ledger.ProviderReported, StartsAt: day(d), EndsAt: day(d + 1),
	}
	e.DeriveID()
	return e
}

func TestTotalsPreferCostRowsAndUnknownIsItsOwnLine(t *testing.T) {
	cs := ledger.Tags{Team: "customer-success", Agent: "support-bot"}
	events := []ledger.Event{
		usage("openai", "gpt-4o", "key_1", 1, ledger.Usage{InputTokens: 1000, CacheRead: 600}, cs, 3.0),
		usage("openai", "gpt-4o", "key_2", 1, ledger.Usage{InputTokens: 1000}, ledger.Tags{}, 2.0), // unmapped
		cost("openai", "gpt-4o", 1, 12.5), // the provider's own number
		usage("anthropic", "claude-sonnet-4-5", "key_3", 1, ledger.Usage{InputTokens: 500, CacheRead: 50}, ledger.Tags{Team: "eng"}, 4.0),
	}
	r := Build(events)
	// openai has cost rows, so its usage rows are detail only: total = 12.5 + 4.0.
	if r.TotalUSD != 16.5 {
		t.Fatalf("total: %.2f", r.TotalUSD)
	}
	if len(r.ByProvider) != 2 || r.ByProvider[0].Key != "openai" || r.ByProvider[0].USD != 12.5 || r.ByProvider[0].Confidence != ledger.ProviderReported {
		t.Fatalf("by provider: %+v", r.ByProvider)
	}
	// The openai cost row has no tags, so its 12.5 is unknown; the anthropic usage row is eng.
	var unknown, eng float64
	for _, l := range r.ByOwner {
		switch l.Key {
		case "unknown":
			unknown = l.USD
		case "eng":
			eng = l.USD
		}
	}
	if unknown != 12.5 || eng != 4.0 || r.UnknownUSD != 12.5 {
		t.Fatalf("owners: %+v unknown=%.2f", r.ByOwner, r.UnknownUSD)
	}
	if len(r.Notes) != 1 || !strings.Contains(r.Notes[0], "openai") {
		t.Fatalf("notes: %+v", r.Notes)
	}
	// Cache picture comes from usage rows regardless.
	if len(r.Cache) != 2 || r.Cache[1].Provider != "openai" {
		t.Fatalf("cache: %+v", r.Cache)
	}
	oa := r.Cache[1]
	// openai inclusive: 2000 input of which 600 cached → 600 / 2000.
	if oa.CacheRead != 600 || oa.CachedShare < 0.29 || oa.CachedShare > 0.31 {
		t.Fatalf("openai cache share: %+v", oa)
	}
	if oa.ReadSavingsUSD <= 0 || oa.UncachedUSD <= 0 {
		t.Fatalf("openai cache dollars: %+v", oa)
	}
	// Reconciliation: openai August estimated 5.0 vs reported 12.5.
	if len(r.Reconciliations) != 1 || r.Reconciliations[0].EstimatedUSD != 5.0 || r.Reconciliations[0].ReportedUSD != 12.5 {
		t.Fatalf("reconciliation: %+v", r.Reconciliations)
	}
}

func TestSpikesNeedAMedianAndAMargin(t *testing.T) {
	var events []ledger.Event
	for d := 1; d <= 10; d++ {
		usd := 10.0
		if d == 7 {
			usd = 200.0 // 20x the median, well over the $25 margin
		}
		events = append(events, usage("openai", "gpt-4o", "key_1", d, ledger.Usage{InputTokens: 10}, ledger.Tags{Team: "t"}, usd))
	}
	// A second key that is noisy but small never spikes: 4x of a $1 median is under the margin.
	for d := 1; d <= 6; d++ {
		usd := 1.0
		if d == 3 {
			usd = 5.0
		}
		events = append(events, usage("openai", "gpt-4o", "key_2", d, ledger.Usage{InputTokens: 10}, ledger.Tags{Team: "t"}, usd))
	}
	r := Build(events)
	if len(r.Spikes) != 1 || r.Spikes[0].Day != "2025-08-07" || r.Spikes[0].Value != "key_1" || r.Spikes[0].Factor < 19 {
		t.Fatalf("spikes: %+v", r.Spikes)
	}
}

func TestSpikesComeFromUsageRowsEvenWhenCostRowsDriveTotals(t *testing.T) {
	var events []ledger.Event
	for d := 1; d <= 10; d++ {
		usd := 10.0
		if d == 5 {
			usd = 400.0
		}
		events = append(events, usage("openai", "gpt-4o", "key_1", d, ledger.Usage{InputTokens: 10}, ledger.Tags{Team: "t"}, usd))
		// Flat cost rows for the same provider: the totals come from these, the spike must not.
		events = append(events, cost("openai", "gpt-4o", d, 50.0))
	}
	r := Build(events)
	if r.TotalUSD != 500 {
		t.Fatalf("totals must come from the cost rows: %.2f", r.TotalUSD)
	}
	if len(r.Spikes) != 1 || r.Spikes[0].Value != "key_1" || r.Spikes[0].Day != "2025-08-05" {
		t.Fatalf("spike must come from the usage rows: %+v", r.Spikes)
	}
}

func TestTextNamesSourcesAndLimits(t *testing.T) {
	r := Build([]ledger.Event{usage("anthropic", "claude-sonnet-4-5", "k", 1, ledger.Usage{InputTokens: 100}, ledger.Tags{}, 1)})
	out := Text(r)
	for _, want := range []string{"sources: anthropic-usage", "By owner", "unknown", "Not visible in provider reports", "upper bound"} {
		if !strings.Contains(out, want) {
			t.Fatalf("text missing %q:\n%s", want, out)
		}
	}
}

func TestUnlistedModelsAreNamedNotHidden(t *testing.T) {
	price := func(model string) float64 {
		return pricing.Price("openai", model, pricing.Usage{InputTokens: 1000, OutputTokens: 1000}).USD
	}
	tokens := ledger.Usage{InputTokens: 1000, OutputTokens: 1000, Requests: 1}
	known := usage("openai", "gpt-4o-mini-2024-07-18", "key_a", 1, tokens, ledger.Tags{}, price("gpt-4o-mini-2024-07-18"))
	guess := usage("openai", "gpt-5.5-2026-04-23", "key_a", 1, tokens, ledger.Tags{}, price("gpt-5.5-2026-04-23"))
	r := Build([]ledger.Event{known, guess})
	if len(r.Unlisted) != 1 || r.Unlisted[0].Key != "openai/gpt-5.5-2026-04-23" || r.Unlisted[0].Events != 1 {
		t.Fatalf("unlisted: %+v", r.Unlisted)
	}
	if math.Abs(r.Unlisted[0].USD-2000*0.000003) > 1e-9 {
		t.Fatalf("fallback dollars: %v", r.Unlisted[0].USD)
	}
	txt := Text(r)
	for _, want := range []string{"Not in price table " + pricing.Version, "fallback", "openai/gpt-5.5-2026-04-23"} {
		if !strings.Contains(txt, want) {
			t.Fatalf("text lacks %q:\n%s", want, txt)
		}
	}
	if section := txt[strings.Index(txt, "Not in price table"):]; strings.Contains(section, "gpt-4o-mini") {
		t.Fatalf("a listed snapshot must not be called unlisted:\n%s", section)
	}
	// Nothing to say when every model is listed.
	if r := Build([]ledger.Event{known}); len(r.Unlisted) != 0 || strings.Contains(Text(r), "Not in price table") {
		t.Fatalf("listed-only report mentions the fallback: %+v", r.Unlisted)
	}
}
