package openai

import (
	"strings"
	"testing"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
)

// Synthetic, shaped like the API reference example; no customer data.
const usagePage = `{
  "object": "page",
  "data": [
    {
      "object": "bucket",
      "start_time": 1755129600,
      "end_time": 1755216000,
      "results": [
        {
          "object": "organization.usage.completions.result",
          "input_tokens": 1000, "input_cached_tokens": 400, "input_cache_write_tokens": 100,
          "input_uncached_tokens": 500, "output_tokens": 500, "num_model_requests": 5,
          "project_id": "proj_alpha", "user_id": null, "api_key_id": "key_1", "model": "gpt-4o",
          "batch": false, "service_tier": "default"
        },
        {
          "object": "organization.usage.completions.result",
          "input_tokens": 200, "input_cached_tokens": 0, "output_tokens": 20, "num_model_requests": 1,
          "project_id": null, "user_id": null, "api_key_id": "key_2", "model": "gpt-99-turbo",
          "batch": null, "service_tier": null
        }
      ]
    }
  ],
  "has_more": true,
  "next_page": "page_AAAA"
}`

const costsPage = `{
  "object": "page",
  "data": [
    {
      "object": "bucket",
      "start_time": 1755129600,
      "end_time": 1755216000,
      "results": [
        { "object": "organization.costs.result", "amount": { "value": 12.5, "currency": "usd" }, "line_item": "gpt-4o-2024-08-06, input", "project_id": "proj_alpha", "api_key_id": null },
        { "object": "organization.costs.result", "amount": { "value": 0.75, "currency": "usd" }, "line_item": "Web search", "project_id": null, "api_key_id": null }
      ]
    }
  ],
  "has_more": false,
  "next_page": null
}`

func TestParseUsage(t *testing.T) {
	p, err := ParseUsage(strings.NewReader(usagePage))
	if err != nil {
		t.Fatal(err)
	}
	if !p.HasMore || p.NextPage != "page_AAAA" || len(p.Events) != 2 {
		t.Fatalf("page: %+v", p)
	}
	e := p.Events[0]
	if e.Source != SourceUsage || e.Provider != "openai" || e.Model != "gpt-4o" {
		t.Fatalf("identity: %+v", e)
	}
	if e.Usage != (ledger.Usage{InputTokens: 1000, CacheRead: 400, CacheWrite5m: 100, OutputTokens: 500, Requests: 5}) {
		t.Fatalf("usage: %+v", e.Usage)
	}
	if e.Dimensions["project_id"] != "proj_alpha" || e.Dimensions["api_key_id"] != "key_1" || e.Dimensions["service_tier"] != "default" {
		t.Fatalf("dimensions: %+v", e.Dimensions)
	}
	if _, has := e.Dimensions["user_id"]; has {
		t.Fatal("null user_id must not become a dimension")
	}
	if e.Confidence != ledger.Estimated || e.PriceVersion == "" || e.CostUSD <= 0 {
		t.Fatalf("pricing: %+v", e)
	}
	// input 1000 includes 400 cached + 100 written: 500 uncached at full, 400 at 0.5x, 100 at 1x, 500 out.
	want := 500*0.0000025 + 400*0.0000025*0.5 + 100*0.0000025 + 500*0.000010
	if diff := e.CostUSD - want; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("cost: got %.9f want %.9f", e.CostUSD, want)
	}
	if e.Period != "2025-08" || e.StartsAt.Format("2006-01-02") != "2025-08-14" {
		t.Fatalf("bucket/period: %s %s", e.StartsAt, e.Period)
	}
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
	// The unlisted model is flagged, not silently priced as if known.
	u := p.Events[1]
	if u.Dimensions["unpriced_model"] != "true" {
		t.Fatalf("unpriced model not flagged: %+v", u.Dimensions)
	}
	if _, has := u.Dimensions["project_id"]; has {
		t.Fatal("null project_id must not become a dimension")
	}
}

func TestParseUsageIsIdempotent(t *testing.T) {
	a, _ := ParseUsage(strings.NewReader(usagePage))
	b, _ := ParseUsage(strings.NewReader(usagePage))
	for i := range a.Events {
		if a.Events[i].ID != b.Events[i].ID {
			t.Fatal("re-parsing the same page produced different ids")
		}
	}
	if a.Events[0].ID == a.Events[1].ID {
		t.Fatal("two rows collided on one id")
	}
}

func TestParseCosts(t *testing.T) {
	p, err := ParseCosts(strings.NewReader(costsPage))
	if err != nil {
		t.Fatal(err)
	}
	if p.HasMore || len(p.Events) != 2 {
		t.Fatalf("page: %+v", p)
	}
	e := p.Events[0]
	if e.Source != SourceCosts || e.CostUSD != 12.5 || e.Confidence != ledger.ProviderReported {
		t.Fatalf("cost row: %+v", e)
	}
	if e.Model != "gpt-4o-2024-08-06" || e.Dimensions["line_item"] != "gpt-4o-2024-08-06, input" || e.Dimensions["project_id"] != "proj_alpha" {
		t.Fatalf("line item: %+v", e)
	}
	if p.Events[1].Model != "" || p.Events[1].CostUSD != 0.75 {
		t.Fatalf("non-model line item: %+v", p.Events[1])
	}
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestParseRejectsWrongShapeAndCurrency(t *testing.T) {
	if _, err := ParseUsage(strings.NewReader(`{"object":"list","data":[]}`)); err == nil {
		t.Fatal("a non-page object must be rejected")
	}
	if _, err := ParseUsage(strings.NewReader(`not json`)); err == nil {
		t.Fatal("garbage must be rejected")
	}
	eur := strings.Replace(costsPage, `"currency": "usd"`, `"currency": "eur"`, 1)
	if _, err := ParseCosts(strings.NewReader(eur)); err == nil {
		t.Fatal("a non-usd amount must be rejected rather than mispriced")
	}
}

func TestModelFromLineItem(t *testing.T) {
	cases := map[string]string{
		"gpt-4o-2024-08-06, input":     "gpt-4o-2024-08-06",
		"gpt-4o-mini, output (cached)": "gpt-4o-mini",
		"Web search":                   "",
		"text-embedding-3-small":       "text-embedding-3-small",
		"":                             "",
		"Image generation, 1024x1024":  "",
	}
	for in, want := range cases {
		if got := modelFromLineItem(in); got != want {
			t.Errorf("%q: got %q want %q", in, got, want)
		}
	}
}
