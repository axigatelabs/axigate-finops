package anthropic

import (
	"strings"
	"testing"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
)

// Synthetic, shaped like the API reference example; no customer data.
const usagePage = `{
  "data": [
    {
      "starting_at": "2025-08-01T00:00:00Z",
      "ending_at": "2025-08-02T00:00:00Z",
      "results": [
        {
          "account_id": null,
          "api_key_id": "apikey_01Rj2N8SVvo6BePZj99NhmiT",
          "cache_creation": { "ephemeral_1h_input_tokens": 1000, "ephemeral_5m_input_tokens": 500 },
          "cache_read_input_tokens": 200,
          "context_window": "0-200k",
          "inference_geo": "global",
          "model": "claude-sonnet-4-5",
          "output_tokens": 500,
          "server_tool_use": { "web_search_requests": 10 },
          "service_account_id": null,
          "service_tier": "standard",
          "uncached_input_tokens": 1500,
          "workspace_id": "wrkspc_01JwQvzr7rXLA5AGx3HKfFUJ"
        }
      ]
    }
  ],
  "has_more": true,
  "next_page": "page_MjAyNS0wNS0xNFQwMDowMDowMFo="
}`

const costPage = `{
  "data": [
    {
      "starting_at": "2025-08-01T00:00:00Z",
      "ending_at": "2025-08-02T00:00:00Z",
      "results": [
        {
          "amount": "123.78912", "context_window": "0-200k", "cost_type": "tokens", "currency": "USD",
          "description": "Claude Sonnet 4.5 Usage - Input Tokens", "inference_geo": "global",
          "model": "claude-sonnet-4-5", "service_tier": "standard", "token_type": "uncached_input_tokens",
          "workspace_id": "wrkspc_01JwQvzr7rXLA5AGx3HKfFUJ"
        },
        {
          "amount": "50", "context_window": null, "cost_type": "web_search", "currency": "USD",
          "description": "Web Search Usage", "inference_geo": null, "model": null, "service_tier": null,
          "token_type": null, "workspace_id": null
        }
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
	if !p.HasMore || !strings.HasPrefix(p.NextPage, "page_") || len(p.Events) != 1 {
		t.Fatalf("page: %+v", p)
	}
	e := p.Events[0]
	if e.Source != SourceUsage || e.Provider != "anthropic" || e.Model != "claude-sonnet-4-5" {
		t.Fatalf("identity: %+v", e)
	}
	if e.Usage != (ledger.Usage{InputTokens: 1500, CacheRead: 200, CacheWrite5m: 500, CacheWrite1h: 1000, OutputTokens: 500}) {
		t.Fatalf("usage: %+v", e.Usage)
	}
	if e.Dimensions["workspace_id"] == "" || e.Dimensions["api_key_id"] == "" || e.Dimensions["web_search_requests"] != "10" {
		t.Fatalf("dimensions: %+v", e.Dimensions)
	}
	if _, has := e.Dimensions["account_id"]; has {
		t.Fatal("null account_id must not become a dimension")
	}
	// Exclusive semantics: 1500 uncached at full, 200 reads at 0.1x, 500 5m writes at 1.25x,
	// 1000 1h writes at 2x, 500 out — on claude-sonnet-4-5 ($3 / $15 per million).
	want := 1500*0.000003 + 200*0.000003*0.1 + 500*0.000003*1.25 + 1000*0.000003*2.0 + 500*0.000015
	if diff := e.CostUSD - want; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("cost: got %.9f want %.9f", e.CostUSD, want)
	}
	if e.Confidence != ledger.Estimated || e.Period != "2025-08" {
		t.Fatalf("state/period: %+v", e)
	}
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestParseCosts(t *testing.T) {
	p, err := ParseCosts(strings.NewReader(costPage))
	if err != nil {
		t.Fatal(err)
	}
	if p.HasMore || p.NextPage != "" || len(p.Events) != 2 {
		t.Fatalf("page: %+v", p)
	}
	e := p.Events[0]
	// "123.78912" cents is $1.2378912.
	if diff := e.CostUSD - 1.2378912; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("amount: got %.9f", e.CostUSD)
	}
	if e.Source != SourceCosts || e.Confidence != ledger.ProviderReported || e.Model != "claude-sonnet-4-5" {
		t.Fatalf("cost row: %+v", e)
	}
	if e.Dimensions["token_type"] != "uncached_input_tokens" || e.Dimensions["description"] == "" {
		t.Fatalf("dimensions: %+v", e.Dimensions)
	}
	w := p.Events[1]
	if w.CostUSD != 0.5 || w.Model != "" || w.Dimensions["cost_type"] != "web_search" {
		t.Fatalf("web search row: %+v", w)
	}
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestParseIsIdempotentAndDistinct(t *testing.T) {
	a, _ := ParseCosts(strings.NewReader(costPage))
	b, _ := ParseCosts(strings.NewReader(costPage))
	if a.Events[0].ID != b.Events[0].ID || a.Events[0].ID == a.Events[1].ID {
		t.Fatal("ids must be stable across parses and distinct across rows")
	}
}

func TestParseRejectsBadInput(t *testing.T) {
	if _, err := ParseUsage(strings.NewReader(`{"has_more": false}`)); err == nil {
		t.Fatal("a report with no data field must be rejected")
	}
	if _, err := ParseUsage(strings.NewReader(`{"data":[{"starting_at":"yesterday","ending_at":"today","results":[]}]}`)); err == nil {
		t.Fatal("a bad timestamp must be rejected")
	}
	eur := strings.Replace(costPage, `"currency": "USD"`, `"currency": "EUR"`, 1)
	if _, err := ParseCosts(strings.NewReader(eur)); err == nil {
		t.Fatal("a non-USD amount must be rejected rather than mispriced")
	}
	bad := strings.Replace(costPage, `"amount": "50"`, `"amount": "fifty"`, 1)
	if _, err := ParseCosts(strings.NewReader(bad)); err == nil {
		t.Fatal("a non-numeric amount must be rejected")
	}
}
