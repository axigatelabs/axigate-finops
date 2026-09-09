package anyfile

import (
	"strings"
	"testing"
)

func TestDetect(t *testing.T) {
	cases := map[string]Kind{
		`{"object":"page","data":[{"object":"bucket","start_time":1,"end_time":2,"results":[{"object":"organization.usage.completions.result","input_tokens":1}]}],"has_more":false}`:                                     OpenAIUsage,
		`{"object":"page","data":[{"object":"bucket","start_time":1,"end_time":2,"results":[{"object":"organization.costs.result","amount":{"value":1,"currency":"usd"}}]}]}`:                                             OpenAICosts,
		`{"data":[{"starting_at":"2025-08-01T00:00:00Z","ending_at":"2025-08-02T00:00:00Z","results":[{"uncached_input_tokens":5,"cache_read_input_tokens":0,"output_tokens":1,"cache_creation":{}}]}],"has_more":false}`: AnthropicUsage,
		`{"data":[{"starting_at":"2025-08-01T00:00:00Z","ending_at":"2025-08-02T00:00:00Z","results":[{"amount":"12.5","currency":"USD"}]}],"has_more":false}`:                                                            AnthropicCosts,
	}
	for body, want := range cases {
		got, err := Detect([]byte(body))
		if err != nil || got != want {
			t.Errorf("%s: got %q err %v want %q", body[:40], got, err, want)
		}
	}
	for _, bad := range []string{`not json`, `{"foo":1}`, `{"object":"page","data":[]}`, `{"data":[{"starting_at":"x","results":[]}]}`} {
		if _, err := Detect([]byte(bad)); err == nil {
			t.Errorf("%s: expected an error", bad)
		}
	}
}

func TestParseBytesRoutesToTheImporter(t *testing.T) {
	events, kind, err := ParseBytes([]byte(`{"data":[{"starting_at":"2025-08-01T00:00:00Z","ending_at":"2025-08-02T00:00:00Z","results":[{"amount":"250","currency":"USD","model":"claude-sonnet-4-5"}]}],"has_more":false}`))
	if err != nil || kind != AnthropicCosts || len(events) != 1 || events[0].CostUSD != 2.5 {
		t.Fatalf("kind %q err %v events %+v", kind, err, events)
	}
}

func TestDetectAnthropicCostCSV(t *testing.T) {
	csv := "usage_date_utc,model,cost_usd\n2026-08-01,Claude Haiku 4.5,0.10\n"
	if k, err := Detect([]byte(csv)); err != nil || k != AnthropicCostCSV {
		t.Fatalf("cost csv: %q %v", k, err)
	}
	// A leading BOM (Excel-saved) must not fool the sniffer.
	if k, err := Detect([]byte("\ufeff" + csv)); err != nil || k != AnthropicCostCSV {
		t.Fatalf("bom csv: %q %v", k, err)
	}
	evs, k, err := ParseBytes([]byte(csv))
	if err != nil || k != AnthropicCostCSV || len(evs) != 1 || evs[0].Provider != "anthropic" {
		t.Fatalf("parse: %d evs, kind %q, err %v", len(evs), k, err)
	}
	// A CSV that is not the cost export is named, not misread.
	if _, err := Detect([]byte("a,b,c\n1,2,3\n")); err == nil || !strings.Contains(err.Error(), "not a report this tool reads") {
		t.Fatalf("wrong csv: %v", err)
	}
	// JSON still routes to the JSON path.
	if _, err := Detect([]byte(`{"object":"page","data":[]}`)); err == nil {
		t.Fatal("an empty OpenAI page should be reported as unidentifiable, not CSV")
	}
}

func TestDetectGatewayLedger(t *testing.T) {
	line := `{"ID":"x","Source":"gateway","Provider":"anthropic","Model":"claude-haiku-4-5","Dimensions":{"request_id":"1"},"Usage":{"InputTokens":10,"Requests":1},"Tags":{"Agent":"bot","Run":"r1"},"CostUSD":0.001,"Confidence":"estimated","StartsAt":"2026-08-01T00:00:00Z","EndsAt":"2026-08-01T00:00:00Z","Period":"2026-08"}`
	jsonl := line + "\n" + line + "\n" // two lines; same id → dedup later
	if k, err := Detect([]byte(jsonl)); err != nil || k != GatewayLedger {
		t.Fatalf("detect: %q %v", k, err)
	}
	evs, k, err := ParseBytes([]byte(jsonl))
	if err != nil || k != GatewayLedger || len(evs) != 2 || evs[0].Tags.Agent != "bot" {
		t.Fatalf("parse: %d %q %v", len(evs), k, err)
	}
	// A single-document provider report must NOT be taken for a gateway ledger.
	if k, _ := Detect([]byte(`{"object":"page","data":[{"object":"bucket","start_time":1,"end_time":2,"results":[{"object":"organization.usage.completions.result","input_tokens":1}]}]}`)); k != OpenAIUsage {
		t.Fatalf("provider report misdetected as gateway: %q", k)
	}
}
