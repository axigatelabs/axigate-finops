package anthropic

import (
	"strings"
	"testing"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
)

// A synthetic export in the real column order. Values are invented; no real
// account data is used, per the repo's fixture rule.
const sampleCostCSV = `usage_date_utc,model,workspace,api_key,usage_type,context_window,token_type,cost_usd,list_price_usd,cost_type,inference_geo,speed,api_key_id,api_key_status
2026-08-04,Claude Haiku 4.5,Default,demo-key,message,≤ 200k,input_no_cache,0.05,0.05,token,not_available,,apikey_01AAA,archived
2026-08-04,Claude Haiku 4.5,Default,demo-key,message,≤ 200k,output,0.02,0.02,token,not_available,,apikey_01AAA,archived
2026-08-05,Claude Sonnet 4.5,Default,demo-key,message,≤ 200k,input_cache_read,0.10,1.00,token,not_available,,apikey_01AAA,archived
2026-08-05,Claude Opus 4.5,Default,other-key,message,≤ 200k,output,0.30,0.30,token,not_available,,apikey_01BBB,archived
`

func TestParseCostCSVReadsEveryRowAsAProviderReportedCostRow(t *testing.T) {
	p, err := ParseCostCSV(strings.NewReader(sampleCostCSV))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Events) != 4 {
		t.Fatalf("events: %d", len(p.Events))
	}
	var total float64
	for _, e := range p.Events {
		if e.Provider != "anthropic" || e.Source != SourceCostCSV {
			t.Fatalf("wrong source/provider: %+v", e)
		}
		if e.Confidence != ledger.ProviderReported {
			t.Fatalf("cost rows must be provider-reported, got %q", e.Confidence)
		}
		if e.Usage != (ledger.Usage{}) {
			t.Fatalf("a cost row carries no token counts: %+v", e.Usage)
		}
		if e.Period != "2026-08" {
			t.Fatalf("period: %q", e.Period)
		}
		if err := e.Validate(); err != nil {
			t.Fatalf("invalid event: %v", err)
		}
		total += e.CostUSD
	}
	if total < 0.469 || total > 0.471 {
		t.Fatalf("total cost: %.4f want 0.47", total)
	}
}

func TestParseCostCSVNormalizesModelAndKeepsTheJoinKey(t *testing.T) {
	p, err := ParseCostCSV(strings.NewReader(sampleCostCSV))
	if err != nil {
		t.Fatal(err)
	}
	if p.Events[0].Model != "claude-haiku-4-5" {
		t.Fatalf("model not normalized: %q", p.Events[0].Model)
	}
	if p.Events[2].Model != "claude-sonnet-4-5" || p.Events[3].Model != "claude-opus-4-5" {
		t.Fatalf("models: %q %q", p.Events[2].Model, p.Events[3].Model)
	}
	// api_key_id is the dimension an owner mapping joins on.
	if p.Events[0].Dimensions["api_key_id"] != "apikey_01AAA" {
		t.Fatalf("api_key_id dim: %q", p.Events[0].Dimensions["api_key_id"])
	}
	if p.Events[0].Dimensions["token_type"] != "input_no_cache" {
		t.Fatalf("token_type dim: %q", p.Events[0].Dimensions["token_type"])
	}
}

func TestParseCostCSVIsIdempotentPerRow(t *testing.T) {
	// Two rows that differ only by token_type must be two distinct ids;
	// re-importing the same file must reuse ids so a re-import never doubles.
	p1, _ := ParseCostCSV(strings.NewReader(sampleCostCSV))
	p2, _ := ParseCostCSV(strings.NewReader(sampleCostCSV))
	seen := map[string]bool{}
	for _, e := range p1.Events {
		if seen[e.ID] {
			t.Fatalf("two rows collided on id %s", e.ID)
		}
		seen[e.ID] = true
	}
	for i := range p2.Events {
		if p1.Events[i].ID != p2.Events[i].ID {
			t.Fatalf("re-import changed an id: %s vs %s", p1.Events[i].ID, p2.Events[i].ID)
		}
	}
}

func TestParseCostCSVRejectsAWrongFileNamingItsColumns(t *testing.T) {
	_, err := ParseCostCSV(strings.NewReader("foo,bar,baz\n1,2,3\n"))
	if err == nil || !strings.Contains(err.Error(), "usage_date_utc") || !strings.Contains(err.Error(), "foo, bar, baz") {
		t.Fatalf("a wrong CSV must be named, got: %v", err)
	}
}

func TestParseCostCSVRejectsBadCostAndDate(t *testing.T) {
	bad := "usage_date_utc,cost_usd\n2026-08-01,notmoney\n"
	if _, err := ParseCostCSV(strings.NewReader(bad)); err == nil || !strings.Contains(err.Error(), "cost_usd") {
		t.Fatalf("bad cost accepted: %v", err)
	}
	bad = "usage_date_utc,cost_usd\nAugust,0.10\n"
	if _, err := ParseCostCSV(strings.NewReader(bad)); err == nil || !strings.Contains(err.Error(), "usage_date_utc") {
		t.Fatalf("bad date accepted: %v", err)
	}
	neg := "usage_date_utc,cost_usd\n2026-08-01,-0.10\n"
	if _, err := ParseCostCSV(strings.NewReader(neg)); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("negative cost accepted: %v", err)
	}
}

func TestParseCostCSVMinimalColumnsAndBlankLines(t *testing.T) {
	// Only the two required columns, plus a blank line to skip.
	min := "usage_date_utc,cost_usd\n2026-08-01,0.25\n\n2026-08-02,0.75\n"
	p, err := ParseCostCSV(strings.NewReader(min))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Events) != 2 {
		t.Fatalf("events: %d", len(p.Events))
	}
	if p.Events[0].Model != "" {
		t.Fatalf("no model column should leave Model empty, got %q", p.Events[0].Model)
	}
}

func TestNormalizeModel(t *testing.T) {
	cases := map[string]string{
		"Claude Haiku 4.5":  "claude-haiku-4-5",
		"Claude Sonnet 4.5": "claude-sonnet-4-5",
		"Claude Opus 4.5":   "claude-opus-4-5",
		"  ":                "",
		"":                  "",
		"Already-Fine":      "already-fine",
	}
	for in, want := range cases {
		if got := normalizeModel(in); got != want {
			t.Errorf("normalizeModel(%q) = %q want %q", in, got, want)
		}
	}
}

func FuzzParseCostCSV(f *testing.F) {
	f.Add(sampleCostCSV)
	f.Add("usage_date_utc,cost_usd\n2026-08-01,0.10\n")
	f.Add("usage_date_utc,cost_usd\n")
	f.Add("not,a,report\n1,2,3\n")
	f.Add("")
	f.Fuzz(func(t *testing.T, s string) {
		// The parser must never panic and must never return events with a
		// malformed period when it returns without error.
		p, err := ParseCostCSV(strings.NewReader(s))
		if err != nil {
			return
		}
		for _, e := range p.Events {
			if verr := e.Validate(); verr != nil {
				t.Fatalf("parsed an invalid event from %q: %v", s, verr)
			}
		}
	})
}
