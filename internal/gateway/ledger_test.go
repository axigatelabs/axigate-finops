package gateway

import (
	"bytes"
	"strings"
	"testing"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
)

func TestLedgerRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	rec := NewJSONLRecorder(&buf)
	e := ledger.Event{
		Source: SourceGateway, Provider: "anthropic", Model: "claude-haiku-4-5",
		Dimensions: map[string]string{"request_id": "1-1", "status": "200"},
		Usage:      ledger.Usage{InputTokens: 100, OutputTokens: 20, Requests: 1},
		Tags:       ledger.Tags{Team: "platform", Agent: "bot", Run: "r1"},
		CostUSD:    0.001, Confidence: ledger.Estimated, PriceVersion: "v",
	}
	e.DeriveID()
	rec.Record(e)
	rec.Record(e) // a duplicate line: same id, so analyze will dedupe it

	got, err := ParseLedger(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != e.ID || got[0].Tags.Run != "r1" || got[0].CostUSD != 0.001 {
		t.Fatalf("round-trip: %+v", got)
	}
}

func TestParseLedgerSkipsBlanksAndRejectsGarbage(t *testing.T) {
	good := `{"ID":"abc","Source":"gateway","Provider":"openai","Model":"gpt-4o","Dimensions":{"request_id":"1"},"Usage":{"Requests":1},"Confidence":"estimated","StartsAt":"2026-08-01T00:00:00Z","EndsAt":"2026-08-01T00:00:00Z","Period":"2026-08"}`
	if evs, err := ParseLedger(strings.NewReader("\n" + good + "\n\n")); err != nil || len(evs) != 1 {
		t.Fatalf("blanks: %d %v", len(evs), err)
	}
	if _, err := ParseLedger(strings.NewReader("{not json}\n")); err == nil {
		t.Fatal("garbage line should error")
	}
}
