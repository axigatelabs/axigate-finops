package console

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
	"github.com/axigatelabs/axigate-finops/internal/pricing"
)

// simRun builds one run: 5 served calls then 15 the gateway refused, exactly
// the shape the directive asks the drill-down to handle.
func simRun() ([]ledger.Event, float64) {
	day := time.Date(2026, 8, 15, 9, 0, 0, 0, time.UTC)
	var evs []ledger.Event
	var okCost float64
	mk := func(n int, blocked, flagged bool) ledger.Event {
		e := ledger.Event{
			Source: "gateway", Provider: "openai", Model: "gpt-4o-mini",
			Dimensions: map[string]string{"request_id": fmt.Sprintf("1700000000000000000-%d", n), "status": "200"},
			Tags:       ledger.Tags{Team: "platform", Agent: "planner", Run: "sim-run"},
			Confidence: ledger.Estimated, StartsAt: day, EndsAt: day,
		}
		if blocked {
			e.Dimensions["status"] = "429"
			e.Dimensions["blocked"] = "run reached the call cap of 5"
		} else {
			u := ledger.Usage{InputTokens: 1000, OutputTokens: 300, Requests: 1}
			e.Usage = u
			c := pricing.Price("openai", "gpt-4o-mini", pricing.Usage{InputTokens: u.InputTokens, OutputTokens: u.OutputTokens})
			e.CostUSD = c.USD
			e.PriceVersion = c.Version
			okCost += c.USD
		}
		if flagged {
			e.Dimensions["loop"] = "suspected"
			e.Dimensions["loop_signal"] = "repeat"
		}
		e.DeriveID()
		return e
	}
	n := 0
	for i := 0; i < 5; i++ {
		n++
		evs = append(evs, mk(n, false, i >= 3)) // last two served calls flagged as a loop
	}
	for i := 0; i < 15; i++ {
		n++
		evs = append(evs, mk(n, true, false))
	}
	return evs, okCost
}

func TestRunDetailTimelineAndPreventedSpendMath(t *testing.T) {
	evs, okCost := simRun()
	srv := New(evs)
	d := runDetail(srv.events, "sim-run")
	if !d.Found || d.Total != 20 || d.OK != 5 || d.BlockedN != 15 {
		t.Fatalf("counts: %+v", d)
	}
	if d.FlaggedN != 2 {
		t.Fatalf("flagged: %d want 2", d.FlaggedN)
	}
	// Prevented spend = the run's average served call x the number refused,
	// priced from the model's token rates.
	avg := okCost / 5
	want := avg * 15
	if math.Abs(d.AvoidedUSD-want) > 1e-9 {
		t.Fatalf("avoided %.6f want %.6f", d.AvoidedUSD, want)
	}
	if math.Abs(d.SpentUSD-okCost) > 1e-9 {
		t.Fatalf("spent %.6f want %.6f", d.SpentUSD, okCost)
	}
	// The timeline is in order and marks the first refused call as the trip point.
	firstBlock := -1
	for i, c := range d.Calls {
		if c.FirstBlock {
			firstBlock = i
		}
	}
	if firstBlock != 5 { // calls 1..5 served (index 0..4), index 5 is the first block
		t.Fatalf("first-block anchor at index %d, want 5", firstBlock)
	}
}

func TestRunPageRendersAndUnknownRunIs404(t *testing.T) {
	evs, _ := simRun()
	ts := httptest.NewServer(New(evs))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/run/sim-run")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	html := string(b)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	for _, want := range []string{"sim-run", "Prevented spend", "CAP TRIPPED", "Timeline", "blocked"} {
		if !strings.Contains(html, want) {
			t.Fatalf("run page missing %q", want)
		}
	}

	nf, _ := http.Get(ts.URL + "/run/does-not-exist")
	body, _ := io.ReadAll(nf.Body)
	nf.Body.Close()
	if nf.StatusCode != http.StatusNotFound || !strings.Contains(string(body), "Run not found") {
		t.Fatalf("unknown run should 404 with a message, got %d", nf.StatusCode)
	}
}

func TestDashboardRunRowsLinkToDetail(t *testing.T) {
	evs, _ := simRun()
	ts := httptest.NewServer(New(evs))
	defer ts.Close()
	resp, _ := http.Get(ts.URL + "/?days=all")
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), `href="/run/sim-run"`) {
		t.Fatal("the runaway card should link each run to its drill-down")
	}
}
