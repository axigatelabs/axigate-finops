package console

import (
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
	"github.com/axigatelabs/axigate-finops/internal/report"
	"github.com/axigatelabs/axigate-finops/internal/seed"
)

func cents(v float64) int64 { return int64(math.Round(v * 100)) }

func getSummary(t *testing.T, base, path string) Summary {
	t.Helper()
	resp, err := http.Get(base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var s Summary
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSummaryMathMatchesTheReportAllTime(t *testing.T) {
	events := seed.Generate(seed.Options{Events: 5000, Seed: 5})
	srv := httptest.NewServer(New(events))
	defer srv.Close()
	s := getSummary(t, srv.URL, "/api/summary?days=all")
	if cents(s.TotalUSD) != cents(report.Build(events).TotalUSD) {
		t.Fatalf("summary total %.4f != report total %.4f", s.TotalUSD, report.Build(events).TotalUSD)
	}
	// by-team dollars sum to the total (every event has a team in the seed).
	var teamSum float64
	for _, l := range s.ByTeam {
		teamSum += l.USD
	}
	if cents(teamSum) != cents(s.TotalUSD) {
		t.Fatalf("by-team %.4f != total %.4f", teamSum, s.TotalUSD)
	}
	if s.LoopsBlocked == 0 || len(s.BlockedRuns) == 0 {
		t.Fatal("the seed's runaway loop should show blocked calls")
	}
}

// appendEventsJSONL writes events as JSON lines, creating or appending, the way
// the gateway records them.
func appendEventsJSONL(t *testing.T, path string, evs []ledger.Event) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, e := range evs {
		if err := enc.Encode(e); err != nil {
			t.Fatal(err)
		}
	}
}

// TestConsoleLiveReloadsLedgerFile proves the dashboard reflects events the
// gateway appends after the console started — no restart — which is what makes
// the one-container "run your agent, watch spend appear" moment work.
func TestConsoleLiveReloadsLedgerFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	first := seed.Generate(seed.Options{Events: 50, Seed: 1})
	appendEventsJSONL(t, path, first)

	srv := New(nil) // no fixed events; live-reads the file below
	srv.SetLedgerFile(path)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	s1 := getSummary(t, ts.URL, "/api/summary?days=all")
	if s1.Events != len(first) {
		t.Fatalf("live console should show %d events, got %d", len(first), s1.Events)
	}
	// Append more, as the gateway would while the console keeps running.
	appendEventsJSONL(t, path, seed.Generate(seed.Options{Events: 30, Seed: 2}))
	s2 := getSummary(t, ts.URL, "/api/summary?days=all")
	if s2.Events <= s1.Events {
		t.Fatalf("live console must pick up appended events without a restart: before=%d after=%d", s1.Events, s2.Events)
	}
}

// TestConsoleIsUnrestricted proves the self-hosted console has no paywall:
// all-time history is not clamped and the FOCUS export is served, out of the box.
func TestConsoleIsUnrestricted(t *testing.T) {
	events := seed.Generate(seed.Options{Events: 5000, Seed: 5})
	srv := httptest.NewServer(New(events))
	defer srv.Close()

	s := getSummary(t, srv.URL, "/api/summary?days=all")
	if s.Days != 0 || s.Events != len(events) {
		t.Fatalf("all-time must show every event, unclamped: days=%d events=%d want events=%d", s.Days, s.Events, len(events))
	}
	resp, err := http.Get(srv.URL + "/api/export/focus?days=all")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(resp.Header.Get("Content-Type"), "csv") {
		t.Fatalf("FOCUS export should be 200 CSV, got %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
}

func TestDashboardBootsAndRendersTheHeadlineNumbers(t *testing.T) {
	events := seed.Generate(seed.Options{Events: 3000, Seed: 9})
	srv := httptest.NewServer(New(events))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("dashboard status %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	html := string(raw)
	for _, want := range []string{"AxiGate", "Total spend", "Runaway", "Spend by team", "FOCUS", `every figure is the <span class="num">estimated</span> state`} {
		if !strings.Contains(html, want) {
			t.Fatalf("dashboard missing %q", want)
		}
	}
	// The common case — a ledger with no provider bill — must not show the
	// gap line, and its total must read as the estimated state.
	for _, never := range []string{"(billed, not metered)", "provider-reported"} {
		if strings.Contains(html, never) {
			t.Fatalf("dashboard over a bill-free ledger must not mention %q", never)
		}
	}
	if sum := getSummary(t, srv.URL, "/api/summary"); sum.TotalState != "estimated" || sum.UnmeteredUSD != 0 {
		t.Fatalf("bill-free ledger: total_state=%q unmetered=%.4f, want estimated / 0", sum.TotalState, sum.UnmeteredUSD)
	}
}

func TestBlockedRunsShowAgentModelAndAvoidedSpend(t *testing.T) {
	events := seed.Generate(seed.Options{Events: 5000, Seed: 5})
	srv := New(events)
	s := summaryOf(scope{evs: srv.events, earliest: srv.earliest, latest: srv.latest}, 0) // all time
	if len(s.BlockedRuns) == 0 {
		t.Fatal("the seed's runaway loop should produce a blocked run")
	}
	top := s.BlockedRuns[0]
	if top.Blocked == 0 || top.AvoidedUSD <= 0 {
		t.Fatalf("blocked run should carry a count and estimated avoided spend: %+v", top)
	}
	if top.Model == "" {
		t.Fatalf("blocked run should carry the model: %+v", top)
	}
	if s.AvoidedUSD <= 0 {
		t.Fatalf("summary should total the avoided spend: %v", s.AvoidedUSD)
	}
	// The estimate is the run's average successful call times the blocked count,
	// so it must be positive and finite, not a guess pulled from thin air.
	if s.AvoidedUSD < top.AvoidedUSD {
		t.Fatalf("total avoided (%.4f) must be >= the top run's (%.4f)", s.AvoidedUSD, top.AvoidedUSD)
	}
}

// A served call with no usage behind it is named, never mistaken for a free one.
func TestUnknownCostCallsAreNamedNotFree(t *testing.T) {
	known := seed.Generate(seed.Options{Events: 20, Seed: 4})
	unknown := known[0]
	unknown.Usage = ledger.Usage{Requests: 1}
	unknown.CostUSD = 0
	unknown.Dimensions = map[string]string{"status": "200", "usage": "unknown", "request_id": "u-1"}
	unknown.DeriveID()
	srv := httptest.NewServer(New(append(known, unknown)))
	defer srv.Close()
	sum := getSummary(t, srv.URL, "/api/summary?days=all")
	if sum.UnknownCostCalls != 1 {
		t.Fatalf("unknown_cost_calls = %d, want 1", sum.UnknownCostCalls)
	}
	resp, err := http.Get(srv.URL + "/?days=all")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), "came back without usage") || !strings.Contains(string(raw), "not as free") {
		t.Fatal("dashboard should name the unknown-cost call")
	}
	// Without such a call the footnote stays away.
	plain := httptest.NewServer(New(known))
	defer plain.Close()
	resp2, err := http.Get(plain.URL + "/?days=all")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	raw2, _ := io.ReadAll(resp2.Body)
	if strings.Contains(string(raw2), "came back without usage") {
		t.Fatal("no unknown-cost footnote without such a call")
	}
}
