package console

import (
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
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

// Spend rolls up per API key too, refusals included, from the fingerprint the
// gateway recorded — never the key.
func TestSpendByKeyRollsUpCallsAndRefusals(t *testing.T) {
	base := seed.Generate(seed.Options{Events: 6, Seed: 12})
	var evs []ledger.Event
	for i, e := range base {
		e.Dimensions = map[string]string{"status": "200", "request_id": "k" + strconv.Itoa(i), "key": "k-3fa9c2b1e0d4-7788"}
		if i == 0 {
			e.Dimensions["key_name"] = "ci-runner" // a label the caller gave; the fingerprint stays the identity
		}
		if i == 5 { // a refusal on the same key, on a call with no run
			e.Dimensions["status"], e.Dimensions["blocked"] = "429", "key …7788 reached its daily spend cap of $1.00 (resets at midnight UTC)"
			e.CostUSD = 0
			e.Usage = ledger.Usage{}
			e.Tags.Run = ""
		}
		e.DeriveID()
		evs = append(evs, e)
	}
	srv := httptest.NewServer(New(evs))
	defer srv.Close()
	sum := getSummary(t, srv.URL, "/api/summary?days=all")
	if len(sum.ByKey) != 1 || sum.ByKey[0].Label != "ci-runner (…7788)" || sum.ByKey[0].Calls != 5 || sum.ByKey[0].Blocked != 1 || sum.ByKey[0].SpentUSD <= 0 {
		t.Fatalf("by_key = %+v", sum.ByKey)
	}
	if sum.LoopsBlocked != 0 || len(sum.BlockedRuns) != 0 {
		t.Fatalf("a key at its cap is not a runaway loop: blocked=%d runs=%+v", sum.LoopsBlocked, sum.BlockedRuns)
	}
	resp, err := http.Get(srv.URL + "/?days=all")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), "Spend by key") || !strings.Contains(string(raw), "key ci-runner (…7788)") || !strings.Contains(string(raw), "1</span> refused") || strings.Contains(string(raw), `href="/run/"`) {
		t.Fatal("dashboard should show the key with its refusal")
	}
	// No key dimension anywhere: the card stays away.
	plain := httptest.NewServer(New(base))
	defer plain.Close()
	resp2, err := http.Get(plain.URL + "/?days=all")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	raw2, _ := io.ReadAll(resp2.Body)
	if strings.Contains(string(raw2), "Spend by key") {
		t.Fatal("no key card without keyed rows")
	}
}

func TestShadowModeRollsUpWhatACapWouldHaveStopped(t *testing.T) {
	base := seed.Generate(seed.Options{Events: 6, Seed: 21})
	var evs []ledger.Event
	for i, e := range base {
		e.Dimensions = map[string]string{"status": "200", "request_id": "s" + strconv.Itoa(i)}
		e.Tags.Run = "run-shadow"
		e.CostUSD = 0.5
		if i >= 3 { // three calls past the run's cap, served in shadow mode
			e.Dimensions["would_refuse"] = "run reached the spend cap of $1.00"
		}
		if i == 5 { // one past a key's cap
			e.Dimensions["would_refuse"] = "key …7788 reached its daily spend cap of $1.00 (resets at midnight UTC)"
			e.Dimensions["key"] = "k-3fa9c2b1e0d4-7788"
		}
		e.DeriveID()
		evs = append(evs, e)
	}
	srv := httptest.NewServer(New(evs))
	defer srv.Close()
	sum := getSummary(t, srv.URL, "/api/summary?days=all")
	if sum.ShadowCalls != 3 || sum.ShadowSpendUSD < 1.49 || sum.ShadowSpendUSD > 1.51 || len(sum.ShadowLines) != 2 {
		t.Fatalf("shadow rollup = %d %.2f %+v", sum.ShadowCalls, sum.ShadowSpendUSD, sum.ShadowLines)
	}
	if sum.ShadowLines[0].Who != "run-shadow" || sum.ShadowLines[0].Calls != 2 || sum.ShadowLines[1].Who != "key …7788" || sum.ShadowLines[1].Kind != "key" {
		t.Fatalf("lines = %+v", sum.ShadowLines)
	}
	if sum.LoopsBlocked != 0 || len(sum.BlockedRuns) != 0 {
		t.Fatal("nothing was refused")
	}
	resp, err := http.Get(srv.URL + "/?days=all")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), "Shadow mode: what a cap would have stopped") || !strings.Contains(string(raw), "Would have been refused") || !strings.Contains(string(raw), "2 calls past the cap") {
		t.Fatal("dashboard should show the shadow card and chip")
	}
	plain := httptest.NewServer(New(base))
	defer plain.Close()
	resp2, err := http.Get(plain.URL + "/?days=all")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	raw2, _ := io.ReadAll(resp2.Body)
	if strings.Contains(string(raw2), "Shadow mode") {
		t.Fatal("no shadow card without marked rows")
	}
	// The run page the card links to tells the same story: which calls a cap
	// would have refused, where it would have tripped, and what they cost.
	resp3, err := http.Get(srv.URL + "/run/run-shadow")
	if err != nil {
		t.Fatal(err)
	}
	defer resp3.Body.Close()
	page, _ := io.ReadAll(resp3.Body)
	for _, want := range []string{"would have been paused (shadow mode)", "CAP WOULD HAVE TRIPPED", "3 would have been refused", "Spent past the cap", "served, a cap would have refused it: run reached the spend cap of $1.00"} {
		if !strings.Contains(string(page), want) {
			t.Fatalf("run page missing %q", want)
		}
	}
	if strings.Contains(string(page), "Prevented spend") || strings.Contains(string(page), "paused by the gateway") {
		t.Fatal("a shadow-only run claims nothing it did not do")
	}
}
