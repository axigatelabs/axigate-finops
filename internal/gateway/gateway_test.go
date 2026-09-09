package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
)

// TestForwardsLargeRequestBodyUnchanged guards the truncation fix: a request
// body far above the old 1 MiB response-buffer cap must reach the upstream in
// full, not silently cut.
func TestForwardsLargeRequestBodyUnchanged(t *testing.T) {
	var got int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = int64(len(b))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"gpt-4o","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer up.Close()
	gw := New(Config{Upstream: up.URL, Provider: "openai", Recorder: &capture{}, Now: fixedClock()})
	front := httptest.NewServer(gw)
	defer front.Close()

	body := bytes.Repeat([]byte("a"), 3<<20) // 3 MiB, well over the old cap
	resp, err := http.Post(front.URL+"/v1/chat/completions", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got != int64(len(body)) {
		t.Fatalf("upstream received %d bytes, want the full %d — request body was truncated", got, len(body))
	}
}

func TestParseMoneyRejectsNonFinite(t *testing.T) {
	for _, s := range []string{"Inf", "Infinity", "+Inf", "inf", "NaN", "-5", "abc", "", "0", "  "} {
		if v := parseMoney(s); v != 0 {
			t.Fatalf("parseMoney(%q) = %v, want 0 (a bad value must never set a live cap)", s, v)
		}
	}
	if v := parseMoney("5.00"); v != 5.0 {
		t.Fatalf("parseMoney(5.00) = %v, want 5", v)
	}
	if v := parseMoney("$2.50"); v != 2.5 {
		t.Fatalf("parseMoney($2.50) = %v, want 2.5", v)
	}
}

type capture struct {
	mu     sync.Mutex
	events []ledger.Event
}

func (c *capture) Record(e ledger.Event) { c.mu.Lock(); c.events = append(c.events, e); c.mu.Unlock() }

// count safely reports how many events have been recorded. record() runs in the
// server goroutine after the response is relayed, so a streamed test must wait
// for it rather than read the slice the instant the client's read returns.
func (c *capture) count() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.events) }

func fixedClock() func() time.Time {
	t := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	return func() time.Time { return t }
}

// front sets up a gateway in front of a fake upstream and returns a client
// request helper plus the capture recorder.
func front(t *testing.T, provider string, upstream http.HandlerFunc) (*httptest.Server, *capture) {
	t.Helper()
	up := httptest.NewServer(upstream)
	t.Cleanup(up.Close)
	rec := &capture{}
	gw := New(Config{Upstream: up.URL, Provider: provider, Recorder: rec, Now: fixedClock()})
	front := httptest.NewServer(gw)
	t.Cleanup(front.Close)
	return front, rec
}

func TestInlineMaxSpendHeaderCapsARunEndToEnd(t *testing.T) {
	// A fake OpenAI upstream that always reports large usage, so one call costs
	// well over any tiny cap regardless of the exact price table.
	const answer = `{"model":"gpt-4o","choices":[{"message":{"content":"ok"}}],` +
		`"usage":{"prompt_tokens":1000000,"completion_tokens":1000000}}`
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(answer))
	}))
	defer up.Close()
	rec := &capture{}
	gw := New(Config{Upstream: up.URL, Provider: "openai", Recorder: rec, Now: fixedClock(),
		Control: ControlPolicy{AllowRequestCaps: true, Now: fixedClock()}})
	front := httptest.NewServer(gw)
	defer front.Close()

	call := func() int {
		req, _ := http.NewRequest("POST", front.URL+"/v1/chat/completions",
			strings.NewReader(`{"model":"gpt-4o","messages":[]}`))
		req.Header.Set("X-AxiGate-Run", "run-1")
		req.Header.Set("X-AxiGate-Max-Spend", "0.01") // a 1-cent cap, set inline from "code"
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	// The first call is forwarded and its cost blows past the 1-cent cap; the
	// second call for the same run is refused before it reaches the provider.
	if code := call(); code != http.StatusOK {
		t.Fatalf("first call should be forwarded (200), got %d", code)
	}
	if code := call(); code != http.StatusTooManyRequests {
		t.Fatalf("second call should be refused by the inline cap (429), got %d", code)
	}
	// A different run, uncapped, is unaffected.
	req, _ := http.NewRequest("POST", front.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"gpt-4o","messages":[]}`))
	req.Header.Set("X-AxiGate-Run", "run-2") // no max-spend header
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("an uncapped run must not be refused, got %d", resp.StatusCode)
	}
}

func TestForwardsAnthropicUnchangedAndRecordsUsage(t *testing.T) {
	const answer = `{"id":"msg_1","model":"claude-sonnet-4-5","content":[{"type":"text","text":"hi"}],` +
		`"usage":{"input_tokens":1000,"output_tokens":200,"cache_read_input_tokens":500,` +
		`"cache_creation":{"ephemeral_5m_input_tokens":300,"ephemeral_1h_input_tokens":0}}}`
	var gotAuth, gotVersion, gotBody string
	front, rec := front(t, "anthropic", func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("x-api-key")
		gotVersion = r.Header.Get("anthropic-version")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		if r.URL.Path != "/v1/messages" {
			t.Errorf("path not forwarded: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(answer))
	})

	req, _ := http.NewRequest("POST", front.URL+"/v1/messages",
		strings.NewReader(`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"SECRET PROMPT"}]}`))
	req.Header.Set("x-api-key", "sk-ant-secret")
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("X-AxiGate-Team", "platform")
	req.Header.Set("X-AxiGate-Agent", "research-bot")
	req.Header.Set("X-AxiGate-Run", "run-42")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	// The client gets the provider's answer byte for byte, and the provider saw
	// the caller's key and request unchanged.
	if string(body) != answer {
		t.Fatalf("body altered:\n got %s\nwant %s", body, answer)
	}
	if gotAuth != "sk-ant-secret" || gotVersion != "2023-06-01" {
		t.Fatalf("auth headers not forwarded: %q %q", gotAuth, gotVersion)
	}
	if !strings.Contains(gotBody, "SECRET PROMPT") {
		t.Fatalf("request body not forwarded intact: %s", gotBody)
	}

	if len(rec.events) != 1 {
		t.Fatalf("events: %d", len(rec.events))
	}
	e := rec.events[0]
	if e.Source != SourceGateway || e.Provider != "anthropic" || e.Model != "claude-sonnet-4-5" {
		t.Fatalf("event id fields: %+v", e)
	}
	want := ledger.Usage{InputTokens: 1000, CacheRead: 500, CacheWrite5m: 300, OutputTokens: 200, Requests: 1}
	if e.Usage != want {
		t.Fatalf("usage: %+v want %+v", e.Usage, want)
	}
	if e.Tags.Team != "platform" || e.Tags.Agent != "research-bot" || e.Tags.Run != "run-42" {
		t.Fatalf("tags not taken from headers: %+v", e.Tags)
	}
	if e.CostUSD <= 0 || e.Confidence != ledger.Estimated || e.Period != "2026-08" {
		t.Fatalf("cost/confidence/period: %+v", e)
	}
	// Metadata only: the recorded event must not carry the prompt or the answer.
	blob, _ := json.Marshal(e)
	for _, leak := range []string{"SECRET PROMPT", "hi", "content"} {
		if strings.Contains(string(blob), leak) {
			t.Fatalf("event leaked message text %q: %s", leak, blob)
		}
	}
}

func TestForwardsOpenAIAndPricesInclusiveInput(t *testing.T) {
	const answer = `{"id":"chatcmpl-1","model":"gpt-4o-2024-08-06","choices":[],` +
		`"usage":{"prompt_tokens":1000,"completion_tokens":100,"prompt_tokens_details":{"cached_tokens":400}}}`
	front, rec := front(t, "openai", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer sk-x" {
			t.Errorf("auth not forwarded: %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(answer))
	})
	req, _ := http.NewRequest("POST", front.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"gpt-4o","messages":[]}`))
	req.Header.Set("Authorization", "Bearer sk-x")
	resp, _ := http.DefaultClient.Do(req)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if len(rec.events) != 1 {
		t.Fatalf("events: %d", len(rec.events))
	}
	e := rec.events[0]
	// Model comes from the response (the dated snapshot), not the request's family name.
	if e.Model != "gpt-4o-2024-08-06" {
		t.Fatalf("model: %q", e.Model)
	}
	want := ledger.Usage{InputTokens: 1000, CacheRead: 400, OutputTokens: 100, Requests: 1}
	if e.Usage != want {
		t.Fatalf("usage: %+v want %+v", e.Usage, want)
	}
}

type boomRecorder struct{}

func (boomRecorder) Record(ledger.Event) { panic("a recorder must never break the response path") }

func TestFailOpenWhenTheRecorderPanics(t *testing.T) {
	const answer = `{"model":"gpt-4o","usage":{"prompt_tokens":10,"completion_tokens":1}}`
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(answer))
	}))
	defer up.Close()
	gw := New(Config{Upstream: up.URL, Provider: "openai", Recorder: boomRecorder{}, Now: fixedClock()})
	fr := httptest.NewServer(gw)
	defer fr.Close()
	resp, err := http.Post(fr.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"gpt-4o"}`))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != answer {
		t.Fatalf("a panicking recorder changed the response: %s", body)
	}
}

func TestFailOpenWhenUsageCannotBeRead(t *testing.T) {
	// A streamed response: the client still gets every byte, and an event is
	// still recorded, marked usage-unknown rather than guessed.
	front, rec := front(t, "anthropic", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("event: message_start\ndata: {}\n\nevent: message_stop\ndata: {}\n\n"))
	})
	req, _ := http.NewRequest("POST", front.URL+"/v1/messages", strings.NewReader(`{"model":"claude-haiku-4-5"}`))
	resp, _ := http.DefaultClient.Do(req)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "message_stop") {
		t.Fatalf("stream not passed through: %s", body)
	}
	if len(rec.events) != 1 {
		t.Fatalf("events: %d", len(rec.events))
	}
	e := rec.events[0]
	if e.Dimensions["stream"] != "true" || e.Dimensions["usage"] != "unknown" {
		t.Fatalf("streamed event not marked: %+v", e.Dimensions)
	}
	if e.Model != "claude-haiku-4-5" { // fell back to the request's model
		t.Fatalf("model fallback: %q", e.Model)
	}
	if e.CostUSD != 0 {
		t.Fatalf("no usage means no invented cost, got %v", e.CostUSD)
	}
}

func TestUpstreamUnreachableFailsTheRequestWithoutRecordingCost(t *testing.T) {
	rec := &capture{}
	gw := New(Config{Upstream: "http://127.0.0.1:1", Provider: "openai", Recorder: rec, Now: fixedClock()})
	fr := httptest.NewServer(gw)
	defer fr.Close()
	resp, err := http.Post(fr.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"gpt-4o"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("want 502, got %d", resp.StatusCode)
	}
	// A call that never reached the provider billed nothing, so no cost event.
	if len(rec.events) != 0 {
		t.Fatalf("recorded an event for a call that never billed: %+v", rec.events)
	}
}

func TestLoopDetectionAnnotatesTheEventReportOnly(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"model":"gpt-4o","usage":{"prompt_tokens":10,"completion_tokens":1}}`))
	}))
	defer up.Close()
	rec := &capture{}
	gw := New(Config{
		Upstream: up.URL, Provider: "openai", Recorder: rec, Now: fixedClock(),
		Loop: LoopPolicy{Window: time.Minute, MaxPerRun: 3, Now: fixedClock()},
	})
	fr := httptest.NewServer(gw)
	defer fr.Close()
	send := func(run string) {
		req, _ := http.NewRequest("POST", fr.URL+"/v1/chat/completions", strings.NewReader(`{"model":"gpt-4o"}`))
		req.Header.Set("X-AxiGate-Run", run)
		resp, _ := http.DefaultClient.Do(req)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	for i := 0; i < 3; i++ {
		send("run-loop")
	}
	// A request in another run, and one with no run id, are unaffected.
	send("run-other")
	req, _ := http.NewRequest("POST", fr.URL+"/v1/chat/completions", strings.NewReader(`{"model":"gpt-4o"}`))
	resp, _ := http.DefaultClient.Do(req) // no X-AxiGate-Run
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if len(rec.events) != 5 {
		t.Fatalf("events: %d", len(rec.events))
	}
	// The third run-loop request crosses the threshold; the first two do not.
	if rec.events[0].Dimensions["loop"] != "" || rec.events[1].Dimensions["loop"] != "" {
		t.Fatalf("flagged too early: %+v %+v", rec.events[0].Dimensions, rec.events[1].Dimensions)
	}
	third := rec.events[2].Dimensions
	if third["loop"] != "suspected" || third["loop_signal"] != "rate" || third["loop_count"] != "3" {
		t.Fatalf("third request not flagged: %+v", third)
	}
	// The response is unaffected: report-only never blocks.
	if rec.events[2].Dimensions["status"] != "200" {
		t.Fatalf("a flagged request must still be served: %+v", third)
	}
	if rec.events[3].Dimensions["loop"] != "" { // run-other
		t.Fatalf("a different run must not be flagged: %+v", rec.events[3].Dimensions)
	}
	if rec.events[4].Dimensions["loop"] != "" { // no run id
		t.Fatalf("a request with no run id must never be flagged: %+v", rec.events[4].Dimensions)
	}
}

func TestLoopControlBlocksResumesBypassesAndKills(t *testing.T) {
	var upstreamHits int
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits++
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"model":"gpt-4o","usage":{"prompt_tokens":10,"completion_tokens":1}}`))
	}))
	defer up.Close()
	rec := &capture{}
	gw := New(Config{
		Upstream: up.URL, Provider: "openai", Recorder: rec, Now: fixedClock(),
		Control: ControlPolicy{MaxCallsPerRun: 2, AdminToken: "tok", Now: fixedClock()},
	})
	fr := httptest.NewServer(gw)
	defer fr.Close()

	do := func(run string, hdr map[string]string) int {
		req, _ := http.NewRequest("POST", fr.URL+"/v1/chat/completions", strings.NewReader(`{"model":"gpt-4o"}`))
		if run != "" {
			req.Header.Set("X-AxiGate-Run", run)
		}
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}

	// Two calls allowed, the third refused with 429 and never forwarded.
	if do("run-x", nil) != 200 || do("run-x", nil) != 200 {
		t.Fatal("first two calls should pass")
	}
	if code := do("run-x", nil); code != http.StatusTooManyRequests {
		t.Fatalf("third call should be refused with 429, got %d", code)
	}
	if upstreamHits != 2 {
		t.Fatalf("a blocked call must not reach the provider: hits=%d", upstreamHits)
	}
	// The blocked request is in the ledger, marked blocked, with no cost.
	last := rec.events[len(rec.events)-1]
	if last.Dimensions["blocked"] == "" || last.Dimensions["status"] != "429" || last.CostUSD != 0 {
		t.Fatalf("blocked event not recorded honestly: %+v", last.Dimensions)
	}

	// Bypass forces one through despite the pause.
	if code := do("run-x", map[string]string{"X-AxiGate-Bypass": "tok"}); code != 200 {
		t.Fatalf("bypass should pass, got %d", code)
	}

	// Admin resume clears the pause.
	adminReq, _ := http.NewRequest("POST", fr.URL+"/_axigate/runs/resume?run=run-x", nil)
	adminReq.Header.Set("X-AxiGate-Admin", "tok")
	ar, _ := http.DefaultClient.Do(adminReq)
	ar.Body.Close()
	if ar.StatusCode != 200 {
		t.Fatalf("resume status: %d", ar.StatusCode)
	}
	if code := do("run-x", nil); code != 200 {
		t.Fatalf("after resume the run should pass, got %d", code)
	}

	// Admin without the token is rejected and never forwarded.
	noTok, _ := http.NewRequest("GET", fr.URL+"/_axigate/status", nil)
	st, _ := http.DefaultClient.Do(noTok)
	st.Body.Close()
	if st.StatusCode != http.StatusUnauthorized {
		t.Fatalf("admin without token should be 401, got %d", st.StatusCode)
	}

	// Kill switch refuses everything with 503.
	killReq, _ := http.NewRequest("POST", fr.URL+"/_axigate/kill", nil)
	killReq.Header.Set("X-AxiGate-Admin", "tok")
	kr, _ := http.DefaultClient.Do(killReq)
	kr.Body.Close()
	if code := do("fresh-run", nil); code != http.StatusServiceUnavailable {
		t.Fatalf("kill switch should refuse with 503, got %d", code)
	}
}

func TestEachRequestGetsADistinctId(t *testing.T) {
	front, rec := front(t, "openai", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"model":"gpt-4o","usage":{"prompt_tokens":10,"completion_tokens":1}}`))
	})
	for i := 0; i < 3; i++ {
		resp, _ := http.Post(front.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"gpt-4o"}`))
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if len(rec.events) != 3 {
		t.Fatalf("events: %d", len(rec.events))
	}
	ids := map[string]bool{}
	for _, e := range rec.events {
		if ids[e.ID] {
			t.Fatalf("two live requests collided on id %s", e.ID)
		}
		ids[e.ID] = true
	}
}
