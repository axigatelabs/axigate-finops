package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
)

func TestSSEUsageAnthropic(t *testing.T) {
	// input + cache on message_start, final output on message_delta.
	stream := strings.Join([]string{
		"event: message_start",
		`data: {"type":"message_start","message":{"model":"claude-sonnet-4-5","usage":{"input_tokens":1000,"cache_read_input_tokens":400,"cache_creation":{"ephemeral_5m_input_tokens":100,"ephemeral_1h_input_tokens":0},"output_tokens":1}}}`,
		"",
		"event: message_delta",
		`data: {"type":"message_delta","usage":{"output_tokens":250}}`,
		"",
		"event: message_stop",
		`data: {"type":"message_stop"}`,
		"", "",
	}, "\n")
	s := newSSEUsage("anthropic", defaultMaxBody)
	// feed in awkward chunks to prove cross-write reassembly
	for i := 0; i < len(stream); i += 7 {
		end := i + 7
		if end > len(stream) {
			end = len(stream)
		}
		s.Write([]byte(stream[i:end]))
	}
	u, model, ok := s.result()
	if !ok || model != "claude-sonnet-4-5" {
		t.Fatalf("ok=%v model=%q", ok, model)
	}
	want := ledger.Usage{InputTokens: 1000, CacheRead: 400, CacheWrite5m: 100, OutputTokens: 250}
	if u != want {
		t.Fatalf("usage: %+v want %+v", u, want)
	}
}

func TestSSEUsageOpenAITakesTheFinalChunk(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"model":"gpt-4o-2024-08-06","choices":[{"delta":{"content":"hi"}}]}`,
		"",
		`data: {"model":"gpt-4o-2024-08-06","choices":[],"usage":{"prompt_tokens":900,"completion_tokens":120,"prompt_tokens_details":{"cached_tokens":300}}}`,
		"",
		"data: [DONE]",
		"", "",
	}, "\n")
	s := newSSEUsage("openai", defaultMaxBody)
	s.Write([]byte(stream))
	u, model, ok := s.result()
	if !ok || model != "gpt-4o-2024-08-06" {
		t.Fatalf("ok=%v model=%q", ok, model)
	}
	want := ledger.Usage{InputTokens: 900, CacheRead: 300, OutputTokens: 120}
	if u != want {
		t.Fatalf("usage: %+v want %+v", u, want)
	}
}

func TestSSEUsageAbsentStaysUnknown(t *testing.T) {
	// OpenAI stream without include_usage: no usage anywhere.
	s := newSSEUsage("openai", defaultMaxBody)
	s.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\ndata: [DONE]\n\n"))
	if _, _, ok := s.result(); ok {
		t.Fatal("no usage in the stream must stay unknown")
	}
}

func TestStreamedRequestIsPricedEndToEnd(t *testing.T) {
	answer := strings.Join([]string{
		"event: message_start",
		`data: {"type":"message_start","message":{"model":"claude-haiku-4-5","usage":{"input_tokens":2000,"cache_read_input_tokens":0,"output_tokens":1}}}`,
		"",
		"event: message_delta",
		`data: {"type":"message_delta","usage":{"output_tokens":500}}`,
		"", "",
	}, "\n")
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(answer))
	}))
	defer up.Close()
	rec := &capture{}
	gw := New(Config{Upstream: up.URL, Provider: "anthropic", Recorder: rec, Now: fixedClock()})
	fr := httptest.NewServer(gw)
	defer fr.Close()
	resp, _ := http.Post(fr.URL+"/v1/messages", "application/json", strings.NewReader(`{"model":"claude-haiku-4-5","stream":true}`))
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "message_delta") {
		t.Fatalf("stream not passed through: %s", body)
	}
	// record() runs in the server goroutine after the streamed body is relayed;
	// wait for it rather than racing the client's read completion.
	for i := 0; i < 200 && rec.count() == 0; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if rec.count() != 1 {
		t.Fatalf("events: %d", rec.count())
	}
	e := rec.events[0]
	if e.Dimensions["stream"] != "true" || e.Dimensions["usage"] == "unknown" {
		t.Fatalf("a streamed response with usage should be priced, not unknown: %+v", e.Dimensions)
	}
	want := ledger.Usage{InputTokens: 2000, OutputTokens: 500, Requests: 1}
	if e.Usage != want || e.CostUSD <= 0 || e.Model != "claude-haiku-4-5" {
		t.Fatalf("streamed event: usage=%+v cost=%v model=%q", e.Usage, e.CostUSD, e.Model)
	}
}
