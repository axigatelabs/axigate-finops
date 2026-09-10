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

const geminiAnswer = `{"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"},"finishReason":"STOP"}],` +
	`"usageMetadata":{"promptTokenCount":1000,"candidatesTokenCount":500,"totalTokenCount":1550,"cachedContentTokenCount":200,"thoughtsTokenCount":50},` +
	`"modelVersion":"gemini-2.5-flash"}`

func TestForwardsGeminiAndPricesTheModelFromThePath(t *testing.T) {
	var gotPath string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(geminiAnswer))
	}))
	defer up.Close()
	rec := &capture{}
	gw := New(Config{Upstream: up.URL, Provider: "gemini", Recorder: rec, Now: fixedClock()})
	fr := httptest.NewServer(gw)
	defer fr.Close()

	resp, err := http.Post(fr.URL+"/v1beta/models/gemini-2.5-flash:generateContent", "application/json",
		strings.NewReader(`{"contents":[{"parts":[{"text":"hi"}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "usageMetadata") {
		t.Fatalf("response not passed through unchanged: %s", body)
	}
	if gotPath != "/v1beta/models/gemini-2.5-flash:generateContent" {
		t.Fatalf("upstream path = %q", gotPath)
	}
	rec.waitEvents(t, 1)
	e := rec.events[0]
	// prompt count is inclusive of the 200 cached; thinking (50) bills as output
	want := ledger.Usage{InputTokens: 1000, CacheRead: 200, OutputTokens: 550, Requests: 1}
	if e.Usage != want {
		t.Fatalf("usage = %+v, want %+v", e.Usage, want)
	}
	if e.Model != "gemini-2.5-flash" {
		t.Fatalf("model = %q; must come from the request path", e.Model)
	}
	if e.CostUSD <= 0 || e.Dimensions["unpriced_model"] != "" {
		t.Fatalf("should be priced from the table: cost=%v dims=%v", e.CostUSD, e.Dimensions)
	}
}

func TestGeminiStreamIsPricedFromTheLastChunk(t *testing.T) {
	answer := strings.Join([]string{
		`data: {"candidates":[{"content":{"parts":[{"text":"he"}]}}],"usageMetadata":{"promptTokenCount":1000,"candidatesTokenCount":1,"totalTokenCount":1001},"modelVersion":"gemini-2.5-flash"}`,
		"",
		`data: {"candidates":[{"content":{"parts":[{"text":"llo"}]}}],"usageMetadata":{"promptTokenCount":1000,"candidatesTokenCount":500,"totalTokenCount":1500,"cachedContentTokenCount":200},"modelVersion":"gemini-2.5-flash"}`,
		"", "",
	}, "\n")
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(answer))
	}))
	defer up.Close()
	rec := &capture{}
	gw := New(Config{Upstream: up.URL, Provider: "gemini", Recorder: rec, Now: fixedClock()})
	fr := httptest.NewServer(gw)
	defer fr.Close()

	resp, _ := http.Post(fr.URL+"/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse", "application/json", strings.NewReader(`{"contents":[]}`))
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "llo") {
		t.Fatalf("stream not passed through: %s", body)
	}
	for i := 0; i < 200 && rec.count() == 0; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if rec.count() != 1 {
		t.Fatalf("events: %d", rec.count())
	}
	e := rec.events[0]
	want := ledger.Usage{InputTokens: 1000, CacheRead: 200, OutputTokens: 500, Requests: 1}
	if e.Usage != want || e.Model != "gemini-2.5-flash" || e.CostUSD <= 0 || e.Dimensions["stream"] != "true" {
		t.Fatalf("streamed gemini event: usage=%+v model=%q cost=%v dims=%v", e.Usage, e.Model, e.CostUSD, e.Dimensions)
	}
}

func TestGeminiUsageFromAJSONArrayStreamBody(t *testing.T) {
	// streamGenerateContent WITHOUT alt=sse returns a JSON array of chunks.
	body := `[{"candidates":[],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":1}},` +
		`{"candidates":[],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":7},"modelVersion":"gemini-2.5-pro"}]`
	u, model, ok := extractUsage("gemini", []byte(body))
	if !ok || u.InputTokens != 10 || u.OutputTokens != 7 || model != "gemini-2.5-pro" {
		t.Fatalf("array body: ok=%v usage=%+v model=%q", ok, u, model)
	}
	if _, _, ok := extractUsage("gemini", []byte(`{"candidates":[]}`)); ok {
		t.Fatal("a body with no usageMetadata must not claim usage")
	}
}

func TestGeminiModelFromPath(t *testing.T) {
	cases := map[string]string{
		"/v1beta/models/gemini-2.5-flash:generateContent":     "gemini-2.5-flash",
		"/v1beta/models/gemini-2.5-pro:streamGenerateContent": "gemini-2.5-pro",
		"/v1/models/gemini-2.5-flash-lite:countTokens":        "gemini-2.5-flash-lite",
		"/v1beta/models":       "",
		"/v1/chat/completions": "",
	}
	for p, want := range cases {
		if got := geminiModelFromPath(p); got != want {
			t.Errorf("%s → %q, want %q", p, got, want)
		}
	}
}
