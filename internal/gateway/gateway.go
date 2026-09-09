// Package gateway is the self-hosted, metadata-only pass-through proxy. An
// application points its provider base URL at this process; every request is
// forwarded to the real provider unchanged and the response is streamed back
// byte for byte. On the way back the gateway reads only the token counts and
// the caller's own tags, prices the call, and hands one immutable ledger
// event to a recorder.
//
// Three rules, from the repo's gateway contract:
//
//   - Fail open. Recording is best-effort and never affects the response. If
//     usage cannot be read (a streamed body, a short buffer, a parse error),
//     the request still succeeds and an event is still recorded, marked with
//     what could not be determined rather than guessed.
//   - Metadata only. The prompt and the completion are forwarded but never
//     recorded. An event carries counts, ids and tags, never message text.
//   - Never change the answer. The gateway adds no fields, drops none, and
//     does not retry or rewrite. It only observes.
//
// Loops and per-run caps build on this in later slices; this slice is the
// pass-through and the per-request event.
package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
	"github.com/axigatelabs/axigate-finops/internal/pricing"
)

// SourceGateway names events the gateway saw at request time.
const SourceGateway = "gateway"

// defaultMaxBody bounds how much of a response the gateway buffers to read
// usage. The client always receives the whole body; only the copy kept for
// parsing is capped. Usage blocks are small, so this is generous.
const defaultMaxBody = 1 << 20 // 1 MiB

// Recorder receives one event per request. Implementations must not block the
// response path for long and must treat the event as metadata.
type Recorder interface {
	Record(ledger.Event)
}

// Config configures a Gateway. Upstream and Provider are required.
type Config struct {
	Upstream string   // provider base URL, e.g. "https://api.anthropic.com"
	Provider string   // "openai" | "anthropic" (usage extraction + pricing)
	Recorder Recorder // where events go; a nil recorder drops them
	MaxBody  int64    // response bytes buffered for usage; 0 = default
	Client   *http.Client
	Now      func() time.Time // injectable clock; nil = time.Now
	Loop     LoopPolicy       // opt-in loop detection; zero value = off
	Control  ControlPolicy    // opt-in loop control; zero value = off
}

// Gateway is an http.Handler that proxies to one provider.
type Gateway struct {
	cfg  Config
	seq  atomic.Uint64
	det  *detector   // nil when loop detection is off
	ctrl *controller // nil when loop control is off
}

// New returns a Gateway, filling defaults.
func New(cfg Config) *Gateway {
	if cfg.MaxBody <= 0 {
		cfg.MaxBody = defaultMaxBody
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 10 * time.Minute}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	cfg.Upstream = strings.TrimRight(cfg.Upstream, "/")
	g := &Gateway{cfg: cfg}
	if cfg.Loop.active() {
		if cfg.Loop.Now == nil {
			cfg.Loop.Now = cfg.Now
		}
		g.det = newDetector(cfg.Loop)
	}
	if cfg.Control.active() {
		if cfg.Control.Now == nil {
			cfg.Control.Now = cfg.Now
		}
		g.ctrl = newController(cfg.Control)
	}
	return g
}

// hopByHop headers are connection-scoped and must not be forwarded.
var hopByHop = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true,
	"Proxy-Authorization": true, "Te": true, "Trailer": true,
	"Transfer-Encoding": true, "Upgrade": true,
}

func copyHeaders(dst, src http.Header) {
	for k, vs := range src {
		if hopByHop[http.CanonicalHeaderKey(k)] {
			continue
		}
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// The control plane lives under /_axigate/ and is never forwarded.
	if strings.HasPrefix(r.URL.Path, adminPrefix) {
		g.serveAdmin(w, r)
		return
	}

	start := g.cfg.Now().UTC()
	reqBody, _ := io.ReadAll(io.LimitReader(r.Body, g.cfg.MaxBody))
	_ = r.Body.Close()
	model := jsonModel(reqBody) // may be overwritten by the response's model
	run := strings.TrimSpace(r.Header.Get("X-AxiGate-Run"))

	// Loop detection reads the caller's explicit run id; a request with no run
	// id cannot be tied to a run, so it is never flagged.
	var dec Decision
	if g.det != nil && run != "" {
		dec = g.det.observe(run, signature(g.cfg.Provider, model, reqBody), g.cfg.Now().UTC())
		if dec.Suspected {
			fmt.Fprintf(os.Stderr, "gateway: suspected loop (%s) run=%q count=%d repeat=%d\n",
				dec.Signal, run, dec.Count, dec.Repeat)
		}
	}

	// Loop control refuses a paused or killed request before it leaves. A
	// bypass credential forces one request through a per-run pause.
	if g.ctrl != nil {
		// A caller may set its own per-run spend cap inline, from code, with no
		// server flag or restart: X-AxiGate-Max-Spend, keyed to the run it names.
		if g.cfg.Control.AllowRequestCaps {
			if cap := parseMoney(r.Header.Get("X-AxiGate-Max-Spend")); cap > 0 {
				g.ctrl.setRunCap(run, cap)
			}
		}
		bypass := g.cfg.Control.AdminToken != "" && r.Header.Get("X-AxiGate-Bypass") == g.cfg.Control.AdminToken
		if ok, reason := g.ctrl.admit(run, bypass); !ok {
			code, typ := http.StatusTooManyRequests, "run_paused"
			if reason == "kill switch engaged" {
				code, typ = http.StatusServiceUnavailable, "killed"
			}
			g.recordBlocked(r, model, run, reason, code, start)
			fmt.Fprintf(os.Stderr, "gateway: refused (%s) run=%q: %s\n", typ, run, reason)
			writeJSONError(w, code, typ, "axigate gateway: "+reason)
			return
		}
	}

	target := g.cfg.Upstream + r.URL.Path
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	outReq, err := http.NewRequestWithContext(r.Context(), r.Method, target, bytes.NewReader(reqBody))
	if err != nil {
		http.Error(w, "gateway: bad request: "+err.Error(), http.StatusBadGateway)
		return
	}
	copyHeaders(outReq.Header, r.Header)
	outReq.Header.Del("Accept-Encoding") // let the client see an uncompressed body we can also read

	resp, err := g.cfg.Client.Do(outReq)
	if err != nil {
		// The provider is unreachable. We cannot invent a response; the client
		// sees the failure. We do not record a cost for a call that never billed.
		http.Error(w, "gateway: upstream unreachable: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)

	// Stream the body to the client unchanged while teeing a copy for usage.
	// A streamed response carries usage in its final SSE event, so it is sniffed
	// frame by frame; a normal JSON response is buffered up to the cap. Either
	// way the client receives every byte; the tee only observes.
	streamed := strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream")
	usage, gotUsage, model2 := ledger.Usage{}, false, model
	if streamed {
		sniff := newSSEUsage(g.cfg.Provider, g.cfg.MaxBody)
		_, _ = io.Copy(w, io.TeeReader(resp.Body, sniff))
		if u, m, ok := sniff.result(); ok {
			usage, gotUsage = u, true
			if m != "" {
				model2 = m
			}
		}
	} else {
		cap := &capWriter{limit: g.cfg.MaxBody}
		_, _ = io.Copy(w, io.TeeReader(resp.Body, cap))
		if u, m, ok := extractUsage(g.cfg.Provider, cap.buf.Bytes()); ok {
			usage, gotUsage = u, true
			if m != "" {
				model2 = m
			}
		}
	}

	// From here on nothing may affect the response: it is already sent.
	g.record(r, resp.StatusCode, streamed, model2, usage, gotUsage, start, dec)
}

// parseMoney reads a dollar amount from a header like "5.00" or "$5". A
// non-positive or unparseable value is treated as unset (0), so a malformed
// header never silently disables a cap the caller thinks they set — it just
// does not set one, and the server-wide policy (if any) still applies.
func parseMoney(s string) float64 {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "$")
	if s == "" {
		return 0
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil || v <= 0 {
		return 0
	}
	return v
}

// signature identifies "the same request" for loop detection: the provider,
// the model, and a digest of the request body. The body is hashed in memory
// and never stored; only the short hex digest travels, so this stays metadata.
func signature(provider, model string, body []byte) string {
	h := sha256.Sum256(body)
	return provider + "|" + model + "|" + hex.EncodeToString(h[:8])
}

// record builds one metadata-only event and hands it to the recorder. It never
// returns an error: recording is best-effort by contract.
func (g *Gateway) record(r *http.Request, statusCode int, streamed bool, model string, usage ledger.Usage, gotUsage bool, start time.Time, dec Decision) {
	if g.cfg.Recorder == nil {
		return
	}
	// Fail open: a misbehaving recorder must never surface on the response
	// path, which by this point has already been fully written anyway.
	defer func() { _ = recover() }()
	usage.Requests = 1

	dims := map[string]string{
		"request_id": strconv.FormatInt(start.UnixNano(), 10) + "-" + strconv.FormatUint(g.seq.Add(1), 10),
		"status":     strconv.Itoa(statusCode),
	}
	if streamed {
		dims["stream"] = "true"
	}
	if !gotUsage {
		dims["usage"] = "unknown"
	}
	if dec.Suspected {
		dims["loop"] = "suspected"
		dims["loop_signal"] = dec.Signal
		dims["loop_count"] = strconv.Itoa(dec.Count)
		if dec.Repeat > 0 {
			dims["loop_repeat"] = strconv.Itoa(dec.Repeat)
		}
	}

	e := ledger.Event{
		Source:     SourceGateway,
		Provider:   g.cfg.Provider,
		Model:      model,
		Dimensions: dims,
		Usage:      usage,
		Tags:       tagsFromHeaders(r.Header),
		Confidence: ledger.Estimated,
		StartsAt:   start,
		EndsAt:     g.cfg.Now().UTC(),
	}
	if gotUsage {
		c := pricing.Price(g.cfg.Provider, model, pricing.Usage{
			InputTokens: usage.InputTokens, CacheRead: usage.CacheRead,
			CacheWrite5m: usage.CacheWrite5m, CacheWrite1h: usage.CacheWrite1h,
			OutputTokens: usage.OutputTokens,
		})
		e.CostUSD = c.USD
		e.PriceVersion = c.Version
		if !c.Listed {
			dims["unpriced_model"] = "true"
		}
	}
	if e.EndsAt.Before(e.StartsAt) {
		e.EndsAt = e.StartsAt
	}
	e.DeriveID()
	g.cfg.Recorder.Record(e)

	// Tally the completed request; this is what pauses a run that just crossed
	// a cap or tripped the loop signal, so the next request is refused.
	if g.ctrl != nil {
		g.ctrl.record(e.Tags.Run, e.CostUSD, dec.Suspected)
	}
}

// recordBlocked writes an event for a request the gateway refused, so the
// ledger shows enforcement, not a silent gap. No provider call was made, so
// there is no usage and no cost.
func (g *Gateway) recordBlocked(r *http.Request, model, run, reason string, code int, start time.Time) {
	if g.cfg.Recorder == nil {
		return
	}
	defer func() { _ = recover() }()
	dims := map[string]string{
		"request_id": strconv.FormatInt(start.UnixNano(), 10) + "-" + strconv.FormatUint(g.seq.Add(1), 10),
		"status":     strconv.Itoa(code),
		"blocked":    reason,
	}
	e := ledger.Event{
		Source: SourceGateway, Provider: g.cfg.Provider, Model: model, Dimensions: dims,
		Usage: ledger.Usage{}, Tags: tagsFromHeaders(r.Header),
		Confidence: ledger.Estimated, StartsAt: start, EndsAt: start,
	}
	e.DeriveID()
	g.cfg.Recorder.Record(e)
}

// tagsFromHeaders reads the caller's own attribution off X-AxiGate-* headers.
// These are how a request is attributed to an owner; the gateway sees them and
// records them, and never infers them.
func tagsFromHeaders(h http.Header) ledger.Tags {
	return ledger.Tags{
		Team:     strings.TrimSpace(h.Get("X-AxiGate-Team")),
		Project:  strings.TrimSpace(h.Get("X-AxiGate-Project")),
		Customer: strings.TrimSpace(h.Get("X-AxiGate-Customer")),
		Agent:    strings.TrimSpace(h.Get("X-AxiGate-Agent")),
		Run:      strings.TrimSpace(h.Get("X-AxiGate-Run")),
	}
}

// capWriter keeps at most limit bytes and silently drops the rest, so a large
// or streamed body never grows memory without bound.
type capWriter struct {
	buf   bytes.Buffer
	limit int64
}

func (c *capWriter) Write(p []byte) (int, error) {
	if room := c.limit - int64(c.buf.Len()); room > 0 {
		if int64(len(p)) <= room {
			c.buf.Write(p)
		} else {
			c.buf.Write(p[:room])
		}
	}
	return len(p), nil // always report full write; we are a sink, not a bottleneck
}

// jsonModel pulls "model" from a request or response body without a full
// decode failing on unknown fields. Returns "" when absent.
func jsonModel(b []byte) string {
	var v struct {
		Model string `json:"model"`
	}
	_ = json.Unmarshal(b, &v)
	return v.Model
}

// providerUsage mirrors the usage blocks of both providers' non-streaming
// responses. Unset fields stay zero.
type providerUsage struct {
	Model string `json:"model"`
	Usage struct {
		// OpenAI
		PromptTokens        int64 `json:"prompt_tokens"`
		CompletionTokens    int64 `json:"completion_tokens"`
		PromptTokensDetails struct {
			CachedTokens int64 `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
		// Anthropic
		InputTokens           int64 `json:"input_tokens"`
		OutputTokens          int64 `json:"output_tokens"`
		CacheReadInputTokens  int64 `json:"cache_read_input_tokens"`
		CacheCreationInputTok int64 `json:"cache_creation_input_tokens"`
		CacheCreation         struct {
			Ephemeral5m int64 `json:"ephemeral_5m_input_tokens"`
			Ephemeral1h int64 `json:"ephemeral_1h_input_tokens"`
		} `json:"cache_creation"`
	} `json:"usage"`
}

// extractUsage reads token counts from a non-streaming response body for the
// given provider, normalizing to the ledger's convention (openai input is
// inclusive of cache reads; anthropic input is the uncached figure). ok is
// false when no usage block was present.
func extractUsage(provider string, body []byte) (ledger.Usage, string, bool) {
	var p providerUsage
	if err := json.Unmarshal(body, &p); err != nil {
		return ledger.Usage{}, "", false
	}
	switch provider {
	case "openai":
		if p.Usage.PromptTokens == 0 && p.Usage.CompletionTokens == 0 {
			return ledger.Usage{}, p.Model, false
		}
		return ledger.Usage{
			InputTokens:  p.Usage.PromptTokens, // inclusive of cached, per the table's convention
			CacheRead:    p.Usage.PromptTokensDetails.CachedTokens,
			OutputTokens: p.Usage.CompletionTokens,
		}, p.Model, true
	case "anthropic":
		if p.Usage.InputTokens == 0 && p.Usage.OutputTokens == 0 && p.Usage.CacheReadInputTokens == 0 {
			return ledger.Usage{}, p.Model, false
		}
		w5 := p.Usage.CacheCreation.Ephemeral5m
		w1 := p.Usage.CacheCreation.Ephemeral1h
		if w5 == 0 && w1 == 0 { // older shape: a single cache-creation figure, treated as 5-minute
			w5 = p.Usage.CacheCreationInputTok
		}
		return ledger.Usage{
			InputTokens:  p.Usage.InputTokens, // uncached, per the table's convention
			CacheRead:    p.Usage.CacheReadInputTokens,
			CacheWrite5m: w5,
			CacheWrite1h: w1,
			OutputTokens: p.Usage.OutputTokens,
		}, p.Model, true
	}
	return ledger.Usage{}, p.Model, false
}
