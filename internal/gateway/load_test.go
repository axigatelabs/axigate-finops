//go:build loadtest

// A repeatable, offline load harness. It never touches a real provider: a fake
// upstream with a fixed delay stands in, so there are no keys and no spend. It
// answers two questions the Phase 2 proof bar asks:
//
//   - What latency does the gateway add at 50 requests per second?
//   - How many requests slip past a per-run cap under concurrency (the
//     in-memory overspend bound), both for a realistic low-concurrency agent
//     and for a worst-case simultaneous burst?
//
// Run it out of the normal test path:
//
//	go test -tags loadtest -run TestLoad -v ./internal/gateway/
package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const upstreamDelay = 5 * time.Millisecond

func fakeUpstream(hits *int64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits != nil {
			atomic.AddInt64(hits, 1)
		}
		time.Sleep(upstreamDelay)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"model":"gpt-4o","usage":{"prompt_tokens":100,"completion_tokens":20}}`))
	}))
}

func loadClient() *http.Client {
	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConns: 256, MaxIdleConnsPerHost: 256},
	}
}

func pct(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	i := int(float64(len(ds)-1) * p)
	return ds[i]
}

// drive sends rps requests per second for dur against base, returning the
// per-request latencies. Each tick launches its own request, so a slow response
// never throttles the offered rate.
func drive(base string, rps int, dur time.Duration, cl *http.Client) []time.Duration {
	var mu sync.Mutex
	var lat []time.Duration
	var wg sync.WaitGroup
	tick := time.NewTicker(time.Second / time.Duration(rps))
	defer tick.Stop()
	deadline := time.Now().Add(dur)
	for now := range tick.C {
		if now.After(deadline) {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			t0 := time.Now()
			resp, err := cl.Post(base+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"gpt-4o"}`))
			if err != nil {
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			d := time.Since(t0)
			mu.Lock()
			lat = append(lat, d)
			mu.Unlock()
		}()
	}
	wg.Wait()
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	return lat
}

func TestLoadLatencyOverheadAt50RPS(t *testing.T) {
	up := fakeUpstream(nil)
	defer up.Close()
	gw := New(Config{Upstream: up.URL, Provider: "openai", Recorder: &capture{}})
	fr := httptest.NewServer(gw)
	defer fr.Close()
	cl := loadClient()

	// Warm connections, then measure direct and through the gateway.
	drive(up.URL, 50, 500*time.Millisecond, cl)
	direct := drive(up.URL, 50, 4*time.Second, cl)
	drive(fr.URL, 50, 500*time.Millisecond, cl)
	via := drive(fr.URL, 50, 4*time.Second, cl)

	t.Logf("requests: direct=%d via-gateway=%d (upstream delay %s)", len(direct), len(via), upstreamDelay)
	for _, p := range []struct {
		name string
		p    float64
	}{{"p50", 0.50}, {"p90", 0.90}, {"p99", 0.99}, {"max", 1.0}} {
		d, v := pct(direct, p.p), pct(via, p.p)
		t.Logf("%-4s  direct %-8s  via %-8s  gateway overhead %s", p.name,
			d.Round(10*time.Microsecond), v.Round(10*time.Microsecond), (v - d).Round(10*time.Microsecond))
	}
}

// capBound fires total requests at the given concurrency for one run, against a
// gateway with the given call cap, and reports how many were admitted (reached
// the upstream) versus the cap. Admitted-minus-cap is the overspend bound.
func capBound(t *testing.T, label string, cap, total, concurrency int) {
	var hits int64
	up := fakeUpstream(&hits)
	defer up.Close()
	gw := New(Config{Upstream: up.URL, Provider: "openai", Recorder: &capture{},
		Control: ControlPolicy{MaxCallsPerRun: cap}})
	fr := httptest.NewServer(gw)
	defer fr.Close()
	cl := loadClient()

	var ok200, got429 int64
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			req, _ := http.NewRequest("POST", fr.URL+"/v1/chat/completions", strings.NewReader(`{"model":"gpt-4o"}`))
			req.Header.Set("X-AxiGate-Run", "burst")
			resp, err := cl.Do(req)
			if err != nil {
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			switch resp.StatusCode {
			case 200:
				atomic.AddInt64(&ok200, 1)
			case 429:
				atomic.AddInt64(&got429, 1)
			}
		}()
	}
	wg.Wait()
	admitted := atomic.LoadInt64(&hits)
	t.Logf("%-22s cap=%d total=%d concurrency=%-3d  admitted=%d  refused(429)=%d  overspend=%d",
		label, cap, total, concurrency, admitted, got429, admitted-int64(cap))
}

func TestLoadCapBoundUnderConcurrency(t *testing.T) {
	// A realistic agent: a handful of calls in flight at once.
	capBound(t, "sequential agent", 100, 400, 1)
	capBound(t, "low concurrency", 100, 400, 8)
	// Worst case: a thundering herd that all arrive before any completes.
	capBound(t, "thundering herd", 100, 400, 400)
}
