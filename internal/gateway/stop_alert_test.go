package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStopAlertFiresOncePerStopWithAttribution(t *testing.T) {
	c := newController(ControlPolicy{MaxSpendUSDPerRun: 0.10, Now: fixed()})
	got := make(chan StopEvent, 4)
	c.onPause = func(ev StopEvent) { got <- ev }
	c.noteRun("r", "reconciler", "payments", "gpt-4o")

	c.admit("r", false)
	c.record("r", 0.06, false) // under the cap
	c.admit("r", false)
	c.record("r", 0.06, false) // $0.12 >= $0.10 → the run stops → one alert

	select {
	case ev := <-got:
		if ev.Run != "r" || ev.Agent != "reconciler" || ev.Team != "payments" || ev.Model != "gpt-4o" {
			t.Fatalf("alert attribution wrong: %+v", ev)
		}
		if ev.Calls != 2 || ev.SpendUSD < 0.119 || ev.SpendUSD > 0.121 || ev.Reason == "" {
			t.Fatalf("alert counts/reason wrong: %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no stop alert fired when the run was stopped")
	}

	// Later refusals of the already-stopped run must NOT re-fire the alert.
	c.admit("r", false)
	c.admit("r", false)
	select {
	case ev := <-got:
		t.Fatalf("alert re-fired on a plain refusal: %+v", ev)
	case <-time.After(150 * time.Millisecond):
	}

	// After a resume, a fresh trip is a new stop and alerts again.
	c.resumeRun("r")
	c.admit("r", false)
	c.record("r", 0.20, false)
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("alert should fire again after resume + a new trip")
	}
}

func TestStopAlertFiresOnAdmitSideCapTrip(t *testing.T) {
	c := newController(ControlPolicy{MaxCallsPerRun: 2, Now: fixed()})
	got := make(chan StopEvent, 2)
	c.onPause = func(ev StopEvent) { got <- ev }
	c.admit("r", false)
	c.admit("r", false) // two in flight = the cap
	if ok, _ := c.admit("r", false); ok {
		t.Fatal("third concurrent call should be refused at admit")
	}
	select {
	case ev := <-got:
		if !strings.Contains(ev.Reason, "call cap") {
			t.Fatalf("reason = %q", ev.Reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("an admit-side (reserve-at-admit) trip should fire the alert")
	}
}

func TestStopAlertSenderPostsSlackAndDiscordCompatibleJSON(t *testing.T) {
	got := make(chan map[string]any, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m map[string]any
		_ = json.NewDecoder(r.Body).Decode(&m)
		got <- m
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	send := newStopAlertSender(srv.URL)
	send(StopEvent{
		Run: "r1", Agent: "reconciler", Team: "payments", Model: "gpt-4o",
		Reason: "run reached the spend cap of $0.25", Calls: 9, SpendUSD: 0.27,
		At: time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC),
	})
	select {
	case m := <-got:
		for _, k := range []string{"text", "content", "event", "run", "agent", "team", "model", "reason", "calls", "spend_usd", "at"} {
			if _, ok := m[k]; !ok {
				t.Fatalf("payload missing %q: %v", k, m)
			}
		}
		if m["event"] != "run_stopped" || m["run"] != "r1" || m["agent"] != "reconciler" {
			t.Fatalf("payload fields wrong: %v", m)
		}
		line, _ := m["text"].(string)
		if !strings.Contains(line, "reconciler") || !strings.Contains(line, "$0.27") || !strings.Contains(line, "spend cap") {
			t.Fatalf("human line wrong: %q", line)
		}
		if m["text"] != m["content"] {
			t.Fatal("text (Slack) and content (Discord) should carry the same line")
		}
		for _, k := range []string{"prompt", "messages", "completion"} { // metadata only
			if _, ok := m[k]; ok {
				t.Fatalf("payload must never carry %q", k)
			}
		}
	case <-time.After(3 * time.Second):
		t.Fatal("sender did not POST the alert")
	}
}

func TestStopAlertSenderFailsOpenWhenUnreachable(t *testing.T) {
	send := newStopAlertSender("http://127.0.0.1:1/hook") // closed port
	done := make(chan struct{})
	go func() { send(StopEvent{Run: "x", Reason: "test"}); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("sender hung instead of failing open")
	}
}

// A key stop names the key once, says whether the figures are today's, and
// a run stop carries no key field at all.
func TestStopAlertPayloadNamesAKeyOnce(t *testing.T) {
	m := stopAlertPayload(StopEvent{
		Key: "k-3fa9c2b1e0d4-7788", Agent: "scraper",
		Reason: "key …7788 reached its daily spend cap of $50.00 (resets at midnight UTC)",
		Calls:  3, SpendUSD: 0.02, At: time.Date(2026, 9, 11, 0, 0, 0, 0, time.UTC), dayPause: true,
	})
	if m["event"] != "key_stopped" || m["key"] != "k-3fa9c2b1e0d4-7788" || m["run"] != "" {
		t.Fatalf("key event fields: %v", m)
	}
	line, _ := m["text"].(string)
	want := "AxiGate stopped scraper (key …7788) — reached its daily spend cap of $50.00 (resets at midnight UTC). 3 calls, $0.02 spent today; further calls are being refused."
	if line != want {
		t.Fatalf("line = %q", line)
	}
	total := stopAlertPayload(StopEvent{Key: "k-3fa9c2b1e0d4-7788", Reason: "key …7788 reached its spend cap of $500.00", Calls: 40, SpendUSD: 500})
	if l, _ := total["text"].(string); !strings.HasPrefix(l, "AxiGate stopped key …7788 — reached its spend cap of $500.00. 40 calls, $500.00 spent so far") {
		t.Fatalf("total line = %q", l)
	}
	r := stopAlertPayload(StopEvent{Run: "r1", Reason: "run reached the call cap of 3"})
	if _, ok := r["key"]; ok || r["event"] != "run_stopped" {
		t.Fatalf("a run stop has no key field: %v", r)
	}
}

// A shadow-mode stop is an alert about what would have happened, and says so.
func TestStopAlertPayloadSaysWouldHaveStoppedInShadowMode(t *testing.T) {
	m := stopAlertPayload(StopEvent{Run: "r1", Agent: "scraper", Reason: "run reached the spend cap of $5.00", Calls: 12, SpendUSD: 5.2, Shadow: true})
	line, _ := m["text"].(string)
	if m["event"] != "run_flagged" || m["shadow"] != true || line != "AxiGate would have stopped scraper (run r1) — run reached the spend cap of $5.00. 12 calls, $5.20 spent so far; shadow mode is on, so its calls continue." {
		t.Fatalf("shadow payload = %v", m)
	}
	k := stopAlertPayload(StopEvent{Key: "k-3fa9c2b1e0d4-7788", Reason: "key …7788 reached its spend cap of $500.00", Shadow: true})
	if k["event"] != "key_flagged" {
		t.Fatalf("key shadow event = %v", k["event"])
	}
	if _, ok := stopAlertPayload(StopEvent{Run: "r1", Reason: "x"})["shadow"]; ok {
		t.Fatal("an enforced stop carries no shadow field")
	}
}
