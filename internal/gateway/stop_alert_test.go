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
