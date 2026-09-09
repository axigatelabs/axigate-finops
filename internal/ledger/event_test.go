package ledger

import (
	"testing"
	"time"
)

func TestConfidenceOrderAndSigning(t *testing.T) {
	if !Final.AtLeast(InvoiceReconciled) || !InvoiceReconciled.AtLeast(Signable) {
		t.Fatal("reconciled and final must be signable")
	}
	if Estimated.AtLeast(Signable) || ProviderReported.AtLeast(Signable) {
		t.Fatal("estimated and provider-reported must never be signable")
	}
	if Confidence("exact").Valid() {
		t.Fatal("'exact' is not a state; the word is banned on purpose")
	}
}

func sample() Event {
	start := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	e := Event{
		Source: "openai-usage", Provider: "openai", Model: "gpt-4o",
		Dimensions: map[string]string{"project_id": "proj_1", "api_key_id": "key_9"},
		Usage:      Usage{InputTokens: 1000, CacheRead: 400, OutputTokens: 100, Requests: 3},
		Confidence: ProviderReported, StartsAt: start, EndsAt: start.Add(24 * time.Hour),
	}
	e.DeriveID()
	return e
}

func TestDeriveIDIsDeterministicAndPeriodFollowsBucket(t *testing.T) {
	a, b := sample(), sample()
	if a.ID == "" || a.ID != b.ID {
		t.Fatalf("ids differ or empty: %q %q", a.ID, b.ID)
	}
	if a.Period != "2026-08" {
		t.Fatalf("period: got %q", a.Period)
	}
	// The numbers are not part of the identity: a re-import with corrected
	// counts is the same row.
	c := sample()
	c.Usage.InputTokens = 999
	c.DeriveID()
	if c.ID != a.ID {
		t.Fatal("changing counts changed the id")
	}
	// A different dimension is a different row.
	d := sample()
	d.Dimensions["api_key_id"] = "key_10"
	d.DeriveID()
	if d.ID == a.ID {
		t.Fatal("different dimensions produced the same id")
	}
}

func TestEventValidate(t *testing.T) {
	if err := sample().Validate(); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	base := func() Event {
		return Event{ID: "x", Source: "s", Provider: "p", Confidence: Estimated, Period: "2026-08", StartsAt: start, EndsAt: start.Add(time.Hour)}
	}
	cases := map[string]func(*Event){
		"no id":         func(e *Event) { e.ID = "" },
		"no provider":   func(e *Event) { e.Provider = "" },
		"bad state":     func(e *Event) { e.Confidence = "exact" },
		"negative cost": func(e *Event) { e.CostUSD = -1 },
		"bad period":    func(e *Event) { e.Period = "Aug 2026" },
		"ends first":    func(e *Event) { e.EndsAt = start.Add(-time.Hour) },
	}
	for name, mutate := range cases {
		e := base()
		mutate(&e)
		if err := e.Validate(); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}
