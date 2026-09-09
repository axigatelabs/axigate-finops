// Package seed generates synthetic gateway events for building and testing the
// console without real keys or real traffic. Everything here is invented: no
// customer data, no real ids. The generator is deterministic given a seed, so a
// test sees the same ledger every run.
package seed

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
	"github.com/axigatelabs/axigate-finops/internal/pricing"
)

type owner struct {
	team, project, agent, provider, model string
}

// owners is a small cast of teams and agents across both providers.
var owners = []owner{
	{"platform", "gateway", "router-bot", "anthropic", "claude-haiku-4-5"},
	{"platform", "gateway", "router-bot", "anthropic", "claude-sonnet-4-5"},
	{"research", "experiments", "planner", "anthropic", "claude-opus-4-5"},
	{"research", "experiments", "planner", "openai", "gpt-4o"},
	{"growth", "crawler", "scraper", "openai", "gpt-4o-mini"},
	{"support", "helpdesk", "support-bot", "anthropic", "claude-haiku-4-5"},
}

// Options controls a run.
type Options struct {
	Events int       // total events to generate (including the runaway loop)
	Start  time.Time // first event time (UTC); events span ~30 days from here
	Seed   int64     // RNG seed for determinism
}

// Generate returns Options.Events synthetic events: normal traffic spread over
// the window across the owners, plus one runaway loop, a single run that fires
// hundreds of identical calls in an hour, some of which the gateway blocks.
func Generate(o Options) []ledger.Event {
	if o.Events <= 0 {
		o.Events = 10000
	}
	if o.Start.IsZero() {
		o.Start = time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	}
	rng := rand.New(rand.NewSource(o.Seed))
	window := 30 * 24 * time.Hour

	loopCount := o.Events / 20 // ~5% of the traffic is one runaway loop
	normal := o.Events - loopCount
	events := make([]ledger.Event, 0, o.Events)
	var seq int64

	next := func(t time.Time, ow owner, run string, blocked bool) ledger.Event {
		seq++
		u := ledger.Usage{
			InputTokens:  int64(200 + rng.Intn(4000)),
			CacheRead:    int64(rng.Intn(1500)),
			OutputTokens: int64(50 + rng.Intn(1200)),
			Requests:     1,
		}
		if ow.provider == "anthropic" && rng.Intn(3) == 0 {
			u.CacheWrite5m = int64(rng.Intn(800))
		}
		dims := map[string]string{
			"request_id": fmt.Sprintf("%d-%d", t.UnixNano(), seq),
			"status":     "200",
		}
		e := ledger.Event{
			Source:     "gateway",
			Provider:   ow.provider,
			Model:      ow.model,
			Dimensions: dims,
			Usage:      u,
			Tags:       ledger.Tags{Team: ow.team, Project: ow.project, Agent: ow.agent, Run: run},
			Confidence: ledger.Estimated,
			StartsAt:   t.UTC(),
			EndsAt:     t.UTC(),
		}
		if blocked {
			e.Usage = ledger.Usage{}
			e.Dimensions["status"] = "429"
			e.Dimensions["blocked"] = "run reached the call cap of 50"
			e.Confidence = ledger.Estimated
		} else {
			c := pricing.Price(ow.provider, ow.model, pricing.Usage{
				InputTokens: u.InputTokens, CacheRead: u.CacheRead,
				CacheWrite5m: u.CacheWrite5m, OutputTokens: u.OutputTokens,
			})
			e.CostUSD = c.USD
			e.PriceVersion = c.Version
		}
		e.DeriveID()
		return e
	}

	for i := 0; i < normal; i++ {
		ow := owners[rng.Intn(len(owners))]
		t := o.Start.Add(time.Duration(rng.Int63n(int64(window))))
		run := fmt.Sprintf("run-%s-%d", ow.agent, rng.Intn(2000))
		events = append(events, next(t, ow, run, false))
	}

	// The runaway loop: one run, one hour, hundreds of identical calls. The
	// gateway lets the first 50 through (the cap) and blocks the rest.
	ow := owners[2] // research / planner / claude-opus-4-5 — the expensive one
	loopStart := o.Start.Add(12 * 24 * time.Hour)
	for i := 0; i < loopCount; i++ {
		t := loopStart.Add(time.Duration(i) * (time.Hour / time.Duration(loopCount+1)))
		events = append(events, next(t, ow, "run-planner-RUNAWAY", i >= 50))
	}
	return events
}
