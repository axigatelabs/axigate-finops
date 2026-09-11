// Package report turns ledger events into the diagnostic a developer reads
// first: totals by provider, spend by owner with unknown as its own row, cache
// use with the honest bounds, day spikes, and the usage-versus-cost-report
// difference per provider. Every section names the source it came from; a
// finding that needs request-level data is not invented from bucketed data.
package report

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
	"github.com/axigatelabs/axigate-finops/internal/pricing"
)

// Line is one aggregated row.
type Line struct {
	Key        string
	USD        float64
	Confidence ledger.Confidence
	Events     int
}

// ProviderCache is the cache picture for one provider, from usage rows.
type ProviderCache struct {
	Provider       string
	InputTokens    int64 // uncached input tokens
	CacheRead      int64
	CacheWrite     int64
	CachedShare    float64 // reads / (uncached + reads + writes)
	ReadSavingsUSD float64 // what the reads cost less than full price
	UncachedUSD    float64 // what the uncached input cost: the upper bound of what better caching could touch
}

// Spike is a day where one dimension's spend stood far above its own median.
type Spike struct {
	Day       string
	Dimension string
	Value     string
	USD       float64
	Median    float64
	Factor    float64
}

// Reconciliation compares our priced usage rows against the provider's own
// cost rows for the same provider and period, when both were imported.
type Reconciliation struct {
	Provider     string
	Period       string
	EstimatedUSD float64 // sum of usage rows priced from the table
	ReportedUSD  float64 // sum of the provider's cost rows
	DiffUSD      float64
	DiffPct      float64
}

// Report is the whole picture.
type Report struct {
	Periods         []string
	ByProvider      []Line
	ByOwner         []Line // key is "team / project / agent"; "unknown" is its own line
	ByModel         []Line
	UnknownUSD      float64
	UnknownShare    float64
	TotalUSD        float64
	UnknownDims     []Line // which unmapped ids carry the unknown spend
	Cache           []ProviderCache
	Unlisted        []Line // usage-row models absent from the price table, priced at the fallback; key is provider/model
	Spikes          []Spike
	Reconciliations []Reconciliation
	Sources         []string
	Notes           []string
}

// IsCostRow says whether an event is a provider's own cost figure — dollars in
// the provider-reported state with no token usage behind them — rather than a
// metered call. A cost row can set a total; it can never be attributed to a
// team, an agent or a run, because the provider does not know them.
func IsCostRow(e ledger.Event) bool {
	return e.Usage == (ledger.Usage{}) && e.Confidence.AtLeast(ledger.ProviderReported)
}

// Metered returns the events that describe individual calls — priced usage
// rows and the gateway's refusals — leaving provider cost rows out. Anything
// attributed (by team, agent, run, model or day) is built from these.
func Metered(events []ledger.Event) []ledger.Event {
	out := make([]ledger.Event, 0, len(events))
	for _, e := range events {
		if !IsCostRow(e) {
			out = append(out, e)
		}
	}
	return out
}

// Coverage is the set of provider-days a provider's own cost rows cover. A
// bill replaces the priced usage rows inside its own bucket and no others: a
// daily cost page imported mid-month sets the total for the days it holds and
// the gateway's later days stay priced usage, and last month's bill never
// silences this month's live rows.
type Coverage map[string]bool

const maxCoverageDays = 400 // a cost row's bucket is a day or a month; anything longer is malformed

func coverageKey(provider string, t time.Time) string {
	return provider + "|" + t.UTC().Format("2006-01-02")
}

// CostCoverage reads the days covered by every cost row in events. EndsAt is
// exclusive, the way the importers write buckets; a row with no span covers
// its start day.
func CostCoverage(events []ledger.Event) Coverage {
	cov := Coverage{}
	for _, e := range events {
		if !IsCostRow(e) {
			continue
		}
		start := e.StartsAt.UTC().Truncate(24 * time.Hour)
		end := e.EndsAt.UTC()
		if !end.After(start) {
			end = start.Add(24 * time.Hour)
		}
		for d, n := start, 0; d.Before(end) && n < maxCoverageDays; d, n = d.Add(24*time.Hour), n+1 {
			cov[coverageKey(e.Provider, d)] = true
		}
	}
	return cov
}

// Covers says whether a provider's own cost rows cover the day this event
// falls on, in which case the event's dollars are token detail only.
func (c Coverage) Covers(e ledger.Event) bool { return c[coverageKey(e.Provider, e.StartsAt)] }

// countsTowardTotal says whether an event's dollars belong in the totals: a
// cost row always, a usage row only on days no cost row covers.
func countsTowardTotal(e ledger.Event, cov Coverage) bool {
	return IsCostRow(e) || !cov.Covers(e)
}

func ownerKey(t ledger.Tags) string {
	if t == (ledger.Tags{}) {
		return "unknown"
	}
	parts := []string{}
	for _, p := range []string{t.Team, t.Project, t.Agent} {
		if p != "" {
			parts = append(parts, p)
		}
	}
	if t.Customer != "" {
		parts = append(parts, "customer:"+t.Customer)
	}
	return strings.Join(parts, " / ")
}

func lowestConfidence(a, b ledger.Confidence) ledger.Confidence {
	if a == "" {
		return b
	}
	if b.AtLeast(a) {
		return a
	}
	return b
}

type acc struct {
	usd  float64
	conf ledger.Confidence
	n    int
}

func linesOf(m map[string]*acc) []Line {
	out := make([]Line, 0, len(m))
	for k, a := range m {
		out = append(out, Line{Key: k, USD: a.usd, Confidence: a.conf, Events: a.n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].USD != out[j].USD {
			return out[i].USD > out[j].USD
		}
		return out[i].Key < out[j].Key
	})
	return out
}

func add(m map[string]*acc, key string, e ledger.Event) {
	a := m[key]
	if a == nil {
		a = &acc{}
		m[key] = a
	}
	a.usd += e.CostUSD
	a.conf = lowestConfidence(a.conf, e.Confidence)
	a.n++
}

// Included returns the events whose dollars count toward the total: on the
// days a provider's own cost rows cover, those drive the total and the usage
// rows are token detail only, so nothing is counted twice; on every other day
// the priced usage rows count. Exports (FOCUS) use this so their sums match
// the report total to the cent.
func Included(events []ledger.Event) []ledger.Event {
	cov := CostCoverage(events)
	out := make([]ledger.Event, 0, len(events))
	for _, e := range events {
		if countsTowardTotal(e, cov) {
			out = append(out, e)
		}
	}
	return out
}

// Build aggregates the events. It never reads request-level data; loops and
// runs live in a different report because they need a different source.
func Build(events []ledger.Event) Report {
	var r Report
	cov := CostCoverage(events)
	// Spikes are read from the most granular rows a provider has: usage rows
	// carry keys and models per day, cost rows only projects and line items.
	hasUsage := map[string]bool{}
	for _, e := range events {
		if e.Usage != (ledger.Usage{}) {
			hasUsage[e.Provider] = true
		}
	}
	byProvider, byOwner, byModel, byUnknownDim, byUnlisted := map[string]*acc{}, map[string]*acc{}, map[string]*acc{}, map[string]*acc{}, map[string]*acc{}
	periods, sources := map[string]bool{}, map[string]bool{}
	cache := map[string]*ProviderCache{}
	est, rep := map[string]float64{}, map[string]float64{} // key provider|period
	daily := map[string]map[string]float64{}               // dimension=value → day → usd

	for _, e := range events {
		periods[e.Period] = true
		sources[e.Source] = true
		if e.Usage != (ledger.Usage{}) {
			c := cache[e.Provider]
			if c == nil {
				c = &ProviderCache{Provider: e.Provider}
				cache[e.Provider] = c
			}
			c.InputTokens += uncachedOnly(e)
			c.CacheRead += e.Usage.CacheRead
			c.CacheWrite += e.Usage.CacheWrite5m + e.Usage.CacheWrite1h
			inputUsage := pricing.Usage{InputTokens: e.Usage.InputTokens, CacheRead: e.Usage.CacheRead, CacheWrite5m: e.Usage.CacheWrite5m, CacheWrite1h: e.Usage.CacheWrite1h}
			full := pricing.FullPriceEquivalent(e.Provider, e.Model, inputUsage)
			priced := pricing.Price(e.Provider, e.Model, inputUsage)
			if full > priced.USD {
				c.ReadSavingsUSD += full - priced.USD
			}
			// A model the table does not know was priced at the fallback; the
			// reader must see that before trusting any difference it feeds.
			if !priced.Listed && e.Model != "" {
				add(byUnlisted, e.Provider+"/"+e.Model, e)
			}
			c.UncachedUSD += pricing.Price(e.Provider, e.Model, pricing.Usage{InputTokens: uncachedOnly(e)}).USD
			if cov.Covers(e) { // reconcile like with like: only the days the bill covers
				est[e.Provider+"|"+e.Period] += e.CostUSD
			}
		} else if e.Confidence.AtLeast(ledger.ProviderReported) {
			rep[e.Provider+"|"+e.Period] += e.CostUSD
		}
		isUsageRow := e.Usage != (ledger.Usage{})
		if isUsageRow || !hasUsage[e.Provider] {
			for _, dim := range []string{"api_key_id", "project_id", "workspace_id"} {
				if v, ok := e.Dimensions[dim]; ok {
					k := dim + "=" + v
					if daily[k] == nil {
						daily[k] = map[string]float64{}
					}
					daily[k][e.StartsAt.UTC().Format("2006-01-02")] += e.CostUSD
					break
				}
			}
		}
		if !countsTowardTotal(e, cov) {
			continue
		}
		r.TotalUSD += e.CostUSD
		add(byProvider, e.Provider, e)
		key := ownerKey(e.Tags)
		add(byOwner, key, e)
		if key == "unknown" {
			r.UnknownUSD += e.CostUSD
			for _, dim := range []string{"api_key_id", "project_id", "workspace_id", "user_id", "account_id", "service_account_id"} {
				if v, ok := e.Dimensions[dim]; ok {
					add(byUnknownDim, dim+"="+v, e)
					break
				}
			}
		}
		model := e.Model
		if model == "" {
			model = "(not grouped by model)"
		}
		add(byModel, model, e)
	}

	for p := range periods {
		r.Periods = append(r.Periods, p)
	}
	sort.Strings(r.Periods)
	for s := range sources {
		r.Sources = append(r.Sources, s)
	}
	sort.Strings(r.Sources)
	r.ByProvider, r.ByOwner, r.ByModel, r.UnknownDims, r.Unlisted = linesOf(byProvider), linesOf(byOwner), linesOf(byModel), linesOf(byUnknownDim), linesOf(byUnlisted)
	if r.TotalUSD > 0 {
		r.UnknownShare = r.UnknownUSD / r.TotalUSD
	}
	for _, c := range cache {
		denom := float64(c.InputTokens + c.CacheRead + c.CacheWrite)
		if denom > 0 {
			c.CachedShare = float64(c.CacheRead) / denom
		}
		r.Cache = append(r.Cache, *c)
	}
	sort.Slice(r.Cache, func(i, j int) bool { return r.Cache[i].Provider < r.Cache[j].Provider })

	for key, days := range daily {
		if len(days) < 4 {
			continue
		}
		vals := make([]float64, 0, len(days))
		for _, v := range days {
			if v > 0 {
				vals = append(vals, v)
			}
		}
		if len(vals) < 4 {
			continue
		}
		sort.Float64s(vals)
		median := vals[len(vals)/2]
		if median <= 0 {
			continue
		}
		dim, value, _ := strings.Cut(key, "=")
		for day, v := range days {
			if v >= 4*median && v-median >= 25 {
				r.Spikes = append(r.Spikes, Spike{Day: day, Dimension: dim, Value: value, USD: v, Median: median, Factor: v / median})
			}
		}
	}
	sort.Slice(r.Spikes, func(i, j int) bool {
		if r.Spikes[i].USD != r.Spikes[j].USD {
			return r.Spikes[i].USD > r.Spikes[j].USD
		}
		return r.Spikes[i].Day < r.Spikes[j].Day
	})

	for key, reported := range rep {
		provider, period, _ := strings.Cut(key, "|")
		estimated, ok := est[key]
		if !ok {
			continue
		}
		rc := Reconciliation{Provider: provider, Period: period, EstimatedUSD: estimated, ReportedUSD: reported, DiffUSD: estimated - reported}
		if reported > 0 {
			rc.DiffPct = math.Abs(rc.DiffUSD) / reported
		}
		r.Reconciliations = append(r.Reconciliations, rc)
	}
	sort.Slice(r.Reconciliations, func(i, j int) bool {
		if r.Reconciliations[i].Provider != r.Reconciliations[j].Provider {
			return r.Reconciliations[i].Provider < r.Reconciliations[j].Provider
		}
		return r.Reconciliations[i].Period < r.Reconciliations[j].Period
	})

	// One note per billed provider, naming the months its cost report sets.
	billedMonths := map[string]map[string]bool{}
	for key := range cov {
		p, day, _ := strings.Cut(key, "|")
		if billedMonths[p] == nil {
			billedMonths[p] = map[string]bool{}
		}
		billedMonths[p][day[:7]] = true
	}
	for p, months := range billedMonths {
		ms := make([]string, 0, len(months))
		for m := range months {
			ms = append(ms, m)
		}
		sort.Strings(ms)
		r.Notes = append(r.Notes, fmt.Sprintf("%s: the provider's own cost report sets the total for the days it covers (%s); usage rows supply the token detail there and the total elsewhere", p, strings.Join(ms, ", ")))
	}
	sort.Strings(r.Notes)
	return r
}

func uncachedOnly(e ledger.Event) int64 {
	p, known := pricing.Providers[e.Provider]
	if known && !p.Inclusive {
		return e.Usage.InputTokens
	}
	u := e.Usage.InputTokens - e.Usage.CacheRead - e.Usage.CacheWrite5m - e.Usage.CacheWrite1h
	if u < 0 {
		return 0
	}
	return u
}
