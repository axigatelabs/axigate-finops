// Package console is the local dashboard's read side: an HTTP API and a
// dark-mode dashboard over the gateway's ledger. The money math is not
// reimplemented here; totals and inclusion come from internal/report and the
// FOCUS export from internal/focus, so the dashboard and the exports can never
// disagree with the statement.
//
// It is self-hosted and unrestricted: full history and every export, no tiers
// and no account. Point it at a ledger file and it re-reads it on each request,
// so a refresh reflects whatever the gateway has appended.
package console

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/axigatelabs/axigate-finops/internal/focus"
	"github.com/axigatelabs/axigate-finops/internal/ledger"
	"github.com/axigatelabs/axigate-finops/internal/report"
)

// Server serves the console over a set of events, or live from a ledger file.
type Server struct {
	events   []ledger.Event
	earliest time.Time
	latest   time.Time
	mux      *http.ServeMux

	// ledgerPath, when set, makes the console re-read this JSONL ledger, so a
	// dashboard refresh reflects events the gateway has appended since boot.
	// Empty = serve the fixed events from New. The parse is memoized on the
	// file's size+mtime so an unchanged ledger is not re-parsed on every request.
	ledgerPath   string
	ledgerMu     sync.Mutex
	ledgerCache  []ledger.Event
	ledgerBounds [2]time.Time
	ledgerKey    string
}

// liveLedger returns the ledger and its time bounds, re-reading and re-parsing
// the file only when it has changed (keyed on size+mtime), so a dashboard
// refresh is cheap instead of O(history) on every request.
func (s *Server) liveLedger() ([]ledger.Event, time.Time, time.Time) {
	fi, err := os.Stat(s.ledgerPath)
	if err != nil {
		return nil, time.Time{}, time.Time{}
	}
	key := fmt.Sprintf("%d:%d", fi.Size(), fi.ModTime().UnixNano())
	s.ledgerMu.Lock()
	defer s.ledgerMu.Unlock()
	if key != s.ledgerKey || s.ledgerCache == nil {
		evs := readLedgerFile(s.ledgerPath)
		e, l := boundsOf(evs)
		s.ledgerCache, s.ledgerBounds, s.ledgerKey = evs, [2]time.Time{e, l}, key
	}
	return s.ledgerCache, s.ledgerBounds[0], s.ledgerBounds[1]
}

// New builds a server over events. Pass a nil slice with SetLedgerFile to serve
// a ledger file live instead.
func New(events []ledger.Event) *Server {
	s := &Server{events: events}
	s.earliest, s.latest = boundsOf(events)
	s.mux = http.NewServeMux()
	s.mux.HandleFunc("/", s.handleDashboard)
	s.mux.HandleFunc("/api/summary", s.handleSummary)
	s.mux.HandleFunc("/api/export/focus", s.handleFocus)
	s.mux.HandleFunc("/api/export/statement", s.handleStatement)
	s.mux.HandleFunc("/api/export/json", s.handleJSON)
	s.mux.HandleFunc("/run/", s.handleRun)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// SetLedgerFile makes the console read this JSONL ledger live, on every request,
// so a dashboard refresh reflects events the gateway has appended since the
// console started. Without it the console serves the fixed events passed to New.
func (s *Server) SetLedgerFile(path string) { s.ledgerPath = path }

// readLedgerFile reads a JSONL ledger from disk for live serving, deduped by id.
// It is tolerant on purpose: a blank or unparseable line — for instance a final
// line the gateway is still in the middle of appending — is skipped rather than
// failing the whole read, so a dashboard refresh during a write shows the events
// written so far instead of an error.
func readLedgerFile(path string) []ledger.Event {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	seen := map[string]bool{}
	var out []ledger.Event
	for sc.Scan() {
		b := bytes.TrimSpace(sc.Bytes())
		if len(b) == 0 {
			continue
		}
		var e ledger.Event
		if json.Unmarshal(b, &e) != nil {
			continue
		}
		if e.Validate() != nil {
			continue
		}
		if !seen[e.ID] {
			seen[e.ID] = true
			out = append(out, e)
		}
	}
	return out
}

func boundsOf(evs []ledger.Event) (earliest, latest time.Time) {
	for _, e := range evs {
		if earliest.IsZero() || e.StartsAt.Before(earliest) {
			earliest = e.StartsAt
		}
		if e.StartsAt.After(latest) {
			latest = e.StartsAt
		}
	}
	return
}

// scope is the request's view of the ledger: the events it may see and their
// time bounds. Live when a ledger file is set, otherwise the fixed events.
type scope struct {
	evs      []ledger.Event
	earliest time.Time
	latest   time.Time
}

func (s *Server) scopeFor() scope {
	if s.ledgerPath != "" {
		evs, e, l := s.liveLedger()
		return scope{evs: evs, earliest: e, latest: l}
	}
	return scope{evs: s.events, earliest: s.earliest, latest: s.latest}
}

// windowOf resolves a requested day count. Days <= 0 means "all time".
func windowOf(sc scope, reqDays int) (from time.Time, days int) {
	days = reqDays
	if days <= 0 {
		return sc.earliest, 0
	}
	// Cap the arithmetic so a huge ?days value can't overflow time.Duration (in
	// ns) and wrap to a future instant that excludes every event; never return a
	// window that predates the ledger.
	d := days
	if d > 36600 {
		d = 36600
	}
	from = sc.latest.Add(-time.Duration(d) * 24 * time.Hour)
	if from.Before(sc.earliest) {
		from = sc.earliest
	}
	return from, days
}

func inWindowOf(sc scope, from time.Time) []ledger.Event {
	if from.IsZero() || !from.After(sc.earliest) {
		return sc.evs
	}
	out := make([]ledger.Event, 0, len(sc.evs))
	for _, e := range sc.evs {
		if !e.StartsAt.Before(from) {
			out = append(out, e)
		}
	}
	return out
}

// Line is one labelled dollar figure for the API.
type Line struct {
	Key string  `json:"key"`
	USD float64 `json:"usd"`
}

// DayPoint is one day's spend, for the spend-over-time chart.
type DayPoint struct {
	Day string  `json:"day"`
	USD float64 `json:"usd"`
}

// BlockedRun is a run the gateway refused calls for. AvoidedUSD estimates what
// the blocked calls would have cost had they run, from this run's own average
// successful call, so the value of the block is legible, not just a count.
type BlockedRun struct {
	Run        string  `json:"run"`
	Agent      string  `json:"agent"`
	Model      string  `json:"model"`
	Blocked    int     `json:"blocked"`
	AvoidedUSD float64 `json:"avoided_usd"`
}

// ShadowLine is a run or a key that a cap would have stopped while the
// gateway ran in shadow mode: every one of its calls was served, so the
// dollars here were actually spent past the cap — a receipt, not an estimate.
type ShadowLine struct {
	Who      string  `json:"who"`  // the run id, or the key's label
	Kind     string  `json:"kind"` // "run" or "key"
	Agent    string  `json:"agent"`
	Model    string  `json:"model"`
	Calls    int     `json:"calls"` // calls served that a cap would have refused
	SpentUSD float64 `json:"spent_usd"`
	Reason   string  `json:"reason"`
}

// Summary is the dashboard payload.
type Summary struct {
	Days         int          `json:"days"` // 0 = all time
	RangeStart   string       `json:"range_start"`
	RangeEnd     string       `json:"range_end"`
	Events       int          `json:"events"`
	TotalUSD     float64      `json:"total_usd"`
	ByProvider   []Line       `json:"by_provider"`
	ByTeam       []Line       `json:"by_team"`
	ByAgent      []Line       `json:"by_agent"`
	ByModel      []Line       `json:"by_model"`
	LoopsBlocked int          `json:"loops_blocked"`
	BlockedRuns  []BlockedRun `json:"blocked_runs"`
	ByRun        []RunLine    `json:"by_run"` // top runs by spend — cost per run, not just per call
	ByKey        []KeyLine    `json:"by_key"` // top API keys by spend, with what the gateway refused them — a leak shows here first
	// ShadowCalls counts served calls that a cap would have refused while the
	// gateway ran in shadow mode; ShadowSpendUSD is what they actually cost.
	ShadowCalls    int          `json:"shadow_calls"`
	ShadowSpendUSD float64      `json:"shadow_spend_usd"`
	ShadowLines    []ShadowLine `json:"shadow_lines"`
	// TotalState is the confidence state of TotalUSD: "estimated" when it is
	// priced from tokens, "provider-reported" when a provider's own bill sets
	// all of it, "mixed" when a bill covers some days and priced usage the
	// rest. The team and agent figures stay estimated either way.
	TotalState string `json:"total_state"`
	// MeteredCalls is how many rows in the window came from metered calls
	// rather than a provider's bill — zero for a ledger fed by imports alone.
	MeteredCalls int `json:"metered_calls"`
	// Teams and Agents count the named ones — the "(untagged)" and
	// "(billed, not metered)" lines are rows in the lists, not teams.
	Teams  int `json:"teams"`
	Agents int `json:"agents"`
	// UnmeteredUSD is what the provider's bill has that no metered call
	// accounts for — traffic that went around the gateway, or a price the
	// table has wrong. It is shown as one line, never spread across teams.
	UnmeteredUSD float64 `json:"unmetered_usd"`
	// UnknownCostCalls is how many served calls came back without usage (a
	// stream that ended before its usage frame), so their cost is unknown and
	// counted as nothing. They are named so a run never looks cheaper than it
	// was; the provider's bill is what fills them in.
	UnknownCostCalls int        `json:"unknown_cost_calls"`
	Daily            []DayPoint `json:"daily"`
	PeakDayUSD       float64    `json:"peak_day_usd"`
	AvoidedUSD       float64    `json:"avoided_usd"`
}

// RunLine is one run's rollup: what it spent, how many calls that took, how
// many failed or were refused, and what the refusals prevented. Retries and
// dead work are where a budget actually disappears, and a per-call list hides
// that; a per-run line shows it.
type RunLine struct {
	Run        string  `json:"run"`
	Agent      string  `json:"agent"`
	Model      string  `json:"model"`
	Calls      int     `json:"calls"`   // served (successful or failed), not refused
	Failed     int     `json:"failed"`  // served but the provider returned a non-2xx
	Blocked    int     `json:"blocked"` // refused by the gateway
	SpentUSD   float64 `json:"spent_usd"`
	AvoidedUSD float64 `json:"avoided_usd"`
}

// KeyLine is one API key's rollup: the fingerprint the gateway recorded (never
// the key), what it spent, and how often the gateway refused it. A refused
// count on a key nobody recognises is the first sign of a leak.
type KeyLine struct {
	Key      string  `json:"key"`   // fingerprint (k-…-last4) or the name the caller gave
	Label    string  `json:"label"` // "…last4" or the name, for display
	Calls    int     `json:"calls"`
	Failed   int     `json:"failed"`
	Blocked  int     `json:"blocked"`
	SpentUSD float64 `json:"spent_usd"`
}

// keyLabelOf shortens a key fingerprint for display the way providers do; a
// fingerprint without a suffix (a short token) shows its hex; anything else
// is a name and is shown as given.
func keyLabelOf(key string) string {
	if len(key) >= 15 && strings.HasPrefix(key, "k-") && key[14] == '-' && isHex(key[2:14]) {
		if suffix := key[15:]; suffix != "" {
			return "…" + suffix
		}
		return key[2:14]
	}
	return key
}

func isHex(s string) bool {
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return s != ""
}

// maxRunLines bounds the per-run rollup shown on the dashboard and in the API.
const maxRunLines = 12

// unmeteredKey is the by-team / by-agent line for spend the provider billed
// that no call through the gateway accounts for.
const unmeteredKey = "(billed, not metered)"

func linesOf(m map[string]float64) []Line {
	out := make([]Line, 0, len(m))
	for k, v := range m {
		out = append(out, Line{Key: k, USD: v})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].USD != out[j].USD {
			return out[i].USD > out[j].USD
		}
		return out[i].Key < out[j].Key
	})
	return out
}

func summaryOf(sc scope, reqDays int) Summary {
	from, days := windowOf(sc, reqDays)
	evs := inWindowOf(sc, from)
	r := report.Build(evs)

	// The team, agent, run, model and day figures come from metered calls —
	// the gateway's priced rows and its refusals — never from a provider's
	// bill: a bill row carries no team, agent or run, so letting it in would
	// only turn known spend into "(untagged)". A bill row is attributed only
	// when nothing metered exists for its provider and month (a ledger fed
	// by imports alone). The total still comes from the report, where a bill
	// sets the figure for the days it covers.
	metered := report.Metered(evs)
	meteredIn := map[string]bool{}
	for _, e := range metered {
		meteredIn[e.Provider+"|"+e.Period] = true
	}
	cov := report.CostCoverage(evs)
	billed, coveredEst := map[string]float64{}, map[string]float64{} // provider|period
	attributable := make([]ledger.Event, 0, len(evs))
	for _, e := range evs {
		key := e.Provider + "|" + e.Period
		switch {
		case report.IsCostRow(e):
			billed[key] += e.CostUSD
			if !meteredIn[key] {
				attributable = append(attributable, e)
			}
		default:
			attributable = append(attributable, e)
			if cov.Covers(e) {
				coveredEst[key] += e.CostUSD
			}
		}
	}

	byTeam, byAgent := map[string]float64{}, map[string]float64{}
	byRun := map[string]int{}
	blocked := 0
	for _, e := range attributable {
		if t := e.Tags.Team; t != "" {
			byTeam[t] += e.CostUSD
		} else {
			byTeam["(untagged)"] += e.CostUSD
		}
		if a := e.Tags.Agent; a != "" {
			byAgent[a] += e.CostUSD
		} else {
			byAgent["(untagged)"] += e.CostUSD
		}
	}
	// Per run: how much it actually spent on successful calls, and how many
	// calls were refused. The average successful call estimates what each
	// refused call would have cost.
	sumByRun, cntByRun := map[string]float64{}, map[string]int{}
	callsByRun, failedByRun := map[string]int{}, map[string]int{} // every served call, and the non-2xx ones
	agentByRun, modelByRun := map[string]string{}, map[string]string{}
	var globalSum float64
	var globalCnt int
	shadowCalls, shadowSpend := 0, 0.0
	shadow := map[string]*ShadowLine{}
	for _, e := range metered {
		if e.Source != "gateway" {
			continue // a run is a gateway concept; an imported usage bucket is not a call
		}
		if wr := e.Dimensions["would_refuse"]; wr != "" { // shadow mode: served, and a cap would have refused it
			shadowCalls++
			shadowSpend += e.CostUSD
			id, kind, who := "run:"+e.Tags.Run, "run", e.Tags.Run
			if strings.HasPrefix(wr, "key ") {
				id, kind, who = "key:"+e.Dimensions["key"], "key", "key "+keyLabelOf(e.Dimensions["key"])
			}
			l := shadow[id]
			if l == nil {
				l = &ShadowLine{Who: who, Kind: kind, Agent: e.Tags.Agent, Model: e.Model, Reason: wr}
				shadow[id] = l
			}
			l.Calls++
			l.SpentUSD += e.CostUSD
		}
		if b := e.Dimensions["blocked"]; b != "" {
			if strings.HasPrefix(b, "key ") || e.Tags.Run == "" {
				continue // a key at its cap is not a runaway loop (it shows as refused under Spend by key), and a call with no run has no run to list
			}
			blocked++
			byRun[e.Tags.Run]++
			if agentByRun[e.Tags.Run] == "" {
				agentByRun[e.Tags.Run] = e.Tags.Agent
			}
			if modelByRun[e.Tags.Run] == "" {
				modelByRun[e.Tags.Run] = e.Model
			}
			continue
		}
		callsByRun[e.Tags.Run]++
		if s := e.Dimensions["status"]; s != "" && !strings.HasPrefix(s, "2") {
			failedByRun[e.Tags.Run]++ // dead work: it ran, the provider errored
		}
		if agentByRun[e.Tags.Run] == "" {
			agentByRun[e.Tags.Run] = e.Tags.Agent
		}
		if modelByRun[e.Tags.Run] == "" {
			modelByRun[e.Tags.Run] = e.Model
		}
		if e.CostUSD > 0 {
			sumByRun[e.Tags.Run] += e.CostUSD
			cntByRun[e.Tags.Run]++
			if modelByRun[e.Tags.Run] == "" {
				modelByRun[e.Tags.Run] = e.Model
			}
			if agentByRun[e.Tags.Run] == "" {
				agentByRun[e.Tags.Run] = e.Tags.Agent
			}
			globalSum += e.CostUSD
			globalCnt++
		}
	}
	avgCall := func(run string) float64 {
		if cntByRun[run] > 0 {
			return sumByRun[run] / float64(cntByRun[run])
		}
		if globalCnt > 0 {
			return globalSum / float64(globalCnt)
		}
		return 0
	}
	shadowLines := make([]ShadowLine, 0, len(shadow))
	for _, l := range shadow {
		shadowLines = append(shadowLines, *l)
	}
	sort.Slice(shadowLines, func(i, j int) bool {
		if shadowLines[i].SpentUSD != shadowLines[j].SpentUSD {
			return shadowLines[i].SpentUSD > shadowLines[j].SpentUSD
		}
		return shadowLines[i].Who < shadowLines[j].Who
	})
	if len(shadowLines) > 12 {
		shadowLines = shadowLines[:12]
	}
	runs := make([]BlockedRun, 0, len(byRun))
	var avoidedTotal float64
	for run, n := range byRun {
		av := avgCall(run) * float64(n)
		avoidedTotal += av
		runs = append(runs, BlockedRun{Run: run, Agent: agentByRun[run], Model: modelByRun[run], Blocked: n, AvoidedUSD: av})
	}
	sort.Slice(runs, func(i, j int) bool {
		if runs[i].AvoidedUSD != runs[j].AvoidedUSD {
			return runs[i].AvoidedUSD > runs[j].AvoidedUSD
		}
		return runs[i].Blocked > runs[j].Blocked
	})

	// Cost per run: every run seen (served or refused), spend-first, so retry
	// storms and dead work surface as one expensive line instead of many cheap
	// calls. Untagged calls roll up under one line and never get a drill-down.
	seenRun := map[string]bool{}
	for run := range callsByRun {
		seenRun[run] = true
	}
	for run := range byRun {
		seenRun[run] = true
	}
	byRunLines := make([]RunLine, 0, len(seenRun))
	for run := range seenRun {
		name := run
		if name == "" {
			name = "(untagged)"
		}
		byRunLines = append(byRunLines, RunLine{
			Run: name, Agent: agentByRun[run], Model: modelByRun[run],
			Calls: callsByRun[run], Failed: failedByRun[run], Blocked: byRun[run],
			SpentUSD: sumByRun[run], AvoidedUSD: avgCall(run) * float64(byRun[run]),
		})
	}
	sort.Slice(byRunLines, func(i, j int) bool {
		if byRunLines[i].SpentUSD != byRunLines[j].SpentUSD {
			return byRunLines[i].SpentUSD > byRunLines[j].SpentUSD
		}
		if byRunLines[i].AvoidedUSD != byRunLines[j].AvoidedUSD {
			return byRunLines[i].AvoidedUSD > byRunLines[j].AvoidedUSD
		}
		return byRunLines[i].Run < byRunLines[j].Run
	})
	if len(byRunLines) > maxRunLines {
		byRunLines = byRunLines[:maxRunLines]
	}

	// Cost per key: what each API key spent across every run it started, and
	// how often the gateway refused it. Only gateway rows carry a key.
	type keyAgg struct {
		calls, failed, blocked int
		spent                  float64
		name                   string
	}
	keys := map[string]*keyAgg{}
	for _, e := range metered {
		k := e.Dimensions["key"]
		if e.Source != "gateway" || k == "" {
			continue
		}
		a := keys[k]
		if a == nil {
			a = &keyAgg{}
			keys[k] = a
		}
		if n := e.Dimensions["key_name"]; n != "" && e.Dimensions["blocked"] == "" {
			a.name = n // a label from a served call; a refused call cannot rename a key on the dashboard
		}
		if e.Dimensions["blocked"] != "" {
			a.blocked++
			continue
		}
		a.calls++
		if st := e.Dimensions["status"]; st != "" && !strings.HasPrefix(st, "2") {
			a.failed++
		}
		a.spent += e.CostUSD
	}
	byKeyLines := make([]KeyLine, 0, len(keys))
	for k, a := range keys {
		label := keyLabelOf(k)
		if a.name != "" {
			label = a.name + " (" + label + ")"
		}
		byKeyLines = append(byKeyLines, KeyLine{Key: k, Label: label, Calls: a.calls, Failed: a.failed, Blocked: a.blocked, SpentUSD: a.spent})
	}
	sort.Slice(byKeyLines, func(i, j int) bool {
		if byKeyLines[i].SpentUSD != byKeyLines[j].SpentUSD {
			return byKeyLines[i].SpentUSD > byKeyLines[j].SpentUSD
		}
		if byKeyLines[i].Blocked != byKeyLines[j].Blocked {
			return byKeyLines[i].Blocked > byKeyLines[j].Blocked
		}
		return byKeyLines[i].Key < byKeyLines[j].Key
	})
	if len(byKeyLines) > maxRunLines {
		byKeyLines = byKeyLines[:maxRunLines]
	}

	// What the bill has that nothing metered accounts for, per provider and
	// month, taken from the same rows the bars are built from (so a month of
	// nothing but refusals still shows its bill): one line, so the bars add up
	// to the total. Never spread across teams — that would be a guess. A bill
	// below the estimate adds nothing.
	var unmetered float64
	for key, b := range billed {
		if meteredIn[key] && b > coveredEst[key] {
			unmetered += b - coveredEst[key]
		}
	}
	if unmetered > 0 {
		byTeam[unmeteredKey] += unmetered
		byAgent[unmeteredKey] += unmetered
	}
	unknownCost := 0
	for _, e := range metered {
		if e.Source == "gateway" && e.Dimensions["usage"] == "unknown" && e.Dimensions["blocked"] == "" {
			unknownCost++
		}
	}
	named := func(m map[string]float64) int {
		n := 0
		for k := range m {
			if !strings.HasPrefix(k, "(") {
				n++
			}
		}
		return n
	}
	// The total's state: provider-reported only when every dollar in it is
	// the provider's own figure; mixed when a bill covers some days and priced
	// usage the rest; estimated when no bill is in.
	var fromBill, fromUsage bool
	for _, e := range report.Included(evs) {
		if report.IsCostRow(e) {
			fromBill = true
		} else if e.CostUSD > 0 {
			fromUsage = true
		}
	}
	totalState := string(ledger.Estimated)
	switch {
	case fromBill && fromUsage:
		totalState = "mixed"
	case fromBill:
		totalState = string(ledger.ProviderReported)
	}

	// The day chart is drawn from what was metered day by day; a monthly bill
	// is not a day and must not appear as a spike on the 1st.
	byDay := map[string]float64{}
	for _, e := range attributable {
		byDay[e.StartsAt.UTC().Format("2006-01-02")] += e.CostUSD
	}
	dayKeys := make([]string, 0, len(byDay))
	for d := range byDay {
		dayKeys = append(dayKeys, d)
	}
	sort.Strings(dayKeys)
	daily := make([]DayPoint, 0, len(dayKeys))
	var peak float64
	for _, d := range dayKeys {
		daily = append(daily, DayPoint{Day: d, USD: byDay[d]})
		if byDay[d] > peak {
			peak = byDay[d]
		}
	}
	prov := map[string]float64{}
	for _, l := range r.ByProvider {
		prov[l.Key] = l.USD // a bill is per provider, so provider totals may use it
	}
	model := map[string]float64{}
	for _, e := range attributable {
		k := e.Model
		if k == "" {
			k = "(not grouped by model)"
		}
		model[k] += e.CostUSD
	}
	start := sc.earliest
	if !from.IsZero() && from.After(sc.earliest) {
		start = from
	}
	return Summary{
		Days:       days,
		RangeStart: start.UTC().Format("2006-01-02"), RangeEnd: sc.latest.UTC().Format("2006-01-02"),
		Events: len(evs), TotalUSD: r.TotalUSD,
		ByProvider: linesOf(prov), ByTeam: linesOf(byTeam), ByAgent: linesOf(byAgent), ByModel: linesOf(model),
		LoopsBlocked: blocked, BlockedRuns: runs, ByRun: byRunLines, ByKey: byKeyLines,
		ShadowCalls: shadowCalls, ShadowSpendUSD: shadowSpend, ShadowLines: shadowLines,
		TotalState: totalState, MeteredCalls: len(metered), UnmeteredUSD: unmetered, UnknownCostCalls: unknownCost, Teams: named(byTeam), Agents: named(byAgent),
		Daily: daily, PeakDayUSD: peak, AvoidedUSD: avoidedTotal,
	}
}

func reqDays(r *http.Request) int {
	switch r.URL.Query().Get("days") {
	case "", "all":
		return 0
	default:
		n, _ := strconv.Atoi(r.URL.Query().Get("days"))
		return n
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) handleSummary(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, summaryOf(s.scopeFor(), reqDays(r)))
}

func (s *Server) handleFocus(w http.ResponseWriter, r *http.Request) {
	sc := s.scopeFor()
	from, _ := windowOf(sc, reqDays(r))
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", "attachment; filename=axigate-focus.csv")
	_ = focus.WriteCSV(w, inWindowOf(sc, from), "axigate")
}

func (s *Server) handleJSON(w http.ResponseWriter, r *http.Request) {
	sc := s.scopeFor()
	from, _ := windowOf(sc, reqDays(r))
	writeJSON(w, http.StatusOK, report.Included(inWindowOf(sc, from)))
}
