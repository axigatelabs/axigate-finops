package console

import (
	"html/template"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
)

// RunCall is one request in a run's timeline, in the order it happened.
type RunCall struct {
	N          int
	Time       string
	Model      string
	Status     string
	Blocked    bool
	Reason     string // why it was refused, when blocked
	Loop       bool   // detection flagged this call as a suspected loop
	LoopSignal string
	InTok      int64
	OutTok     int64
	CacheRead  int64
	CostUSD    float64
	FirstBlock bool // the call where the cap first tripped — the divider anchor
}

// RunDetail is everything the per-run drill-down shows: the run's identity, its
// timeline, and the spend the gateway prevented by stopping the loop.
type RunDetail struct {
	Run        string
	Found      bool
	Team       string
	Project    string
	Agent      string
	Customer   string
	Provider   string
	Model      string
	Calls      []RunCall
	Total      int
	OK         int
	BlockedN   int
	FlaggedN   int
	SpentUSD   float64
	AvoidedUSD float64
	Paused     bool
}

// seqOf pulls the monotonic counter out of a "unixnano-seq" request id so calls
// that share a timestamp still order the way they happened.
func seqOf(dims map[string]string) (int64, int64) {
	id := dims["request_id"]
	nano, seq := id, ""
	if i := strings.LastIndex(id, "-"); i >= 0 {
		nano, seq = id[:i], id[i+1:]
	}
	n, _ := strconv.ParseInt(nano, 10, 64)
	s, _ := strconv.ParseInt(seq, 10, 64)
	return n, s
}

// runDetail assembles the timeline for one run from the given events.
func runDetail(events []ledger.Event, run string) RunDetail {
	d := RunDetail{Run: run}
	var evs []ledger.Event
	for _, e := range events {
		if e.Tags.Run == run {
			evs = append(evs, e)
		}
	}
	if len(evs) == 0 {
		return d
	}
	sort.Slice(evs, func(i, j int) bool {
		ni, si := seqOf(evs[i].Dimensions)
		nj, sj := seqOf(evs[j].Dimensions)
		if ni != nj {
			return ni < nj
		}
		return si < sj
	})
	d.Found = true
	var okSum float64
	var okCnt int
	firstBlockSet := false
	for i, e := range evs {
		blocked := e.Dimensions["blocked"] != ""
		call := RunCall{
			N: i + 1, Time: e.StartsAt.UTC().Format("15:04:05"),
			Model: e.Model, Status: e.Dimensions["status"],
			Blocked: blocked, Reason: e.Dimensions["blocked"],
			Loop: e.Dimensions["loop"] == "suspected", LoopSignal: e.Dimensions["loop_signal"],
			InTok: e.Usage.InputTokens, OutTok: e.Usage.OutputTokens, CacheRead: e.Usage.CacheRead,
			CostUSD: e.CostUSD,
		}
		if call.Loop {
			d.FlaggedN++
		}
		if blocked {
			d.BlockedN++
			if !firstBlockSet {
				call.FirstBlock = true
				firstBlockSet = true
			}
		} else {
			d.OK++
			if e.CostUSD > 0 {
				okSum += e.CostUSD
				okCnt++
			}
		}
		// carry the run's identity from whichever call has it
		if d.Team == "" {
			d.Team = e.Tags.Team
		}
		if d.Project == "" {
			d.Project = e.Tags.Project
		}
		if d.Agent == "" {
			d.Agent = e.Tags.Agent
		}
		if d.Customer == "" {
			d.Customer = e.Tags.Customer
		}
		if d.Provider == "" {
			d.Provider = e.Provider
		}
		if d.Model == "" {
			d.Model = e.Model
		}
		d.Calls = append(d.Calls, call)
	}
	d.Total = len(evs)
	d.SpentUSD = okSum
	avg := 0.0
	if okCnt > 0 {
		avg = okSum / float64(okCnt)
	}
	d.AvoidedUSD = avg * float64(d.BlockedN)
	d.Paused = d.BlockedN > 0
	return d
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	// r.URL.Path is already percent-decoded by net/http; decoding again here
	// double-decodes a run id containing '%' and breaks its drill-down link.
	run := strings.TrimPrefix(r.URL.Path, "/run/")
	if run == "" {
		http.NotFound(w, r)
		return
	}
	d := runDetail(s.scopeFor().evs, run)
	if !d.Found {
		w.WriteHeader(http.StatusNotFound)
	}
	if err := runTmpl.Execute(w, d); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

var runTmpl = template.Must(template.New("run").Funcs(funcs).Parse(runHTML))
