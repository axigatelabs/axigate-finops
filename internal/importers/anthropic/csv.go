package anthropic

// The Anthropic Console's Cost page exports a CSV, one row per day, model,
// workspace, key, token type and context window, with the provider's own
// dollar figure in cost_usd. It needs no Admin API key: viewing your own
// costs in the Console is enough to download it. That makes it the on-ramp
// for the many users who are not organization admins.
//
// These are cost rows: cost_usd is authoritative, so nothing is priced from
// the table and every row carries provider-reported confidence. There are no
// token counts in this export, so the cache picture and the usage-vs-cost
// reconciliation (which need usage rows) are simply absent, not guessed.

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
)

// SourceCostCSV names rows lifted from the Console's Cost export.
const SourceCostCSV = "anthropic-cost-csv"

// CostCSVHeaderCols are the columns the export must carry to be recognized.
// The rest are optional and become dimensions when present.
var CostCSVHeaderCols = []string{"usage_date_utc", "cost_usd"}

// dimColumns are the export columns kept as row dimensions, in the order they
// are looked for. api_key_id is the join key to an owner mapping.
var dimColumns = []string{
	"api_key_id", "api_key", "workspace", "token_type",
	"context_window", "cost_type", "usage_type", "inference_geo", "speed",
}

// ParseCostCSV reads the Console cost export. It reports an error naming the
// columns it found when the file is a CSV but not this export, so a wrong
// file is diagnosed rather than silently misread.
func ParseCostCSV(r io.Reader) (Page, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1 // tolerate a trailing empty column
	head, err := cr.Read()
	if err != nil {
		return Page{}, fmt.Errorf("anthropic cost CSV: reading header: %w", err)
	}
	col := map[string]int{}
	for i, name := range head {
		col[strings.ToLower(strings.TrimSpace(name))] = i
	}
	for _, need := range CostCSVHeaderCols {
		if _, ok := col[need]; !ok {
			return Page{}, fmt.Errorf("anthropic cost CSV: missing the %q column; columns were: %s", need, strings.Join(head, ", "))
		}
	}
	get := func(rec []string, name string) string {
		if i, ok := col[name]; ok && i < len(rec) {
			return strings.TrimSpace(rec[i])
		}
		return ""
	}

	var out Page
	line := 1
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		line++
		if err != nil {
			return Page{}, fmt.Errorf("anthropic cost CSV: line %d: %w", line, err)
		}
		if isBlank(rec) {
			continue
		}
		day := get(rec, "usage_date_utc")
		start, err := time.Parse("2006-01-02", day)
		if err != nil {
			return Page{}, fmt.Errorf("anthropic cost CSV: line %d: usage_date_utc %q: want YYYY-MM-DD", line, day)
		}
		start = start.UTC()
		costStr := get(rec, "cost_usd")
		cost, err := strconv.ParseFloat(costStr, 64)
		if err != nil {
			return Page{}, fmt.Errorf("anthropic cost CSV: line %d: cost_usd %q: %w", line, costStr, err)
		}
		if cost < 0 {
			return Page{}, fmt.Errorf("anthropic cost CSV: line %d: cost_usd %q is negative", line, costStr)
		}
		dims := map[string]string{}
		for _, c := range dimColumns {
			if v := get(rec, c); v != "" {
				dims[c] = v
			}
		}
		e := ledger.Event{
			Source:     SourceCostCSV,
			Provider:   Provider,
			Model:      normalizeModel(get(rec, "model")),
			Dimensions: dims,
			CostUSD:    cost,
			Confidence: ledger.ProviderReported,
			StartsAt:   start,
			EndsAt:     start.Add(24 * time.Hour),
		}
		e.DeriveID()
		out.Events = append(out.Events, e)
	}
	return out, nil
}

func isBlank(rec []string) bool {
	for _, f := range rec {
		if strings.TrimSpace(f) != "" {
			return false
		}
	}
	return true
}

// normalizeModel turns the Console's display name ("Claude Haiku 4.5") into
// the API model id ("claude-haiku-4-5") so a cost row lines up with a usage
// row for the same model. The rule is deterministic: lowercase, then every
// run of spaces or dots becomes a single hyphen. It is a display label only
// (cost rows are never priced), so an unrecognized name still yields a stable
// key rather than being dropped.
func normalizeModel(display string) string {
	display = strings.TrimSpace(display)
	if display == "" {
		return ""
	}
	var b strings.Builder
	prevHyphen := false
	for _, r := range strings.ToLower(display) {
		if r == ' ' || r == '.' || r == '\t' {
			if !prevHyphen {
				b.WriteByte('-')
				prevHyphen = true
			}
			continue
		}
		b.WriteRune(r)
		prevHyphen = false
	}
	return strings.Trim(b.String(), "-")
}
