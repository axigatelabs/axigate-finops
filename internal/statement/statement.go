// Package statement writes the ledger as the flat CSV a person opens in a
// spreadsheet: one row per included event with its owner, confidence state,
// cost and token buckets. The CLI and the console both call it, so the file a
// user downloads is the same wherever it comes from.
package statement

import (
	"encoding/csv"
	"io"
	"sort"
	"strconv"

	"github.com/axigatelabs/axigate-finops/internal/csvsafe"
	"github.com/axigatelabs/axigate-finops/internal/ledger"
)

var header = []string{
	"period", "provider", "model", "team", "project", "customer", "agent",
	"confidence", "cost_usd", "input_tokens", "cache_read", "cache_write_5m",
	"cache_write_1h", "output_tokens", "requests", "source", "price_version", "event_id",
}

// WriteCSV writes one row per event: the full line-level detail, not the
// deduplicated total (a provider's usage and cost rows both appear, each with
// its own confidence state). Callers dedupe by id before calling.
func WriteCSV(w io.Writer, events []ledger.Event) error {
	rows := append([]ledger.Event(nil), events...)
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Period != rows[j].Period {
			return rows[i].Period < rows[j].Period
		}
		if rows[i].Provider != rows[j].Provider {
			return rows[i].Provider < rows[j].Provider
		}
		return rows[i].ID < rows[j].ID
	})
	cw := csv.NewWriter(w)
	if err := cw.Write(header); err != nil {
		return err
	}
	for _, e := range rows {
		rec := []string{
			// Guard caller/provider text (provider, model, tags) against
			// spreadsheet formula injection; period/confidence/numbers are safe.
			e.Period, csvsafe.Field(e.Provider), csvsafe.Field(e.Model),
			csvsafe.Field(e.Tags.Team), csvsafe.Field(e.Tags.Project), csvsafe.Field(e.Tags.Customer), csvsafe.Field(e.Tags.Agent),
			string(e.Confidence), strconv.FormatFloat(e.CostUSD, 'f', 6, 64),
			strconv.FormatInt(e.Usage.InputTokens, 10), strconv.FormatInt(e.Usage.CacheRead, 10),
			strconv.FormatInt(e.Usage.CacheWrite5m, 10), strconv.FormatInt(e.Usage.CacheWrite1h, 10),
			strconv.FormatInt(e.Usage.OutputTokens, 10), strconv.FormatInt(e.Usage.Requests, 10),
			e.Source, e.PriceVersion, e.ID,
		}
		if err := cw.Write(rec); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}
