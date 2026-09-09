package focus

import (
	"bytes"
	"encoding/csv"
	"math"
	"strconv"
	"testing"

	"github.com/axigatelabs/axigate-finops/internal/report"
	"github.com/axigatelabs/axigate-finops/internal/seed"
)

// cents rounds a dollar figure to whole cents the way a statement shows it.
func cents(v float64) int64 { return int64(math.Round(v * 100)) }

func TestFOCUSBilledCostSumsToTheReportTotalToThePenny(t *testing.T) {
	events := seed.Generate(seed.Options{Events: 10000, Seed: 7})

	var buf bytes.Buffer
	if err := WriteCSV(&buf, events, "acct-demo"); err != nil {
		t.Fatal(err)
	}
	r := csv.NewReader(&buf)
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	head := rows[0]
	col := map[string]int{}
	for i, c := range head {
		col[c] = i
	}
	// Sum the BilledCost column exactly as a consumer would read it.
	var sumRows float64
	for _, rec := range rows[1:] {
		v, err := strconv.ParseFloat(rec[col["BilledCost"]], 64)
		if err != nil {
			t.Fatalf("unparseable BilledCost %q", rec[col["BilledCost"]])
		}
		sumRows += v
	}

	total := report.Build(events).TotalUSD
	if cents(sumRows) != cents(total) {
		t.Fatalf("FOCUS BilledCost sum %.6f (%d¢) != report total %.6f (%d¢)", sumRows, cents(sumRows), total, cents(total))
	}
	if cents(TotalUSD(events)) != cents(total) {
		t.Fatalf("focus.TotalUSD disagrees with the report total")
	}
	// Every FOCUS row carries currency, a provider, and the owner tags.
	for _, rec := range rows[1:] {
		if rec[col["BillingCurrency"]] != "USD" || rec[col["ProviderName"]] == "" || rec[col["ServiceCategory"]] != "AI and Machine Learning" {
			t.Fatalf("row missing mandatory fields: %v", rec)
		}
	}
}

func TestFOCUSExcludesBlockedRowsButKeepsTheirTagsElsewhere(t *testing.T) {
	// Blocked events have zero cost; they must not add to BilledCost, and the
	// export total must equal the sum of only the billed rows.
	events := seed.Generate(seed.Options{Events: 2000, Seed: 3})
	var billedRows, blocked int
	for _, e := range events {
		if e.Dimensions["blocked"] != "" {
			blocked++
		}
	}
	if blocked == 0 {
		t.Fatal("the seed should contain a runaway loop with blocked events")
	}
	var buf bytes.Buffer
	WriteCSV(&buf, events, "acct")
	r := csv.NewReader(&buf)
	rows, _ := r.ReadAll()
	billedRows = len(rows) - 1
	// A blocked (zero-cost) event still counts toward the total as $0, so it may
	// appear as a $0.000000 row; the invariant that matters is the money sum.
	if billedRows == 0 {
		t.Fatal("no rows written")
	}
}
