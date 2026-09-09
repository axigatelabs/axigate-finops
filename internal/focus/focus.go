// Package focus writes the ledger as a FOCUS export: the FinOps Open Cost and
// Usage Specification, the format finance and FinOps tools ingest without a
// custom mapping. One row per billed event, with our owner tags carried as
// provider-defined x_ columns, which FOCUS explicitly allows.
//
// FOCUS models three cost figures. We do not model commitment discounts yet, so
// billed, effective and list are the same figure, the event's cost; a later
// phase that prices a discount will split them. Nothing is invented: the sum of
// the BilledCost column equals the report total to the cent, which the tests
// assert.
package focus

import (
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strconv"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
	"github.com/axigatelabs/axigate-finops/internal/report"
)

// SpecVersion is the FOCUS version these columns target.
const SpecVersion = "1.2"

// columns is the export header, in order. The mandatory FOCUS columns first,
// then the provider-defined x_ columns carrying our attribution.
var columns = []string{
	"BillingAccountId", "BillingAccountName",
	"ChargePeriodStart", "ChargePeriodEnd", "BillingPeriodStart",
	"BilledCost", "EffectiveCost", "ListCost", "BillingCurrency",
	"ProviderName", "PublisherName", "InvoiceIssuerName",
	"ServiceName", "ServiceCategory",
	"ChargeCategory", "ChargeDescription",
	"SkuId", "ResourceId", "ConsumedQuantity", "ConsumedUnit",
	"x_Team", "x_Project", "x_Customer", "x_Agent", "x_Run",
	"x_Confidence", "x_Source", "x_PriceVersion",
}

// money renders a dollar figure the way a FOCUS consumer expects: a plain
// decimal, six places, no currency symbol or grouping.
func money(v float64) string { return strconv.FormatFloat(v, 'f', 6, 64) }

func consumedTokens(u ledger.Usage) int64 {
	return u.InputTokens + u.CacheRead + u.CacheWrite5m + u.CacheWrite1h + u.OutputTokens
}

// Row is one FOCUS record as a map, for callers that want the data rather than
// CSV. Keys are the column names above.
func Row(e ledger.Event, account string) map[string]string {
	cost := money(e.CostUSD)
	dim := func(k string) string { return e.Dimensions[k] }
	resource := dim("api_key_id")
	if resource == "" {
		resource = dim("project_id")
	}
	if resource == "" {
		resource = dim("workspace_id")
	}
	return map[string]string{
		"BillingAccountId":   account,
		"BillingAccountName": account,
		"ChargePeriodStart":  e.StartsAt.UTC().Format("2006-01-02T15:04:05Z"),
		"ChargePeriodEnd":    e.EndsAt.UTC().Format("2006-01-02T15:04:05Z"),
		"BillingPeriodStart": e.Period,
		"BilledCost":         cost,
		"EffectiveCost":      cost,
		"ListCost":           cost,
		"BillingCurrency":    "USD",
		"ProviderName":       e.Provider,
		"PublisherName":      e.Provider,
		"InvoiceIssuerName":  e.Provider,
		"ServiceName":        serviceName(e.Provider),
		"ServiceCategory":    "AI and Machine Learning",
		"ChargeCategory":     "Usage",
		"ChargeDescription":  chargeDescription(e),
		"SkuId":              e.Model,
		"ResourceId":         resource,
		"ConsumedQuantity":   strconv.FormatInt(consumedTokens(e.Usage), 10),
		"ConsumedUnit":       "Tokens",
		"x_Team":             e.Tags.Team,
		"x_Project":          e.Tags.Project,
		"x_Customer":         e.Tags.Customer,
		"x_Agent":            e.Tags.Agent,
		"x_Run":              e.Tags.Run,
		"x_Confidence":       string(e.Confidence),
		"x_Source":           e.Source,
		"x_PriceVersion":     e.PriceVersion,
	}
}

func serviceName(provider string) string {
	switch provider {
	case "openai":
		return "OpenAI API"
	case "anthropic":
		return "Anthropic API"
	case "gemini":
		return "Google Gemini API"
	default:
		return provider
	}
}

func chargeDescription(e ledger.Event) string {
	m := e.Model
	if m == "" {
		m = "usage"
	}
	return fmt.Sprintf("%s %s (%s)", serviceName(e.Provider), m, e.Confidence)
}

// WriteCSV writes the FOCUS export for the events that count toward the total,
// so the BilledCost column sums to the report total. account names the billing
// account the rows belong to.
func WriteCSV(w io.Writer, events []ledger.Event, account string) error {
	rows := report.Included(events)
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].StartsAt.Equal(rows[j].StartsAt) {
			return rows[i].StartsAt.Before(rows[j].StartsAt)
		}
		return rows[i].ID < rows[j].ID
	})
	cw := csv.NewWriter(w)
	if err := cw.Write(columns); err != nil {
		return err
	}
	for _, e := range rows {
		r := Row(e, account)
		rec := make([]string, len(columns))
		for i, c := range columns {
			rec[i] = r[c]
		}
		if err := cw.Write(rec); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

// TotalUSD sums the billed cost of the included events, the figure the export's
// BilledCost column adds up to.
func TotalUSD(events []ledger.Event) float64 {
	var t float64
	for _, e := range report.Included(events) {
		t += e.CostUSD
	}
	return t
}
