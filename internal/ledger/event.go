// Package ledger defines the one immutable event every request or import row
// becomes, and the confidence state every cost figure carries.
package ledger

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Confidence says how much a cost figure can be trusted, in increasing order.
type Confidence string

const (
	// Estimated is computed from request telemetry and a price table.
	Estimated Confidence = "estimated"
	// ProviderReported is present in the provider's own usage or cost data.
	ProviderReported Confidence = "provider-reported"
	// InvoiceReconciled is tied to charges recognized on the provider's invoice.
	InvoiceReconciled Confidence = "invoice-reconciled"
	// Final is reconciled with exceptions documented and the period closed.
	Final Confidence = "final"
)

var order = map[Confidence]int{Estimated: 0, ProviderReported: 1, InvoiceReconciled: 2, Final: 3}

// Valid reports whether c is one of the four states.
func (c Confidence) Valid() bool { _, ok := order[c]; return ok }

// AtLeast reports whether c is as trustworthy as floor. Signing requires
// AtLeast(InvoiceReconciled).
func (c Confidence) AtLeast(floor Confidence) bool { return order[c] >= order[floor] }

// Signable is the floor below which nothing is ever signed.
const Signable = InvoiceReconciled

// Usage is the token buckets a provider meters. Every field is a count.
// InputTokens follows the provider's own convention: inclusive of the cache
// buckets for openai and gemini, exclusive (uncached only) for anthropic; the
// pricing package normalizes that.
type Usage struct {
	InputTokens  int64
	CacheRead    int64
	CacheWrite5m int64
	CacheWrite1h int64
	OutputTokens int64
	Requests     int64
}

// Tags are the customer's own dimensions. Empty values mean "unknown", which
// the ledger reports as unknown rather than guessing.
type Tags struct {
	Team     string
	Project  string
	Customer string
	Agent    string
	Run      string
}

// Event is one row of the ledger: a request the gateway saw, or a row of a
// provider's usage or cost report. It is never updated; corrections are new
// events. Its ID is a deterministic digest of what it describes, so importing
// the same report twice yields the same IDs and never double counts.
type Event struct {
	ID       string
	Source   string // "gateway", "openai-usage", "openai-costs", "anthropic-usage", "anthropic-costs", "litellm-log", ...
	Provider string
	Model    string // "" when the report was not grouped by model
	// Dimensions are the provider's own ids for the row (project_id,
	// api_key_id, user_id, workspace_id, line_item, ...), the join keys to a
	// customer's owner mapping.
	Dimensions map[string]string
	Usage      Usage
	Tags       Tags
	// CostUSD is the dollar figure for the row; for a usage row it is priced
	// from the table (estimated), for a cost row it is the provider's own.
	CostUSD      float64
	Confidence   Confidence
	PriceVersion string
	// StartsAt and EndsAt are the report bucket, UTC.
	StartsAt time.Time
	EndsAt   time.Time
	// Period is the invoice period, YYYY-MM in UTC, derived from StartsAt.
	Period string
}

// DeriveID sets the deterministic id from the fields that identify the row.
// Two rows with the same source, bucket, dimensions and model are the same
// row, whatever their numbers, which is what makes a re-import idempotent.
func (e *Event) DeriveID() {
	keys := make([]string, 0, len(e.Dimensions))
	for k := range e.Dimensions {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(e.Source)
	b.WriteString("|")
	b.WriteString(e.Provider)
	b.WriteString("|")
	b.WriteString(e.Model)
	b.WriteString("|")
	b.WriteString(e.StartsAt.UTC().Format(time.RFC3339))
	b.WriteString("|")
	b.WriteString(e.EndsAt.UTC().Format(time.RFC3339))
	for _, k := range keys {
		b.WriteString("|")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(e.Dimensions[k])
	}
	sum := sha256.Sum256([]byte(b.String()))
	e.ID = hex.EncodeToString(sum[:16])
	e.Period = e.StartsAt.UTC().Format("2006-01")
}

// Validate rejects an event the ledger must not store.
func (e Event) Validate() error {
	if e.ID == "" {
		return fmt.Errorf("event has no id")
	}
	if e.Source == "" || e.Provider == "" {
		return fmt.Errorf("event %s has no source or provider", e.ID)
	}
	if !e.Confidence.Valid() {
		return fmt.Errorf("event %s has an unknown confidence state %q", e.ID, e.Confidence)
	}
	if e.CostUSD < 0 {
		return fmt.Errorf("event %s has a negative cost", e.ID)
	}
	if len(e.Period) != 7 || e.Period[4] != '-' {
		return fmt.Errorf("event %s has a malformed period %q (want YYYY-MM)", e.ID, e.Period)
	}
	if e.EndsAt.Before(e.StartsAt) {
		return fmt.Errorf("event %s ends before it starts", e.ID)
	}
	return nil
}
