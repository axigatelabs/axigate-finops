// Package openai reads the pages OpenAI's organization usage and costs
// endpoints return and turns them into ledger events.
//
// Shapes verified against the API reference on 2026-09-06:
//
//	GET /v1/organization/usage/completions?start_time=<unix>&bucket_width=1d&group_by=project_id,api_key_id,model
//	  page → data[] bucket{start_time, end_time, results[]{input_tokens, input_cached_tokens,
//	  input_cache_write_tokens, input_uncached_tokens, output_tokens, num_model_requests,
//	  project_id, user_id, api_key_id, model, batch, service_tier}}, has_more, next_page
//
//	GET /v1/organization/costs?start_time=<unix>&bucket_width=1d&group_by=project_id,line_item
//	  page → data[] bucket{start_time, end_time, results[]{amount{value, currency}, line_item,
//	  project_id, api_key_id}}, has_more, next_page
//
// Buckets are Unix seconds, aligned to UTC days. input_tokens INCLUDES the
// cached and cache-write tokens; the pricing package knows that. A usage row
// is priced from the table and carries the estimated state; a cost row is the
// provider's own number and carries the provider-reported state.
package openai

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
	"github.com/axigatelabs/axigate-finops/internal/pricing"
)

const (
	SourceUsage = "openai-usage"
	SourceCosts = "openai-costs"
	Provider    = "openai"
)

type page[T any] struct {
	Object   string `json:"object"`
	Data     []bucket[T]
	HasMore  bool   `json:"has_more"`
	NextPage string `json:"next_page"`
}

type bucket[T any] struct {
	StartTime int64 `json:"start_time"`
	EndTime   int64 `json:"end_time"`
	Results   []T   `json:"results"`
}

type usageResult struct {
	InputTokens           int64   `json:"input_tokens"`
	InputCachedTokens     int64   `json:"input_cached_tokens"`
	InputCacheWriteTokens int64   `json:"input_cache_write_tokens"`
	OutputTokens          int64   `json:"output_tokens"`
	NumModelRequests      int64   `json:"num_model_requests"`
	ProjectID             *string `json:"project_id"`
	UserID                *string `json:"user_id"`
	APIKeyID              *string `json:"api_key_id"`
	Model                 *string `json:"model"`
	Batch                 *bool   `json:"batch"`
	ServiceTier           *string `json:"service_tier"`
}

type costResult struct {
	Amount struct {
		Value    float64 `json:"value"`
		Currency string  `json:"currency"`
	} `json:"amount"`
	LineItem  *string `json:"line_item"`
	ProjectID *string `json:"project_id"`
	APIKeyID  *string `json:"api_key_id"`
}

// Page is what one call returned: the events and the cursor for the next call.
type Page struct {
	Events   []ledger.Event
	HasMore  bool
	NextPage string
}

func str(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// ParseUsage reads one usage/completions page.
func ParseUsage(r io.Reader) (Page, error) {
	var p page[usageResult]
	if err := json.NewDecoder(r).Decode(&p); err != nil {
		return Page{}, fmt.Errorf("openai usage page: %w", err)
	}
	if p.Object != "page" {
		return Page{}, fmt.Errorf("openai usage page: object is %q, want \"page\"", p.Object)
	}
	out := Page{HasMore: p.HasMore, NextPage: p.NextPage}
	for _, b := range p.Data {
		start, end := time.Unix(b.StartTime, 0).UTC(), time.Unix(b.EndTime, 0).UTC()
		for _, r := range b.Results {
			usage := ledger.Usage{
				InputTokens:  r.InputTokens,
				CacheRead:    r.InputCachedTokens,
				CacheWrite5m: r.InputCacheWriteTokens,
				OutputTokens: r.OutputTokens,
				Requests:     r.NumModelRequests,
			}
			dims := map[string]string{}
			if v := str(r.ProjectID); v != "" {
				dims["project_id"] = v
			}
			if v := str(r.UserID); v != "" {
				dims["user_id"] = v
			}
			if v := str(r.APIKeyID); v != "" {
				dims["api_key_id"] = v
			}
			if r.Batch != nil && *r.Batch {
				dims["batch"] = "true"
			}
			if v := str(r.ServiceTier); v != "" {
				dims["service_tier"] = v
			}
			model := str(r.Model)
			cost := pricing.Price(Provider, model, pricing.Usage{
				InputTokens: usage.InputTokens, CacheRead: usage.CacheRead, CacheWrite5m: usage.CacheWrite5m, OutputTokens: usage.OutputTokens,
			})
			e := ledger.Event{
				Source: SourceUsage, Provider: Provider, Model: model, Dimensions: dims, Usage: usage,
				CostUSD: cost.USD, Confidence: ledger.Estimated, PriceVersion: cost.Version,
				StartsAt: start, EndsAt: end,
			}
			if !cost.Listed {
				dims["unpriced_model"] = "true"
			}
			e.DeriveID()
			out.Events = append(out.Events, e)
		}
	}
	return out, nil
}

// ParseCosts reads one costs page. Amounts are the provider's own dollars.
func ParseCosts(r io.Reader) (Page, error) {
	var p page[costResult]
	if err := json.NewDecoder(r).Decode(&p); err != nil {
		return Page{}, fmt.Errorf("openai costs page: %w", err)
	}
	if p.Object != "page" {
		return Page{}, fmt.Errorf("openai costs page: object is %q, want \"page\"", p.Object)
	}
	out := Page{HasMore: p.HasMore, NextPage: p.NextPage}
	for _, b := range p.Data {
		start, end := time.Unix(b.StartTime, 0).UTC(), time.Unix(b.EndTime, 0).UTC()
		for _, r := range b.Results {
			if cur := strings.ToLower(r.Amount.Currency); cur != "" && cur != "usd" {
				return Page{}, fmt.Errorf("openai costs page: currency %q is not usd", r.Amount.Currency)
			}
			dims := map[string]string{}
			if v := str(r.LineItem); v != "" {
				dims["line_item"] = v
			}
			if v := str(r.ProjectID); v != "" {
				dims["project_id"] = v
			}
			if v := str(r.APIKeyID); v != "" {
				dims["api_key_id"] = v
			}
			e := ledger.Event{
				Source: SourceCosts, Provider: Provider, Model: modelFromLineItem(str(r.LineItem)), Dimensions: dims,
				CostUSD: r.Amount.Value, Confidence: ledger.ProviderReported,
				StartsAt: start, EndsAt: end,
			}
			e.DeriveID()
			out.Events = append(out.Events, e)
		}
	}
	return out, nil
}

// modelFromLineItem pulls the model out of a costs line item such as
// "gpt-4o-2024-08-06, input" or "gpt-4o-mini, output (cached)"; a bare id
// such as "text-embedding-3-small" is the model itself. Returns "" for
// anything with spaces ("Web search"); the line item stays in the dimensions.
func modelFromLineItem(item string) string {
	head, _, _ := strings.Cut(item, ",")
	head = strings.TrimSpace(head)
	if head == "" || strings.ContainsAny(head, " /") {
		return ""
	}
	return head
}
