// Package anthropic reads the pages Anthropic's Admin API usage report and
// cost report return and turns them into ledger events.
//
// Shapes verified against the API reference on 2026-09-06:
//
//	GET /v1/organizations/usage_report/messages?starting_at=<rfc3339>&bucket_width=1d&group_by[]=api_key_id&group_by[]=workspace_id&group_by[]=model
//	  data[] {starting_at, ending_at, results[]{api_key_id, workspace_id, account_id,
//	  service_account_id, model, service_tier, context_window, inference_geo,
//	  uncached_input_tokens, cache_creation{ephemeral_5m_input_tokens, ephemeral_1h_input_tokens},
//	  cache_read_input_tokens, output_tokens, server_tool_use{web_search_requests}}}, has_more, next_page
//
//	GET /v1/organizations/cost_report?starting_at=<rfc3339>&group_by[]=workspace_id&group_by[]=description
//	  data[] {starting_at, ending_at, results[]{amount, currency, description, cost_type,
//	  model, service_tier, token_type, context_window, inference_geo, workspace_id}}, has_more, next_page
//
// Buckets are RFC 3339 in UTC. uncached_input_tokens EXCLUDES the cache
// buckets; the pricing package adds them at their rates. Cost amounts are the
// lowest currency unit (cents) as a decimal string: "123.45" in USD is $1.2345.
package anthropic

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
	"github.com/axigatelabs/axigate-finops/internal/pricing"
)

const (
	SourceUsage = "anthropic-usage"
	SourceCosts = "anthropic-costs"
	Provider    = "anthropic"
)

type report[T any] struct {
	Data     []bucket[T] `json:"data"`
	HasMore  bool        `json:"has_more"`
	NextPage *string     `json:"next_page"`
}

type bucket[T any] struct {
	StartingAt string `json:"starting_at"`
	EndingAt   string `json:"ending_at"`
	Results    []T    `json:"results"`
}

type usageResult struct {
	AccountID        *string `json:"account_id"`
	APIKeyID         *string `json:"api_key_id"`
	ServiceAccountID *string `json:"service_account_id"`
	WorkspaceID      *string `json:"workspace_id"`
	Model            *string `json:"model"`
	ServiceTier      *string `json:"service_tier"`
	ContextWindow    *string `json:"context_window"`
	InferenceGeo     *string `json:"inference_geo"`
	UncachedInput    int64   `json:"uncached_input_tokens"`
	CacheCreation    struct {
		Ephemeral5m int64 `json:"ephemeral_5m_input_tokens"`
		Ephemeral1h int64 `json:"ephemeral_1h_input_tokens"`
	} `json:"cache_creation"`
	CacheRead     int64 `json:"cache_read_input_tokens"`
	OutputTokens  int64 `json:"output_tokens"`
	ServerToolUse struct {
		WebSearchRequests int64 `json:"web_search_requests"`
	} `json:"server_tool_use"`
}

type costResult struct {
	Amount        string  `json:"amount"`
	Currency      string  `json:"currency"`
	Description   *string `json:"description"`
	CostType      *string `json:"cost_type"`
	Model         *string `json:"model"`
	ServiceTier   *string `json:"service_tier"`
	TokenType     *string `json:"token_type"`
	ContextWindow *string `json:"context_window"`
	InferenceGeo  *string `json:"inference_geo"`
	WorkspaceID   *string `json:"workspace_id"`
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

func put(dims map[string]string, key string, p *string) {
	if v := str(p); v != "" {
		dims[key] = v
	}
}

func window(b bucket[usageResult]) (time.Time, time.Time, error) {
	return parseWindow(b.StartingAt, b.EndingAt)
}

func parseWindow(startingAt, endingAt string) (time.Time, time.Time, error) {
	start, err := time.Parse(time.RFC3339, startingAt)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("bucket starting_at %q: %w", startingAt, err)
	}
	end, err := time.Parse(time.RFC3339, endingAt)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("bucket ending_at %q: %w", endingAt, err)
	}
	return start.UTC(), end.UTC(), nil
}

// ParseUsage reads one usage-report page.
func ParseUsage(r io.Reader) (Page, error) {
	var rep report[usageResult]
	if err := json.NewDecoder(r).Decode(&rep); err != nil {
		return Page{}, fmt.Errorf("anthropic usage report: %w", err)
	}
	if rep.Data == nil {
		return Page{}, fmt.Errorf("anthropic usage report: no data field")
	}
	out := Page{HasMore: rep.HasMore, NextPage: str(rep.NextPage)}
	for _, b := range rep.Data {
		start, end, err := window(b)
		if err != nil {
			return Page{}, fmt.Errorf("anthropic usage report: %w", err)
		}
		for _, r := range b.Results {
			usage := ledger.Usage{
				InputTokens:  r.UncachedInput,
				CacheRead:    r.CacheRead,
				CacheWrite5m: r.CacheCreation.Ephemeral5m,
				CacheWrite1h: r.CacheCreation.Ephemeral1h,
				OutputTokens: r.OutputTokens,
			}
			dims := map[string]string{}
			put(dims, "api_key_id", r.APIKeyID)
			put(dims, "workspace_id", r.WorkspaceID)
			put(dims, "account_id", r.AccountID)
			put(dims, "service_account_id", r.ServiceAccountID)
			put(dims, "service_tier", r.ServiceTier)
			put(dims, "context_window", r.ContextWindow)
			put(dims, "inference_geo", r.InferenceGeo)
			if r.ServerToolUse.WebSearchRequests > 0 {
				dims["web_search_requests"] = strconv.FormatInt(r.ServerToolUse.WebSearchRequests, 10)
			}
			model := str(r.Model)
			cost := pricing.Price(Provider, model, pricing.Usage{
				InputTokens: usage.InputTokens, CacheRead: usage.CacheRead,
				CacheWrite5m: usage.CacheWrite5m, CacheWrite1h: usage.CacheWrite1h, OutputTokens: usage.OutputTokens,
			})
			if !cost.Listed {
				dims["unpriced_model"] = "true"
			}
			e := ledger.Event{
				Source: SourceUsage, Provider: Provider, Model: model, Dimensions: dims, Usage: usage,
				CostUSD: cost.USD, Confidence: ledger.Estimated, PriceVersion: cost.Version,
				StartsAt: start, EndsAt: end,
			}
			e.DeriveID()
			out.Events = append(out.Events, e)
		}
	}
	return out, nil
}

// ParseCosts reads one cost-report page. Amounts are cents as decimal strings.
func ParseCosts(r io.Reader) (Page, error) {
	var rep report[costResult]
	if err := json.NewDecoder(r).Decode(&rep); err != nil {
		return Page{}, fmt.Errorf("anthropic cost report: %w", err)
	}
	if rep.Data == nil {
		return Page{}, fmt.Errorf("anthropic cost report: no data field")
	}
	out := Page{HasMore: rep.HasMore, NextPage: str(rep.NextPage)}
	for _, b := range rep.Data {
		start, end, err := parseWindow(b.StartingAt, b.EndingAt)
		if err != nil {
			return Page{}, fmt.Errorf("anthropic cost report: %w", err)
		}
		for _, r := range b.Results {
			if cur := strings.ToUpper(r.Currency); cur != "" && cur != "USD" {
				return Page{}, fmt.Errorf("anthropic cost report: currency %q is not USD", r.Currency)
			}
			cents, err := strconv.ParseFloat(strings.TrimSpace(r.Amount), 64)
			if err != nil {
				return Page{}, fmt.Errorf("anthropic cost report: amount %q: %w", r.Amount, err)
			}
			dims := map[string]string{}
			put(dims, "description", r.Description)
			put(dims, "cost_type", r.CostType)
			put(dims, "token_type", r.TokenType)
			put(dims, "service_tier", r.ServiceTier)
			put(dims, "context_window", r.ContextWindow)
			put(dims, "inference_geo", r.InferenceGeo)
			put(dims, "workspace_id", r.WorkspaceID)
			e := ledger.Event{
				Source: SourceCosts, Provider: Provider, Model: str(r.Model), Dimensions: dims,
				CostUSD: cents / 100, Confidence: ledger.ProviderReported,
				StartsAt: start, EndsAt: end,
			}
			e.DeriveID()
			out.Events = append(out.Events, e)
		}
	}
	return out, nil
}
