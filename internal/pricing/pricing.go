// Package pricing is the versioned provider price table and the cost
// arithmetic that turns metered tokens into an estimated dollar figure.
//
// Lifted from AxiGate's internal/models on 2026-09-06 (the CostPerToken table
// and the three-bucket ModelCostCacheAware semantics), reshaped so that:
//
//   - every table has a Version, recorded on every ledger event priced by it,
//     so a later price change never silently rewrites history;
//   - cache multipliers live in the table per provider, with per-model
//     overrides, instead of in a switch statement;
//   - an unpriced model is priced at a conservative fallback and REPORTED as
//     such, so the caller can carry the "estimated" confidence state.
//
// Provider semantics, normalized here and nowhere else:
//
//   - anthropic: the usage report's uncached_input_tokens EXCLUDES the cache
//     buckets. Reads bill at 0.1x the input rate; 5-minute writes at 1.25x;
//     1-hour writes at 2x. All are added to the uncached figure.
//   - openai: the usage report's input_tokens INCLUDES cached tokens (and
//     cache-write tokens on the newer models). Cached reads bill at the
//     model's read multiplier (0.5x for the gpt-4o family, 0.1x for later
//     families); cache writes at 1x unless the model lists a write premium.
//   - gemini: prompt tokens INCLUDE cached tokens; implicit caching discounts
//     them to 0.25x; explicit context caching also bills hourly storage, which
//     this package does not model.
//
// Prices are USD per token. The table is a snapshot; a provider's own cost
// report is always the better source and carries a higher confidence state.
package pricing

import "regexp"

// Version identifies the table. Bump it, with the effective date, on any
// change to a price or a multiplier.
const Version = "2026-09-06.1"

// Usage is the token buckets a request or an import row carries. For
// providers whose reports are inclusive (openai, gemini) InputTokens includes
// CacheRead and CacheWrite; for anthropic it is the uncached figure.
type Usage struct {
	InputTokens  int64
	CacheRead    int64
	CacheWrite5m int64
	CacheWrite1h int64
	OutputTokens int64
}

// Rate is one model's price. Multipliers of zero mean "use the provider's".
type Rate struct {
	InputPerToken  float64
	OutputPerToken float64
	CacheReadMult  float64 // fraction of the input rate charged for a cache read
	CacheWriteMult float64 // fraction of the input rate charged for a 5-minute cache write
}

// Provider is a provider's default cache semantics.
type Provider struct {
	Inclusive        bool    // input token counts include the cache buckets
	CacheReadMult    float64 // default read multiplier
	CacheWrite5mMult float64 // default 5-minute write multiplier
	CacheWrite1hMult float64 // default 1-hour write multiplier
}

// Providers by name as they appear on a ledger event.
var Providers = map[string]Provider{
	"anthropic": {Inclusive: false, CacheReadMult: 0.10, CacheWrite5mMult: 1.25, CacheWrite1hMult: 2.0},
	"openai":    {Inclusive: true, CacheReadMult: 0.50, CacheWrite5mMult: 1.0, CacheWrite1hMult: 1.0},
	"gemini":    {Inclusive: true, CacheReadMult: 0.25, CacheWrite5mMult: 1.0, CacheWrite1hMult: 1.0},
}

// Fallback prices an unlisted cloud model: the conservative rate AxiGate used,
// $3 per million tokens either way.
var Fallback = Rate{InputPerToken: 0.000003, OutputPerToken: 0.000003}

// Rates by model id. Verified dates are in the comments; a price with no
// verification date was carried over from AxiGate as of 2026-08.
var Rates = map[string]Rate{
	// OpenAI — gpt-4o family caches at 0.5x (verified 2026-08-01).
	"gpt-4o":      {InputPerToken: 0.0000025, OutputPerToken: 0.000010, CacheReadMult: 0.5},
	"gpt-4o-mini": {InputPerToken: 0.00000015, OutputPerToken: 0.0000006, CacheReadMult: 0.5},
	// Anthropic.
	"claude-3-haiku-20240307":  {InputPerToken: 0.00000025, OutputPerToken: 0.00000125},
	"claude-3-sonnet-20240229": {InputPerToken: 0.000003, OutputPerToken: 0.000015},
	"claude-3-opus-20240229":   {InputPerToken: 0.000015, OutputPerToken: 0.000075},
	"claude-haiku-4-5":         {InputPerToken: 0.000001, OutputPerToken: 0.000005},
	"claude-sonnet-4-5":        {InputPerToken: 0.000003, OutputPerToken: 0.000015},
	"claude-opus-4-5":          {InputPerToken: 0.000015, OutputPerToken: 0.000075},
	// Gemini — ai.google.dev/gemini-api/docs/pricing, verified 2026-08-24.
	"gemini-2.5-flash-lite":  {InputPerToken: 0.0000001, OutputPerToken: 0.0000004},
	"gemini-2.5-flash":       {InputPerToken: 0.0000003, OutputPerToken: 0.0000025},
	"gemini-2.5-pro":         {InputPerToken: 0.00000125, OutputPerToken: 0.00001},
	"gemini-3.5-flash-lite":  {InputPerToken: 0.0000003, OutputPerToken: 0.0000025},
	"gemini-3.5-flash":       {InputPerToken: 0.0000015, OutputPerToken: 0.000009},
	"gemini-3.1-pro-preview": {InputPerToken: 0.000002, OutputPerToken: 0.000012},
}

// Cost is the priced result. Listed is false when the model was priced at
// the fallback; the caller must then carry the "estimated" state and say so.
// PricedAs is the table entry that was used, "" for the fallback.
type Cost struct {
	USD      float64
	Listed   bool
	Version  string
	PricedAs string
}

// dateSuffix is a provider's snapshot date at the end of a model id: OpenAI
// writes -YYYY-MM-DD, Anthropic -YYYYMMDD.
var dateSuffix = regexp.MustCompile(`-(\d{4}-\d{2}-\d{2}|\d{8})$`)

// Resolve finds the table entry for a model id. An exact id wins; otherwise a
// dated snapshot ("gpt-4o-mini-2024-07-18", "claude-sonnet-4-5-20250929")
// resolves to its family entry, because provider reports name snapshots and
// price lists name families. Nothing else is guessed: "gpt-4o-audio-preview"
// is a different price from "gpt-4o", so it stays unlisted and gets reported.
func Resolve(model string) (key string, rate Rate, listed bool) {
	if r, ok := Rates[model]; ok {
		return model, r, true
	}
	if family := dateSuffix.ReplaceAllString(model, ""); family != model {
		if r, ok := Rates[family]; ok {
			return family, r, true
		}
	}
	return "", Fallback, false
}

// Price turns usage into dollars under the provider's cache semantics.
// A provider not in Providers is treated as inclusive with a 0.5x read rate
// and no write premium: the least-claiming interpretation.
func Price(provider, model string, u Usage) Cost {
	key, rate, listed := Resolve(model)
	p, known := Providers[provider]
	if !known {
		p = Provider{Inclusive: true, CacheReadMult: 0.5, CacheWrite5mMult: 1.0, CacheWrite1hMult: 1.0}
	}
	readMult := p.CacheReadMult
	if rate.CacheReadMult > 0 {
		readMult = rate.CacheReadMult
	}
	write5m := p.CacheWrite5mMult
	if rate.CacheWriteMult > 0 {
		write5m = rate.CacheWriteMult
	}
	write1h := p.CacheWrite1hMult

	writes := u.CacheWrite5m + u.CacheWrite1h
	uncached := u.InputTokens
	if p.Inclusive {
		uncached = u.InputTokens - u.CacheRead - writes
		if uncached < 0 {
			uncached = 0
		}
	}
	in := rate.InputPerToken
	usd := float64(uncached)*in +
		float64(u.CacheRead)*in*readMult +
		float64(u.CacheWrite5m)*in*write5m +
		float64(u.CacheWrite1h)*in*write1h +
		float64(u.OutputTokens)*rate.OutputPerToken
	return Cost{USD: usd, Listed: listed, Version: Version, PricedAs: key}
}

// FullPriceEquivalent is what the same input tokens would have cost with no
// cache at all: the "you would have paid" half of a cache-leakage line. Output
// tokens are excluded because caching never touches them.
func FullPriceEquivalent(model string, u Usage) float64 {
	_, rate, _ := Resolve(model)
	return float64(u.InputTokens+u.CacheRead+u.CacheWrite5m+u.CacheWrite1h) * rate.InputPerToken
}
