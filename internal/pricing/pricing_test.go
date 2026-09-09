package pricing

import (
	"math"
	"testing"
)

func close(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestAnthropicBucketsAreAddedToUncached(t *testing.T) {
	// 1,000 uncached in, 5,000 cache reads, 2,000 5-minute writes, 500 out on claude-sonnet-4-5.
	c := Price("anthropic", "claude-sonnet-4-5", Usage{InputTokens: 1000, CacheRead: 5000, CacheWrite5m: 2000, OutputTokens: 500})
	want := 1000*0.000003 + 5000*0.000003*0.10 + 2000*0.000003*1.25 + 500*0.000015
	if !close(c.USD, want) || !c.Listed || c.Version != Version {
		t.Fatalf("anthropic: got %+v want %.9f", c, want)
	}
	// A 1-hour write costs 2x the input rate.
	h := Price("anthropic", "claude-sonnet-4-5", Usage{CacheWrite1h: 1000})
	if !close(h.USD, 1000*0.000003*2.0) {
		t.Fatalf("anthropic 1h write: got %.9f", h.USD)
	}
}

func TestOpenAIInputIncludesCachedTokens(t *testing.T) {
	// input_tokens 1,000 of which 400 cached; gpt-4o reads at 0.5x.
	c := Price("openai", "gpt-4o", Usage{InputTokens: 1000, CacheRead: 400, OutputTokens: 100})
	want := 600*0.0000025 + 400*0.0000025*0.5 + 100*0.000010
	if !close(c.USD, want) {
		t.Fatalf("openai: got %.9f want %.9f", c.USD, want)
	}
	// Cached tokens beyond the input count never go negative.
	z := Price("openai", "gpt-4o", Usage{InputTokens: 100, CacheRead: 400})
	if !close(z.USD, 400*0.0000025*0.5) {
		t.Fatalf("openai clamp: got %.9f", z.USD)
	}
}

func TestGeminiImplicitCacheDiscount(t *testing.T) {
	c := Price("gemini", "gemini-2.5-flash", Usage{InputTokens: 1000, CacheRead: 1000})
	if !close(c.USD, 1000*0.0000003*0.25) {
		t.Fatalf("gemini: got %.9f", c.USD)
	}
}

func TestUnlistedModelIsFallbackAndFlagged(t *testing.T) {
	c := Price("openai", "gpt-99-turbo", Usage{InputTokens: 1000, OutputTokens: 1000})
	if c.Listed {
		t.Fatal("unlisted model reported as listed")
	}
	if !close(c.USD, 2000*0.000003) {
		t.Fatalf("fallback: got %.9f", c.USD)
	}
}

func TestUnknownProviderIsLeastClaiming(t *testing.T) {
	c := Price("mystery", "claude-sonnet-4-5", Usage{InputTokens: 1000, CacheRead: 1000})
	// inclusive, 0.5x reads, no write premium
	want := 0*0.000003 + 1000*0.000003*0.5
	if !close(c.USD, want) {
		t.Fatalf("unknown provider: got %.9f want %.9f", c.USD, want)
	}
}

func TestZeroCacheBucketsMatchPlainPricing(t *testing.T) {
	for _, provider := range []string{"anthropic", "openai", "gemini"} {
		c := Price(provider, "claude-sonnet-4-5", Usage{InputTokens: 1000, OutputTokens: 100})
		if !close(c.USD, 1000*0.000003+100*0.000015) {
			t.Fatalf("%s plain: got %.9f", provider, c.USD)
		}
	}
}

func TestFullPriceEquivalentCountsEveryInputToken(t *testing.T) {
	// anthropic is exclusive: InputTokens is the uncached figure, so the full
	// no-cache count is the uncached input plus every cached token.
	got := FullPriceEquivalent("anthropic", "claude-sonnet-4-5", Usage{InputTokens: 1000, CacheRead: 5000, CacheWrite5m: 2000, OutputTokens: 500})
	if !close(got, 8000*0.000003) {
		t.Fatalf("full price: got %.9f", got)
	}
}

func TestFullPriceEquivalentDoesNotDoubleCountInclusiveCache(t *testing.T) {
	// openai/gemini are inclusive: InputTokens already includes cached tokens,
	// so the full no-cache baseline is InputTokens alone — cached tokens must
	// NOT be re-added (that overstated the "reads saved" figure).
	u := Usage{InputTokens: 1000, CacheRead: 800}
	rate := Rates["gpt-4o"].InputPerToken
	got := FullPriceEquivalent("openai", "gpt-4o", u)
	if !close(got, 1000*rate) {
		t.Fatalf("inclusive full price should be InputTokens*rate=%.9f, got %.9f (cached tokens double-counted?)", 1000*rate, got)
	}
	// The honest savings (full - priced) must be positive but never exceed the
	// full baseline — the old double-count made it larger than full itself.
	saved := got - Price("openai", "gpt-4o", u).USD
	if saved <= 0 || saved >= got {
		t.Fatalf("savings should be positive and below the full baseline %.9f, got %.9f", got, saved)
	}
}

func TestTableIsSane(t *testing.T) {
	for model, r := range Rates {
		if r.InputPerToken <= 0 || r.OutputPerToken <= 0 || r.InputPerToken > 0.001 || r.OutputPerToken > 0.001 {
			t.Errorf("%s: implausible rate %+v", model, r)
		}
		if r.CacheReadMult < 0 || r.CacheReadMult > 1 || r.CacheWriteMult < 0 || r.CacheWriteMult > 3 {
			t.Errorf("%s: implausible multiplier %+v", model, r)
		}
	}
	if Version == "" {
		t.Fatal("the table must carry a version")
	}
}

func TestDatedSnapshotsResolveToTheirFamilyAndNothingElseIsGuessed(t *testing.T) {
	// Provider reports name snapshots; the price list names families.
	for model, family := range map[string]string{
		"gpt-4o-2024-08-06":          "gpt-4o",
		"gpt-4o-mini-2024-07-18":     "gpt-4o-mini",
		"claude-sonnet-4-5-20250929": "claude-sonnet-4-5",
		"gemini-2.5-flash":           "gemini-2.5-flash",
	} {
		key, rate, listed := Resolve(model)
		if !listed || key != family || rate != Rates[family] {
			t.Errorf("%s: resolved to %q listed=%v", model, key, listed)
		}
		if c := Price("openai", model, Usage{InputTokens: 1000}); c.PricedAs != family || !c.Listed {
			t.Errorf("%s: Price says %+v", model, c)
		}
	}
	// gpt-4o-mini at $0.15/M must not be priced as gpt-4o at $2.50/M or at the fallback.
	if c := Price("openai", "gpt-4o-mini-2024-07-18", Usage{InputTokens: 1_000_000}); !close(c.USD, 0.15) {
		t.Fatalf("gpt-4o-mini snapshot priced at %.4f", c.USD)
	}
	// A variant is a different price: it stays unlisted rather than borrowing its family's.
	for _, model := range []string{"gpt-4o-audio-preview-2024-12-17", "gpt-4o-mini-tts", "gpt-5.5-2026-04-23", "claude-3-5-sonnet-20241022", ""} {
		if key, _, listed := Resolve(model); listed || key != "" {
			t.Errorf("%q must stay unlisted, got %q", model, key)
		}
	}
	if got := FullPriceEquivalent("openai", "gpt-4o-mini-2024-07-18", Usage{InputTokens: 1_000_000}); !close(got, 0.15) {
		t.Fatalf("full price of a snapshot: %.4f", got)
	}
}
