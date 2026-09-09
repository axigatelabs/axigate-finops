package report

import (
	"fmt"
	"strings"

	"github.com/axigatelabs/axigate-finops/internal/pricing"
)

func usd(v float64) string { return fmt.Sprintf("$%.2f", v) }

func pct(v float64) string { return fmt.Sprintf("%.0f%%", v*100) }

// Text renders the report the way the terminal mock shows it: every section
// says where it came from, and what the sources cannot show is said too.
func Text(r Report) string {
	var b strings.Builder
	w := func(format string, a ...any) { fmt.Fprintf(&b, format, a...) }

	w("AXIGATE SPEND DIAGNOSTIC · %s · sources: %s\n\n", strings.Join(r.Periods, ", "), strings.Join(r.Sources, ", "))
	w("  Total                              %12s\n", usd(r.TotalUSD))
	for _, l := range r.ByProvider {
		w("    %-32s %12s   %s\n", l.Key, usd(l.USD), l.Confidence)
	}
	w("\n  By owner\n")
	for _, l := range r.ByOwner {
		mark := " "
		if l.Key == "unknown" {
			mark = "!"
		}
		w("  %s %-32s %12s   %s\n", mark, l.Key, usd(l.USD), l.Confidence)
	}
	if r.UnknownUSD > 0 {
		w("\n  Unknown: %s (%s of total) sits on ids with no owner in the mapping:\n", usd(r.UnknownUSD), pct(r.UnknownShare))
		for i, l := range r.UnknownDims {
			if i == 8 {
				w("    … and %d more\n", len(r.UnknownDims)-8)
				break
			}
			w("    %-40s %12s\n", l.Key, usd(l.USD))
		}
	}
	w("\n  By model\n")
	for i, l := range r.ByModel {
		if i == 12 {
			w("    … and %d more\n", len(r.ByModel)-12)
			break
		}
		w("    %-32s %12s\n", l.Key, usd(l.USD))
	}
	if len(r.Cache) > 0 {
		w("\n  Prompt cache (from usage rows; input tokens only)\n")
		for _, c := range r.Cache {
			w("    %-10s cached %s of input tokens · reads saved %s vs full price · uncached input cost %s (the most better caching could touch; an upper bound, not a promise)\n",
				c.Provider, pct(c.CachedShare), usd(c.ReadSavingsUSD), usd(c.UncachedUSD))
		}
	}
	if len(r.Spikes) > 0 {
		w("\n  Spikes (a day at 4x or more of that id's median day)\n")
		for i, s := range r.Spikes {
			if i == 10 {
				w("    … and %d more\n", len(r.Spikes)-10)
				break
			}
			w("    %s  %s=%s  %s in one day (%.0fx its median of %s)\n", s.Day, s.Dimension, s.Value, usd(s.USD), s.Factor, usd(s.Median))
		}
	}
	if len(r.Reconciliations) > 0 {
		w("\n  Priced usage vs the provider's own cost report\n")
		for _, rc := range r.Reconciliations {
			w("    %-10s %s  estimated %s · reported %s · difference %s (%.1f%%)\n", rc.Provider, rc.Period, usd(rc.EstimatedUSD), usd(rc.ReportedUSD), usd(rc.DiffUSD), rc.DiffPct*100)
		}
	}
	if len(r.Unlisted) > 0 {
		w("\n  Not in price table %s: priced at the $%.0f/M fallback, so these estimates are guesses and any difference above includes them\n",
			pricing.Version, pricing.Fallback.InputPerToken*1e6)
		for _, l := range r.Unlisted {
			w("    %-40s %12s   %d rows\n", l.Key, usd(l.USD), l.Events)
		}
	}
	for _, n := range r.Notes {
		w("\n  note: %s", n)
	}
	w("\n\n  Not visible in provider reports: which agent, which run, which tool call, and whether anything looped.\n")
	w("  Point this tool at request logs (LiteLLM, Langfuse, Helicone, or the AxiGate gateway) for that.\n")
	return b.String()
}
