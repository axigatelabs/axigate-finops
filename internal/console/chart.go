package console

import (
	"fmt"
	"html/template"
	"strings"
)

// areaChart renders the spend-over-time series as an inline SVG: a soft area
// under a 2px line, a faint baseline, and an emphasized endpoint. One hue
// (money green, via CSS classes), thin marks, no axis clutter. It scales to its
// container via the viewBox. Returns an empty-state note when there is nothing
// to plot.
func areaChart(daily []DayPoint, peak float64) template.HTML {
	const w, h, pad = 1000.0, 220.0, 14.0
	if len(daily) == 0 || peak <= 0 {
		return template.HTML(`<div class="chart-empty">No spend in this range.</div>`)
	}
	// A single day cannot draw a line; show a centered marker instead.
	plotW, plotH := w-2*pad, h-2*pad
	x := func(i int) float64 {
		if len(daily) == 1 {
			return w / 2
		}
		return pad + plotW*float64(i)/float64(len(daily)-1)
	}
	y := func(v float64) float64 { return pad + plotH*(1-v/peak) }

	var line strings.Builder
	for i, d := range daily {
		if i == 0 {
			fmt.Fprintf(&line, "M %.1f %.1f", x(i), y(d.USD))
		} else {
			fmt.Fprintf(&line, " L %.1f %.1f", x(i), y(d.USD))
		}
	}
	area := line.String() + fmt.Sprintf(" L %.1f %.1f L %.1f %.1f Z", x(len(daily)-1), h-pad, x(0), h-pad)
	lastX, lastY := x(len(daily)-1), y(daily[len(daily)-1].USD)

	var b strings.Builder
	fmt.Fprintf(&b, `<svg class="chart" viewBox="0 0 %.0f %.0f" preserveAspectRatio="none" role="img" aria-label="spend over time">`, w, h)
	b.WriteString(`<defs><linearGradient id="axg-area" x1="0" y1="0" x2="0" y2="1">`)
	b.WriteString(`<stop offset="0%" class="g0"/><stop offset="100%" class="g1"/></linearGradient></defs>`)
	// baseline
	fmt.Fprintf(&b, `<line class="chart-base" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"/>`, pad, h-pad, w-pad, h-pad)
	fmt.Fprintf(&b, `<path class="chart-area" d="%s"/>`, area)
	fmt.Fprintf(&b, `<path class="chart-line" d="%s"/>`, line.String())
	fmt.Fprintf(&b, `<circle class="chart-dot" cx="%.1f" cy="%.1f" r="5"/>`, lastX, lastY)
	b.WriteString(`</svg>`)
	return template.HTML(b.String())
}
