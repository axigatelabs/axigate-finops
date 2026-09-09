package console

import (
	"strings"
	"testing"
)

func TestAreaChartRendersAndHandlesEmpty(t *testing.T) {
	// Empty / zero-peak → an honest empty state, not a broken SVG.
	if got := string(areaChart(nil, 0)); !strings.Contains(got, "chart-empty") {
		t.Fatalf("empty chart: %q", got)
	}
	if got := string(areaChart([]DayPoint{{"2026-08-01", 0}}, 0)); !strings.Contains(got, "chart-empty") {
		t.Fatalf("zero-peak chart should be empty state: %q", got)
	}
	// A real series → an svg with a line path, an area, and an endpoint dot.
	svg := string(areaChart([]DayPoint{
		{"2026-08-01", 1.0}, {"2026-08-02", 3.5}, {"2026-08-03", 2.0},
	}, 3.5))
	for _, want := range []string{"<svg", "chart-line", "chart-area", "chart-dot", "</svg>"} {
		if !strings.Contains(svg, want) {
			t.Fatalf("chart svg missing %q: %s", want, svg)
		}
	}
	// A single point must not divide by zero and must still draw a marker.
	one := string(areaChart([]DayPoint{{"2026-08-01", 2.0}}, 2.0))
	if !strings.Contains(one, "chart-dot") {
		t.Fatalf("single-point chart: %s", one)
	}
}
