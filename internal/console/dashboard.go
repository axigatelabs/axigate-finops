package console

import (
	"html/template"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/axigatelabs/axigate-finops/internal/statement"
)

func (s *Server) handleStatement(w http.ResponseWriter, r *http.Request) {
	sc := s.scopeFor()
	from, _ := windowOf(sc, reqDays(r))
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", "attachment; filename=axigate-statement.csv")
	_ = statement.WriteCSV(w, inWindowOf(sc, from))
}

// view is the data the dashboard template renders.
type view struct {
	Sum      Summary
	MaxTeam  float64
	MaxAgent float64
	Ranges   []rangeOpt
}

type rangeOpt struct {
	Label  string
	Days   string
	Active bool
}

func maxUSD(ls []Line) float64 {
	m := 0.0
	for _, l := range ls {
		if l.USD > m {
			m = l.USD
		}
	}
	return m
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	rd := reqDays(r)
	sum := summaryOf(s.scopeFor(), rd)
	opts := []rangeOpt{
		{"7 days", "7", rd == 7},
		{"14 days", "14", rd == 14},
		{"30 days", "30", rd == 30},
		{"All time", "all", rd == 0},
	}
	if err := dashTmpl.Execute(w, view{Sum: sum, MaxTeam: maxUSD(sum.ByTeam), MaxAgent: maxUSD(sum.ByAgent), Ranges: opts}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// fmtUSD shows two decimals for normal amounts, but keeps precision for a
// sub-cent figure that would otherwise round to $0.00. Developers on Haiku or
// gpt-4o-mini need to see micro-transactions like $0.000489, not a flat zero.
func fmtUSD(v float64) string {
	if v != 0 && math.Abs(v) < 0.005 {
		s := strconv.FormatFloat(v, 'f', 6, 64)
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
		return "$" + s
	}
	return "$" + strconv.FormatFloat(v, 'f', 2, 64)
}

var funcs = template.FuncMap{
	"usd":   fmtUSD,
	"chart": areaChart,
	"barWidth": func(v, max float64) template.CSS {
		p := 0
		if max > 0 {
			p = int(v / max * 100)
		}
		if p < 2 && v > 0 {
			p = 2
		}
		return template.CSS("width:" + strconv.Itoa(p) + "%")
	},
}

var dashTmpl = template.Must(template.New("dash").Funcs(funcs).Parse(dashHTML))
