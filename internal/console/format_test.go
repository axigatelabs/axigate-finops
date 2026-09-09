package console

import "testing"

func TestFmtUSDKeepsSubCentPrecision(t *testing.T) {
	for _, c := range []struct {
		in   float64
		want string
	}{
		{0, "$0.00"},
		{202.6, "$202.60"},
		{8.797, "$8.80"},
		{0.01, "$0.01"},
		{0.005, "$0.01"}, // rounds up to a cent, fine
		{0.000489, "$0.000489"},
		{0.004, "$0.004"},
		{0.0000041, "$0.000004"},
		{-0.0002, "$-0.0002"},
	} {
		if got := fmtUSD(c.in); got != c.want {
			t.Errorf("fmtUSD(%v) = %q want %q", c.in, got, c.want)
		}
	}
}
