package csvsafe

import "testing"

func TestFieldNeutralizesFormulaTriggers(t *testing.T) {
	cases := map[string]string{
		`=HYPERLINK("http://evil",A1)`: `'=HYPERLINK("http://evil",A1)`,
		"+1+1":                         "'+1+1",
		"-2+3":                         "'-2+3",
		"@SUM(A1)":                     "'@SUM(A1)",
		"\tx":                          "'\tx",
		"\rx":                          "'\rx",
		"planner":                      "planner",    // ordinary text is untouched
		"team-alpha":                   "team-alpha", // '-' triggers only as the FIRST character
		"a=b":                          "a=b",
		"":                             "",
	}
	for in, want := range cases {
		if got := Field(in); got != want {
			t.Errorf("Field(%q) = %q, want %q", in, got, want)
		}
	}
}
