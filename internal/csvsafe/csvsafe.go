// Package csvsafe neutralizes spreadsheet formula injection in exported CSV
// cells. encoding/csv quotes for delimiters, quotes and newlines, but does
// nothing about a cell a spreadsheet will execute as a formula (=, +, -, @) or
// a DDE / whitespace trigger. Any cell carrying caller- or provider-supplied
// text (tags, model names, descriptions) is passed through Field before it is
// written, so opening the export in Excel or Sheets can never run a formula.
package csvsafe

// Field prefixes a single quote when s begins with a character a spreadsheet
// would treat as the start of a formula or command, so the cell is displayed as
// literal text. It must be applied only to text columns, never to numeric ones
// (a leading '-' on a negative number would be turned into a string).
func Field(s string) string {
	if s == "" {
		return s
	}
	switch s[0] {
	case '=', '+', '-', '@', '\t', '\r':
		return "'" + s
	}
	return s
}
