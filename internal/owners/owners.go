// Package owners maps a provider's own ids (api_key_id, project_id,
// workspace_id, ...) to the customer's dimensions: team, project, customer,
// agent. A row with no mapping is unknown and reported as such, never guessed.
//
// The mapping is a CSV a spreadsheet can produce:
//
//	dimension,value,team,project,customer,agent
//	api_key_id,key_1,customer-success,support-bot,,support-bot
//	project_id,proj_alpha,engineering,code-review,,
//
// Dimensions are matched in a fixed order, most specific first, so a key
// mapping wins over a project mapping for the same row.
package owners

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
)

// Precedence is the order dimensions are consulted: the most specific id wins.
var Precedence = []string{"api_key_id", "service_account_id", "user_id", "account_id", "project_id", "workspace_id"}

// Map is the loaded mapping.
type Map struct {
	byDim map[string]map[string]ledger.Tags
	rows  int
}

// Empty is a map with no entries: every row resolves to unknown.
func Empty() *Map { return &Map{byDim: map[string]map[string]ledger.Tags{}} }

// Load reads the CSV. Unknown columns are ignored; the four owner columns are
// optional; `dimension` and `value` are required.
func Load(r io.Reader) (*Map, error) {
	rd := csv.NewReader(r)
	rd.TrimLeadingSpace = true
	header, err := rd.Read()
	if err != nil {
		return nil, fmt.Errorf("owners: reading header: %w", err)
	}
	col := map[string]int{}
	for i, h := range header {
		col[strings.ToLower(strings.TrimSpace(h))] = i
	}
	for _, need := range []string{"dimension", "value"} {
		if _, ok := col[need]; !ok {
			return nil, fmt.Errorf("owners: header has no %q column", need)
		}
	}
	m := Empty()
	get := func(rec []string, name string) string {
		i, ok := col[name]
		if !ok || i >= len(rec) {
			return ""
		}
		return strings.TrimSpace(rec[i])
	}
	line := 1
	for {
		rec, err := rd.Read()
		if err == io.EOF {
			break
		}
		line++
		if err != nil {
			return nil, fmt.Errorf("owners: line %d: %w", line, err)
		}
		dim, value := strings.ToLower(get(rec, "dimension")), get(rec, "value")
		if dim == "" || value == "" {
			return nil, fmt.Errorf("owners: line %d: dimension and value are required", line)
		}
		if m.byDim[dim] == nil {
			m.byDim[dim] = map[string]ledger.Tags{}
		}
		if _, dup := m.byDim[dim][value]; dup {
			return nil, fmt.Errorf("owners: line %d: %s %q is mapped twice", line, dim, value)
		}
		m.byDim[dim][value] = ledger.Tags{
			Team: get(rec, "team"), Project: get(rec, "project"), Customer: get(rec, "customer"), Agent: get(rec, "agent"),
		}
		m.rows++
	}
	return m, nil
}

// Len is the number of mapping rows loaded.
func (m *Map) Len() int { return m.rows }

// Resolve returns the owner for an event's dimensions and which dimension
// matched. ok is false when nothing matched: the row is unknown.
func (m *Map) Resolve(dims map[string]string) (tags ledger.Tags, matchedBy string, ok bool) {
	for _, dim := range Precedence {
		v, has := dims[dim]
		if !has {
			continue
		}
		if t, found := m.byDim[dim][v]; found {
			return t, dim, true
		}
	}
	return ledger.Tags{}, "", false
}

// Apply resolves every event in place and returns how many stayed unknown.
func (m *Map) Apply(events []ledger.Event) (unknown int) {
	for i := range events {
		t, _, ok := m.Resolve(events[i].Dimensions)
		if !ok {
			// An event that already carries tags (for example a gateway event
			// tagged from request headers) is attributed; only a truly
			// tag-less, unmapped row is unknown.
			if events[i].Tags == (ledger.Tags{}) {
				unknown++
			}
			continue
		}
		events[i].Tags = t
	}
	return unknown
}
