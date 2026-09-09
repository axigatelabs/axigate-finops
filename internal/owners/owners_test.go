package owners

import (
	"strings"
	"testing"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
)

const mapping = `dimension,value,team,project,customer,agent
api_key_id,key_1,customer-success,support-bot,,support-bot
project_id,proj_alpha,engineering,code-review,,
workspace_id,wrkspc_9,data,,acme-corp,
`

func TestLoadAndResolvePrecedence(t *testing.T) {
	m, err := Load(strings.NewReader(mapping))
	if err != nil {
		t.Fatal(err)
	}
	if m.Len() != 3 {
		t.Fatalf("rows: %d", m.Len())
	}
	// A key mapping wins over the project the same row also carries.
	tags, by, ok := m.Resolve(map[string]string{"project_id": "proj_alpha", "api_key_id": "key_1"})
	if !ok || by != "api_key_id" || tags.Team != "customer-success" || tags.Agent != "support-bot" {
		t.Fatalf("precedence: %+v by %q ok=%v", tags, by, ok)
	}
	// Falls through to the project when the key is unmapped.
	tags, by, ok = m.Resolve(map[string]string{"project_id": "proj_alpha", "api_key_id": "key_unmapped"})
	if !ok || by != "project_id" || tags.Project != "code-review" {
		t.Fatalf("fallthrough: %+v by %q", tags, by)
	}
	// Nothing matches: unknown, never guessed.
	if _, _, ok := m.Resolve(map[string]string{"api_key_id": "key_x", "project_id": "proj_x"}); ok {
		t.Fatal("unmapped row resolved to an owner")
	}
	if _, _, ok := Empty().Resolve(map[string]string{"api_key_id": "key_1"}); ok {
		t.Fatal("empty map resolved something")
	}
}

func TestApplyCountsUnknown(t *testing.T) {
	m, _ := Load(strings.NewReader(mapping))
	events := []ledger.Event{
		{Dimensions: map[string]string{"api_key_id": "key_1"}},
		{Dimensions: map[string]string{"workspace_id": "wrkspc_9"}},
		{Dimensions: map[string]string{"api_key_id": "nobody"}},
	}
	if unknown := m.Apply(events); unknown != 1 {
		t.Fatalf("unknown: %d", unknown)
	}
	if events[0].Tags.Team != "customer-success" || events[1].Tags.Customer != "acme-corp" || events[2].Tags != (ledger.Tags{}) {
		t.Fatalf("tags: %+v", events)
	}
}

func TestLoadRejectsBadFiles(t *testing.T) {
	cases := map[string]string{
		"no header columns": "team,project\nx,y\n",
		"missing value":     "dimension,value,team\napi_key_id,,eng\n",
		"duplicate":         "dimension,value,team\napi_key_id,k,eng\napi_key_id,k,data\n",
	}
	for name, body := range cases {
		if _, err := Load(strings.NewReader(body)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	// Extra columns and different casing are fine.
	m, err := Load(strings.NewReader("Dimension,Value,Team,Notes\napi_key_id,k,eng,whatever\n"))
	if err != nil || m.Len() != 1 {
		t.Fatalf("lenient header: %v %d", err, m.Len())
	}
}

func TestApplyDoesNotCountAlreadyTaggedEventsAsUnknown(t *testing.T) {
	m := Empty()
	evs := []ledger.Event{
		{Tags: ledger.Tags{Agent: "bot", Run: "r1"}},                // gateway-style, already tagged
		{Dimensions: map[string]string{"api_key_id": "k-unmapped"}}, // truly unknown
	}
	if unknown := m.Apply(evs); unknown != 1 {
		t.Fatalf("only the untagged, unmapped row is unknown, got %d", unknown)
	}
	if evs[0].Tags.Agent != "bot" {
		t.Fatal("existing tags must be preserved")
	}
}
