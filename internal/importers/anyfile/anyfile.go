// Package anyfile recognizes a saved provider report by its shape and hands
// it to the right importer, so the CLI takes files in any order.
package anyfile

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/axigatelabs/axigate-finops/internal/gateway"
	"github.com/axigatelabs/axigate-finops/internal/importers/anthropic"
	"github.com/axigatelabs/axigate-finops/internal/importers/openai"
	"github.com/axigatelabs/axigate-finops/internal/ledger"
)

// Kind names what a file turned out to be.
type Kind string

const (
	OpenAIUsage      Kind = "openai-usage"
	OpenAICosts      Kind = "openai-costs"
	AnthropicUsage   Kind = "anthropic-usage"
	AnthropicCosts   Kind = "anthropic-costs"
	AnthropicCostCSV Kind = "anthropic-cost-csv"
	GatewayLedger    Kind = "gateway"
)

type probe struct {
	Object string `json:"object"`
	Data   []struct {
		Object     string            `json:"object"`
		StartingAt string            `json:"starting_at"`
		Results    []json.RawMessage `json:"results"`
	} `json:"data"`
}

// Detect names the kind of report the bytes hold, or an error naming why not.
func Detect(b []byte) (Kind, error) {
	// A file that is not JSON may be a Console CSV export. Decide by the first
	// non-space byte so a JSON report never reaches the CSV sniffer.
	trimmed := bytes.TrimLeft(b, " \t\r\n\ufeff")
	if len(trimmed) > 0 && trimmed[0] != '{' && trimmed[0] != '[' {
		return detectCSV(trimmed)
	}
	if k := detectGatewayJSONL(trimmed); k != "" {
		return k, nil
	}
	var p probe
	if err := json.Unmarshal(b, &p); err != nil {
		return "", fmt.Errorf("not a JSON report: %w", err)
	}
	if p.Object == "page" {
		// OpenAI pages: the result objects say what they are, when there are any.
		for _, d := range p.Data {
			for _, raw := range d.Results {
				var r struct {
					Object string `json:"object"`
				}
				_ = json.Unmarshal(raw, &r)
				switch r.Object {
				case "organization.usage.completions.result":
					return OpenAIUsage, nil
				case "organization.costs.result":
					return OpenAICosts, nil
				}
			}
		}
		return "", fmt.Errorf("an OpenAI page with no results to identify it (empty page?)")
	}
	if p.Data != nil && len(p.Data) > 0 && p.Data[0].StartingAt != "" {
		for _, d := range p.Data {
			for _, raw := range d.Results {
				var r map[string]json.RawMessage
				_ = json.Unmarshal(raw, &r)
				if _, ok := r["uncached_input_tokens"]; ok {
					return AnthropicUsage, nil
				}
				if _, ok := r["amount"]; ok {
					return AnthropicCosts, nil
				}
			}
		}
		return "", fmt.Errorf("an Anthropic report with no results to identify it (empty page?)")
	}
	return "", fmt.Errorf("not a report shape this tool reads (OpenAI usage/costs page, Anthropic usage/cost report)")
}

// detectGatewayJSONL recognizes the gateway's own JSONL ledger by its first
// line: a compact event object whose Source is "gateway". A provider report is
// a single JSON document, so its first line is either the whole doc (no gateway
// Source) or an opening fragment that fails to parse; either way this returns
// "" and the single-JSON path handles it.
func detectGatewayJSONL(b []byte) Kind {
	line := b
	if i := bytes.IndexByte(b, '\n'); i >= 0 {
		line = b[:i]
	}
	var probe struct {
		Source string `json:"Source"`
	}
	if json.Unmarshal(bytes.TrimSpace(line), &probe) == nil && probe.Source == gateway.SourceGateway {
		return GatewayLedger
	}
	return ""
}

// detectCSV names a CSV export by its header line. Today the only CSV this
// tool reads is Anthropic's Console cost export; another CSV is reported as
// such, with its header, rather than misread.
func detectCSV(b []byte) (Kind, error) {
	line := b
	if i := bytes.IndexByte(b, '\n'); i >= 0 {
		line = b[:i]
	}
	header := strings.ToLower(string(bytes.TrimRight(line, "\r")))
	if strings.Contains(header, "usage_date_utc") && strings.Contains(header, "cost_usd") {
		return AnthropicCostCSV, nil
	}
	return "", fmt.Errorf("a CSV, but not a report this tool reads (Anthropic cost export); header was: %s", header)
}

// Parse reads one file and returns its events and kind.
func Parse(path string) ([]ledger.Event, Kind, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	return ParseBytes(b)
}

// ParseBytes is Parse over bytes already in memory.
func ParseBytes(b []byte) ([]ledger.Event, Kind, error) {
	kind, err := Detect(b)
	if err != nil {
		return nil, "", err
	}
	var r io.Reader = bytes.NewReader(b)
	switch kind {
	case OpenAIUsage:
		p, err := openai.ParseUsage(r)
		return p.Events, kind, err
	case OpenAICosts:
		p, err := openai.ParseCosts(r)
		return p.Events, kind, err
	case AnthropicUsage:
		p, err := anthropic.ParseUsage(r)
		return p.Events, kind, err
	case AnthropicCosts:
		p, err := anthropic.ParseCosts(r)
		return p.Events, kind, err
	case AnthropicCostCSV:
		p, err := anthropic.ParseCostCSV(r)
		return p.Events, kind, err
	case GatewayLedger:
		evs, err := gateway.ParseLedger(r)
		return evs, kind, err
	}
	return nil, kind, fmt.Errorf("no importer for %s", kind)
}
