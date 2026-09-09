package gateway

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
)

// ParseLedger reads the JSONL file the gateway writes, one event per line, back
// into events. It is the read side of NewJSONLRecorder, so analyze can fold
// request-level events, with their per-agent and per-run tags, into the same
// statement as the provider reports. Blank lines are skipped; a malformed or
// invalid line is an error naming the line, because this is the tool's own
// format and a bad line means a corrupt ledger, not an unknown dialect.
func ParseLedger(r io.Reader) ([]ledger.Event, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	var out []ledger.Event
	line := 0
	for sc.Scan() {
		line++
		b := bytes.TrimSpace(sc.Bytes())
		if len(b) == 0 {
			continue
		}
		var e ledger.Event
		if err := json.Unmarshal(b, &e); err != nil {
			return nil, fmt.Errorf("gateway ledger line %d: %w", line, err)
		}
		if err := e.Validate(); err != nil {
			return nil, fmt.Errorf("gateway ledger line %d: %w", line, err)
		}
		out = append(out, e)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("gateway ledger: %w", err)
	}
	return out, nil
}
