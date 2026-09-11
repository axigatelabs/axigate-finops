package gateway

import (
	"net/http"
	"testing"
)

func hdr(kv ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Set(kv[i], kv[i+1])
	}
	return h
}

func TestIdentityReadsNativeRunHeadersWithExplicitOverride(t *testing.T) {
	cases := []struct {
		name                        string
		h                           http.Header
		run, agent, parent, runFrom string
	}{
		{"nothing", hdr(), "", "", "", ""},
		{"explicit only", hdr("X-AxiGate-Run", "r1", "X-AxiGate-Agent", "planner"), "r1", "planner", "", ""},
		{"claude code session", hdr("x-claude-code-session-id", "sess-9"), "sess-9", "", "", "claude-code"},
		{"claude code subagent", hdr("x-claude-code-session-id", "sess-9", "x-claude-code-agent-id", "agent-a", "x-claude-code-parent-agent-id", "agent-root"), "sess-9", "agent-a", "agent-root", "claude-code"},
		{"explicit beats claude code", hdr("X-AxiGate-Run", "r1", "X-AxiGate-Agent", "planner", "x-claude-code-session-id", "sess-9", "x-claude-code-agent-id", "agent-a"), "r1", "planner", "", ""},
		{"litellm trace id", hdr("x-litellm-trace-id", "lt-3"), "lt-3", "", "", "litellm"},
		{"claude code beats litellm", hdr("x-litellm-trace-id", "lt-3", "x-claude-code-session-id", "sess-9"), "sess-9", "", "", "claude-code"},
		{"traceparent", hdr("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"), "4bf92f3577b34da6a3ce929d0e0e4736", "", "", "traceparent"},
		{"traceparent all-zero trace ignored", hdr("traceparent", "00-00000000000000000000000000000000-00f067aa0ba902b7-01"), "", "", "", ""},
		{"traceparent malformed ignored", hdr("traceparent", "00-notahex-00f067aa0ba902b7-01"), "", "", "", ""},
		{"traceparent uppercase ignored", hdr("traceparent", "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01"), "", "", "", ""},
		{"whitespace trimmed", hdr("x-claude-code-session-id", "  sess-9 "), "sess-9", "", "", "claude-code"},
	}
	for _, c := range cases {
		got := identityFromHeaders(c.h)
		if got.Run != c.run || got.Agent != c.agent || got.Parent != c.parent || got.RunFrom != c.runFrom {
			t.Errorf("%s: got %+v, want run=%q agent=%q parent=%q from=%q", c.name, got, c.run, c.agent, c.parent, c.runFrom)
		}
	}
}

func TestIdentityDimsRecordOnlyTheNativeFacts(t *testing.T) {
	m := map[string]string{"status": "200"}
	identityFromHeaders(hdr("X-AxiGate-Run", "r1")).dims(m)
	if len(m) != 1 {
		t.Fatalf("an explicit run adds no identity dimensions: %v", m)
	}
	identityFromHeaders(hdr("x-claude-code-session-id", "s", "x-claude-code-agent-id", "a", "x-claude-code-parent-agent-id", "p")).dims(m)
	if m["run_id_from"] != "claude-code" || m["parent_agent"] != "p" {
		t.Fatalf("native identity dims missing: %v", m)
	}
}
