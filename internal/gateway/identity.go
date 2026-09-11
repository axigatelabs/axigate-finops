package gateway

import (
	"net/http"
	"strings"
)

// Run identity is read from the headers agents already send, so a Claude Code
// session, a LiteLLM-fronted agent or an OpenTelemetry-traced client is a run
// with nothing to tag. An explicit X-AxiGate-* header always wins: what a
// caller says is never second-guessed, the fallbacks fill in only when the
// caller said nothing. The gateway still never infers a run from a request
// that carries none of these.
//
//	run:    X-AxiGate-Run → x-claude-code-session-id → x-litellm-trace-id → traceparent trace-id
//	agent:  X-AxiGate-Agent → x-claude-code-agent-id
//	parent: x-claude-code-parent-agent-id (recorded as a dimension, never a tag)
type identity struct {
	Run, Agent, Parent string
	// RunFrom names the native header the run came from ("claude-code",
	// "litellm", "traceparent"); empty when X-AxiGate-Run set it or no run.
	RunFrom string
}

func identityFromHeaders(h http.Header) identity {
	id := identity{
		Run:   strings.TrimSpace(h.Get("X-AxiGate-Run")),
		Agent: strings.TrimSpace(h.Get("X-AxiGate-Agent")),
	}
	if id.Run == "" {
		switch {
		case strings.TrimSpace(h.Get("X-Claude-Code-Session-Id")) != "":
			id.Run, id.RunFrom = strings.TrimSpace(h.Get("X-Claude-Code-Session-Id")), "claude-code"
		case strings.TrimSpace(h.Get("X-Litellm-Trace-Id")) != "":
			id.Run, id.RunFrom = strings.TrimSpace(h.Get("X-Litellm-Trace-Id")), "litellm"
		case traceID(h.Get("Traceparent")) != "":
			id.Run, id.RunFrom = traceID(h.Get("Traceparent")), "traceparent"
		}
	}
	if id.Agent == "" {
		id.Agent = strings.TrimSpace(h.Get("X-Claude-Code-Agent-Id"))
	}
	id.Parent = strings.TrimSpace(h.Get("X-Claude-Code-Parent-Agent-Id"))
	return id
}

// dims adds the identity facts that are not tags: which native header named
// the run, and the parent agent of a nested Claude Code subagent.
func (id identity) dims(m map[string]string) {
	if id.RunFrom != "" {
		m["run_id_from"] = id.RunFrom
	}
	if id.Parent != "" {
		m["parent_agent"] = id.Parent
	}
}

// traceID returns the trace-id of a W3C traceparent header
// (version-traceid-parentid-flags): 32 lowercase hex digits, not all zero.
// Anything else is ignored rather than guessed at.
func traceID(v string) string {
	parts := strings.Split(strings.TrimSpace(v), "-")
	if len(parts) < 4 || len(parts[1]) != 32 {
		return ""
	}
	allZero := true
	for _, c := range parts[1] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return ""
		}
		if c != '0' {
			allZero = false
		}
	}
	if allZero {
		return ""
	}
	return parts[1]
}
