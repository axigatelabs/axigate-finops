package gateway

import (
	"encoding/json"
	"net/http"
)

// adminPrefix is the path space reserved for the gateway's own control plane.
// Provider APIs live under /v1, so this never collides with forwarded traffic.
const adminPrefix = "/_axigate/"

// serveAdmin handles the control plane: the kill switch, resuming a paused run
// after review, and a status snapshot. It is available only when an admin token
// is configured, and every call must present it. These paths are never
// forwarded upstream.
func (g *Gateway) serveAdmin(w http.ResponseWriter, r *http.Request) {
	if g.ctrl == nil || g.cfg.Control.AdminToken == "" {
		http.NotFound(w, r)
		return
	}
	if r.Header.Get("X-AxiGate-Admin") != g.cfg.Control.AdminToken {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "the admin endpoints need a matching X-AxiGate-Admin header")
		return
	}
	switch r.URL.Path {
	case adminPrefix + "status":
		writeJSON(w, http.StatusOK, g.ctrl.status())
	case adminPrefix + "kill":
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "method", "POST to engage the kill switch")
			return
		}
		g.ctrl.setKilled(true)
		writeJSON(w, http.StatusOK, g.adminReply(map[string]any{"killed": true}))
	case adminPrefix + "resume":
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "method", "POST to clear the kill switch")
			return
		}
		g.ctrl.setKilled(false)
		writeJSON(w, http.StatusOK, g.adminReply(map[string]any{"killed": false}))
	case adminPrefix + "runs/resume":
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "method", "POST to resume a run")
			return
		}
		run := r.URL.Query().Get("run")
		if run == "" {
			writeJSONError(w, http.StatusBadRequest, "bad_request", "name the run to resume with ?run=<id>")
			return
		}
		writeJSON(w, http.StatusOK, g.adminReply(map[string]any{"run": run, "resumed": g.ctrl.resumeRun(run)}))
	default:
		http.NotFound(w, r)
	}
}

// adminReply adds a note when an operator action could only be applied on this
// gateway for now, because the shared counter is unreachable.
func (g *Gateway) adminReply(m map[string]any) map[string]any {
	if g.ctrl.pending() {
		m["note"] = "part of what this gateway owes the shared store has not landed yet; it is written as soon as the store answers"
	}
	return m
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// writeJSONError writes an error in the shape both providers use, so a client's
// existing error handling reads it without special cases.
func writeJSONError(w http.ResponseWriter, code int, typ, message string) {
	writeJSON(w, code, map[string]any{"type": "error", "error": map[string]string{"type": typ, "message": message}})
}
