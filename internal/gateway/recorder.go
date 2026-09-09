package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
)

// JSONLRecorder appends each event as one JSON line to a writer. The file it
// writes is the request-level ledger that analyze reads later. It records only
// what the event carries, which is metadata by construction; no prompt or
// completion text ever reaches it.
//
// Recording is fail-open: a write error is reported to errFn (default: a line
// on stderr) and dropped, never propagated into the response path.
type JSONLRecorder struct {
	mu    sync.Mutex
	w     io.Writer
	errFn func(error)
}

// NewJSONLRecorder writes events to w.
func NewJSONLRecorder(w io.Writer) *JSONLRecorder {
	return &JSONLRecorder{w: w, errFn: func(err error) {
		fmt.Fprintln(os.Stderr, "gateway: dropping an event, recorder error:", err)
	}}
}

// Record writes one event line. It never panics and never blocks on a bad
// event: an event that fails validation is reported and dropped.
func (r *JSONLRecorder) Record(e ledger.Event) {
	if err := e.Validate(); err != nil {
		r.errFn(err)
		return
	}
	b, err := json.Marshal(e)
	if err != nil {
		r.errFn(err)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.w.Write(append(b, '\n')); err != nil {
		r.errFn(err)
	}
}
