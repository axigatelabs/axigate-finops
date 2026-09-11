package gateway

// budget is the seam every run counter sits behind: the in-memory controller
// (the default, per process) and the shared counter (one store, every
// replica). The gateway asks these questions and nothing else; how the tally is
// kept is the counter's business.
//
// A call is settled (record or release) by the counter that admitted it, so
// admitWhere says where the decision was made and the gateway hands that back.
type budget interface {
	noteRun(run, agent, team, model string)
	noteKey(key, agent, team, model string)
	// admitWhere decides before the request leaves and says where it decided:
	// "" for the per-process counter, "shared" (with a per-call token after a
	// colon) when the shared store answered, "local" when the store was
	// unreachable and the per-process fallback decided instead. The row
	// carries the word before the colon (counterLabel). key is the call's
	// API-key fingerprint, "" when the request carries none. In shadow mode a
	// call a cap would refuse comes back ok with the reason it would have
	// been refused for; otherwise ok and reason are exclusive.
	admitWhere(run, key string, bypass bool) (ok bool, reason, where string)
	record(run, key string, costUSD float64, suspectedLoop bool, where string)
	release(run, key string, where string)
	setRunCap(run string, maxSpendUSD float64)
	resumeRun(run string) bool
	resumeKey(key string) bool
	setKilled(on bool)
	status() Status
	setOnPause(func(StopEvent))
	// pending says whether something an operator or caller asked for has not
	// reached every replica yet (a shared store still owes it).
	pending() bool
	// mode is the counter's state right now: "" for the per-process counter,
	// "shared" or "local" for a shared store (see admitWhere).
	mode() string
}

// memoryBudget is the per-process controller behind the seam. Its decisions
// carry no "where" — there is only one place they could have been made.
type memoryBudget struct{ *controller }

func (m memoryBudget) setOnPause(f func(StopEvent)) { m.onPause = f }
func (m memoryBudget) mode() string                 { return "" }
func (m memoryBudget) pending() bool                { return false }

func (m memoryBudget) admitWhere(run, key string, bypass bool) (bool, string, string) {
	ok, reason := m.admitKeyed(run, key, bypass)
	return ok, reason, ""
}

func (m memoryBudget) record(run, key string, costUSD float64, suspectedLoop bool, _ string) {
	m.recordKeyed(run, key, costUSD, suspectedLoop)
}

func (m memoryBudget) release(run, key string, _ string) { m.releaseKeyed(run, key) }
