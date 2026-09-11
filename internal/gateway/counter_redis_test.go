package gateway

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The shared-counter tests need a Redis on 127.0.0.1:6379 (CI runs one as a
// service; locally: docker run -d -p 127.0.0.1:6379:6379 redis:7-alpine). They
// skip, not fail, when none answers, and keep to their own key prefix, which
// they delete on cleanup.
const testRedis = "redis://127.0.0.1:6379"

func redisUp(t *testing.T) *respClient {
	t.Helper()
	c, err := newRESPClient(testRedis)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.do("PING"); err != nil {
		t.Skipf("no redis at %s: %v", testRedis, err)
	}
	return c
}

// newTestShared builds a counter on its own prefix. The probe goroutine is
// stopped so a test that drives recovery by hand is the only driver.
func newTestShared(t *testing.T, p ControlPolicy, prefix string) *sharedCounter {
	t.Helper()
	c := redisUp(t)
	if p.Now == nil {
		p.Now = fixed()
	}
	s, err := newSharedCounter(p, testRedis)
	if err != nil {
		t.Fatal(err)
	}
	s.prefix = prefix
	s.close()
	t.Cleanup(func() {
		if v, err := c.do("KEYS", prefix+"*"); err == nil {
			for _, k := range v.([]any) {
				_, _ = c.do("DEL", asString(k))
			}
		}
	})
	return s
}

func testPrefix(t *testing.T) string {
	return "axigate-test:" + strconv.FormatInt(time.Now().UnixNano(), 36) + ":" + strings.ToLower(t.Name()) + ":"
}

// waitOwed blocks until the counter owes at least n items (or 2 s).
func waitOwed(t *testing.T, s *sharedCounter, n int) {
	t.Helper()
	for i := 0; i < 200; i++ {
		s.mu.Lock()
		got := len(s.owed)
		s.mu.Unlock()
		if got >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("still fewer than %d owed items after 2 s", n)
}

// simulateGap puts a counter into the per-process fallback as an outage would.
func simulateGap(s *sharedCounter) { s.degrade(errors.New("simulated outage"), false) }

// alertCounter collects stop alerts and waits for an expected count.
type alertCounter struct {
	mu   sync.Mutex
	evs  []StopEvent
	seen chan struct{}
}

func newAlertCounter() *alertCounter { return &alertCounter{seen: make(chan struct{}, 64)} }
func (a *alertCounter) hook(ev StopEvent) {
	a.mu.Lock()
	a.evs = append(a.evs, ev)
	a.mu.Unlock()
	a.seen <- struct{}{}
}

// wait blocks until n alerts arrived (or 2 s), then a short settle time to
// catch any extra one, and returns the count.
func (a *alertCounter) wait(n int) int {
	deadline := time.After(2 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-a.seen:
		case <-deadline:
			return a.count()
		}
	}
	select {
	case <-a.seen:
	case <-time.After(50 * time.Millisecond):
	}
	return a.count()
}

func (a *alertCounter) count() int { a.mu.Lock(); defer a.mu.Unlock(); return len(a.evs) }
func (a *alertCounter) last() StopEvent {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.evs[len(a.evs)-1]
}

func TestSharedCounterBoundsABurstAcrossReplicas(t *testing.T) {
	prefix := testPrefix(t)
	pol := ControlPolicy{MaxSpendUSDPerRun: 0.10, ReserveUSDPerCall: 0.05}
	a := newTestShared(t, pol, prefix)
	b := newTestShared(t, pol, prefix)
	al := newAlertCounter()
	a.setOnPause(al.hook)
	b.setOnPause(al.hook)
	a.noteRun("r", "planner", "research", "gpt-4o")

	// Two calls reserve the whole cap, one on each replica; the third, on
	// either replica, is refused before it leaves.
	if ok, _, where := a.admitWhere("r", "", false); !ok || counterLabel(where) != "shared" {
		t.Fatalf("first call on replica A should be admitted by the store: ok=%v where=%s", ok, where)
	}
	if ok, _, _ := b.admitWhere("r", "", false); !ok {
		t.Fatal("second call on replica B should be admitted")
	}
	if ok, reason, _ := a.admitWhere("r", "", false); ok || reason != "run reached the spend cap of $0.10" {
		t.Fatalf("third call should be refused at the cap: ok=%v reason=%q", ok, reason)
	}
	if ok, reason, _ := b.admitWhere("r", "", false); ok || reason != "run reached the spend cap of $0.10" {
		t.Fatalf("the other replica sees the pause too: ok=%v reason=%q", ok, reason)
	}
	if n := al.wait(1); n != 1 {
		t.Fatalf("exactly one stop alert across replicas, got %d", n)
	}
	if last := al.last(); last.Run != "r" || last.Agent != "planner" || last.Team != "research" || last.Model != "gpt-4o" {
		t.Fatalf("alert attribution: %+v", last)
	}
}

func TestSharedCounterABurstWiderThanThePoolStaysOnTheStore(t *testing.T) {
	s := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 100}, testPrefix(t))
	const n = 200 // three times the pool
	var wg sync.WaitGroup
	var admitted, local int32
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, _, where := s.admitWhere("wide", "", false)
			if ok {
				atomic.AddInt32(&admitted, 1)
			}
			if counterLabel(where) == "local" {
				atomic.AddInt32(&local, 1)
			}
		}()
	}
	wg.Wait()
	if admitted != n || local != 0 || s.mode() != "shared" {
		t.Fatalf("admitted=%d local=%d mode=%s; a wide burst must wait for a connection, not fall back", admitted, local, s.mode())
	}
	v, _ := s.client.do("HGET", s.runKey("wide"), "inflight")
	if got, _ := asInt(v); got != n {
		t.Fatalf("store inflight = %d, want %d", got, n)
	}
}

func TestSharedCounterSettlesPausesAndResumesWithAFreshBudget(t *testing.T) {
	s := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 0.02}, testPrefix(t))
	al := newAlertCounter()
	s.setOnPause(al.hook)
	for i := 0; i < 3; i++ { // $0.0075 each: the third crosses $0.02
		ok, _, where := s.admitWhere("r", "", false)
		if !ok {
			t.Fatalf("call %d should be admitted", i+1)
		}
		s.record("r", "", 0.0075, false, where)
	}
	if ok, reason, _ := s.admitWhere("r", "", false); ok || reason != "run reached the spend cap of $0.02" {
		t.Fatalf("fourth call should be refused: ok=%v reason=%q", ok, reason)
	}
	st := s.status()
	if len(st.PausedRuns) != 1 || st.PausedRuns[0].Run != "r" || st.PausedRuns[0].Calls != 3 || st.PausedRuns[0].SpendUSD != 0.0225 {
		t.Fatalf("status: %+v", st.PausedRuns)
	}
	if st.Counter == nil || st.Counter.State != "ok" || st.Counter.Store != "redis://127.0.0.1:6379" || st.Counter.PausedTotal != 1 {
		t.Fatalf("counter report: %+v", st.Counter)
	}
	if !s.resumeRun("r") {
		t.Fatal("resume should find the paused run")
	}
	if s.resumeRun("r") {
		t.Fatal("a second resume finds nothing paused")
	}
	if ok, _, _ := s.admitWhere("r", "", false); !ok {
		t.Fatal("a resumed run gets a fresh budget")
	}
	if len(s.status().PausedRuns) != 0 {
		t.Fatal("resumed run should leave the paused list")
	}
	if n := al.wait(1); n != 1 {
		t.Fatalf("one alert for one stop, got %d", n)
	}
	// The paused set expires with the runs, so it cannot grow forever; a
	// refused call refreshes it too.
	_, _, w := s.admitWhere("q", "", false)
	s.record("q", "", 0.05, false, w)
	s.admitWhere("q", "", false)
	ttl, _ := s.client.do("TTL", s.pausedKey())
	if n, _ := asInt(ttl); n <= 0 {
		t.Fatalf("paused set should carry an expiry, TTL=%d", n)
	}
}

func TestSharedCounterEveryRunKeyExpiresEvenWithoutTags(t *testing.T) {
	s := newTestShared(t, ControlPolicy{MaxCallsPerRun: 5}, testPrefix(t))
	_, _, w := s.admitWhere("untagged", "", false) // no noteRun: nothing but the reservation is written
	if ttl, _ := s.client.do("TTL", s.runKey("untagged")); func() bool { n, _ := asInt(ttl); return n <= 0 }() {
		t.Fatalf("a run key must expire after a bare admit, TTL=%v", ttl)
	}
	s.release("untagged", "", w)
	if ttl, _ := s.client.do("TTL", s.runKey("untagged")); func() bool { n, _ := asInt(ttl); return n <= 0 }() {
		t.Fatalf("a run key must still expire after a release, TTL=%v", ttl)
	}
}

func TestSharedCounterSettlementsRunOncePerToken(t *testing.T) {
	s := newTestShared(t, ControlPolicy{MaxCallsPerRun: 10}, testPrefix(t))
	_, _, w := s.admitWhere("tok", "", false)
	s.record("tok", "", 0.10, false, w)
	s.record("tok", "", 0.10, false, w) // a retry of the same settlement
	h, _ := s.client.do("HMGET", s.runKey("tok"), "inflight", "calls", "spend")
	f := h.([]any)
	if asString(f[0]) != "0" || asString(f[1]) != "1" || asString(f[2]) != "100000" {
		t.Fatalf("a repeated settlement must not count twice: inflight=%v calls=%v spend=%v", f[0], f[1], f[2])
	}
	_, _, w2 := s.admitWhere("tok", "", false)
	s.release("tok", "", w2)
	s.release("tok", "", w2)
	h, _ = s.client.do("HGET", s.runKey("tok"), "inflight")
	if asString(h) != "0" {
		t.Fatalf("a repeated release must not decrement twice: inflight=%v", h)
	}
}

func TestSharedCounterMeetsTheCapAtExactEqualityLikeTheInMemoryCounter(t *testing.T) {
	s := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 0.80}, testPrefix(t))
	m := newController(ControlPolicy{MaxSpendUSDPerRun: 0.80, Now: fixed()})
	for _, c := range []float64{0.7, 0.1} {
		_, _, w := s.admitWhere("eq", "", false)
		s.record("eq", "", c, false, w)
		m.admit("eq", false)
		m.record("eq", c, false)
	}
	okS, _, _ := s.admitWhere("eq", "", false)
	okM, _ := m.admit("eq", false)
	if okS || okM {
		t.Fatalf("both counters should refuse at exactly the cap: shared=%v memory=%v", okS, okM)
	}
}

func TestSharedCounterCallCapInlineCapBypassAndKill(t *testing.T) {
	prefix := testPrefix(t)
	a := newTestShared(t, ControlPolicy{MaxCallsPerRun: 2, MaxSpendUSDPerRun: 1.00, AdminToken: "tok"}, prefix)
	b := newTestShared(t, ControlPolicy{MaxCallsPerRun: 2, MaxSpendUSDPerRun: 1.00, AdminToken: "tok"}, prefix)

	a.admitWhere("calls", "", false)
	b.admitWhere("calls", "", false)
	if ok, reason, _ := a.admitWhere("calls", "", false); ok || reason != "run reached the call cap of 2" {
		t.Fatalf("call cap counts reservations: ok=%v reason=%q", ok, reason)
	}
	a.setRunCap("inline", 0.01)
	_, _, w := b.admitWhere("inline", "", false)
	b.record("inline", "", 0.02, false, w)
	if ok, reason, _ := a.admitWhere("inline", "", false); ok || reason != "run reached the spend cap of $0.01" {
		t.Fatalf("inline cap binds on any replica: ok=%v reason=%q", ok, reason)
	}
	if ok, _, _ := b.admitWhere("inline", "", true); !ok {
		t.Fatal("bypass should pass a paused run")
	}
	a.setKilled(true)
	if ok, reason, _ := b.admitWhere("fresh", "", false); ok || reason != "kill switch engaged" {
		t.Fatalf("kill is cluster-wide: ok=%v reason=%q", ok, reason)
	}
	if ok, _, _ := b.admitWhere("", "", false); ok {
		t.Fatal("kill refuses untagged calls too")
	}
	if b.status().Killed != 1 {
		t.Fatal("status should show the kill switch")
	}
	a.setKilled(false)
	if ok, _, _ := b.admitWhere("fresh", "", false); !ok {
		t.Fatal("clearing the kill switch admits again")
	}
}

func TestSharedCounterReleaseKeepsInFlightHonest(t *testing.T) {
	s := newTestShared(t, ControlPolicy{MaxCallsPerRun: 2}, testPrefix(t))
	_, _, w := s.admitWhere("r", "", false)
	s.release("r", "", w) // the call never left; its reservation is returned
	s.admitWhere("r", "", false)
	if ok, _, _ := s.admitWhere("r", "", false); !ok {
		t.Fatal("after a release, two calls should still fit under a cap of 2")
	}
}

func TestSharedCounterFallsBackPerProcessWhenTheStoreIsDown(t *testing.T) {
	s, err := newSharedCounter(ControlPolicy{MaxSpendUSDPerRun: 0.01, Now: fixed()}, "redis://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	al := newAlertCounter()
	s.setOnPause(al.hook)
	if s.mode() != "local" {
		t.Fatalf("a store that is down at boot means local decisions, got %q", s.mode())
	}
	ok, _, where := s.admitWhere("r", "", false)
	if !ok || where != "local" {
		t.Fatalf("local fallback admits under the cap and says so: ok=%v where=%s", ok, where)
	}
	s.record("r", "", 0.02, false, where)
	if ok, reason, _ := s.admitWhere("r", "", false); ok || reason != "run reached the spend cap of $0.01" {
		t.Fatalf("local fallback refuses at the cap: ok=%v reason=%q", ok, reason)
	}
	st := s.status()
	st2 := s.status()
	if st.Counter == nil || st.Counter.State != "degraded" || st.Counter.DegradedSince == "" || len(st.PausedRuns) != 1 {
		t.Fatalf("degraded status should say so and list local pauses: %+v %+v", st.Counter, st.PausedRuns)
	}
	if st.Counter.LocalDecided != 2 || st2.Counter.LocalDecided != 2 {
		t.Fatalf("decided_locally counts cap decisions only (2), not lookups: %d then %d", st.Counter.LocalDecided, st2.Counter.LocalDecided)
	}
	if n := al.wait(1); n != 1 {
		t.Fatalf("the local fallback still alerts once, got %d", n)
	}
}

func TestSharedCounterWrongPasswordIsAConfigurationErrorAtBoot(t *testing.T) {
	redisUp(t)
	if _, err := newSharedCounter(ControlPolicy{Now: fixed()}, "redis://:definitely-wrong@127.0.0.1:6379"); err == nil || !strings.Contains(err.Error(), "answered with an error") {
		t.Fatalf("a store that answers AUTH with an error must not start silently: %v", err)
	} else if strings.Contains(err.Error(), "definitely-wrong") {
		t.Fatal("the password must not be printed")
	}
	// And the gateway refuses to start on it rather than quietly capping per process.
	g := New(Config{Upstream: "http://127.0.0.1:1", Provider: "openai", Control: ControlPolicy{MaxSpendUSDPerRun: 1, Now: fixed()}, SharedCounter: "redis://:definitely-wrong@127.0.0.1:6379", Now: fixed()})
	if g.CounterError() == nil {
		t.Fatal("a shared-counter configuration error must be reported by the gateway")
	}
}

func TestSharedCounterAnErrorOnOneKeyDecidesOneCallNotTheCounter(t *testing.T) {
	s := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 0.01}, testPrefix(t))
	// A colliding key of the wrong type makes the scripts for that run fail.
	if _, err := s.client.do("SET", s.runKey("clash"), "not-a-hash"); err != nil {
		t.Fatal(err)
	}
	ok, _, where := s.admitWhere("clash", "", false)
	if !ok || where != "local" || s.mode() != "shared" {
		t.Fatalf("one bad key decides that call locally and leaves the counter on the store: ok=%v where=%s mode=%s", ok, where, s.mode())
	}
	if st := s.status(); st.Counter.OneOffDecided != 1 || !strings.Contains(st.Counter.LastError, "WRONGTYPE") {
		t.Fatalf("status must count and report it: %+v", st.Counter)
	}
	// A store that cannot serve at all switches the counter over at once.
	s.fail(respError("LOADING Redis is loading the dataset in memory"))
	if s.mode() != "local" {
		t.Fatal("a reply that the store cannot serve is an outage")
	}
}

func TestSharedCounterOneDroppedAnswerDecidesOneCallNotTheCounter(t *testing.T) {
	s := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 1}, testPrefix(t))
	s.fail(errPoolBusy)
	if s.mode() != "shared" {
		t.Fatal("a busy pool is never an outage")
	}
	s.fail(io.EOF)
	s.fail(io.EOF)
	if s.mode() != "shared" {
		t.Fatal("a dropped answer or two is one bad connection, not an outage")
	}
	s.answered()
	s.fail(io.EOF)
	s.fail(io.EOF)
	if s.mode() != "shared" {
		t.Fatal("the streak resets when the store answers")
	}
	s.fail(io.EOF)
	if s.mode() != "local" {
		t.Fatal("three dropped answers in a row is an outage")
	}
	s2 := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 1}, testPrefix(t))
	s2.fail(&net.OpError{Op: "dial", Err: errors.New("connection refused")})
	if s2.mode() != "local" {
		t.Fatal("a refused dial means the store is not there")
	}
}

func TestSharedCounterKillSwitchSurvivesAGapInEveryDirection(t *testing.T) {
	prefix := testPrefix(t)
	a := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 1}, prefix)
	b := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 1}, prefix)

	// (1) Engaged in the store before a gap: the fallback honours it.
	a.setKilled(true)
	b.admitWhere("x", "", false) // b learns the store's state
	simulateGap(b)
	if ok, reason, _ := b.admitWhere("x", "", false); ok || reason != "kill switch engaged" {
		t.Fatalf("a kill known before the gap must hold during it: ok=%v reason=%q", ok, reason)
	}
	b.recovered()
	a.setKilled(false)

	// (2) Engaged during a gap: written back on recovery, so the other replica refuses.
	simulateGap(a)
	a.setKilled(true)
	if ok, _, _ := a.admitWhere("y", "", false); ok {
		t.Fatal("kill during the gap refuses locally")
	}
	a.recovered()
	if a.mode() != "shared" {
		t.Fatal("a should have recovered")
	}
	if ok, reason, _ := b.admitWhere("y", "", false); ok || reason != "kill switch engaged" {
		t.Fatalf("a kill engaged in a gap must reach the store: ok=%v reason=%q", ok, reason)
	}
	// (3) Cleared during a gap: the store's copy is cleared on recovery.
	simulateGap(a)
	a.setKilled(false)
	a.recovered()
	if ok, _, _ := b.admitWhere("z", "", false); !ok {
		t.Fatal("clearing the kill in a gap must reach the store")
	}
	if a.status().Killed != 0 {
		t.Fatal("status after recovery should show the kill cleared")
	}
	// (4) Engaged on ANOTHER replica while this one is in a gap: this one's
	// recovery must not clear it, and must learn it.
	a.admitWhere("w", "", false) // a last saw: not killed
	simulateGap(a)
	b.setKilled(true)
	a.recovered()
	if ok, reason, _ := b.admitWhere("w", "", false); ok || reason != "kill switch engaged" {
		t.Fatalf("a replica recovering from a gap must not overwrite a kill it did not set: ok=%v reason=%q", ok, reason)
	}
	if ok, reason, _ := a.admitWhere("w", "", false); ok || reason != "kill switch engaged" {
		t.Fatalf("the recovering replica must learn the kill: ok=%v reason=%q", ok, reason)
	}
	b.setKilled(false)
}

func TestSharedCounterSettlesEachCallWhereItWasAdmitted(t *testing.T) {
	prefix := testPrefix(t)
	s := newTestShared(t, ControlPolicy{MaxCallsPerRun: 3, MaxSpendUSDPerRun: 1}, prefix)
	// Admitted by the store, settled during a gap: owed and delivered on
	// recovery, so no phantom reservation is left behind.
	_, _, w := s.admitWhere("p", "", false)
	if counterLabel(w) != "shared" {
		t.Fatal("expected the store to admit")
	}
	simulateGap(s)
	s.record("p", "", 0.10, false, w)
	if st := s.status(); st.Counter.Pending != 1 {
		t.Fatalf("the settlement should be owed to the store: %+v", st.Counter)
	}
	s.recovered()
	h, _ := s.client.do("HMGET", s.runKey("p"), "inflight", "calls", "spend")
	f := h.([]any)
	if asString(f[0]) != "0" || asString(f[1]) != "1" || asString(f[2]) != "100000" {
		t.Fatalf("store after recovery: inflight=%v calls=%v spend=%v, want 0/1/100000", f[0], f[1], f[2])
	}
	// Admitted locally during a gap, settled after recovery: stays local, and
	// the store's tally is untouched — that spend is not merged.
	simulateGap(s)
	_, _, w = s.admitWhere("q", "", false)
	if w != "local" {
		t.Fatal("expected the fallback to admit")
	}
	s.recovered()
	s.record("q", "", 0.42, false, w)
	if v, _ := s.client.do("EXISTS", s.runKey("q")); v != int64(0) {
		t.Fatal("a locally admitted call must not be recorded into the store")
	}
}

func TestSharedCounterWritesBackLocalPausesAndResumesWhenTheStoreReturns(t *testing.T) {
	prefix := testPrefix(t)
	s := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 0.01}, prefix)
	other := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 0.01}, prefix)
	_, _, w := s.admitWhere("old", "", false)
	s.record("old", "", 0.05, false, w)
	simulateGap(s)
	s.resumeRun("old") // remembered, delivered with the gap
	_, _, w = s.admitWhere("blip", "", false)
	s.record("blip", "", 0.05, false, w) // paused locally: owed to the store
	waitOwed(t, s, 2)                    // the pause hook runs on its own goroutine; 'u' + 'p'
	s.recovered()
	if s.mode() != "shared" {
		t.Fatalf("should have recovered, mode=%q", s.mode())
	}
	if ok, reason, _ := other.admitWhere("blip", "", false); ok || reason != "run reached the spend cap of $0.01" {
		t.Fatalf("the gap's pause should be in the store: ok=%v reason=%q", ok, reason)
	}
	if ok, _, _ := other.admitWhere("old", "", false); !ok {
		t.Fatal("the resume asked for during the gap should have reached the store")
	}
	if st := s.status(); st.Counter.Outages != 1 || st.Counter.LastOutage == "" || st.Counter.Pending != 0 {
		t.Fatalf("the gap should stay visible after recovery with nothing owed: %+v", st.Counter)
	}
}

func TestSharedCounterKeepsTheGapWhenWriteBackFailsAndDeliversOnlyOnce(t *testing.T) {
	s := newTestShared(t, ControlPolicy{MaxCallsPerRun: 10}, testPrefix(t))
	_, _, w1 := s.admitWhere("good", "", false)
	_, _, w2 := s.admitWhere("good", "", false)
	simulateGap(s)
	s.record("good", "", 0.10, false, w1)
	s.record("good", "", 0.10, false, w2)
	// The store "answers the ping" but the write-back fails: nothing is
	// dropped, the gap continues.
	good := s.client
	dead, _ := newRESPClient("redis://127.0.0.1:1")
	s.client = dead
	s.recovered()
	if s.mode() != "local" {
		t.Fatal("a failed write-back must keep the fallback in charge")
	}
	s.client = good
	s.recovered()
	h, _ := s.client.do("HMGET", s.runKey("good"), "inflight", "calls", "spend")
	f := h.([]any)
	if s.mode() != "shared" || asString(f[0]) != "0" || asString(f[1]) != "2" || asString(f[2]) != "200000" {
		t.Fatalf("after a retried write-back the store must hold each settlement exactly once: mode=%s inflight=%v calls=%v spend=%v", s.mode(), f[0], f[1], f[2])
	}
}

func TestSharedCounterAnOwedItemTheStoreRefusesIsDroppedNotRetriedForever(t *testing.T) {
	s := newTestShared(t, ControlPolicy{MaxCallsPerRun: 10}, testPrefix(t))
	_, _, w := s.admitWhere("fine", "", false)
	simulateGap(s)
	s.owe(owed{kind: 'r', run: "poison", token: "badtoken", costUSD: 0.10}) // a settlement for a run the store will refuse
	if _, err := s.client.do("SET", s.runKey("poison"), "not-a-hash"); err != nil {
		t.Fatal(err)
	}
	s.record("fine", "", 0.10, false, w)
	s.recovered()
	if s.mode() != "shared" {
		t.Fatal("one refused item must not pin the counter in the gap")
	}
	st := s.status()
	if st.Counter.Refused != 1 || st.Counter.Pending != 0 {
		t.Fatalf("the refused item is dropped and counted, the rest delivered: %+v", st.Counter)
	}
	if h, _ := s.client.do("HGET", s.runKey("fine"), "calls"); asString(h) != "1" {
		t.Fatalf("the item behind the poison must still land: calls=%v", h)
	}
}

func TestSharedCounterOwedWorkIsDeliveredWhileHealthy(t *testing.T) {
	s := newTestShared(t, ControlPolicy{MaxCallsPerRun: 10}, testPrefix(t))
	// An operator action the store could not take at the time (a busy pool)
	// is remembered and delivered by the next flush, without any outage.
	s.mu.Lock()
	s.killDirty, s.killed = true, true
	s.local.setKilled(true)
	s.mu.Unlock()
	s.owe(owed{kind: 'u', run: "r"})
	if !s.pending() {
		t.Fatal("pending should say the store still owes something")
	}
	s.flush()
	if v, _ := s.client.do("EXISTS", s.killKey()); v != int64(1) {
		t.Fatal("the kill switch must reach the store on the next flush")
	}
	if s.pending() {
		t.Fatalf("nothing should be owed after a flush: %+v", s.report())
	}
	if d := s.report().Delivered; d != 2 {
		t.Fatalf("owed_delivered counts owed items only (the kill and the resume): %d", d)
	}
	_, _, w := s.admitWhere("plain", "", false)
	s.record("plain", "", 0.01, false, w) // a healthy settlement is not an owed delivery
	if d := s.report().Delivered; d != 2 {
		t.Fatalf("a healthy settlement must not count as an owed delivery: %d", d)
	}
	s.setKilled(false)
}

func TestReasonTextDecodesStoredCodes(t *testing.T) {
	for code, want := range map[string]string{"spend:15000": "run reached the spend cap of $0.015", "spend:1000000": "run reached the spend cap of $1.00", "calls:6": "run reached the call cap of 6", "loop": "suspected loop", "text:custom reason": "custom reason"} {
		if got := reasonText(code); got != want {
			t.Errorf("reasonText(%q) = %q, want %q", code, got, want)
		}
	}
	if redact("redis://user:pw@cache:6379/0?password=hunter2#x") != "redis://cache:6379/0" {
		t.Fatalf("redact must strip the password and the query: %s", redact("redis://user:pw@cache:6379/0?password=hunter2#x"))
	}
}

// fakeRESP is a tiny server for client tests: it answers +PONG to anything,
// and can be told to close a connection after its first reply or to stall.
func fakeRESP(t *testing.T, closeAfterFirst func(connIndex int) bool, stall func(connIndex, cmdIndex int) time.Duration) (addr string, accepted *int32) {
	return fakeRESPWith(t, closeAfterFirst, stall, nil)
}

// fakeRESPWith is fakeRESP with a responder: given the command's words it
// returns the raw reply to send (nil means +PONG).
func fakeRESPWith(t *testing.T, closeAfterFirst func(connIndex int) bool, stall func(connIndex, cmdIndex int) time.Duration, respond func(cmd []string) []byte) (addr string, accepted *int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var n int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			idx := int(atomic.AddInt32(&n, 1))
			go func(conn net.Conn, idx int) {
				defer conn.Close()
				r := bufio.NewReader(conn)
				for cmd := 0; ; cmd++ {
					line, err := readLine(r)
					if err != nil {
						return
					}
					k := 0
					for _, ch := range line[1:] {
						k = k*10 + int(ch-'0')
					}
					var words []string
					for j := 0; j < k; j++ {
						if _, err := readLine(r); err != nil {
							return
						}
						w, err := readLine(r)
						if err != nil {
							return
						}
						words = append(words, w)
					}
					if stall != nil {
						time.Sleep(stall(idx, cmd))
					}
					reply := []byte("+PONG\r\n")
					if respond != nil {
						if rp := respond(words); rp != nil {
							reply = rp
						}
					}
					_, _ = conn.Write(reply)
					if closeAfterFirst != nil && closeAfterFirst(idx) {
						return
					}
				}
			}(conn, idx)
		}
	}()
	return ln.Addr().String(), &n
}

// A slow reply must never be retried: the store may have run the command.
func TestRESPClientDoesNotRetryAfterATimeout(t *testing.T) {
	var executions int32
	addr, _ := fakeRESP(t, nil, func(_, cmd int) time.Duration {
		if atomic.AddInt32(&executions, 1) > 1 {
			return 700 * time.Millisecond // longer than the deadline, after the warm-up
		}
		return 0
	})
	c, _ := newRESPClient("redis://" + addr)
	if _, err := c.do("PING"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.do("PING"); err == nil {
		t.Fatal("a reply after the deadline is an error, not a success")
	}
	time.Sleep(800 * time.Millisecond)
	if n := atomic.LoadInt32(&executions); n != 2 {
		t.Fatalf("the slow command must run once, not be retried: executions=%d (1 warm-up + 1)", n)
	}
}

// After a store restart every idle connection is stale; the client must skip
// them all and succeed on a fresh one, without counting failures.
func TestRESPClientSkipsEveryStalePooledConnection(t *testing.T) {
	addr, accepted := fakeRESP(t, func(idx int) bool { return idx <= 8 }, nil)
	c, _ := newRESPClient("redis://" + addr)
	// Warm eight connections at once, all of which the server then closes.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = c.do("PING") }()
	}
	wg.Wait()
	time.Sleep(50 * time.Millisecond)
	for i := 0; i < 4; i++ {
		if v, err := c.do("PING"); err != nil || v != "PONG" {
			t.Fatalf("call %d after the restart: %v %v", i+1, v, err)
		}
	}
	if got := atomic.LoadInt32(accepted); got < 9 {
		t.Fatalf("expected a fresh connection after the stale ones, accepted=%d", got)
	}
}

// A dial that never completes is bounded like everything else.
func TestRESPClientBoundsTheWaitForADial(t *testing.T) {
	c, _ := newRESPClient("redis://127.0.0.1:6379")
	c.dial = func() (net.Conn, error) { time.Sleep(2 * time.Second); return nil, errors.New("slow") }
	c.dials = make(chan struct{}, 1)
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = c.do("PING") }()
	}
	wg.Wait()
	if d := time.Since(start); d > 2*c.timeout+2*time.Second {
		t.Fatalf("waiting callers must give up within a deadline of the first dial finishing, took %s", d)
	}
}

// A store that answers writes with "cannot serve" (a failover made it
// read-only) is an outage: the write-back keeps everything and the gap stays.
func TestSharedCounterAStoreThatCannotServeDuringWriteBackKeepsTheGap(t *testing.T) {
	addr, _ := fakeRESPWith(t, nil, nil, func(cmd []string) []byte {
		switch cmd[0] {
		case "PING":
			return nil
		case "EXISTS":
			return []byte(":0\r\n")
		}
		return []byte("-READONLY You can't write against a read only replica.\r\n")
	})
	s, err := newSharedCounter(ControlPolicy{MaxSpendUSDPerRun: 1, Now: fixed()}, "redis://"+addr)
	if err != nil {
		t.Fatal(err)
	}
	s.close()
	simulateGap(s)
	s.setKilled(true)
	s.owe(owed{kind: 'r', run: "x", token: "t1", costUSD: 0.10})
	s.owe(owed{kind: 'u', run: "y"})
	s.recovered()
	if s.mode() != "local" {
		t.Fatal("a store that cannot serve must not end the gap")
	}
	st := s.status()
	if st.Counter.Pending != 3 || st.Counter.Refused != 0 || st.Killed != 1 {
		t.Fatalf("nothing may be dropped: %+v killed=%d", st.Counter, st.Killed)
	}
}

// A pause the fallback made, then a resume the operator asked for, must land
// in that order: the resume is the last word.
func TestSharedCounterResumeAfterAPauseInAGapLandsInOrder(t *testing.T) {
	prefix := testPrefix(t)
	s := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 0.01}, prefix)
	other := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 0.01}, prefix)
	simulateGap(s)
	_, _, w := s.admitWhere("x", "", false)
	s.record("x", "", 0.05, false, w) // the fallback pauses x
	waitOwed(t, s, 1)
	if !s.resumeRun("x") {
		t.Fatal("the resume is accepted during the gap")
	}
	s.recovered()
	if ok, reason, _ := other.admitWhere("x", "", false); !ok {
		t.Fatalf("the resume must be the last word in the store: refused with %q", reason)
	}
}

// A kill the operator engaged here that the store has not taken yet holds on
// this gateway meanwhile, and lands on the next delivery.
func TestSharedCounterAKillTheStoreHasNotTakenIsEnforcedHere(t *testing.T) {
	prefix := testPrefix(t)
	s := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 1}, prefix)
	s.mu.Lock()
	s.killed, s.killDirty = true, true
	s.local.setKilled(true)
	s.mu.Unlock()
	if ok, reason, where := s.admitWhere("r", "", false); ok || reason != "kill switch engaged" || where != "local" {
		t.Fatalf("a pending kill must hold here: ok=%v reason=%q where=%s", ok, reason, where)
	}
	if !s.pending() || s.status().Killed != 1 {
		t.Fatal("status and pending must show the kill")
	}
	s.flush()
	other := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 1}, prefix)
	if ok, _, _ := other.admitWhere("r", "", false); ok {
		t.Fatal("the flush must land the kill in the store")
	}
	s.setKilled(false)
}

func TestSharedCounterErrorsOnOneKeyNeverSwitchTheCounter(t *testing.T) {
	s := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 1}, testPrefix(t))
	if _, err := s.client.do("SET", s.runKey("clash"), "not-a-hash"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, _, where := s.admitWhere("clash", "", false); where != "local" {
			t.Fatalf("call %d on a bad key is decided locally", i+1)
		}
	}
	if s.mode() != "shared" || s.status().Counter.OneOffDecided != 5 {
		t.Fatalf("five errors on one key must not switch the counter: mode=%s report=%+v", s.mode(), s.status().Counter)
	}
}

// A run the store had paused stays paused for this gateway during a gap.
func TestSharedCounterARunTheStorePausedStaysPausedDuringAGap(t *testing.T) {
	prefix := testPrefix(t)
	a := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 0.01}, prefix)
	b := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 0.01}, prefix)
	_, _, w := a.admitWhere("x", "", false)
	a.record("x", "", 0.05, false, w) // the store pauses x
	if ok, _, _ := b.admitWhere("x", "", false); ok {
		t.Fatal("b should see the pause")
	}
	simulateGap(b)
	if ok, reason, where := b.admitWhere("x", "", false); ok || where != "local" || reason == "" {
		t.Fatalf("the fallback must honour a pause it had seen: ok=%v reason=%q where=%s", ok, reason, where)
	}
}

// Both the admit and the settlement run once per token even when retried.
func TestSharedCounterAdmitAndSettlementTokensReplayOnce(t *testing.T) {
	s := newTestShared(t, ControlPolicy{MaxCallsPerRun: 10}, testPrefix(t))
	args := append([]string{"r", "0", "10", "0", "0", s.ttlArg(), "", "", "", "tok-1", ""}, s.keyArgs()...)
	keys := []string{s.runKey("r"), s.killKey(), s.pausedKey(), s.noneKey(), s.pausedKeysKey()}
	if _, err := s.client.eval(luaAdmit, shaAdmit, keys, args...); err != nil {
		t.Fatal(err)
	}
	v, err := s.client.eval(luaAdmit, shaAdmit, keys, args...) // the same admit again, as a stale-connection retry would
	if err != nil {
		t.Fatal(err)
	}
	if r, _ := v.([]any); asString(r[1]) != "dup" || func() bool { n, _ := asInt(r[0]); return n != 1 }() {
		t.Fatalf("a replayed admit must report the original answer: %v", v)
	}
	if h, _ := s.client.do("HGET", s.runKey("r"), "inflight"); asString(h) != "1" {
		t.Fatalf("a replayed admit must not reserve twice: inflight=%v", h)
	}
	s.record("r", "", 0.10, false, "shared:tok-1")
	s.record("r", "", 0.10, false, "shared:tok-1")
	h, _ := s.client.do("HMGET", s.runKey("r"), "inflight", "calls", "spend")
	f := h.([]any)
	if asString(f[0]) != "0" || asString(f[1]) != "1" || asString(f[2]) != "100000" {
		t.Fatalf("a replayed settlement must count once: %v", f)
	}
}

// A store that accepts but never answers at boot starts the counter in the
// fallback, as the startup line says.
func TestSharedCounterAHungStoreAtBootStartsInTheFallback(t *testing.T) {
	addr, _ := fakeRESP(t, nil, func(_, _ int) time.Duration { return 900 * time.Millisecond })
	s, err := newSharedCounter(ControlPolicy{MaxSpendUSDPerRun: 1, Now: fixed()}, "redis://"+addr)
	if err != nil {
		t.Fatal(err)
	}
	s.close()
	if s.mode() != "local" {
		t.Fatalf("a store that does not answer at boot means the fallback decides, got %q", s.mode())
	}
}

// A pause is owed before the call that caused it returns, so a resume issued
// right after it can never be delivered ahead of it.
func TestSharedCounterAPauseIsOwedBeforeItsCallReturns(t *testing.T) {
	prefix := testPrefix(t)
	s := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 0.01}, prefix)
	other := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 0.01}, prefix)
	simulateGap(s)
	for i := 0; i < 20; i++ {
		run := "x" + strconv.Itoa(i)
		_, _, w := s.admitWhere(run, "", false)
		s.record(run, "", 0.05, false, w) // pauses, and owes the pause before returning
		s.resumeRun(run)                  // immediately: must land after the pause
	}
	s.mu.Lock()
	firstPause, firstResume := map[string]int{}, map[string]int{}
	for i, o := range s.owed {
		switch o.kind {
		case 'p':
			if _, seen := firstPause[o.run]; !seen {
				firstPause[o.run] = i
			}
		case 'u':
			if _, seen := firstResume[o.run]; !seen {
				firstResume[o.run] = i
			}
		}
	}
	s.mu.Unlock()
	for run, pi := range firstPause {
		if ui, ok := firstResume[run]; !ok || ui < pi {
			t.Fatalf("run %s: resume queued at %d, pause at %d — the pause must come first", run, ui, pi)
		}
	}
	s.recovered()
	for i := 0; i < 20; i++ {
		if ok, reason, _ := other.admitWhere("x"+strconv.Itoa(i), "", false); !ok {
			t.Fatalf("run x%d should be resumed in the store, got %q", i, reason)
		}
	}
}

// A bypassed admit says nothing about the pause; this gateway's own
// settlement that pauses a run is remembered like a refusal is.
func TestSharedCounterRemembersStorePausesAcrossBypassAndOwnSettlement(t *testing.T) {
	prefix := testPrefix(t)
	a := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 0.01, AdminToken: "tok"}, prefix)
	b := newTestShared(t, ControlPolicy{MaxSpendUSDPerRun: 0.01, AdminToken: "tok"}, prefix)
	_, _, w := a.admitWhere("x", "", false)
	a.record("x", "", 0.05, false, w) // a's own settlement pauses x in the store
	simulateGap(a)
	if ok, _, _ := a.admitWhere("x", "", false); ok {
		t.Fatal("the gateway that stopped the run must remember it during a gap")
	}
	a.recovered()
	b.admitWhere("x", "", false) // b learns the pause
	b.admitWhere("x", "", true)  // a bypass passes, and must not forget it
	simulateGap(b)
	if ok, _, _ := b.admitWhere("x", "", false); ok {
		t.Fatal("a bypass must not erase the memory of a store pause")
	}
}

// A leaked key spending across replicas hits its ceiling once, everywhere.
func TestSharedCounterKeyCeilingHoldsAcrossReplicas(t *testing.T) {
	prefix := testPrefix(t)
	pol := ControlPolicy{MaxSpendUSDPerKeyDay: 0.02}
	a := newTestShared(t, pol, prefix)
	b := newTestShared(t, pol, prefix)
	al := newAlertCounter()
	a.setOnPause(al.hook)
	b.setOnPause(al.hook)
	a.noteKey("k-0123456789ab-9999", "thief", "", "gpt-4o")
	_, _, w := a.admitWhere("r1", "k-0123456789ab-9999", false)
	a.record("r1", "k-0123456789ab-9999", 0.0075, false, w)
	_, _, w = b.admitWhere("r2", "k-0123456789ab-9999", false)
	b.record("r2", "k-0123456789ab-9999", 0.0075, false, w)
	_, _, w = a.admitWhere("r3", "k-0123456789ab-9999", false)
	a.record("r3", "k-0123456789ab-9999", 0.0075, false, w) // $0.0225 crosses $0.02
	if ok, reason, _ := b.admitWhere("r4", "k-0123456789ab-9999", false); ok || reason != "key …9999 reached its daily spend cap of $0.02 (resets at midnight UTC)" {
		t.Fatalf("the other replica refuses the key: ok=%v reason=%q", ok, reason)
	}
	if n := al.wait(1); n != 1 || al.last().Key != "k-0123456789ab-9999" || al.last().Agent != "thief" {
		t.Fatalf("one key alert with attribution, got %d: %+v", n, al.last())
	}
	st := a.status()
	if len(st.PausedKeys) != 1 || st.PausedKeys[0].Key != "k-0123456789ab-9999" || st.PausedKeys[0].CallsToday != 3 {
		t.Fatalf("status lists the paused key: %+v", st.PausedKeys)
	}
	if !a.resumeKey("k-0123456789ab-9999") {
		t.Fatal("resume finds the key")
	}
	if ok, _, _ := b.admitWhere("r5", "k-0123456789ab-9999", false); !ok {
		t.Fatal("a resumed key is admitted everywhere")
	}
	// A run cap and a key cap coexist: a run refused by its own cap does not touch the key.
	if _, _, w := a.admitWhere("r6", "k-other-1111", false); counterLabel(w) != "shared" {
		t.Fatal("another key admits")
	}
}

// A key paused by the fallback during a gap is written back, and a key the
// store had paused stays paused for the fallback.
func TestSharedCounterKeyPausesSurviveAGap(t *testing.T) {
	prefix := testPrefix(t)
	a := newTestShared(t, ControlPolicy{MaxSpendUSDPerKey: 0.01}, prefix)
	b := newTestShared(t, ControlPolicy{MaxSpendUSDPerKey: 0.01}, prefix)
	simulateGap(a)
	_, _, w := a.admitWhere("r1", "k-0123456789ab-2222", false)
	a.record("r1", "k-0123456789ab-2222", 0.05, false, w) // the fallback pauses the key
	waitOwed(t, a, 1)
	a.recovered()
	if ok, reason, _ := b.admitWhere("r2", "k-0123456789ab-2222", false); ok || !strings.Contains(reason, "key …2222") {
		t.Fatalf("the gap's key pause must be in the store: ok=%v reason=%q", ok, reason)
	}
	// b saw the pause; during b's own gap it still refuses the key.
	simulateGap(b)
	if ok, _, where := b.admitWhere("r3", "k-0123456789ab-2222", false); ok || where != "local" {
		t.Fatalf("the fallback honours a key pause it had seen: ok=%v where=%s", ok, where)
	}
}

func redisInt(t *testing.T, c *respClient, cmd, key string) int64 {
	t.Helper()
	v, err := c.do(cmd, key)
	if err != nil {
		t.Fatal(err)
	}
	n, _ := asInt(v)
	return n
}

// Untagged keyed calls keep their per-call ids out of the key's 400-day
// hash: the tallies stay a handful of fields, the ids live as long as a run.
func TestSharedCounterUntaggedKeyedCallsDoNotGrowTheKeyHash(t *testing.T) {
	prefix := testPrefix(t)
	a := newTestShared(t, ControlPolicy{MaxSpendUSDPerKeyDay: 100}, prefix)
	c := redisUp(t)
	for i := 0; i < 20; i++ {
		_, _, w := a.admitWhere("", "k-plain-1111", false)
		if i%2 == 0 {
			a.record("", "k-plain-1111", 0.001, false, w)
		} else {
			a.release("", "k-plain-1111", w)
		}
	}
	if n := redisInt(t, c, "HLEN", prefix+"key:k-plain-1111"); n > 12 {
		t.Fatalf("the key hash holds tallies only, got %d fields", n)
	}
	bucket := prefix + "keytok:k-plain-1111:" + a.policy.Now().UTC().Format("20060102")
	if n := redisInt(t, c, "HLEN", bucket); n != 40 {
		t.Fatalf("every admit and settle left its id in today's bucket: %d", n)
	}
	if ttl := redisInt(t, c, "TTL", bucket); ttl <= 0 || ttl > 3*24*3600 {
		t.Fatalf("a bucket dies three days after its day starts, ttl=%d", ttl)
	}
	// A settle that lands the next day still finds its bucket, and the
	// bucket's clock keeps running down rather than being refreshed.
	later := newTestShared(t, ControlPolicy{MaxSpendUSDPerKeyDay: 100, Now: func() time.Time { return a.policy.Now().Add(24 * time.Hour) }}, prefix)
	_, _, w := a.admitWhere("", "k-plain-1111", false)
	later.record("", "k-plain-1111", 0.001, false, w)
	if n := redisInt(t, c, "HLEN", bucket); n != 42 {
		t.Fatalf("the next-day settle joined its admit's bucket: %d", n)
	}
	if ttl := redisInt(t, c, "TTL", bucket); ttl > 2*24*3600 {
		t.Fatalf("a touch only shortens a bucket's life, ttl=%d", ttl)
	}
	if ttl := redisInt(t, c, "TTL", prefix+"key:k-plain-1111"); ttl <= 48*3600 {
		t.Fatalf("the tally hash keeps its long life, ttl=%d", ttl)
	}
}

// A replica whose clock lags never resets the store's day: the day only
// moves forward, so a tally and a daily pause survive a lagging replica.
func TestSharedCounterLaggingClockNeverResetsTheDay(t *testing.T) {
	prefix := testPrefix(t)
	today := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	a := newTestShared(t, ControlPolicy{MaxSpendUSDPerKeyDay: 0.02, Now: func() time.Time { return today }}, prefix)
	b := newTestShared(t, ControlPolicy{MaxSpendUSDPerKeyDay: 0.02, Now: func() time.Time { return today.Add(-24 * time.Hour) }}, prefix)
	for i, s := range []*sharedCounter{a, b, a} {
		ok, _, w := s.admitWhere("r", "k-0123456789ab-3333", false)
		if !ok {
			t.Fatalf("call %d admits", i+1)
		}
		s.record("r", "k-0123456789ab-3333", 0.0075, false, w) // $0.0225 on one store day: paused on the third
	}
	if ok, _, _ := b.admitWhere("r", "k-0123456789ab-3333", false); ok {
		t.Fatal("the lagging replica must neither reset the tally nor clear the pause")
	}
	if ok, _, _ := a.admitWhere("r", "k-0123456789ab-3333", false); ok {
		t.Fatal("the pause holds for the replica on the right day")
	}
	if st := b.status(); len(st.PausedKeys) != 1 {
		t.Fatalf("the lagging replica still lists the pause: %+v", st.PausedKeys)
	}
}

// A daily pause the fallback made during a gap is written back as a daily
// pause, so the store still clears it at midnight.
func TestSharedCounterGapDayPauseClearsAtMidnight(t *testing.T) {
	prefix := testPrefix(t)
	now := time.Date(2026, 9, 11, 23, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	pol := ControlPolicy{MaxSpendUSDPerKeyDay: 0.01, MaxSpendUSDPerKey: 1, Now: clock}
	a := newTestShared(t, pol, prefix)
	b := newTestShared(t, pol, prefix)
	simulateGap(a)
	_, _, w := a.admitWhere("r1", "k-0123456789ab-4444", false)
	a.record("r1", "k-0123456789ab-4444", 0.05, false, w) // the fallback day-pauses the key
	waitOwed(t, a, 1)
	a.recovered()
	if ok, reason, _ := b.admitWhere("r2", "k-0123456789ab-4444", false); ok || !strings.Contains(reason, "resets at midnight") {
		t.Fatalf("the gap's daily pause is in the store as a daily pause: ok=%v reason=%q", ok, reason)
	}
	now = now.Add(2 * time.Hour)
	if ok, _, _ := b.admitWhere("r3", "k-0123456789ab-4444", false); !ok {
		t.Fatal("a daily pause written back from a gap still clears at midnight")
	}
}

// On a healthy store the total cap holds at admit and at record, survives
// midnight, and a resume after a daily pause keeps the total.
func TestSharedCounterKeyTotalCapOnAHealthyStore(t *testing.T) {
	prefix := testPrefix(t)
	now := time.Date(2026, 9, 11, 23, 0, 0, 0, time.UTC)
	a := newTestShared(t, ControlPolicy{MaxSpendUSDPerKeyDay: 0.05, MaxSpendUSDPerKey: 0.06, Now: func() time.Time { return now }}, prefix)
	al := newAlertCounter()
	a.setOnPause(al.hook)
	spend := func(run string, n int) {
		for i := 0; i < n; i++ {
			ok, _, w := a.admitWhere(run, "k-0123456789ab-5555", false)
			if !ok {
				return
			}
			a.record(run, "k-0123456789ab-5555", 0.0075, false, w)
		}
	}
	spend("r1", 7) // $0.0525 today: the daily cap pauses at record
	if ok, reason, _ := a.admitWhere("r2", "k-0123456789ab-5555", false); ok || !strings.Contains(reason, "daily") {
		t.Fatalf("daily pause: ok=%v reason=%q", ok, reason)
	}
	if n := al.wait(1); n != 1 || !al.last().dayPause || al.last().Calls != 7 {
		t.Fatalf("one daily alert with today's figures, got %d: %+v", n, al.last())
	}
	if !a.resumeKey("k-0123456789ab-5555") {
		t.Fatal("resume finds the key")
	}
	spend("r3", 1) // $0.06 total: the total cap fires, the daily resume did not wipe it
	if ok, reason, _ := a.admitWhere("r4", "k-0123456789ab-5555", false); ok || reason != "key …5555 reached its spend cap of $0.06" {
		t.Fatalf("total pause: ok=%v reason=%q", ok, reason)
	}
	now = now.Add(2 * time.Hour) // midnight: a total pause stays
	if ok, _, _ := a.admitWhere("r5", "k-0123456789ab-5555", false); ok {
		t.Fatal("a total pause does not clear with the day")
	}
	if st := a.status(); len(st.PausedKeys) != 1 || st.PausedKeys[0].TotalUSD < 0.06 || st.PausedKeys[0].CallsToday != 0 {
		t.Fatalf("status shows the total and today's fresh figures: %+v", st.PausedKeys)
	}
	a.resumeKey("k-0123456789ab-5555")
	if ok, _, _ := a.admitWhere("r6", "k-0123456789ab-5555", false); !ok {
		t.Fatal("a resume after a total pause grants a fresh total")
	}
	if n := al.wait(2); n != 2 {
		t.Fatalf("one alert per key stop, got %d", n)
	}
}

// A refusal because the run is paused still gives a new key's hash its expiry.
func TestSharedCounterRunPausedRefusalExpiresTheKeyHash(t *testing.T) {
	prefix := testPrefix(t)
	a := newTestShared(t, ControlPolicy{MaxCallsPerRun: 1, MaxSpendUSDPerKeyDay: 1}, prefix)
	_, _, w := a.admitWhere("r1", "k-one-6666", false)
	a.record("r1", "k-one-6666", 0.001, false, w) // the run is paused at one call
	if ok, _, _ := a.admitWhere("r1", "k-new-7777", false); ok {
		t.Fatal("the run is paused")
	}
	if ttl := redisInt(t, redisUp(t), "TTL", prefix+"key:k-new-7777"); ttl <= 0 {
		t.Fatalf("a key hash created on a refused call must expire, ttl=%d", ttl)
	}
}

// A run refused because its key is paused still gets its hash an expiry.
func TestSharedCounterKeyPausedRefusalExpiresTheRunHash(t *testing.T) {
	prefix := testPrefix(t)
	a := newTestShared(t, ControlPolicy{MaxSpendUSDPerKeyDay: 0.01}, prefix)
	_, _, w := a.admitWhere("r1", "k-0123456789ab-8888", false)
	a.record("r1", "k-0123456789ab-8888", 0.05, false, w) // the key is paused
	a.noteRun("r-fresh", "agent", "", "gpt-4o")
	if ok, _, _ := a.admitWhere("r-fresh", "k-0123456789ab-8888", false); ok {
		t.Fatal("the key is paused")
	}
	if ttl := redisInt(t, redisUp(t), "TTL", prefix+"run:r-fresh"); ttl <= 0 {
		t.Fatalf("a run hash created on a key refusal must expire, ttl=%d", ttl)
	}
}

// A daily pause the fallback made yesterday is over by the time it is
// written back today: it is not resurrected in the store, and a note of it
// does not seed a fallback built today.
func TestSharedCounterStaleDayPauseIsNotResurrected(t *testing.T) {
	prefix := testPrefix(t)
	now := time.Date(2026, 9, 11, 23, 30, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	pol := ControlPolicy{MaxSpendUSDPerKeyDay: 0.01, Now: clock}
	a := newTestShared(t, pol, prefix)
	b := newTestShared(t, pol, prefix)
	// b sees the store pause the key tonight, then a gap spans midnight.
	_, _, w := b.admitWhere("r0", "k-0123456789ab-6666", false)
	b.record("r0", "k-0123456789ab-6666", 0.05, false, w)
	if ok, _, _ := b.admitWhere("r1", "k-0123456789ab-6666", false); ok {
		t.Fatal("paused tonight")
	}
	simulateGap(a)
	_, _, w = a.admitWhere("r2", "k-0123456789ab-6666", false)
	a.record("r2", "k-0123456789ab-6666", 0.05, false, w) // the fallback day-pauses it too
	waitOwed(t, a, 1)
	now = now.Add(time.Hour) // midnight passed before the store came back
	a.recovered()
	if ok, _, where := b.admitWhere("r3", "k-0123456789ab-6666", false); !ok || where == "local" {
		t.Fatalf("yesterday's daily pause, written back today, must not refuse today: ok=%v where=%s", ok, where)
	}
	simulateGap(b) // b remembered last night's pause; a fallback built today must not honour it
	if ok, _, where := b.admitWhere("r4", "k-0123456789ab-6666", false); !ok || where != "local" {
		t.Fatalf("a stale note does not seed the fallback: ok=%v where=%s", ok, where)
	}
}

// Shadow mode on the store: the replica in shadow serves a capped run with
// the reason and alerts once; the state is shared, so a replica that
// enforces refuses that run; a gap keeps the same manners.
func TestSharedCounterShadowModeServesWithReasonAndSharesTheState(t *testing.T) {
	prefix := testPrefix(t)
	watch := newTestShared(t, ControlPolicy{MaxCallsPerRun: 2, MaxSpendUSDPerKeyDay: 0.01, Shadow: true}, prefix)
	enforce := newTestShared(t, ControlPolicy{MaxCallsPerRun: 2, MaxSpendUSDPerKeyDay: 0.01}, prefix)
	al := newAlertCounter()
	watch.setOnPause(al.hook)
	enforce.setOnPause(al.hook)
	for i := 0; i < 2; i++ {
		ok, reason, w := watch.admitWhere("r1", "", false)
		if !ok || reason != "" {
			t.Fatalf("call %d under the cap: ok=%v reason=%q", i+1, ok, reason)
		}
		watch.record("r1", "", 0.001, false, w)
	}
	ok, reason, w := watch.admitWhere("r1", "", false)
	if !ok || reason != "run reached the call cap of 2" || counterLabel(w) != "shared" {
		t.Fatalf("shadow serves with the reason: ok=%v reason=%q where=%s", ok, reason, w)
	}
	watch.record("r1", "", 0.001, false, w)
	if n := al.wait(1); n != 1 || !al.last().Shadow {
		t.Fatalf("one shadow alert, got %d: %+v", n, al.last())
	}
	if ok, _, _ := enforce.admitWhere("r1", "", false); ok {
		t.Fatal("the pause is shared: a replica that enforces refuses the run")
	}
	if st := watch.status(); len(st.PausedRuns) != 1 || st.PausedRuns[0].Run != "r1" || !st.Policy.Shadow || !strings.Contains(st.Note, "marked, not refused") {
		t.Fatalf("status lists the marked run, the mode and the note: %+v", st)
	}
	if st := enforce.status(); st.Policy.Shadow || st.Note != "" {
		t.Fatalf("the enforcing replica reports its own mode: %+v", st)
	}
	// A key past its cap: served with the key's reason, once alerted.
	_, _, w = watch.admitWhere("", "k-0123456789ab-2468", false)
	watch.record("", "k-0123456789ab-2468", 0.05, false, w)
	if ok, reason, _ := watch.admitWhere("", "k-0123456789ab-2468", false); !ok || !strings.HasPrefix(reason, "key …2468 reached its daily spend cap") {
		t.Fatalf("shadow serves a capped key with its reason: ok=%v reason=%q", ok, reason)
	}
	if n := al.wait(2); n != 2 {
		t.Fatalf("one alert per stop, got %d", n)
	}
	// In a gap the fallback keeps the same manners.
	simulateGap(watch)
	if ok, reason, where := watch.admitWhere("r1", "", false); !ok || reason == "" || where != "local" {
		t.Fatalf("the fallback serves and marks too: ok=%v reason=%q where=%s", ok, reason, where)
	}
}
