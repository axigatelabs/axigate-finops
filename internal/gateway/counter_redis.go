package gateway

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// sharedCounter keeps run tallies in Redis so a cap holds across gateway
// replicas. Every replica reserves against the same counter inside one script
// the store runs as a single step, so a burst spread over replicas trips the
// cap at the boundary the way one process does; a pause, a resume and the
// kill switch are cluster-wide too. Dollars are kept as whole micro-dollars,
// so every replica and every machine agree on the boundary (the in-memory
// counter rounds the same way; the two can differ only below a micro-dollar).
//
// Each call is settled by the counter that admitted it: admitWhere returns a
// word the gateway hands back to record or release. For the store that word
// carries a per-call token, kept in the run's own record, and both the admit
// and the settlement run once per token no matter how often they are sent, so
// either can be retried safely.
//
// When the store is unreachable — it does not answer, its dial is refused, or
// it answers that it cannot serve (wrong password, loading, read-only, moved)
// — the counter falls back to a fresh per-process tally that starts from the
// kill switch and the paused runs it last knew: a backstop that bounds
// overspend during the gap, not exact accounting. It says so: rows decided
// locally carry counter=local, the status endpoint reports since when and how
// many decisions were made that way, and what the store owes is written back,
// in order, when it returns: settlements of calls the store had admitted, runs
// the fallback paused, and what an operator asked for meanwhile (the kill
// switch, resumes, inline caps). Spend tallied for calls the fallback itself
// admitted is not merged; that would be a guess.
//
// A busy pool, a single dropped answer, or an error on one key decides that
// one call locally without switching the counter over, and such a decision
// never pauses a run for anyone else; only three dropped answers in a row
// switch it. A background probe is the only way back, and it also delivers
// whatever the store still owes while things are healthy. Requests never wait
// for either.
//
// A key ceiling works the same way: each API key has its own record beside
// the run records, with today's and total spend, reserved at admit, rolled
// over at midnight UTC inside the script, paused and resumed cluster-wide.
//
// Single Redis, not Redis Cluster: the status script reads run keys it does
// not declare.
type sharedCounter struct {
	policy     ControlPolicy
	client     *respClient
	onPause    func(StopEvent)
	prefix     string
	ttl        time.Duration
	probeEvery time.Duration
	storeURL   string
	tokenBase  string
	tokenSeq   atomic.Uint64

	deliverMu sync.Mutex // one deliverer at a time, so what is owed lands in order

	mu            sync.Mutex
	local         *controller // the per-process fallback; swapped only under mu
	degraded      bool
	degradedSince time.Time
	lastErr       string
	outages       int
	lastSince     time.Time
	lastUntil     time.Time
	localDecided  int                     // cap decisions the fallback made in the current or last gap
	busyDecided   int                     // cap decisions made locally because every connection was busy, since start
	oneOffDecided int                     // cap decisions made locally on one dropped answer or one bad key, since start
	failStreak    int                     // dropped answers in a row
	killed        bool                    // the kill switch as last mirrored from the store, or as the operator set it
	killDirty     bool                    // the operator set it and the store has not taken it yet
	killGen       uint64                  // bumped by every setKilled, so a delivery clears only what it wrote
	pausedSeen    map[string]string       // runs the store reported paused, and why; seeds the fallback
	keysSeen      map[string]keyPauseNote // keys the store reported paused, and why; seeds the fallback (a daily pause is remembered with its day)
	notes         map[string]noteTags
	keyNotes      map[string]noteTags
	owed          []owed // what the store still has to take, in order
	delivered     int    // owed items the store took, since start
	dropped       int    // owed items dropped because the queue was full, since start
	refused       int    // owed items the store answered with an error, dropped, since start
	flushing      bool
	recovering    bool

	quit     chan struct{}
	stopOnce sync.Once
}

type noteTags struct{ agent, team, model string }

// owed is one thing the store still has to take, delivered in arrival order:
// a settlement (record or release, once per token), a pause the fallback made
// during a gap, a resume or an inline cap an operator or caller asked for.
type owed struct {
	kind    byte // 'r' record, 'l' release, 'p' run pause, 'P' key pause, 'u' run resume, 'U' key resume, 'c' cap
	run     string
	key     string
	token   string
	costUSD float64
	loop    bool
	reason  string
	capUSD  float64
	day     bool   // a key pause made by the daily cap, which clears at midnight
	onDay   string // the UTC day that daily pause was made on
}

const (
	sharedRunTTL        = 48 * time.Hour
	sharedKeyTTLSeconds = "34560000" // 400 days: a key's total tally outlives any run
	maxOwed             = 10000
	maxStatusRuns       = 200
	maxNotes            = 10000
	writeBackPasses     = 3
	flushBatch          = 200
	sharedProbeEvery    = 2 * time.Second
	failStreakToDegrade = 3
)

// newSharedCounter connects to the store. A store that does not answer at
// boot is the same as one that goes away later: the counter starts in the
// per-process fallback and the probe keeps trying. A store that answers with
// an error (wrong password, wrong database) is a configuration mistake and is
// returned as one.
func newSharedCounter(p ControlPolicy, rawURL string) (*sharedCounter, error) {
	client, err := newRESPClient(rawURL)
	if err != nil {
		return nil, err
	}
	if p.Now == nil {
		p.Now = time.Now
	}
	var b [6]byte
	_, _ = rand.Read(b[:])
	s := &sharedCounter{
		policy: p, client: client, prefix: "axigate:", ttl: sharedRunTTL, probeEvery: sharedProbeEvery, storeURL: redact(rawURL),
		tokenBase: hex.EncodeToString(b[:]), pausedSeen: map[string]string{}, keysSeen: map[string]keyPauseNote{}, notes: map[string]noteTags{}, keyNotes: map[string]noteTags{}, quit: make(chan struct{}),
	}
	s.local = s.freshFallback(false, false)
	if _, err := client.do("PING"); err != nil {
		var re respError
		if errors.As(err, &re) {
			return nil, fmt.Errorf("shared counter at %s answered with an error: %v", s.storeURL, err)
		}
		s.degrade(err, false)
	}
	go s.probeLoop()
	return s, nil
}

// CheckSharedCounterURL says whether a --shared-counter value can be used,
// before anything is started; connectivity is not checked here.
func CheckSharedCounterURL(raw string) error {
	_, err := newRESPClient(raw)
	return err
}

// redact prints a store URL without its password or query, for logs and status.
func redact(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "redis://?"
	}
	u.User, u.RawQuery, u.Fragment = nil, "", ""
	return u.String()
}

// counterLabel is the word a row carries for where a decision was made: the
// store's token is not part of it.
func counterLabel(where string) string {
	if i := strings.IndexByte(where, ':'); i >= 0 {
		return where[:i]
	}
	return where
}

func (s *sharedCounter) close() { s.stopOnce.Do(func() { close(s.quit) }) }

// freshFallback builds a per-process controller that starts from the kill
// switch and the paused runs as last known. A fallback built for a gap owes
// every pause it makes to the store, in order with whatever the caller does
// next, and alerts; one built for a healthy period (whose decisions are
// one-offs) does neither — the store is deciding for everyone.
func (s *sharedCounter) freshFallback(killed, gap bool) *controller {
	c := newController(s.policy)
	c.setKilled(killed)
	for run, reason := range s.pausedSeen {
		c.markPaused(run, reason)
	}
	today := s.today()
	for key, n := range s.keysSeen {
		if n.day != "" && today > n.day {
			continue // a daily pause from an earlier day is over
		}
		c.markKeyPaused(key, n.reason, n.day != "")
	}
	if !gap {
		return c
	}
	c.onPauseSync = func(ev StopEvent) {
		if ev.Key != "" && ev.Run == "" {
			s.owe(owed{kind: 'P', key: ev.Key, reason: ev.Reason, day: ev.dayPause, onDay: ev.At.UTC().Format("2006-01-02")})
			return
		}
		s.owe(owed{kind: 'p', run: ev.Run, reason: ev.Reason})
	}
	c.onPause = func(ev StopEvent) {
		s.mu.Lock()
		f := s.onPause
		s.mu.Unlock()
		if f != nil {
			f(ev)
		}
	}
	return c
}

func (s *sharedCounter) setOnPause(f func(StopEvent)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onPause = f
}

func (s *sharedCounter) mode() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.degraded {
		return "local"
	}
	return "shared"
}

// pending says whether the store still owes something asked for through this
// gateway, so an admin reply can say so.
func (s *sharedCounter) pending() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.degraded || s.killDirty || len(s.owed) > 0
}

func (s *sharedCounter) runKey(run string) string { return s.prefix + "run:" + run }
func (s *sharedCounter) keyKey(key string) string { return s.prefix + "key:" + key }
func (s *sharedCounter) keyTokKey(key, tok string) string {
	return s.prefix + "keytok:" + key + ":" + tokenDay(tok) // per-call ids for keyed calls with no run, one bucket per admit day
}
func (s *sharedCounter) pausedKeysKey() string { return s.prefix + "pausedkeys" }
func (s *sharedCounter) noneKey() string       { return s.prefix + "none" } // a placeholder KEYS slot the scripts never touch
func (s *sharedCounter) today() string         { return s.policy.Now().UTC().Format("2006-01-02") }

// runOrNone / keyOrNone give the scripts a real key name in every KEYS slot.
func (s *sharedCounter) runOrNone(run string) string {
	if run == "" {
		return s.noneKey()
	}
	return s.runKey(run)
}

func (s *sharedCounter) keyOrNone(key string) string {
	if key == "" {
		return s.noneKey()
	}
	return s.keyKey(key)
}

func (s *sharedCounter) keyTokOrNone(key, tok string) string {
	if key == "" {
		return s.noneKey()
	}
	return s.keyTokKey(key, tok)
}

func (s *sharedCounter) keyArgs() []string {
	return []string{s.today(), strconv.FormatInt(micro(s.policy.MaxSpendUSDPerKeyDay), 10), strconv.FormatInt(micro(s.policy.MaxSpendUSDPerKey), 10), sharedKeyTTLSeconds}
}
func (s *sharedCounter) killKey() string   { return s.prefix + "killed" }
func (s *sharedCounter) pausedKey() string { return s.prefix + "paused" }
func (s *sharedCounter) ttlArg() string    { return strconv.Itoa(int(s.ttl.Seconds())) }

// token is the per-call id: the admit day, then a process-unique counter. The
// day names the bucket a run-less keyed call's id lives in (see keyTokKey).
func (s *sharedCounter) token() string {
	return s.policy.Now().UTC().Format("20060102") + "." + s.tokenBase + strconv.FormatUint(s.tokenSeq.Add(1), 36)
}

// keyTokTTLSeconds is how long a token bucket has left: every bucket dies at
// a fixed time, three days after the start of the day it is named for, so a
// key that never rests still sheds its old ids. A touch can only shorten it.
func (s *sharedCounter) keyTokTTLSeconds(tok string) string {
	day, err := time.Parse("20060102", tokenDay(tok))
	if err != nil {
		return s.ttlArg()
	}
	left := day.Add(3 * 24 * time.Hour).Sub(s.policy.Now().UTC())
	if left < time.Second {
		left = time.Second
	}
	return strconv.FormatInt(int64(left/time.Second), 10)
}

func tokenDay(tok string) string {
	if i := strings.IndexByte(tok, '.'); i == 8 {
		return tok[:8]
	}
	return ""
}

// fallback returns the per-process controller and whether it is deciding.
func (s *sharedCounter) fallback() (*controller, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.local, s.degraded
}

// cannotServe says whether an error reply means the store as a whole cannot
// serve right now (as opposed to one key or one script being wrong).
func cannotServe(re respError) bool {
	for _, p := range []string{"NOAUTH", "WRONGPASS", "ERR AUTH", "LOADING", "READONLY", "MOVED", "ASK", "MASTERDOWN", "NOPERM", "MISCONF", "OOM", "NOREPLICAS", "BUSY"} {
		if strings.HasPrefix(string(re), p) {
			return true
		}
	}
	return false
}

// refusal says whether an error is the store answering that it will not take
// this one thing (a bad key, a script error): retrying cannot help, and the
// store itself is fine.
func refusal(err error) bool {
	var re respError
	return errors.As(err, &re) && !cannotServe(re)
}

// dialFailure says whether an error came from trying to reach the store at
// all, as opposed to a connection that was open and then went wrong.
func dialFailure(err error) bool {
	var oe *net.OpError
	return errors.As(err, &oe) && oe.Op == "dial"
}

// fail records a store failure and decides whether the whole counter falls
// back. A busy pool never does; nor does the store refusing one thing. A
// refused dial, or a reply that the store cannot serve, does at once. A
// dropped answer is one bad connection, and the counter falls back only
// after failStreakToDegrade of those in a row.
func (s *sharedCounter) fail(err error) {
	s.mu.Lock()
	s.lastErr = err.Error()
	if errors.Is(err, errPoolBusy) || s.degraded {
		s.mu.Unlock()
		return
	}
	var re respError
	if errors.As(err, &re) {
		s.mu.Unlock()
		if cannotServe(re) {
			s.degrade(err, false)
		}
		return
	}
	if dialFailure(err) {
		s.mu.Unlock()
		s.degrade(err, false)
		return
	}
	s.failStreak++
	if s.failStreak < failStreakToDegrade {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.degrade(err, false)
}

// degrade switches the counter to a fresh per-process fallback. keep says the
// gap never really ended (a recovery failed at the last step): the clock and
// the count stay as they were.
func (s *sharedCounter) degrade(err error, keep bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.degraded {
		return
	}
	s.degraded = true
	s.failStreak = 0
	s.lastErr = err.Error()
	if !keep {
		s.degradedSince = s.policy.Now().UTC()
		s.outages++
		s.lastSince, s.lastUntil, s.localDecided = s.degradedSince, time.Time{}, 0
	}
	s.local = s.freshFallback(s.killed, true)
	var re respError
	if errors.As(err, &re) {
		fmt.Fprintf(os.Stderr, "gateway: shared counter answered with an error (%v); deciding caps per process until it answers normally — approximate, not exact\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "gateway: shared counter unreachable (%v); deciding caps per process until it returns — approximate, not exact\n", err)
	}
}

// answered resets the failure streak: the store is talking.
func (s *sharedCounter) answered() {
	s.mu.Lock()
	s.failStreak = 0
	s.mu.Unlock()
}

// probeLoop pings while the counter is in the fallback and brings it back
// when the store answers; while healthy it delivers anything still owed.
// Requests never pay for either.
func (s *sharedCounter) probeLoop() {
	t := time.NewTicker(s.probeEvery)
	defer t.Stop()
	for {
		select {
		case <-s.quit:
			return
		case <-t.C:
			s.mu.Lock()
			degraded := s.degraded
			s.mu.Unlock()
			if degraded {
				if _, err := s.client.do("PING"); err == nil {
					s.answered()
					s.recovered()
				}
				continue
			}
			s.flush()
		}
	}
}

// owe queues something the store still has to take.
func (s *sharedCounter) owe(o owed) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.owed) >= maxOwed {
		s.owed = s.owed[1:]
		s.dropped++
	}
	s.owed = append(s.owed, o)
}

// putBack restores what a delivery could not complete, ahead of anything newer.
func (s *sharedCounter) putBack(items []owed) {
	s.mu.Lock()
	s.owed = append(items, s.owed...)
	s.mu.Unlock()
}

// deliver sends everything owed, in order: the kill switch first if the
// operator set it, then the queue. A connection failure, or a store that
// cannot serve, stops it and the remainder is put back; the store refusing
// one item drops that item and counts it. Returns what was delivered.
func (s *sharedCounter) deliver() (int, error) {
	s.deliverMu.Lock()
	defer s.deliverMu.Unlock()
	n := 0
	s.mu.Lock()
	dirty, killed, gen := s.killDirty, s.killed, s.killGen
	s.mu.Unlock()
	if dirty {
		var err error
		if killed {
			_, err = s.client.do("SET", s.killKey(), "1")
		} else {
			_, err = s.client.do("DEL", s.killKey())
		}
		switch {
		case err == nil:
			n++
			s.answered()
			s.mu.Lock()
			s.delivered++
			s.mu.Unlock()
		case refusal(err):
			s.mu.Lock()
			s.refused++
			s.mu.Unlock()
		default:
			return n, err
		}
		s.mu.Lock()
		if s.killGen == gen {
			s.killDirty = false
		}
		s.mu.Unlock()
	}
	for {
		s.mu.Lock()
		if len(s.owed) == 0 {
			s.mu.Unlock()
			return n, nil
		}
		k := len(s.owed)
		if k > flushBatch {
			k = flushBatch
		}
		batch := s.owed[:k:k]
		s.owed = s.owed[k:]
		s.mu.Unlock()
		for i, o := range batch {
			if err := s.take(o); err != nil {
				if refusal(err) {
					s.mu.Lock()
					s.refused++
					s.mu.Unlock()
					continue
				}
				s.putBack(batch[i:])
				return n, err
			}
			n++
			s.mu.Lock()
			s.delivered++
			s.mu.Unlock()
		}
	}
}

// take sends one owed item to the store.
func (s *sharedCounter) take(o owed) error {
	var err error
	switch o.kind {
	case 'l':
		_, err = s.client.eval(luaRelease, shaRelease, []string{s.runOrNone(o.run), s.keyOrNone(o.key), s.keyTokOrNone(o.key, o.token)}, s.ttlArg(), o.token, sharedKeyTTLSeconds, b2s(o.run != ""), b2s(o.key != ""), s.keyTokTTLSeconds(o.token))
	case 'p':
		_, err = s.client.eval(luaPause, shaPause, []string{s.runKey(o.run), s.pausedKey()}, o.run, "text:"+o.reason, s.ttlArg())
	case 'P':
		_, err = s.client.eval(luaKeyPause, shaKeyPause, []string{s.keyKey(o.key), s.pausedKeysKey()}, o.key, "text:"+o.reason, sharedKeyTTLSeconds, b2s(o.day), o.onDay)
	case 'u':
		_, err = s.client.eval(luaResume, shaResume, []string{s.runKey(o.run), s.pausedKey()}, o.run)
	case 'U':
		_, err = s.client.eval(luaKeyResume, shaKeyResume, []string{s.keyKey(o.key), s.pausedKeysKey()}, o.key)
	case 'c':
		_, err = s.client.eval(luaCap, shaCap, []string{s.runKey(o.run)}, strconv.FormatInt(micro(o.capUSD), 10), s.ttlArg())
	default:
		var v any
		args := []string{o.run, strconv.FormatInt(micro(o.costUSD), 10), b2s(o.loop), strconv.Itoa(s.policy.MaxCallsPerRun),
			strconv.FormatInt(micro(s.policy.MaxSpendUSDPerRun), 10), b2s(s.policy.PauseOnSuspectedLoop), s.ttlArg(), o.token, o.key}
		args = append(args, s.keyArgs()...)
		args = append(args, s.keyTokTTLSeconds(o.token))
		v, err = s.client.eval(luaRecord, shaRecord, []string{s.runOrNone(o.run), s.pausedKey(), s.keyOrNone(o.key), s.pausedKeysKey(), s.keyTokOrNone(o.key, o.token)}, args...)
		if err == nil {
			r, _ := v.([]any)
			if len(r) != 14 {
				return respError(fmt.Sprintf("ERR record script returned %d values", len(r)))
			}
			if t, _ := asInt(r[0]); t == 1 {
				s.notePaused(o.run, reasonText(asString(r[1])))
				s.stop(o.run, "", asString(r[1]), r[2], r[3], r[4], r[5], r[6])
			}
			if t, _ := asInt(r[7]); t == 1 {
				s.noteKeyPaused(o.key, keyReason(asString(r[8]), o.key))
				s.stop("", o.key, asString(r[8]), r[9], r[10], r[11], r[12], r[13])
			}
		}
	}
	if err == nil {
		s.answered()
	}
	return err
}

// flush delivers what is owed, off the request path, while the store is
// healthy. One at a time.
func (s *sharedCounter) flush() {
	s.mu.Lock()
	if s.flushing || s.recovering || s.degraded || (!s.killDirty && len(s.owed) == 0) {
		s.mu.Unlock()
		return
	}
	s.flushing = true
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.flushing = false; s.mu.Unlock() }()
	if _, err := s.deliver(); err != nil {
		s.fail(err)
	}
}

// recovered ends a gap: while still deciding locally it delivers everything
// owed (a few passes, since the gap keeps producing), learns the store's kill
// switch unless the operator changed it meanwhile, hands decisions back to
// the store with a fresh fallback, and delivers once more for what slipped in
// during the swap. Any failure keeps the gap; nothing is discarded.
func (s *sharedCounter) recovered() {
	s.mu.Lock()
	if !s.degraded || s.recovering {
		s.mu.Unlock()
		return
	}
	s.recovering = true
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.recovering = false; s.mu.Unlock() }()
	total := 0
	for pass := 0; pass < writeBackPasses; pass++ {
		n, err := s.deliver()
		total += n
		if err != nil {
			s.mu.Lock()
			s.lastErr = err.Error()
			s.mu.Unlock()
			return // still degraded; the next probe retries
		}
		s.mu.Lock()
		quiet := !s.killDirty && len(s.owed) == 0
		s.mu.Unlock()
		if quiet {
			break
		}
	}
	s.mu.Lock()
	dirty := s.killDirty
	s.mu.Unlock()
	storeKilled := false
	if !dirty {
		v, err := s.client.do("EXISTS", s.killKey())
		if err != nil {
			s.mu.Lock()
			s.lastErr = err.Error()
			s.mu.Unlock()
			return
		}
		n, _ := asInt(v)
		storeKilled = n == 1
	}
	s.mu.Lock()
	if !s.killDirty { // the operator may have acted while we were reading
		s.killed = storeKilled
	}
	s.local = s.freshFallback(s.killed, false)
	s.degraded = false
	s.failStreak = 0
	s.lastUntil = s.policy.Now().UTC()
	since := s.degradedSince
	s.mu.Unlock()
	n, err := s.deliver()
	total += n
	if err != nil {
		s.degrade(err, true) // what slipped in during the swap stays owed; the gap continues
		return
	}
	fmt.Fprintf(os.Stderr, "gateway: shared counter back after %s; %d item(s) delivered; local tallies dropped\n",
		s.lastUntil.Sub(since).Round(time.Second), total)
}

// keyReason turns a key's stored reason code into the sentence a caller sees.
func keyReason(code, key string) string {
	switch {
	case strings.HasPrefix(code, "kday:"):
		if v, err := strconv.ParseInt(strings.TrimPrefix(code, "kday:"), 10, 64); err == nil {
			return keyLabel(key) + " reached its daily spend cap of " + money(float64(v)/1e6) + " (resets at midnight UTC)"
		}
	case strings.HasPrefix(code, "klife:"):
		if v, err := strconv.ParseInt(strings.TrimPrefix(code, "klife:"), 10, 64); err == nil {
			return keyLabel(key) + " reached its spend cap of " + money(float64(v)/1e6)
		}
	}
	return reasonText(code)
}

// reasonText turns a stored reason code into the sentence a caller sees.
func reasonText(code string) string {
	switch {
	case strings.HasPrefix(code, "spend:"):
		if v, err := strconv.ParseInt(strings.TrimPrefix(code, "spend:"), 10, 64); err == nil {
			return "run reached the spend cap of " + money(float64(v)/1e6)
		}
	case strings.HasPrefix(code, "calls:"):
		return "run reached the call cap of " + strings.TrimPrefix(code, "calls:")
	case code == "loop":
		return "suspected loop"
	case strings.HasPrefix(code, "text:"):
		return strings.TrimPrefix(code, "text:")
	}
	return code
}

// The scripts. Each is one step in the store; Go formats reasons and fires
// alerts from what they return. Money is in whole micro-dollars throughout.
// Every path that touches a run key refreshes its expiry last. Tokens live in
// the run's own record: 'a:<token>' for an admit, 's:<token>' for a settlement.
var (
	// admit: KEYS run, killed, pausedSet
	//        ARGV run, bypass, maxCalls, capMicro, floorMicro, ttl, agent, team, model, token
	// returns {ok, code, transitioned, calls, spendMicro, agent, team, model}
	// admit: KEYS run (or a placeholder), killed, pausedSet, keyHash (or a placeholder), pausedKeys, keyTokens (or a placeholder)
	//        ARGV run, mode ('0' decide, '1' bypass, '2' shadow: decide but serve), maxCalls, capMicro, floorMicro, ttl, agent, team, model, token, key, today, kdayMicro, klifeMicro, keyTTL, keyTokTTL
	// returns {ok, code, transitioned, calls, spendMicro, agent, team, model, who}; who is 'run' or 'key'
	luaAdmit = `
local hasRun = ARGV[1] ~= ''
local hasKey = ARGV[11] ~= ''
local bypass, shadow = ARGV[2] == '1', ARGV[2] == '2'
local seenAt = hasRun and KEYS[1] or KEYS[6]
local seen = redis.call('HGET', seenAt, 'a:' .. ARGV[10])
if seen then
  if seen == '1' then return {1, 'dup', 0, 0, 0, '', '', '', 'run'} end
  return {0, seen, 0, 0, 0, '', '', '', string.sub(seen, 1, 1) == 'k' and 'key' or 'run'}
end
if redis.call('EXISTS', KEYS[2]) == 1 then return {0, 'killed', 0, 0, 0, '', '', '', 'run'} end
local function attrib(h)
  if ARGV[7] ~= '' then redis.call('HSET', h, 'agent', ARGV[7]) end
  if ARGV[8] ~= '' then redis.call('HSET', h, 'team', ARGV[8]) end
  if ARGV[9] ~= '' then redis.call('HSET', h, 'model', ARGV[9]) end
end
local calls, inflight, spend, runcap, rpaused, rreason = 0, 0, 0, 0, false, ''
local ragent, rteam, rmodel = '', '', ''
if hasRun then
  attrib(KEYS[1])
  local h = redis.call('HMGET', KEYS[1], 'calls', 'inflight', 'spend', 'cap', 'paused', 'reason', 'agent', 'team', 'model')
  calls = tonumber(h[1]) or 0; inflight = tonumber(h[2]) or 0; spend = tonumber(h[3]) or 0; runcap = tonumber(h[4]) or 0
  rpaused = h[5] == '1'; rreason = h[6] or 'paused'; ragent = h[7] or ''; rteam = h[8] or ''; rmodel = h[9] or ''
end
local kdcalls, kdspend, klcalls, klspend, kinflight, kpaused, kreason = 0, 0, 0, 0, 0, false, ''
local kagent, kteam, kmodel = '', '', ''
if hasKey then
  local d = redis.call('HGET', KEYS[4], 'day')
  if not d or ARGV[12] > d then
    redis.call('HSET', KEYS[4], 'day', ARGV[12], 'dcalls', 0, 'dspend', 0, 'inflight', 0)
    if redis.call('HGET', KEYS[4], 'dp') == '1' then
      redis.call('HSET', KEYS[4], 'paused', '0', 'reason', '', 'dp', '0')
      redis.call('SREM', KEYS[5], ARGV[11])
    end
  end
  attrib(KEYS[4])
  local k = redis.call('HMGET', KEYS[4], 'dcalls', 'dspend', 'lcalls', 'lspend', 'inflight', 'paused', 'reason', 'agent', 'team', 'model')
  kdcalls = tonumber(k[1]) or 0; kdspend = tonumber(k[2]) or 0; klcalls = tonumber(k[3]) or 0; klspend = tonumber(k[4]) or 0
  kinflight = tonumber(k[5]) or 0; kpaused = k[6] == '1'; kreason = k[7] or 'paused'; kagent = k[8] or ''; kteam = k[9] or ''; kmodel = k[10] or ''
end
local function reserve()
  if hasRun then
    redis.call('HINCRBY', KEYS[1], 'inflight', 1)
    redis.call('HSET', KEYS[1], 'a:' .. ARGV[10], '1')
    redis.call('EXPIRE', KEYS[1], ARGV[6])
  end
  if hasKey then
    redis.call('HINCRBY', KEYS[4], 'inflight', 1)
    if not hasRun then
      redis.call('HSET', KEYS[6], 'a:' .. ARGV[10], '1')
      redis.call('EXPIRE', KEYS[6], ARGV[16])
    end
    redis.call('EXPIRE', KEYS[4], ARGV[15])
  end
end
if hasRun and rpaused then
  redis.pcall('EXPIRE', KEYS[3], ARGV[6])
  if bypass or shadow then reserve(); return {1, shadow and rreason or '', 0, calls, spend, ragent, rteam, rmodel, 'run'} end
  redis.call('EXPIRE', KEYS[1], ARGV[6])
  if hasKey then redis.call('EXPIRE', KEYS[4], ARGV[15]) end
  return {0, rreason, 0, calls, spend, ragent, rteam, rmodel, 'run'}
end
if hasKey and kpaused then
  redis.pcall('EXPIRE', KEYS[5], ARGV[15])
  if bypass or shadow then reserve(); return {1, shadow and kreason or '', 0, kdcalls, kdspend, kagent, kteam, kmodel, 'key'} end
  redis.call('EXPIRE', KEYS[4], ARGV[15])
  if hasRun then redis.call('EXPIRE', KEYS[1], ARGV[6]) end
  return {0, kreason, 0, kdcalls, kdspend, kagent, kteam, kmodel, 'key'}
end
if hasKey and not bypass then
  local per = tonumber(ARGV[5])
  if klcalls > 0 and klspend / klcalls > per then per = klspend / klcalls end
  local kday, klife = tonumber(ARGV[13]), tonumber(ARGV[14])
  local kcode, dp = '', '0'
  if klife > 0 and klspend + per * kinflight >= klife then kcode = 'klife:' .. string.format('%d', klife)
  elseif kday > 0 and kdspend + per * kinflight >= kday then kcode = 'kday:' .. string.format('%d', kday); dp = '1' end
  if kcode ~= '' then
    redis.pcall('SADD', KEYS[5], ARGV[11])
    redis.pcall('EXPIRE', KEYS[5], ARGV[15])
    redis.call('HSET', KEYS[4], 'paused', '1', 'reason', kcode, 'dp', dp)
    local c, sp = klcalls, klspend
    if dp == '1' then c, sp = kdcalls, kdspend end
    if shadow then reserve(); return {1, kcode, 1, c, sp, kagent, kteam, kmodel, 'key'} end
    redis.call('HSET', seenAt, 'a:' .. ARGV[10], kcode)
    if not hasRun then redis.call('EXPIRE', KEYS[6], ARGV[16]) end
    redis.call('EXPIRE', KEYS[4], ARGV[15])
    if hasRun then redis.call('EXPIRE', KEYS[1], ARGV[6]) end
    return {0, kcode, 1, c, sp, kagent, kteam, kmodel, 'key'}
  end
end
if hasRun and not bypass then
  local code = ''
  local maxCalls = tonumber(ARGV[3])
  if maxCalls > 0 and calls + inflight >= maxCalls then code = 'calls:' .. ARGV[3] end
  if code == '' then
    local cap = tonumber(ARGV[4])
    if runcap > 0 and (cap == 0 or runcap < cap) then cap = runcap end
    if cap > 0 then
      local per = tonumber(ARGV[5])
      if calls > 0 and spend / calls > per then per = spend / calls end
      if spend + per * inflight >= cap then code = 'spend:' .. string.format('%d', cap) end
    end
  end
  if code ~= '' then
    redis.pcall('SADD', KEYS[3], ARGV[1])
    redis.pcall('EXPIRE', KEYS[3], ARGV[6])
    redis.call('HSET', KEYS[1], 'paused', '1', 'reason', code)
    if shadow then reserve(); return {1, code, 1, calls, spend, ragent, rteam, rmodel, 'run'} end
    redis.call('HSET', KEYS[1], 'a:' .. ARGV[10], code)
    redis.call('EXPIRE', KEYS[1], ARGV[6])
    if hasKey then redis.call('EXPIRE', KEYS[4], ARGV[15]) end
    return {0, code, 1, calls, spend, ragent, rteam, rmodel, 'run'}
  end
end
reserve()
return {1, '', 0, calls, spend, ragent, rteam, rmodel, 'run'}`

	// record: KEYS run (or placeholder), pausedSet, keyHash (or placeholder), pausedKeys, keyTokens (or placeholder)
	//         ARGV run, costMicro, loop, maxCalls, capMicro, pauseOnLoop, ttl, token, key, today, kdayMicro, klifeMicro, keyTTL, keyTokTTL
	// returns {rt, rcode, calls, spend, agent, team, model, kt, kcode, kcalls, kspend, kagent, kteam, kmodel}; a repeat of a token is a no-op
	luaRecord = `
local hasRun = ARGV[1] ~= ''
local hasKey = ARGV[9] ~= ''
local seenAt = hasRun and KEYS[1] or KEYS[5]
if redis.call('HSETNX', seenAt, 's:' .. ARGV[8], '1') == 0 then return {0, 'dup', 0, 0, '', '', '', 0, '', 0, 0, '', '', ''} end
if not hasRun then redis.call('EXPIRE', KEYS[5], ARGV[14]) end
local rt, rcode, calls, spend, ragent, rteam, rmodel = 0, '', 0, 0, '', '', ''
if hasRun then
  local inflight = tonumber(redis.call('HGET', KEYS[1], 'inflight')) or 0
  if inflight > 0 then redis.call('HINCRBY', KEYS[1], 'inflight', -1) end
  calls = redis.call('HINCRBY', KEYS[1], 'calls', 1)
  spend = redis.call('HINCRBY', KEYS[1], 'spend', ARGV[2])
  local h = redis.call('HMGET', KEYS[1], 'cap', 'paused', 'agent', 'team', 'model')
  ragent = h[3] or ''; rteam = h[4] or ''; rmodel = h[5] or ''
  if h[2] ~= '1' then
    local maxCalls = tonumber(ARGV[4])
    local cap = tonumber(ARGV[5])
    local runcap = tonumber(h[1]) or 0
    if runcap > 0 and (cap == 0 or runcap < cap) then cap = runcap end
    if maxCalls > 0 and calls >= maxCalls then rcode = 'calls:' .. ARGV[4]
    elseif cap > 0 and spend >= cap then rcode = 'spend:' .. string.format('%d', cap)
    elseif ARGV[3] == '1' and ARGV[6] == '1' then rcode = 'loop' end
    if rcode ~= '' then
      rt = 1
      redis.pcall('SADD', KEYS[2], ARGV[1])
      redis.pcall('EXPIRE', KEYS[2], ARGV[7])
      redis.call('HSET', KEYS[1], 'paused', '1', 'reason', rcode)
    end
  end
  redis.call('EXPIRE', KEYS[1], ARGV[7])
end
local kt, kcode, kcalls, kspend, kagent, kteam, kmodel = 0, '', 0, 0, '', '', ''
if hasKey then
  local d = redis.call('HGET', KEYS[3], 'day')
  if not d or ARGV[10] > d then
    redis.call('HSET', KEYS[3], 'day', ARGV[10], 'dcalls', 0, 'dspend', 0, 'inflight', 0)
    if redis.call('HGET', KEYS[3], 'dp') == '1' then
      redis.call('HSET', KEYS[3], 'paused', '0', 'reason', '', 'dp', '0')
      redis.call('SREM', KEYS[4], ARGV[9])
    end
  end
  local kinflight = tonumber(redis.call('HGET', KEYS[3], 'inflight')) or 0
  if kinflight > 0 then redis.call('HINCRBY', KEYS[3], 'inflight', -1) end
  local dcalls = redis.call('HINCRBY', KEYS[3], 'dcalls', 1)
  local dspend = redis.call('HINCRBY', KEYS[3], 'dspend', ARGV[2])
  local lcalls = redis.call('HINCRBY', KEYS[3], 'lcalls', 1)
  local lspend = redis.call('HINCRBY', KEYS[3], 'lspend', ARGV[2])
  local k = redis.call('HMGET', KEYS[3], 'paused', 'agent', 'team', 'model')
  kagent = k[2] or ''; kteam = k[3] or ''; kmodel = k[4] or ''
  kcalls, kspend = lcalls, lspend
  if k[1] ~= '1' then
    local kday, klife = tonumber(ARGV[11]), tonumber(ARGV[12])
    local dp = '0'
    if klife > 0 and lspend >= klife then kcode = 'klife:' .. string.format('%d', klife)
    elseif kday > 0 and dspend >= kday then kcode = 'kday:' .. string.format('%d', kday); dp = '1'; kcalls, kspend = dcalls, dspend end
    if kcode ~= '' then
      kt = 1
      redis.pcall('SADD', KEYS[4], ARGV[9])
      redis.pcall('EXPIRE', KEYS[4], ARGV[13])
      redis.call('HSET', KEYS[3], 'paused', '1', 'reason', kcode, 'dp', dp)
    end
  end
  redis.call('EXPIRE', KEYS[3], ARGV[13])
end
return {rt, rcode, calls, spend, ragent, rteam, rmodel, kt, kcode, kcalls, kspend, kagent, kteam, kmodel}`

	// release: KEYS run (or placeholder), keyHash (or placeholder), keyTokens (or placeholder); ARGV ttl, token, keyTTL, hasRun, hasKey, keyTokTTL — a repeat of a token is a no-op
	luaRelease = `
local seenAt = ARGV[4] == '1' and KEYS[1] or KEYS[3]
if redis.call('HSETNX', seenAt, 's:' .. ARGV[2], '1') == 0 then return 0 end
if ARGV[4] ~= '1' then redis.call('EXPIRE', KEYS[3], ARGV[6]) end
if ARGV[4] == '1' then
  local i = tonumber(redis.call('HGET', KEYS[1], 'inflight')) or 0
  if i > 0 then redis.call('HINCRBY', KEYS[1], 'inflight', -1) end
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
if ARGV[5] == '1' then
  local i = tonumber(redis.call('HGET', KEYS[2], 'inflight')) or 0
  if i > 0 then redis.call('HINCRBY', KEYS[2], 'inflight', -1) end
  redis.call('EXPIRE', KEYS[2], ARGV[3])
end
return 1`

	// cap: KEYS run; ARGV capMicro, ttl
	luaCap = `
redis.call('HSET', KEYS[1], 'cap', ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])
return 1`

	// resume: KEYS run, pausedSet; ARGV run — a resumed run gets a fresh budget
	luaResume = `
if redis.call('HGET', KEYS[1], 'paused') ~= '1' then return 0 end
redis.call('HSET', KEYS[1], 'paused', '0', 'reason', '', 'calls', '0', 'spend', '0', 'inflight', '0')
redis.call('SREM', KEYS[2], ARGV[1])
return 1`

	// keyResume: KEYS keyHash, pausedKeys; ARGV key — a resumed key gets a fresh budget for the cap that
	// paused it: today's after a daily pause, today's and total after a total pause
	luaKeyResume = `
if redis.call('HGET', KEYS[1], 'paused') ~= '1' then return 0 end
if redis.call('HGET', KEYS[1], 'dp') == '1' then
  redis.call('HSET', KEYS[1], 'paused', '0', 'reason', '', 'dp', '0', 'dcalls', '0', 'dspend', '0', 'inflight', '0')
else
  redis.call('HSET', KEYS[1], 'paused', '0', 'reason', '', 'dp', '0', 'dcalls', '0', 'dspend', '0', 'lcalls', '0', 'lspend', '0', 'inflight', '0')
end
redis.call('SREM', KEYS[2], ARGV[1])
return 1`

	// pause: KEYS run, pausedSet; ARGV run, code, ttl — writes back a pause the fallback made
	luaPause = `
redis.pcall('SADD', KEYS[2], ARGV[1])
redis.pcall('EXPIRE', KEYS[2], ARGV[3])
redis.call('HSET', KEYS[1], 'paused', '1', 'reason', ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 1`

	// keyPause: KEYS keyHash, pausedKeys; ARGV key, code, keyTTL, dayPause, pauseDay — writes back a key pause the
	// fallback made; a daily pause is stamped with the day it was made on, so the next touch does not read it as
	// last night's, and one from an earlier day than the store's is already over and is not written at all
	luaKeyPause = `
if ARGV[4] == '1' then
  local d = redis.call('HGET', KEYS[1], 'day')
  if d and ARGV[5] < d then redis.call('EXPIRE', KEYS[1], ARGV[3]); return 0 end
  if not d or ARGV[5] > d then redis.call('HSET', KEYS[1], 'day', ARGV[5], 'dcalls', 0, 'dspend', 0, 'inflight', 0) end
end
redis.pcall('SADD', KEYS[2], ARGV[1])
redis.pcall('EXPIRE', KEYS[2], ARGV[3])
redis.call('HSET', KEYS[1], 'paused', '1', 'reason', ARGV[2], 'dp', ARGV[4])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 1`

	// status: KEYS killed, pausedSet, pausedKeys; ARGV prefix, limit, today
	// returns {killed, pausedTotal, {run, reason, calls, spend, ...}, {key, reason, dcalls, dspend, lspend, ...}}, pruning stale members
	luaStatus = `
local runs, keys = {}, {}
local n, total = 0, 0
for _, run in ipairs(redis.call('SMEMBERS', KEYS[2])) do
  local h = redis.call('HMGET', ARGV[1] .. 'run:' .. run, 'paused', 'reason', 'calls', 'spend')
  if h[1] ~= '1' then
    redis.call('SREM', KEYS[2], run)
  else
    total = total + 1
    if n < tonumber(ARGV[2]) then
      n = n + 1
      runs[#runs+1] = run; runs[#runs+1] = h[2] or ''; runs[#runs+1] = tonumber(h[3]) or 0; runs[#runs+1] = tonumber(h[4]) or 0
    end
  end
end
local kn = 0
for _, key in ipairs(redis.call('SMEMBERS', KEYS[3])) do
  local h = redis.call('HMGET', ARGV[1] .. 'key:' .. key, 'paused', 'reason', 'dcalls', 'dspend', 'lspend', 'dp', 'day')
  if h[1] ~= '1' or (h[6] == '1' and h[7] and ARGV[3] > h[7]) then
    redis.call('SREM', KEYS[3], key)
  elseif kn < tonumber(ARGV[2]) then
    kn = kn + 1
    keys[#keys+1] = key; keys[#keys+1] = h[2] or ''; keys[#keys+1] = tonumber(h[3]) or 0; keys[#keys+1] = tonumber(h[4]) or 0; keys[#keys+1] = tonumber(h[5]) or 0
  end
end
return {redis.call('EXISTS', KEYS[1]), total, runs, keys}`

	shaAdmit, shaRecord, shaRelease, shaCap, shaResume, shaKeyResume, shaPause, shaKeyPause, shaStatus = sha(luaAdmit), sha(luaRecord), sha(luaRelease), sha(luaCap), sha(luaResume), sha(luaKeyResume), sha(luaPause), sha(luaKeyPause), sha(luaStatus)
)

func sha(script string) string {
	h := sha1.Sum([]byte(script))
	return hex.EncodeToString(h[:])
}

func b2s(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// stop fires the alert exactly once per transition, from the tallies and
// attribution the script returned, for a run or for a key.
func (s *sharedCounter) stop(run, key, code string, calls, spend, agent, team, model any) {
	s.mu.Lock()
	f := s.onPause
	s.mu.Unlock()
	if f == nil {
		return
	}
	c, _ := asInt(calls)
	sp, _ := asInt(spend)
	reason := reasonText(code)
	if key != "" {
		reason = keyReason(code, key)
	}
	ev := StopEvent{Run: run, Key: key, Agent: asString(agent), Team: asString(team), Model: asString(model),
		Reason: reason, Calls: int(c), SpendUSD: float64(sp) / 1e6, At: s.policy.Now().UTC(), Shadow: s.policy.Shadow, dayPause: strings.HasPrefix(code, "kday:")}
	go f(ev)
}

// isDayPauseReason tells a daily-cap pause from a total-cap one by the
// sentence the store or the controller produced for it.
func isDayPauseReason(reason string) bool {
	return strings.HasSuffix(reason, "(resets at midnight UTC)")
}

// keyPauseNote is what this process remembers of a key pause the store
// reported: why, and for a daily pause, the day it was seen on.
type keyPauseNote struct{ reason, day string }

func (s *sharedCounter) keyNote(reason string) keyPauseNote {
	n := keyPauseNote{reason: reason}
	if isDayPauseReason(reason) {
		n.day = s.today()
	}
	return n
}

// noteKeyPaused / forgetKeyPaused mirror notePaused for keys.
func (s *sharedCounter) noteKeyPaused(key, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, known := s.keysSeen[key]; known || len(s.keysSeen) < maxStatusRuns {
		s.keysSeen[key] = s.keyNote(reason)
	}
}

func (s *sharedCounter) forgetKeyPaused(key string) {
	s.mu.Lock()
	delete(s.keysSeen, key)
	s.mu.Unlock()
}

func (s *sharedCounter) noteKey(key, agent, team, model string) {
	if key == "" {
		return
	}
	local, yes := s.fallback()
	if yes {
		local.noteKey(key, agent, team, model)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n, known := s.keyNotes[key]
	if !known && len(s.keyNotes) >= maxNotes {
		return
	}
	if agent != "" {
		n.agent = agent
	}
	if team != "" {
		n.team = team
	}
	if model != "" {
		n.model = model
	}
	s.keyNotes[key] = n
}

// notePaused remembers that the store reports a run paused, so a fallback
// started later starts from that; forget clears it.
func (s *sharedCounter) notePaused(run, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, known := s.pausedSeen[run]; known || len(s.pausedSeen) < maxStatusRuns {
		s.pausedSeen[run] = reason
	}
}

func (s *sharedCounter) forgetPaused(run string) {
	s.mu.Lock()
	delete(s.pausedSeen, run)
	s.mu.Unlock()
}

func (s *sharedCounter) noteRun(run, agent, team, model string) {
	if run == "" {
		return
	}
	local, yes := s.fallback()
	if yes {
		local.noteRun(run, agent, team, model)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n, known := s.notes[run]
	if !known && len(s.notes) >= maxNotes {
		return
	}
	if agent != "" {
		n.agent = agent
	}
	if team != "" {
		n.team = team
	}
	if model != "" {
		n.model = model
	}
	s.notes[run] = n
}

func (s *sharedCounter) takeNote(run string) noteTags {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.notes[run]
	delete(s.notes, run)
	return n
}

// keepNote puts attribution back after a failed admit, without clobbering
// anything newer.
func (s *sharedCounter) keepNote(run string, n noteTags) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.notes[run]
	if cur.agent == "" {
		cur.agent = n.agent
	}
	if cur.team == "" {
		cur.team = n.team
	}
	if cur.model == "" {
		cur.model = n.model
	}
	if cur != (noteTags{}) && (len(s.notes) < maxNotes || s.notes[run] != (noteTags{})) {
		s.notes[run] = cur
	}
}

func (s *sharedCounter) admit(run string, bypass bool) (bool, string) {
	ok, reason, _ := s.admitWhere(run, "", bypass)
	return ok, reason
}

func (s *sharedCounter) admitWhere(run, key string, bypass bool) (bool, string, string) {
	s.mu.Lock()
	local, gap, killHere := s.local, s.degraded, s.killDirty && s.killed
	if gap {
		s.localDecided++
	}
	s.mu.Unlock()
	if gap {
		ok, reason := local.admitKeyed(run, key, bypass)
		return ok, reason, "local"
	}
	if killHere {
		// The operator engaged the kill switch here and the store has not
		// taken it yet: it holds on this gateway meanwhile.
		return false, "kill switch engaged", "local"
	}
	if run == "" && key == "" {
		// Untagged, keyless calls are never capped, but the kill switch still applies.
		v, err := s.client.do("EXISTS", s.killKey())
		if err != nil {
			return s.admitLocally(run, key, bypass, err)
		}
		s.answered()
		n, _ := asInt(v)
		s.mirrorKill(n == 1)
		if n == 1 {
			return false, "kill switch engaged", "shared"
		}
		return true, "", "shared"
	}
	var n, kn noteTags
	if run != "" {
		n = s.takeNote(run)
	}
	if key != "" { // what the key's own calls said fills anything the run's note left empty
		s.mu.Lock()
		kn = s.keyNotes[key]
		delete(s.keyNotes, key)
		s.mu.Unlock()
		if n.agent == "" {
			n.agent = kn.agent
		}
		if n.team == "" {
			n.team = kn.team
		}
		if n.model == "" {
			n.model = kn.model
		}
	}
	tok := s.token()
	mode := b2s(bypass) // '0' decide, '1' bypass; '2' shadow: decide, but serve and reserve either way
	if s.policy.Shadow && !bypass {
		mode = "2"
	}
	args := []string{run, mode, strconv.Itoa(s.policy.MaxCallsPerRun), strconv.FormatInt(micro(s.policy.MaxSpendUSDPerRun), 10),
		strconv.FormatInt(micro(s.policy.ReserveUSDPerCall), 10), s.ttlArg(), n.agent, n.team, n.model, tok, key}
	args = append(args, s.keyArgs()...)
	args = append(args, s.keyTokTTLSeconds(tok))
	v, err := s.client.eval(luaAdmit, shaAdmit, []string{s.runOrNone(run), s.killKey(), s.pausedKey(), s.keyOrNone(key), s.pausedKeysKey(), s.keyTokOrNone(key, tok)}, args...)
	r, _ := v.([]any)
	if err == nil && len(r) != 9 {
		err = respError(fmt.Sprintf("ERR admit script returned %d values", len(r)))
	}
	if err != nil {
		if run != "" {
			s.keepNote(run, n)
		}
		if key != "" && kn != (noteTags{}) {
			s.mu.Lock()
			if _, known := s.keyNotes[key]; !known {
				s.keyNotes[key] = kn
			}
			s.mu.Unlock()
		}
		return s.admitLocally(run, key, bypass, err)
	}
	s.answered()
	go s.flush()
	ok, _ := asInt(r[0])
	code := asString(r[1])
	who := asString(r[8])
	s.mirrorKill(code == "killed")
	if code == "killed" {
		return false, "kill switch engaged", "shared"
	}
	if t, _ := asInt(r[2]); t == 1 {
		if who == "key" {
			s.stop("", key, code, r[3], r[4], r[5], r[6], r[7])
		} else {
			s.stop(run, "", code, r[3], r[4], r[5], r[6], r[7])
		}
	}
	if ok == 1 {
		if code != "" && code != "dup" { // shadow mode: served, and a cap would have refused it
			if who == "key" {
				reason := keyReason(code, key)
				s.noteKeyPaused(key, reason)
				return true, reason, "shared:" + tok
			}
			s.notePaused(run, reasonText(code))
			return true, reasonText(code), "shared:" + tok
		}
		if !bypass && code == "" { // a bypass, or a replayed id, says nothing about whether the run or the key is still paused
			s.forgetPaused(run)
			s.forgetKeyPaused(key)
		}
		return true, "", "shared:" + tok
	}
	if code == "" || code == "dup" {
		return false, reasonText(code), "shared"
	}
	if who == "key" {
		reason := keyReason(code, key)
		s.noteKeyPaused(key, reason)
		return false, reason, "shared"
	}
	s.notePaused(run, reasonText(code))
	return false, reasonText(code), "shared"
}

// admitLocally decides one call per process after a store failure. The
// failure may or may not switch the whole counter over (see fail).
func (s *sharedCounter) admitLocally(run, key string, bypass bool, err error) (bool, string, string) {
	s.fail(err)
	s.mu.Lock()
	switch {
	case s.degraded:
		s.localDecided++
	case errors.Is(err, errPoolBusy):
		s.busyDecided++
	default:
		s.oneOffDecided++
	}
	local := s.local
	s.mu.Unlock()
	ok, reason := local.admitKeyed(run, key, bypass)
	return ok, reason, "local"
}

// mirrorKill keeps the kill switch in step with what the store says, unless
// the operator has set it and the store has not taken that yet.
func (s *sharedCounter) mirrorKill(on bool) {
	s.mu.Lock()
	if !s.killDirty && s.killed != on {
		s.killed = on
		s.local.setKilled(on)
	}
	s.mu.Unlock()
}

// settle sends a settlement to the counter that admitted the call: the
// fallback for a local decision; the store otherwise, owed if it cannot take
// it right now (safe: the store runs a token once).
func (s *sharedCounter) settle(o owed, where string, apply func(*controller)) {
	label, token, _ := strings.Cut(where, ":")
	if label != "shared" {
		local, _ := s.fallback()
		apply(local)
		return
	}
	o.token = token
	if _, gap := s.fallback(); gap {
		s.owe(o)
		return
	}
	if err := s.take(o); err != nil {
		if refusal(err) {
			s.mu.Lock()
			s.refused++
			s.mu.Unlock()
			s.fail(err)
			return
		}
		s.fail(err)
		s.owe(o)
	}
}

func (s *sharedCounter) record(run, key string, costUSD float64, suspectedLoop bool, where string) {
	if run == "" && key == "" {
		return
	}
	s.settle(owed{kind: 'r', run: run, key: key, costUSD: costUSD, loop: suspectedLoop}, where,
		func(c *controller) { c.recordKeyed(run, key, costUSD, suspectedLoop) })
}

func (s *sharedCounter) release(run, key string, where string) {
	if run == "" && key == "" {
		return
	}
	s.settle(owed{kind: 'l', run: run, key: key}, where, func(c *controller) { c.releaseKeyed(run, key) })
}

// resumeKey clears a key's pause everywhere, the way resumeRun does for a run.
func (s *sharedCounter) resumeKey(key string) bool {
	s.forgetKeyPaused(key)
	local, gap := s.fallback()
	if gap {
		local.resumeKey(key)
		s.owe(owed{kind: 'U', key: key})
		return true
	}
	v, err := s.client.eval(luaKeyResume, shaKeyResume, []string{s.keyKey(key), s.pausedKeysKey()}, key)
	if err != nil {
		if refusal(err) {
			s.fail(err)
			return false
		}
		s.fail(err)
		local.resumeKey(key)
		s.owe(owed{kind: 'U', key: key})
		return true
	}
	s.answered()
	local.resumeKey(key)
	n, _ := asInt(v)
	return n == 1
}

// setRunCap applies a caller's inline cap everywhere. If the store cannot take
// it right now it is applied here and delivered later.
func (s *sharedCounter) setRunCap(run string, maxSpendUSD float64) {
	if run == "" || maxSpendUSD <= 0 {
		return
	}
	local, gap := s.fallback()
	local.setRunCap(run, maxSpendUSD)
	if gap {
		s.owe(owed{kind: 'c', run: run, capUSD: maxSpendUSD})
		return
	}
	if _, err := s.client.eval(luaCap, shaCap, []string{s.runKey(run)}, strconv.FormatInt(micro(maxSpendUSD), 10), s.ttlArg()); err != nil {
		if !refusal(err) {
			s.owe(owed{kind: 'c', run: run, capUSD: maxSpendUSD})
		}
		s.fail(err)
		return
	}
	s.answered()
}

// resumeRun clears a pause everywhere. If the store cannot take it right now
// the local copy is cleared and the ask is delivered later, after any pause
// owed for the same run, so the operator's decision is the last word.
func (s *sharedCounter) resumeRun(run string) bool {
	s.forgetPaused(run)
	local, gap := s.fallback()
	if gap {
		local.resumeRun(run)
		s.owe(owed{kind: 'u', run: run})
		return true
	}
	v, err := s.client.eval(luaResume, shaResume, []string{s.runKey(run), s.pausedKey()}, run)
	if err != nil {
		if refusal(err) {
			s.fail(err)
			return false
		}
		s.fail(err)
		local.resumeRun(run)
		s.owe(owed{kind: 'u', run: run})
		return true
	}
	s.answered()
	local.resumeRun(run) // the fallback's own copy, if it had one, is cleared too
	n, _ := asInt(v)
	return n == 1
}

func (s *sharedCounter) setKilled(on bool) {
	s.mu.Lock()
	s.killed = on
	s.killDirty = true
	s.killGen++
	gen := s.killGen
	s.local.setKilled(on)
	gap := s.degraded
	s.mu.Unlock()
	if gap {
		return // delivered with the gap
	}
	s.deliverMu.Lock() // kill writes never interleave with each other or with a delivery
	var err error
	if on {
		_, err = s.client.do("SET", s.killKey(), "1")
	} else {
		_, err = s.client.do("DEL", s.killKey())
	}
	s.deliverMu.Unlock()
	if err != nil {
		if refusal(err) {
			s.mu.Lock()
			s.refused++
			s.mu.Unlock()
		}
		s.fail(err) // stays dirty; the next flush delivers it
		return
	}
	s.answered()
	s.mu.Lock()
	if s.killGen == gen {
		s.killDirty = false
	}
	s.mu.Unlock()
}

// CounterReport is the status endpoint's view of a shared counter. The counts
// are since this gateway started unless they say otherwise.
type CounterReport struct {
	Store         string `json:"store"`                         // where tallies live, without any password
	State         string `json:"state"`                         // "ok", or "degraded" while deciding per process
	DegradedSince string `json:"degraded_since,omitempty"`      // while degraded
	LastError     string `json:"last_error,omitempty"`          // what the store last said or failed with
	Outages       int    `json:"outages"`                       // gaps since this gateway started
	LastOutage    string `json:"last_outage,omitempty"`         // "since → until" of the last finished gap
	LocalDecided  int    `json:"decided_locally,omitempty"`     // cap decisions the fallback made in the current or last gap
	BusyDecided   int    `json:"decided_while_busy,omitempty"`  // cap decisions made locally because every connection was busy
	OneOffDecided int    `json:"decided_on_one_drop,omitempty"` // cap decisions made locally on a single dropped answer or bad key
	Pending       int    `json:"owed_to_store"`                 // settlements, pauses, caps, resumes and the kill switch the store has not taken yet
	Delivered     int    `json:"owed_delivered,omitempty"`      // owed items the store has taken
	Dropped       int    `json:"owed_dropped,omitempty"`        // owed items dropped because the queue was full
	Refused       int    `json:"owed_refused,omitempty"`        // owed items the store answered with an error
	PausedTotal   int    `json:"paused_total,omitempty"`        // paused runs in the store; paused_runs lists at most 200
	RunsTracked   string `json:"runs_tracked_means"`            // what runs_tracked counts for this counter
	Note          string `json:"note,omitempty"`                // when this status call itself could not reach the store
}

func (s *sharedCounter) report() *CounterReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := &CounterReport{Store: s.storeURL, State: "ok", LastError: s.lastErr, Outages: s.outages,
		LocalDecided: s.localDecided, BusyDecided: s.busyDecided, OneOffDecided: s.oneOffDecided,
		Pending: len(s.owed), Delivered: s.delivered, Dropped: s.dropped, Refused: s.refused, RunsTracked: "paused runs only"}
	if s.killDirty {
		r.Pending++
	}
	if s.degraded {
		r.State, r.DegradedSince = "degraded", s.degradedSince.Format(time.RFC3339)
		r.RunsTracked = "runs the per-process fallback is tracking"
	} else if !s.lastUntil.IsZero() {
		r.LastOutage = s.lastSince.Format(time.RFC3339) + " → " + s.lastUntil.Format(time.RFC3339)
	}
	return r
}

func (s *sharedCounter) status() Status {
	st := Status{Policy: PolicyReport{
		MaxCallsPerRun: s.policy.MaxCallsPerRun, MaxSpendUSDPerRun: s.policy.MaxSpendUSDPerRun,
		ReserveUSDPerCall: s.policy.ReserveUSDPerCall, PauseOnSuspectedLoop: s.policy.PauseOnSuspectedLoop,
		MaxSpendUSDPerKeyDay: s.policy.MaxSpendUSDPerKeyDay, MaxSpendUSDPerKey: s.policy.MaxSpendUSDPerKey,
		Shadow: s.policy.Shadow,
	}, PausedRuns: []PausedRun{}}
	local, gap := s.fallback()
	if !gap {
		v, err := s.client.eval(luaStatus, shaStatus, []string{s.killKey(), s.pausedKey(), s.pausedKeysKey()}, s.prefix, strconv.Itoa(maxStatusRuns), s.today())
		r, _ := v.([]any)
		var runs, keys []any
		if len(r) == 4 {
			runs, _ = r[2].([]any)
			keys, _ = r[3].([]any)
		}
		if err == nil && (len(r) != 4 || len(runs)%4 != 0 || len(keys)%5 != 0) {
			err = respError(fmt.Sprintf("ERR status script returned %d values", len(r)))
		}
		if err == nil {
			s.answered()
			k, _ := asInt(r[0])
			s.mirrorKill(k == 1)
			total, _ := asInt(r[1])
			seen := map[string]string{}
			for i := 0; i+3 < len(runs); i += 4 {
				calls, _ := asInt(runs[i+2])
				spend, _ := asInt(runs[i+3])
				p := PausedRun{Run: asString(runs[i]), Reason: reasonText(asString(runs[i+1])), Calls: int(calls), SpendUSD: float64(spend) / 1e6}
				st.PausedRuns = append(st.PausedRuns, p)
				seen[p.Run] = p.Reason
			}
			sortPaused(st.PausedRuns)
			kseen := map[string]keyPauseNote{}
			for i := 0; i+4 < len(keys); i += 5 {
				key := asString(keys[i])
				dcalls, _ := asInt(keys[i+2])
				dspend, _ := asInt(keys[i+3])
				lspend, _ := asInt(keys[i+4])
				p := PausedKey{Key: key, Reason: keyReason(asString(keys[i+1]), key), CallsToday: int(dcalls), SpendUSDToday: float64(dspend) / 1e6, TotalUSD: float64(lspend) / 1e6}
				st.PausedKeys = append(st.PausedKeys, p)
				kseen[key] = s.keyNote(p.Reason)
			}
			sort.Slice(st.PausedKeys, func(i, j int) bool { return st.PausedKeys[i].Key < st.PausedKeys[j].Key })
			st.Runs = int(total)
			s.mu.Lock()
			s.pausedSeen, s.keysSeen = seen, kseen
			if k == 1 || (s.killDirty && s.killed) {
				st.Killed = 1
			}
			s.mu.Unlock()
			st.Counter = s.report()
			st.Counter.PausedTotal = int(total)
			st.Note = shadowNote(s.policy, st.Killed == 1)
			return st
		}
		s.fail(err)
		local, _ = s.fallback()
		st.Counter = s.report()
		if st.Counter.State == "ok" {
			st.Counter.Note = "the store did not answer this status call; killed and paused_runs in this reply are this gateway's own fallback view"
			st.Counter.RunsTracked = "runs the per-process fallback is tracking"
		}
	} else {
		st.Counter = s.report()
	}
	ls := local.status()
	st.Killed, st.Runs, st.PausedKeys = ls.Killed, ls.Runs, ls.PausedKeys
	if ls.PausedRuns != nil {
		st.PausedRuns = ls.PausedRuns
	}
	st.Note = shadowNote(s.policy, st.Killed == 1)
	return st
}
