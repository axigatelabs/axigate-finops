// Command axigate-finops is the local CLI: it reads provider reports on the
// user's machine and writes the diagnostic and the statement next to them.
// Nothing is uploaded. `fetch` pulls the reports from a provider's admin API
// into local files first, so what `analyze` reads is always inspectable.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/axigatelabs/axigate-finops/internal/console"
	"github.com/axigatelabs/axigate-finops/internal/fetch"
	"github.com/axigatelabs/axigate-finops/internal/focus"
	"github.com/axigatelabs/axigate-finops/internal/gateway"
	"github.com/axigatelabs/axigate-finops/internal/importers/anyfile"
	"github.com/axigatelabs/axigate-finops/internal/ledger"
	"github.com/axigatelabs/axigate-finops/internal/owners"
	"github.com/axigatelabs/axigate-finops/internal/report"
	"github.com/axigatelabs/axigate-finops/internal/seed"
	"github.com/axigatelabs/axigate-finops/internal/statement"
)

const usage = `axigate-finops — AI FinOps for agentic workloads

Usage:
  axigate-finops analyze <report.json>... [--owners owners.csv] [--out DIR]
  axigate-finops fetch openai    --since YYYY-MM-DD [--until YYYY-MM-DD] --out DIR   (OPENAI_ADMIN_KEY)
  axigate-finops fetch anthropic --since YYYY-MM-DD [--until YYYY-MM-DD] --out DIR   (ANTHROPIC_ADMIN_KEY)
  axigate-finops gateway --provider openai|anthropic|gemini --upstream URL [--listen ADDR] [--ledger FILE]
  axigate-finops serve [--provider openai|anthropic|gemini] [--gateway-listen ADDR] [--console-listen ADDR] [--ledger FILE]
  axigate-finops seed --out FILE [--events N] [--seed N]
  axigate-finops console --ledger FILE [--listen ADDR]

analyze reads saved provider reports (OpenAI usage/costs pages, Anthropic
usage/cost reports, the Anthropic Console cost CSV export, or the gateway's
own JSONL ledger, in any order), maps ids to owners from a CSV, and writes
report.txt and statement.csv into --out (default: the current directory).
Gateway events already carry per-agent and per-run tags. Everything stays on
this machine.

fetch calls the provider's admin API with a read-only admin key from the
environment and saves every page as JSON into --out, so you can see exactly
what was read before analyze reads it. Nothing else is sent anywhere.

console serves a dark-mode dashboard and a JSON/FOCUS/statement API over a
gateway JSONL ledger — full history and every export, nothing gated. It
re-reads the ledger on each request, so a refresh shows new events. Point a
browser at the listen address.

seed writes N synthetic gateway events (default 10000) to a JSONL file, across
several teams and agents with one runaway loop, for building and testing the
console without real keys or traffic. The data is invented and says so.

gateway runs a self-hosted pass-through in front of one provider: point your
app's base URL at it, keep using your normal key, and every call is forwarded
unchanged while one metadata-only ledger event per request is appended to
--ledger (default: ./gateway-events.jsonl). It reads token counts and your
X-AxiGate-Team/Project/Customer/Agent/Run headers, and honors an inline
X-AxiGate-Max-Spend header as a per-run spend cap set from your code; it never
records prompts or answers and never alters the response. Read the ledger later
with analyze.

serve runs the gateway and the live dashboard together from this one binary, the
whole product in one command: the gateway on --gateway-listen (default
0.0.0.0:8080) and the dashboard on --console-listen (default 0.0.0.0:8906),
sharing one --ledger file so spend appears in the dashboard the moment a call
lands. This is what the Docker image runs by default.
`

func main() {
	if len(os.Args) < 2 || os.Args[1] == "--help" || os.Args[1] == "-h" {
		fmt.Fprint(os.Stdout, usage)
		return
	}
	var err error
	switch os.Args[1] {
	case "analyze":
		err = analyze(os.Args[2:])
	case "fetch":
		err = doFetch(os.Args[2:])
	case "gateway":
		err = doGateway(os.Args[2:])
	case "serve":
		err = doServe(os.Args[2:])
	case "seed":
		err = doSeed(os.Args[2:])
	case "console":
		err = doConsole(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "axigate-finops: unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "axigate-finops: %v\n", err)
		os.Exit(1)
	}
}

func analyze(args []string) error {
	fs := flag.NewFlagSet("analyze", flag.ContinueOnError)
	ownersPath := fs.String("owners", "", "CSV mapping provider ids to team/project/customer/agent")
	out := fs.String("out", ".", "directory for report.txt and statement.csv")
	focusPath := fs.String("focus", "", "also write a FOCUS-format export to this file")
	var files []string
	// Accept flags before or after the file list.
	rest := []string{}
	for i := 0; i < len(args); i++ {
		a := args[i]
		if strings.HasPrefix(a, "-") {
			rest = append(rest, a)
			if !strings.Contains(a, "=") && i+1 < len(args) {
				rest = append(rest, args[i+1])
				i++
			}
			continue
		}
		files = append(files, a)
	}
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("analyze needs at least one report file")
	}
	m := owners.Empty()
	if *ownersPath != "" {
		f, err := os.Open(*ownersPath)
		if err != nil {
			return err
		}
		defer f.Close()
		if m, err = owners.Load(f); err != nil {
			return err
		}
	}
	var events []ledger.Event
	kinds := map[anyfile.Kind]int{}
	for _, path := range files {
		evs, kind, err := anyfile.Parse(path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		kinds[kind]++
		events = append(events, evs...)
	}
	// Idempotence: the same row from two overlapping pages is one row.
	seen := map[string]bool{}
	deduped := events[:0]
	for _, e := range events {
		if seen[e.ID] {
			continue
		}
		seen[e.ID] = true
		deduped = append(deduped, e)
	}
	events = deduped
	unknown := m.Apply(events)

	r := report.Build(events)
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	text := report.Text(r)
	if err := os.WriteFile(filepath.Join(*out, "report.txt"), []byte(text), 0o644); err != nil {
		return err
	}
	sf, err := os.Create(filepath.Join(*out, "statement.csv"))
	if err != nil {
		return err
	}
	if err := statement.WriteCSV(sf, events); err != nil {
		sf.Close()
		return err
	}
	sf.Close()
	if *focusPath != "" {
		ff, err := os.Create(*focusPath)
		if err != nil {
			return err
		}
		if err := focus.WriteCSV(ff, events, "axigate"); err != nil {
			ff.Close()
			return err
		}
		ff.Close()
	}
	fmt.Print(text)
	var kindList []string
	for k, n := range kinds {
		kindList = append(kindList, fmt.Sprintf("%s×%d", k, n))
	}
	sort.Strings(kindList)
	fmt.Printf("\n  Read %d files (%s) → %d ledger rows, %d without an owner (mapping: %d rows).\n", len(files), strings.Join(kindList, ", "), len(events), unknown, m.Len())
	fmt.Printf("  Written: %s, %s\n", filepath.Join(*out, "report.txt"), filepath.Join(*out, "statement.csv"))
	return nil
}

func doFetch(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("fetch needs a provider: openai or anthropic")
	}
	provider := args[0]
	fs := flag.NewFlagSet("fetch", flag.ContinueOnError)
	since := fs.String("since", "", "first day to include, YYYY-MM-DD (UTC)")
	until := fs.String("until", "", "day to stop before, YYYY-MM-DD (UTC); default: today")
	out := fs.String("out", ".", "directory to save the pages into")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *since == "" {
		return fmt.Errorf("fetch needs --since YYYY-MM-DD")
	}
	var files []string
	var err error
	switch provider {
	case "openai":
		key := os.Getenv("OPENAI_ADMIN_KEY")
		if key == "" {
			return fmt.Errorf("OPENAI_ADMIN_KEY is not set: an Admin API key from platform.openai.com → Settings → Organization → Admin keys (Owner role), not a project key")
		}
		if w := fetch.KeyShapeWarning("openai", key); w != "" {
			fmt.Fprintln(os.Stderr, "warning: "+w)
		}
		files, err = fetch.OpenAI(key, *since, *until, *out)
	case "anthropic":
		key := os.Getenv("ANTHROPIC_ADMIN_KEY")
		if key == "" {
			return fmt.Errorf("ANTHROPIC_ADMIN_KEY is not set: an Admin API key (sk-ant-admin…) or a key created with Scope: Organization at platform.claude.com → Organization settings → API keys; a workspace key cannot read reports")
		}
		if w := fetch.KeyShapeWarning("anthropic", key); w != "" {
			fmt.Fprintln(os.Stderr, "warning: "+w)
		}
		if os.Getenv("ANTHROPIC_WORKSPACE_ID") != "" {
			fmt.Fprintln(os.Stderr, "note: ANTHROPIC_WORKSPACE_ID is ignored; the usage and cost reports are Admin API endpoints and reject a workspace header. Unset it if a request is refused for that reason.")
		}
		files, err = fetch.Anthropic(key, *since, *until, *out)
	default:
		return fmt.Errorf("unknown provider %q: openai or anthropic", provider)
	}
	if err != nil {
		return err
	}
	fmt.Printf("saved %d page(s) into %s:\n", len(files), *out)
	for _, f := range files {
		fmt.Println("  " + f)
	}
	fmt.Println("next: axigate-finops analyze " + filepath.Join(*out, "*.json") + " --owners owners.csv")
	return nil
}

// envOr returns the flag value, or the named environment variable when the flag
// was left empty — so a secret-ish URL can stay out of the command line.
func envOr(flagVal, env string) string {
	if flagVal != "" {
		return flagVal
	}
	return os.Getenv(env)
}

func doGateway(args []string) error {
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	provider := fs.String("provider", "", "openai, anthropic or gemini (usage extraction and pricing)")
	upstream := fs.String("upstream", "", "provider base URL; default: the provider's own")
	listen := fs.String("listen", "127.0.0.1:8787", "address to listen on")
	ledgerPath := fs.String("ledger", "gateway-events.jsonl", "file to append metadata-only events to")
	loopWindow := fs.Duration("loop-window", time.Minute, "window for loop detection")
	loopMaxReq := fs.Int("loop-max-requests", 0, "requests per run within the window that flag a loop (0 = off)")
	loopMaxRep := fs.Int("loop-max-repeats", 0, "identical requests per run within the window that flag a loop (0 = off)")
	maxCalls := fs.Int("max-calls-per-run", 0, "pause a run after this many calls (0 = off)")
	maxSpend := fs.Float64("max-spend-per-run", 0, "pause a run after this many USD of estimated spend (0 = off)")
	reservePerCall := fs.Float64("reserve-per-call", 0, "least USD a call still running is assumed to cost for the spend cap, so a burst that hits a brand-new run (no cost recorded yet) is held at the cap (0 = reserve at the run's average only)")
	keyDayCap := fs.Float64("max-spend-per-key-day", 0, "pause an API key after this many USD in a UTC day, whatever runs it starts — the leaked-key case (0 = off)")
	keyCap := fs.Float64("max-spend-per-key", 0, "pause an API key after this many USD in total — for as long as this gateway runs, or as long as the store keeps it with --shared-counter; needs --admin-token so the key can be resumed (0 = off)")
	pauseOnLoop := fs.Bool("pause-on-loop", false, "pause a run as soon as a loop is suspected (needs loop detection on)")
	shadow := fs.Bool("shadow", false, "serve every call, but mark the ones a cap would have refused and send the alert as \"would have stopped\" — watch a week before you enforce (the kill switch still refuses; give every replica sharing a store the same setting)")
	adminToken := fs.String("admin-token", "", "token for the bypass header and the /_axigate control endpoints (empty = no bypass, no admin)")
	requestCaps := fs.Bool("request-caps", true, "honor a caller's X-AxiGate-Max-Spend header as a per-run spend cap set from code")
	stopAlertURL := fs.String("stop-alert-url", "", "webhook to POST when a run is stopped — a Slack/Discord incoming-webhook URL works as-is (or set AXIGATE_STOP_ALERT_URL); metadata only, fail-open")
	sharedCounter := fs.String("shared-counter", "", "redis:// or rediss:// URL to keep run tallies in, so caps hold across gateway replicas (empty = per-process, in memory)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *sharedCounter != "" {
		if err := gateway.CheckSharedCounterURL(*sharedCounter); err != nil {
			return err
		}
	}
	base := *upstream
	switch *provider {
	case "openai":
		if base == "" {
			base = "https://api.openai.com"
		}
	case "anthropic":
		if base == "" {
			base = "https://api.anthropic.com"
		}
	case "gemini":
		if base == "" {
			base = "https://generativelanguage.googleapis.com"
		}
	default:
		return fmt.Errorf("gateway needs --provider openai, anthropic or gemini")
	}
	f, err := os.OpenFile(*ledgerPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	gw := gateway.New(gateway.Config{
		Upstream: base, Provider: *provider, Recorder: gateway.NewJSONLRecorder(f), StopAlertURL: envOr(*stopAlertURL, "AXIGATE_STOP_ALERT_URL"), SharedCounter: *sharedCounter,
		Loop:    gateway.LoopPolicy{Window: *loopWindow, MaxPerRun: *loopMaxReq, MaxRepeat: *loopMaxRep},
		Control: gateway.ControlPolicy{MaxCallsPerRun: *maxCalls, MaxSpendUSDPerRun: *maxSpend, ReserveUSDPerCall: *reservePerCall, MaxSpendUSDPerKeyDay: *keyDayCap, MaxSpendUSDPerKey: *keyCap, Shadow: *shadow, PauseOnSuspectedLoop: *pauseOnLoop, AdminToken: *adminToken, AllowRequestCaps: *requestCaps},
	})
	if err := gw.CounterError(); err != nil {
		return err
	}
	if *keyCap > 0 && *sharedCounter != "" && *adminToken == "" {
		return errors.New("--max-spend-per-key with --shared-counter needs --admin-token: a key at its total cap stays paused in the store across restarts, and only the admin endpoint can resume it")
	}

	srv := &http.Server{Addr: *listen, Handler: gw, ReadHeaderTimeout: 30 * time.Second, ReadTimeout: 10 * time.Minute, IdleTimeout: 120 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	loopMsg := "off"
	if *loopMaxReq > 0 || *loopMaxRep > 0 {
		loopMsg = fmt.Sprintf("on (window %s, max %d/run, max %d identical)", *loopWindow, *loopMaxReq, *loopMaxRep)
	}
	ctrlMsg := "off"
	if *maxCalls > 0 || *maxSpend > 0 || *pauseOnLoop || *keyDayCap > 0 || *keyCap > 0 {
		ctrlMsg = fmt.Sprintf("on (max %d calls/run, max $%.2f/run, max $%.2f/key/day, max $%.2f/key total, pause-on-loop=%v)", *maxCalls, *maxSpend, *keyDayCap, *keyCap, *pauseOnLoop)
		if *shadow {
			ctrlMsg = "shadow " + ctrlMsg[len("on "):]
		}
		if *adminToken == "" {
			if *shadow {
				ctrlMsg += " — no admin token: a marked run or key needs a restart to clear"
			} else {
				ctrlMsg += " — no admin token: a paused run or key needs a restart to clear"
			}
		}
	} else if *shadow && *requestCaps {
		ctrlMsg = "shadow (no server-wide cap; a caller's X-AxiGate-Max-Spend is shadowed)"
	} else if *shadow {
		ctrlMsg = "shadow (no cap in effect)"
	}
	if *shadow {
		ctrlMsg += "\n  shadow mode: every call is served; the ones a cap would refuse are marked on the row and alerted as \"would have stopped\"; the kill switch still refuses; every replica sharing a store should run with the same --shadow setting"
	}
	fmt.Printf("axigate-finops gateway: %s -> %s (%s)\n  events: %s\n  loop detection: %s\n  loop control: %s\n  counter: %s\n  point your app's base URL at http://%s and keep your normal key\n  Claude Code: set ANTHROPIC_BASE_URL to it — each session is a run, nothing to tag\n",
		*listen, base, *provider, *ledgerPath, loopMsg, ctrlMsg, gw.CounterLine(), *listen)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		fmt.Fprintln(os.Stderr, "\naxigate-finops gateway: shutting down")
		return srv.Shutdown(shutCtx)
	}
}

// doServe runs the gateway and the live dashboard together in one process, over
// one shared ledger. It is the zero-friction path: one command, two ports, the
// dashboard reflecting the gateway's traffic live. It is what the Docker image
// runs by default.
func doServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	provider := fs.String("provider", "openai", "openai, anthropic or gemini (usage extraction and pricing)")
	upstream := fs.String("upstream", "", "provider base URL; default: the provider's own")
	gwListen := fs.String("gateway-listen", "0.0.0.0:8080", "address for the gateway; point your app's base URL here")
	consoleListen := fs.String("console-listen", "0.0.0.0:8906", "address for the dashboard; open it in a browser")
	ledgerPath := fs.String("ledger", "gateway-events.jsonl", "file the gateway appends events to and the dashboard reads live")
	loopWindow := fs.Duration("loop-window", time.Minute, "window for loop detection")
	loopMaxReq := fs.Int("loop-max-requests", 0, "requests per run within the window that flag a loop (0 = off)")
	loopMaxRep := fs.Int("loop-max-repeats", 0, "identical requests per run within the window that flag a loop (0 = off)")
	maxCalls := fs.Int("max-calls-per-run", 0, "server-wide per-run call cap (0 = off)")
	maxSpend := fs.Float64("max-spend-per-run", 0, "server-wide per-run spend cap in USD (0 = off; callers can also set X-AxiGate-Max-Spend from code)")
	reservePerCall := fs.Float64("reserve-per-call", 0, "least USD a call still running is assumed to cost for the spend cap, so a burst that hits a brand-new run (no cost recorded yet) is held at the cap (0 = reserve at the run's average only)")
	keyDayCap := fs.Float64("max-spend-per-key-day", 0, "pause an API key after this many USD in a UTC day, whatever runs it starts — the leaked-key case (0 = off)")
	keyCap := fs.Float64("max-spend-per-key", 0, "pause an API key after this many USD in total — for as long as this gateway runs, or as long as the store keeps it with --shared-counter; needs --admin-token so the key can be resumed (0 = off)")
	pauseOnLoop := fs.Bool("pause-on-loop", false, "pause a run as soon as a loop is suspected (needs loop detection on)")
	shadow := fs.Bool("shadow", false, "serve every call, but mark the ones a cap would have refused and send the alert as \"would have stopped\" — watch a week before you enforce (the kill switch still refuses; give every replica sharing a store the same setting)")
	adminToken := fs.String("admin-token", "", "token for the bypass header and the /_axigate control endpoints")
	stopAlertURL := fs.String("stop-alert-url", "", "webhook to POST when a run is stopped — a Slack/Discord incoming-webhook URL works as-is (or set AXIGATE_STOP_ALERT_URL); metadata only, fail-open")
	sharedCounter := fs.String("shared-counter", "", "redis:// or rediss:// URL to keep run tallies in, so caps hold across gateway replicas (empty = per-process, in memory)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *sharedCounter != "" {
		if err := gateway.CheckSharedCounterURL(*sharedCounter); err != nil {
			return err
		}
	}
	base := *upstream
	switch *provider {
	case "openai":
		if base == "" {
			base = "https://api.openai.com"
		}
	case "anthropic":
		if base == "" {
			base = "https://api.anthropic.com"
		}
	case "gemini":
		if base == "" {
			base = "https://generativelanguage.googleapis.com"
		}
	default:
		return fmt.Errorf("serve needs --provider openai, anthropic or gemini")
	}
	f, err := os.OpenFile(*ledgerPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	gw := gateway.New(gateway.Config{
		Upstream: base, Provider: *provider, Recorder: gateway.NewJSONLRecorder(f), StopAlertURL: envOr(*stopAlertURL, "AXIGATE_STOP_ALERT_URL"), SharedCounter: *sharedCounter,
		Loop:    gateway.LoopPolicy{Window: *loopWindow, MaxPerRun: *loopMaxReq, MaxRepeat: *loopMaxRep},
		Control: gateway.ControlPolicy{MaxCallsPerRun: *maxCalls, MaxSpendUSDPerRun: *maxSpend, ReserveUSDPerCall: *reservePerCall, MaxSpendUSDPerKeyDay: *keyDayCap, MaxSpendUSDPerKey: *keyCap, Shadow: *shadow, PauseOnSuspectedLoop: *pauseOnLoop, AdminToken: *adminToken, AllowRequestCaps: true},
	})
	if err := gw.CounterError(); err != nil {
		return err
	}
	if *keyCap > 0 && *sharedCounter != "" && *adminToken == "" {
		return errors.New("--max-spend-per-key with --shared-counter needs --admin-token: a key at its total cap stays paused in the store across restarts, and only the admin endpoint can resume it")
	}
	con := console.New(nil)
	con.SetLedgerFile(*ledgerPath) // live: the dashboard re-reads the shared ledger per request

	gwSrv := &http.Server{Addr: *gwListen, Handler: gw, ReadHeaderTimeout: 30 * time.Second, ReadTimeout: 10 * time.Minute, IdleTimeout: 120 * time.Second}
	conSrv := &http.Server{Addr: *consoleListen, Handler: con, ReadHeaderTimeout: 30 * time.Second, ReadTimeout: 10 * time.Minute, IdleTimeout: 120 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 2)
	go func() { errc <- gwSrv.ListenAndServe() }()
	go func() { errc <- conSrv.ListenAndServe() }()
	fmt.Printf("axigate-finops serve (%s):\n"+
		"  gateway    http://%s   → point your app's base URL here, keep your normal key\n"+
		"  dashboard  http://%s   → open in a browser; spend appears live\n"+
		"  ledger     %s (metadata only)\n"+
		"  inline cap: set X-AxiGate-Run and X-AxiGate-Max-Spend in your code to cap a run, no restart\n"+
		"  Claude Code: set ANTHROPIC_BASE_URL to the gateway — each session is a run, nothing to tag\n"+
		"  counter:    %s\n",
		*provider, *gwListen, *consoleListen, *ledgerPath, gw.CounterLine())
	if *shadow {
		fmt.Println("  shadow mode: every call is served; the ones a cap would refuse are marked on the dashboard and alerted as \"would have stopped\"; the kill switch still refuses; every replica sharing a store should run with the same --shadow setting")
	}
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		fmt.Fprintln(os.Stderr, "\naxigate-finops serve: shutting down")
		_ = gwSrv.Shutdown(shutCtx)
		return conSrv.Shutdown(shutCtx)
	}
}

func doSeed(args []string) error {
	fs := flag.NewFlagSet("seed", flag.ContinueOnError)
	out := fs.String("out", "", "JSONL file to write the synthetic events to")
	n := fs.Int("events", 10000, "number of events to generate")
	seedN := fs.Int64("seed", 1, "RNG seed for a deterministic ledger")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return fmt.Errorf("seed needs --out FILE")
	}
	events := seed.Generate(seed.Options{Events: *n, Seed: *seedN})
	f, err := os.Create(*out)
	if err != nil {
		return err
	}
	defer f.Close()
	rec := gateway.NewJSONLRecorder(f)
	var total float64
	var blocked int
	for _, e := range events {
		rec.Record(e)
		total += e.CostUSD
		if e.Dimensions["blocked"] != "" {
			blocked++
		}
	}
	fmt.Printf("wrote %d synthetic events to %s\n  total priced spend: $%.2f · blocked (runaway loop): %d\n  next: axigate-finops analyze %s --focus focus.csv\n",
		len(events), *out, total, blocked, *out)
	return nil
}

func doConsole(args []string) error {
	fs := flag.NewFlagSet("console", flag.ContinueOnError)
	ledgerPath := fs.String("ledger", "", "gateway JSONL ledger to serve")
	listen := fs.String("listen", "127.0.0.1:8900", "address to listen on")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *ledgerPath == "" {
		return fmt.Errorf("console needs --ledger FILE (make one with: axigate-finops seed --out demo.jsonl)")
	}
	events, _, err := anyfile.Parse(*ledgerPath)
	if err != nil {
		return fmt.Errorf("%s: %w", *ledgerPath, err)
	}
	// Idempotence: the same row from an appended ledger is one row.
	seen := map[string]bool{}
	deduped := events[:0]
	for _, e := range events {
		if !seen[e.ID] {
			seen[e.ID] = true
			deduped = append(deduped, e)
		}
	}
	srv := console.New(deduped)
	srv.SetLedgerFile(*ledgerPath) // live: re-read on each request so a refresh shows new gateway events
	hs := &http.Server{Addr: *listen, Handler: srv, ReadHeaderTimeout: 30 * time.Second, ReadTimeout: 10 * time.Minute, IdleTimeout: 120 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- hs.ListenAndServe() }()
	fmt.Printf("axigate-finops console: http://%s  (%d events; live-reloads %s)\n", *listen, len(deduped), *ledgerPath)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return hs.Shutdown(shutCtx)
	}
}
