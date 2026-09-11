# axigate-finops

**Stop runaway AI agents from burning your API budget.** A self-hosted,
metadata-only FinOps gateway and local dashboard for OpenAI, Anthropic and
Gemini.

Point your agents at a drop-in proxy. It forwards every call unchanged, stops a
run that loops or blows a budget before it reaches the provider, attributes
spend to the team, agent and customer that caused it, and hands finance a
statement in [FOCUS](https://focus.finops.org/) format. One static Go binary,
no runtime dependencies, and about **0.15 ms of added latency at the median**
(measured; see below).

It runs on your ordinary inference key, inside your own network. No org-admin
key, no vendor lock-in. It records metadata only — token counts, cost and your
tags — never prompts or completions, so your keys and your data never leave your
machines.

---

## Why

Provider dashboards tell you last month's total. They can't tell you which
agent looped 400 times on Tuesday, or stop the next one. That needs to sit in
the request path. Cost tools (Vantage, CloudZero, Datadog) roll spend up after
the fact but can't enforce anything. axigate-finops is the piece in between: a
gateway that both *sees* every request and can *refuse* one.

## Try it in one command

The whole product — the gateway and a live dashboard — is one small static
binary, so one command runs all of it. Nothing to install, nothing to configure,
and your keys and prompts never leave your machine:

```bash
docker run -p 8080:8080 -p 8906:8906 shmeeee/axigate-finops:latest
```

Port `8080` is the gateway you route through; port `8906` is the dashboard. Then
two lines in your app — point the SDK's base URL at the gateway, and tag each
call. Keep using your normal API key:

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8080/v1")   # 1. route through the local gateway
resp = client.chat.completions.create(
    model="gpt-4o", messages=msgs,
    extra_headers={                        # 2. name the run and cap it, right here in code
        "X-AxiGate-Run": run_id,           #    the run this call and its cap belong to
        "X-AxiGate-Agent": "support-bot",  #    who spent it
        "X-AxiGate-Max-Spend": "5.00",     #    pause this run at $5 — no server restart
    })
```

Run your agent, then open **http://localhost:8906**: spend, tokens, and any
runaway loop the gateway stopped appear live as the calls land. If the run
crosses $5, the next call is refused with a `429` before it reaches OpenAI.
Nothing is written outside the container, so `Ctrl+C` leaves your laptop clean;
add `-v "$PWD/axigate-data:/data"` to keep the ledger between runs.

To route Anthropic or Gemini instead of OpenAI, append the command with your
provider — `docker run … shmeeee/axigate-finops:latest serve --provider anthropic`
or `--provider gemini` — and point that SDK at `http://localhost:8080`.

## Quickstart — route an agent through the gateway

Start the gateway (build the binary with `go build -o axigate-finops ./cmd/axigate-finops`, or use Docker below):

```bash
axigate-finops gateway --provider anthropic --listen 127.0.0.1:8787 \
  --max-calls-per-run 200 --loop-max-repeats 25 --ledger ./gateway-events.jsonl
```

Then two lines in your app: point the SDK's base URL at the gateway, and tag
each call with the run it belongs to. Keep using your normal API key.

**Using Claude Code?** There is nothing to tag. Set `ANTHROPIC_BASE_URL` to the
gateway and every Claude Code session is a run: the gateway reads the
`x-claude-code-session-id` header Claude Code already sends, and the agent and
parent-agent ids on subagent calls, so the caps, the loop detector and the
run drill-down work on a session with no code change. A W3C `traceparent`
header or LiteLLM's `x-litellm-trace-id` count as a run the same way. Your own
`X-AxiGate-Run` and `X-AxiGate-Agent` headers always win when you set them.

**Python (OpenAI):**

```python
client = OpenAI(base_url="http://localhost:8787/v1")                    # 1. route through the gateway
resp = client.chat.completions.create(
    model="gpt-4o", messages=msgs,
    extra_headers={"X-AxiGate-Run": run_id, "X-AxiGate-Agent": "planner"})  # 2. tag the run
```

**Python (Anthropic):**

```python
client = Anthropic(base_url="http://localhost:8787")                    # 1. route through the gateway
resp = client.messages.create(
    model="claude-haiku-4-5", max_tokens=1024, messages=msgs,
    extra_headers={"X-AxiGate-Run": run_id, "X-AxiGate-Agent": "planner"})  # 2. tag the run
```

**Node (OpenAI):**

```js
const client = new OpenAI({ baseURL: "http://localhost:8787/v1" });      // 1. route through the gateway
await client.chat.completions.create(
  { model: "gpt-4o", messages },
  { headers: { "X-AxiGate-Run": runId, "X-AxiGate-Agent": "planner" } }); // 2. tag the run
```

**Python (Gemini):** run the gateway with `--provider gemini`, then

```python
from google import genai
client = genai.Client(http_options={"base_url": "http://localhost:8787"})      # 1. route through the gateway
resp = client.models.generate_content(
    model="gemini-2.5-flash", contents=msgs,
    config={"http_options": {"headers": {"X-AxiGate-Run": run_id, "X-AxiGate-Agent": "planner"}}})  # 2. tag the run
```

That's it. Every call is now priced, attributed to `planner` and `run_id`, and
a run that exceeds `--max-calls-per-run` (or repeats an identical call past
`--loop-max-repeats`) is refused with a `429` before it costs you anything. You
can also cap a run from your code with `X-AxiGate-Max-Spend` (e.g. `"5.00"`) —
no server flag or restart — and the `X-AxiGate-Team`, `X-AxiGate-Project` and
`X-AxiGate-Customer` headers attribute further. If your agents fan out (many
workers firing at once under one run id), add `--reserve-per-call 0.05`: the
least each in-flight call is assumed to cost, so a burst that hits a brand-new
run is held at the cap instead of slipping past it before the first cost lands.

## The CLI — analyze what you already have, no keys required

Don't want to route traffic yet? The CLI reads an export you can download today.
On the Anthropic Console, open Cost, export the CSV, and:

```bash
axigate-finops analyze ~/Downloads/claude_api_cost_2026_08.csv --focus focus.csv
```

It prints spend by owner, by model and by day, flags cost spikes, shows the
prompt-cache picture, and writes `statement.csv` and a FOCUS export. It reads
OpenAI usage/cost JSON, Anthropic cost CSV, and the gateway's own ledger, in any
order, and reconciles them into one statement. Everything stays on your machine.

## Build it yourself

No Docker? It is one static Go binary that runs the same thing:

```bash
go build -o axigate-finops ./cmd/axigate-finops
./axigate-finops serve --provider openai      # gateway on :8080, dashboard on :8906
```

Or build the image from source with `docker build -t axigate-finops .` — the
image is the static binary plus CA roots, nothing else, about 20 MB. The gateway
and the dashboard can also run as separate commands (`axigate-finops gateway …`
and `axigate-finops console --ledger …`) when you want them on different hosts.

## The local dashboard

`axigate-finops console --ledger ./gateway-events.jsonl` serves a dashboard over
the gateway's ledger, running entirely on your machine: total spend, spend by team
and agent, the runaway loops it stopped with the spend each one prevented, a
per-run drill-down, and CSV / JSON / FOCUS exports. The ledger never leaves your
network.

The self-hosted dashboard is free and completely unrestricted — full history and
the FOCUS export, no tiers, no account, no upgrade.

## What it does, precisely

- **Loop control.** Per-run call and spend caps, and a repeat/rate detector,
  pause a run so its next call is refused. A global kill switch and a bypass
  credential are available through a token-guarded admin endpoint. Loops are
  keyed to the caller's explicit run id; a request with no run id is never
  flagged, because a loop is never inferred. `--shadow` serves every call and
  only marks what a cap would have refused, for watching before enforcing.
- **Attribution.** Every call carries the team/project/customer/agent/run tags
  you set, so cost lands on the owner that caused it. Spend on unmapped ids is
  shown as *unknown*, never guessed.
- **Provider-correct cost.** Prices come from a versioned table; each figure
  carries a confidence state (`estimated` → `provider-reported` →
  `invoice-reconciled` → `final`) and is never shown without it. Model snapshots
  (`gpt-4o-mini-2024-07-18`) resolve to their family's price; an unknown model
  is flagged, not silently mispriced.
- **FOCUS export.** A FOCUS 1.2 statement finance tools ingest without a custom
  mapping. Its BilledCost column sums to the report total to the cent.
- **Metadata only, fails open.** The gateway forwards prompts and completions
  but never records them. If anything about recording fails, the request still
  succeeds. It never changes a model or an answer.

## Why not just cap tool calls?

Most agent frameworks already have a `max_iterations` / `max_turns` knob, so it
is fair to ask what this adds. Our per-*call* cap is the same idea in a
different place; that is not the point. The difference is:

- **It runs where the app can't reach it.** A framework cap lives in code you
  must remember to set, correctly, in every agent. A new script, a misconfig, a
  third-party agent, or a raw API loop has no cap. The gateway sits at the
  network boundary and catches every call regardless. It is the guardrail for
  when the seatbelt is missing.
- **It stops on dollars, not counts.** Ten calls with a 200k-token context cost
  far more than a hundred tiny ones; a count cap can't tell the difference.
  `--max-spend-per-run` stops on the money.
- **It survives the agent's crash-restart loop.** When a framework crashes and
  restarts, its own in-code counter resets each time and keeps burning money.
  The gateway's cap is keyed to the run id (the `X-AxiGate-Run` you pass, or
  the session id Claude Code sends), so as long as
  the gateway stays up it keeps counting a run across those restarts — a
  per-process framework counter never sees them. (Without `--shared-counter`
  the gateway's *own* restart resets the tally — see the limits below.)
- **It's one policy across every framework, language and provider**, owned by
  the platform team, not scattered through app code.
- **It leaves a receipt.** The same mechanism that blocks produces the
  attribution, the ledger, the FOCUS statement, and the drill-down showing which
  call tripped. A framework cap prevents silently and shows finance nothing.

In one line: `max_iterations` is a local, count-based safety knob inside code
you control; this is spend-based, out-of-band governance that catches what the
app forgets, spans restarts, and leaves a receipt. Keep your framework cap, this
is the backstop, not a replacement. For a single well-configured agent in one
framework, the framework cap is simpler and free; the gateway earns its place at
team scale, across many agents and providers, where you want uniform control and
the finance byproduct.

## Get told the moment a run is stopped

Refusing the next call is the safety; knowing it happened is the point. Give
the gateway a webhook and it POSTs a small JSON message the instant a run is
stopped — metadata only, never a prompt:

```bash
axigate-finops serve --provider openai --stop-alert-url https://hooks.slack.com/services/…
```

A Slack incoming-webhook or Discord webhook URL works as-is (the body carries
the line under both `text` and `content`), so it lands in your channel:

> AxiGate stopped refund-reconciler (run refund-batch-10) — run reached the
> spend cap of $0.05. 7 calls, $0.05 spent so far; further calls are being
> refused.

Anything else gets the structured fields too (`event`, `run`, `agent`, `team`,
`model`, `reason`, `calls`, `spend_usd`, `at`; a key stop adds `key`). It fires
once per stop, not once per refused call, and it is fail-open: a slow or dead
webhook is logged and dropped, never allowed to touch a request.
`AXIGATE_STOP_ALERT_URL` works in place of the flag. In shadow mode (below)
the event is `run_flagged` or `key_flagged` and the body carries
`"shadow": true`, so a consumer can tell a mark from a stop.

## Cap a key, not just a run

Two of the biggest AI bills written up in 2026 came from stolen keys, not
loops — one of them around $600k. A run cap cannot help with that: the spend
is spread over many fresh runs. A key cap can, for every call that reaches
the gateway — a key an internal agent or CI job is burning across runs, or a
leaked key on a network where only the gateway can reach the provider:

```bash
axigate-finops gateway --provider openai \
  --max-spend-per-key-day 50 --max-spend-per-key 500
```

Every call carries the key that pays for it. The gateway keeps a fingerprint
of that key — never the key itself — and tallies spend per key across every
run it starts, counted against the key before the call leaves, the way a run
is. A key at its daily cap is refused (`429`, naming the key by its last four
characters) until midnight UTC; a key at its total cap is refused until an
operator resumes it, which needs `--admin-token`:

```bash
# the gateway was started with --admin-token <token>
curl -X POST "http://localhost:8787/_axigate/keys/resume?key=k-3fa9c2b1e0d4-7788" \
  -H "X-AxiGate-Admin: <token>"
```

The stop alert fires once per key stop, with the agent and team that were
using it. The dashboard's **Spend by key** card shows what each key spent and
how often it was refused; a refused count on a key you do not recognise is the
first sign of a leak. Label a key with an `X-AxiGate-Key: ci-runner` header
and the dashboard shows that name next to the fingerprint; the ceiling still
binds to the credential itself, so whoever holds a key cannot dodge it by
renaming. Resuming after a daily pause keeps the total; resuming after a
total pause grants a fresh one. The fingerprint is all the ledger ever sees;
the key itself goes to the provider and nowhere else. A real provider key
cannot be recovered from its fingerprint; a short or guessable token (the
`--api-key` you give a self-hosted model server) could be, by guessing,
and one shorter than eight characters gets no visible suffix at all. With
`--shared-counter` the key's tally is shared across replicas, so a leaked key
meets its ceiling once, everywhere. It cannot stop someone calling the
provider directly with your raw key — set the provider's own spending limit
for that.

## Watch before you enforce

Nobody turns a cap on for real traffic on day one. Start with `--shadow`:

```bash
axigate-finops gateway --provider openai --max-spend-per-run 5 --shadow
```

Every call is served. The ones a cap would have refused are marked on the
row (`would_refuse`, with the reason), the dashboard shows what a cap would
have stopped and what those calls actually cost, and the stop alert still
arrives — worded "would have stopped", so nobody mistakes it for a real stop.
The tallies, pauses and resumes work the same way they would in earnest; only
the refusal is withheld, so a marked run keeps spending and its tally keeps
climbing. The alert fires once, when the cap trips, with the figures at that
moment; what the run spent after that is only on the dashboard. The kill
switch still refuses. Every replica sharing a `--shared-counter` store should
run with the same `--shadow` setting: the mark is shared, the choice to refuse
is each gateway's own. When the week's picture looks right, start the gateway
without `--shadow`: with `--shared-counter` a run or key already past its cap
is refused from the restart on; without it the tallies start again from zero,
and a run or key is refused when it reaches its cap again.

## Caps across gateway replicas

One gateway keeps its tallies in memory. Two or more behind a load balancer
would each count on their own, so a run could spend the cap once per replica.
Point them at one Redis and they share a counter:

```bash
axigate-finops gateway --provider openai --max-spend-per-run 5 \
  --shared-counter redis://cache.internal:6379
```

Every replica then reserves against the same tally in a single step that no
other replica can slip between, so a burst spread across replicas trips the
cap at the boundary exactly as one process does; a pause, a resume and the
kill switch apply everywhere. The kill switch lives in the store, so it stays
engaged across gateway restarts, for as long as the store keeps its data,
until it is cleared. Nothing but run ids, counts, dollars, the attribution
tags, why a run was paused, the kill switch and an id per call (kept with the
run, so a retried write counts once) is stored. A run is remembered for two
days of silence, its pause included. Without a key cap the store never sees a
key. With one, each key's fingerprint, its daily and total tallies and any
pause are kept for four hundred days, so a total cap means what it says — and
a key at its total cap stays paused across restarts, which is why that flag
needs `--admin-token` alongside the store; the per-call ids of keyed calls
that carry no run id are kept until three days after the start of the UTC day
they were made on. One Redis, not Redis Cluster.
`rediss://` turns on TLS; a password goes in the URL, which anyone who can
list processes on that host can read.

If the store stops answering, or answers with an error, the gateway does not
stop serving and does not stop capping: it decides per process until the
store answers again, and says so. Rows decided that way carry `counter=local`,
the status endpoint reports since when and how many decisions were made that
way, and when the store returns the gap is written back to it: runs paused
during it, the kill switch if an operator changed it, resumes an operator
asked for, inline caps callers sent, and the cost of calls the store had
already let through (the status field `owed_to_store` counts what is still
owed). Spend tallied for calls this gateway let through on its own is not
merged into the shared tally; that bound is approximate, by design, rather
than a guess. A kill switch engaged before the gap is honoured during it, and
so are the runs this gateway last saw paused in the store; a kill or a pause
made on another replica during the gap is seen when the store returns.

## Honest about the limits

- Without `--shared-counter`, per-run caps are enforced in memory: the
  gateway's own restart resets a run's tally, and the cap is per-process
  (behind a load balancer each replica counts on its own). With or without
  it, each call is reserved at admit — at the run's average cost so
  far, or at `--reserve-per-call` when that is higher — so a concurrent burst
  trips the cap at the boundary. Without that floor, the calls already in flight
  before a run's first cost is recorded can slip past the cap, because there is
  no average to reserve against yet. For a sequential agent it is exact. Run
  more than one gateway? Give them one counter with `--shared-counter` (above)
  and the cap holds across all of them.
- A call the provider served but never reported usage for (a stream that ended
  early) settles at no cost so the run keeps moving, and is counted on the
  dashboard as a call with unknown cost — not as a free one. The provider's
  bill is what fills it in.
- In shadow mode the alert fires once per run or key, when its cap trips, and
  the run keeps spending after it; the dashboard is where the rest shows. The
  admin status endpoint lists marked runs and keys under `paused_runs` and
  `paused_keys` with a note saying they are marked, not refused.
- A key cap governs the calls that reach the gateway, not a key used directly
  against the provider. Without `--shared-counter` the total is per process
  and resets with a restart; with it, a key at its total cap stays paused until
  an operator resumes it through the admin endpoint.
- A provider export shows spend, cache use and spikes, but not loops. Loops need
  the request-level data the gateway captures.
- Nothing is signed below the `invoice-reconciled` state.

## Measured overhead

An offline load harness (`go test -tags loadtest -run TestLoad ./internal/gateway/`)
against a 5 ms upstream at 50 requests per second:

| percentile | added by the gateway |
|---|---|
| p50 | ~0.15 ms |
| p90 | ~0.23 ms |
| p99 | ~0.40 ms |

Negligible beside real provider latency of hundreds of milliseconds.

## Design

One Go module, one static binary, one implementation of the cost math reused by
the CLI, the gateway and the console, so no two surfaces can disagree. See
[`ARCHITECTURE.md`](ARCHITECTURE.md).

## License

Apache License 2.0 — see [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE). The
self-hosted gateway and local dashboard are free and open source; use them
however you like.
