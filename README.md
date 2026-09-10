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
`X-AxiGate-Customer` headers attribute further.

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
  flagged, because a loop is never inferred.
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
  The gateway's cap is keyed to the `X-AxiGate-Run` id you pass, so as long as
  the gateway stays up it keeps counting a run across those restarts — a
  per-process framework counter never sees them. (The gateway's *own* restart
  resets the tally; it lives in memory — see the limits below.)
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
`model`, `reason`, `calls`, `spend_usd`, `at`). It fires once per stop, not once
per refused call, and it is fail-open: a slow or dead webhook is logged and
dropped, never allowed to touch a request. `AXIGATE_STOP_ALERT_URL` works in
place of the flag.

## Honest about the limits

- Per-run caps are enforced in memory: the gateway's own restart resets a run's
  tally, and the cap is per-process (behind a load balancer each replica counts
  on its own). Each call is reserved at admit, so a concurrent burst trips the
  cap at the boundary; the residual overshoot is the calls already in flight
  before the run's first cost is recorded. For a sequential agent it is exact. A
  durable, exact bound across replicas is the hosted tier's job, on the roadmap.
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
