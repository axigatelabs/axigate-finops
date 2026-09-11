# axigate-finops — architecture

One Go module, one static binary, one implementation of the money math. Cost,
confidence states, dedup and the FOCUS/statement exports live in Go and every
surface reuses them, so the dashboard, the CLI and the exports can never
disagree.

## The binary and its subcommands

`cmd/axigate-finops` is the whole product:

- `fetch` — pull a provider's usage/cost reports to local JSON (admin key from
  the environment; nothing uploaded).
- `analyze` — read provider reports and/or the gateway ledger, map ids to
  owners, and write `report.txt`, `statement.csv` and (with `--focus`) a FOCUS
  export.
- `gateway` — the self-hosted pass-through proxy: forward unchanged, price a
  metadata-only event per request, detect and stop runaway loops, append the
  event to a JSONL ledger.
- `seed` — generate synthetic events for building/testing the console.
- `console` — serve the dashboard, the per-run drill-down (`/run/{id}`: the
  call-by-call timeline with the loop-detection and cap-trip moments and the
  spend the block prevented), and the JSON/statement/FOCUS API over a ledger,
  which it re-reads on each request so a refresh shows new events.
- `serve` — run the gateway and the dashboard together from the one binary, on
  two ports, over one shared ledger.

## Packages

| Package | Role |
|---|---|
| `internal/ledger` | the immutable event and its confidence states; `DeriveID` for idempotent imports |
| `internal/pricing` | the versioned provider price tables and the cost arithmetic |
| `internal/importers/*` | OpenAI/Anthropic JSON reports, the Anthropic Console cost CSV, the gateway JSONL, and `anyfile` detection |
| `internal/owners` | map provider ids to team/project/customer/agent |
| `internal/report` | aggregation, the unknown bucket, reconciliation; `Included` sets the total's membership |
| `internal/statement`, `internal/focus` | the flat statement CSV (every row, usage and bill, each with its confidence state) and the FOCUS 1.2 export (over `report.Included`, so BilledCost sums to the total) |
| `internal/gateway` | the proxy: forwarding, usage extraction (incl. streaming), loop detection and control, the JSONL recorder |
| `internal/console` | the HTTP dashboard, the per-run loop drill-down, the JSON/statement/FOCUS API |
| `internal/seed` | synthetic events for the console |

## Data flow

```
app → [gateway] → provider            gateway writes one metadata-only event per
        │                              request to a JSONL ledger (never prompts)
        ▼
   gateway-events.jsonl ──┐
   provider reports ──────┼─→ [analyze] → report.txt · statement.csv · FOCUS
   Console cost CSV ──────┘        │
                                   └─→ [console] → dashboard + API + FOCUS,
                                                    re-read live from the ledger
```

## Money and safety invariants

- Every figure carries a confidence state; nothing is signed below
  invoice-reconciled; unknown spend is shown as unknown, never guessed.
- The gateway is metadata-only, fails open, and never changes an answer.
- No real API keys or customer data in the repo or in tests; fixtures are
  synthetic. Secrets come from the environment and are never committed or logged.

## Status

The CLI and reconciliation, the self-hosted gateway, and the local dashboard
with the FOCUS export are built and tested.
