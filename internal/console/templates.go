package console

// A dark, considered analytics console. Inter for the interface, JetBrains Mono
// only for figures and codes (money is truth). Green is reserved for money and
// nothing else; red marks blocked spend. Everything is one self-contained
// document, no external assets required, so the console renders identically
// offline.

const baseCSS = `
:root{
  --bg:#0a0b0e; --surface:#131720; --surface-2:#1a1f2b; --raise:#1e2431;
  --line:#242b39; --line-2:#2f3749;
  --ink:#eef1f6; --ink-2:#9aa4b6; --ink-3:#616b7d;
  --money:#3ddc84; --money-dim:#1c7a49;
  --pro:#8b93ff; --danger:#ff6b6b; --partial:#f5a623;
  --sans:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  --mono:'JetBrains Mono',ui-monospace,SFMono-Regular,Menlo,monospace;
  --sh:0 1px 0 rgba(255,255,255,.02) inset,0 1px 2px rgba(0,0,0,.4),0 10px 30px -16px rgba(0,0,0,.7);
}
*{box-sizing:border-box} html,body{margin:0}
body{background-color:var(--bg);
  background-image:radial-gradient(1100px 560px at 78% -12%,rgba(61,220,132,.06),transparent 60%);
  color:var(--ink);font-family:var(--sans);line-height:1.5;-webkit-font-smoothing:antialiased;letter-spacing:-.005em}
a{color:inherit;text-decoration:none}
.wrap{max-width:1120px;margin:0 auto;padding:30px 28px 72px}
.eyebrow{font-family:var(--mono);font-size:10.5px;letter-spacing:.14em;text-transform:uppercase;color:var(--ink-3)}
.num{font-family:var(--mono);font-variant-numeric:tabular-nums}

.top{display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:24px}
.brand{display:flex;align-items:center;gap:11px}
.logo{width:30px;height:30px;border-radius:8px;background:linear-gradient(140deg,var(--money),var(--money-dim));
  display:grid;place-items:center;box-shadow:0 4px 14px -4px rgba(61,220,132,.5)}
.logo svg{width:17px;height:17px;display:block}
.brand .name{font-weight:700;font-size:16px;letter-spacing:-.01em}
.brand .name b{color:var(--money);font-weight:700}
.brand .sub{color:var(--ink-3);font-size:12px;margin-top:1px}
.top-right{display:flex;align-items:center;gap:10px}
.pill{font-family:var(--mono);font-size:11px;padding:6px 11px;border-radius:999px;border:1px solid var(--line-2);color:var(--ink-2);display:inline-flex;gap:6px;align-items:center}
.pill.pro{color:var(--pro);border-color:color-mix(in srgb,var(--pro) 45%,transparent);background:color-mix(in srgb,var(--pro) 12%,transparent)}
.pill.up{color:var(--pro);border-color:color-mix(in srgb,var(--pro) 45%,transparent);cursor:pointer}
.pill .dot{width:6px;height:6px;border-radius:50%;background:currentColor}

.seg{display:inline-flex;background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:3px}
.seg a{font-size:12.5px;padding:6px 13px;border-radius:7px;color:var(--ink-2);font-weight:500;display:inline-flex;gap:5px;align-items:center}
.seg a.on{background:var(--raise);color:var(--ink);box-shadow:0 1px 2px rgba(0,0,0,.3)}
.seg a.lock{color:var(--ink-3)} .seg a .lk{color:var(--pro);font-size:9.5px;letter-spacing:.06em}

.hero{display:grid;grid-template-columns:minmax(230px,300px) 1fr;gap:1px;background:var(--line);
  border:1px solid var(--line);border-radius:18px;overflow:hidden;box-shadow:var(--sh);margin-bottom:16px}
.hero>div{background:var(--surface)}
.hero .total{padding:24px 26px}
.hero .total .eyebrow{margin-bottom:12px}
.hero .total .big{font-family:var(--mono);font-weight:700;font-size:42px;line-height:1;color:var(--money);letter-spacing:-.02em;word-break:break-all}
.hero .total .meta{color:var(--ink-3);font-size:12.5px;margin-top:16px;line-height:1.8}
.hero .total .meta b{color:var(--ink-2);font-weight:600}
.hero .plot{padding:20px 24px 14px;display:flex;flex-direction:column}
.hero .plot .head{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:10px}
.hero .plot .head .peak{font-size:12px;color:var(--ink-3)} .hero .plot .head .peak b{color:var(--money);font-family:var(--mono)}
.chart{width:100%;height:150px;display:block}
.chart .g0{stop-color:var(--money);stop-opacity:.28}
.chart .g1{stop-color:var(--money);stop-opacity:0}
.chart-area{fill:url(#axg-area);stroke:none}
.chart-line{fill:none;stroke:var(--money);stroke-width:2.5;vector-effect:non-scaling-stroke;stroke-linejoin:round;stroke-linecap:round}
.chart-base{stroke:var(--line-2);stroke-width:1;vector-effect:non-scaling-stroke}
.chart-dot{fill:var(--money);stroke:var(--bg);stroke-width:2.5}
.chart-empty{color:var(--ink-3);font-size:13px;display:grid;place-items:center;height:150px}

.chips{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:16px}
.chip{background:var(--surface);border:1px solid var(--line);border-radius:14px;padding:16px 18px;box-shadow:var(--sh);display:flex;align-items:center;gap:14px}
.chip .ic{width:38px;height:38px;border-radius:10px;display:grid;place-items:center;flex:0 0 auto}
.chip .ic svg{width:19px;height:19px}
.chip.danger .ic{background:color-mix(in srgb,var(--danger) 15%,transparent);color:var(--danger)}
.chip.n .ic{background:var(--surface-2);color:var(--ink-2)}
.chip .v{font-family:var(--mono);font-size:24px;font-weight:700;line-height:1}
.chip.danger .v{color:var(--danger)}
.chip .l{font-size:12px;color:var(--ink-3);margin-top:4px}

.grid2{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
.card{background:var(--surface);border:1px solid var(--line);border-radius:16px;padding:20px 22px;box-shadow:var(--sh)}
.card>h2{display:flex;align-items:center;gap:8px;font-size:12.5px;font-weight:600;color:var(--ink-2);margin:0 0 16px;letter-spacing:.01em}
.card>h2 .ct{margin-left:auto;font-family:var(--mono);font-size:11px;color:var(--ink-3);font-weight:400}
.bar{display:grid;grid-template-columns:1fr 100px;gap:14px;align-items:center;margin:14px 0}
.bar:first-of-type{margin-top:0}
.bar .lbl{min-width:0}
.bar .lbl .nm{font-size:13.5px;color:var(--ink);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-weight:500}
.bar .track{margin-top:8px;height:7px;background:var(--surface-2);border-radius:99px;overflow:hidden}
.bar .fill{display:block;height:100%;min-width:3px;border-radius:99px;background:linear-gradient(90deg,var(--money-dim),var(--money))}
.bar .amt{font-family:var(--mono);font-size:13.5px;text-align:right;color:var(--ink);font-variant-numeric:tabular-nums}
.empty{color:var(--ink-3);font-size:13px;padding:6px 0;line-height:1.6}

.loops>h2 .saved{margin-left:auto;font-family:var(--mono);font-size:11.5px;font-weight:500;color:var(--money);
  background:color-mix(in srgb,var(--money) 12%,transparent);padding:5px 11px;border-radius:999px}
.loop-row{display:flex;align-items:center;gap:13px;padding:14px 0;border-top:1px solid var(--line)}
.loop-row:first-of-type{border-top:0;padding-top:2px}
.loop-row .who{min-width:0}
.loop-row .rid{font-family:var(--mono);font-size:13.5px;color:var(--ink);font-weight:500}
.loop-row .meta{font-size:12px;color:var(--ink-3);margin-top:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.loop-row .right{margin-left:auto;text-align:right;flex:0 0 auto}
.loop-row .warn{font-family:var(--mono);font-size:12px;color:var(--danger);background:color-mix(in srgb,var(--danger) 13%,transparent);padding:5px 11px;border-radius:999px;white-space:nowrap;display:inline-block}
.loop-row .avoided{font-family:var(--mono);font-size:11.5px;color:var(--money);margin-top:6px}
.loop-row .spent{font-family:var(--mono);font-size:11.5px;color:var(--ink-2);margin-top:6px}
.loops>h2 .lost{margin-left:auto;font-family:var(--mono);font-size:11.5px;font-weight:500;color:var(--danger);
  background:color-mix(in srgb,var(--danger) 12%,transparent);padding:5px 11px;border-radius:999px}
.loop-row .rr{width:8px;height:8px;border-radius:50%;background:var(--danger);flex:0 0 auto;box-shadow:0 0 0 4px color-mix(in srgb,var(--danger) 16%,transparent)}
.loop-row .chev{color:var(--ink-3);font-size:18px;margin-left:2px;transition:color .15s,transform .15s}
.loop-row:hover{border-radius:10px;background:color-mix(in srgb,var(--danger) 6%,transparent)}
.loop-row.still:hover{border-radius:0;background:none}
.loop-row:hover .chev{color:var(--danger);transform:translateX(2px)}

/* per-run rollup: same layout as a loop row, neutral (spend, not danger) */
.run-row{display:flex;align-items:center;gap:13px;padding:12px 0;border-top:1px solid var(--line)}
.run-row:first-of-type{border-top:0;padding-top:2px}
.run-row .who{min-width:0}
.run-row .rid{font-family:var(--mono);font-size:13.5px;color:var(--ink);font-weight:500}
.run-row .rid a{color:inherit} .run-row .rid a:hover{color:var(--money)}
.run-row .meta{font-size:12px;color:var(--ink-3);margin-top:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.run-row .right{margin-left:auto;text-align:right;flex:0 0 auto}
.run-row .amt{font-family:var(--mono);font-size:13.5px;color:var(--ink);font-variant-numeric:tabular-nums}
.run-row .avoided{font-family:var(--mono);font-size:11.5px;color:var(--money);margin-top:4px}

/* run detail */
.back{display:inline-flex;align-items:center;gap:7px;font-size:12.5px;color:var(--ink-2);font-family:var(--mono);margin-bottom:18px}
.back:hover{color:var(--ink)}
.rhead{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:22px}
.rhead .rid{font-family:var(--mono);font-size:22px;font-weight:600;letter-spacing:-.01em;word-break:break-all}
.rhead .meta{color:var(--ink-3);font-size:13px;margin-top:6px}
.rhead .meta .num{color:var(--ink-2)}
.rstat{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:24px}
.rstat .s{background:var(--surface);border:1px solid var(--line);border-radius:14px;padding:16px 18px;box-shadow:var(--sh)}
.rstat .s .l{font-family:var(--mono);font-size:10.5px;letter-spacing:.06em;text-transform:uppercase;color:var(--ink-3)}
.rstat .s .v{font-family:var(--mono);font-size:24px;font-weight:700;margin-top:6px}
.rstat .s.saved .v{color:var(--money)} .rstat .s.saved{border-color:color-mix(in srgb,var(--money) 35%,transparent)}
.rstat .s .sub{font-size:11.5px;color:var(--ink-3);margin-top:4px}
.tl{background:var(--surface);border:1px solid var(--line);border-radius:16px;padding:8px 22px 14px;box-shadow:var(--sh)}
.tl h2{margin:16px 0 4px}
.call{display:grid;grid-template-columns:26px 62px 1fr auto;gap:12px;align-items:center;padding:11px 0;border-bottom:1px solid var(--line);position:relative}
.call:last-child{border-bottom:0}
.call .node{width:11px;height:11px;border-radius:50%;justify-self:center;background:var(--money);box-shadow:0 0 0 3px color-mix(in srgb,var(--money) 15%,transparent)}
.call.blk .node{background:var(--danger);box-shadow:0 0 0 3px color-mix(in srgb,var(--danger) 15%,transparent)}
.call.flag .node{background:var(--partial);box-shadow:0 0 0 3px color-mix(in srgb,var(--partial) 18%,transparent)}
.call .t{font-family:var(--mono);font-size:12px;color:var(--ink-3)}
.call .mid .m{font-size:13.5px;font-family:var(--mono);color:var(--ink)}
.call .mid .tok{font-size:11.5px;color:var(--ink-3);margin-top:2px}
.call .mid .flagtag{color:var(--partial);font-family:var(--mono);font-size:10.5px;margin-left:8px}
.call .amt{font-family:var(--mono);font-size:13px;text-align:right;color:var(--ink)}
.call .amt.blk{color:var(--danger);background:color-mix(in srgb,var(--danger) 13%,transparent);padding:4px 10px;border-radius:999px;font-size:11.5px}
.tripped{display:flex;align-items:center;gap:10px;margin:6px 0;padding:10px 12px;border-radius:10px;
  background:color-mix(in srgb,var(--danger) 10%,transparent);border:1px solid color-mix(in srgb,var(--danger) 30%,transparent)}
.tripped .lab{font-family:var(--mono);font-size:11.5px;color:var(--danger);font-weight:600}
.tripped .txt{font-size:12.5px;color:var(--ink-2)}

.exports{display:flex;gap:10px;flex-wrap:wrap}
.btn{display:inline-flex;align-items:center;gap:8px;font-size:13px;font-weight:500;padding:10px 15px;border-radius:10px;border:1px solid var(--line-2);background:var(--surface-2);color:var(--ink);transition:border-color .15s,background .15s}
.btn:hover{border-color:#3a4256;background:var(--raise)}
.btn svg{width:15px;height:15px;color:var(--ink-3)}
.btn.locked{color:var(--ink-3)} .btn.locked .lk{color:var(--pro);font-family:var(--mono);font-size:10.5px;letter-spacing:.06em}
.foot{color:var(--ink-3);font-size:12px;margin-top:26px;line-height:1.7;max-width:72ch}
.foot .mono{font-family:var(--mono);color:var(--ink-2)}
@media(max-width:860px){.hero{grid-template-columns:1fr}.chips{grid-template-columns:1fr}.grid2{grid-template-columns:1fr}}
`

const iconLogo = `<svg viewBox="0 0 24 24" fill="none"><path d="M4 15l5-9 4 6 3-4 4 7" stroke="#07120c" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"/></svg>`
const iconStop = `<svg viewBox="0 0 24 24" fill="none"><circle cx="12" cy="12" r="8.5" stroke="currentColor" stroke-width="2"/><path d="M8.5 8.5l7 7" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>`
const iconTeam = `<svg viewBox="0 0 24 24" fill="none"><circle cx="9" cy="9" r="3" stroke="currentColor" stroke-width="2"/><path d="M3.5 19a5.5 5.5 0 0 1 11 0M16 7a3 3 0 0 1 0 6M20.5 19a5.5 5.5 0 0 0-3-5" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>`
const iconBot = `<svg viewBox="0 0 24 24" fill="none"><rect x="4" y="8" width="16" height="11" rx="3" stroke="currentColor" stroke-width="2"/><path d="M12 8V4M9 13h.01M15 13h.01" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>`
const iconDl = `<svg viewBox="0 0 24 24" fill="none"><path d="M12 4v10m0 0l-3.5-3.5M12 14l3.5-3.5M5 19h14" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`

const dashHTML = `<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>AxiGate FinOps</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap">
<style>` + baseCSS + `</style></head><body><div class="wrap">

<div class="top">
  <div class="brand">
    <span class="logo">` + iconLogo + `</span>
    <div><div class="name">Axi<b>Gate</b> FinOps</div>
      <div class="sub num">{{if .Sum.Events}}{{.Sum.RangeStart}} → {{.Sum.RangeEnd}} · {{.Sum.Events}} events{{else}}no events yet — route an agent through the gateway{{end}}</div></div>
  </div>
  <div class="top-right">
    <div class="seg">{{range .Ranges}}<a class="{{if .Active}}on{{end}}" href="/?days={{.Days}}">{{.Label}}</a>{{end}}</div>
  </div>
</div>

<div class="hero">
  <div class="total">
    <div class="eyebrow">Total spend</div>
    <div class="big">{{usd .Sum.TotalUSD}}</div>
    <div class="meta"><b>{{len .Sum.ByProvider}}</b> provider(s) · <b>{{len .Sum.ByModel}}</b> model(s)<br>{{if eq .Sum.TotalState "estimated"}}every figure is the <span class="num">estimated</span> state{{else}}total is {{if eq .Sum.TotalState "mixed"}}part <span class="num">provider-reported</span>, part <span class="num">estimated</span>{{else}}the <span class="num">provider-reported</span> state{{end}}{{if .Sum.MeteredCalls}} · the team and agent figures are the gateway's <span class="num">estimated</span> state{{end}}{{end}}</div>
  </div>
  <div class="plot">
    <div class="head"><div class="eyebrow">Spend over time</div><div class="peak">peak day <b>{{usd .Sum.PeakDayUSD}}</b></div></div>
    {{chart .Sum.Daily .Sum.PeakDayUSD}}
  </div>
</div>

<div class="chips">
  <div class="chip danger"><span class="ic">` + iconStop + `</span><div><div class="v">{{.Sum.LoopsBlocked}}</div><div class="l">Runaway calls blocked</div></div></div>
  {{if .Sum.ShadowCalls}}<div class="chip danger"><span class="ic">` + iconStop + `</span><div><div class="v">{{.Sum.ShadowCalls}}</div><div class="l">Would have been refused (shadow mode)</div></div></div>{{end}}
  <div class="chip n"><span class="ic">` + iconTeam + `</span><div><div class="v">{{.Sum.Teams}}</div><div class="l">Teams</div></div></div>
  <div class="chip n"><span class="ic">` + iconBot + `</span><div><div class="v">{{.Sum.Agents}}</div><div class="l">Agents</div></div></div>
</div>

<div class="grid2">
  <div class="card"><h2>Spend by team <span class="ct">{{len .Sum.ByTeam}}</span></h2>
    {{range .Sum.ByTeam}}<div class="bar"><div class="lbl"><div class="nm">{{.Key}}</div><div class="track"><span class="fill" style="{{barWidth .USD $.MaxTeam}}"></span></div></div><div class="amt">{{usd .USD}}</div></div>{{else}}<div class="empty">No spend in this range.</div>{{end}}
  </div>
  <div class="card"><h2>Spend by agent <span class="ct">{{len .Sum.ByAgent}}</span></h2>
    {{range .Sum.ByAgent}}<div class="bar"><div class="lbl"><div class="nm">{{.Key}}</div><div class="track"><span class="fill" style="{{barWidth .USD $.MaxAgent}}"></span></div></div><div class="amt">{{usd .USD}}</div></div>{{else}}<div class="empty">No spend in this range.</div>{{end}}
  </div>
</div>
{{if gt .Sum.UnmeteredUSD 0.0}}<p class="foot" style="margin:0 0 16px"><span class="num">(billed, not metered)</span> is {{usd .Sum.UnmeteredUSD}} on the provider's bill that nothing metered accounts for — traffic that went around the gateway, or a price in the price list that is wrong. It stays its own line; it is never spread across teams or agents.</p>{{end}}
{{if gt .Sum.UnknownCostCalls 0}}<p class="foot" style="margin:0 0 16px"><span class="num">{{.Sum.UnknownCostCalls}}</span> served call{{if ne .Sum.UnknownCostCalls 1}}s{{end}} came back without usage (a stream that ended before its usage frame), so {{if eq .Sum.UnknownCostCalls 1}}its{{else}}their{{end}} cost is unknown and counted as nothing here — not as free. The total is that much low until the provider's bill fills it in.</p>{{end}}

<div class="card runs" style="margin-bottom:16px"><h2>Spend by run <span class="ct">top {{len .Sum.ByRun}} by spend</span></h2>
  {{range .Sum.ByRun}}<div class="run-row">
      <div class="who"><div class="rid">{{if ne .Run "(untagged)"}}<a href="/run/{{urlquery .Run}}">{{.Run}}</a>{{else}}{{.Run}}{{end}}</div>
        <div class="meta">{{if .Agent}}{{.Agent}}{{else}}unknown agent{{end}}{{if .Model}} · <span class="num">{{.Model}}</span>{{end}} · <span class="num">{{.Calls}}</span> calls{{if .Failed}} · <span class="num">{{.Failed}}</span> failed{{end}}{{if .Blocked}} · <span class="num">{{.Blocked}}</span> refused{{end}}</div></div>
      <div class="right"><div class="amt">{{usd .SpentUSD}}</div>{{if .Blocked}}<div class="avoided">≈{{usd .AvoidedUSD}} prevented</div>{{end}}</div>
    </div>{{else}}<div class="empty">No runs in this range. Tag calls with <span class="num">X-AxiGate-Run</span> and each run rolls up here — spend, calls, failures and refusals in one line.</div>{{end}}
</div>

{{if .Sum.ByKey}}<div class="card runs" style="margin-bottom:16px"><h2>Spend by key <span class="ct">top {{len .Sum.ByKey}} by spend</span></h2>
  {{range .Sum.ByKey}}<div class="run-row">
      <div class="who"><div class="rid"><span class="num">key {{.Label}}</span></div>
        <div class="meta"><span class="num">{{.Calls}}</span> calls{{if .Failed}} · <span class="num">{{.Failed}}</span> failed{{end}}{{if .Blocked}} · <span class="num">{{.Blocked}}</span> refused{{end}}</div></div>
      <div class="right"><div class="amt">{{usd .SpentUSD}}</div></div>
    </div>{{end}}
  <p class="foot" style="margin:8px 0 0">The gateway keeps a fingerprint of each API key, never the key. A key you do not recognise spending here is the first sign of a leak; <span class="num">--max-spend-per-key-day</span> and <span class="num">--max-spend-per-key</span> cap what any one key may spend.</p>
</div>
{{end}}
{{if .Sum.ShadowLines}}<div class="card loops" style="margin-bottom:16px"><h2>Shadow mode: what a cap would have stopped <span class="lost">{{usd .Sum.ShadowSpendUSD}} spent past the caps</span></h2>
  {{range .Sum.ShadowLines}}{{if eq .Kind "run"}}<a class="loop-row" href="/run/{{urlquery .Who}}">{{else}}<div class="loop-row still">{{end}}
      <span class="rr"></span>
      <div class="who"><div class="rid">{{if eq .Kind "run"}}{{.Who}}{{else}}<span class="num">{{.Who}}</span>{{end}}</div><div class="meta">{{if .Agent}}{{.Agent}}{{else}}unknown agent{{end}}{{if .Model}} · <span class="num">{{.Model}}</span>{{end}} · {{.Reason}}</div></div>
      <div class="right"><div class="warn">{{.Calls}} call{{if ne .Calls 1}}s{{end}} past the cap</div><div class="spent">{{usd .SpentUSD}} spent</div></div>
      {{if eq .Kind "run"}}<span class="chev">›</span></a>{{else}}</div>{{end}}{{end}}
  <p class="foot" style="margin-top:14px">In shadow mode every call is served; the ones a cap would have refused are marked, and the alert says "would have stopped". These dollars were spent. Start the gateway without <span class="num">--shadow</span> to enforce: with <span class="num">--shared-counter</span> a run or key already past its cap is refused from the restart on; without it the tallies start again from zero, and a run or key is refused when it reaches its cap again.</p>
</div>
{{end}}<div class="card loops" style="margin-bottom:16px"><h2>Runaway loops the gateway stopped
    {{if .Sum.BlockedRuns}}<span class="saved">≈{{usd .Sum.AvoidedUSD}} of runaway spend prevented</span>{{end}}</h2>
  {{if .Sum.BlockedRuns}}{{range .Sum.BlockedRuns}}<a class="loop-row" href="/run/{{urlquery .Run}}">
      <span class="rr"></span>
      <div class="who"><div class="rid">{{.Run}}</div><div class="meta">{{if .Agent}}{{.Agent}}{{else}}unknown agent{{end}}{{if .Model}} · <span class="num">{{.Model}}</span>{{end}}</div></div>
      <div class="right"><div class="warn">{{.Blocked}} calls blocked</div><div class="avoided">≈{{usd .AvoidedUSD}} saved</div></div>
      <span class="chev">›</span>
    </a>{{end}}
  {{else}}<div class="empty">Nothing blocked in this range. When an agent loops or blows its budget, the run appears here the moment the gateway refuses a call, with the spend it prevented.</div>{{end}}
</div>

<div class="card"><h2>Export</h2>
  <div class="exports">
    <a class="btn" href="/api/export/statement{{if .Sum.Days}}?days={{.Sum.Days}}{{end}}">` + iconDl + `Statement CSV</a>
    <a class="btn" href="/api/export/json{{if .Sum.Days}}?days={{.Sum.Days}}{{end}}">` + iconDl + `JSON</a>
    <a class="btn" href="/api/export/focus{{if .Sum.Days}}?days={{.Sum.Days}}{{end}}">` + iconDl + `FOCUS CSV</a>
  </div>
</div>

<p class="foot">Totals and the FOCUS export come from one Go implementation, so this dashboard can never disagree with that download; the statement lists every row, usage and bill, each with its confidence state. Spend on ids with no owner is shown as <span class="mono">unknown</span>, never guessed.</p>
</div></body></html>`

const runHTML = `<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>AxiGate FinOps · Run</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap">
<style>` + baseCSS + `</style></head><body><div class="wrap">
<a class="back" href="/">‹ back to dashboard</a>
{{if not .Found}}
  <div class="rhead"><div><div class="rid">Run not found</div><div class="meta">No events for this run in the ledger.</div></div></div>
{{else}}
<div class="rhead">
  <div><div class="rid">{{.Run}}</div>
    <div class="meta">{{if .Agent}}{{.Agent}}{{else}}unknown agent{{end}}{{if .Team}} · {{.Team}}{{end}} · {{.Provider}} · <span class="num">{{.Model}}</span></div></div>
  {{if .Paused}}<span class="pill" style="color:var(--danger);border-color:color-mix(in srgb,var(--danger) 45%,transparent);background:color-mix(in srgb,var(--danger) 12%,transparent)"><span class="dot"></span>paused by the gateway</span>{{else if .ShadowN}}<span class="pill" style="color:var(--danger);border-color:color-mix(in srgb,var(--danger) 45%,transparent);background:color-mix(in srgb,var(--danger) 12%,transparent)"><span class="dot"></span>would have been paused (shadow mode)</span>{{end}}
</div>

<div class="rstat">
  <div class="s"><div class="l">Spent</div><div class="v">{{usd .SpentUSD}}</div><div class="sub">{{.OK}} calls served</div></div>
  <div class="s"><div class="l">Calls</div><div class="v">{{.Total}}</div><div class="sub">{{.BlockedN}} blocked · {{.FlaggedN}} flagged{{if .ShadowN}} · {{.ShadowN}} would have been refused{{end}}</div></div>
  {{if and .ShadowN (not .BlockedN)}}<div class="s"><div class="l">Spent past the cap</div><div class="v">{{usd .ShadowUSD}}</div><div class="sub">{{.ShadowN}} calls served in shadow mode</div></div>{{else}}<div class="s saved"><div class="l">Prevented spend</div><div class="v">≈{{usd .AvoidedUSD}}</div><div class="sub">{{.BlockedN}} calls refused before the provider</div></div>{{end}}
</div>

<div class="tl"><h2>Timeline</h2>
  {{range .Calls}}
    {{if .FirstBlock}}<div class="tripped"><span class="lab">CAP TRIPPED</span><span class="txt">the gateway detected the loop and refused the calls marked blocked below</span></div>{{end}}
    {{if .FirstShadow}}<div class="tripped"><span class="lab">CAP WOULD HAVE TRIPPED</span><span class="txt">shadow mode: every call from here on was served, and marked</span></div>{{end}}
    <div class="call {{if .Blocked}}blk{{else if or .Loop .Shadow}}flag{{end}}">
      <span class="node"></span>
      <span class="t">#{{.N}} · {{.Time}}</span>
      <div class="mid"><span class="m">{{if .Model}}{{.Model}}{{else}}—{{end}}</span>{{if and .Loop (not .Blocked)}}<span class="flagtag">⚠ loop {{.LoopSignal}}</span>{{end}}
        <div class="tok">{{if .Blocked}}refused · {{.Reason}}{{else}}{{.InTok}} in · {{.OutTok}} out{{if .CacheRead}} · {{.CacheRead}} cached{{end}}{{if .Shadow}} · served, a cap would have refused it: {{.WouldRefuse}}{{end}}{{end}}</div></div>
      {{if .Blocked}}<span class="amt blk">blocked</span>{{else}}<span class="amt">{{usd .CostUSD}}</span>{{end}}
    </div>
  {{end}}
</div>
{{end}}
</div></body></html>`
