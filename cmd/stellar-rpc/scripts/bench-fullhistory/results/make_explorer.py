#!/usr/bin/env python3
"""Build a self-contained interactive HTML explorer from bench-fullhistory runs.

Usage:
    python3 make_explorer.py <run-root> <out.html> [--title "..."] [--source "gs://..."]

<run-root> contains one subdir per machine (the per-machine result dirs as
uploaded to gs://rpc-full-history/benchmarks/<date>/). The output is a single
HTML file with all data embedded — no server, no build step, works offline.
"""
import csv, json, os, sys, argparse

# machine dir prefix -> short label
def machine_label(dirname):
    return dirname.split("-2026")[0].split("-2025")[0]

def rc(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return list(csv.DictReader(f))

def sweep_rows(d, fn, machine, tier, workload, path):
    out = []
    for r in rc(os.path.join(d, fn)):
        out.append({
            "machine": machine, "tier": tier, "workload": workload, "path": path,
            "c": int(r["query_concurrency"]),
            "p50": float(r["p50_ms"]), "p90": float(r["p90_ms"]),
            "p99": float(r["p99_ms"]), "max": float(r["max_ms"]),
            "ops": float(r["ops_per_sec"]),
        })
    return out

def stage_rows(d, fn, machine, tier, mode, mapping):
    """mapping: {csv_stage: display_stage}"""
    out = []
    for r in rc(os.path.join(d, fn)):
        st = r["stage"]
        if st not in mapping:
            continue
        out.append({
            "machine": machine, "tier": tier, "mode": mode, "stage": mapping[st],
            "p50": float(r["p50_ns"]) / 1e6, "p90": float(r["p90_ns"]) / 1e6,
            "p99": float(r["p99_ns"]) / 1e6, "max": float(r["max_ns"]) / 1e6,
        })
    return out

def total_ns(d, fn, stage="total_per_ledger"):
    for r in rc(os.path.join(d, fn)):
        if r["stage"] == stage:
            return int(r["total_ns"])
    return None

def build(run_root):
    machines, queries, ingest, throughput, buildidx = [], [], [], [], []
    dirs = sorted(x for x in os.listdir(run_root) if os.path.isdir(os.path.join(run_root, x)))
    # stable machine ordering: 2x,4x,8x,arm
    order = {"c6id.2xlarge": 0, "c6id.4xlarge": 1, "c6id.8xlarge": 2, "im4gn.4xlarge": 3}
    dirs.sort(key=lambda x: order.get(machine_label(x), 99))
    for dn in dirs:
        d = os.path.join(run_root, dn)
        m = machine_label(dn)
        machines.append(m)
        for tier in ("cold", "hot"):
            queries += sweep_rows(d, f"{tier}-ledgers.csv", m, tier, "ledgers", "raw")
            queries += sweep_rows(d, f"{tier}-txpage-20-roundtrip-sweep.csv", m, tier, "tx-page", "roundtrip")
            queries += sweep_rows(d, f"{tier}-txpage-20-xdrviews-sweep.csv", m, tier, "tx-page", "xdr-views")
            queries += sweep_rows(d, f"{tier}-txhash-roundtrip-sweep.csv", m, tier, "tx-hash", "roundtrip")
            queries += sweep_rows(d, f"{tier}-txhash-xdrviews-sweep.csv", m, tier, "tx-hash", "xdr-views")
            queries += sweep_rows(d, f"{tier}-events-query-sweep.csv", m, tier, "events", "roundtrip")
            queries += sweep_rows(d, f"{tier}-events-query-xdrviews-sweep.csv", m, tier, "events", "xdr-views")
        # hot ingest: view + parsed
        for mode in ("view", "parsed"):
            ingest += stage_rows(d, f"hot-ledgers-{mode}.csv", m, "hot", mode, {"write": "ledgers.write"})
            ingest += stage_rows(d, f"hot-txhash-{mode}.csv", m, "hot", mode, {"extract": "txhash.extract", "hot_write": "txhash.write"})
            ingest += stage_rows(d, f"hot-events-{mode}.csv", m, "hot", mode, {"extract": "events.extract", "hot_write": "events.write"})
            ingest += stage_rows(d, f"hot-driver-{mode}.csv", m, "hot", mode, {
                "read_blocked": "driver.read_blocked", "fan_out_per_ledger": "driver.fan_out",
                "lcm_decode": "driver.lcm_decode", "total_per_ledger": "driver.total_per_ledger"})
            tn = total_ns(d, f"hot-driver-{mode}.csv")
            if tn:
                throughput.append({"machine": m, "tier": "hot", "mode": mode, "ledgers_per_s": round(10000 / (tn / 1e9), 1)})
        # cold ingest: view only
        ingest += stage_rows(d, "cold-ledgers-view.csv", m, "cold", "view", {"write": "ledgers.write"})
        ingest += stage_rows(d, "cold-txhash-view.csv", m, "cold", "view", {"extract": "txhash.extract"})
        ingest += stage_rows(d, "cold-events-view.csv", m, "cold", "view", {
            "extract": "events.extract", "term_index": "events.term_index", "cold_append": "events.cold_append"})
        ingest += stage_rows(d, "cold-driver-view.csv", m, "cold", "view", {
            "read_blocked": "driver.read_blocked", "fan_out_per_ledger": "driver.fan_out"})
        cw = next((r for r in rc(os.path.join(d, "cold-driver-view.csv")) if r["stage"] == "chunk_wall"), None)
        if cw:
            n = int(cw["n"]); secs = (int(cw["total_ns"]) / 1e9) / 8.0  # /chunk-workers
            throughput.append({"machine": m, "tier": "cold", "mode": "view", "ledgers_per_s": round((n * 10000) / secs, 0)})
        bi = rc(os.path.join(d, "build-txhash-index.csv"))
        if bi:
            r = bi[0]
            keys = int(r["total_keys"]); secs = (int(r["feed_ns"]) + int(r["finish_ns"])) / 1e9
            buildidx.append({"machine": m, "keys_per_s": round(keys / secs), "idx_mb": round(int(r["index_bytes"]) / 1e6)})
    return {"machines": machines, "queries": queries, "ingest": ingest,
            "throughput": throughput, "build_index": buildidx}

HTML = r"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
:root{--bg:#0f1117;--panel:#1a1d27;--line:#2a2f3d;--fg:#e6e8ee;--mut:#9aa3b2;--accent:#5b9dff;--accent2:#ffb454;}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
header{padding:16px 20px;border-bottom:1px solid var(--line)}
h1{font-size:18px;margin:0 0 4px}
.sub{color:var(--mut);font-size:12px}
.tabs{display:flex;gap:6px;padding:10px 20px 0}
.tab{padding:8px 14px;border:1px solid var(--line);border-bottom:none;border-radius:8px 8px 0 0;cursor:pointer;background:var(--panel);color:var(--mut)}
.tab.active{color:var(--fg);background:var(--bg);border-color:var(--accent)}
.wrap{padding:16px 20px}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:12px 14px;margin-bottom:14px}
.filters{display:flex;flex-wrap:wrap;gap:18px}
.fgroup{min-width:120px}
.fgroup h4{margin:0 0 6px;font-size:11px;text-transform:uppercase;letter-spacing:.05em;color:var(--mut)}
label{display:block;cursor:pointer;padding:2px 0;user-select:none;white-space:nowrap}
label input{margin-right:6px;vertical-align:middle}
.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.btn{font-size:12px;padding:3px 9px;border:1px solid var(--line);border-radius:6px;background:#222634;color:var(--fg);cursor:pointer}
.btn:hover{border-color:var(--accent)}
select{background:#222634;color:var(--fg);border:1px solid var(--line);border-radius:6px;padding:4px 8px}
table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}
th,td{padding:5px 9px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap}
th:first-child,td:first-child{text-align:left}
th{position:sticky;top:0;background:var(--panel);cursor:pointer;font-size:12px;color:var(--mut)}
th.sortcol{color:var(--accent)}
tbody tr:hover{background:#171b26}
.tag{display:inline-block;padding:1px 6px;border-radius:4px;font-size:11px;border:1px solid var(--line)}
.cold{color:#7fd0ff}.hot{color:#ffb454}.xdr{color:#7fffb0}.rt{color:#ff8d8d}.raw{color:var(--mut)}
.bars{display:flex;flex-direction:column;gap:3px;margin-top:8px}
.bar{display:flex;align-items:center;gap:8px}
.bar .lab{width:330px;color:var(--mut);font-size:12px;text-align:left;overflow:hidden;text-overflow:ellipsis}
.bar .track{flex:1;background:#11141d;border-radius:4px;height:16px;position:relative}
.bar .fill{height:100%;border-radius:4px;background:linear-gradient(90deg,var(--accent),var(--accent2))}
.bar .val{width:84px;text-align:right;font-size:12px}
.count{color:var(--mut);font-size:12px;margin-left:auto}
.line svg{width:100%;height:auto;display:block}
.line line.axis{stroke:var(--line)}
.line text{fill:var(--mut);font-size:11px}
.legend{display:flex;flex-wrap:wrap;gap:6px 14px;margin-top:10px}
.ser{display:flex;align-items:center;gap:5px;cursor:pointer;font-size:12px;color:var(--fg);user-select:none}
.ser.off{opacity:.35;text-decoration:line-through}
.sw{width:12px;height:12px;border-radius:3px;display:inline-block;flex:0 0 auto}
.hint{color:var(--mut);font-size:12px;margin:0 0 8px}
.hidden{display:none}
.note{color:var(--mut);font-size:12px;margin:6px 0 0}
</style></head>
<body>
<header><h1>__TITLE__</h1><div class="sub">__SOURCE__ · interactive explorer · all data embedded (offline-capable). Toggle tier / decode-path / percentiles / machines / concurrency; click a column header to sort.</div></header>
<div class="tabs"><div class="tab active" data-tab="queries">Queries</div><div class="tab" data-tab="ingest">Ingest</div></div>
<div class="wrap">
  <div id="queries">
    <div class="panel"><div class="filters" id="qfilters"></div>
      <div class="row" style="margin-top:10px"><button class="btn" data-all="q">all</button><button class="btn" data-none="q">none</button>
      <span style="margin-left:14px">chart metric: <select id="qchart"></select></span><span class="count" id="qcount"></span></div>
    </div>
    <div class="panel"><p class="hint">Line graph — one line per series (machine · tier · workload · path), x-axis = concurrency. Filter to one workload/path/tier to overlay machines; click a legend entry to hide/show a line.</p><div class="line" id="qline"></div><div class="legend" id="qlegend"></div></div>
    <div class="panel"><p class="hint">Bar chart — one bar per visible row.</p><div class="bars" id="qbars"></div></div>
    <div class="panel" style="overflow:auto;max-height:60vh"><table id="qtable"></table></div>
  </div>
  <div id="ingest" class="hidden">
    <div class="panel"><div class="filters" id="ifilters"></div>
      <div class="row" style="margin-top:10px"><button class="btn" data-all="i">all</button><button class="btn" data-none="i">none</button>
      <span style="margin-left:14px">chart metric: <select id="ichart"></select></span><span class="count" id="icount"></span></div>
      <div class="note">Per-ledger stage latencies (ms). Hot ingest ran <code>--parallel</code> in <b>view</b> (xdr-views) and <b>parsed</b> modes; cold ran view only. <code>driver.lcm_decode</code> exists only in parsed mode.</div>
    </div>
    <div class="panel"><p class="hint">Line graph — one line per series (machine · tier/mode), x-axis = pipeline stage. Filter to one tier/mode to overlay machines; click a legend entry to hide/show a line.</p><div class="line" id="iline"></div><div class="legend" id="ilegend"></div></div>
    <div class="panel"><p class="hint">Bar chart — one bar per visible row.</p><div class="bars" id="ibars"></div></div>
    <div class="panel" style="overflow:auto;max-height:50vh"><table id="itable"></table></div>
    <div class="panel"><h4 style="margin:0 0 8px;color:var(--mut)">Throughput</h4><table id="ttable"></table></div>
  </div>
</div>
<script id="data" type="application/json">__DATA__</script>
<script>
const DATA = JSON.parse(document.getElementById('data').textContent);
const $ = s => document.querySelector(s);
const pathClass = p => p==='xdr-views'?'xdr':p==='roundtrip'?'rt':'raw';

// ---- tabs ----
document.querySelectorAll('.tab').forEach(t=>t.onclick=()=>{
  document.querySelectorAll('.tab').forEach(x=>x.classList.toggle('active',x===t));
  $('#queries').classList.toggle('hidden',t.dataset.tab!=='queries');
  $('#ingest').classList.toggle('hidden',t.dataset.tab!=='ingest');
});

// ---- generic faceted view ----
function uniq(rows,k){return [...new Set(rows.map(r=>r[k]))];}
function makeView(cfg){
  const state={}; cfg.facets.forEach(f=>state[f.key]=new Set(uniq(cfg.rows,f.key)));
  let sortKey=cfg.metrics[0], sortDir=1;
  const hidden=new Set();   // series hidden via legend click (line chart)
  const fbox=$(cfg.filters);
  cfg.facets.forEach(f=>{
    const g=document.createElement('div');g.className='fgroup';
    g.innerHTML=`<h4>${f.label}</h4>`;
    uniq(cfg.rows,f.key).forEach(v=>{
      const id=cfg.key+'_'+f.key+'_'+v;
      const lab=document.createElement('label');
      lab.innerHTML=`<input type=checkbox checked data-f="${f.key}" value="${v}"> <span class="${f.cls?f.cls(v):''}">${v}</span>`;
      lab.querySelector('input').onchange=e=>{e.target.checked?state[f.key].add(typeof v==='number'?+e.target.value:e.target.value):state[f.key].delete(typeof v==='number'?+e.target.value:e.target.value);render();};
      g.appendChild(lab);
    });
    fbox.appendChild(g);
  });
  // metric toggles
  const mg=document.createElement('div');mg.className='fgroup';mg.innerHTML='<h4>metrics</h4>';
  const shown=new Set(cfg.metrics.slice(0,3));
  cfg.metrics.forEach(mk=>{
    const lab=document.createElement('label');
    lab.innerHTML=`<input type=checkbox ${shown.has(mk)?'checked':''} value="${mk}"> ${cfg.mlabel[mk]}`;
    lab.querySelector('input').onchange=e=>{e.target.checked?shown.add(mk):shown.delete(mk);render();};
    mg.appendChild(lab);
  });
  fbox.appendChild(mg);
  // chart metric dropdown
  const sel=$(cfg.chart);cfg.metrics.forEach(mk=>{const o=document.createElement('option');o.value=mk;o.textContent=cfg.mlabel[mk];sel.appendChild(o);});
  sel.onchange=render;
  // all/none
  document.querySelector(`[data-all="${cfg.key}"]`).onclick=()=>{cfg.facets.forEach(f=>state[f.key]=new Set(uniq(cfg.rows,f.key)));fbox.querySelectorAll('input[data-f]').forEach(c=>c.checked=true);render();};
  document.querySelector(`[data-none="${cfg.key}"]`).onclick=()=>{cfg.facets.forEach(f=>state[f.key]=new Set());fbox.querySelectorAll('input[data-f]').forEach(c=>c.checked=false);render();};

  function filtered(){return cfg.rows.filter(r=>cfg.facets.every(f=>state[f.key].has(r[f.key])));}
  function render(){
    const rows=filtered();
    const metrics=cfg.metrics.filter(m=>shown.has(m));
    rows.sort((a,b)=>{const A=a[sortKey],B=b[sortKey];if(typeof A==='number')return (A-B)*sortDir;return (''+A).localeCompare(''+B)*sortDir;});
    // table
    const cols=[...cfg.dims, ...metrics];
    const th=cols.map(c=>`<th data-k="${c}" class="${c===sortKey?'sortcol':''}">${cfg.clabel[c]||c}${c===sortKey?(sortDir>0?' ▲':' ▼'):''}</th>`).join('');
    const body=rows.map(r=>'<tr>'+cols.map(c=>{
      if(cfg.dims.includes(c)){const cl=c==='tier'?r[c]:c==='path'?pathClass(r[c]):'';return `<td class="${c==='tier'?r.tier:''}"><span class="${cl}">${r[c]}</span></td>`;}
      return `<td>${fmt(r[c])}</td>`;
    }).join('')+'</tr>').join('');
    $(cfg.table).innerHTML=`<thead><tr>${th}</tr></thead><tbody>${body}</tbody>`;
    $(cfg.table).querySelectorAll('th').forEach(h=>h.onclick=()=>{const k=h.dataset.k;if(k===sortKey)sortDir*=-1;else{sortKey=k;sortDir= (typeof rows[0]?.[k]==='number')?-1:1;}render();});
    $(cfg.count).textContent=rows.length+' rows';
    // bars
    const cm=sel.value;const mx=Math.max(1,...rows.map(r=>r[cm]));
    $(cfg.bars).innerHTML=rows.map(r=>{const w=(r[cm]/mx*100).toFixed(1);
      return `<div class="bar"><div class="lab">${cfg.barLabel(r)}</div><div class="track"><div class="fill" style="width:${w}%"></div></div><div class="val">${fmt(r[cm])}</div></div>`;}).join('');
    renderLine(rows, cm);
  }
  function renderLine(rows, metric){
    const groups={};
    rows.forEach(r=>{const k=cfg.seriesLabel(r);(groups[k]=groups[k]||[]).push(r);});
    const keys=Object.keys(groups).sort();
    let xvals=[...new Set(rows.map(r=>r[cfg.xDim]))];
    if(cfg.xOrder) xvals.sort((a,b)=>cfg.xOrder.indexOf(a)-cfg.xOrder.indexOf(b));
    else if(typeof xvals[0]==='number') xvals.sort((a,b)=>a-b);
    const vis=keys.filter(k=>!hidden.has(k));
    const maxY=Math.max(1,...vis.flatMap(k=>groups[k].map(r=>r[metric]||0)));
    const ny=niceMax(maxY);
    const W=920,H=380,mL=66,mR=14,mT=14,mB=cfg.xRotate?92:54;
    const pW=W-mL-mR,pH=H-mT-mB;
    const xpos=i=>mL+(xvals.length>1?i/(xvals.length-1)*pW:pW/2);
    const ypos=v=>mT+pH-(v/ny)*pH;
    let s=`<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="xMidYMid meet">`;
    for(let g=0;g<=4;g++){const v=ny*g/4,y=ypos(v);
      s+=`<line class="axis" x1="${mL}" y1="${y}" x2="${W-mR}" y2="${y}"/><text x="${mL-8}" y="${y+4}" text-anchor="end">${fmt(v)}</text>`;}
    xvals.forEach((xv,i)=>{const x=xpos(i),y=H-mB+16;
      s+= cfg.xRotate ? `<text x="${x}" y="${y}" text-anchor="end" transform="rotate(-30 ${x} ${y})">${xv}</text>`
                      : `<text x="${x}" y="${y}" text-anchor="middle">${(cfg.xPrefix||'')+xv}</text>`;});
    if(!cfg.xRotate) s+=`<text x="${mL+pW/2}" y="${H-8}" text-anchor="middle">${cfg.xLabel}</text>`;
    keys.forEach((k,ki)=>{
      if(hidden.has(k))return;
      const col=palette[ki%palette.length], pts=[];
      xvals.forEach((xv,i)=>{const r=groups[k].find(r=>r[cfg.xDim]===xv);if(r&&r[metric]!=null)pts.push([xpos(i),ypos(r[metric])]);});
      if(pts.length>1)s+=`<polyline fill="none" stroke="${col}" stroke-width="2" points="${pts.map(p=>p[0].toFixed(1)+','+p[1].toFixed(1)).join(' ')}"/>`;
      pts.forEach(p=>{s+=`<circle cx="${p[0].toFixed(1)}" cy="${p[1].toFixed(1)}" r="3" fill="${col}"/>`;});
    });
    s+='</svg>';
    $(cfg.line).innerHTML=s;
    $(cfg.legend).innerHTML=keys.map((k,ki)=>`<span class="ser ${hidden.has(k)?'off':''}" data-k="${encodeURIComponent(k)}"><span class="sw" style="background:${palette[ki%palette.length]}"></span>${k}</span>`).join('');
    $(cfg.legend).querySelectorAll('.ser').forEach(el=>el.onclick=()=>{const k=decodeURIComponent(el.dataset.k);hidden.has(k)?hidden.delete(k):hidden.add(k);render();});
  }
  render();
}
const palette=['#5b9dff','#ffb454','#7fffb0','#ff8d8d','#c792ea','#7fd0ff','#f78c6c','#c3e88d','#ff5370','#89ddff','#b39ddb','#ffcb6b','#80cbc4','#f07178'];
function niceMax(v){const p=Math.pow(10,Math.floor(Math.log10(v)));const n=v/p;const m=n<=1?1:n<=2?2:n<=2.5?2.5:n<=5?5:10;return m*p;}
function fmt(v){if(typeof v!=='number')return v;if(v>=1000)return v.toLocaleString(undefined,{maximumFractionDigits:0});return v.toLocaleString(undefined,{maximumFractionDigits:2});}

makeView({
  key:'q', rows:DATA.queries, filters:'#qfilters', table:'#qtable', bars:'#qbars', chart:'#qchart', count:'#qcount',
  facets:[{key:'machine',label:'Machine'},{key:'tier',label:'Tier',cls:v=>v},{key:'workload',label:'Workload'},
          {key:'path',label:'Decode path',cls:pathClass},{key:'c',label:'Concurrency'}],
  dims:['machine','tier','workload','path','c'],
  metrics:['p50','p99','p90','max','ops'],
  mlabel:{p50:'p50 (ms)',p90:'p90 (ms)',p99:'p99 (ms)',max:'max (ms)',ops:'ops/sec'},
  clabel:{machine:'Machine',tier:'Tier',workload:'Workload',path:'Path',c:'conc',p50:'p50 ms',p90:'p90 ms',p99:'p99 ms',max:'max ms',ops:'ops/s'},
  barLabel:r=>`${r.machine} · ${r.tier} · ${r.workload} · ${r.path} · c=${r.c}`,
  line:'#qline', legend:'#qlegend', xDim:'c', xLabel:'query-concurrency', xPrefix:'c=',
  seriesLabel:r=>`${r.machine} · ${r.tier} · ${r.workload} · ${r.path}`,
});
makeView({
  key:'i', rows:DATA.ingest, filters:'#ifilters', table:'#itable', bars:'#ibars', chart:'#ichart', count:'#icount',
  facets:[{key:'machine',label:'Machine'},{key:'tier',label:'Tier',cls:v=>v},{key:'mode',label:'Mode'},{key:'stage',label:'Stage'}],
  dims:['machine','tier','mode','stage'],
  metrics:['p50','p99','p90','max'],
  mlabel:{p50:'p50 (ms)',p90:'p90 (ms)',p99:'p99 (ms)',max:'max (ms)'},
  clabel:{machine:'Machine',tier:'Tier',mode:'Mode',stage:'Stage',p50:'p50 ms',p90:'p90 ms',p99:'p99 ms',max:'max ms'},
  barLabel:r=>`${r.machine} · ${r.tier}/${r.mode} · ${r.stage}`,
  line:'#iline', legend:'#ilegend', xDim:'stage', xLabel:'stage', xRotate:true,
  xOrder:['ledgers.write','txhash.extract','txhash.write','events.extract','events.term_index','events.cold_append','events.write','driver.read_blocked','driver.fan_out','driver.lcm_decode','driver.total_per_ledger'],
  seriesLabel:r=>`${r.machine} · ${r.tier}/${r.mode}`,
});
// throughput table (static)
(function(){
  const t=DATA.throughput.slice().sort((a,b)=>a.machine.localeCompare(b.machine)||a.tier.localeCompare(b.tier)||a.mode.localeCompare(b.mode));
  const bi=Object.fromEntries(DATA.build_index.map(r=>[r.machine,r]));
  let h='<thead><tr><th>Machine</th><th>Tier</th><th>Mode</th><th>ledgers/s</th><th>build-txhash keys/s</th><th>idx MB</th></tr></thead><tbody>';
  t.forEach(r=>{h+=`<tr><td>${r.machine}</td><td class="${r.tier}"><span class="${r.tier}">${r.tier}</span></td><td>${r.mode}</td><td>${fmt(r.ledgers_per_s)}</td><td>${bi[r.machine]?fmt(bi[r.machine].keys_per_s):''}</td><td>${bi[r.machine]?bi[r.machine].idx_mb:''}</td></tr>`;});
  $('#ttable').innerHTML=h+'</tbody>';
})();
</script></body></html>"""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_root")
    ap.add_argument("out")
    ap.add_argument("--title", default="stellar-rpc full-history bench explorer")
    ap.add_argument("--source", default="")
    a = ap.parse_args()
    data = build(a.run_root)
    html = (HTML.replace("__TITLE__", a.title)
                .replace("__SOURCE__", a.source or a.run_root)
                .replace("__DATA__", json.dumps(data, separators=(",", ":"))))
    with open(a.out, "w") as f:
        f.write(html)
    print(f"wrote {a.out}: {len(data['queries'])} query rows, {len(data['ingest'])} ingest rows, "
          f"{len(data['machines'])} machines, {len(html)} bytes")

if __name__ == "__main__":
    main()
