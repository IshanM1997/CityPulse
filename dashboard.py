"""
dashboard.py — City Pulse Interactive Dashboard
================================================
Flask app serving a rich Plotly dashboard that answers:
  "Which hour of the day is Manhattan most alive?"

Run:
    python dashboard.py
    Open: http://localhost:5050
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
from datetime import datetime
from flask import Flask, jsonify, send_from_directory
import database as db
from config import DASHBOARD_PORT, DASHBOARD_HOST

app = Flask(__name__, static_folder="static", static_url_path="/static")

@app.after_request
def cors(r):
    r.headers["Access-Control-Allow-Origin"]  = "*"
    r.headers["Access-Control-Allow-Headers"] = "Content-Type"
    r.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    return r

# ── API routes ─────────────────────────────────────────────────────────────────
@app.route("/api/kpis")
def api_kpis():
    return jsonify(db.query_kpis())

@app.route("/api/hourly-pulse")
def api_hourly_pulse():
    return jsonify(db.query_hourly_pulse(days=30))

@app.route("/api/weekday-pattern")
def api_weekday_pattern():
    return jsonify(db.query_weekday_pattern())

@app.route("/api/neighborhood-stats")
def api_neighborhood_stats():
    return jsonify(db.query_neighborhood_stats())

@app.route("/api/daily-summary")
def api_daily_summary():
    return jsonify(db.query_daily_summary(days=30))

@app.route("/api/top-hours")
def api_top_hours():
    return jsonify(db.query_top_hours(10))

@app.route("/api/weather-impact")
def api_weather_impact():
    return jsonify(db.query_weather_impact())

@app.route("/api/pipeline-history")
def api_pipeline_history():
    return jsonify(db.get_pipeline_history(20))

@app.route("/api/dq-summary")
def api_dq_summary():
    return jsonify(db.get_dq_summary())

@app.route("/api/data-counts")
def api_data_counts():
    return jsonify(db.get_data_counts())

@app.route("/api/recent-trips")
def api_recent_trips():
    return jsonify(db.get_recent_trips(50))

@app.route("/api/trigger-run", methods=["POST"])
def api_trigger_run():
    """Trigger a mini ETL run from the UI."""
    import threading
    from scheduler import run_hourly_pipeline
    def _run():
        try:
            run_hourly_pipeline(n_rows=10_000, span_days=30)
        except Exception as e:
            print(f"[API trigger] Error: {e}")
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return jsonify({"ok": True, "message": "Pipeline run triggered (10k rows)"})

@app.route("/api/trigger-backfill", methods=["POST"])
def api_trigger_backfill():
    """Trigger backfill from the UI."""
    import threading
    from scheduler import run_backfill
    def _run():
        try:
            run_backfill(n_rows=100_000, span_days=90)
        except Exception as e:
            print(f"[API backfill] Error: {e}")
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return jsonify({"ok": True, "message": "Backfill started (100k rows, 90 days)"})

@app.route("/")
def index():
    return DASHBOARD_HTML

# ── Embedded HTML dashboard ────────────────────────────────────────────────────
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>🏙 City Pulse — NYC ETL Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root{
  --bg:#06080F;--bg2:#0D1117;--bg3:#161B22;--bg4:#21262D;
  --border:#30363D;--border2:#3D444D;
  --gold:#FFD700;--gold-dim:rgba(255,215,0,.12);
  --cyan:#00D4FF;--cyan-dim:rgba(0,212,255,.12);
  --jade:#3FB950;--jade-dim:rgba(63,185,80,.12);
  --amber:#F0A500;--red:#F85149;--purple:#BC8CFF;
  --text:#E6EDF3;--text2:#8B949E;--text3:#484F58;
  --font:'Inter',system-ui,sans-serif;--mono:'JetBrains Mono',monospace;
}
*{box-sizing:border-box;margin:0;padding:0;}
html{font-size:14px;}
body{font-family:var(--font);background:var(--bg);color:var(--text);min-height:100vh;-webkit-font-smoothing:antialiased;}
::-webkit-scrollbar{width:5px;height:5px;}
::-webkit-scrollbar-track{background:var(--bg2);}
::-webkit-scrollbar-thumb{background:var(--border2);border-radius:3px;}

/* ── Header */
.header{
  background:linear-gradient(135deg,#0D1117 0%,#161B22 100%);
  border-bottom:1px solid var(--border);
  padding:20px 28px;
  display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:16px;
}
.brand{display:flex;align-items:center;gap:12px;}
.brand-icon{
  width:44px;height:44px;border-radius:12px;
  background:linear-gradient(135deg,#FFD700,#F0A500);
  display:flex;align-items:center;justify-content:center;font-size:1.4rem;
}
.brand-text h1{font-size:1.2rem;font-weight:800;letter-spacing:-.02em;}
.brand-text p{font-size:.72rem;color:var(--text2);margin-top:2px;font-family:var(--mono);}
.header-actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap;}
.btn{
  display:inline-flex;align-items:center;gap:6px;
  padding:8px 16px;border-radius:8px;font-size:.8rem;font-weight:600;
  border:none;cursor:pointer;font-family:var(--font);transition:.15s;
}
.btn:hover{filter:brightness(1.1);transform:translateY(-1px);}
.btn-gold{background:var(--gold);color:#0D1117;}
.btn-jade{background:var(--jade);color:#0D1117;}
.btn-ghost{background:var(--bg4);color:var(--text2);border:1px solid var(--border);}
.status-dot{width:8px;height:8px;border-radius:50%;background:var(--jade);animation:pulse 2s infinite;}
@keyframes pulse{0%,100%{opacity:1;box-shadow:0 0 0 0 rgba(63,185,80,.5)}50%{opacity:.6;box-shadow:0 0 0 6px rgba(63,185,80,0)}}

/* ── KPI strip */
.kpi-strip{
  display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));
  gap:12px;padding:20px 28px;background:var(--bg2);
  border-bottom:1px solid var(--border);
}
.kpi{
  background:var(--bg3);border:1px solid var(--border);border-radius:10px;
  padding:14px 16px;position:relative;overflow:hidden;
}
.kpi::after{content:'';position:absolute;bottom:0;left:0;right:0;height:2px;}
.kpi.gold::after{background:var(--gold);}
.kpi.jade::after{background:var(--jade);}
.kpi.cyan::after{background:var(--cyan);}
.kpi.amber::after{background:var(--amber);}
.kpi.purple::after{background:var(--purple);}
.kpi.red::after{background:var(--red);}
.kpi-icon{font-size:1.2rem;margin-bottom:6px;}
.kpi-val{font-size:1.5rem;font-weight:800;font-family:var(--mono);letter-spacing:-.03em;}
.kpi-lbl{font-size:.68rem;color:var(--text2);margin-top:2px;}

/* ── Layout */
.main{padding:20px 28px;display:flex;flex-direction:column;gap:16px;}
.row{display:grid;gap:16px;}
.row-2{grid-template-columns:1fr 1fr;}
.row-3{grid-template-columns:1fr 1fr 1fr;}
.row-4{grid-template-columns:repeat(4,1fr);}
@media(max-width:1100px){.row-3{grid-template-columns:1fr 1fr;}.row-4{grid-template-columns:1fr 1fr;}}
@media(max-width:700px){.row-2,.row-3,.row-4{grid-template-columns:1fr;}}

/* ── Chart cards */
.card{
  background:var(--bg2);border:1px solid var(--border);border-radius:12px;
  overflow:hidden;
}
.card.full{grid-column:1/-1;}
.card-header{
  padding:14px 18px 0;display:flex;align-items:flex-start;
  justify-content:space-between;margin-bottom:8px;
}
.card-title{font-size:.85rem;font-weight:700;color:var(--text);}
.card-sub{font-size:.7rem;color:var(--text2);margin-top:2px;}
.plotly-wrap{height:280px;}
.plotly-wrap.tall{height:360px;}
.plotly-wrap.short{height:200px;}
.plotly-wrap.sm{height:160px;}

/* ── Pulse hero */
.pulse-hero{
  background:linear-gradient(135deg,#0D1117,#161B22);
  border:1px solid var(--gold);border-radius:12px;
  padding:24px;position:relative;overflow:hidden;
  grid-column:1/-1;
}
.pulse-hero::before{
  content:'';position:absolute;top:-60px;right:-60px;
  width:240px;height:240px;border-radius:50%;
  background:radial-gradient(circle,rgba(255,215,0,.08),transparent 70%);
  pointer-events:none;
}
.pulse-title{font-size:1.1rem;font-weight:800;margin-bottom:4px;
  background:linear-gradient(90deg,var(--gold),var(--amber));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;}
.pulse-sub{font-size:.78rem;color:var(--text2);margin-bottom:16px;}

/* ── Top hours */
.top-hours-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:10px;}
.hour-card{
  background:var(--bg3);border:1px solid var(--border);border-radius:10px;
  padding:14px;text-align:center;transition:.15s;
}
.hour-card.rank-1{border-color:var(--gold);background:linear-gradient(135deg,rgba(255,215,0,.08),var(--bg3));}
.hour-card.rank-2{border-color:rgba(192,192,192,.6);}
.hour-card.rank-3{border-color:rgba(205,127,50,.6);}
.hour-rank{font-size:.65rem;color:var(--text3);text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;}
.hour-time{font-size:1.2rem;font-weight:800;font-family:var(--mono);color:var(--gold);}
.hour-score{font-size:.78rem;color:var(--text2);margin-top:4px;}
.hour-bar{height:4px;border-radius:2px;background:var(--border);margin-top:8px;overflow:hidden;}
.hour-bar-fill{height:100%;border-radius:2px;background:linear-gradient(90deg,var(--gold),var(--amber));}

/* ── Table */
.tbl-wrap{overflow-x:auto;max-height:320px;overflow-y:auto;}
table{width:100%;border-collapse:collapse;font-size:.78rem;}
thead th{
  background:var(--bg4);padding:9px 14px;text-align:left;
  font-size:.68rem;font-weight:700;color:var(--text2);
  text-transform:uppercase;letter-spacing:.05em;
  border-bottom:1px solid var(--border);position:sticky;top:0;z-index:1;
}
tbody tr{border-bottom:1px solid rgba(48,54,61,.5);transition:.1s;}
tbody tr:hover{background:var(--bg3);}
td{padding:9px 14px;vertical-align:middle;}
td.mono{font-family:var(--mono);font-size:.74rem;}
td.num{text-align:right;font-family:var(--mono);}
.badge{display:inline-block;padding:2px 7px;border-radius:10px;font-size:.65rem;font-weight:700;}
.badge-jade{background:var(--jade-dim);color:var(--jade);}
.badge-amber{background:rgba(240,165,0,.15);color:var(--amber);}
.badge-red{background:rgba(248,81,73,.15);color:var(--red);}
.badge-gray{background:var(--bg4);color:var(--text2);}

/* ── Pipeline log */
.pipeline-item{
  display:flex;align-items:center;gap:12px;
  padding:10px 16px;border-bottom:1px solid rgba(48,54,61,.4);
  font-size:.78rem;transition:.1s;
}
.pipeline-item:hover{background:var(--bg3);}
.pi-icon{font-size:1rem;flex-shrink:0;}
.pi-info{flex:1;min-width:0;}
.pi-dag{font-weight:600;color:var(--text);}
.pi-meta{font-size:.68rem;color:var(--text2);font-family:var(--mono);}
.pi-rows{font-family:var(--mono);color:var(--cyan);font-size:.76rem;}

/* ── Toast */
.toast-wrap{position:fixed;bottom:20px;right:20px;z-index:9999;display:flex;flex-direction:column;gap:8px;}
.toast{
  background:var(--bg3);border:1px solid var(--border);border-radius:8px;
  padding:10px 15px;font-size:.8rem;min-width:220px;
  box-shadow:0 8px 24px rgba(0,0,0,.6);
  animation:slideIn .2s ease;display:flex;align-items:center;gap:8px;
}
.toast.success{border-color:rgba(63,185,80,.4);}
.toast.error{border-color:rgba(248,81,73,.4);}
@keyframes slideIn{from{opacity:0;transform:translateX(20px)}to{opacity:1;transform:translateX(0)}}

/* ── Empty state */
.empty{text-align:center;padding:40px 20px;color:var(--text2);font-size:.85rem;}
.empty-icon{font-size:2.5rem;margin-bottom:10px;}
.loading{text-align:center;padding:30px;color:var(--text3);font-size:.82rem;}
</style>
</head>
<body>

<!-- ══ HEADER ══ -->
<header class="header">
  <div class="brand">
    <div class="brand-icon">🏙</div>
    <div class="brand-text">
      <h1>City Pulse</h1>
      <p>NYC ETL Pipeline Dashboard — Powered by Airflow + Pandas + SQLite</p>
    </div>
  </div>
  <div class="header-actions">
    <div class="status-dot" title="Pipeline active"></div>
    <span id="last-run-label" style="font-size:.72rem;color:var(--text2);font-family:var(--mono);">Loading…</span>
    <button class="btn btn-ghost" onclick="refreshAll()">🔄 Refresh</button>
    <button class="btn btn-jade" onclick="triggerRun()">▶ Run ETL (10k)</button>
    <button class="btn btn-gold" onclick="triggerBackfill()">📦 Backfill 90 Days</button>
  </div>
</header>

<!-- ══ KPI STRIP ══ -->
<div class="kpi-strip" id="kpi-strip">
  <div class="kpi gold"><div class="kpi-icon">🚕</div><div class="kpi-val" id="kpi-trips">—</div><div class="kpi-lbl">Total Trips Analyzed</div></div>
  <div class="kpi jade"><div class="kpi-icon">💰</div><div class="kpi-val" id="kpi-revenue">—</div><div class="kpi-lbl">Total Revenue</div></div>
  <div class="kpi cyan"><div class="kpi-icon">💸</div><div class="kpi-val" id="kpi-fare">—</div><div class="kpi-lbl">Avg Fare</div></div>
  <div class="kpi amber"><div class="kpi-icon">🏎</div><div class="kpi-val" id="kpi-speed">—</div><div class="kpi-lbl">Avg Speed (mph)</div></div>
  <div class="kpi purple"><div class="kpi-icon">⚡</div><div class="kpi-val" id="kpi-busiest">—</div><div class="kpi-lbl">Busiest Hour</div></div>
  <div class="kpi red"><div class="kpi-icon">🔄</div><div class="kpi-val" id="kpi-runs">—</div><div class="kpi-lbl">Pipeline Runs</div></div>
</div>

<!-- ══ MAIN ══ -->
<main class="main">

  <!-- Pulse Hero -->
  <div class="row">
    <div class="pulse-hero card">
      <div class="pulse-title">⚡ Which Hour is Manhattan Most Alive?</div>
      <div class="pulse-sub">Composite pulse score (0–100) weighted by trip volume, speed, surge usage, and revenue</div>
      <div class="top-hours-grid" id="top-hours-grid">
        <div class="loading">Loading pulse data…</div>
      </div>
    </div>
  </div>

  <!-- Row 1: Heatmap + Hourly line -->
  <div class="row">
    <div class="card full">
      <div class="card-header">
        <div>
          <div class="card-title">📅 Manhattan Pulse Heatmap — Hour × Day of Week</div>
          <div class="card-sub">Avg trips per hour by weekday — darker = more activity</div>
        </div>
      </div>
      <div class="plotly-wrap tall" id="chart-heatmap"></div>
    </div>
  </div>

  <!-- Row 2: Hourly + Daily -->
  <div class="row row-2">
    <div class="card">
      <div class="card-header"><div>
        <div class="card-title">🕐 Average Trips by Hour</div>
        <div class="card-sub">City rhythm — rush hours visible</div>
      </div></div>
      <div class="plotly-wrap" id="chart-hourly-avg"></div>
    </div>
    <div class="card">
      <div class="card-header"><div>
        <div class="card-title">📈 Daily Trip Volume</div>
        <div class="card-sub">30-day trend</div>
      </div></div>
      <div class="plotly-wrap" id="chart-daily"></div>
    </div>
  </div>

  <!-- Row 3: Neighborhood + Revenue -->
  <div class="row row-2">
    <div class="card">
      <div class="card-header"><div>
        <div class="card-title">🗺 Top Neighborhoods by Trips</div>
        <div class="card-sub">Where NYC moves most</div>
      </div></div>
      <div class="plotly-wrap" id="chart-neighborhood"></div>
    </div>
    <div class="card">
      <div class="card-header"><div>
        <div class="card-title">💵 Revenue by Neighborhood</div>
        <div class="card-sub">Total fare revenue per pickup zone</div>
      </div></div>
      <div class="plotly-wrap" id="chart-revenue"></div>
    </div>
  </div>

  <!-- Row 4: Weekday pattern + Pulse line -->
  <div class="row row-2">
    <div class="card">
      <div class="card-header"><div>
        <div class="card-title">📊 Weekday vs Weekend Pattern</div>
        <div class="card-sub">Avg trips — Mon–Fri vs Sat–Sun</div>
      </div></div>
      <div class="plotly-wrap" id="chart-weekday"></div>
    </div>
    <div class="card">
      <div class="card-header"><div>
        <div class="card-title">⚡ Pulse Score by Hour</div>
        <div class="card-sub">Composite city-alive score 0–100</div>
      </div></div>
      <div class="plotly-wrap" id="chart-pulse-line"></div>
    </div>
  </div>

  <!-- Row 5: Weather + Fare distribution -->
  <div class="row row-2">
    <div class="card">
      <div class="card-header"><div>
        <div class="card-title">🌧 Weather Impact on Trips</div>
        <div class="card-sub">Trip count vs temperature</div>
      </div></div>
      <div class="plotly-wrap" id="chart-weather"></div>
    </div>
    <div class="card">
      <div class="card-header"><div>
        <div class="card-title">💸 Fare Distribution</div>
        <div class="card-sub">Avg fare by hour of day</div>
      </div></div>
      <div class="plotly-wrap" id="chart-fare-hour"></div>
    </div>
  </div>

  <!-- Row 6: Pipeline log + DQ + Data counts -->
  <div class="row row-3">
    <div class="card">
      <div class="card-header"><div class="card-title">🔄 Pipeline Run History</div></div>
      <div id="pipeline-log" style="max-height:280px;overflow-y:auto;">
        <div class="loading">Loading…</div>
      </div>
    </div>
    <div class="card">
      <div class="card-header"><div class="card-title">✅ Data Quality Checks</div></div>
      <div id="dq-log" style="max-height:280px;overflow-y:auto;">
        <div class="loading">Loading…</div>
      </div>
    </div>
    <div class="card">
      <div class="card-header"><div class="card-title">🗄 Warehouse Row Counts</div></div>
      <div class="plotly-wrap short" id="chart-counts"></div>
    </div>
  </div>

  <!-- Row 7: Recent trips table -->
  <div class="card">
    <div class="card-header"><div class="card-title">🚖 Recent Trips (Latest 50)</div></div>
    <div class="tbl-wrap">
      <table>
        <thead><tr>
          <th>Pickup Time</th><th>From</th><th>To</th><th>Passengers</th>
          <th>Distance</th><th>Duration</th><th>Fare</th><th>Tip</th>
          <th>Total</th><th>Payment</th><th>Surge?</th>
        </tr></thead>
        <tbody id="trips-tbody"></tbody>
      </table>
    </div>
  </div>

</main>

<!-- Toast container -->
<div class="toast-wrap" id="toast-wrap"></div>

<script>
const API = 'http://localhost:5050/api';

/* ── Plotly config ─────────────────────────────────────────────────────────── */
const PLY_LAYOUT = {
  paper_bgcolor:'transparent', plot_bgcolor:'transparent',
  font:{family:'Inter',color:'#8B949E',size:11},
  margin:{t:10,r:14,b:40,l:50},
  xaxis:{gridcolor:'#21262D',linecolor:'#30363D',tickfont:{color:'#8B949E'}},
  yaxis:{gridcolor:'#21262D',linecolor:'#30363D',tickfont:{color:'#8B949E'}},
  showlegend:false,
};
const CFG = {displayModeBar:false,responsive:true};
const DAYS = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'];
const COLORS = ['#FFD700','#3FB950','#00D4FF','#BC8CFF','#F0A500','#F85149','#39D5FF','#E3B341'];

/* ── Utils ───────────────────────────────────────────────────────────────────*/
const fmt = n => n==null?'—':Number(n).toLocaleString('en-US',{maximumFractionDigits:0});
const fmtD = n => n==null?'—':'$'+Number(n).toFixed(2);
const fmtK = n => n==null?'—':Number(n)>=1e6?'$'+(n/1e6).toFixed(1)+'M':Number(n)>=1e3?'$'+(n/1e3).toFixed(0)+'K':'$'+Number(n).toFixed(0);
const hour12 = h => h==null?'—':`${h%12||12}${h<12?'am':'pm'}`;

function toast(msg, type='info'){
  const w=document.getElementById('toast-wrap');
  const d=document.createElement('div');
  d.className=`toast ${type}`;
  d.innerHTML=`<span>${type==='success'?'✅':type==='error'?'❌':'ℹ️'}</span> ${msg}`;
  w.appendChild(d);
  setTimeout(()=>{d.style.opacity='0';d.style.transition='.3s';},3500);
  setTimeout(()=>d.remove(),3900);
}

/* ── Load all data ───────────────────────────────────────────────────────────*/
async function fetchJSON(path){
  const r = await fetch(API+path);
  return r.json();
}

async function refreshAll(){
  toast('Refreshing dashboard…','info');
  await Promise.all([
    loadKPIs(), loadTopHours(), loadHeatmap(), loadHourlyAvg(),
    loadDaily(), loadNeighborhood(), loadWeekday(),
    loadWeather(), loadFareByHour(), loadPipelineLog(),
    loadDQLog(), loadDataCounts(), loadRecentTrips(),
  ]);
  toast('Dashboard refreshed','success');
}

/* ── KPIs ────────────────────────────────────────────────────────────────────*/
async function loadKPIs(){
  const d = await fetchJSON('/kpis');
  document.getElementById('kpi-trips').textContent   = fmt(d.total_trips);
  document.getElementById('kpi-revenue').textContent = fmtK(d.total_revenue);
  document.getElementById('kpi-fare').textContent    = fmtD(d.avg_fare);
  document.getElementById('kpi-speed').textContent   = d.avg_speed?d.avg_speed.toFixed(1)+'mph':'—';
  document.getElementById('kpi-busiest').textContent = d.busiest_hour!=null?hour12(d.busiest_hour):'—';
  document.getElementById('kpi-runs').textContent    = fmt(d.pipeline_runs);
  document.getElementById('last-run-label').textContent =
    d.last_run ? 'Last run: '+d.last_run.slice(0,16).replace('T',' ') : 'No runs yet';
}

/* ── Top Hours ───────────────────────────────────────────────────────────────*/
async function loadTopHours(){
  const data = await fetchJSON('/top-hours');
  if(!data.length){
    document.getElementById('top-hours-grid').innerHTML=
      '<div class="empty"><div class="empty-icon">⚡</div><p>Run the ETL pipeline to see pulse data</p></div>';
    return;
  }
  const maxPulse = Math.max(...data.map(h=>h.avg_pulse||0));
  const ranks = ['🥇','🥈','🥉'];
  document.getElementById('top-hours-grid').innerHTML = data.map((h,i)=>`
    <div class="hour-card rank-${i+1}">
      <div class="hour-rank">${ranks[i]||'#'+(i+1)} ${i===0?'BUSIEST':''}</div>
      <div class="hour-time">${hour12(h.hour)}</div>
      <div class="hour-score">Pulse: ${(h.avg_pulse||0).toFixed(1)}</div>
      <div class="hour-score">Trips: ~${Math.round(h.avg_trips||0).toLocaleString()}</div>
      <div class="hour-score">Fare: ${fmtD(h.avg_fare)}</div>
      <div class="hour-bar"><div class="hour-bar-fill" style="width:${maxPulse?((h.avg_pulse||0)/maxPulse*100).toFixed(1):0}%"></div></div>
    </div>`).join('');
}

/* ── Heatmap ─────────────────────────────────────────────────────────────────*/
async function loadHeatmap(){
  const data = await fetchJSON('/weekday-pattern');
  if(!data.length){
    document.getElementById('chart-heatmap').innerHTML='<div class="empty"><div class="empty-icon">📅</div><p>No pattern data yet</p></div>';
    return;
  }

  const hours   = Array.from({length:24},(_,i)=>i);
  const zMatrix = DAYS.map(day =>
    hours.map(h => {
      const row = data.find(r=>r.day_name===day&&r.hour===h);
      return row ? row.avg_trips : 0;
    })
  );
  const hoverMatrix = DAYS.map((day,di) =>
    hours.map((h,hi) => {
      const v = zMatrix[di][hi];
      return `${day} ${hour12(h)}<br><b>${Math.round(v)} avg trips</b>`;
    })
  );

  Plotly.newPlot('chart-heatmap',[{
    z: zMatrix, x: hours.map(hour12), y: DAYS,
    type:'heatmap',
    colorscale:[
      [0,'#0D1117'],[0.1,'#0E4429'],[0.3,'#006D32'],
      [0.6,'#26A641'],[0.8,'#39D353'],[1,'#FFD700']
    ],
    hovertext: hoverMatrix, hoverinfo:'text',
    showscale:true,
    colorbar:{tickfont:{color:'#8B949E'},thickness:12,title:{text:'trips',font:{color:'#8B949E'}}},
  }],{
    ...PLY_LAYOUT, height:320,
    margin:{t:10,r:80,b:50,l:100},
    xaxis:{...PLY_LAYOUT.xaxis,title:'Hour of Day'},
    yaxis:{...PLY_LAYOUT.yaxis,automargin:true},
  },CFG);
}

/* ── Hourly Avg ──────────────────────────────────────────────────────────────*/
async function loadHourlyAvg(){
  const data = await fetchJSON('/weekday-pattern');
  if(!data.length) return;

  // Average across all days
  const hourMap = {};
  for(const r of data){
    if(!hourMap[r.hour]) hourMap[r.hour]={sum:0,cnt:0,pulse:0};
    hourMap[r.hour].sum   += r.avg_trips||0;
    hourMap[r.hour].cnt   += 1;
    hourMap[r.hour].pulse += r.avg_pulse||0;
  }
  const hours  = Array.from({length:24},(_,i)=>i);
  const avgTrips = hours.map(h => hourMap[h]?(hourMap[h].sum/hourMap[h].cnt):0);
  const maxT    = Math.max(...avgTrips);

  Plotly.newPlot('chart-hourly-avg',[{
    x: hours.map(hour12), y: avgTrips, type:'bar',
    marker:{
      color: avgTrips.map(v=>v===maxT?'#FFD700':`rgba(255,215,0,${0.2+0.6*v/maxT})`),
      line:{width:0}
    },
    hovertemplate:'%{x}<br><b>%{y:.0f} avg trips</b><extra></extra>',
  }],{
    ...PLY_LAYOUT,
    shapes:[{
      type:'rect',xref:'paper',yref:'paper',x0:0.29,x1:0.42,y0:0,y1:1,
      fillcolor:'rgba(255,215,0,.06)',line:{width:0},
    },{
      type:'rect',xref:'paper',yref:'paper',x0:0.67,x1:0.83,y0:0,y1:1,
      fillcolor:'rgba(255,215,0,.06)',line:{width:0},
    }]
  },CFG);
}

/* ── Daily trend ─────────────────────────────────────────────────────────────*/
async function loadDaily(){
  const data = await fetchJSON('/daily-summary');
  if(!data.length) return;
  const sorted = data.sort((a,b)=>a.date<b.date?-1:1);
  Plotly.newPlot('chart-daily',[
    {x:sorted.map(r=>r.date), y:sorted.map(r=>r.total_trips),
     type:'scatter',mode:'lines+markers',
     line:{color:'#3FB950',width:2},
     marker:{size:4,color:'#3FB950'},
     fill:'tozeroy',fillcolor:'rgba(63,185,80,.08)',
     hovertemplate:'%{x}<br><b>%{y:,} trips</b><extra></extra>'},
  ],PLY_LAYOUT,CFG);
}

/* ── Neighborhood ────────────────────────────────────────────────────────────*/
async function loadNeighborhood(){
  const data = await fetchJSON('/neighborhood-stats');
  if(!data.length) return;
  const top = data.slice(0,12).sort((a,b)=>b.total_trips-a.total_trips);

  Plotly.newPlot('chart-neighborhood',[{
    x: top.map(r=>r.total_trips),
    y: top.map(r=>r.neighborhood),
    type:'bar',orientation:'h',
    marker:{color:top.map((_,i)=>COLORS[i%COLORS.length]),line:{width:0}},
    hovertemplate:'%{y}<br><b>%{x:,} trips</b><extra></extra>',
  }],{
    ...PLY_LAYOUT,
    margin:{t:10,r:14,b:30,l:160},
    yaxis:{...PLY_LAYOUT.yaxis,automargin:true},
  },CFG);

  Plotly.newPlot('chart-revenue',[{
    x: top.map(r=>r.total_revenue||0),
    y: top.map(r=>r.neighborhood),
    type:'bar',orientation:'h',
    marker:{color:top.map((_,i)=>`rgba(255,215,0,${0.9-i*0.06})`),line:{width:0}},
    hovertemplate:'%{y}<br><b>$%{x:,.0f} revenue</b><extra></extra>',
  }],{
    ...PLY_LAYOUT,
    margin:{t:10,r:14,b:30,l:160},
    xaxis:{...PLY_LAYOUT.xaxis,tickprefix:'$'},
    yaxis:{...PLY_LAYOUT.yaxis,automargin:true},
  },CFG);
}

/* ── Weekday pattern ─────────────────────────────────────────────────────────*/
async function loadWeekday(){
  const data = await fetchJSON('/weekday-pattern');
  if(!data.length) return;
  const hours = Array.from({length:24},(_,i)=>i);

  const weekdays = [0,1,2,3,4]; // Mon-Fri
  const weekends = [5,6];
  const wdAvg = hours.map(h=>{
    const rows = data.filter(r=>weekdays.includes(r.day_of_week)&&r.hour===h);
    return rows.length?rows.reduce((a,r)=>a+(r.avg_trips||0),0)/rows.length:0;
  });
  const weAvg = hours.map(h=>{
    const rows = data.filter(r=>weekends.includes(r.day_of_week)&&r.hour===h);
    return rows.length?rows.reduce((a,r)=>a+(r.avg_trips||0),0)/rows.length:0;
  });

  Plotly.newPlot('chart-weekday',[
    {x:hours.map(hour12),y:wdAvg,name:'Weekday',type:'scatter',mode:'lines',
     line:{color:'#00D4FF',width:2},fill:'tozeroy',fillcolor:'rgba(0,212,255,.06)'},
    {x:hours.map(hour12),y:weAvg,name:'Weekend',type:'scatter',mode:'lines',
     line:{color:'#BC8CFF',width:2,dash:'dot'},fill:'tozeroy',fillcolor:'rgba(188,140,255,.06)'},
  ],{...PLY_LAYOUT,showlegend:true,legend:{font:{color:'#8B949E'},bgcolor:'transparent'}},CFG);

  // Pulse line chart
  const pulseByHour = hours.map(h=>{
    const rows = data.filter(r=>r.hour===h);
    return rows.length?rows.reduce((a,r)=>a+(r.avg_pulse||0),0)/rows.length:0;
  });
  const maxP = Math.max(...pulseByHour);
  Plotly.newPlot('chart-pulse-line',[{
    x:hours.map(hour12), y:pulseByHour,
    type:'scatter',mode:'lines+markers',
    line:{color:'#FFD700',width:3},
    marker:{size:pulseByHour.map(p=>p===maxP?10:5),
            color:pulseByHour.map(p=>p===maxP?'#FFD700':'rgba(255,215,0,.5)')},
    fill:'tozeroy',fillcolor:'rgba(255,215,0,.06)',
    hovertemplate:'%{x}<br><b>Pulse: %{y:.1f}</b><extra></extra>',
  }],{...PLY_LAYOUT,yaxis:{...PLY_LAYOUT.yaxis,range:[0,105],title:'Pulse Score'}},CFG);
}

/* ── Weather ─────────────────────────────────────────────────────────────────*/
async function loadWeather(){
  const data = await fetchJSON('/weather-impact');
  if(!data.length){
    document.getElementById('chart-weather').innerHTML='<div class="empty"><div class="empty-icon">🌧</div><p>No weather data yet</p></div>';
    return;
  }
  const byTemp = data.filter(r=>r.temp_f&&r.trip_count);
  Plotly.newPlot('chart-weather',[{
    x: byTemp.map(r=>r.temp_f),
    y: byTemp.map(r=>r.trip_count),
    mode:'markers',
    marker:{
      size:7,opacity:.7,
      color:byTemp.map(r=>r.is_rain?'#00D4FF':r.is_snow?'#BC8CFF':'#3FB950'),
    },
    hovertemplate:'Temp: %{x}°F<br>Trips: %{y:,}<extra></extra>',
  }],{
    ...PLY_LAYOUT,
    xaxis:{...PLY_LAYOUT.xaxis,title:'Temperature (°F)'},
    yaxis:{...PLY_LAYOUT.yaxis,title:'Trip Count'},
  },CFG);
}

/* ── Fare by hour ────────────────────────────────────────────────────────────*/
async function loadFareByHour(){
  const data = await fetchJSON('/weekday-pattern');
  if(!data.length) return;
  const hours = Array.from({length:24},(_,i)=>i);
  const fares = hours.map(h=>{
    const rows = data.filter(r=>r.hour===h&&r.avg_fare);
    return rows.length?rows.reduce((a,r)=>a+(r.avg_fare||0),0)/rows.length:0;
  });
  Plotly.newPlot('chart-fare-hour',[{
    x:hours.map(hour12),y:fares,type:'scatter',mode:'lines+markers',
    line:{color:'#F0A500',width:2},
    marker:{size:5,color:'#F0A500'},
    fill:'tozeroy',fillcolor:'rgba(240,165,0,.08)',
    hovertemplate:'%{x}<br><b>Avg fare: $%{y:.2f}</b><extra></extra>',
  }],{...PLY_LAYOUT,yaxis:{...PLY_LAYOUT.yaxis,tickprefix:'$'}},CFG);
}

/* ── Pipeline log ────────────────────────────────────────────────────────────*/
async function loadPipelineLog(){
  const data = await fetchJSON('/pipeline-history');
  const el = document.getElementById('pipeline-log');
  if(!data.length){el.innerHTML='<div class="empty"><div class="empty-icon">🔄</div><p>No pipeline runs yet</p></div>';return;}
  const icons={success:'✅',failed:'❌',running:'🔄',skipped:'⏭'};
  el.innerHTML = data.map(r=>`
    <div class="pipeline-item">
      <div class="pi-icon">${icons[r.status]||'❓'}</div>
      <div class="pi-info">
        <div class="pi-dag">${r.dag_id} <span style="color:var(--text3);font-weight:400">→ ${r.task_id}</span></div>
        <div class="pi-meta">${r.started_at?.slice(0,19).replace('T',' ')||'—'} · ${r.run_id}</div>
      </div>
      <div class="pi-rows">${(r.rows_loaded||0).toLocaleString()} rows</div>
    </div>`).join('');
}

/* ── DQ log ──────────────────────────────────────────────────────────────────*/
async function loadDQLog(){
  const data = await fetchJSON('/dq-summary');
  const el = document.getElementById('dq-log');
  if(!data.length){el.innerHTML='<div class="empty"><div class="empty-icon">✅</div><p>No DQ checks yet</p></div>';return;}
  const cls={PASS:'jade',FAIL:'red',WARN:'amber'};
  el.innerHTML = data.slice(0,30).map(r=>`
    <div class="pipeline-item">
      <div><span class="badge badge-${cls[r.status]||'gray'}">${r.status}</span></div>
      <div class="pi-info">
        <div class="pi-dag" style="font-size:.78rem;">${r.check_name}</div>
        <div class="pi-meta">${r.table_name} · ${r.logged_at?.slice(0,16).replace('T',' ')||'—'}</div>
      </div>
      <div style="font-size:.7rem;color:var(--text2);text-align:right;">
        <div>exp: ${r.expected}</div>
        <div>got: ${r.actual}</div>
      </div>
    </div>`).join('');
}

/* ── Data counts ─────────────────────────────────────────────────────────────*/
async function loadDataCounts(){
  const d = await fetchJSON('/data-counts');
  const labels = Object.keys(d).map(k=>k.replace(/_/g,' '));
  const values = Object.values(d);
  Plotly.newPlot('chart-counts',[{
    x:values,y:labels,type:'bar',orientation:'h',
    marker:{color:labels.map((_,i)=>COLORS[i%COLORS.length]),line:{width:0}},
    hovertemplate:'%{y}<br><b>%{x:,}</b><extra></extra>',
    text:values.map(v=>v.toLocaleString()),textposition:'auto',
  }],{
    ...PLY_LAYOUT, height:190,
    margin:{t:10,r:14,b:30,l:120},
    yaxis:{...PLY_LAYOUT.yaxis,automargin:true},
  },CFG);
}

/* ── Recent trips table ──────────────────────────────────────────────────────*/
async function loadRecentTrips(){
  const data = await fetchJSON('/recent-trips');
  const tbody = document.getElementById('trips-tbody');
  if(!data.length){tbody.innerHTML='<tr><td colspan="11" class="empty">No trip data yet</td></tr>';return;}
  tbody.innerHTML = data.map(t=>`<tr>
    <td class="mono" style="color:var(--text2);white-space:nowrap;">${t.pickup_datetime?.slice(0,16).replace('T',' ')||'—'}</td>
    <td style="max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${t.pickup_neighborhood||'—'}</td>
    <td style="max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${t.dropoff_neighborhood||'—'}</td>
    <td class="num">${t.passenger_count||1}</td>
    <td class="num">${(t.trip_distance_miles||0).toFixed(1)}mi</td>
    <td class="num">${(t.trip_duration_min||0).toFixed(0)}min</td>
    <td class="num" style="color:var(--jade);">$${(t.fare_amount||0).toFixed(2)}</td>
    <td class="num" style="color:var(--amber);">$${(t.tip_amount||0).toFixed(2)}</td>
    <td class="num" style="color:var(--gold);font-weight:700;">$${(t.total_amount||0).toFixed(2)}</td>
    <td><span class="badge ${t.payment_type==='Credit Card'?'badge-jade':t.payment_type==='Cash'?'badge-gray':'badge-amber'}">${t.payment_type||'—'}</span></td>
    <td>${t.is_surge_hour?'⚡':'—'}</td>
  </tr>`).join('');
}

/* ── Action buttons ──────────────────────────────────────────────────────────*/
async function triggerRun(){
  toast('Triggering ETL run (10k rows)…','info');
  const r = await fetch(API+'/trigger-run',{method:'POST'});
  const d = await r.json();
  toast(d.message,'success');
  setTimeout(refreshAll, 8000);
}

async function triggerBackfill(){
  if(!confirm('Run 90-day backfill? This generates ~100k rows and takes 30-60 seconds.')) return;
  toast('Backfill started — this takes 30-60 seconds…','info');
  const r = await fetch(API+'/trigger-backfill',{method:'POST'});
  const d = await r.json();
  toast(d.message,'success');
  setTimeout(refreshAll, 15000);
}

/* ── Init ────────────────────────────────────────────────────────────────────*/
refreshAll();
setInterval(loadKPIs, 15000);
setInterval(loadPipelineLog, 10000);
</script>
</body>
</html>"""

if __name__ == "__main__":
    db.init_db()
    print(f"""
╔══════════════════════════════════════════════════════╗
║   🏙  City Pulse — NYC ETL Dashboard                ║
║   http://localhost:{DASHBOARD_PORT}                         ║
╚══════════════════════════════════════════════════════╝
  ▶ Click "Run ETL (10k)" to load sample data
  📦 Click "Backfill 90 Days" for full history
""")
    app.run(host=DASHBOARD_HOST, port=DASHBOARD_PORT, debug=False, threaded=True)
