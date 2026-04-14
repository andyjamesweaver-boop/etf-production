"""
Standalone Insights Pages for the ETF Dashboard
================================================
Each page is a fully self-contained HTML document that fetches data
from /api/v1/insights/* and renders rich charts + tables.
"""

# ---------------------------------------------------------------------------
# Shared head / helpers injected into every page
# ---------------------------------------------------------------------------
_HEAD = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title} — Australian ETF Market</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/luxon@3.4.4/build/global/luxon.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon@1.3.1/dist/chartjs-adapter-luxon.umd.min.js"></script>
<style>
  body { font-family: 'Inter', system-ui, -apple-system, sans-serif; background: #f8fafc; color: #0f172a; }
  .card { background: #fff; border-radius: 12px; box-shadow: 0 1px 3px rgba(0,0,0,.06),0 1px 2px rgba(0,0,0,.04); border: 1px solid #e2e8f0; padding: 1.25rem; }
  table { width: 100%; border-collapse: collapse; }
  th { text-align: left; font-size: .7rem; font-weight: 700; text-transform: uppercase; letter-spacing: .04em;
       color: #64748b; padding: .45rem .75rem; background: #f8fafc; border-bottom: 1px solid #e2e8f0; }
  td { padding: .45rem .75rem; font-size: .8rem; border-bottom: 1px solid #f1f5f9; vertical-align: middle; color: #0f172a; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: #f8fafc; }
  .bar-track { height: 6px; background: #e2e8f0; border-radius: 3px; overflow: hidden; }
  .bar-fill  { height: 100%; border-radius: 3px; transition: width .5s ease; }
  .badge { display: inline-block; padding: .15rem .45rem; border-radius: 4px; font-size: .68rem; font-weight: 600; }
  .spinner { width: 28px; height: 28px; border: 3px solid #e2e8f0; border-top-color: #2563eb;
             border-radius: 50%; animation: spin .7s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
  .sv { font-size: 1.9rem; font-weight: 800; line-height: 1; color: #0f172a; }
  .sl { font-size: .68rem; font-weight: 700; text-transform: uppercase; letter-spacing: .04em; color: #64748b; }
  .ss { font-size: .73rem; color: #64748b; margin-top: .2rem; }
  .pos { color: #16a34a; } .neg { color: #dc2626; }
  .hbar-row { display: flex; align-items: center; gap: 8px; margin-bottom: 6px; }
  .hbar-label { font-size: .75rem; color: #0f172a; font-weight: 500; width: 110px; flex-shrink: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .hbar-track { flex: 1; height: 16px; background: #e2e8f0; border-radius: 4px; overflow: hidden; }
  .hbar-fill  { height: 100%; border-radius: 4px; transition: width .6s ease; }
  .hbar-val   { font-size: .73rem; color: #475569; width: 52px; text-align: right; flex-shrink: 0; }
  .hbar-sub   { font-size: .65rem; color: #94a3b8; width: 32px; text-align: right; flex-shrink: 0; }
  .dim-btn { font-size: .72rem; font-weight: 600; padding: .3rem .75rem; border-radius: 6px;
             border: 1px solid #e2e8f0; background: #fff; color: #475569; cursor: pointer; transition: all .15s; }
  .dim-btn:hover { border-color: #2563eb; color: #2563eb; background: #eff6ff; }
  .dim-btn.active-dim { background: #2563eb; border-color: #2563eb; color: #fff; }
  ::-webkit-scrollbar { width: 5px; height: 5px; }
  ::-webkit-scrollbar-track { background: #f1f5f9; }
  ::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 4px; }
</style>
</head>"""

_COMMON_JS = """
const BASE = '';
async function api(url) {
  const r = await fetch(BASE + url);
  if (!r.ok) throw new Error(r.status + ' ' + url);
  return r.json();
}
function fmtFum(v) {
  if (v == null) return '—';
  if (Math.abs(v) >= 1000) return '$' + (v / 1000).toFixed(2) + 'B';
  return '$' + v.toFixed(0) + 'M';
}
function pct(v, d) {
  if (v == null) return '—';
  d = d == null ? 2 : d;
  const s = Number(v).toFixed(d) + '%';
  return (v > 0 ? '+' : '') + s;
}
function pcls(v) { return v > 0 ? 'pos' : (v < 0 ? 'neg' : ''); }
function mer(v) { return v != null ? Number(v).toFixed(2) + '%' : '—'; }
function rank(n) { return n + 1; }
const AC_COLORS = {
  'International Equities': '#3b82f6', 'Australian Equities': '#10b981',
  'Fixed Income': '#f59e0b', 'Commodities': '#f97316',
  'Property': '#8b5cf6', 'Diversified': '#06b6d4',
  'Cash': '#84cc16', 'Thematic': '#ec4899',
  'Alternatives': '#6366f1', 'Digital Assets': '#14b8a6',
  'leveraged & inverse': '#ef4444', 'Currency': '#a855f7',
};
const ISS_COLORS = {
  'Betashares':                '#1b2b6b',
  'BetaShares':                '#1b2b6b',
  'iShares':                   '#13294b',
  'Global X':                  '#00adef',
  'VanEck':                    '#f7941d',
  'Vanguard':                  '#c41230',
  'J.P. Morgan':               '#003087',
  'JPMorgan':                  '#003087',
  'State Street Investment Management': '#1a9dd9',
  'State Street':              '#1a9dd9',
  'StateStreet':               '#1a9dd9',
  'SPDR':                      '#1a9dd9',
  'Macquarie':                 '#002b5c',
  'Dimensional':               '#004b87',
  'DFA':                       '#004b87',
  'Magellan':                  '#b8141a',
  'Russell Investments':       '#007dc5',
  'Fidelity':                  '#4caf50',
  'Schroders':                 '#00429c',
  'PIMCO':                     '#00a0af',
  'Janus Henderson':           '#cc3300',
  'Franklin Templeton':        '#af1f24',
  'ClearBridge / Franklin Templeton': '#af1f24',
  'Perpetual':                 '#5b0c8f',
  'J.P. Morgan / Perpetual':   '#5b0c8f',
  'JPMAM / Perpetual':         '#5b0c8f',
  'Coolabah':                  '#0d6efd',
  'Hyperion / Pinnacle':       '#7c3aed',
  'Hyperion':                  '#7c3aed',
  'Resolution / Pinnacle':     '#0891b2',
  'The Perth Mint':            '#d4a017',
  'Hejaz / EQT':               '#2e7d32',
  'InvestSMART':               '#e65100',
  'Australian Ethical':        '#388e3c',
  'Platinum':                  '#7b1fa2',
  'Monochrome':                '#212121',
  'Munro / GSFM':              '#00695c',
  'Ausbil':                    '#0277bd',
  'Avantis':                   '#558b2f',
};
function acColor(n) { return AC_COLORS[n] || '#94a3b8'; }
function issColor(n) { return ISS_COLORS[n] || '#64748b'; }
function hbar(label, value, maxVal, fillColor, valStr, subStr) {
  const w = maxVal > 0 ? Math.min(value / maxVal * 100, 100).toFixed(1) : 0;
  return `<div class="hbar-row">
    <span class="hbar-label" title="${label}">${label}</span>
    <div class="hbar-track"><div class="hbar-fill" style="width:${w}%;background:${fillColor}"></div></div>
    <span class="hbar-val">${valStr}</span>
    <span class="hbar-sub">${subStr || ''}</span>
  </div>`;
}
function slugify(name) {
  return name.toLowerCase()
    .replace(/\\s*\\/\\s*/g, '-').replace(/\\s*&\\s*/g, '-')
    .replace(/\\./g, '').replace(/'/g, '')
    .replace(/\\s+/g, '-')
    .replace(/[^a-z0-9-]/g, '').replace(/-+/g, '-').replace(/^-|-$/g, '');
}
function issLink(name) {
  if (!name) return '—';
  return `<a href="/issuers/${slugify(name)}" style="color:${issColor(name)}" class="hover:underline font-semibold">${name}</a>`;
}
"""

def _page(title, subtitle, body_html, init_js):
    return f"""{_HEAD.replace('{title}', title)}
<body>
<header class="bg-white border-b border-slate-200">
  <div class="max-w-7xl mx-auto px-5 py-3 flex items-center gap-5">
    <a href="/dashboard" class="text-slate-500 hover:text-blue-600 text-sm font-medium transition-colors shrink-0 flex items-center gap-1">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 18 9 12 15 6"/></svg>
      Dashboard
    </a>
    <div class="w-px h-5 bg-slate-200 shrink-0"></div>
    <div class="flex-1 min-w-0">
      <h1 class="text-base font-bold text-slate-900">{title}</h1>
      <p class="text-slate-500 text-xs mt-0.5">{subtitle}</p>
    </div>
    <span id="ts" class="text-slate-400 text-xs shrink-0"></span>
  </div>
</header>
<main class="max-w-7xl mx-auto px-4 py-6">
  <div id="loading" class="flex justify-center py-20"><div class="spinner"></div></div>
  <div id="page" class="hidden space-y-5">{body_html}</div>
</main>
<script>
{_COMMON_JS}
{init_js}
window.addEventListener('DOMContentLoaded', () => init().catch(e => {{
  document.getElementById('loading').innerHTML = '<p class="text-red-500 text-sm">' + e.message + '</p>';
}}));
</script>
</body></html>"""


# ---------------------------------------------------------------------------
# FUM ANALYSIS PAGE
# ---------------------------------------------------------------------------
_FUM_BODY = """
<div id="hero" class="grid grid-cols-2 sm:grid-cols-4 gap-4"></div>
<div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-4">Issuers by FUM</h2>
    <div id="issuer-bars"></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-4">Asset Classes by FUM</h2>
    <div class="relative" style="height:260px"><canvas id="chart-ac"></canvas></div>
  </div>
</div>
<div class="card">
  <h2 class="font-semibold text-sm text-slate-700 mb-3">Top ETFs by FUM</h2>
  <div class="overflow-x-auto"><table>
    <thead><tr>
      <th>#</th><th>Code</th><th>Name</th><th>Issuer</th><th>Asset Class</th>
      <th class="text-right">FUM</th><th class="text-right">% Market</th><th style="min-width:100px"></th>
    </tr></thead>
    <tbody id="fum-table"></tbody>
  </table></div>
</div>
<div class="card">
  <h2 class="font-semibold text-sm text-slate-700 mb-4">Top 5 ETFs by FUM — per Asset Class</h2>
  <div id="per-class" class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4"></div>
</div>
<div class="card">
  <h2 class="font-semibold text-sm text-slate-700 mb-4">Top 5 ETFs by FUM — per Issuer</h2>
  <div id="per-issuer" class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4"></div>
</div>
"""

_FUM_JS = """
async function init() {
  const d = await api('/api/v1/insights/fum');
  document.getElementById('ts').textContent = new Date().toLocaleTimeString('en-AU');
  const total = d.total_fum;

  document.getElementById('hero').innerHTML = [
    ['Total Market FUM', fmtFum(total), 'all exchanges'],
    ['Largest ETF', d.top_etfs[0]?.code || '—', fmtFum(d.top_etfs[0]?.fum)],
    ['Top 10 Concentration', (d.concentration.top10_pct).toFixed(1) + '%', 'of total market FUM'],
    ['No. of Issuers', d.by_issuer.length, 'active fund managers'],
  ].map(([l,v,s]) => `<div class="card"><div class="sl">${l}</div><div class="sv">${v}</div><div class="ss">${s}</div></div>`).join('');

  // Issuer bars
  const issMax = d.by_issuer[0]?.total_fum || 1;
  document.getElementById('issuer-bars').innerHTML = d.by_issuer
    .map(r => hbar(r.issuer, r.total_fum, issMax, issColor(r.issuer), fmtFum(r.total_fum), r.pct + '%'))
    .join('');

  // Asset class bar chart
  const ac = d.by_asset_class;
  new Chart(document.getElementById('chart-ac').getContext('2d'), {
    type: 'bar',
    data: {
      labels: ac.map(r => r.asset_class),
      datasets: [{ data: ac.map(r => r.total_fum), backgroundColor: ac.map(r => acColor(r.asset_class)), borderRadius: 5, borderSkipped: false }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: ctx => ' ' + fmtFum(ctx.parsed.y) + '  (' + ac[ctx.dataIndex].pct + '%)' } } },
      scales: { x: { ticks: { font: { size: 10 } } }, y: { ticks: { callback: v => fmtFum(v), font: { size: 10 } }, grid: { color: '#f1f5f9' } } },
    },
  });

  // Top ETFs table
  const topMax = d.top_etfs[0]?.fum || 1;
  document.getElementById('fum-table').innerHTML = d.top_etfs.map((e, i) => `<tr>
    <td class="text-slate-500 tabular-nums">${i + 1}</td>
    <td class="font-bold text-green-600">${e.code}</td>
    <td class="text-slate-700 max-w-xs" style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${e.name}">${e.name}</td>
    <td class="text-slate-400">${e.issuer || '—'}</td>
    <td><span class="badge" style="background:${acColor(e.asset_class)}22;color:${acColor(e.asset_class)}">${e.asset_class || '—'}</span></td>
    <td class="text-right font-semibold tabular-nums">${fmtFum(e.fum)}</td>
    <td class="text-right text-slate-400 tabular-nums">${e.pct}%</td>
    <td><div class="bar-track"><div class="bar-fill bg-green-400" style="width:${Math.min(e.fum / topMax * 100, 100).toFixed(1)}%"></div></div></td>
  </tr>`).join('');

  // Per class mini tables
  const classCard = (ac_name, etfs) => `<div class="border border-slate-200 rounded-lg p-3 bg-slate-50">
    <div class="text-xs font-bold uppercase mb-2" style="color:${acColor(ac_name)}">${ac_name}</div>
    ${etfs.map((e, i) => `<div class="flex items-center gap-2 py-1 ${i < etfs.length - 1 ? 'border-b border-slate-100' : ''}">
      <span class="text-slate-500 text-xs w-3 tabular-nums">${i + 1}</span>
      <span class="font-bold text-green-600 text-xs w-10">${e.code}</span>
      <span class="flex-1 text-xs text-slate-400 truncate" title="${e.name}">${e.name}</span>
      <span class="text-xs font-semibold text-slate-700 ml-1 tabular-nums">${fmtFum(e.fum)}</span>
    </div>`).join('')}
  </div>`;

  document.getElementById('per-class').innerHTML =
    Object.entries(d.top_per_asset_class).map(([ac_name, etfs]) => classCard(ac_name, etfs)).join('');

  document.getElementById('per-issuer').innerHTML =
    Object.entries(d.top_per_issuer).map(([iss_name, etfs]) => `<div class="border border-slate-200 rounded-lg p-3 bg-slate-50">
      <div class="text-xs font-bold uppercase mb-2"><a href="/issuers/${slugify(iss_name)}" style="color:${issColor(iss_name)}" class="hover:underline">${iss_name}</a></div>
      ${etfs.map((e, i) => `<div class="flex items-center gap-2 py-1 ${i < etfs.length - 1 ? 'border-b border-slate-100' : ''}">
        <span class="text-slate-500 text-xs w-3 tabular-nums">${i + 1}</span>
        <span class="font-bold text-green-600 text-xs w-10">${e.code}</span>
        <span class="flex-1 text-xs text-slate-400 truncate" title="${e.name}">${e.name}</span>
        <span class="text-xs font-semibold text-slate-700 ml-1 tabular-nums">${fmtFum(e.fum)}</span>
      </div>`).join('')}
    </div>`).join('');

  document.getElementById('loading').classList.add('hidden');
  document.getElementById('page').classList.remove('hidden');
}
"""

PAGE_FUM = _page(
    'Market Size',
    'Total market AUM, by asset class, by issuer, and largest funds',
    _FUM_BODY,
    _FUM_JS,
)


# ---------------------------------------------------------------------------
# LISTINGS PAGE
# ---------------------------------------------------------------------------
_LISTINGS_BODY = """
<div id="hero" class="grid grid-cols-2 sm:grid-cols-4 gap-4"></div>

<!-- Year chart with filters -->
<div class="card">
  <div class="flex flex-wrap items-center justify-between gap-3 mb-4">
    <h2 class="font-semibold text-sm text-slate-700">ETFs Listed by Year</h2>
    <div id="dim-tabs" class="flex flex-wrap gap-1">
      <button data-dim="all"          class="dim-btn active-dim">All</button>
      <button data-dim="exchange"     class="dim-btn">Exchange</button>
      <button data-dim="asset_class"  class="dim-btn">Asset Class</button>
      <button data-dim="fund_type"    class="dim-btn">Style</button>
      <button data-dim="issuer"       class="dim-btn">Issuer</button>
    </div>
  </div>
  <div class="relative" style="height:300px"><canvas id="chart-years"></canvas></div>
</div>

<div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-3">Most Recent Listings</h2>
    <div class="overflow-x-auto"><table>
      <thead><tr><th>Code</th><th>Name</th><th>Issuer</th><th>Inception</th></tr></thead>
      <tbody id="tbl-recent"></tbody>
    </table></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-1">Longest Running ETFs</h2>
    <p class="text-xs text-slate-500 mb-3">STW and SFY were Australia's first ETFs, both listed on 27 August 2001.</p>
    <div class="overflow-x-auto"><table>
      <thead><tr><th>Code</th><th>Name</th><th>Issuer</th><th>Inception</th><th></th></tr></thead>
      <tbody id="tbl-oldest"></tbody>
    </table></div>
  </div>
</div>

<div class="grid grid-cols-1 lg:grid-cols-4 gap-5">
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-4">By Issuer</h2>
    <div id="bars-issuer"></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-4">By Asset Class</h2>
    <div id="bars-asset"></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-4">By Exchange</h2>
    <div id="bars-exchange"></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-4">By Investment Style</h2>
    <div id="bars-fundtype"></div>
  </div>
</div>

<!-- Upcoming & recent new issues -->
<div class="card">
  <div class="flex items-center justify-between mb-4">
    <h2 class="font-semibold text-sm text-slate-700">Upcoming & Recent Issues</h2>
    <a href="/insights/upcoming" class="text-xs text-blue-400 hover:text-blue-300">Full new listings view ›</a>
  </div>
  <div id="listings-upcoming" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3 mb-4">
    <p class="text-sm text-slate-500">Loading…</p>
  </div>
  <h3 class="font-semibold text-xs text-slate-400 uppercase tracking-wider mb-3 mt-2">Recently Listed (30 days)</h3>
  <div class="overflow-x-auto">
    <table>
      <thead><tr><th>Code</th><th>Fund Name</th><th>Issuer</th><th>Asset Class</th><th>Listed</th><th style="text-align:right">MER</th></tr></thead>
      <tbody id="listings-recent-new"></tbody>
    </table>
  </div>
</div>
"""

_LISTINGS_PALETTE = """
const PALETTES = {
  exchange:   {'ASX':'#3b82f6','CXA':'#8b5cf6','Unknown':'#94a3b8'},
  fund_type:  {'ETF':'#3b82f6','Active':'#f59e0b','Complex':'#ef4444','SP':'#10b981','Index':'#6366f1'},
  asset_class:{'Equities':'#3b82f6','Fixed Income':'#10b981','Multi-Asset':'#f59e0b','Commodities':'#f97316',
               'Currency':'#8b5cf6','Cash':'#06b6d4','Property':'#ec4899','Alternatives':'#6b7280','Other':'#94a3b8'},
};
const FALLBACK_COLORS = ['#3b82f6','#8b5cf6','#f59e0b','#10b981','#ef4444','#f97316','#06b6d4','#ec4899','#6366f1','#84cc16','#a855f7','#14b8a6'];
function dimColor(dim, val, i) {
  if (dim === 'issuer') return issColor(val);
  if (dim === 'asset_class') return acColor(val);
  return (PALETTES[dim] && PALETTES[dim][val]) ? PALETTES[dim][val] : FALLBACK_COLORS[i % FALLBACK_COLORS.length];
}
"""

_LISTINGS_JS = _LISTINGS_PALETTE + """
let _data = null;
let _yearChart = null;
let _activeDim = 'all';

async function init() {
  _data = await api('/api/v1/insights/listings');
  document.getElementById('ts').textContent = new Date().toLocaleTimeString('en-AU');

  // Toggle button handlers
  document.getElementById('dim-tabs').addEventListener('click', e => {
    const btn = e.target.closest('.dim-btn');
    if (!btn) return;
    document.querySelectorAll('.dim-btn').forEach(b => b.classList.remove('active-dim'));
    btn.classList.add('active-dim');
    _activeDim = btn.dataset.dim;
    renderYearChart();
  });

  const allYearCounts = {};
  _data.etf_list.forEach(e => { allYearCounts[e.year] = (allYearCounts[e.year]||0) + 1; });
  const peakYear = Object.entries(allYearCounts).reduce((a,b) => b[1]>a[1] ? b : a, ['—',0]);
  const oldest = _data.oldest[0];

  document.getElementById('hero').innerHTML = [
    ['Total ETFs', _data.total, 'ASX + CXA'],
    ['Newest', _data.recent[0]?.code || '—', _data.recent[0]?.inception_date || '—'],
    ['Oldest', oldest?.code || '—', oldest?.inception_date || '—'],
    ['Most Active Year', peakYear[0], peakYear[1] + ' new listings'],
  ].map(([l,v,s]) => `<div class="card"><div class="sl">${l}</div><div class="sv">${v}</div><div class="ss">${s}</div></div>`).join('');

  // Show the page before rendering the chart so the canvas has real dimensions
  document.getElementById('loading').classList.add('hidden');
  document.getElementById('page').classList.remove('hidden');

  renderYearChart();
  renderTables();
  renderBars();

  // Fetch upcoming data for the new section
  try {
    const up = await api('/api/v1/insights/upcoming');
    const upcoming = (up.upcoming || []).slice(0, 6);
    const upEl = document.getElementById('listings-upcoming');
    if (upcoming.length) {
      upEl.innerHTML = upcoming.map(u => `
        <div class="border border-slate-200 rounded-lg p-3 bg-slate-50">
          <div class="flex items-center justify-between mb-1">
            <span class="font-bold text-blue-400 text-sm">${u.code || '—'}</span>
            <span class="text-xs text-indigo-400 font-semibold">Coming Soon</span>
          </div>
          <div class="text-xs text-slate-700 font-medium leading-snug mb-1">${u.name || '—'}</div>
          <div class="text-xs text-slate-500">${u.issuer ? `<a href="/issuers/${slugify(u.issuer)}" style="color:inherit" class="hover:underline">${u.issuer}</a>` : ''}</div>
          ${u.expected_date ? `<div class="text-xs text-slate-500 mt-1">Expected: ${u.expected_date}</div>` : ''}
        </div>`).join('');
    } else {
      upEl.innerHTML = '<p class="text-sm text-slate-500 col-span-3">No upcoming listings at this time.</p>';
    }
    // Recently listed (last 30 days)
    const recent30 = (up.recent || []).filter(r => {
      if (!r.inception_date) return false;
      const d = new Date(r.inception_date);
      return (Date.now() - d.getTime()) < 30 * 86400 * 1000;
    }).slice(0, 15);
    document.getElementById('listings-recent-new').innerHTML = recent30.map(r => `<tr>
      <td class="font-bold text-blue-400">${r.code}</td>
      <td class="text-slate-700" style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${r.name||'—'}</td>
      <td class="text-slate-400 text-xs">${r.issuer ? `<a href="/issuers/${slugify(r.issuer)}" style="color:#475569" class="hover:underline">${r.issuer}</a>` : '—'}</td>
      <td class="text-slate-400 text-xs">${r.asset_class||'—'}</td>
      <td class="text-slate-500 tabular-nums text-xs">${r.inception_date||'—'}</td>
      <td class="text-right text-slate-400 tabular-nums text-xs">${r.management_fee ? r.management_fee.toFixed(2)+'%' : '—'}</td>
    </tr>`).join('') || '<tr><td colspan="6" class="text-center text-slate-500 py-4">No listings in last 30 days</td></tr>';
  } catch(_) {}
}

function renderYearChart() {
  const dim = _activeDim;
  const etfs = _data.etf_list;
  const allYears = [...new Set(etfs.map(e => e.year))].filter(y => +y >= 2001).sort();

  let datasets;
  if (dim === 'all') {
    const counts = {};
    etfs.forEach(e => { counts[e.year] = (counts[e.year]||0) + 1; });
    datasets = [{
      label: 'All ETFs',
      data: allYears.map(y => counts[y]||0),
      backgroundColor: '#3b82f6',
      borderRadius: 3,
      borderSkipped: false,
    }];
  } else {
    // For issuer, only show top 10 by total count to keep chart readable
    let groups = [...new Set(etfs.map(e => e[dim]))].sort();
    if (dim === 'issuer') {
      const totals = {};
      etfs.forEach(e => { totals[e[dim]] = (totals[e[dim]]||0) + 1; });
      groups = Object.entries(totals).sort((a,b) => b[1]-a[1]).slice(0, 10).map(([g]) => g);
      // Everything else → "Other"
      const topSet = new Set(groups);
      const hasOther = etfs.some(e => !topSet.has(e[dim]));
      if (hasOther) groups.push('Other');
    }
    datasets = groups.map((g, i) => {
      const counts = {};
      const subset = (dim === 'issuer' && g === 'Other')
        ? etfs.filter(e => !new Set(groups.slice(0,-1)).has(e[dim]))
        : etfs.filter(e => e[dim] === g);
      subset.forEach(e => { counts[e.year] = (counts[e.year]||0) + 1; });
      return {
        label: g,
        data: allYears.map(y => counts[y]||0),
        backgroundColor: dimColor(dim, g, i),
        borderSkipped: false,
      };
    });
  }

  const ctx = document.getElementById('chart-years').getContext('2d');
  if (_yearChart) _yearChart.destroy();
  _yearChart = new Chart(ctx, {
    type: 'bar',
    data: { labels: allYears, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: {
          display: dim !== 'all',
          position: 'bottom',
          labels: { font: { size: 10 }, boxWidth: 12, padding: 10 },
        },
        tooltip: {
          mode: 'index', intersect: false,
          callbacks: { label: ctx => ` ${ctx.dataset.label}: ${ctx.parsed.y}` },
        },
      },
      scales: {
        x: { stacked: true, ticks: { font: { size: 10 } }, grid: { display: false } },
        y: { stacked: true, ticks: { font: { size: 10 }, stepSize: 5 }, grid: { color: '#f1f5f9' } },
      },
    },
  });
}

function renderTables() {
  // Annotations for notable ETFs
  const NOTES = {
    STW: { label: "Australia's first ETF", cls: 'bg-green-50 text-green-700' },
    SFY: { label: "Australia's first ETF", cls: 'bg-green-50 text-green-700' },
    PMGOLD: { label: 'Date as per ASX report', cls: 'bg-amber-50 text-amber-700' },
  };

  const etfRow = (e) => {
    const note = NOTES[e.code];
    const badge = note ? `<span class="badge ${note.cls}">${note.label}</span>` : '';
    return `<tr>
      <td class="font-bold text-green-600">${e.code}</td>
      <td class="text-slate-700" style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${e.name||''}">${e.name||'—'}</td>
      <td class="text-slate-400">${e.issuer||'—'}</td>
      <td class="text-slate-500 tabular-nums text-xs">${e.inception_date||'—'}</td>
      <td>${badge}</td>
    </tr>`;
  };

  document.getElementById('tbl-recent').innerHTML = _data.recent.map(etfRow).join('');
  document.getElementById('tbl-oldest').innerHTML = _data.oldest.map(etfRow).join('');
}

function renderBars() {
  const issMax = _data.by_issuer[0]?.count || 1;
  document.getElementById('bars-issuer').innerHTML = _data.by_issuer.slice(0, 15)
    .map(r => hbar(r.issuer, r.count, issMax, issColor(r.issuer), r.count + ' ETFs', '')).join('');

  const acMax = _data.by_asset_class[0]?.count || 1;
  document.getElementById('bars-asset').innerHTML = _data.by_asset_class
    .map(r => hbar(r.asset_class, r.count, acMax, acColor(r.asset_class), r.count + ' ETFs', '')).join('');

  const exMax = _data.by_exchange[0]?.count || 1;
  document.getElementById('bars-exchange').innerHTML = _data.by_exchange
    .map(r => hbar(r.exchange||'Unknown', r.count, exMax, dimColor('exchange', r.exchange, 0), r.count + ' ETFs', '')).join('');

  const ftMax = _data.by_fund_type[0]?.count || 1;
  document.getElementById('bars-fundtype').innerHTML = _data.by_fund_type
    .map((r,i) => hbar(r.fund_type, r.count, ftMax, dimColor('fund_type', r.fund_type, i), r.count + ' ETFs', '')).join('');
}
"""

PAGE_LISTINGS = _page(
    'ETF Listings',
    'Historical listings, recent additions, and upcoming ETFs',
    _LISTINGS_BODY,
    _LISTINGS_JS,
)


# ---------------------------------------------------------------------------
# RETURNS PAGE
# ---------------------------------------------------------------------------
_RETURNS_BODY = """
<div id="hero" class="grid grid-cols-2 sm:grid-cols-4 gap-4"></div>
<div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-4">1Y Return Distribution</h2>
    <div class="relative" style="height:220px"><canvas id="chart-dist"></canvas></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-4">Average 1Y Return — by Asset Class</h2>
    <div id="bars-ac"></div>
  </div>
</div>
<div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-3">Top Performers — 1Y Return</h2>
    <div class="overflow-x-auto"><table>
      <thead><tr><th>#</th><th>Code</th><th>Name</th><th>Issuer</th><th class="text-right">1Y</th><th class="text-right">3Y</th><th class="text-right">5Y</th><th class="text-right">FUM</th></tr></thead>
      <tbody id="tbl-top"></tbody>
    </table></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-3">Worst Performers — 1Y Return</h2>
    <div class="overflow-x-auto"><table>
      <thead><tr><th>#</th><th>Code</th><th>Name</th><th>Issuer</th><th class="text-right">1Y</th><th class="text-right">3Y</th><th class="text-right">5Y</th><th class="text-right">FUM</th></tr></thead>
      <tbody id="tbl-bot"></tbody>
    </table></div>
  </div>
</div>
<div class="card">
  <h2 class="font-semibold text-sm text-slate-700 mb-4">Average 1Y Return — by Issuer</h2>
  <div id="bars-issuer"></div>
</div>
"""

_RETURNS_JS = """
async function init() {
  const d = await api('/api/v1/insights/returns');
  document.getElementById('ts').textContent = new Date().toLocaleTimeString('en-AU');

  const best = d.top_performers[0];
  const worst = d.bottom_performers[0];
  document.getElementById('hero').innerHTML = [
    ['Market Avg 1Y Return', pct(d.avg_1y), 'across all ETFs'],
    ['Best Performer', best?.code || '—', best ? pct(best.return_1y) + ' 1Y' : ''],
    ['Worst Performer', worst?.code || '—', worst ? pct(worst.return_1y) + ' 1Y' : ''],
    ['ETFs with Return Data', d.top_performers.length + d.bottom_performers.length + '+', 'have 1Y history'],
  ].map(([l, v, s]) => `<div class="card"><div class="sl">${l}</div><div class="sv ${pcls(parseFloat(v))}">${v}</div><div class="ss">${s}</div></div>`).join('');

  // Distribution histogram
  const dist = d.distribution;
  const distColors = dist.map(b => {
    if (b.bucket.startsWith('<') || b.bucket.startsWith('-')) return 'rgba(239,68,68,.7)';
    if (b.bucket === '0 to 5%') return 'rgba(156,163,175,.7)';
    return 'rgba(59,130,246,.7)';
  });
  new Chart(document.getElementById('chart-dist').getContext('2d'), {
    type: 'bar',
    data: {
      labels: dist.map(b => b.bucket),
      datasets: [{ data: dist.map(b => b.count), backgroundColor: distColors, borderRadius: 3, borderSkipped: false }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: ctx => ' ' + ctx.parsed.y + ' ETFs' } } },
      scales: { x: { ticks: { font: { size: 9 }, maxRotation: 45 } }, y: { ticks: { font: { size: 10 }, stepSize: 5 }, grid: { color: '#f1f5f9' } } },
    },
  });

  // Asset class bars
  const acMax = Math.max(...d.by_asset_class.map(r => Math.abs(r.avg_1y || 0)));
  document.getElementById('bars-ac').innerHTML = d.by_asset_class
    .map(r => `<div class="hbar-row">
      <span class="hbar-label" title="${r.asset_class}">${r.asset_class}</span>
      <div class="hbar-track"><div class="hbar-fill" style="width:${r.avg_1y != null ? Math.min(Math.abs(r.avg_1y) / acMax * 100, 100).toFixed(1) : 0}%;background:${r.avg_1y >= 0 ? acColor(r.asset_class) : '#ef4444'}"></div></div>
      <span class="hbar-val ${pcls(r.avg_1y)}">${pct(r.avg_1y, 1)}</span>
      <span class="hbar-sub">${r.etf_count}e</span>
    </div>`)
    .join('');

  const perfRow = (e, i) => `<tr>
    <td class="text-slate-500 tabular-nums">${i + 1}</td>
    <td class="font-bold text-green-600">${e.code}</td>
    <td class="text-slate-700" style="max-width:170px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${e.name}">${e.name}</td>
    <td class="text-slate-400 text-xs">${issLink(e.issuer)}</td>
    <td class="text-right font-semibold ${pcls(e.return_1y)} tabular-nums">${pct(e.return_1y, 1)}</td>
    <td class="text-right text-slate-400 tabular-nums">${pct(e.return_3y, 1)}</td>
    <td class="text-right text-slate-400 tabular-nums">${pct(e.return_5y, 1)}</td>
    <td class="text-right text-slate-500 tabular-nums text-xs">${fmtFum(e.fund_size_aud_millions)}</td>
  </tr>`;
  document.getElementById('tbl-top').innerHTML = d.top_performers.map(perfRow).join('');
  document.getElementById('tbl-bot').innerHTML = d.bottom_performers.map(perfRow).join('');

  // Issuer bars
  const issMax = Math.max(...d.by_issuer.map(r => Math.abs(r.avg_1y || 0)));
  document.getElementById('bars-issuer').innerHTML =
    `<div class="grid grid-cols-1 md:grid-cols-2 gap-x-8">` +
    d.by_issuer.map(r => `<div class="hbar-row">
      <span class="hbar-label"><a href="/issuers/${slugify(r.issuer)}" style="color:inherit" class="hover:underline">${r.issuer}</a></span>
      <div class="hbar-track"><div class="hbar-fill" style="width:${r.avg_1y != null ? Math.min(Math.abs(r.avg_1y) / issMax * 100, 100).toFixed(1) : 0}%;background:${r.avg_1y >= 0 ? issColor(r.issuer) : '#ef4444'}"></div></div>
      <span class="hbar-val ${pcls(r.avg_1y)}">${pct(r.avg_1y, 1)}</span>
      <span class="hbar-sub">${r.etf_count}e</span>
    </div>`).join('') + `</div>`;

  document.getElementById('loading').classList.add('hidden');
  document.getElementById('page').classList.remove('hidden');
}
"""

PAGE_RETURNS = _page(
    'Performance',
    'Market returns by ETF, asset class, and issuer',
    _RETURNS_BODY,
    _RETURNS_JS,
)


# ---------------------------------------------------------------------------
# EXPENSE / COST PAGE
# ---------------------------------------------------------------------------
_EXPENSE_BODY = """
<div id="hero" class="grid grid-cols-2 sm:grid-cols-4 gap-4"></div>

<!-- Cost explainer -->
<div class="card">
  <h2 class="font-semibold text-sm text-slate-700 mb-3">Understanding ETF Costs</h2>
  <p class="text-xs text-slate-400 mb-4 leading-relaxed">
    The total cost of holding an ETF is made up of several components — some visible, some less so.
    Even small differences in fees compound significantly over time: a 0.50% annual cost difference
    on a $100,000 portfolio costs roughly $5,000 extra over 10 years, before any performance drag.
  </p>
  <div class="grid grid-cols-1 sm:grid-cols-3 gap-3">
    <div class="rounded-lg p-3 bg-slate-50 border border-slate-200">
      <div class="text-xs font-bold uppercase text-blue-400 mb-1.5">Management Expense Ratio (MER)</div>
      <p class="text-xs text-slate-400 leading-relaxed">
        The annual fee charged by the fund manager, expressed as a percentage of your investment. It covers
        portfolio management, custody, administration, and legal costs. Deducted daily from the fund's NAV —
        you never see a bill, but the return is reduced by this amount every year. <span class="text-slate-700">This is the most
        commonly quoted cost.</span>
      </p>
    </div>
    <div class="rounded-lg p-3 bg-slate-50 border border-slate-200">
      <div class="text-xs font-bold uppercase text-amber-400 mb-1.5">Bid/Ask Spread</div>
      <p class="text-xs text-slate-400 leading-relaxed">
        When you buy an ETF on the ASX, you pay the <em>ask</em> (offer) price; when you sell, you receive
        the <em>bid</em> price. The gap between these — the spread — is a per-transaction cost paid to
        market makers for providing liquidity. <span class="text-slate-700">Spreads are larger for illiquid or thinly traded ETFs</span>
        and matter more for frequent traders than long-term holders.
      </p>
    </div>
    <div class="rounded-lg p-3 bg-slate-50 border border-slate-200">
      <div class="text-xs font-bold uppercase text-emerald-400 mb-1.5">Premium / Discount to NAV</div>
      <p class="text-xs text-slate-400 leading-relaxed">
        An ETF's market price can diverge from its underlying net asset value (NAV). If you buy at a premium,
        you pay more than the basket of assets is worth; a discount works in your favour. For most large,
        liquid ETFs premiums and discounts are tiny (under 0.10%). <span class="text-slate-700">They can be larger for
        ETFs holding illiquid assets</span> (e.g. fixed income, small caps) or those tracking overseas markets
        outside trading hours.
      </p>
    </div>
  </div>
  <p class="text-xs text-slate-500 mt-3 leading-relaxed">
    <span class="text-slate-400 font-medium">Other costs to be aware of:</span>
    brokerage commissions charged by your broker (not the ETF), buy/sell spread on some unlisted unit
    classes, and potential capital gains tax drag from portfolio rebalancing inside the fund (more
    relevant for actively managed ETFs).
  </p>
</div>

<!-- Cost ranking (moved above distribution charts) -->
<div class="card">
  <div class="flex flex-wrap items-center justify-between gap-3 mb-3">
    <h2 class="font-semibold text-sm text-slate-700">ETF Cost Ranking — Top 20</h2>
    <div class="flex flex-wrap gap-2">
      <div class="flex rounded-lg overflow-hidden border border-slate-200 text-xs font-medium">
        <button id="rank-cheapest" onclick="setRankDir('cheapest')"
          class="px-3 py-1.5 bg-blue-600 text-white transition-colors">Cheapest</button>
        <button id="rank-priciest" onclick="setRankDir('priciest')"
          class="px-3 py-1.5 text-slate-400 hover:text-slate-900 transition-colors">Most Expensive</button>
      </div>
      <div class="flex rounded-lg overflow-hidden border border-slate-200 text-xs font-medium">
        <button id="metric-mer" onclick="setMetric('mer')"
          class="px-3 py-1.5 bg-blue-600 text-white transition-colors">MER</button>
        <button id="metric-spread" onclick="setMetric('spread')"
          class="px-3 py-1.5 text-slate-400 hover:text-slate-900 transition-colors">Bid/Ask Spread</button>
        <button id="metric-combined" onclick="setMetric('combined')"
          class="px-3 py-1.5 text-slate-400 hover:text-slate-900 transition-colors">MER + Spread</button>
      </div>
    </div>
  </div>
  <!-- Filters row -->
  <div class="flex flex-wrap items-center gap-2 mb-3">
    <span class="text-xs text-slate-500 shrink-0">Asset class:</span>
    <div id="ac-pills" class="flex flex-wrap gap-1"></div>
    <span class="text-xs text-slate-500 shrink-0 ml-2">Issuer:</span>
    <select id="issuer-select" onchange="setIssuer(this.value)"
      class="text-xs bg-white border border-slate-200 text-slate-700 rounded px-2 py-1 focus:outline-none focus:border-blue-500">
      <option value="">All issuers</option>
    </select>
    <span id="rank-count" class="text-xs text-slate-500 ml-auto"></span>
  </div>
  <p id="rank-note" class="text-xs text-slate-500 mb-3"></p>
  <div class="relative" style="height:420px"><canvas id="chart-ranking"></canvas></div>
  <div class="overflow-x-auto mt-4">
    <table>
      <thead><tr>
        <th>#</th><th>Code</th><th>Name</th><th>Issuer</th>
        <th class="text-right">MER</th><th class="text-right">Bid/Ask</th>
        <th class="text-right" id="rank-col-hdr">Total</th>
        <th class="text-right">FUM</th>
      </tr></thead>
      <tbody id="tbl-ranking"></tbody>
    </table>
  </div>
</div>

<!-- NAV prem/disc summary -->
<div class="grid grid-cols-2 sm:grid-cols-4 gap-4" id="cost-hero"></div>
<div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
  <div class="card">
    <div class="flex items-center justify-between mb-3">
      <h2 class="font-semibold text-sm text-slate-700">Market Premium/Discount (90 days)</h2>
      <a href="/insights/nav" class="text-xs text-blue-400 hover:text-blue-300">Full NAV analysis ›</a>
    </div>
    <div class="relative" style="height:180px"><canvas id="cost-nav-chart"></canvas></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-3">Today's Spread Distribution</h2>
    <div class="relative" style="height:180px"><canvas id="cost-nav-dist"></canvas></div>
  </div>
</div>
<div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-4">MER Distribution</h2>
    <div class="relative" style="height:220px"><canvas id="chart-dist"></canvas></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-1">Average MER — by Asset Class</h2>
    <p class="text-xs text-slate-500 mb-3">Simple avg · FUM-weighted avg per class</p>
    <div id="bars-ac"></div>
  </div>
</div>

<div class="card">
  <h2 class="font-semibold text-sm text-slate-700 mb-1">MER by Issuer — Simple vs FUM-Weighted</h2>
  <p class="text-xs text-slate-500 mb-4">FUM-weighted MER reflects what investors actually pay on average, weighted by fund size.</p>
  <div class="overflow-x-auto">
    <table>
      <thead><tr>
        <th>Issuer</th><th class="text-right">ETFs</th><th class="text-right">Avg MER</th>
        <th class="text-right">FUM-Wtd MER</th><th class="text-right">Total FUM</th>
        <th style="min-width:140px">FUM-Weighted</th>
      </tr></thead>
      <tbody id="tbl-issuers"></tbody>
    </table>
  </div>
</div>
<div class="card">
  <h2 class="font-semibold text-sm text-slate-700 mb-4">FUM-Weighted MER — by Issuer</h2>
  <div class="relative" style="height:300px"><canvas id="chart-issuer"></canvas></div>
</div>
"""

_EXPENSE_JS = """
async function init() {
  const d = await api('/api/v1/insights/expense');
  document.getElementById('ts').textContent = new Date().toLocaleTimeString('en-AU');

  // Fetch NAV data for prem/disc section
  try {
    const nav = await api('/api/v1/insights/nav');
    const sm  = nav.summary || {};
    const avgPd = sm.avg_pd ?? 0;
    const total = sm.total || 1;
    const pctAtPremium = ((sm.at_premium || 0) / total * 100);
    const pdCls = avgPd >= 0 ? 'text-green-400' : 'text-red-400';
    document.getElementById('cost-hero').innerHTML = [
      ['Market Avg MER',  (d.avg_mer||0).toFixed(2)+'%',                           'simple average'],
      ['FUM-Wtd MER',     (d.fum_weighted_mer||0).toFixed(2)+'%',                  'cost per $1 invested'],
      ['Avg Prem/Disc',   (avgPd>=0?'+':'')+avgPd.toFixed(2)+'%',                  'vs NAV · ' + nav.snapshot_date],
      ['ETFs at Premium', pctAtPremium.toFixed(0)+'%',                             sm.at_premium + ' of ' + total + ' ETFs'],
    ].map(([l,v,s]) => `<div class="card"><div class="sl">${l}</div><div class="sv ${l.includes('Prem/Disc') ? pdCls : ''}">${v}</div><div class="ss">${s}</div></div>`).join('');

    // nav trend chart — market_history has {date, avg_pd, min_pd, max_pd}
    const hist = nav.market_history || [];
    const fmtD = s => { const d=new Date(s); return d.toLocaleDateString('en-AU',{day:'numeric',month:'short'}); };
    // Show every ~10th label to avoid crowding
    const step = Math.max(1, Math.floor(hist.length / 8));
    new Chart(document.getElementById('cost-nav-chart').getContext('2d'), {
      type:'line',
      data:{
        labels: hist.map(r=>r.date),
        datasets:[
          {label:'Avg Prem/Disc %', data:hist.map(r=>r.avg_pd), borderColor:'#3b82f6', backgroundColor:'#3b82f620', fill:true, tension:0.3, pointRadius:0, borderWidth:2},
          {label:'Max',  data:hist.map(r=>r.max_pd), borderColor:'#22c55e66', fill:false, pointRadius:0, borderWidth:1},
          {label:'Min',  data:hist.map(r=>r.min_pd), borderColor:'#ef444466', fill:false, pointRadius:0, borderWidth:1},
        ]
      },
      options:{
        responsive:true, maintainAspectRatio:false,
        plugins:{legend:{display:false}, tooltip:{callbacks:{label:c=>` ${c.dataset.label}: ${c.parsed.y>0?'+':''}${c.parsed.y.toFixed(3)}%`}}},
        scales:{
          x:{ticks:{font:{size:10}, maxTicksLimit:8, callback:(v,i)=> i%step===0 ? fmtD(hist[i]?.date||'') : null}, grid:{display:false}},
          y:{ticks:{font:{size:10},callback:v=>(v>0?'+':'')+v.toFixed(2)+'%'},grid:{color:'#f1f5f9'}},
        }
      },
    });

    // P/D distribution — bucket snapshot into ranges from the snapshot data
    const snap = nav.snapshot || [];
    const buckets = {'< -1%':0, '-1 to -0.5%':0, '-0.5 to -0.1%':0, '-0.1 to 0.1%':0, '0.1 to 0.5%':0, '0.5 to 1%':0, '> 1%':0};
    snap.forEach(r => {
      const v = r.premium_discount_pct;
      if (v == null) return;
      if (v < -1)          buckets['< -1%']++;
      else if (v < -0.5)   buckets['-1 to -0.5%']++;
      else if (v < -0.1)   buckets['-0.5 to -0.1%']++;
      else if (v <= 0.1)   buckets['-0.1 to 0.1%']++;
      else if (v <= 0.5)   buckets['0.1 to 0.5%']++;
      else if (v <= 1)     buckets['0.5 to 1%']++;
      else                 buckets['> 1%']++;
    });
    const bColors = ['#ef4444','#f97316','#fbbf24','#6366f1','#34d399','#22c55e','#16a34a'];
    new Chart(document.getElementById('cost-nav-dist').getContext('2d'), {
      type:'bar',
      data:{labels:Object.keys(buckets),datasets:[{label:'ETFs',data:Object.values(buckets),backgroundColor:bColors,borderRadius:3}]},
      options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>` ${c.parsed.y} ETFs`}}},scales:{x:{ticks:{font:{size:9}}},y:{ticks:{font:{size:10},stepSize:5},grid:{color:'#f1f5f9'}}}},
    });
  } catch(e) { console.warn('NAV section error:', e); }

  const cheapest = (d.all_etfs || []).slice().sort((a,b) => a.effective_mer - b.effective_mer)[0];
  const priciest = (d.all_etfs || []).slice().sort((a,b) => b.effective_mer - a.effective_mer)[0];
  document.getElementById('hero').innerHTML = [
    ['Market Simple Avg MER', mer(d.avg_mer), 'equal-weighted average'],
    ['FUM-Weighted Avg MER', mer(d.fum_weighted_mer), 'weighted by fund size'],
    ['Cheapest ETF', cheapest?.code || '—', mer(cheapest?.effective_mer)],
    ['Most Expensive', priciest?.code || '—', mer(priciest?.effective_mer)],
  ].map(([l, v, s]) => `<div class="card"><div class="sl">${l}</div><div class="sv">${v}</div><div class="ss">${s}</div></div>`).join('');

  // MER distribution histogram
  new Chart(document.getElementById('chart-dist').getContext('2d'), {
    type: 'bar',
    data: {
      labels: d.distribution.map(b => b.bucket),
      datasets: [{ data: d.distribution.map(b => b.count), backgroundColor: '#10b981', borderRadius: 3, borderSkipped: false }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: ctx => ' ' + ctx.parsed.y + ' ETFs' } } },
      scales: { x: { ticks: { font: { size: 10 }, maxRotation: 30 } }, y: { ticks: { font: { size: 10 }, stepSize: 10 }, grid: { color: '#f1f5f9' } } },
    },
  });

  // Asset class bars (show both simple and FUM-weighted)
  const acMax = Math.max(...d.by_asset_class.map(r => r.fum_weighted_mer || r.avg_mer || 0));
  document.getElementById('bars-ac').innerHTML = d.by_asset_class.map(r => `
    <div class="mb-3">
      <div class="flex items-center justify-between mb-0.5">
        <span class="text-xs font-medium text-slate-700" style="min-width:150px">${r.asset_class}</span>
        <span class="text-xs text-slate-500 tabular-nums">${mer(r.avg_mer)} avg · <span class="font-semibold text-slate-700">${mer(r.fum_weighted_mer)}</span> wtd</span>
      </div>
      <div class="flex gap-1">
        <div class="bar-track flex-1" title="Simple avg: ${mer(r.avg_mer)}">
          <div class="bar-fill" style="width:${acMax > 0 ? (r.avg_mer/acMax*100).toFixed(1) : 0}%;background:${acColor(r.asset_class)}88"></div>
        </div>
        <div class="bar-track flex-1" title="FUM-weighted: ${mer(r.fum_weighted_mer)}">
          <div class="bar-fill" style="width:${acMax > 0 ? ((r.fum_weighted_mer||0)/acMax*100).toFixed(1) : 0}%;background:${acColor(r.asset_class)}"></div>
        </div>
      </div>
    </div>`).join('');

  // ── Interactive cost ranking chart ──────────────────────────────────────
  window._rs = { metric:'mer', dir:'cheapest', ac:'', issuer:'', chart:null, all: d.all_etfs || [] };

  const _rankNotes = {
    mer:      'Ranked by Management Expense Ratio (MER) only.',
    spread:   'Ranked by live bid/ask spread — a per-transaction cost that varies with liquidity.',
    combined: 'MER + bid/ask spread stacked. Tooltip shows total on hover.',
  };

  // Build asset-class pills
  const allAC = [...new Set(window._rs.all.map(e => e.asset_class).filter(Boolean))].sort();
  document.getElementById('ac-pills').innerHTML =
    ['', ...allAC].map(ac => `<button
      id="ac-pill-${ac.replace(/\s+/g,'_')}"
      onclick='setAC(${JSON.stringify(ac)})'
      class="px-2 py-0.5 rounded text-xs border transition-colors ${ac==='' ? 'bg-blue-600 text-white border-blue-600' : 'border-slate-200 text-slate-500 hover:text-slate-900 bg-white'}"
    >${ac || 'All'}</button>`).join('');

  function _filteredETFs() {
    const { ac, issuer, all } = window._rs;
    return all.filter(e =>
      (!ac     || e.asset_class === ac) &&
      (!issuer || e.issuer === issuer)
    );
  }

  function _refreshIssuerDropdown() {
    const { ac, issuer } = window._rs;
    const issuers = [...new Set(_filteredETFs().map(e=>e.issuer).filter(Boolean))].sort();
    const sel = document.getElementById('issuer-select');
    const cur = sel.value;
    sel.innerHTML = '<option value="">All issuers</option>' +
      issuers.map(s => `<option value="${s}" ${s===cur?'selected':''}>${s}</option>`).join('');
    // if previously selected issuer no longer valid, reset
    if (cur && !issuers.includes(cur)) { window._rs.issuer = ''; sel.value = ''; }
  }

  window.renderRanking = function() {
    const { metric, dir } = window._rs;
    const pool = _filteredETFs();

    const sortKey = metric === 'spread'   ? e => e.spread
                  : metric === 'combined' ? e => (e.effective_mer||0) + (e.spread||0)
                  :                        e => e.effective_mer||0;
    const sorted = [...pool].sort((a,b) => dir==='cheapest' ? sortKey(a)-sortKey(b) : sortKey(b)-sortKey(a));
    const rows   = sorted.slice(0, 20);

    document.getElementById('rank-note').textContent = _rankNotes[metric];
    document.getElementById('rank-count').textContent =
      pool.length < window._rs.all.length ? `${pool.length} ETFs match filters` : '';

    const showTotal = metric === 'combined';
    document.getElementById('rank-col-hdr').style.display = showTotal ? '' : 'none';

    const merVals = rows.map(r => +(r.effective_mer||0).toFixed(4));
    const sprVals = rows.map(r => +(r.spread||0).toFixed(4));

    const datasets = metric === 'combined'
      ? [
          { label:'MER',     data:merVals, backgroundColor:'#3b82f6cc', borderRadius:3 },
          { label:'Bid/Ask', data:sprVals, backgroundColor:'#f59e0bcc', borderRadius:3 },
        ]
      : metric === 'spread'
      ? [{ label:'Bid/Ask Spread', data:sprVals, backgroundColor:'#f59e0bcc', borderRadius:3 }]
      : [{ label:'MER',           data:merVals, backgroundColor:'#3b82f6cc', borderRadius:3 }];

    if (window._rs.chart) window._rs.chart.destroy();
    window._rs.chart = new Chart(document.getElementById('chart-ranking'), {
      type: 'bar',
      data: { labels: rows.map(r=>r.code), datasets },
      options: {
        indexAxis: 'y',
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { display: metric==='combined', position:'top', labels:{font:{size:10},boxWidth:10} },
          tooltip: {
            mode: 'index', axis: 'y',
            callbacks: {
              label: ctx => ` ${ctx.dataset.label}: ${ctx.parsed.x.toFixed(4)}%`,
              footer: ctxArr => {
                if (metric !== 'combined') return null;
                const total = ctxArr.reduce((s,c) => s + c.parsed.x, 0);
                return `Total cost: ${total.toFixed(4)}%`;
              },
            },
            footerFont: { weight:'bold' },
            footerColor: '#f8fafc',
          },
        },
        scales: {
          x: {
            stacked: metric==='combined',
            ticks: { font:{size:10}, callback: v => v.toFixed(2)+'%' },
            grid: { color:'#f1f5f9' },
            title: { display:true, text: metric==='spread' ? '% per transaction' : '% p.a.', font:{size:10}, color:'#64748b' },
          },
          y: { stacked: metric==='combined', ticks:{font:{size:11}}, grid:{display:false} },
        },
      },
    });

    document.getElementById('tbl-ranking').innerHTML = rows.map((e,i) => {
      const totalCell = showTotal
        ? `<td class="text-right tabular-nums font-semibold">${((e.effective_mer||0)+(e.spread||0)).toFixed(4)}%</td>`
        : `<td style="display:none"></td>`;
      return `<tr>
        <td class="text-slate-500 tabular-nums">${i+1}</td>
        <td><a href="/?code=${e.code}" class="font-bold text-blue-400 hover:underline">${e.code}</a></td>
        <td class="text-slate-700" style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${e.name}">${e.name}</td>
        <td class="text-slate-400 text-xs">${e.issuer||'—'}</td>
        <td class="text-right tabular-nums">${mer(e.effective_mer)}</td>
        <td class="text-right tabular-nums text-amber-400">${e.spread>0 ? e.spread.toFixed(4)+'%' : '—'}</td>
        ${totalCell}
        <td class="text-right text-slate-500 tabular-nums text-xs">${fmtFum(e.fund_size_aud_millions)}</td>
      </tr>`;
    }).join('');
  };

  window.setMetric = function(m) {
    window._rs.metric = m;
    ['mer','spread','combined'].forEach(x => {
      const b = document.getElementById('metric-'+x);
      b.classList.toggle('bg-blue-600',    x===m);
      b.classList.toggle('text-white',     x===m);
      b.classList.toggle('text-slate-400', x!==m);
    });
    window.renderRanking();
  };

  window.setRankDir = function(dir) {
    window._rs.dir = dir;
    ['cheapest','priciest'].forEach(x => {
      const b = document.getElementById('rank-'+x);
      b.classList.toggle('bg-blue-600',    x===dir);
      b.classList.toggle('text-white',     x===dir);
      b.classList.toggle('text-slate-400', x!==dir);
    });
    window.renderRanking();
  };

  window.setAC = function(ac) {
    window._rs.ac = ac;
    window._rs.issuer = '';
    document.getElementById('issuer-select').value = '';
    allAC.forEach(a => {
      const pill = document.getElementById('ac-pill-'+a.replace(/\s+/g,'_'));
      if (pill) {
        pill.classList.toggle('bg-blue-600',    a===ac);
        pill.classList.toggle('text-white',     a===ac);
        pill.classList.toggle('border-blue-600',a===ac);
        pill.classList.toggle('text-slate-400', a!==ac);
        pill.classList.toggle('border-slate-200',a!==ac);
      }
    });
    // "All" pill
    const allPill = document.getElementById('ac-pill-');
    if (allPill) {
      allPill.classList.toggle('bg-blue-600',     ac==='');
      allPill.classList.toggle('text-white',      ac==='');
      allPill.classList.toggle('border-blue-600', ac==='');
      allPill.classList.toggle('text-slate-400',  ac!=='');
      allPill.classList.toggle('border-slate-200',ac!=='');
    }
    _refreshIssuerDropdown();
    window.renderRanking();
  };

  window.setIssuer = function(issuer) {
    window._rs.issuer = issuer;
    window.renderRanking();
  };

  _refreshIssuerDropdown();
  window.renderRanking();

  // Issuer comparison table (sorted by FUM-weighted MER)
  const issSorted = [...d.by_issuer].sort((a, b) => (a.fum_weighted_mer || 99) - (b.fum_weighted_mer || 99));
  const fwMax = Math.max(...issSorted.map(r => r.fum_weighted_mer || 0));
  document.getElementById('tbl-issuers').innerHTML = issSorted.map(r => `<tr>
    <td class="font-semibold text-sm">${issLink(r.issuer)}</td>
    <td class="text-right tabular-nums text-slate-400">${r.etf_count}</td>
    <td class="text-right tabular-nums text-slate-400">${mer(r.avg_mer)}</td>
    <td class="text-right tabular-nums font-semibold">${mer(r.fum_weighted_mer)}</td>
    <td class="text-right tabular-nums text-slate-500 text-xs">${fmtFum(r.total_fum)}</td>
    <td>
      <div class="flex items-center gap-1">
        <div class="bar-track flex-1">
          <div class="bar-fill" style="width:${fwMax > 0 ? ((r.fum_weighted_mer||0)/fwMax*100).toFixed(1) : 0}%;background:${issColor(r.issuer)}"></div>
        </div>
        <span class="text-xs text-slate-500 tabular-nums w-10 text-right">${mer(r.fum_weighted_mer)}</span>
      </div>
    </td>
  </tr>`).join('');

  // FUM-weighted MER bar chart by issuer
  const chartIssuers = [...issSorted].filter(r => r.fum_weighted_mer);
  new Chart(document.getElementById('chart-issuer').getContext('2d'), {
    type: 'bar',
    data: {
      labels: chartIssuers.map(r => r.issuer),
      datasets: [
        { label: 'FUM-Weighted MER', data: chartIssuers.map(r => r.fum_weighted_mer), backgroundColor: chartIssuers.map(r => issColor(r.issuer)), borderRadius: 4, borderSkipped: false },
        { label: 'Simple Avg MER',   data: chartIssuers.map(r => r.avg_mer), backgroundColor: 'rgba(203,213,225,0.6)', borderRadius: 4, borderSkipped: false },
      ],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { position: 'top', labels: { font: { size: 10 }, boxWidth: 10 } },
        tooltip: { callbacks: { label: ctx => ' ' + ctx.dataset.label + ': ' + mer(ctx.parsed.y) } },
      },
      scales: {
        x: { ticks: { font: { size: 10 }, maxRotation: 40 } },
        y: { ticks: { font: { size: 10 }, callback: v => v.toFixed(2) + '%' }, grid: { color: '#f1f5f9' }, beginAtZero: true },
      },
    },
  });

  document.getElementById('loading').classList.add('hidden');
  document.getElementById('page').classList.remove('hidden');
}
"""

PAGE_EXPENSE = _page(
    'Costs & Efficiency',
    'Management fees, FUM-weighted MER, and trading spreads',
    _EXPENSE_BODY,
    _EXPENSE_JS,
)


# ---------------------------------------------------------------------------
# ISSUERS PAGE
# ---------------------------------------------------------------------------
_ISSUERS_BODY = """
<div id="hero" class="grid grid-cols-2 sm:grid-cols-4 gap-4"></div>

<!-- Issuer profiles -->
<div>
  <h2 class="font-semibold text-sm text-slate-700 mb-3">Who Are the Issuers?</h2>
  <div id="profiles" class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4"></div>
</div>

<!-- Comparison table (moved up) -->
<div class="card overflow-x-auto">
  <h2 class="font-semibold text-sm text-slate-700 mb-3">Issuer Comparison</h2>
  <table>
    <thead><tr>
      <th>#</th><th>Issuer</th><th class="text-right">FUM</th><th class="text-right">Mkt Share</th>
      <th class="text-right">ETFs</th><th class="text-right">Avg MER</th>
      <th class="text-right">Avg 1Y Ret</th><th>Largest ETF</th><th>Primary Category</th>
    </tr></thead>
    <tbody id="tbl-issuers"></tbody>
  </table>
</div>

<!-- Charts -->
<div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-4">Market Share by FUM</h2>
    <div class="relative" style="height:280px"><canvas id="chart-share"></canvas></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-4">ETF Count by Issuer</h2>
    <div id="bars-count"></div>
  </div>
</div>
<div class="card">
  <h2 class="font-semibold text-sm text-slate-700 mb-4">Asset Class Mix — by Issuer</h2>
  <div id="mix-chart" class="space-y-3"></div>
</div>
"""

_ISSUERS_JS = """
const ISSUER_PROFILES = {
  'Vanguard': {
    desc: "The world's second-largest asset manager, founded by index investing pioneer John Bogle. Vanguard's mutual ownership structure means profits flow back to fund investors as lower fees. In Australia since 1996, they dominate by FUM with a focused range of ultra-low-cost broad-market ETFs.",
    known: 'Ultra-low fees · Broad index funds · Long-term passive investing',
  },
  'BetaShares': {
    desc: "Australia's most prolific ETF issuer by product count, and the first Australian-founded ETF manager (est. 2010). Known for innovation — from currency ETFs and inverse products to the dominant Nasdaq ETF and Australia's largest cash ETF. Now part of the Global Indemnity Group.",
    known: 'Broadest range · Income & smart beta · Leveraged/inverse · AAA cash ETF',
  },
  'iShares': {
    desc: "iShares is BlackRock's ETF brand — BlackRock being the world's largest asset manager with over US$10 trillion AUM. iShares products are institutional in origin and carry deep liquidity. IVV (S&P 500) is Australia's second-largest ETF by FUM.",
    known: 'Institutional liquidity · S&P 500 (IVV) · ASX 200 (IOZ) · Global breadth',
  },
  'VanEck': {
    desc: "Dutch-origin specialist ETF manager with a strong emphasis on factor investing, fixed income, and sector strategies. Known for QUAL (quality factor) — one of Australia's largest single ETFs — and a comprehensive range of subordinated debt and hybrid credit products.",
    known: 'Quality factor (QUAL) · Fixed income depth · Smart beta · Sector ETFs',
  },
  'DFA': {
    desc: "Dimensional Fund Advisors applies academic research (Fama-French factor model) to build systematic, evidence-based equity portfolios. Only six ETFs in Australia but over A$18B in FUM — reflecting the loyalty of fee-based financial advisers who favour Dimensional's approach.",
    known: 'Evidence-based factor investing · Adviser-distributed · High FUM per product',
  },
  'Global X': {
    desc: "Part of Mirae Asset Global Investments. Global X pioneered thematic ETFs in Australia and manages Australia's largest physical gold ETF (GOLD, A$6.3B). Their range spans precious metals, energy transition, technology themes, and income-oriented covered-call strategies.",
    known: 'Physical gold (GOLD) · Thematic ETFs · Covered call income · Crypto',
  },
  'State Street Investment Management': {
    desc: "State Street Investment Management (SSIM), the asset management arm of State Street Corporation, manages Australia's SPDR-branded ETFs. STW was Australia's first ASX-listed ETF (2001). The local range is deliberately compact, focused on flagship index exposures for institutional and wholesale use.",
    known: "Australia's first ETF (STW) · Institutional-grade · ASX 200 · Global property",
  },
  'Magellan': {
    desc: "Australian active fund manager founded in 2006. Magellan built one of Australia's largest active equity franchises before a period of significant underperformance and management changes from 2021. Now managing a smaller, restructured range of global equity and infrastructure products.",
    known: 'Active global equities · Infrastructure · Airlie Australian shares (AASF)',
  },
  'Macquarie': {
    desc: "Macquarie Asset Management offers a range of active and multi-asset ETFs, including the popular Walter Scott Global Equity Active ETF (MQWS). Best known in ETF markets for active management with institutional-grade risk processes.",
    known: 'Active management · Walter Scott Global Equity (MQWS) · Multi-asset',
  },
  'Franklin Templeton': {
    desc: "US-headquartered global asset manager (est. 1947) with a broad ETF range covering fixed income, equities, and multi-asset strategies. Franklin Templeton entered the Australian ETF market to distribute established active strategies in an ETF wrapper.",
    known: 'Active fixed income · Global equities · Multi-asset strategies',
  },
  'Russell Investments': {
    desc: "Global investment manager known for multi-asset and diversified portfolio solutions. The Australian ETF range centres on diversified balanced funds and sector-specific exposures, popular with advisers building model portfolios.",
    known: 'Diversified portfolios · Model portfolio building blocks · Balanced funds',
  },
};

async function init() {
  const d = await api('/api/v1/insights/issuers');
  document.getElementById('ts').textContent = new Date().toLocaleTimeString('en-AU');

  const top = d.issuers[0];
  const avgMers = d.issuers.filter(r => r.avg_mer).map(r => r.avg_mer);
  const mktAvgMer = avgMers.length ? avgMers.reduce((a, b) => a + b, 0) / avgMers.length : null;

  document.getElementById('hero').innerHTML = [
    ['Total Issuers', d.issuers.length, 'active fund managers'],
    ['Largest Issuer', issLink(top?.issuer) || '—', fmtFum(top?.total_fum)],
    ['Top 3 Market Share', d.issuers.slice(0, 3).reduce((s, r) => s + (r.market_share_pct || 0), 0).toFixed(1) + '%', 'of total FUM'],
    ['Mkt Avg MER', mer(mktAvgMer), 'across all issuers'],
  ].map(([l, v, s]) => `<div class="card"><div class="sl">${l}</div><div class="sv text-base">${v}</div><div class="ss">${s}</div></div>`).join('');

  // Issuer profiles grid
  document.getElementById('profiles').innerHTML = d.issuers.map(r => {
    const profile = ISSUER_PROFILES[r.issuer];
    const primaryAC = Object.entries(r.asset_classes || {}).sort((a, b) => b[1] - a[1])[0]?.[0] || '—';
    const acEntries = Object.entries(r.asset_classes || {}).sort((a, b) => b[1] - a[1]).slice(0, 3);
    const acTotal = Object.values(r.asset_classes || {}).reduce((s, v) => s + v, 0) || 1;
    const acBar = acEntries.map(([ac, cnt]) =>
      `<div title="${ac}: ${cnt}" style="width:${(cnt/acTotal*100).toFixed(1)}%;background:${acColor(ac)};height:100%;display:inline-block;flex-shrink:0"></div>`
    ).join('');
    const topEtfs = r.top_etfs || (r.top_etf ? [r.top_etf] : []);
    const desc = profile?.desc || `${r.issuer} manages ${r.etf_count} ETF${r.etf_count !== 1 ? 's' : ''} listed on Australian exchanges, with ${fmtFum(r.total_fum)} in total funds under management.`;
    const known = profile?.known || primaryAC;
    return `<div class="card flex flex-col gap-3" style="border-top:3px solid ${issColor(r.issuer)}">
      <div class="flex items-start justify-between gap-2">
        <a href="/issuers/${slugify(r.issuer)}" class="font-bold text-base hover:underline" style="color:${issColor(r.issuer)}">${r.issuer}</a>
        <span class="text-xs font-semibold tabular-nums text-slate-700 shrink-0">${fmtFum(r.total_fum)}</span>
      </div>
      <p class="text-xs text-slate-400 leading-relaxed">${desc}</p>
      <div class="text-xs text-slate-500 italic">${known}</div>
      <div class="flex-1 flex flex-col justify-end gap-2">
        ${topEtfs.length ? `<div class="text-xs text-slate-500 font-medium uppercase tracking-wide">Top products</div>
        <div class="flex flex-col gap-1">${topEtfs.map(e => `
          <div class="flex items-center justify-between gap-2">
            <a href="/?code=${e.code}" class="font-bold text-blue-400 hover:underline text-xs">${e.code}</a>
            <span class="text-xs text-slate-400 truncate flex-1 mx-2" title="${e.name || ''}">${(e.name||'').replace(/^(BETASHARES|VANGUARD|ISHARES|VANECK|GLOBAL X|STATE STREET INVESTMENT MANAGEMENT|STATE STREET SPDR|STATE STREET|SPDR|DIMENSIONAL|MAGELLAN)\s+/i,'')}</span>
            <span class="text-xs tabular-nums text-slate-700 shrink-0">${fmtFum(e.fund_size_aud_millions)}</span>
          </div>`).join('')}
        </div>` : ''}
        <div class="h-3 rounded overflow-hidden bg-slate-200 flex mt-1">${acBar}</div>
        <div class="flex gap-1 flex-wrap">${acEntries.map(([ac]) =>
          `<span class="text-[10px] px-1.5 py-0.5 rounded" style="background:${acColor(ac)}22;color:${acColor(ac)}">${ac}</span>`
        ).join('')}</div>
      </div>
    </div>`;
  }).join('');

  // Comparison table
  document.getElementById('tbl-issuers').innerHTML = d.issuers.map((r, i) => {
    const primaryAC = Object.entries(r.asset_classes || {}).sort((a, b) => b[1] - a[1])[0]?.[0] || '—';
    return `<tr>
      <td class="text-slate-500 tabular-nums">${i + 1}</td>
      <td class="font-semibold">${issLink(r.issuer)}</td>
      <td class="text-right font-semibold tabular-nums">${fmtFum(r.total_fum)}</td>
      <td class="text-right tabular-nums">
        <div class="flex items-center justify-end gap-1">
          <div class="bar-track w-12"><div class="bar-fill" style="width:${r.market_share_pct}%;background:${issColor(r.issuer)}"></div></div>
          <span class="text-slate-400">${r.market_share_pct}%</span>
        </div>
      </td>
      <td class="text-right tabular-nums">${r.etf_count}</td>
      <td class="text-right tabular-nums">${mer(r.avg_mer)}</td>
      <td class="text-right font-semibold ${pcls(r.avg_return_1y)} tabular-nums">${pct(r.avg_return_1y, 1)}</td>
      <td class="text-xs">${r.top_etf ? `<a href="/?code=${r.top_etf.code}" class="font-bold text-blue-400 hover:underline">${r.top_etf.code}</a><span class="text-slate-500 ml-1">${fmtFum(r.top_etf.fund_size_aud_millions)}</span>` : '—'}</td>
      <td><span class="badge" style="background:${acColor(primaryAC)}22;color:${acColor(primaryAC)}">${primaryAC}</span></td>
    </tr>`;
  }).join('');

  // FUM doughnut
  const top10 = d.issuers.slice(0, 10);
  const others = d.issuers.slice(10).reduce((s, r) => s + (r.total_fum || 0), 0);
  const chartData = [...top10.map(r => r.total_fum), others > 0 ? others : null].filter(v => v);
  const chartLabels = [...top10.map(r => r.issuer), others > 0 ? 'Others' : null].filter(v => v);
  const chartColors = [...top10.map(r => issColor(r.issuer)), '#e2e8f0'];
  new Chart(document.getElementById('chart-share').getContext('2d'), {
    type: 'doughnut',
    data: { labels: chartLabels, datasets: [{ data: chartData, backgroundColor: chartColors, borderWidth: 2, borderColor: '#fff', hoverOffset: 6 }] },
    options: {
      responsive: true, maintainAspectRatio: false, cutout: '60%',
      plugins: {
        legend: { position: 'right', labels: { font: { size: 10 }, padding: 6, boxWidth: 10 } },
        tooltip: { callbacks: { label: ctx => ' ' + fmtFum(ctx.parsed) + '  (' + (top10[ctx.dataIndex]?.market_share_pct ?? '') + '%)' } },
      },
    },
  });

  // Count bars — issuer name is a link
  const cntMax = d.issuers[0]?.etf_count || 1;
  document.getElementById('bars-count').innerHTML = d.issuers.map(r => {
    const w = cntMax > 0 ? Math.min(r.etf_count / cntMax * 100, 100).toFixed(1) : 0;
    return `<div class="hbar-row">
      <span class="hbar-label">${issLink(r.issuer)}</span>
      <div class="hbar-track"><div class="hbar-fill" style="width:${w}%;background:${issColor(r.issuer)}"></div></div>
      <span class="hbar-val">${r.etf_count} ETFs</span>
    </div>`;
  }).join('');

  // Asset class mix stacked bars
  document.getElementById('mix-chart').innerHTML = d.issuers.slice(0, 15).map(r => {
    const total = Object.values(r.asset_classes || {}).reduce((s, v) => s + v, 0) || 1;
    const segments = Object.entries(r.asset_classes || {})
      .sort((a, b) => b[1] - a[1])
      .map(([ac, cnt]) => `<div title="${ac}: ${cnt}" style="width:${(cnt/total*100).toFixed(1)}%;background:${acColor(ac)};height:100%;display:inline-block"></div>`)
      .join('');
    return `<div class="flex items-center gap-3">
      <span class="text-xs font-medium text-slate-700 w-24 shrink-0 truncate">${issLink(r.issuer)}</span>
      <div class="flex-1 h-5 rounded overflow-hidden bg-slate-200 flex">${segments}</div>
      <span class="text-xs text-slate-500 w-12 text-right">${r.etf_count} ETFs</span>
    </div>`;
  }).join('');

  document.getElementById('loading').classList.add('hidden');
  document.getElementById('page').classList.remove('hidden');
}
"""

PAGE_ISSUERS = _page(
    'Issuer Analysis',
    'Market share, fund counts, and performance by ETF issuer',
    _ISSUERS_BODY,
    _ISSUERS_JS,
)


# ---------------------------------------------------------------------------
# NAV PREMIUM / DISCOUNT PAGE
# ---------------------------------------------------------------------------
_NAV_BODY = """
<!-- Hero stats -->
<div id="hero" class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-3"></div>

<!-- Market-wide 90-day premium/discount trend + distribution -->
<div class="grid grid-cols-1 lg:grid-cols-3 gap-5">
  <div class="card lg:col-span-2">
    <div class="flex items-center justify-between mb-3">
      <h2 class="font-semibold text-sm text-slate-700">Market-wide Premium/Discount — Last 90 Days</h2>
      <span class="text-xs text-slate-500">Average across all ETFs with NAV data</span>
    </div>
    <div class="relative" style="height:220px"><canvas id="chart-hist"></canvas></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-3">Today's Distribution</h2>
    <div class="relative" style="height:220px"><canvas id="chart-dist"></canvas></div>
  </div>
</div>

<!-- By asset class + by issuer -->
<div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-3">Avg Premium/Discount by Asset Class</h2>
    <div id="ac-bars"></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-3">Avg Premium/Discount by Issuer</h2>
    <div id="iss-bars"></div>
  </div>
</div>

<!-- Historical explorer for individual ETF -->
<div class="card">
  <div class="flex flex-wrap items-center gap-3 mb-4">
    <h2 class="font-semibold text-sm text-slate-700">Historical Premium/Discount Explorer</h2>
    <select id="etf-picker" class="border border-slate-200 rounded-lg px-2.5 py-1.5 text-sm bg-slate-50 text-slate-700 focus:ring-2 focus:ring-blue-300 outline-none min-w-[200px]">
      <option value="">— select an ETF —</option>
    </select>
    <select id="period-picker" class="border border-slate-200 rounded-lg px-2.5 py-1.5 text-sm bg-slate-50 text-slate-700 focus:ring-2 focus:ring-blue-300 outline-none">
      <option value="3m">3 months</option>
      <option value="1y" selected>1 year</option>
      <option value="3y">3 years</option>
      <option value="all">All time</option>
    </select>
    <span id="etf-stats" class="text-xs text-slate-500"></span>
  </div>
  <div class="relative" style="height:260px"><canvas id="chart-etf"></canvas></div>
</div>

<!-- Today's full snapshot table -->
<div class="card">
  <div class="flex items-center justify-between mb-3">
    <h2 class="font-semibold text-sm text-slate-700">Today's Snapshot — All ETFs</h2>
    <div class="flex items-center gap-2">
      <input id="snap-search" type="text" placeholder="Filter by code or name…"
             class="border border-slate-200 rounded-lg px-2.5 py-1.5 text-sm bg-slate-50 text-slate-700 focus:ring-2 focus:ring-blue-300 outline-none w-52">
      <select id="snap-sort" class="border border-slate-200 rounded-lg px-2.5 py-1.5 text-sm bg-slate-50 text-slate-700 focus:ring-2 focus:ring-blue-300 outline-none">
        <option value="pd-desc">Largest premium first</option>
        <option value="pd-asc">Largest discount first</option>
        <option value="fum-desc">FUM (largest first)</option>
        <option value="abs-desc">Largest deviation first</option>
      </select>
    </div>
  </div>
  <div class="overflow-x-auto">
    <table>
      <thead><tr>
        <th>Code</th><th>Fund Name</th><th>Issuer</th><th>Asset Class</th>
        <th style="text-align:right">NAV</th>
        <th style="text-align:right">Price</th>
        <th style="text-align:right">Prem/Disc</th>
        <th style="text-align:right">FUM</th>
      </tr></thead>
      <tbody id="snap-table"></tbody>
    </table>
  </div>
</div>
"""

_NAV_JS = """
let _data = null;
let _etfChart = null;

async function init() {
  _data = await api('/api/v1/insights/nav');

  document.getElementById('ts').textContent = 'Snapshot: ' + _data.snapshot_date;
  document.getElementById('loading').classList.add('hidden');
  document.getElementById('page').classList.remove('hidden');

  renderHero();
  renderHistChart();
  renderDistChart();
  renderAcBars();
  renderIssBars();
  populateEtfPicker();
  renderSnapTable();

  document.getElementById('etf-picker').addEventListener('change', loadEtfHistory);
  document.getElementById('period-picker').addEventListener('change', loadEtfHistory);

  let snapTimer;
  document.getElementById('snap-search').addEventListener('input', () => {
    clearTimeout(snapTimer);
    snapTimer = setTimeout(renderSnapTable, 250);
  });
  document.getElementById('snap-sort').addEventListener('change', renderSnapTable);
}

function fmtPd(v, decimals) {
  if (v == null) return '—';
  const d = decimals != null ? decimals : 3;
  return (v >= 0 ? '+' : '') + v.toFixed(d) + '%';
}
function pdCls(v) {
  if (v == null) return '';
  if (v > 0.05) return 'pos';
  if (v < -0.05) return 'neg';
  return 'text-slate-400';
}
function pdBar(v, maxAbs) {
  if (v == null) return '';
  const pct = maxAbs > 0 ? Math.min(Math.abs(v) / maxAbs * 100, 100).toFixed(1) : 0;
  const col = v > 0 ? '#16a34a' : '#dc2626';
  const dir = v > 0 ? 'left' : 'right';
  // Centred bar: positive goes right from centre, negative goes left
  if (v >= 0) {
    return `<div style="display:flex;align-items:center;gap:4px;justify-content:flex-end">
      <span class="${pdCls(v)}" style="font-size:.75rem;font-weight:600;min-width:60px;text-align:right">${fmtPd(v)}</span>
      <div style="width:60px;height:8px;background:#e2e8f0;border-radius:4px;overflow:hidden">
        <div style="width:${pct}%;height:100%;background:${col};border-radius:4px;margin-left:0"></div>
      </div>
    </div>`;
  } else {
    return `<div style="display:flex;align-items:center;gap:4px;justify-content:flex-end">
      <span class="${pdCls(v)}" style="font-size:.75rem;font-weight:600;min-width:60px;text-align:right">${fmtPd(v)}</span>
      <div style="width:60px;height:8px;background:#e2e8f0;border-radius:4px;overflow:hidden;display:flex;justify-content:flex-end">
        <div style="width:${pct}%;height:100%;background:${col};border-radius:4px"></div>
      </div>
    </div>`;
  }
}

function renderHero() {
  const s = _data.summary;
  const cards = [
    { label: 'ETFs with NAV',   val: s.total,       sub: 'today',         col: '#3b82f6' },
    { label: 'At Premium',      val: s.at_premium,  sub: '> +0.05%',      col: '#16a34a' },
    { label: 'Near Par',        val: s.near_par,    sub: '±0.05%',        col: '#6b7280' },
    { label: 'At Discount',     val: s.at_discount, sub: '< −0.05%',      col: '#dc2626' },
    { label: 'Avg Prem/Disc',   val: fmtPd(s.avg_pd),  sub: 'market avg', col: s.avg_pd >= 0 ? '#16a34a' : '#dc2626', raw: true },
    { label: 'Max Premium',     val: fmtPd(s.max_premium),  sub: 'today', col: '#16a34a', raw: true },
    { label: 'Max Discount',    val: fmtPd(s.max_discount), sub: 'today', col: '#dc2626', raw: true },
  ];
  document.getElementById('hero').innerHTML = cards.map(c => `
    <div class="card p-3">
      <p class="sl">${c.label}</p>
      <p class="sv mt-1" style="color:${c.col};font-size:1.4rem">${c.raw ? c.val : c.val.toLocaleString()}</p>
      <p class="ss">${c.sub}</p>
    </div>`).join('');
}

function renderHistChart() {
  const hist = _data.market_history;
  if (!hist.length) return;
  const labels = hist.map(r => r.date);
  const avg = hist.map(r => r.avg_pd != null ? +r.avg_pd.toFixed(4) : null);
  const minPd = hist.map(r => r.min_pd != null ? +r.min_pd.toFixed(4) : null);
  const maxPd = hist.map(r => r.max_pd != null ? +r.max_pd.toFixed(4) : null);

  new Chart(document.getElementById('chart-hist'), {
    type: 'line',
    data: {
      labels,
      datasets: [
        { label: 'Max', data: maxPd, borderColor: 'rgba(22,163,74,0.35)',
          backgroundColor: 'rgba(22,163,74,0.08)', fill: '+1', borderWidth: 1, pointRadius: 0, tension: 0.3 },
        { label: 'Avg', data: avg, borderColor: '#3b82f6',
          backgroundColor: 'transparent', borderWidth: 2, pointRadius: 0, tension: 0.3 },
        { label: 'Min', data: minPd, borderColor: 'rgba(220,38,38,0.35)',
          backgroundColor: 'rgba(220,38,38,0.08)', fill: '-1', borderWidth: 1, pointRadius: 0, tension: 0.3 },
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { position: 'top', labels: { boxWidth: 10, font: { size: 11 } } },
        tooltip: {
          callbacks: {
            label: ctx => ` ${ctx.dataset.label}: ${fmtPd(ctx.parsed.y)}`
          }
        }
      },
      scales: {
        x: { ticks: { maxTicksLimit: 8, font: { size: 10 } }, grid: { display: false } },
        y: {
          ticks: { font: { size: 10 }, callback: v => fmtPd(v) },
          grid: { color: '#f1f5f9' },
        }
      }
    }
  });
}

function renderDistChart() {
  const vals = _data.snapshot.map(r => r.premium_discount_pct).filter(v => v != null);
  if (!vals.length) return;

  // Build histogram buckets: -1.0 to +1.0 in 0.1% steps
  const lo = -1.0, hi = 1.0, step = 0.1;
  const buckets = [];
  for (let b = lo; b < hi; b = +(b + step).toFixed(2)) buckets.push(b);
  const counts = new Array(buckets.length).fill(0);
  const overflow = { lo: 0, hi: 0 };
  vals.forEach(v => {
    if (v < lo) { overflow.lo++; return; }
    if (v >= hi) { overflow.hi++; return; }
    const idx = Math.floor((v - lo) / step);
    if (idx >= 0 && idx < counts.length) counts[idx]++;
  });

  const labels = buckets.map(b => fmtPd(b, 1));
  const colors = buckets.map(b => b >= 0 ? 'rgba(22,163,74,0.7)' : 'rgba(220,38,38,0.7)');

  new Chart(document.getElementById('chart-dist'), {
    type: 'bar',
    data: { labels, datasets: [{ data: counts, backgroundColor: colors, borderRadius: 2 }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false },
        tooltip: { callbacks: { title: ctx => ctx[0].label + ' to ' + fmtPd(+ctx[0].label.replace('%','').replace('+','') + step, 1),
                                label: ctx => ctx.parsed.y + ' ETFs' } } },
      scales: {
        x: { ticks: { font: { size: 9 }, maxRotation: 45 }, grid: { display: false } },
        y: { ticks: { font: { size: 10 }, stepSize: 1 }, grid: { color: '#f1f5f9' } }
      }
    }
  });
}

function renderAcBars() {
  const rows = _data.by_asset_class;
  if (!rows.length) { document.getElementById('ac-bars').innerHTML = '<p class="text-xs text-slate-500">No data</p>'; return; }
  const maxAbs = Math.max(...rows.map(r => Math.abs(r.avg_pd || 0)));
  document.getElementById('ac-bars').innerHTML = rows.map(r => {
    const v = r.avg_pd;
    const pct = maxAbs > 0 ? (Math.abs(v) / maxAbs * 100).toFixed(1) : 0;
    const col = v >= 0 ? '#16a34a' : '#dc2626';
    return `<div style="display:flex;align-items:center;gap:8px;margin-bottom:7px">
      <span style="font-size:.73rem;color:#374151;width:160px;flex-shrink:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${r.asset_class}">${r.asset_class || 'Unknown'}</span>
      <div style="flex:1;height:14px;background:#f1f5f9;border-radius:4px;overflow:hidden">
        <div style="width:${pct}%;height:100%;background:${col};border-radius:4px"></div>
      </div>
      <span class="${pdCls(v)}" style="font-size:.73rem;font-weight:600;width:64px;text-align:right">${fmtPd(v)}</span>
      <span style="font-size:.68rem;color:#94a3b8;width:32px;text-align:right">${r.count}</span>
    </div>`;
  }).join('');
}

function renderIssBars() {
  const rows = _data.by_issuer.filter(r => r.count >= 2).slice(0, 15);
  if (!rows.length) { document.getElementById('iss-bars').innerHTML = '<p class="text-xs text-slate-500">No data</p>'; return; }
  const maxAbs = Math.max(...rows.map(r => Math.abs(r.avg_pd || 0)));
  document.getElementById('iss-bars').innerHTML = rows.map(r => {
    const v = r.avg_pd;
    const pct = maxAbs > 0 ? (Math.abs(v) / maxAbs * 100).toFixed(1) : 0;
    const col = v >= 0 ? '#16a34a' : '#dc2626';
    return `<div style="display:flex;align-items:center;gap:8px;margin-bottom:7px">
      <span style="font-size:.73rem;color:#374151;width:120px;flex-shrink:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${r.issuer}"><a href="/issuers/${slugify(r.issuer)}" style="color:inherit" class="hover:underline">${r.issuer}</a></span>
      <div style="flex:1;height:14px;background:#f1f5f9;border-radius:4px;overflow:hidden">
        <div style="width:${pct}%;height:100%;background:${col};border-radius:4px"></div>
      </div>
      <span class="${pdCls(v)}" style="font-size:.73rem;font-weight:600;width:64px;text-align:right">${fmtPd(v)}</span>
      <span style="font-size:.68rem;color:#94a3b8;width:32px;text-align:right">${r.count}</span>
    </div>`;
  }).join('');
}

function populateEtfPicker() {
  const sel = document.getElementById('etf-picker');
  _data.etfs_with_history.forEach(e => {
    const o = document.createElement('option');
    o.value = e.code;
    o.textContent = e.code + ' — ' + (e.name || '') + ' (' + e.days + ' days)';
    sel.appendChild(o);
  });
  // Auto-load first ETF with most history
  if (_data.etfs_with_history.length) {
    sel.value = _data.etfs_with_history[0].code;
    loadEtfHistory();
  }
}

async function loadEtfHistory() {
  const code = document.getElementById('etf-picker').value;
  const period = document.getElementById('period-picker').value;
  if (!code) return;

  const d = await api('/api/v1/etfs/' + code + '/nav-history?period=' + period);
  const rows = d.nav_history || [];
  if (!rows.length) {
    document.getElementById('etf-stats').textContent = 'No data for this period';
    return;
  }

  const pds = rows.map(r => r.premium_discount_pct).filter(v => v != null);
  const avgPd = pds.length ? (pds.reduce((a,b) => a+b, 0) / pds.length).toFixed(3) : null;
  const maxPd = pds.length ? Math.max(...pds).toFixed(3) : null;
  const minPd = pds.length ? Math.min(...pds).toFixed(3) : null;
  document.getElementById('etf-stats').textContent =
    pds.length + ' days · avg ' + fmtPd(+avgPd) + ' · range ' + fmtPd(+minPd) + ' to ' + fmtPd(+maxPd);

  const labels = rows.map(r => r.date);
  const pdData = rows.map(r => r.premium_discount_pct != null ? +r.premium_discount_pct.toFixed(4) : null);

  if (_etfChart) _etfChart.destroy();
  _etfChart = new Chart(document.getElementById('chart-etf'), {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: code + ' Prem/Disc',
        data: pdData,
        borderColor: '#3b82f6',
        backgroundColor: ctx => {
          const v = ctx.parsed?.y;
          return v >= 0 ? 'rgba(22,163,74,0.08)' : 'rgba(220,38,38,0.08)';
        },
        fill: { target: { value: 0 }, above: 'rgba(22,163,74,0.10)', below: 'rgba(220,38,38,0.10)' },
        borderWidth: 1.5,
        pointRadius: rows.length < 60 ? 2 : 0,
        tension: 0.2,
        segment: {
          borderColor: ctx => ctx.p1.parsed.y >= 0 ? '#16a34a' : '#dc2626',
        }
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: { label: ctx => ` ${ctx.dataset.label}: ${fmtPd(ctx.parsed.y)}` }
        },
        annotation: {
          annotations: { zero: { type: 'line', yMin: 0, yMax: 0, borderColor: '#3b5280', borderWidth: 1, borderDash: [4,4] } }
        }
      },
      scales: {
        x: { ticks: { maxTicksLimit: 10, font: { size: 10 } }, grid: { display: false } },
        y: {
          ticks: { font: { size: 10 }, callback: v => fmtPd(v) },
          grid: { color: '#f1f5f9' }
        }
      }
    }
  });
}

function renderSnapTable() {
  const q = document.getElementById('snap-search').value.toLowerCase();
  const sort = document.getElementById('snap-sort').value;

  let rows = _data.snapshot.filter(r =>
    !q || r.code.toLowerCase().includes(q) || (r.name||'').toLowerCase().includes(q) || (r.issuer||'').toLowerCase().includes(q)
  );

  if (sort === 'pd-desc') rows.sort((a,b) => (b.premium_discount_pct||0) - (a.premium_discount_pct||0));
  else if (sort === 'pd-asc') rows.sort((a,b) => (a.premium_discount_pct||0) - (b.premium_discount_pct||0));
  else if (sort === 'fum-desc') rows.sort((a,b) => (b.fum||0) - (a.fum||0));
  else if (sort === 'abs-desc') rows.sort((a,b) => Math.abs(b.premium_discount_pct||0) - Math.abs(a.premium_discount_pct||0));

  document.getElementById('snap-table').innerHTML = rows.map(r => {
    const v = r.premium_discount_pct;
    const maxAbs = 1.0;
    const pct = v != null ? Math.min(Math.abs(v) / maxAbs * 100, 100).toFixed(1) : 0;
    const col = v != null ? (v >= 0 ? '#16a34a' : '#dc2626') : '#94a3b8';
    return `<tr>
      <td><span style="font-weight:700;font-family:monospace">${r.code}</span></td>
      <td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${r.name||''}">${r.name||'—'}</td>
      <td style="color:#475569">${r.issuer ? `<a href="/issuers/${slugify(r.issuer)}" style="color:#64748b" class="hover:underline">${r.issuer}</a>` : '—'}</td>
      <td>${r.asset_class||'—'}</td>
      <td style="text-align:right;font-family:monospace">${r.nav != null ? '$'+r.nav.toFixed(4) : '—'}</td>
      <td style="text-align:right;font-family:monospace">${r.close_price != null ? '$'+r.close_price.toFixed(4) : '—'}</td>
      <td style="text-align:right">
        <div style="display:flex;align-items:center;justify-content:flex-end;gap:6px">
          <div style="width:50px;height:8px;background:#f1f5f9;border-radius:4px;overflow:hidden">
            <div style="width:${pct}%;height:100%;background:${col};border-radius:4px"></div>
          </div>
          <span class="${pdCls(v)}" style="font-weight:600;font-family:monospace;min-width:60px;text-align:right">${fmtPd(v)}</span>
        </div>
      </td>
      <td style="text-align:right">${fmtFum(r.fum)}</td>
    </tr>`;
  }).join('');
}
"""

PAGE_NAV = _page(
    'Premium / Discount to NAV',
    'How Australian ETFs trade relative to their Net Asset Value',
    _NAV_BODY,
    _NAV_JS,
)


# ---------------------------------------------------------------------------
# Upcoming ETF Listings
# ---------------------------------------------------------------------------
_UPCOMING_BODY = """
<div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-5" id="stats-row">
  <div class="card text-center">
    <div class="sv text-indigo-600" id="s-upcoming">—</div>
    <div class="sl mt-1">Coming Soon</div>
    <div class="ss">pending on ASX/Cboe</div>
  </div>
  <div class="card text-center">
    <div class="sv text-green-600" id="s-listed30">—</div>
    <div class="sl mt-1">Listed (30d)</div>
    <div class="ss">new listings this month</div>
  </div>
  <div class="card text-center">
    <div class="sv text-blue-500" id="s-listed90">—</div>
    <div class="sl mt-1">Listed (90d)</div>
    <div class="ss">last three months</div>
  </div>
  <div class="card text-center">
    <div class="sv text-amber-500" id="s-ytd">—</div>
    <div class="sl mt-1">Listed (YTD)</div>
    <div class="ss">this calendar year</div>
  </div>
</div>

<!-- Upcoming -->
<div class="card mb-5">
  <div class="flex items-center justify-between mb-4">
    <div>
      <h2 class="text-base font-bold text-slate-800">Coming Soon</h2>
      <p class="text-xs text-slate-500 mt-0.5">
        Sourced from ASIC Offer Notice Board — PDS lodgements with 7-day exposure period complete.
        Expected dates are approximate; actual listing may vary.
      </p>
    </div>
    <a href="https://regulatoryportal.asic.gov.au/offer-notice-board" target="_blank"
       class="text-xs text-indigo-500 hover:text-indigo-700 font-medium shrink-0 ml-4">
      ASIC Portal ↗
    </a>
  </div>
  <div id="upcoming-cards" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
    <p class="text-sm text-slate-500 col-span-3">Loading…</p>
  </div>
</div>

<!-- Recently Listed -->
<div class="card">
  <div class="flex items-center justify-between mb-3">
    <h2 class="text-base font-bold text-slate-800">Recently Listed <span class="text-slate-500 font-normal text-sm">(last 90 days)</span></h2>
    <div class="flex items-center gap-2">
      <input id="recent-q" type="text" placeholder="Search…"
             oninput="renderRecent()"
             class="border border-slate-200 rounded-lg px-2.5 py-1.5 text-xs w-40 bg-slate-50 text-slate-800">
      <select id="recent-exchange" onchange="renderRecent()"
              class="border border-slate-200 rounded-lg px-2 py-1.5 text-xs bg-slate-50 text-slate-800">
        <option value="">All exchanges</option>
        <option value="ASX">ASX</option>
        <option value="CXA">Cboe</option>
      </select>
    </div>
  </div>
  <table>
    <thead>
      <tr>
        <th>Code</th><th>Fund Name</th><th>Issuer</th>
        <th>Asset Class</th><th>Listing Date</th><th style="text-align:right">MER</th>
        <th style="text-align:right">FUM</th>
      </tr>
    </thead>
    <tbody id="recent-table"></tbody>
  </table>
  <p id="recent-empty" class="text-sm text-slate-500 text-center py-6 hidden">No recent listings found.</p>
</div>
"""

_UPCOMING_JS = """
let _upData = null;

function daysUntil(iso) {
  if (!iso) return null;
  const d = new Date(iso);
  const now = new Date();
  now.setHours(0,0,0,0);
  return Math.round((d - now) / 86400000);
}

function exchangeBadge(ex) {
  if (!ex) return '';
  const styles = {
    'ASX': 'background:#e0f2fe;color:#0369a1',
    'CXA': 'background:#fef3c7;color:#92400e',
  };
  const s = styles[ex] || 'background:#f1f5f9;color:#475569';
  return `<span style="${s};font-size:.65rem;font-weight:700;padding:.15rem .4rem;border-radius:4px">${ex}</span>`;
}

function countdownBadge(days) {
  if (days === null) return '';
  if (days < 0)  return '<span style="background:#fef9c3;color:#854d0e;font-size:.65rem;font-weight:700;padding:.15rem .4rem;border-radius:4px">Date passed</span>';
  if (days === 0) return '<span style="background:#dcfce7;color:#166534;font-size:.65rem;font-weight:700;padding:.15rem .4rem;border-radius:4px">Today</span>';
  if (days <= 7)  return `<span style="background:#dcfce7;color:#166534;font-size:.65rem;font-weight:700;padding:.15rem .4rem;border-radius:4px">${days}d</span>`;
  if (days <= 30) return `<span style="background:#e0f2fe;color:#0369a1;font-size:.65rem;font-weight:700;padding:.15rem .4rem;border-radius:4px">${days}d</span>`;
  return `<span style="background:#f1f5f9;color:#475569;font-size:.65rem;font-weight:700;padding:.15rem .4rem;border-radius:4px">${days}d</span>`;
}

function newBadge(daysAgo) {
  if (daysAgo <= 7)  return '<span style="background:#dcfce7;color:#166534;font-size:.62rem;font-weight:700;padding:.12rem .35rem;border-radius:4px;margin-left:4px">NEW</span>';
  if (daysAgo <= 30) return '<span style="background:#e0f2fe;color:#0369a1;font-size:.62rem;font-weight:700;padding:.12rem .35rem;border-radius:4px;margin-left:4px">NEW</span>';
  return '';
}

function renderUpcoming() {
  const up = (_upData.upcoming || []);
  const container = document.getElementById('upcoming-cards');
  if (!up.length) {
    container.innerHTML = '<p class="text-sm text-slate-500 col-span-3 py-4">No upcoming listings found. Run the upcoming listings scraper to populate.</p>';
    return;
  }
  container.innerHTML = up.map(r => {
    const days = daysUntil(r.expected_listing_date);
    const dateStr = r.expected_listing_date
      ? new Date(r.expected_listing_date).toLocaleDateString('en-AU', {day:'numeric',month:'short',year:'numeric'})
      : '—';
    const lodgedStr = r.pds_lodged_date
      ? new Date(r.pds_lodged_date).toLocaleDateString('en-AU', {day:'numeric',month:'short',year:'numeric'})
      : '—';
    const links = [];
    if (r.asic_detail_url) links.push(`<a href="${r.asic_detail_url}" target="_blank" style="color:#6366f1;font-size:.7rem">ASIC notice ↗</a>`);
    if (r.offer_doc_url)   links.push(`<a href="${r.offer_doc_url}" target="_blank" style="color:#6366f1;font-size:.7rem">Offer doc ↗</a>`);
    return `
    <div style="border:1px solid #e2e8f0;border-radius:10px;padding:1rem;background:#fff">
      <div class="flex items-start justify-between gap-2 mb-2">
        <div class="flex-1 min-w-0">
          <div style="font-weight:700;font-size:.82rem;line-height:1.3;color:#0f172a">${r.name || '—'}</div>
          <div style="font-size:.7rem;color:#64748b;margin-top:.2rem">${r.issuer || '—'}</div>
        </div>
        <div class="flex flex-col items-end gap-1 shrink-0">
          ${exchangeBadge(r.exchange)}
          ${countdownBadge(days)}
        </div>
      </div>
      ${r.fund_type ? `<div style="font-size:.68rem;color:#94a3b8;margin-bottom:.5rem">${r.fund_type}</div>` : ''}
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:.25rem;font-size:.7rem;color:#475569;margin-bottom:.75rem">
        <div><span style="color:#94a3b8">Expected:</span> ${dateStr}</div>
        <div><span style="color:#94a3b8">PDS lodged:</span> ${lodgedStr}</div>
        ${r.arsn ? `<div style="grid-column:span 2"><span style="color:#94a3b8">ARSN:</span> ${r.arsn}</div>` : ''}
      </div>
      ${links.length ? `<div class="flex gap-3">${links.join('')}</div>` : ''}
    </div>`;
  }).join('');
}

function renderRecent() {
  const q  = document.getElementById('recent-q').value.toLowerCase();
  const ex = document.getElementById('recent-exchange').value;
  let rows = (_upData.recent || []).filter(r =>
    (!q  || (r.code||'').toLowerCase().includes(q) || (r.name||'').toLowerCase().includes(q) || (r.issuer||'').toLowerCase().includes(q)) &&
    (!ex || r.exchange === ex)
  );
  const tbody = document.getElementById('recent-table');
  const empty = document.getElementById('recent-empty');
  if (!rows.length) {
    tbody.innerHTML = '';
    empty.classList.remove('hidden');
    return;
  }
  empty.classList.add('hidden');
  const today = new Date(); today.setHours(0,0,0,0);
  tbody.innerHTML = rows.map(r => {
    const listed = r.inception_date ? new Date(r.inception_date) : null;
    const daysAgo = listed ? Math.round((today - listed) / 86400000) : null;
    const daysStr = daysAgo !== null ? `${daysAgo}d ago` : '—';
    const dateStr = listed
      ? listed.toLocaleDateString('en-AU', {day:'numeric',month:'short',year:'numeric'})
      : '—';
    const mer = r.management_fee != null ? r.management_fee.toFixed(2)+'%' : (r.expense_ratio != null ? r.expense_ratio.toFixed(2)+'%' : '—');
    return `<tr>
      <td>
        <span style="font-weight:700;font-family:monospace">${r.code}</span>
        ${exchangeBadge(r.exchange)}
        ${daysAgo !== null ? newBadge(daysAgo) : ''}
      </td>
      <td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${r.name||''}">${r.name||'—'}</td>
      <td style="color:#64748b;font-size:.75rem">${r.issuer ? `<a href="/issuers/${slugify(r.issuer)}" style="color:#64748b" class="hover:underline">${r.issuer}</a>` : '—'}</td>
      <td>${r.asset_class||'—'}</td>
      <td style="white-space:nowrap"><span style="font-family:monospace">${dateStr}</span><br><span style="font-size:.65rem;color:#94a3b8">${daysStr}</span></td>
      <td style="text-align:right;font-family:monospace">${mer}</td>
      <td style="text-align:right">${fmtFum(r.fund_size_aud_millions)}</td>
    </tr>`;
  }).join('');
}

async function init() {
  document.getElementById('ts').textContent = 'Updated ' + new Date().toLocaleTimeString('en-AU', {hour:'2-digit',minute:'2-digit'});
  _upData = await api('/api/v1/insights/upcoming');

  const s = _upData.stats || {};
  document.getElementById('s-upcoming').textContent  = s.upcoming_count ?? '—';
  document.getElementById('s-listed30').textContent  = s.listed_last_30  ?? '—';
  document.getElementById('s-listed90').textContent  = s.listed_last_90  ?? '—';
  document.getElementById('s-ytd').textContent       = s.listed_ytd      ?? '—';

  renderUpcoming();
  renderRecent();

  document.getElementById('loading').classList.add('hidden');
  document.getElementById('page').classList.remove('hidden');
}
"""

PAGE_UPCOMING = _page(
    'Upcoming & Recent ETF Listings',
    'New ETFs coming to ASX & Cboe — sourced from ASIC Offer Notice Board',
    _UPCOMING_BODY,
    _UPCOMING_JS,
)


# ---------------------------------------------------------------------------
# ASSET CLASS ANALYSER PAGE
# ---------------------------------------------------------------------------
_AC_BODY = """
<div id="lens-tabs" class="flex gap-2 flex-wrap mb-5">
  <button data-lens="asset"   class="lens-btn active-lens px-4 py-2 rounded-lg text-sm font-semibold border transition-colors">Asset Classes</button>
  <button data-lens="sector"  class="lens-btn px-4 py-2 rounded-lg text-sm font-semibold border transition-colors">GICS Sectors</button>
  <button data-lens="country" class="lens-btn px-4 py-2 rounded-lg text-sm font-semibold border transition-colors">Geography</button>
  <button data-lens="factor"  class="lens-btn px-4 py-2 rounded-lg text-sm font-semibold border transition-colors">Factors</button>
</div>

<!-- LENS: Asset Classes -->
<div id="lens-asset">
  <div class="grid grid-cols-1 lg:grid-cols-3 gap-5">
    <div class="card lg:col-span-2">
      <h2 class="font-semibold text-sm text-slate-700 mb-3">Market AUM by Asset Class</h2>
      <div style="position:relative;height:320px"><canvas id="ac-fum-chart"></canvas></div>
    </div>
    <div class="card">
      <h2 class="font-semibold text-sm text-slate-700 mb-3">1Y Return by Asset Class</h2>
      <div style="position:relative;height:320px"><canvas id="ac-ret-chart"></canvas></div>
    </div>
  </div>
  <div class="card">
    <div class="flex items-center justify-between mb-3 flex-wrap gap-2">
      <h2 class="font-semibold text-sm text-slate-700" id="ac-drill-title">Click an asset class above to drill into sub-categories</h2>
      <button id="ac-clear" class="text-xs text-blue-400 hover:text-blue-300 hidden">← All classes</button>
    </div>
    <div id="ac-subcats" class="mb-4"></div>
    <div class="overflow-x-auto">
      <table>
        <thead><tr>
          <th>Code</th><th>Fund Name</th><th>Issuer</th><th>Sub-Category</th>
          <th style="text-align:right">FUM</th>
          <th style="text-align:right">1Y</th><th style="text-align:right">3Y</th><th style="text-align:right">5Y</th>
          <th style="text-align:right">MER</th>
        </tr></thead>
        <tbody id="ac-etf-table"></tbody>
      </table>
    </div>
  </div>
</div>

<!-- LENS: GICS Sectors -->
<div id="lens-sector" class="hidden">
  <div class="grid grid-cols-1 lg:grid-cols-3 gap-5">
    <div class="card lg:col-span-2">
      <h2 class="font-semibold text-sm text-slate-700 mb-1">Market Exposure by GICS Sector</h2>
      <p class="text-xs text-slate-500 mb-3">FUM-weighted sector allocation across all ETFs with holdings data. Click a sector to see which ETFs offer the most targeted exposure.</p>
      <div style="position:relative;height:420px"><canvas id="sector-chart"></canvas></div>
    </div>
    <div class="card">
      <h2 class="font-semibold text-sm text-slate-700 mb-1" id="sector-drill-title">Select a sector →</h2>
      <p class="text-xs text-slate-500 mb-3">ETFs with highest allocation to selected sector</p>
      <div id="sector-etf-list"></div>
    </div>
  </div>
</div>

<!-- LENS: Geography -->
<div id="lens-country" class="hidden">
  <div class="grid grid-cols-1 lg:grid-cols-3 gap-5">
    <div class="card lg:col-span-2">
      <h2 class="font-semibold text-sm text-slate-700 mb-1">Geographic Exposure — FUM-Weighted</h2>
      <p class="text-xs text-slate-500 mb-3">Aggregated country weights across all ETFs with holdings data, weighted by FUM. Click a country to see targeted ETFs.</p>
      <div style="position:relative;height:480px"><canvas id="country-chart"></canvas></div>
    </div>
    <div class="card">
      <h2 class="font-semibold text-sm text-slate-700 mb-1" id="country-drill-title">Select a country →</h2>
      <p class="text-xs text-slate-500 mb-3">ETFs with highest allocation to selected country</p>
      <div id="country-etf-list"></div>
    </div>
  </div>
</div>

<!-- LENS: Factors -->
<div id="lens-factor" class="hidden">
  <div id="factor-grid" class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-4 mb-5"></div>
  <div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
    <div class="card">
      <h2 class="font-semibold text-sm text-slate-700 mb-3">Factor Return Comparison</h2>
      <div style="position:relative;height:280px"><canvas id="factor-ret-chart"></canvas></div>
    </div>
    <div class="card">
      <h2 class="font-semibold text-sm text-slate-700 mb-1" id="factor-table-title">Factor ETF List</h2>
      <p class="text-xs text-slate-500 mb-3" id="factor-table-sub">Click a factor card to filter</p>
      <div class="overflow-x-auto" style="max-height:260px;overflow-y:auto">
        <table>
          <thead><tr><th>Code</th><th>Name</th><th>FUM</th><th>1Y</th><th>3Y</th><th>MER</th></tr></thead>
          <tbody id="factor-etf-table"></tbody>
        </table>
      </div>
    </div>
  </div>
</div>
"""

_AC_JS = """
let _d = null;
let _acChart = null, _acRetChart = null, _sectorChart = null, _countryChart = null, _factorChart = null;
let _activeLens = 'asset';
let _activeAC = null;
let _activeSector = null;
let _activeCountry = null;
let _activeFactor = null;

// ── Lens styles ────────────────────────────────────────────────────────────
const LENS_BTN_BASE   = 'lens-btn px-4 py-2 rounded-lg text-sm font-semibold border transition-colors';
const LENS_BTN_ACTIVE = 'bg-blue-600 border-blue-600 text-white';
const LENS_BTN_IDLE   = 'bg-white border-slate-200 text-slate-600 hover:border-blue-400 hover:text-blue-600';

// ── Factor definitions (Fama-French inspired) ──────────────────────────────
const FACTORS = [
  { id:'market',    label:'Market (Beta)',    color:'#3b82f6',
    test: e => /broad market|developed markets|us market|global shares|world shares|all ordinaries|s&p 500|asx 200|asx 300/i.test((e.sub_category||'')+(e.name||'')+(e.benchmark||'')) && !/factor|quality|value|momentum|dividend|income|small|growth|esg|responsible|low.vol/i.test((e.sub_category||'')+(e.name||'')) },
  { id:'quality',   label:'Quality',          color:'#10b981',
    test: e => /quality/i.test((e.sub_category||'')+(e.name||'')) },
  { id:'value',     label:'Value',            color:'#f59e0b',
    test: e => /factor.*value|value.*factor|value equity|value etf/i.test((e.sub_category||'')+(e.name||'')) && !/quality|momentum/i.test(e.name||'') },
  { id:'momentum',  label:'Momentum',         color:'#ef4444',
    test: e => /momentum/i.test((e.sub_category||'')+(e.name||'')) },
  { id:'dividend',  label:'Dividend/Income',  color:'#f97316',
    test: e => /dividend|income|high yield|hyield|harvester/i.test((e.sub_category||'')+(e.name||'')) && !/bond|fixed|credit|hybrid|subordinated/i.test(e.name||'') },
  { id:'smallcap',  label:'Small Cap',        color:'#8b5cf6',
    test: e => /small.cap|small.*mid|smid|small companies|smaller companies/i.test((e.sub_category||'')+(e.name||'')) },
  { id:'growth',    label:'Growth',           color:'#06b6d4',
    test: e => /growth/i.test((e.sub_category||'')+(e.name||'')) && !/dividend|income|value/i.test(e.name||'') && !/diversified/i.test(e.sub_category||'') },
  { id:'esg',       label:'ESG',              color:'#84cc16',
    test: e => /esg|responsible|sustain|ethical|environmental|carbon|climate|green/i.test((e.sub_category||'')+(e.name||'')) },
  { id:'lowvol',    label:'Low Volatility',   color:'#a855f7',
    test: e => /low.vol|min.vol|minimum.vol|defensive|managed.risk/i.test((e.sub_category||'')+(e.name||'')) },
  { id:'emerging',  label:'Emerging Markets', color:'#14b8a6',
    test: e => /emerging/i.test((e.sub_category||'')+(e.name||'')) },
  { id:'hedged',    label:'Currency Hedged',  color:'#ec4899',
    test: e => /hedged|currency.hedged|aud.hedged/i.test(e.name||'') && !/unhedged/i.test(e.name||'') && (e.asset_class||'').includes('Equities') },
  { id:'sector',    label:'Sector ETFs',      color:'#6366f1',
    test: e => /sector/i.test(e.sub_category||'') },
];

function assignFactor(e) {
  for (const f of FACTORS) { if (f.test(e)) return f.id; }
  return null;
}

// ── Colour helpers ─────────────────────────────────────────────────────────
const AC_PALETTE = {
  'International Equities':'#3b82f6','Australian Equities':'#10b981',
  'Fixed Income':'#f59e0b','Commodities':'#f97316','Property':'#8b5cf6',
  'Diversified':'#06b6d4','Cash':'#84cc16','Thematic':'#ec4899',
  'Alternatives':'#6366f1','Digital Assets':'#14b8a6','Currency':'#a855f7',
};
const GICS_PALETTE = {
  'Information Technology':'#3b82f6','Financials':'#10b981','Health Care':'#f59e0b',
  'Consumer Discretionary':'#ef4444','Industrials':'#f97316','Energy':'#8b5cf6',
  'Communication Services':'#06b6d4','Consumer Staples':'#84cc16','Materials':'#a855f7',
  'Real Estate':'#ec4899','Utilities':'#14b8a6',
};
function acColor(n)     { return AC_PALETTE[n]   || '#64748b'; }
function gicsColor(n)   { return GICS_PALETTE[n] || '#64748b'; }
function retColor(v)    { return v == null ? '#64748b' : v >= 0 ? '#16a34a' : '#dc2626'; }
function factorColor(id){ return (FACTORS.find(f=>f.id===id)||{}).color || '#64748b'; }

// ── Init ───────────────────────────────────────────────────────────────────
async function init() {
  _d = await api('/api/v1/insights/asset-classes');
  document.getElementById('ts').textContent = new Date().toLocaleTimeString('en-AU');

  // Lens buttons
  document.getElementById('lens-tabs').addEventListener('click', e => {
    const btn = e.target.closest('.lens-btn');
    if (!btn) return;
    _activeLens = btn.dataset.lens;
    document.querySelectorAll('.lens-btn').forEach(b => {
      b.className = LENS_BTN_BASE + ' ' + (b.dataset.lens === _activeLens ? LENS_BTN_ACTIVE : LENS_BTN_IDLE);
    });
    ['asset','sector','country','factor'].forEach(l => {
      document.getElementById('lens-'+l).classList.toggle('hidden', l !== _activeLens);
    });
    if (_activeLens === 'asset'   && !_acChart)      renderAssetLens();
    if (_activeLens === 'sector'  && !_sectorChart)  renderSectorLens();
    if (_activeLens === 'country' && !_countryChart) renderCountryLens();
    if (_activeLens === 'factor'  && !_factorChart)  renderFactorLens();
  });
  // set initial btn styles
  document.querySelectorAll('.lens-btn').forEach(b => {
    b.className = LENS_BTN_BASE + ' ' + (b.dataset.lens === 'asset' ? LENS_BTN_ACTIVE : LENS_BTN_IDLE);
  });

  renderAssetLens();
  document.getElementById('loading').classList.add('hidden');
  document.getElementById('page').classList.remove('hidden');
}

// ─────────────────────────────────────────────────────────────────────────
// LENS 1: ASSET CLASSES
// ─────────────────────────────────────────────────────────────────────────
function renderAssetLens() {
  // Aggregate by top-level asset class
  const acMap = {};
  _d.by_asset_class.forEach(r => {
    if (!acMap[r.asset_class]) acMap[r.asset_class] = { fum:0, etf_count:0, ret_sum:0, ret_n:0 };
    acMap[r.asset_class].fum       += r.total_fum || 0;
    acMap[r.asset_class].etf_count += r.etf_count;
    if (r.avg_1y != null) { acMap[r.asset_class].ret_sum += r.avg_1y * r.etf_count; acMap[r.asset_class].ret_n += r.etf_count; }
  });
  const acs = Object.entries(acMap).map(([k,v]) => ({
    name: k, fum: v.fum, etf_count: v.etf_count,
    avg_1y: v.ret_n ? v.ret_sum / v.ret_n : null,
  })).sort((a,b) => b.fum - a.fum);

  // FUM chart
  const ctx1 = document.getElementById('ac-fum-chart').getContext('2d');
  if (_acChart) _acChart.destroy();
  _acChart = new Chart(ctx1, {
    type: 'bar',
    data: {
      labels: acs.map(a => a.name),
      datasets: [{ label: 'FUM ($M)', data: acs.map(a => a.fum),
        backgroundColor: acs.map(a => (_activeAC === a.name ? acColor(a.name) : acColor(a.name)+'99')),
        borderColor: acs.map(a => acColor(a.name)), borderWidth: 1, borderRadius: 4 }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend:{display:false}, tooltip:{ callbacks:{ label: c => fmtFum(c.raw) } } },
      scales: {
        x: { ticks:{ font:{size:9}, maxRotation:35 }, grid:{display:false} },
        y: { ticks:{ font:{size:10}, callback: v => fmtFum(v) }, grid:{color:'#f1f5f9'} }
      },
      onClick: (_, els) => { if (els[0]) drillAssetClass(acs[els[0].index].name); }
    }
  });

  // Return chart
  const ctx2 = document.getElementById('ac-ret-chart').getContext('2d');
  if (_acRetChart) _acRetChart.destroy();
  _acRetChart = new Chart(ctx2, {
    type: 'bar',
    data: {
      labels: acs.map(a=>a.name),
      datasets:[{ label:'Avg 1Y Return', data: acs.map(a=>a.avg_1y),
        backgroundColor: acs.map(a => a.avg_1y >= 0 ? '#16a34a33' : '#dc262633'),
        borderColor: acs.map(a => a.avg_1y >= 0 ? '#16a34a' : '#dc2626'),
        borderWidth:1, borderRadius:4 }]
    },
    options:{
      responsive:true, maintainAspectRatio:false, indexAxis:'y',
      plugins:{legend:{display:false}, tooltip:{callbacks:{label:c=>c.parsed.x != null ? c.parsed.x.toFixed(1)+'%' : '—'}}},
      scales:{
        x:{ticks:{font:{size:10},callback:v=>v+'%'}, grid:{color:'#f1f5f9'}},
        y:{ticks:{font:{size:9}}, grid:{display:false}}
      }
    }
  });

  renderACTable(null);
}

function drillAssetClass(ac) {
  _activeAC = ac;
  document.getElementById('ac-drill-title').textContent = ac + ' — Sub-categories';
  document.getElementById('ac-clear').classList.remove('hidden');
  renderACSubcats(ac);
  renderACTable(ac);
}

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('ac-clear').addEventListener('click', () => {
    _activeAC = null;
    document.getElementById('ac-drill-title').textContent = 'Click an asset class above to drill into sub-categories';
    document.getElementById('ac-clear').classList.add('hidden');
    document.getElementById('ac-subcats').innerHTML = '';
    renderACTable(null);
  });
});

function renderACSubcats(ac) {
  const subs = _d.by_asset_class.filter(r => r.asset_class === ac && r.sub_category);
  const maxFum = Math.max(...subs.map(s => s.total_fum || 0), 1);
  document.getElementById('ac-subcats').innerHTML = subs.length
    ? `<div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3 mb-4">${
        subs.map(s => `<div class="border border-slate-200 rounded-lg p-3 bg-slate-50 cursor-pointer hover:border-blue-500/60"
          onclick="renderACTable('${ac}','${(s.sub_category||'').replace(/'/g,"\\'")}')">
          <div class="text-xs text-slate-400 font-medium mb-1 truncate" title="${s.sub_category||''}">${s.sub_category||'—'}</div>
          <div class="text-base font-bold text-slate-100">${fmtFum(s.total_fum)}</div>
          <div class="flex items-center gap-2 mt-1">
            <span class="text-xs text-slate-500">${s.etf_count} ETFs</span>
            ${s.avg_1y != null ? `<span class="text-xs font-semibold ${s.avg_1y>=0?'text-green-400':'text-red-400'}">${s.avg_1y>=0?'+':''}${s.avg_1y.toFixed(1)}%</span>` : ''}
          </div>
          <div class="bar-track mt-2"><div class="bar-fill" style="width:${Math.min(s.total_fum/maxFum*100,100).toFixed(1)}%;background:${acColor(ac)}"></div></div>
        </div>`).join('')
      }</div>` : '';
}

function renderACTable(ac, subcat) {
  let etfs = _d.etf_list;
  if (ac)     etfs = etfs.filter(e => e.asset_class === ac);
  if (subcat) etfs = etfs.filter(e => e.sub_category === subcat);
  etfs = etfs.slice(0, 50);
  document.getElementById('ac-etf-table').innerHTML = etfs.map(e => `<tr>
    <td class="font-bold" style="color:${acColor(e.asset_class)}">${e.code}</td>
    <td class="text-slate-700" style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${e.name||''}">${e.name||'—'}</td>
    <td class="text-slate-400 text-xs">${e.issuer||'—'}</td>
    <td class="text-slate-500 text-xs">${e.sub_category||'—'}</td>
    <td class="text-right tabular-nums text-slate-700">${fmtFum(e.fum)}</td>
    <td class="text-right tabular-nums text-xs ${e.return_1y>=0?'text-green-400':'text-red-400'}">${e.return_1y!=null?(e.return_1y>=0?'+':'')+e.return_1y.toFixed(1)+'%':'—'}</td>
    <td class="text-right tabular-nums text-xs text-slate-400">${e.return_3y!=null?(e.return_3y>=0?'+':'')+e.return_3y.toFixed(1)+'%':'—'}</td>
    <td class="text-right tabular-nums text-xs text-slate-400">${e.return_5y!=null?(e.return_5y>=0?'+':'')+e.return_5y.toFixed(1)+'%':'—'}</td>
    <td class="text-right tabular-nums text-xs text-slate-500">${e.mer!=null?e.mer.toFixed(2)+'%':'—'}</td>
  </tr>`).join('') || '<tr><td colspan="9" class="text-center text-slate-500 py-4">No ETFs found</td></tr>';
}

// ─────────────────────────────────────────────────────────────────────────
// LENS 2: GICS SECTORS
// ─────────────────────────────────────────────────────────────────────────
function renderSectorLens() {
  const sectors = _d.gics_sectors.slice(0, 18);
  const ctx = document.getElementById('sector-chart').getContext('2d');
  if (_sectorChart) _sectorChart.destroy();
  _sectorChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: sectors.map(s => s.sector_norm),
      datasets: [{
        label: 'FUM-Weighted Exposure ($M)',
        data: sectors.map(s => s.fum_weighted_m),
        backgroundColor: sectors.map(s => gicsColor(s.sector_norm) + '99'),
        borderColor: sectors.map(s => gicsColor(s.sector_norm)),
        borderWidth: 1, borderRadius: 4
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false, indexAxis: 'y',
      plugins: { legend:{display:false}, tooltip:{ callbacks:{ label: c => '$' + (c.raw/1000).toFixed(1) + 'B exposure' } } },
      scales: {
        x: { ticks:{font:{size:10}, callback: v => '$'+(v/1000).toFixed(0)+'B'}, grid:{color:'#f1f5f9'} },
        y: { ticks:{font:{size:10}}, grid:{display:false} }
      },
      onClick: (_, els) => { if (els[0]) drillSector(sectors[els[0].index].sector_norm); }
    }
  });
}

function drillSector(sector) {
  _activeSector = sector;
  document.getElementById('sector-drill-title').textContent = sector;
  const etfs = (_d.sector_etfs || [])
    .filter(r => r.sector_norm === sector)
    .sort((a,b) => b.weight_pct - a.weight_pct)
    .slice(0, 10);
  document.getElementById('sector-etf-list').innerHTML = etfs.length
    ? etfs.map(e => `<div class="flex items-center justify-between py-2 border-b border-slate-100 last:border-0">
        <div class="min-w-0">
          <span class="font-bold text-sm" style="color:${gicsColor(sector)}">${e.etf_code}</span>
          <span class="text-xs text-slate-400 ml-2 truncate">${(e.name||'').substring(0,30)}</span>
        </div>
        <div class="text-right shrink-0 ml-2">
          <div class="text-sm font-semibold text-slate-800">${e.weight_pct.toFixed(1)}%</div>
          <div class="text-xs text-slate-500">${fmtFum(e.fum)}</div>
        </div>
      </div>`).join('')
    : '<p class="text-sm text-slate-500">No data</p>';
}

// ─────────────────────────────────────────────────────────────────────────
// LENS 3: GEOGRAPHY
// ─────────────────────────────────────────────────────────────────────────
function renderCountryLens() {
  const countries = _d.countries.slice(0, 25);
  const ctx = document.getElementById('country-chart').getContext('2d');
  if (_countryChart) _countryChart.destroy();
  _countryChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: countries.map(c => c.country_norm),
      datasets: [{
        label: 'FUM-Weighted Exposure ($M)',
        data: countries.map(c => c.fum_weighted_m),
        backgroundColor: '#3b82f666',
        borderColor: '#3b82f6',
        borderWidth: 1, borderRadius: 4
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false, indexAxis: 'y',
      plugins: { legend:{display:false}, tooltip:{ callbacks:{ label: c => '$' + (c.raw/1000).toFixed(1) + 'B exposure · ' + (_d.countries.find(x=>x.country_norm===countries[c.dataIndex].country_norm)||{}).etf_count + ' ETFs' } } },
      scales: {
        x: { ticks:{font:{size:10}, callback: v => '$'+(v/1000).toFixed(0)+'B'}, grid:{color:'#f1f5f9'} },
        y: { ticks:{font:{size:9}}, grid:{display:false} }
      },
      onClick: (_, els) => { if (els[0]) drillCountry(countries[els[0].index].country_norm); }
    }
  });
}

function drillCountry(country) {
  _activeCountry = country;
  document.getElementById('country-drill-title').textContent = country;
  const etfs = (_d.country_etfs || [])
    .filter(r => r.country_norm === country)
    .sort((a,b) => b.total_weight_pct - a.total_weight_pct)
    .slice(0, 10);
  document.getElementById('country-etf-list').innerHTML = etfs.length
    ? etfs.map(e => `<div class="flex items-center justify-between py-2 border-b border-slate-100 last:border-0">
        <div class="min-w-0">
          <span class="font-bold text-sm text-blue-400">${e.etf_code}</span>
          <span class="text-xs text-slate-400 ml-2 truncate">${(e.name||'').substring(0,28)}</span>
        </div>
        <div class="text-right shrink-0 ml-2">
          <div class="text-sm font-semibold text-slate-800">${e.total_weight_pct.toFixed(1)}%</div>
          <div class="text-xs text-slate-500">${fmtFum(e.fum)}</div>
        </div>
      </div>`).join('')
    : '<p class="text-sm text-slate-500">No data for this country</p>';
}

// ─────────────────────────────────────────────────────────────────────────
// LENS 4: FACTORS (Fama-French inspired)
// ─────────────────────────────────────────────────────────────────────────
function renderFactorLens() {
  // Assign each ETF to a factor
  const factorMap = {};
  FACTORS.forEach(f => { factorMap[f.id] = { etfs:[], fum:0, r1:[], r3:[], r5:[] }; });

  _d.etf_list.forEach(e => {
    const fid = assignFactor(e);
    if (!fid || !factorMap[fid]) return;
    const fm = factorMap[fid];
    fm.etfs.push(e);
    fm.fum += e.fum || 0;
    if (e.return_1y != null) fm.r1.push(e.return_1y);
    if (e.return_3y != null) fm.r3.push(e.return_3y);
    if (e.return_5y != null) fm.r5.push(e.return_5y);
  });

  const avg = arr => arr.length ? arr.reduce((a,b)=>a+b,0)/arr.length : null;

  // Factor cards
  document.getElementById('factor-grid').innerHTML = FACTORS.map(f => {
    const fm = factorMap[f.id];
    const r1 = avg(fm.r1);
    return `<div class="card cursor-pointer hover:border-blue-500/60 transition-colors factor-card" data-factor="${f.id}"
        style="border-color:${f.color}30" onclick="drillFactor('${f.id}')">
      <div class="text-xs font-bold uppercase tracking-wider mb-2" style="color:${f.color}">${f.label}</div>
      <div class="text-2xl font-bold text-slate-100">${fm.etfs.length}</div>
      <div class="text-xs text-slate-500 mb-2">ETFs · ${fmtFum(fm.fum)}</div>
      <div class="text-lg font-semibold ${r1==null?'text-slate-500':r1>=0?'text-green-400':'text-red-400'}">${r1!=null?(r1>=0?'+':'')+r1.toFixed(1)+'%':'—'}</div>
      <div class="text-xs text-slate-500">avg 1Y return</div>
    </div>`;
  }).join('');

  // Factor comparison bar chart
  const labels = FACTORS.map(f => f.label);
  const r1data = FACTORS.map(f => avg(factorMap[f.id].r1));
  const r3data = FACTORS.map(f => avg(factorMap[f.id].r3));
  const ctx = document.getElementById('factor-ret-chart').getContext('2d');
  if (_factorChart) _factorChart.destroy();
  _factorChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: '1Y Return', data: r1data, backgroundColor: FACTORS.map(f=>f.color+'99'), borderColor: FACTORS.map(f=>f.color), borderWidth:1, borderRadius:3 },
        { label: '3Y Return (ann.)', data: r3data, backgroundColor: '#ffffff22', borderColor: '#ffffff44', borderWidth:1, borderRadius:3 },
      ]
    },
    options: {
      responsive:true, maintainAspectRatio:false,
      plugins:{ legend:{position:'bottom',labels:{font:{size:10},boxWidth:12}}, tooltip:{callbacks:{label:c=>c.dataset.label+': '+(c.parsed.y!=null?(c.parsed.y>=0?'+':'')+c.parsed.y.toFixed(1)+'%':'—')}}},
      scales:{
        x:{ticks:{font:{size:8},maxRotation:35},grid:{display:false}},
        y:{ticks:{font:{size:10},callback:v=>v+'%'},grid:{color:'#f1f5f9'}}
      },
      onClick:(_, els) => { if(els[0]) drillFactor(FACTORS[els[0].index].id); }
    }
  });

  drillFactor('quality');
}

function drillFactor(fid) {
  _activeFactor = fid;
  const f = FACTORS.find(x => x.id === fid);
  document.querySelectorAll('.factor-card').forEach(c => {
    c.style.borderColor = c.dataset.factor === fid ? (f.color) : (FACTORS.find(x=>x.id===c.dataset.factor)||{}).color + '30';
  });
  const factorMap = {};
  FACTORS.forEach(ff => { factorMap[ff.id] = []; });
  _d.etf_list.forEach(e => { const fid2 = assignFactor(e); if (fid2 && factorMap[fid2]) factorMap[fid2].push(e); });
  const etfs = (factorMap[fid]||[]).sort((a,b)=>(b.fum||0)-(a.fum||0));
  document.getElementById('factor-table-title').textContent = f ? f.label + ' ETFs' : 'Factor ETFs';
  document.getElementById('factor-table-sub').textContent = etfs.length + ' ETFs matched';
  document.getElementById('factor-etf-table').innerHTML = etfs.slice(0,40).map(e=>`<tr>
    <td class="font-bold text-xs" style="color:${f.color}">${e.code}</td>
    <td class="text-slate-700 text-xs" style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${e.name||'—'}</td>
    <td class="text-right tabular-nums text-xs text-slate-700">${fmtFum(e.fum)}</td>
    <td class="text-right tabular-nums text-xs ${e.return_1y>=0?'text-green-400':'text-red-400'}">${e.return_1y!=null?(e.return_1y>=0?'+':'')+e.return_1y.toFixed(1)+'%':'—'}</td>
    <td class="text-right tabular-nums text-xs text-slate-400">${e.return_3y!=null?(e.return_3y>=0?'+':'')+e.return_3y.toFixed(1)+'%':'—'}</td>
    <td class="text-right tabular-nums text-xs text-slate-500">${e.mer!=null?e.mer.toFixed(2)+'%':'—'}</td>
  </tr>`).join('') || '<tr><td colspan="6" class="text-center text-slate-500 py-3">No ETFs matched</td></tr>';
}
"""

PAGE_ASSET_CLASSES = _page(
    'Asset Class Analyser',
    'Drill into GICS sectors, geography, and Fama-French factors across the Australian ETF market',
    _AC_BODY,
    _AC_JS,
)


# ---------------------------------------------------------------------------
# ISSUER PAGE
# ---------------------------------------------------------------------------
_ISSUER_BODY = """
<div id="issuer-hero" class="card">
  <div class="flex flex-col sm:flex-row items-start sm:items-center gap-4">
    <div class="flex-1 min-w-0">
      <div class="flex items-center gap-3 flex-wrap">
        <h2 id="issuer-name" class="text-2xl font-bold text-white"></h2>
        <a id="issuer-website" href="#" target="_blank" rel="noopener"
           class="hidden text-sm text-blue-400 hover:text-blue-300 font-medium">
          Visit website &#8599;
        </a>
      </div>
      <p id="issuer-meta" class="text-sm text-slate-500 mt-1"></p>
    </div>
    <a href="/insights/issuers" class="text-xs text-slate-500 hover:text-blue-600 shrink-0">
      ← All issuers
    </a>
  </div>
</div>

<div id="stats-bar" class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4"></div>

<div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-4">AUM by Asset Class</h2>
    <div class="relative" style="height:260px"><canvas id="chart-ac"></canvas></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-slate-700 mb-4">ETFs by Asset Class</h2>
    <div id="ac-count-bars" class="space-y-1 mt-2"></div>
  </div>
</div>

<div class="card">
  <div class="flex items-center justify-between mb-3 flex-wrap gap-2">
    <h2 class="font-semibold text-sm text-slate-700">All Products</h2>
    <div class="flex items-center gap-2">
      <input id="etf-search" type="text" placeholder="Filter…"
             class="border border-slate-200 rounded-lg px-2.5 py-1.5 text-sm
                    bg-slate-50 text-slate-700 focus:ring-2 focus:ring-blue-300 outline-none w-44">
      <select id="etf-ac-filter"
              class="border border-slate-200 rounded-lg px-2.5 py-1.5 text-sm
                     bg-slate-50 text-slate-700 focus:ring-2 focus:ring-blue-300 outline-none">
        <option value="">All asset classes</option>
      </select>
    </div>
  </div>
  <div class="overflow-x-auto">
    <table class="w-full text-sm">
      <thead>
        <tr class="text-left text-xs text-slate-400 border-b border-slate-200">
          <th class="pb-2 pr-3 font-semibold">Code</th>
          <th class="pb-2 pr-3 font-semibold">Name</th>
          <th class="pb-2 pr-3 font-semibold">Asset Class</th>
          <th class="pb-2 pr-3 font-semibold text-right">FUM</th>
          <th class="pb-2 pr-3 font-semibold text-right">MER</th>
          <th class="pb-2 pr-3 font-semibold text-right">1Y Ret</th>
          <th class="pb-2 pr-3 font-semibold text-right">3Y Ret</th>
          <th class="pb-2 pr-3 font-semibold text-right">Yield</th>
          <th class="pb-2 pr-3 font-semibold text-right">1M Flow</th>
          <th class="pb-2 font-semibold">Inception</th>
        </tr>
      </thead>
      <tbody id="etf-table"></tbody>
    </table>
  </div>
</div>

<div id="articles-section" class="card hidden">
  <h2 class="font-semibold text-sm text-slate-700 mb-4">Related Articles</h2>
  <div id="articles-grid" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4"></div>
</div>
"""

_ISSUER_JS = """
let _issuerData = null;
let _acChart = null;

async function init() {
  const slug = window.location.pathname.replace(/^\\/issuers\\//, '');
  const d = await api('/api/v1/issuers/' + slug);
  _issuerData = d;

  document.title = d.issuer + ' ETFs — Australian ETF Market';
  document.getElementById('ts').textContent = new Date().toLocaleTimeString('en-AU', {hour:'2-digit',minute:'2-digit'});

  // Hero
  document.getElementById('issuer-name').textContent = d.issuer;
  const since = d.stats.inception_earliest ? d.stats.inception_earliest.slice(0,4) : null;
  document.getElementById('issuer-meta').textContent =
    d.stats.etf_count + ' ETFs' + (since ? ' · since ' + since : '') +
    ' · ' + d.stats.market_share_pct + '% market share';

  if (d.website) {
    const a = document.getElementById('issuer-website');
    a.href = d.website;
    a.classList.remove('hidden');
  }

  // Stats bar
  const s = d.stats;
  const flow1m = s.fund_flow_1m;
  document.getElementById('stats-bar').innerHTML = [
    ['Total AUM',       fmtFum(s.total_fum),           ''],
    ['ETF Count',       s.etf_count + ' ETFs',          ''],
    ['Market Share',    s.market_share_pct + '%',       'of Aus ETF market'],
    ['FUM-Wtd MER',     mer(s.fum_weighted_mer),        'asset-weighted cost'],
    ['1M Net Flows',    fmtFum(flow1m),                 flow1m >= 0 ? 'net inflow' : 'net outflow'],
  ].map(([label, val, sub]) => `
    <div class="card">
      <div class="sl">${label}</div>
      <div class="sv ${label==='1M Net Flows'?(flow1m>=0?'pos':'neg'):''}">${val}</div>
      ${sub ? `<div class="ss">${sub}</div>` : ''}
    </div>`).join('');

  // Asset class donut
  const acData = d.asset_class_mix.filter(r => r.fum > 0);
  const totalFum = acData.reduce((s,r) => s+r.fum, 0);
  if (_acChart) _acChart.destroy();
  _acChart = new Chart(document.getElementById('chart-ac').getContext('2d'), {
    type: 'doughnut',
    plugins: [{
      id: 'centre',
      beforeDraw(chart) {
        const {ctx, chartArea:{top,left,width,height}} = chart;
        ctx.save();
        const cx = left+width/2, cy = top+height/2;
        ctx.textAlign='center'; ctx.textBaseline='middle';
        ctx.font='bold 13px Inter,sans-serif'; ctx.fillStyle='#0f172a';
        ctx.fillText(fmtFum(totalFum), cx, cy-7);
        ctx.font='10px Inter,sans-serif'; ctx.fillStyle='#94a3b8';
        ctx.fillText('Total AUM', cx, cy+8);
        ctx.restore();
      }
    }],
    data: {
      labels: acData.map(r => r.ac),
      datasets: [{
        data: acData.map(r => r.fum),
        backgroundColor: acData.map(r => acColor(r.ac)),
        borderWidth: 2, borderColor: '#fff',
        hoverOffset: 6,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false, cutout: '62%',
      plugins: {
        legend: { position: 'bottom', labels: { font:{size:10}, padding:8, boxWidth:10, color:'#94a3b8' } },
        tooltip: { callbacks: {
          label: ctx => {
            const r = acData[ctx.dataIndex];
            const p = totalFum > 0 ? (r.fum/totalFum*100).toFixed(1) : '0';
            return [' ' + fmtFum(r.fum) + '  (' + p + '%)', ' ' + r.cnt + ' ETFs'];
          }
        }}
      }
    }
  });

  // AC count bars
  const cntMax = Math.max(...d.asset_class_mix.map(r => r.cnt), 1);
  document.getElementById('ac-count-bars').innerHTML =
    d.asset_class_mix.map(r =>
      hbar(r.ac, r.cnt, cntMax, acColor(r.ac), r.cnt + ' ETF' + (r.cnt!==1?'s':''), fmtFum(r.fum))
    ).join('');

  // Populate asset class filter
  const acFilter = document.getElementById('etf-ac-filter');
  const acSet = [...new Set(d.etfs.map(e => e.asset_class).filter(Boolean))].sort();
  acSet.forEach(ac => {
    const opt = document.createElement('option');
    opt.value = ac; opt.textContent = ac;
    acFilter.appendChild(opt);
  });

  // ETF table
  renderETFTable();
  document.getElementById('etf-search').addEventListener('input', renderETFTable);
  document.getElementById('etf-ac-filter').addEventListener('change', renderETFTable);

  // Related articles
  if (d.related_articles && d.related_articles.length) {
    document.getElementById('articles-section').classList.remove('hidden');
    document.getElementById('articles-grid').innerHTML = d.related_articles.map(a => `
      <a href="/articles/${a.slug}" class="block rounded-lg border border-slate-200
         hover:border-blue-400 hover:shadow-sm transition-all p-4 bg-white">
        <span class="text-xs font-semibold text-blue-600">${a.category}</span>
        <h3 class="text-sm font-bold text-slate-900 mt-1.5 leading-snug line-clamp-2">${a.title}</h3>
        <p class="text-xs text-slate-500 mt-1 line-clamp-2">${a.subtitle||''}</p>
        <p class="text-xs text-slate-400 mt-2">${a.date}</p>
      </a>`).join('');
  }

  document.getElementById('loading').classList.add('hidden');
  document.getElementById('page').classList.remove('hidden');
}

function renderETFTable() {
  const q = (document.getElementById('etf-search').value || '').toLowerCase();
  const ac = document.getElementById('etf-ac-filter').value;
  const etfs = (_issuerData.etfs || []).filter(e =>
    (!q || (e.code+' '+(e.name||'')).toLowerCase().includes(q)) &&
    (!ac || e.asset_class === ac)
  );
  const tbody = document.getElementById('etf-table');
  tbody.innerHTML = etfs.map(e => {
    const mer_v = e.expense_ratio;
    const flow = e.fund_flow_1m;
    return `<tr class="border-b border-slate-100 hover:bg-slate-50 cursor-pointer"
                onclick="window.location='/dashboard#etf=${e.code}'">
      <td class="py-2 pr-3 font-bold text-blue-400 font-mono text-xs">${e.code}</td>
      <td class="py-2 pr-3 text-xs text-slate-700" style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${e.name||''}">${e.name||'—'}</td>
      <td class="py-2 pr-3"><span class="text-xs px-1.5 py-0.5 rounded font-medium" style="background:${acColor(e.asset_class)}22;color:${acColor(e.asset_class)}">${e.asset_class||'—'}</span></td>
      <td class="py-2 pr-3 text-right tabular-nums text-sm font-semibold text-white">${fmtFum(e.fund_size_aud_millions)}</td>
      <td class="py-2 pr-3 text-right tabular-nums text-xs text-slate-400">${mer_v!=null?mer_v.toFixed(2)+'%':'—'}</td>
      <td class="py-2 pr-3 text-right tabular-nums text-sm font-semibold ${pcls(e.return_1y)}">${pct(e.return_1y,1)}</td>
      <td class="py-2 pr-3 text-right tabular-nums text-xs ${pcls(e.return_3y)}">${pct(e.return_3y,1)}</td>
      <td class="py-2 pr-3 text-right tabular-nums text-xs text-slate-400">${e.distribution_yield!=null?e.distribution_yield.toFixed(1)+'%':'—'}</td>
      <td class="py-2 pr-3 text-right tabular-nums text-xs ${pcls(flow)}">${fmtFum(flow)}</td>
      <td class="py-2 text-xs text-slate-500">${e.inception_date||'—'}</td>
    </tr>`;
  }).join('') || '<tr><td colspan="10" class="text-center text-slate-500 py-4">No ETFs matched</td></tr>';
}
"""

PAGE_ISSUER = _page(
    'Issuer Profile',
    'ETF products, AUM, performance and flows',
    _ISSUER_BODY,
    _ISSUER_JS,
)

def get_issuer_page(slug: str) -> str:
    return PAGE_ISSUER


# ---------------------------------------------------------------------------
# TOTAL MARKET ANALYSER PAGE
# ---------------------------------------------------------------------------
_TM_BODY = """
<div id="tm-tabs" class="flex gap-2 flex-wrap mb-6">
  <button data-tm="exchange"   class="tm-btn active-tm px-4 py-2 rounded-lg text-sm font-semibold border transition-colors">Exchange</button>
  <button data-tm="topfunds"   class="tm-btn px-4 py-2 rounded-lg text-sm font-semibold border transition-colors">Top Funds</button>
  <button data-tm="issuers"    class="tm-btn px-4 py-2 rounded-lg text-sm font-semibold border transition-colors">Issuers</button>
  <button data-tm="assetclass" class="tm-btn px-4 py-2 rounded-lg text-sm font-semibold border transition-colors">Asset Class</button>
  <button data-tm="geography"  class="tm-btn px-4 py-2 rounded-lg text-sm font-semibold border transition-colors">Geography</button>
  <button data-tm="strategy"   class="tm-btn px-4 py-2 rounded-lg text-sm font-semibold border transition-colors">Strategy &amp; Factors</button>
</div>

<!-- EXCHANGE -->
<div id="tm-exchange">
  <div id="tm-exch-stats-row" class="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-5"></div>
  <div class="card mb-5">
    <div class="flex items-start justify-between flex-wrap gap-3 mb-4">
      <div>
        <h2 class="font-semibold text-slate-800 mb-0.5">Australian ETF Market — Historical AUM</h2>
        <p class="text-xs text-slate-400">Monthly ASX-listed ETF market cap since July 2013. Cboe Australia data is current-month only.</p>
      </div>
      <p class="text-xs text-slate-500" id="tm-ts"></p>
    </div>
    <div style="height:320px"><canvas id="tm-exch-hist"></canvas></div>
  </div>
  <div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
    <div class="card">
      <h2 class="font-semibold text-slate-800 text-sm mb-1">Current Market Cap by Exchange</h2>
      <p class="text-xs text-slate-400 mb-4">Live FUM from ETF database</p>
      <div style="height:240px" class="flex items-center justify-center">
        <canvas id="tm-exch-donut" style="max-height:240px"></canvas>
      </div>
    </div>
    <div class="card flex flex-col justify-center">
      <div id="tm-exch-breakdown" class="space-y-5"></div>
    </div>
  </div>
</div>

<!-- TOP FUNDS -->
<div id="tm-topfunds" class="hidden">
  <div class="card mb-5">
    <h2 class="font-semibold text-slate-800 mb-1">Top 10 ETFs — Historical FUM</h2>
    <p class="text-xs text-slate-400 mb-4">Monthly AUM for the 10 largest ETFs by current market cap</p>
    <div style="height:360px"><canvas id="tm-tf-hist"></canvas></div>
  </div>
  <div class="card">
    <div class="flex items-center justify-between mb-4">
      <h2 class="font-semibold text-slate-800 text-sm">Top 20 ETFs by FUM — vs Prior Year</h2>
      <span id="tm-tf-date" class="text-xs text-slate-400"></span>
    </div>
    <div class="overflow-x-auto">
      <table>
        <thead><tr>
          <th>#</th><th>Code</th><th>Name</th><th>Issuer</th>
          <th class="text-right">FUM</th><th class="text-right">Prior Year</th><th class="text-right">Change</th>
        </tr></thead>
        <tbody id="tm-tf-table"></tbody>
      </table>
    </div>
  </div>
</div>

<!-- ISSUERS -->
<div id="tm-issuers" class="hidden">
  <div class="card mb-5">
    <h2 class="font-semibold text-slate-800 mb-1">Top Issuers — Historical AUM</h2>
    <p class="text-xs text-slate-400 mb-4">Stacked monthly AUM for the top 10 fund managers since July 2013</p>
    <div style="height:380px"><canvas id="tm-iss-hist"></canvas></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-slate-800 text-sm mb-4">Current Market Share</h2>
    <div id="tm-iss-bars" class="space-y-2"></div>
  </div>
</div>

<!-- ASSET CLASS -->
<div id="tm-assetclass" class="hidden">
  <div class="card mb-5">
    <h2 class="font-semibold text-slate-800 mb-1">Market AUM by Asset Class — Historical</h2>
    <p class="text-xs text-slate-400 mb-4">Stacked monthly market cap by broad asset class since July 2013</p>
    <div style="height:380px"><canvas id="tm-ac-hist"></canvas></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-slate-800 text-sm mb-4">Current Snapshot</h2>
    <div id="tm-ac-bars" class="space-y-2"></div>
  </div>
</div>

<!-- GEOGRAPHY -->
<div id="tm-geography" class="hidden">
  <div class="card mb-5">
    <h2 class="font-semibold text-slate-800 mb-1">Market AUM by Geographic Focus — Historical</h2>
    <p class="text-xs text-slate-400 mb-4">Stacked monthly market cap by geographic investment mandate since July 2013</p>
    <div style="height:380px"><canvas id="tm-geo-hist"></canvas></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-slate-800 text-sm mb-4">Current Snapshot</h2>
    <div id="tm-geo-bars" class="space-y-2"></div>
  </div>
</div>

<!-- STRATEGY & FACTORS -->
<div id="tm-strategy" class="hidden">
  <div class="card mb-5">
    <h2 class="font-semibold text-slate-800 mb-1">Market AUM by Strategy — Historical</h2>
    <p class="text-xs text-slate-400 mb-4">Stacked monthly market cap by investment style and factor strategy since July 2013</p>
    <div style="height:380px"><canvas id="tm-strat-hist"></canvas></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-slate-800 text-sm mb-4">Current Snapshot</h2>
    <div id="tm-strat-bars" class="space-y-2"></div>
  </div>
</div>
"""

_TM_JS = """
const TM_PALETTE = [
  '#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6',
  '#f97316','#06b6d4','#84cc16','#ec4899','#14b8a6','#a855f7','#6366f1',
];
const TM_BTN_ACTIVE = 'active-tm bg-blue-600 border-blue-600 text-white';
const TM_BTN_IDLE   = 'bg-white border-slate-200 text-slate-600 hover:border-blue-400 hover:text-blue-600';
const TM_ALL_TABS   = ['exchange','topfunds','issuers','assetclass','geography','strategy'];

function setTmBtns(active) {
  document.querySelectorAll('.tm-btn').forEach(b => {
    const on = b.dataset.tm === active;
    b.className = 'tm-btn px-4 py-2 rounded-lg text-sm font-semibold border transition-colors ' + (on ? TM_BTN_ACTIVE : TM_BTN_IDLE);
  });
  TM_ALL_TABS.forEach(id => {
    document.getElementById('tm-' + id).classList.toggle('hidden', id !== active);
  });
}

const _tmLoaded = {};

document.getElementById('tm-tabs').addEventListener('click', async e => {
  const btn = e.target.closest('[data-tm]');
  if (!btn) return;
  const tab = btn.dataset.tm;
  setTmBtns(tab);
  if (!_tmLoaded[tab]) {
    _tmLoaded[tab] = true;
    if (tab === 'exchange')   await loadTmExchange();
    if (tab === 'topfunds')   await loadTmTopFunds();
    if (tab === 'issuers')    await loadTmIssuers();
    if (tab === 'assetclass') await loadTmAssetClass();
    if (tab === 'geography')  await loadTmGeography();
    if (tab === 'strategy')   await loadTmStrategy();
  }
});

// Shared: stacked area chart
function makeStackedArea(canvasId, dates, series) {
  const ctx = document.getElementById(canvasId)?.getContext('2d');
  if (!ctx) return;
  const datasets = series.map((s, i) => ({
    label: s.name,
    data: s.data,
    backgroundColor: TM_PALETTE[i % TM_PALETTE.length] + '99',
    borderColor:     TM_PALETTE[i % TM_PALETTE.length],
    borderWidth: 1.5, fill: true, tension: 0.3, pointRadius: 0,
  }));
  new Chart(ctx, {
    type: 'line',
    data: { labels: dates, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { position: 'bottom', labels: { boxWidth: 10, font: { size: 10 }, color: '#94a3b8', padding: 10 } },
        tooltip: {
          callbacks: {
            label: c => ` ${c.dataset.label}: A$${c.parsed.y.toFixed(1)}B`,
            footer: items => `Total: A$${items.reduce((s,i) => s + i.parsed.y, 0).toFixed(1)}B`,
          }
        }
      },
      scales: {
        x: { type: 'time', time: { unit: 'year', displayFormats: { year: 'yyyy' } },
             ticks: { font: { size: 10 }, color: '#64748b', maxTicksLimit: 12 }, grid: { color: '#f1f5f9' } },
        y: { stacked: true,
             ticks: { font: { size: 10 }, color: '#64748b', callback: v => 'A$' + v + 'B' }, grid: { color: '#f1f5f9' } }
      }
    }
  });
}

// Shared: horizontal bar snapshot
function renderHBars(containerId, series, dates) {
  const idx = dates.length - 1;
  const items = series.map((s, i) => ({
    name: s.name, val: s.data[idx] || 0, color: TM_PALETTE[i % TM_PALETTE.length]
  })).filter(x => x.val > 0).sort((a, b) => b.val - a.val);
  const total = items.reduce((s, x) => s + x.val, 0) || 1;
  document.getElementById(containerId).innerHTML = items.map(x => `
    <div class="flex items-center gap-3">
      <div class="w-3 h-3 rounded-sm shrink-0" style="background:${x.color}"></div>
      <div class="flex-1 min-w-0">
        <div class="flex justify-between text-xs mb-1">
          <span class="text-slate-700 truncate">${x.name}</span>
          <span class="text-slate-800 font-semibold ml-2 shrink-0">A$${x.val.toFixed(1)}B
            <span class="text-slate-500 font-normal">${(x.val/total*100).toFixed(1)}%</span></span>
        </div>
        <div class="h-1.5 bg-slate-200 rounded-full overflow-hidden">
          <div class="h-full rounded-full" style="width:${(x.val/total*100).toFixed(1)}%;background:${x.color}"></div>
        </div>
      </div>
    </div>`).join('');
}

// ── Exchange tab ─────────────────────────────────────────────────────────────
async function loadTmExchange() {
  const [histD, exD] = await Promise.all([
    api('/api/v1/history/industry'),
    api('/api/v1/exchanges'),
  ]);
  document.getElementById('tm-ts').textContent = new Date().toLocaleDateString('en-AU', {month:'long',year:'numeric'});

  // Stats row: Total AUM, YoY growth, monthly flows, ETF count
  if (histD.data && histD.data.length >= 2) {
    const latest = histD.data[histD.data.length - 1];
    const latestDate = new Date(latest.date);
    const targetDate = new Date(latestDate);
    targetDate.setFullYear(targetDate.getFullYear() - 1);
    const prior = histD.data.reduce((best, r) => {
      const d = new Date(r.date);
      return Math.abs(d - targetDate) < Math.abs(new Date(best.date) - targetDate) ? r : best;
    }, histD.data[0]);
    const yoy = prior.aum_b > 0 ? ((latest.aum_b - prior.aum_b) / prior.aum_b * 100).toFixed(1) : null;
    const flowB = latest.flows_m != null ? (latest.flows_m / 1000).toFixed(2) : null;
    document.getElementById('tm-exch-stats-row').innerHTML = [
      ['Total Market AUM', `A$${latest.aum_b.toFixed(1)}B`, 'all exchanges'],
      ['YoY Growth', yoy !== null ? `${+yoy >= 0 ? '+' : ''}${yoy}%` : '—', `vs ${prior.date.slice(0,7)}`],
      ['Monthly Net Flows', flowB !== null ? `A$${flowB}B` : '—', latest.date.slice(0,7)],
      ['Listed ETFs', latest.etf_count, 'ASX-listed products'],
    ].map(([l,v,s]) => {
      const isPos = String(v).startsWith('+'); const isNeg = String(v).startsWith('-');
      return `<div class="card"><div class="sl">${l}</div><div class="sv ${isPos?'pos':isNeg?'neg':''}">${v}</div><div class="ss">${s}</div></div>`;
    }).join('');
  }

  // Historical AUM line chart
  if (histD.data) {
    const dates = histD.data.map(r => r.date);
    const aums  = histD.data.map(r => +(r.aum_b || 0).toFixed(1));
    const ctx = document.getElementById('tm-exch-hist').getContext('2d');
    new Chart(ctx, {
      type: 'line',
      data: { labels: dates, datasets: [{
        label: 'ASX-listed ETF Market Cap',
        data: aums,
        borderColor: '#3b82f6', backgroundColor: '#3b82f620',
        fill: true, tension: 0.3, pointRadius: 0, borderWidth: 2,
      }]},
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { labels: { color: '#94a3b8', font: { size: 11 } } },
          tooltip: { callbacks: { label: c => ` A$${c.parsed.y.toFixed(1)}B` } }
        },
        scales: {
          x: { type: 'time', time: { unit: 'year', displayFormats: { year: 'yyyy' } },
               ticks: { font: { size: 10 }, color: '#64748b' }, grid: { color: '#f1f5f9' } },
          y: { ticks: { font: { size: 10 }, color: '#64748b', callback: v => 'A$' + v + 'B' },
               grid: { color: '#f1f5f9' } }
        }
      }
    });
  }

  // Exchange donut + breakdown
  if (exD.exchanges) {
    const exchanges = exD.exchanges.filter(e => e.exchange && e.total_fum > 0);
    const labels = exchanges.map(e => e.exchange === 'CXA' ? 'Cboe Australia' : e.exchange);
    const fums   = exchanges.map(e => +(e.total_fum / 1000).toFixed(2));
    const total  = fums.reduce((a, b) => a + b, 0);
    const ctx2 = document.getElementById('tm-exch-donut').getContext('2d');
    new Chart(ctx2, {
      type: 'doughnut',
      data: { labels, datasets: [{ data: fums, backgroundColor: TM_PALETTE.slice(0, exchanges.length), borderWidth: 2, borderColor: '#fff' }] },
      options: {
        responsive: true, maintainAspectRatio: false, cutout: '62%',
        plugins: {
          legend: { position: 'bottom', labels: { color: '#94a3b8', boxWidth: 10, font: { size: 11 }, padding: 12 } },
          tooltip: { callbacks: { label: c => ` A$${c.parsed.toFixed(1)}B (${(c.parsed/total*100).toFixed(1)}%)` } }
        }
      }
    });
    document.getElementById('tm-exch-breakdown').innerHTML = exchanges.map((e, i) => {
      const pct_v = (e.total_fum / 1000 / total * 100).toFixed(1);
      const fum = (e.total_fum / 1000).toFixed(1);
      const label = e.exchange === 'CXA' ? 'Cboe Australia' : e.exchange;
      return `<div>
        <div class="flex items-baseline justify-between mb-1">
          <span class="flex items-center gap-2">
            <span class="w-3 h-3 rounded-sm shrink-0 inline-block" style="background:${TM_PALETTE[i]}"></span>
            <span class="text-sm font-semibold text-slate-800">${label}</span>
          </span>
          <span class="text-sm font-bold text-slate-800">A$${fum}B</span>
        </div>
        <div class="flex justify-between text-xs text-slate-400 mb-1.5">
          <span>${e.etf_count} ETFs</span><span>${pct_v}% of market</span>
        </div>
        <div class="h-1.5 bg-slate-200 rounded-full overflow-hidden">
          <div class="h-full rounded-full" style="width:${pct_v}%;background:${TM_PALETTE[i]}"></div>
        </div>
      </div>`;
    }).join('');
  }
}

// ── Top Funds tab ─────────────────────────────────────────────────────────────
async function loadTmTopFunds() {
  const d = await api('/api/v1/history/top-funds');
  if (!d.funds) return;

  document.getElementById('tm-tf-date').textContent = d.prior_date
    ? `vs ${d.prior_date.slice(0,7)}`  : '';

  // Historical line chart — top 10
  if (d.history && d.history.dates.length) {
    const ctx = document.getElementById('tm-tf-hist').getContext('2d');
    new Chart(ctx, {
      type: 'line',
      data: {
        labels: d.history.dates,
        datasets: d.history.series.map((s, i) => ({
          label: s.code,
          data: s.data,
          borderColor: TM_PALETTE[i % TM_PALETTE.length],
          backgroundColor: 'transparent',
          borderWidth: 2, tension: 0.3, pointRadius: 0, spanGaps: true,
        }))
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { position: 'bottom', labels: { boxWidth: 10, font: { size: 10 }, color: '#94a3b8', padding: 8 } },
          tooltip: { callbacks: { label: c => ` ${c.dataset.label}: A$${(c.parsed.y/1000).toFixed(2)}B` } }
        },
        scales: {
          x: { type: 'time', time: { unit: 'year', displayFormats: { year: 'yyyy' } },
               ticks: { font: { size: 10 }, color: '#64748b' }, grid: { color: '#f1f5f9' } },
          y: { ticks: { font: { size: 10 }, color: '#64748b', callback: v => 'A$' + (v/1000).toFixed(0) + 'B' },
               grid: { color: '#f1f5f9' } }
        }
      }
    });
  }

  // Ranking table — top 20
  document.getElementById('tm-tf-table').innerHTML = d.funds.map((f, i) => {
    const chg = f.change_pct;
    const chgAbs = f.change_abs;
    const chgHtml = chg != null
      ? `<div class="${chg >= 0 ? 'pos' : 'neg'} font-semibold text-sm">${chg >= 0 ? '+' : ''}${chg}%</div>
         <div class="text-xs text-slate-400">${chgAbs >= 0 ? '+' : ''}A$${Math.abs(chgAbs)}M</div>`
      : '<span class="text-slate-300">new</span>';
    return `<tr>
      <td class="text-slate-400 text-xs">${i+1}</td>
      <td><a href="/dashboard?etf=${f.code}" class="font-mono font-semibold text-blue-600 hover:underline text-sm">${f.code}</a></td>
      <td class="text-xs text-slate-600 max-w-xs" style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${f.name}</td>
      <td class="text-xs text-slate-500">${f.issuer || '—'}</td>
      <td class="text-right font-semibold text-slate-800 text-sm">A$${(f.fum_m/1000).toFixed(2)}B</td>
      <td class="text-right text-slate-400 text-xs">${f.fum_prior_m != null ? 'A$' + (f.fum_prior_m/1000).toFixed(2) + 'B' : '—'}</td>
      <td class="text-right">${chgHtml}</td>
    </tr>`;
  }).join('');
}

// ── Issuers tab ───────────────────────────────────────────────────────────────
async function loadTmIssuers() {
  const d = await api('/api/v1/history/issuers');
  if (!d.dates) return;
  makeStackedArea('tm-iss-hist', d.dates, d.series);
  const idx = d.dates.length - 1;
  const items = d.series.map(s => ({
    name: s.name, val: s.data[idx] || 0, color: issColor(s.name)
  })).filter(x => x.val > 0).sort((a, b) => b.val - a.val);
  const total = items.reduce((s, x) => s + x.val, 0) || 1;
  document.getElementById('tm-iss-bars').innerHTML = items.map(x => `
    <div class="flex items-center gap-3">
      <div class="w-3 h-3 rounded-sm shrink-0" style="background:${x.color}"></div>
      <div class="flex-1 min-w-0">
        <div class="flex justify-between text-xs mb-1">
          <span class="font-medium">${issLink(x.name)}</span>
          <span class="text-slate-800 font-semibold ml-2 shrink-0">A$${x.val.toFixed(1)}B
            <span class="text-slate-500 font-normal">${(x.val/total*100).toFixed(1)}%</span></span>
        </div>
        <div class="h-1.5 bg-slate-200 rounded-full overflow-hidden">
          <div class="h-full rounded-full" style="width:${(x.val/total*100).toFixed(1)}%;background:${x.color}"></div>
        </div>
      </div>
    </div>`).join('');
}

// ── Asset Class tab ───────────────────────────────────────────────────────────
async function loadTmAssetClass() {
  const d = await api('/api/v1/history/asset-classes');
  if (!d.dates) return;
  makeStackedArea('tm-ac-hist', d.dates, d.series);
  renderHBars('tm-ac-bars', d.series, d.dates);
}

// ── Geography tab ─────────────────────────────────────────────────────────────
async function loadTmGeography() {
  const d = await api('/api/v1/history/geography');
  if (!d.dates) return;
  makeStackedArea('tm-geo-hist', d.dates, d.series);
  renderHBars('tm-geo-bars', d.series, d.dates);
}

// ── Strategy & Factors tab ────────────────────────────────────────────────────
async function loadTmStrategy() {
  const d = await api('/api/v1/history/strategy');
  if (!d.dates) return;
  makeStackedArea('tm-strat-hist', d.dates, d.series);
  renderHBars('tm-strat-bars', d.series, d.dates);
}

// ── Init ──────────────────────────────────────────────────────────────────────
async function init() {
  document.getElementById('loading').classList.add('hidden');
  document.getElementById('page').classList.remove('hidden');
  setTmBtns('exchange');
  _tmLoaded['exchange'] = true;
  await loadTmExchange();
  document.getElementById('ts').textContent = new Date().toLocaleDateString('en-AU', {month:'long',year:'numeric'});
}
"""

PAGE_TOTAL_MARKET = _page(
    'Total Market Analyser',
    'Historic market size by exchange, asset class, geography and strategy',
    _TM_BODY,
    _TM_JS,
)

# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
PAGES = {
    'fum':           PAGE_FUM,
    'listings':      PAGE_LISTINGS,
    'returns':       PAGE_RETURNS,
    'expense':       PAGE_EXPENSE,
    'issuers':       PAGE_ISSUERS,
    'nav':           PAGE_NAV,
    'upcoming':      PAGE_UPCOMING,
    'asset-classes': PAGE_ASSET_CLASSES,
    'total-market':  PAGE_TOTAL_MARKET,
}

def get_insights_page(name: str) -> str | None:
    return PAGES.get(name)
