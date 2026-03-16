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
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  body { font-family: Inter, system-ui, -apple-system, sans-serif; background: #f8fafc; color: #1e293b; }
  .card { background: #fff; border-radius: 12px; box-shadow: 0 1px 3px rgba(0,0,0,.07); border: 1px solid #e2e8f0; padding: 1.25rem; }
  table { width: 100%; border-collapse: collapse; }
  th { text-align: left; font-size: .7rem; font-weight: 700; text-transform: uppercase; letter-spacing: .04em;
       color: #64748b; padding: .45rem .75rem; background: #f8fafc; border-bottom: 1px solid #e2e8f0; }
  td { padding: .45rem .75rem; font-size: .8rem; border-bottom: 1px solid #f1f5f9; vertical-align: middle; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: #f8fafc; }
  .bar-track { height: 6px; background: #e2e8f0; border-radius: 3px; overflow: hidden; }
  .bar-fill  { height: 100%; border-radius: 3px; transition: width .5s ease; }
  .badge { display: inline-block; padding: .15rem .45rem; border-radius: 4px; font-size: .68rem; font-weight: 600; }
  .spinner { width: 28px; height: 28px; border: 3px solid #e2e8f0; border-top-color: #3b82f6;
             border-radius: 50%; animation: spin .7s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
  .sv { font-size: 1.9rem; font-weight: 800; line-height: 1; }
  .sl { font-size: .68rem; font-weight: 700; text-transform: uppercase; letter-spacing: .04em; color: #64748b; }
  .ss { font-size: .73rem; color: #94a3b8; margin-top: .2rem; }
  .pos { color: #16a34a; } .neg { color: #dc2626; }
  .hbar-row { display: flex; align-items: center; gap: 8px; margin-bottom: 6px; }
  .hbar-label { font-size: .75rem; color: #374151; font-weight: 500; width: 110px; flex-shrink: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .hbar-track { flex: 1; height: 16px; background: #e2e8f0; border-radius: 4px; overflow: hidden; }
  .hbar-fill  { height: 100%; border-radius: 4px; transition: width .6s ease; }
  .hbar-val   { font-size: .73rem; color: #475569; width: 52px; text-align: right; flex-shrink: 0; }
  .hbar-sub   { font-size: .65rem; color: #94a3b8; width: 32px; text-align: right; flex-shrink: 0; }
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
  'Vanguard': '#c41230', 'BetaShares': '#e2211c', 'iShares': '#13294b',
  'VanEck': '#f7941d', 'Global X': '#00adef', 'SPDR': '#1a9dd9',
  'DFA': '#004b87', 'Magellan': '#b8141a', 'Perpetual': '#003087',
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
"""

def _page(title, subtitle, body_html, init_js):
    return f"""{_HEAD.replace('{title}', title)}
<body>
<header class="bg-gradient-to-r from-blue-700 via-blue-800 to-indigo-900 text-white">
  <div class="max-w-7xl mx-auto px-5 py-4 flex items-center gap-5">
    <a href="/dashboard" class="text-blue-200 hover:text-white text-sm font-medium transition-colors shrink-0">&#8592; Dashboard</a>
    <div class="flex-1 min-w-0">
      <h1 class="text-xl font-bold">{title}</h1>
      <p class="text-blue-200 text-xs mt-0.5">{subtitle}</p>
    </div>
    <span id="ts" class="text-blue-300 text-xs shrink-0"></span>
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
    <h2 class="font-semibold text-sm text-gray-700 mb-4">Issuers by FUM</h2>
    <div id="issuer-bars"></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-gray-700 mb-4">Asset Classes by FUM</h2>
    <div class="relative" style="height:260px"><canvas id="chart-ac"></canvas></div>
  </div>
</div>
<div class="card">
  <h2 class="font-semibold text-sm text-gray-700 mb-3">Top ETFs by FUM</h2>
  <div class="overflow-x-auto"><table>
    <thead><tr>
      <th>#</th><th>Code</th><th>Name</th><th>Issuer</th><th>Asset Class</th>
      <th class="text-right">FUM</th><th class="text-right">% Market</th><th style="min-width:100px"></th>
    </tr></thead>
    <tbody id="fum-table"></tbody>
  </table></div>
</div>
<div class="card">
  <h2 class="font-semibold text-sm text-gray-700 mb-4">Top 5 ETFs by FUM — per Asset Class</h2>
  <div id="per-class" class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4"></div>
</div>
<div class="card">
  <h2 class="font-semibold text-sm text-gray-700 mb-4">Top 5 ETFs by FUM — per Issuer</h2>
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
    <td class="text-gray-400 tabular-nums">${i + 1}</td>
    <td class="font-bold text-blue-600">${e.code}</td>
    <td class="text-gray-600 max-w-xs" style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${e.name}">${e.name}</td>
    <td class="text-gray-500">${e.issuer || '—'}</td>
    <td><span class="badge" style="background:${acColor(e.asset_class)}22;color:${acColor(e.asset_class)}">${e.asset_class || '—'}</span></td>
    <td class="text-right font-semibold tabular-nums">${fmtFum(e.fum)}</td>
    <td class="text-right text-gray-500 tabular-nums">${e.pct}%</td>
    <td><div class="bar-track"><div class="bar-fill bg-blue-400" style="width:${Math.min(e.fum / topMax * 100, 100).toFixed(1)}%"></div></div></td>
  </tr>`).join('');

  // Per class mini tables
  const classCard = (ac_name, etfs) => `<div class="border border-gray-100 rounded-lg p-3">
    <div class="text-xs font-bold uppercase mb-2" style="color:${acColor(ac_name)}">${ac_name}</div>
    ${etfs.map((e, i) => `<div class="flex items-center gap-2 py-1 ${i < etfs.length - 1 ? 'border-b border-gray-50' : ''}">
      <span class="text-gray-300 text-xs w-3 tabular-nums">${i + 1}</span>
      <span class="font-bold text-blue-600 text-xs w-10">${e.code}</span>
      <span class="flex-1 text-xs text-gray-500 truncate" title="${e.name}">${e.name}</span>
      <span class="text-xs font-semibold text-gray-700 ml-1 tabular-nums">${fmtFum(e.fum)}</span>
    </div>`).join('')}
  </div>`;

  document.getElementById('per-class').innerHTML =
    Object.entries(d.top_per_asset_class).map(([ac_name, etfs]) => classCard(ac_name, etfs)).join('');

  document.getElementById('per-issuer').innerHTML =
    Object.entries(d.top_per_issuer).map(([iss_name, etfs]) => `<div class="border border-gray-100 rounded-lg p-3">
      <div class="text-xs font-bold uppercase mb-2" style="color:${issColor(iss_name)}">${iss_name}</div>
      ${etfs.map((e, i) => `<div class="flex items-center gap-2 py-1 ${i < etfs.length - 1 ? 'border-b border-gray-50' : ''}">
        <span class="text-gray-300 text-xs w-3 tabular-nums">${i + 1}</span>
        <span class="font-bold text-blue-600 text-xs w-10">${e.code}</span>
        <span class="flex-1 text-xs text-gray-500 truncate" title="${e.name}">${e.name}</span>
        <span class="text-xs font-semibold text-gray-700 ml-1 tabular-nums">${fmtFum(e.fum)}</span>
      </div>`).join('')}
    </div>`).join('');

  document.getElementById('loading').classList.add('hidden');
  document.getElementById('page').classList.remove('hidden');
}
"""

PAGE_FUM = _page(
    'FUM Analysis',
    'Total funds under management across Australian-listed ETFs by issuer, asset class, and individual fund',
    _FUM_BODY,
    _FUM_JS,
)


# ---------------------------------------------------------------------------
# LISTINGS PAGE
# ---------------------------------------------------------------------------
_LISTINGS_BODY = """
<div id="hero" class="grid grid-cols-2 sm:grid-cols-4 gap-4"></div>
<div class="card">
  <h2 class="font-semibold text-sm text-gray-700 mb-4">ETFs Listed by Year</h2>
  <div class="relative" style="height:260px"><canvas id="chart-years"></canvas></div>
</div>
<div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
  <div class="card">
    <h2 class="font-semibold text-sm text-gray-700 mb-3">Most Recent Listings</h2>
    <div class="overflow-x-auto"><table>
      <thead><tr><th>Code</th><th>Name</th><th>Issuer</th><th>Inception</th></tr></thead>
      <tbody id="tbl-recent"></tbody>
    </table></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-gray-700 mb-3">Longest Running ETFs</h2>
    <div class="overflow-x-auto"><table>
      <thead><tr><th>Code</th><th>Name</th><th>Issuer</th><th>Inception</th></tr></thead>
      <tbody id="tbl-oldest"></tbody>
    </table></div>
  </div>
</div>
<div class="grid grid-cols-1 lg:grid-cols-3 gap-5">
  <div class="card">
    <h2 class="font-semibold text-sm text-gray-700 mb-4">ETFs Listed — by Issuer</h2>
    <div id="bars-issuer"></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-gray-700 mb-4">ETFs Listed — by Asset Class</h2>
    <div id="bars-asset"></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-gray-700 mb-4">ETFs by Exchange</h2>
    <div id="bars-exchange"></div>
    <div class="mt-5">
      <h3 class="font-semibold text-xs text-gray-500 uppercase mb-3">ETFs per Year — Top Issuers</h3>
      <div id="tbl-issuer-year" class="text-xs text-gray-600"></div>
    </div>
  </div>
</div>
"""

_LISTINGS_JS = """
async function init() {
  const d = await api('/api/v1/insights/listings');
  document.getElementById('ts').textContent = new Date().toLocaleTimeString('en-AU');

  const recentDate = d.recent[0]?.inception_date || '—';
  const oldestDate = d.oldest[0]?.inception_date || '—';
  const peakYear = d.by_year.reduce((a, b) => b.count > a.count ? b : a, { year: '—', count: 0 });

  document.getElementById('hero').innerHTML = [
    ['Total ETFs', d.total, 'ASX + CXA'],
    ['Newest', d.recent[0]?.code || '—', recentDate],
    ['Oldest', d.oldest[0]?.code || '—', oldestDate],
    ['Most Active Year', peakYear.year, peakYear.count + ' new listings'],
  ].map(([l, v, s]) => `<div class="card"><div class="sl">${l}</div><div class="sv">${v}</div><div class="ss">${s}</div></div>`).join('');

  // Year bar chart
  const years = d.by_year.filter(r => /^\d{4}$/.test(r.year) && +r.year >= 2001);
  new Chart(document.getElementById('chart-years').getContext('2d'), {
    type: 'bar',
    data: {
      labels: years.map(r => r.year),
      datasets: [{ data: years.map(r => r.count), backgroundColor: '#3b82f6', borderRadius: 4, borderSkipped: false }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: ctx => ' ' + ctx.parsed.y + ' ETFs listed' } } },
      scales: { x: { ticks: { font: { size: 10 } } }, y: { ticks: { font: { size: 10 }, stepSize: 5 }, grid: { color: '#f1f5f9' } } },
    },
  });

  const etfRow = (e) => `<tr>
    <td class="font-bold text-blue-600">${e.code}</td>
    <td class="text-gray-600" style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${e.name}">${e.name}</td>
    <td class="text-gray-500">${e.issuer || '—'}</td>
    <td class="text-gray-400 tabular-nums text-xs">${e.inception_date || '—'}</td>
  </tr>`;
  document.getElementById('tbl-recent').innerHTML = d.recent.map(etfRow).join('');
  document.getElementById('tbl-oldest').innerHTML = d.oldest.map(etfRow).join('');

  // Issuer bar
  const issMax = d.by_issuer[0]?.count || 1;
  document.getElementById('bars-issuer').innerHTML = d.by_issuer.slice(0, 15)
    .map(r => hbar(r.issuer, r.count, issMax, issColor(r.issuer), r.count + ' ETFs', ''))
    .join('');

  // Asset class bar
  const acMax = d.by_asset_class[0]?.count || 1;
  document.getElementById('bars-asset').innerHTML = d.by_asset_class
    .map(r => hbar(r.asset_class, r.count, acMax, acColor(r.asset_class), r.count + ' ETFs', ''))
    .join('');

  // Exchange bar
  const exMax = d.by_exchange[0]?.count || 1;
  document.getElementById('bars-exchange').innerHTML = d.by_exchange
    .map(r => hbar(r.exchange || 'Unknown', r.count, exMax, '#6366f1', r.count + ' ETFs', ''))
    .join('');

  document.getElementById('loading').classList.add('hidden');
  document.getElementById('page').classList.remove('hidden');
}
"""

PAGE_LISTINGS = _page(
    'ETF Listings',
    'History of Australian ETF listings — by year, issuer, asset class, and exchange',
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
    <h2 class="font-semibold text-sm text-gray-700 mb-4">1Y Return Distribution</h2>
    <div class="relative" style="height:220px"><canvas id="chart-dist"></canvas></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-gray-700 mb-4">Average 1Y Return — by Asset Class</h2>
    <div id="bars-ac"></div>
  </div>
</div>
<div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
  <div class="card">
    <h2 class="font-semibold text-sm text-gray-700 mb-3">Top Performers — 1Y Return</h2>
    <div class="overflow-x-auto"><table>
      <thead><tr><th>#</th><th>Code</th><th>Name</th><th>Issuer</th><th class="text-right">1Y</th><th class="text-right">3Y</th><th class="text-right">5Y</th><th class="text-right">FUM</th></tr></thead>
      <tbody id="tbl-top"></tbody>
    </table></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-gray-700 mb-3">Worst Performers — 1Y Return</h2>
    <div class="overflow-x-auto"><table>
      <thead><tr><th>#</th><th>Code</th><th>Name</th><th>Issuer</th><th class="text-right">1Y</th><th class="text-right">3Y</th><th class="text-right">5Y</th><th class="text-right">FUM</th></tr></thead>
      <tbody id="tbl-bot"></tbody>
    </table></div>
  </div>
</div>
<div class="card">
  <h2 class="font-semibold text-sm text-gray-700 mb-4">Average 1Y Return — by Issuer</h2>
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
    <td class="text-gray-400 tabular-nums">${i + 1}</td>
    <td class="font-bold text-blue-600">${e.code}</td>
    <td class="text-gray-600" style="max-width:170px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${e.name}">${e.name}</td>
    <td class="text-gray-500 text-xs">${e.issuer || '—'}</td>
    <td class="text-right font-semibold ${pcls(e.return_1y)} tabular-nums">${pct(e.return_1y, 1)}</td>
    <td class="text-right text-gray-500 tabular-nums">${pct(e.return_3y, 1)}</td>
    <td class="text-right text-gray-500 tabular-nums">${pct(e.return_5y, 1)}</td>
    <td class="text-right text-gray-400 tabular-nums text-xs">${fmtFum(e.fund_size_aud_millions)}</td>
  </tr>`;
  document.getElementById('tbl-top').innerHTML = d.top_performers.map(perfRow).join('');
  document.getElementById('tbl-bot').innerHTML = d.bottom_performers.map(perfRow).join('');

  // Issuer bars
  const issMax = Math.max(...d.by_issuer.map(r => Math.abs(r.avg_1y || 0)));
  document.getElementById('bars-issuer').innerHTML =
    `<div class="grid grid-cols-1 md:grid-cols-2 gap-x-8">` +
    d.by_issuer.map(r => `<div class="hbar-row">
      <span class="hbar-label" title="${r.issuer}">${r.issuer}</span>
      <div class="hbar-track"><div class="hbar-fill" style="width:${r.avg_1y != null ? Math.min(Math.abs(r.avg_1y) / issMax * 100, 100).toFixed(1) : 0}%;background:${r.avg_1y >= 0 ? issColor(r.issuer) : '#ef4444'}"></div></div>
      <span class="hbar-val ${pcls(r.avg_1y)}">${pct(r.avg_1y, 1)}</span>
      <span class="hbar-sub">${r.etf_count}e</span>
    </div>`).join('') + `</div>`;

  document.getElementById('loading').classList.add('hidden');
  document.getElementById('page').classList.remove('hidden');
}
"""

PAGE_RETURNS = _page(
    'Returns Analysis',
    'Performance across the Australian ETF market — distribution, top/bottom performers, by asset class and issuer',
    _RETURNS_BODY,
    _RETURNS_JS,
)


# ---------------------------------------------------------------------------
# EXPENSE / COST PAGE
# ---------------------------------------------------------------------------
_EXPENSE_BODY = """
<div id="hero" class="grid grid-cols-2 sm:grid-cols-4 gap-4"></div>
<div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
  <div class="card">
    <h2 class="font-semibold text-sm text-gray-700 mb-4">MER Distribution</h2>
    <div class="relative" style="height:220px"><canvas id="chart-dist"></canvas></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-gray-700 mb-1">Average MER — by Asset Class</h2>
    <p class="text-xs text-gray-400 mb-3">Simple avg · FUM-weighted avg per class</p>
    <div id="bars-ac"></div>
  </div>
</div>
<div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
  <div class="card">
    <h2 class="font-semibold text-sm text-gray-700 mb-3">Cheapest ETFs</h2>
    <div class="overflow-x-auto"><table>
      <thead><tr><th>#</th><th>Code</th><th>Name</th><th>Issuer</th><th class="text-right">MER</th><th class="text-right">FUM</th></tr></thead>
      <tbody id="tbl-cheap"></tbody>
    </table></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-gray-700 mb-3">Most Expensive ETFs</h2>
    <div class="overflow-x-auto"><table>
      <thead><tr><th>#</th><th>Code</th><th>Name</th><th>Issuer</th><th class="text-right">MER</th><th class="text-right">FUM</th></tr></thead>
      <tbody id="tbl-exp"></tbody>
    </table></div>
  </div>
</div>
<div class="card">
  <h2 class="font-semibold text-sm text-gray-700 mb-1">MER by Issuer — Simple vs FUM-Weighted</h2>
  <p class="text-xs text-gray-400 mb-4">FUM-weighted MER reflects what investors actually pay on average, weighted by fund size.</p>
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
  <h2 class="font-semibold text-sm text-gray-700 mb-4">FUM-Weighted MER — by Issuer</h2>
  <div class="relative" style="height:300px"><canvas id="chart-issuer"></canvas></div>
</div>
"""

_EXPENSE_JS = """
async function init() {
  const d = await api('/api/v1/insights/expense');
  document.getElementById('ts').textContent = new Date().toLocaleTimeString('en-AU');

  const cheapest = d.cheapest[0];
  const priciest = d.most_expensive[0];
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
        <span class="text-xs font-medium text-gray-700" style="min-width:150px">${r.asset_class}</span>
        <span class="text-xs text-gray-400 tabular-nums">${mer(r.avg_mer)} avg · <span class="font-semibold text-gray-600">${mer(r.fum_weighted_mer)}</span> wtd</span>
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

  // Cheapest / priciest tables
  const merRow = (e, i) => `<tr>
    <td class="text-gray-400 tabular-nums">${i + 1}</td>
    <td class="font-bold text-blue-600">${e.code}</td>
    <td class="text-gray-600" style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${e.name}">${e.name}</td>
    <td class="text-gray-500 text-xs">${e.issuer || '—'}</td>
    <td class="text-right font-semibold tabular-nums">${mer(e.effective_mer)}</td>
    <td class="text-right text-gray-400 tabular-nums text-xs">${fmtFum(e.fund_size_aud_millions)}</td>
  </tr>`;
  document.getElementById('tbl-cheap').innerHTML = d.cheapest.map(merRow).join('');
  document.getElementById('tbl-exp').innerHTML = d.most_expensive.map(merRow).join('');

  // Issuer comparison table (sorted by FUM-weighted MER)
  const issSorted = [...d.by_issuer].sort((a, b) => (a.fum_weighted_mer || 99) - (b.fum_weighted_mer || 99));
  const fwMax = Math.max(...issSorted.map(r => r.fum_weighted_mer || 0));
  document.getElementById('tbl-issuers').innerHTML = issSorted.map(r => `<tr>
    <td class="font-semibold text-sm" style="color:${issColor(r.issuer)}">${r.issuer}</td>
    <td class="text-right tabular-nums text-gray-500">${r.etf_count}</td>
    <td class="text-right tabular-nums text-gray-500">${mer(r.avg_mer)}</td>
    <td class="text-right tabular-nums font-semibold">${mer(r.fum_weighted_mer)}</td>
    <td class="text-right tabular-nums text-gray-400 text-xs">${fmtFum(r.total_fum)}</td>
    <td>
      <div class="flex items-center gap-1">
        <div class="bar-track flex-1">
          <div class="bar-fill" style="width:${fwMax > 0 ? ((r.fum_weighted_mer||0)/fwMax*100).toFixed(1) : 0}%;background:${issColor(r.issuer)}"></div>
        </div>
        <span class="text-xs text-gray-400 tabular-nums w-10 text-right">${mer(r.fum_weighted_mer)}</span>
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
    'Cost Analysis',
    'Management expense ratios across the Australian ETF market — by fund, asset class, and issuer',
    _EXPENSE_BODY,
    _EXPENSE_JS,
)


# ---------------------------------------------------------------------------
# ISSUERS PAGE
# ---------------------------------------------------------------------------
_ISSUERS_BODY = """
<div id="hero" class="grid grid-cols-2 sm:grid-cols-4 gap-4"></div>
<div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
  <div class="card">
    <h2 class="font-semibold text-sm text-gray-700 mb-4">Market Share by FUM</h2>
    <div class="relative" style="height:280px"><canvas id="chart-share"></canvas></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-gray-700 mb-4">ETF Count by Issuer</h2>
    <div id="bars-count"></div>
  </div>
</div>
<div class="card overflow-x-auto">
  <h2 class="font-semibold text-sm text-gray-700 mb-3">Issuer Comparison</h2>
  <table>
    <thead><tr>
      <th>#</th><th>Issuer</th><th class="text-right">FUM</th><th class="text-right">Mkt Share</th>
      <th class="text-right">ETFs</th><th class="text-right">Avg MER</th>
      <th class="text-right">Avg 1Y Ret</th><th>Largest ETF</th><th>Primary Category</th>
    </tr></thead>
    <tbody id="tbl-issuers"></tbody>
  </table>
</div>
<div class="card">
  <h2 class="font-semibold text-sm text-gray-700 mb-4">Asset Class Mix — by Issuer</h2>
  <div id="mix-chart" class="space-y-3"></div>
</div>
"""

_ISSUERS_JS = """
async function init() {
  const d = await api('/api/v1/insights/issuers');
  document.getElementById('ts').textContent = new Date().toLocaleTimeString('en-AU');

  const top = d.issuers[0];
  const avgMers = d.issuers.filter(r => r.avg_mer).map(r => r.avg_mer);
  const mktAvgMer = avgMers.length ? avgMers.reduce((a, b) => a + b, 0) / avgMers.length : null;

  document.getElementById('hero').innerHTML = [
    ['Total Issuers', d.issuers.length, 'active fund managers'],
    ['Largest Issuer', top?.issuer || '—', fmtFum(top?.total_fum)],
    ['Top 3 Market Share', d.issuers.slice(0, 3).reduce((s, r) => s + (r.market_share_pct || 0), 0).toFixed(1) + '%', 'of total FUM'],
    ['Mkt Avg MER', mer(mktAvgMer), 'across all issuers'],
  ].map(([l, v, s]) => `<div class="card"><div class="sl">${l}</div><div class="sv">${v}</div><div class="ss">${s}</div></div>`).join('');

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
        tooltip: { callbacks: { label: ctx => ' ' + fmtFum(ctx.parsed) + '  (' + top10[ctx.dataIndex]?.market_share_pct + '%)' } },
      },
    },
  });

  // Count bars
  const cntMax = d.issuers[0]?.etf_count || 1;
  document.getElementById('bars-count').innerHTML = d.issuers
    .map(r => hbar(r.issuer, r.etf_count, cntMax, issColor(r.issuer), r.etf_count + ' ETFs', ''))
    .join('');

  // Comparison table
  document.getElementById('tbl-issuers').innerHTML = d.issuers.map((r, i) => {
    const primaryAC = Object.entries(r.asset_classes || {}).sort((a, b) => b[1] - a[1])[0]?.[0] || '—';
    return `<tr>
      <td class="text-gray-400 tabular-nums">${i + 1}</td>
      <td class="font-semibold" style="color:${issColor(r.issuer)}">${r.issuer}</td>
      <td class="text-right font-semibold tabular-nums">${fmtFum(r.total_fum)}</td>
      <td class="text-right tabular-nums">
        <div class="flex items-center justify-end gap-1">
          <div class="bar-track w-12"><div class="bar-fill" style="width:${r.market_share_pct}%;background:${issColor(r.issuer)}"></div></div>
          <span class="text-gray-500">${r.market_share_pct}%</span>
        </div>
      </td>
      <td class="text-right tabular-nums">${r.etf_count}</td>
      <td class="text-right tabular-nums">${mer(r.avg_mer)}</td>
      <td class="text-right font-semibold ${pcls(r.avg_return_1y)} tabular-nums">${pct(r.avg_return_1y, 1)}</td>
      <td class="font-bold text-blue-600 text-xs">${r.top_etf?.code || '—'}<span class="text-gray-400 font-normal ml-1">${fmtFum(r.top_etf?.fund_size_aud_millions)}</span></td>
      <td><span class="badge" style="background:${acColor(primaryAC)}22;color:${acColor(primaryAC)}">${primaryAC}</span></td>
    </tr>`;
  }).join('');

  // Asset class mix stacked bars
  const allACs = [...new Set(d.issuers.flatMap(r => Object.keys(r.asset_classes || {})))];
  document.getElementById('mix-chart').innerHTML = d.issuers.slice(0, 15).map(r => {
    const total = Object.values(r.asset_classes || {}).reduce((s, v) => s + v, 0) || 1;
    const segments = Object.entries(r.asset_classes || {})
      .sort((a, b) => b[1] - a[1])
      .map(([ac, cnt]) => `<div title="${ac}: ${cnt}" style="width:${(cnt/total*100).toFixed(1)}%;background:${acColor(ac)};height:100%;display:inline-block"></div>`)
      .join('');
    return `<div class="flex items-center gap-3">
      <span class="text-xs font-medium text-gray-700 w-24 shrink-0 truncate">${r.issuer}</span>
      <div class="flex-1 h-5 rounded overflow-hidden bg-gray-100 flex">${segments}</div>
      <span class="text-xs text-gray-400 w-12 text-right">${r.etf_count} ETFs</span>
    </div>`;
  }).join('');

  document.getElementById('loading').classList.add('hidden');
  document.getElementById('page').classList.remove('hidden');
}
"""

PAGE_ISSUERS = _page(
    'Issuer Analysis',
    'Fund manager comparison — market share, ETF count, fees, returns, and asset class mix',
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
      <h2 class="font-semibold text-sm text-gray-700">Market-wide Premium/Discount — Last 90 Days</h2>
      <span class="text-xs text-gray-400">Average across all ETFs with NAV data</span>
    </div>
    <div class="relative" style="height:220px"><canvas id="chart-hist"></canvas></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-gray-700 mb-3">Today's Distribution</h2>
    <div class="relative" style="height:220px"><canvas id="chart-dist"></canvas></div>
  </div>
</div>

<!-- By asset class + by issuer -->
<div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
  <div class="card">
    <h2 class="font-semibold text-sm text-gray-700 mb-3">Avg Premium/Discount by Asset Class</h2>
    <div id="ac-bars"></div>
  </div>
  <div class="card">
    <h2 class="font-semibold text-sm text-gray-700 mb-3">Avg Premium/Discount by Issuer</h2>
    <div id="iss-bars"></div>
  </div>
</div>

<!-- Historical explorer for individual ETF -->
<div class="card">
  <div class="flex flex-wrap items-center gap-3 mb-4">
    <h2 class="font-semibold text-sm text-gray-700">Historical Premium/Discount Explorer</h2>
    <select id="etf-picker" class="border border-gray-200 rounded-lg px-2.5 py-1.5 text-sm bg-gray-50 focus:ring-2 focus:ring-blue-200 outline-none min-w-[200px]">
      <option value="">— select an ETF —</option>
    </select>
    <select id="period-picker" class="border border-gray-200 rounded-lg px-2.5 py-1.5 text-sm bg-gray-50 focus:ring-2 focus:ring-blue-200 outline-none">
      <option value="3m">3 months</option>
      <option value="1y" selected>1 year</option>
      <option value="3y">3 years</option>
      <option value="all">All time</option>
    </select>
    <span id="etf-stats" class="text-xs text-gray-400"></span>
  </div>
  <div class="relative" style="height:260px"><canvas id="chart-etf"></canvas></div>
</div>

<!-- Today's full snapshot table -->
<div class="card">
  <div class="flex items-center justify-between mb-3">
    <h2 class="font-semibold text-sm text-gray-700">Today's Snapshot — All ETFs</h2>
    <div class="flex items-center gap-2">
      <input id="snap-search" type="text" placeholder="Filter by code or name…"
             class="border border-gray-200 rounded-lg px-2.5 py-1.5 text-sm bg-gray-50 focus:ring-2 focus:ring-blue-200 outline-none w-52">
      <select id="snap-sort" class="border border-gray-200 rounded-lg px-2.5 py-1.5 text-sm bg-gray-50 focus:ring-2 focus:ring-blue-200 outline-none">
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
  return 'text-gray-500';
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
  if (!rows.length) { document.getElementById('ac-bars').innerHTML = '<p class="text-xs text-gray-400">No data</p>'; return; }
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
  if (!rows.length) { document.getElementById('iss-bars').innerHTML = '<p class="text-xs text-gray-400">No data</p>'; return; }
  const maxAbs = Math.max(...rows.map(r => Math.abs(r.avg_pd || 0)));
  document.getElementById('iss-bars').innerHTML = rows.map(r => {
    const v = r.avg_pd;
    const pct = maxAbs > 0 ? (Math.abs(v) / maxAbs * 100).toFixed(1) : 0;
    const col = v >= 0 ? '#16a34a' : '#dc2626';
    return `<div style="display:flex;align-items:center;gap:8px;margin-bottom:7px">
      <span style="font-size:.73rem;color:#374151;width:120px;flex-shrink:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${r.issuer}">${r.issuer}</span>
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
          annotations: { zero: { type: 'line', yMin: 0, yMax: 0, borderColor: '#94a3b8', borderWidth: 1, borderDash: [4,4] } }
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
      <td style="color:#64748b">${r.issuer||'—'}</td>
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
# Registry
# ---------------------------------------------------------------------------
PAGES = {
    'fum':      PAGE_FUM,
    'listings': PAGE_LISTINGS,
    'returns':  PAGE_RETURNS,
    'expense':  PAGE_EXPENSE,
    'issuers':  PAGE_ISSUERS,
    'nav':      PAGE_NAV,
}

def get_insights_page(name: str) -> str | None:
    return PAGES.get(name)
