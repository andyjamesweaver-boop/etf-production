#!/usr/bin/env python3
"""
ETF Dashboard API Server
========================
Stdlib-only HTTP server (http.server) serving the expanded Australian ETF
data platform.  All new endpoints + full dashboard with filters, detail
panel, and analytics.
"""

import http.server
import socketserver
import json
import sqlite3
import urllib.parse
from datetime import datetime
import os
import sys
import re

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etf_data.db')


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


class ETFAPIHandler(http.server.BaseHTTPRequestHandler):

    # ------------------------------------------------------------------ routing
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path.rstrip('/')
        qs = urllib.parse.parse_qs(parsed.query)

        routes = {
            '': self.handle_root,
            '/health': self.handle_health,
            '/dashboard': self.handle_dashboard,
            '/api/v1/etfs': lambda: self.handle_etfs_list(qs),
            '/api/v1/issuers': self.handle_issuers,
            '/api/v1/exchanges': self.handle_exchanges,
            '/api/v1/categories': self.handle_categories,
            '/api/v1/market/overview': self.handle_market_overview,
            '/api/v1/analytics/fund-flows': lambda: self.handle_fund_flows(qs),
            '/api/v1/analytics/cheapest': lambda: self.handle_cheapest(qs),
            '/api/v1/analytics/highest-yield': lambda: self.handle_highest_yield(qs),
            '/api/v1/analytics/top-performers': lambda: self.handle_top_performers(qs),
            '/api/v1/search': lambda: self.handle_search(qs),
            '/api/v1/scrape-status': self.handle_scrape_status,
        }

        handler = routes.get(path)
        if handler:
            handler()
            return

        # Parameterised routes: /api/v1/etfs/{code}[/sub]
        m = re.match(r'^/api/v1/etfs/([A-Za-z0-9]+)(?:/(.+))?$', path)
        if m:
            code = m.group(1).upper()
            sub = m.group(2)
            if sub is None:
                self.handle_etf_detail(code)
            elif sub == 'holdings':
                self.handle_etf_holdings(code)
            elif sub == 'sectors':
                self.handle_etf_sectors(code)
            elif sub == 'dividends':
                self.handle_etf_dividends(code)
            elif sub == 'price-history':
                self.handle_etf_price_history(code, qs)
            else:
                self.send_json({'error': f'Unknown sub-resource: {sub}'}, 404)
            return

        self.send_json({'error': 'Not found'}, 404)

    # ------------------------------------------------------------------ helpers
    def send_json(self, data, status=200):
        body = json.dumps(data, indent=2, default=str).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, html):
        body = html.encode()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _int(self, qs, key, default):
        try:
            return int(qs.get(key, [str(default)])[0])
        except (ValueError, IndexError):
            return default

    def _str(self, qs, key, default=None):
        vals = qs.get(key)
        return vals[0] if vals else default

    # ------------------------------------------------------------------ endpoints

    def handle_root(self):
        self.send_json({
            'service': 'Australian ETF Dashboard API',
            'version': '2.0.0',
            'endpoints': [
                '/health', '/dashboard', '/api/v1/etfs', '/api/v1/etfs/{code}',
                '/api/v1/etfs/{code}/holdings', '/api/v1/etfs/{code}/sectors',
                '/api/v1/etfs/{code}/dividends', '/api/v1/etfs/{code}/price-history',
                '/api/v1/issuers', '/api/v1/exchanges', '/api/v1/categories',
                '/api/v1/market/overview', '/api/v1/analytics/fund-flows',
                '/api/v1/analytics/cheapest', '/api/v1/analytics/highest-yield',
                '/api/v1/analytics/top-performers', '/api/v1/search',
                '/api/v1/scrape-status',
            ],
        })

    def handle_health(self):
        conn = get_db()
        try:
            count = conn.execute("SELECT COUNT(*) FROM etfs").fetchone()[0]
            self.send_json({
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'etf_count': count,
                'database': 'connected',
            })
        except Exception as e:
            self.send_json({'status': 'error', 'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- ETFs list (with filters, sort, pagination) -----
    def handle_etfs_list(self, qs):
        conn = get_db()
        try:
            where, params = ['1=1'], []
            exchange = self._str(qs, 'exchange')
            issuer = self._str(qs, 'issuer')
            asset_class = self._str(qs, 'asset_class')
            category = self._str(qs, 'category')  # alias

            if exchange:
                where.append("exchange = ?"); params.append(exchange.upper())
            if issuer:
                where.append("issuer = ?"); params.append(issuer)
            if asset_class or category:
                where.append("asset_class = ?"); params.append(asset_class or category)

            sort_map = {
                'fum': 'fund_size_aud_millions DESC',
                'price': 'current_price DESC',
                'return_1y': 'return_1y DESC',
                'yield': 'distribution_yield DESC',
                'expense': 'expense_ratio ASC',
                'name': 'name ASC',
                'code': 'code ASC',
                'rank': 'rank_by_fum ASC',
            }
            sort_by = self._str(qs, 'sort_by', 'rank')
            order = sort_map.get(sort_by, 'rank_by_fum ASC')

            limit = min(self._int(qs, 'limit', 100), 500)
            offset = self._int(qs, 'offset', 0)

            w = ' AND '.join(where)
            total = conn.execute(f"SELECT COUNT(*) FROM etfs WHERE {w}", params).fetchone()[0]
            rows = conn.execute(
                f"SELECT * FROM etfs WHERE {w} ORDER BY {order} LIMIT ? OFFSET ?",
                params + [limit, offset]
            ).fetchall()

            self.send_json({
                'data': [dict(r) for r in rows],
                'total': total,
                'limit': limit,
                'offset': offset,
            })
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Single ETF detail -----
    def handle_etf_detail(self, code):
        conn = get_db()
        try:
            row = conn.execute("SELECT * FROM etfs WHERE code = ?", (code,)).fetchone()
            if not row:
                self.send_json({'error': f'ETF {code} not found'}, 404)
                return
            self.send_json(dict(row))
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Holdings -----
    def handle_etf_holdings(self, code):
        conn = get_db()
        try:
            rows = conn.execute(
                "SELECT name, ticker, weight_pct, sector, country FROM etf_holdings "
                "WHERE etf_code = ? ORDER BY weight_pct DESC", (code,)
            ).fetchall()
            self.send_json({'code': code, 'holdings': [dict(r) for r in rows]})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Sectors -----
    def handle_etf_sectors(self, code):
        conn = get_db()
        try:
            rows = conn.execute(
                "SELECT sector, weight_pct FROM etf_sectors "
                "WHERE etf_code = ? ORDER BY weight_pct DESC", (code,)
            ).fetchall()
            self.send_json({'code': code, 'sectors': [dict(r) for r in rows]})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Dividends -----
    def handle_etf_dividends(self, code):
        conn = get_db()
        try:
            rows = conn.execute(
                "SELECT ex_date, pay_date, amount, franking_pct, type FROM etf_dividends "
                "WHERE etf_code = ? ORDER BY ex_date DESC LIMIT 50", (code,)
            ).fetchall()
            self.send_json({'code': code, 'dividends': [dict(r) for r in rows]})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Price history -----
    def handle_etf_price_history(self, code, qs):
        days = self._int(qs, 'days', 90)
        conn = get_db()
        try:
            rows = conn.execute(
                "SELECT date, open, high, low, close, volume FROM price_history "
                "WHERE etf_code = ? ORDER BY date DESC LIMIT ?", (code, days)
            ).fetchall()
            self.send_json({'code': code, 'prices': [dict(r) for r in rows]})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Issuers -----
    def handle_issuers(self):
        conn = get_db()
        try:
            rows = conn.execute(
                "SELECT name, website, etf_count, total_fum, last_updated "
                "FROM issuers WHERE etf_count > 0 ORDER BY total_fum DESC"
            ).fetchall()
            self.send_json({'issuers': [dict(r) for r in rows]})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Exchanges -----
    def handle_exchanges(self):
        conn = get_db()
        try:
            rows = conn.execute(
                "SELECT exchange, COUNT(*) as etf_count, "
                "COALESCE(SUM(fund_size_aud_millions),0) as total_fum "
                "FROM etfs GROUP BY exchange ORDER BY total_fum DESC"
            ).fetchall()
            self.send_json({'exchanges': [dict(r) for r in rows]})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Categories -----
    def handle_categories(self):
        conn = get_db()
        try:
            rows = conn.execute(
                "SELECT asset_class, COUNT(*) as etf_count, "
                "COALESCE(SUM(fund_size_aud_millions),0) as total_fum "
                "FROM etfs WHERE asset_class IS NOT NULL "
                "GROUP BY asset_class ORDER BY total_fum DESC"
            ).fetchall()
            self.send_json({'categories': [dict(r) for r in rows]})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Market overview -----
    def handle_market_overview(self):
        conn = get_db()
        try:
            stats = conn.execute('''
                SELECT COUNT(*) as total_etfs,
                       COALESCE(SUM(fund_size_aud_millions),0) as total_fum_millions,
                       COALESCE(AVG(return_1y),0) as avg_return_1y,
                       COALESCE(AVG(expense_ratio),0) as avg_expense_ratio
                FROM etfs
            ''').fetchone()

            top = conn.execute(
                "SELECT code, name, return_1y FROM etfs WHERE return_1y IS NOT NULL "
                "ORDER BY return_1y DESC LIMIT 1"
            ).fetchone()

            cats = conn.execute(
                "SELECT asset_class as category, COUNT(*) as count, "
                "COALESCE(SUM(fund_size_aud_millions),0) as total_fum "
                "FROM etfs WHERE asset_class IS NOT NULL "
                "GROUP BY asset_class ORDER BY total_fum DESC"
            ).fetchall()

            self.send_json({
                'total_fum_millions': stats['total_fum_millions'],
                'total_etfs': stats['total_etfs'],
                'avg_return_1y': round(stats['avg_return_1y'], 2),
                'avg_expense_ratio': round(stats['avg_expense_ratio'], 3),
                'top_performer': dict(top) if top else None,
                'categories': [dict(c) for c in cats],
                'last_updated': datetime.now().isoformat(),
            })
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Analytics: fund flows -----
    def handle_fund_flows(self, qs):
        limit = self._int(qs, 'limit', 20)
        conn = get_db()
        try:
            inflows = conn.execute(
                "SELECT code, name, fund_flow_1m FROM etfs "
                "WHERE fund_flow_1m IS NOT NULL ORDER BY fund_flow_1m DESC LIMIT ?",
                (limit,)
            ).fetchall()
            outflows = conn.execute(
                "SELECT code, name, fund_flow_1m FROM etfs "
                "WHERE fund_flow_1m IS NOT NULL ORDER BY fund_flow_1m ASC LIMIT ?",
                (limit,)
            ).fetchall()
            self.send_json({
                'top_inflows': [dict(r) for r in inflows],
                'top_outflows': [dict(r) for r in outflows],
            })
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Analytics: cheapest -----
    def handle_cheapest(self, qs):
        limit = self._int(qs, 'limit', 20)
        conn = get_db()
        try:
            rows = conn.execute(
                "SELECT code, name, expense_ratio, management_fee, fund_size_aud_millions "
                "FROM etfs WHERE expense_ratio IS NOT NULL AND expense_ratio > 0 "
                "ORDER BY expense_ratio ASC LIMIT ?", (limit,)
            ).fetchall()
            self.send_json({'cheapest': [dict(r) for r in rows]})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Analytics: highest yield -----
    def handle_highest_yield(self, qs):
        limit = self._int(qs, 'limit', 20)
        conn = get_db()
        try:
            rows = conn.execute(
                "SELECT code, name, distribution_yield, distribution_frequency, fund_size_aud_millions "
                "FROM etfs WHERE distribution_yield IS NOT NULL AND distribution_yield > 0 "
                "ORDER BY distribution_yield DESC LIMIT ?", (limit,)
            ).fetchall()
            self.send_json({'highest_yield': [dict(r) for r in rows]})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Analytics: top performers -----
    def handle_top_performers(self, qs):
        limit = self._int(qs, 'limit', 20)
        conn = get_db()
        try:
            rows = conn.execute(
                "SELECT code, name, return_1y, return_3y, fund_size_aud_millions "
                "FROM etfs WHERE return_1y IS NOT NULL "
                "ORDER BY return_1y DESC LIMIT ?", (limit,)
            ).fetchall()
            self.send_json({'top_performers': [dict(r) for r in rows]})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Search -----
    def handle_search(self, qs):
        q = self._str(qs, 'q', '')
        if len(q) < 1:
            self.send_json({'error': 'Query too short'}, 400)
            return
        conn = get_db()
        try:
            pattern = f'%{q}%'
            rows = conn.execute(
                "SELECT code, name, issuer, asset_class, exchange, "
                "current_price, fund_size_aud_millions, return_1y "
                "FROM etfs WHERE code LIKE ? OR name LIKE ? OR issuer LIKE ? "
                "ORDER BY fund_size_aud_millions DESC LIMIT 50",
                (pattern, pattern, pattern)
            ).fetchall()
            self.send_json({'query': q, 'results': [dict(r) for r in rows]})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Scrape status -----
    def handle_scrape_status(self):
        conn = get_db()
        try:
            rows = conn.execute(
                "SELECT source, status, records_affected, error, duration_secs, "
                "started_at, finished_at FROM scrape_log ORDER BY finished_at DESC LIMIT 20"
            ).fetchall()
            self.send_json({'log': [dict(r) for r in rows]})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ================================================================== DASHBOARD
    def handle_dashboard(self):
        self.send_html(DASHBOARD_HTML)

    def log_message(self, fmt, *args):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{ts}] {fmt % args}")


# ===================================================================== Dashboard HTML

DASHBOARD_HTML = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Australian ETF Dashboard</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  /* Exchange badges */
  .badge-asx { background: #1e40af; color: #fff; }
  .badge-cboe { background: #7c3aed; color: #fff; }

  /* Asset class colour chips */
  .ac-au   { background: #dbeafe; color: #1e40af; }
  .ac-int  { background: #dcfce7; color: #166534; }
  .ac-fi   { background: #fef3c7; color: #92400e; }
  .ac-prop { background: #fce7f3; color: #9d174d; }
  .ac-com  { background: #fef9c3; color: #713f12; }
  .ac-div  { background: #e0e7ff; color: #3730a3; }
  .ac-alt  { background: #f3f4f6; color: #374151; }

  /* Table rows */
  #etf-table tr { border-bottom: 1px solid #f1f5f9; transition: background .1s; }
  #etf-table tr:hover { background: #eff6ff; }
  #etf-table tr.row-selected { background: #dbeafe; }

  /* Detail tabs */
  .dtab { color: #9ca3af; padding-bottom: 10px; font-weight: 500; transition: color .15s; border-bottom: 2px solid transparent; }
  .dtab:hover { color: #2563eb; }
  .tab-active { color: #2563eb !important; border-bottom-color: #2563eb; }

  /* Search dropdown */
  #search-results { position: absolute; z-index: 50; top: calc(100% + 6px); left: 0; right: 0; }

  /* Spinner */
  .spinner { border: 3px solid #e5e7eb; border-top-color: #2563eb; border-radius: 50%;
             width: 28px; height: 28px; animation: spin .7s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* Progress bars animate in */
  .pbar { transition: width .5s cubic-bezier(.4,0,.2,1); }

  /* Scrollbar */
  ::-webkit-scrollbar { width: 5px; height: 5px; }
  ::-webkit-scrollbar-track { background: #f8fafc; }
  ::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 4px; }

  /* Stat card hover lift */
  .stat-card { transition: transform .15s, box-shadow .15s; }
  .stat-card:hover { transform: translateY(-2px); box-shadow: 0 6px 16px rgba(0,0,0,.08); }
</style>
</head>
<body class="bg-slate-100 min-h-screen text-sm text-gray-800 antialiased">

<!-- ── Header ── -->
<header class="bg-gradient-to-r from-blue-900 to-blue-700 shadow-xl">
  <div class="max-w-[1400px] mx-auto px-5 py-3 flex flex-wrap items-center gap-4">
    <div class="flex-1 min-w-[180px]">
      <h1 class="text-lg font-bold text-white tracking-tight leading-tight">
        Australian ETF Dashboard
      </h1>
      <p id="subtitle" class="text-blue-300 text-xs mt-0.5">Loading market data…</p>
    </div>

    <!-- Search -->
    <div class="relative w-80">
      <svg class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-blue-300 pointer-events-none"
           fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
              d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/>
      </svg>
      <input id="search" type="text" placeholder="Search code or name…"
             autocomplete="off"
             class="w-full bg-white/10 border border-white/20 text-white placeholder-blue-300
                    rounded-xl pl-9 pr-3 py-2 text-sm outline-none
                    focus:ring-2 focus:ring-white/40 focus:bg-white/20">
      <div id="search-results"
           class="hidden bg-white border border-gray-200 rounded-xl shadow-2xl max-h-72 overflow-y-auto"></div>
    </div>

    <!-- Live indicator -->
    <div class="flex items-center gap-2 text-xs text-blue-300">
      <span class="w-2 h-2 bg-green-400 rounded-full animate-pulse"></span>
      <span id="last-refresh">Live</span>
    </div>
  </div>
</header>

<main class="max-w-[1400px] mx-auto px-5 py-5">

  <!-- ── Stat cards ── -->
  <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3 mb-5">
    <div class="stat-card bg-white rounded-xl shadow-sm border border-gray-100 p-4">
      <p class="text-gray-400 text-xs font-semibold uppercase tracking-wider">Total FUM</p>
      <p id="c-fum" class="text-2xl font-bold text-gray-900 mt-1 leading-tight">—</p>
      <p class="text-gray-400 text-xs mt-1">AUD</p>
    </div>
    <div class="stat-card bg-white rounded-xl shadow-sm border border-gray-100 p-4">
      <p class="text-gray-400 text-xs font-semibold uppercase tracking-wider">ETFs Listed</p>
      <p id="c-count" class="text-2xl font-bold text-gray-900 mt-1 leading-tight">—</p>
      <p class="text-gray-400 text-xs mt-1">all exchanges</p>
    </div>
    <div class="stat-card bg-white rounded-xl shadow-sm border border-gray-100 p-4">
      <p class="text-gray-400 text-xs font-semibold uppercase tracking-wider">Avg 1Y Return</p>
      <p id="c-ret" class="text-2xl font-bold mt-1 leading-tight">—</p>
      <p class="text-gray-400 text-xs mt-1">market average</p>
    </div>
    <div class="stat-card bg-white rounded-xl shadow-sm border border-gray-100 p-4">
      <p class="text-gray-400 text-xs font-semibold uppercase tracking-wider">Avg Expense</p>
      <p id="c-exp" class="text-2xl font-bold text-gray-900 mt-1 leading-tight">—</p>
      <p class="text-gray-400 text-xs mt-1">management fee</p>
    </div>
    <div class="stat-card bg-white rounded-xl shadow-sm border border-gray-100 p-4">
      <p class="text-gray-400 text-xs font-semibold uppercase tracking-wider">Top Performer</p>
      <p id="c-top" class="text-2xl font-bold text-green-600 mt-1 leading-tight">—</p>
      <p class="text-gray-400 text-xs mt-1">best 1Y return</p>
    </div>
    <div class="stat-card bg-white rounded-xl shadow-sm border border-gray-100 p-4">
      <p class="text-gray-400 text-xs font-semibold uppercase tracking-wider">Issuers</p>
      <p id="c-issuers" class="text-2xl font-bold text-gray-900 mt-1 leading-tight">—</p>
      <p class="text-gray-400 text-xs mt-1">fund managers</p>
    </div>
  </div>

  <!-- ── Filter sidebar + table ── -->
  <div class="grid grid-cols-1 lg:grid-cols-4 gap-4 mb-5">

    <!-- Sidebar -->
    <aside class="lg:col-span-1">
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4 sticky top-4">
        <h3 class="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">Filters</h3>
        <div class="space-y-3">
          <div>
            <label class="block text-xs text-gray-500 mb-1">Exchange</label>
            <select id="f-exchange"
                    class="w-full border border-gray-200 rounded-lg px-2.5 py-2 text-sm bg-gray-50
                           focus:ring-2 focus:ring-blue-200 focus:border-blue-400 outline-none">
              <option value="">All Exchanges</option>
            </select>
          </div>
          <div>
            <label class="block text-xs text-gray-500 mb-1">Issuer</label>
            <select id="f-issuer"
                    class="w-full border border-gray-200 rounded-lg px-2.5 py-2 text-sm bg-gray-50
                           focus:ring-2 focus:ring-blue-200 focus:border-blue-400 outline-none">
              <option value="">All Issuers</option>
            </select>
          </div>
          <div>
            <label class="block text-xs text-gray-500 mb-1">Asset Class</label>
            <select id="f-asset"
                    class="w-full border border-gray-200 rounded-lg px-2.5 py-2 text-sm bg-gray-50
                           focus:ring-2 focus:ring-blue-200 focus:border-blue-400 outline-none">
              <option value="">All Asset Classes</option>
            </select>
          </div>
          <div>
            <label class="block text-xs text-gray-500 mb-1">Sort By</label>
            <select id="f-sort"
                    class="w-full border border-gray-200 rounded-lg px-2.5 py-2 text-sm bg-gray-50
                           focus:ring-2 focus:ring-blue-200 focus:border-blue-400 outline-none">
              <option value="rank">Rank by FUM</option>
              <option value="fum">FUM (largest)</option>
              <option value="return_1y">1Y Return</option>
              <option value="yield">Distribution Yield</option>
              <option value="expense">Expense Ratio</option>
              <option value="price">Price</option>
              <option value="code">Code A–Z</option>
            </select>
          </div>
          <button id="btn-reset"
                  class="w-full border border-gray-200 rounded-lg py-2 text-sm text-gray-500
                         hover:bg-gray-50 hover:text-gray-700 transition-colors">
            Reset Filters
          </button>
        </div>
        <p id="result-count-sidebar" class="mt-4 pt-3 border-t text-xs text-gray-400"></p>
      </div>
    </aside>

    <!-- Table -->
    <div class="lg:col-span-3">
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
        <div class="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
          <h3 class="font-semibold text-gray-700">ETF List</h3>
          <span id="result-count" class="text-xs text-gray-400"></span>
        </div>
        <div class="overflow-x-auto" style="max-height:520px;overflow-y:auto">
          <table class="w-full">
            <thead class="bg-gray-50 text-xs font-semibold text-gray-500 uppercase tracking-wide"
                   style="position:sticky;top:0;z-index:5">
              <tr>
                <th class="px-3 py-2.5 text-left w-8">#</th>
                <th class="px-3 py-2.5 text-left">ETF</th>
                <th class="px-3 py-2.5 text-left">Class</th>
                <th class="px-3 py-2.5 text-right">Price</th>
                <th class="px-3 py-2.5 text-right">FUM</th>
                <th class="px-3 py-2.5 text-right">1Y Rtn</th>
                <th class="px-3 py-2.5 text-right">Yield</th>
                <th class="px-3 py-2.5 text-right">MER</th>
              </tr>
            </thead>
            <tbody id="etf-table" class="text-sm divide-y divide-gray-50"></tbody>
          </table>
        </div>
        <div id="pagination"
             class="px-4 py-2.5 bg-gray-50 border-t border-gray-100 flex items-center justify-between text-xs text-gray-500">
        </div>
      </div>
    </div>
  </div>

  <!-- ── Detail panel ── -->
  <div id="detail-panel" class="hidden bg-white rounded-xl shadow-sm border border-gray-100 mb-5">
    <!-- Header -->
    <div class="px-5 py-4 border-b border-gray-100 flex items-start justify-between gap-4">
      <div>
        <div class="flex items-center gap-2 flex-wrap">
          <span id="d-code" class="text-2xl font-bold text-blue-700"></span>
          <span id="d-badge" class="text-xs px-2 py-0.5 rounded-full font-medium"></span>
        </div>
        <p id="d-name" class="text-gray-500 text-sm mt-0.5"></p>
      </div>
      <button id="d-close"
              class="w-8 h-8 flex items-center justify-center rounded-full text-gray-400
                     hover:bg-gray-100 hover:text-gray-600 transition-colors shrink-0 text-base">
        ✕
      </button>
    </div>
    <!-- Quick-metrics strip -->
    <div id="d-metrics" class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-8 divide-x divide-y sm:divide-y-0 border-b border-gray-100"></div>
    <!-- Tabs + content -->
    <div class="p-5">
      <div class="flex gap-6 border-b border-gray-100 mb-5 text-sm">
        <button class="dtab tab-active" data-tab="overview">Overview</button>
        <button class="dtab" data-tab="holdings">Holdings</button>
        <button class="dtab" data-tab="sectors">Sectors</button>
        <button class="dtab" data-tab="dividends">Dividends</button>
      </div>
      <div id="tab-content"></div>
    </div>
  </div>

  <!-- ── Analytics row ── -->
  <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
    <!-- Asset class doughnut -->
    <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
      <h3 class="font-semibold text-gray-700 text-sm mb-3">Asset Class Breakdown</h3>
      <div class="relative" style="height:220px">
        <canvas id="chart-asset"></canvas>
      </div>
    </div>
    <!-- Issuer market share bars -->
    <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
      <h3 class="font-semibold text-gray-700 text-sm mb-3">Issuer Market Share</h3>
      <div id="a-issuers" class="space-y-2.5"></div>
    </div>
    <!-- Fund flows bar chart -->
    <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
      <h3 class="font-semibold text-gray-700 text-sm mb-3">Fund Flows — Top 1M</h3>
      <div class="relative" style="height:220px">
        <canvas id="chart-flows"></canvas>
      </div>
    </div>
  </div>

</main>

<script>
/* ======================================================= state */
const API = '';
let page = 0, pageSize = 50, selectedCode = null;
let chartAsset = null, chartFlows = null;

const PALETTE = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6',
                 '#ec4899','#06b6d4','#84cc16','#f97316','#6366f1'];

/* ======================================================= formatters */
function fmtFum(v) {
  if (v == null) return '—';
  if (v >= 1e6) return '$' + (v / 1e6).toFixed(2) + 'T';
  if (v >= 1000) return '$' + (v / 1000).toFixed(1) + 'B';
  return '$' + Math.round(v) + 'M';
}
function pct(v, dp = 2) {
  if (v == null) return '—';
  return (v >= 0 ? '+' : '') + v.toFixed(dp) + '%';
}
function pctCls(v) {
  if (v == null) return 'text-gray-500';
  return v >= 0 ? 'text-emerald-600' : 'text-red-500';
}
function money(v) {
  if (v == null) return '—';
  return '$' + Number(v).toFixed(2);
}
function acChip(ac) {
  if (!ac) return '';
  const l = ac.toLowerCase();
  let cls = 'ac-alt';
  if (l.includes('australian equit')) cls = 'ac-au';
  else if (l.includes('international') || l.includes('global equit')) cls = 'ac-int';
  else if (l.includes('fixed') || l.includes('bond') || l.includes('cash')) cls = 'ac-fi';
  else if (l.includes('property') || l.includes('real estate')) cls = 'ac-prop';
  else if (l.includes('commodit') || l.includes('digital') || l.includes('crypto')) cls = 'ac-com';
  else if (l.includes('diversif') || l.includes('multi')) cls = 'ac-div';
  const label = ac.length > 20 ? ac.slice(0, 18) + '…' : ac;
  return `<span class="text-xs px-1.5 py-0.5 rounded-full font-medium ${cls}">${label}</span>`;
}

/* ======================================================= API */
async function api(path) {
  const r = await fetch(API + path);
  return r.json();
}

/* ======================================================= filters */
async function loadFilters() {
  const [ex, iss, cat] = await Promise.all([
    api('/api/v1/exchanges'),
    api('/api/v1/issuers'),
    api('/api/v1/categories'),
  ]);
  const fEx = document.getElementById('f-exchange');
  (ex.exchanges || []).forEach(e => {
    const o = document.createElement('option');
    o.value = e.exchange;
    o.textContent = `${e.exchange} (${e.etf_count})`;
    fEx.appendChild(o);
  });
  const fIss = document.getElementById('f-issuer');
  (iss.issuers || []).forEach(i => {
    const o = document.createElement('option');
    o.value = i.name;
    o.textContent = `${i.name} (${i.etf_count})`;
    fIss.appendChild(o);
  });
  const fAss = document.getElementById('f-asset');
  (cat.categories || []).forEach(c => {
    const o = document.createElement('option');
    o.value = c.asset_class;
    o.textContent = `${c.asset_class} (${c.etf_count})`;
    fAss.appendChild(o);
  });
}

/* ======================================================= overview stats */
async function loadOverview() {
  const m = await api('/api/v1/market/overview');
  document.getElementById('c-fum').textContent = fmtFum(m.total_fum_millions);
  document.getElementById('c-count').textContent = (m.total_etfs || 0).toLocaleString();
  const ret = document.getElementById('c-ret');
  ret.textContent = pct(m.avg_return_1y);
  ret.className = 'text-2xl font-bold mt-1 leading-tight ' + pctCls(m.avg_return_1y);
  document.getElementById('c-exp').textContent =
    (m.avg_expense_ratio || 0).toFixed(2) + '%';
  if (m.top_performer) {
    document.getElementById('c-top').textContent =
      m.top_performer.code + ' ' + pct(m.top_performer.return_1y);
  }
  const now = new Date().toLocaleTimeString();
  document.getElementById('subtitle').textContent =
    `${(m.total_etfs || 0).toLocaleString()} ETFs · ${fmtFum(m.total_fum_millions)} FUM · ${now}`;
  document.getElementById('last-refresh').textContent = 'Live · ' + now;
}

/* ======================================================= table */
async function loadTable() {
  const params = new URLSearchParams();
  const ex  = document.getElementById('f-exchange').value;
  const iss = document.getElementById('f-issuer').value;
  const ac  = document.getElementById('f-asset').value;
  const srt = document.getElementById('f-sort').value;
  if (ex)  params.set('exchange',   ex);
  if (iss) params.set('issuer',     iss);
  if (ac)  params.set('asset_class', ac);
  params.set('sort_by', srt);
  params.set('limit',   pageSize);
  params.set('offset',  page * pageSize);

  const d = await api('/api/v1/etfs?' + params);
  renderTable(d.data || [], d.total || 0);
}

function renderTable(etfs, total) {
  const label = total.toLocaleString() + ' ETF' + (total !== 1 ? 's' : '');
  document.getElementById('result-count').textContent = label;
  document.getElementById('result-count-sidebar').textContent = label + ' matching';

  const tbody = document.getElementById('etf-table');
  tbody.innerHTML = etfs.map(e => {
    const sel = e.code === selectedCode ? 'row-selected' : '';
    return `<tr class="cursor-pointer ${sel}" data-code="${e.code}">
      <td class="px-3 py-2.5 text-gray-300 font-mono text-xs">${e.rank_by_fum || '—'}</td>
      <td class="px-3 py-2.5 max-w-0">
        <div class="font-bold text-gray-900">${e.code}</div>
        <div class="text-xs text-gray-400 truncate">${e.name || ''}</div>
      </td>
      <td class="px-3 py-2.5">${acChip(e.asset_class)}</td>
      <td class="px-3 py-2.5 text-right font-mono">${money(e.current_price)}</td>
      <td class="px-3 py-2.5 text-right">${fmtFum(e.fund_size_aud_millions)}</td>
      <td class="px-3 py-2.5 text-right font-semibold ${pctCls(e.return_1y)}">${pct(e.return_1y)}</td>
      <td class="px-3 py-2.5 text-right text-gray-600">
        ${e.distribution_yield != null ? e.distribution_yield.toFixed(1) + '%' : '—'}
      </td>
      <td class="px-3 py-2.5 text-right text-gray-400">
        ${e.expense_ratio != null ? e.expense_ratio.toFixed(2) + '%' : '—'}
      </td>
    </tr>`;
  }).join('');

  const pages = Math.ceil(total / pageSize);
  document.getElementById('pagination').innerHTML = `
    <span>Page ${page + 1} of ${pages || 1}</span>
    <div class="flex gap-1">
      <button onclick="changePage(-1)"
              class="px-3 py-1 border rounded-lg ${page <= 0 ? 'opacity-30 cursor-default' : 'hover:bg-gray-100'}"
              ${page <= 0 ? 'disabled' : ''}>&#8592; Prev</button>
      <button onclick="changePage(1)"
              class="px-3 py-1 border rounded-lg ${page >= pages - 1 ? 'opacity-30 cursor-default' : 'hover:bg-gray-100'}"
              ${page >= pages - 1 ? 'disabled' : ''}>Next &#8594;</button>
    </div>`;

  tbody.querySelectorAll('tr').forEach(tr =>
    tr.addEventListener('click', () => showDetail(tr.dataset.code))
  );
}

function changePage(dir) { page = Math.max(0, page + dir); loadTable(); }

/* ======================================================= detail panel */
async function showDetail(code) {
  selectedCode = code;
  document.querySelectorAll('#etf-table tr').forEach(tr =>
    tr.classList.toggle('row-selected', tr.dataset.code === code)
  );

  const panel = document.getElementById('detail-panel');
  panel.classList.remove('hidden');
  document.getElementById('d-code').textContent = code;
  document.getElementById('d-name').textContent = 'Loading…';
  document.getElementById('d-metrics').innerHTML = '';
  document.getElementById('tab-content').innerHTML =
    '<div class="flex justify-center py-10"><div class="spinner"></div></div>';

  const d = await api('/api/v1/etfs/' + code);
  document.getElementById('d-name').textContent = d.name || '';
  const badge = document.getElementById('d-badge');
  badge.textContent = d.exchange || 'ASX';
  badge.className = 'text-xs px-2 py-0.5 rounded-full font-medium badge-' +
                    (d.exchange || 'ASX').toLowerCase();

  // Quick metrics strip
  const metrics = [
    { label: 'Price',    value: money(d.current_price) },
    { label: 'Day Chg',  value: pct(d.day_change_pct), num: d.day_change_pct },
    { label: 'FUM',      value: fmtFum(d.fund_size_aud_millions) },
    { label: 'Rank',     value: d.rank_by_fum ? '#' + d.rank_by_fum : '—' },
    { label: '1Y Return', value: pct(d.return_1y), num: d.return_1y },
    { label: 'Yield',    value: d.distribution_yield != null ? d.distribution_yield.toFixed(1) + '%' : '—' },
    { label: 'MER',      value: (d.expense_ratio || d.management_fee) != null
                                ? (d.expense_ratio || d.management_fee).toFixed(2) + '%' : '—' },
    { label: 'Issuer',   value: d.issuer || '—' },
  ];
  document.getElementById('d-metrics').innerHTML = metrics.map(m => `
    <div class="px-4 py-3">
      <p class="text-xs text-gray-400 font-medium">${m.label}</p>
      <p class="font-semibold text-sm mt-0.5 truncate ${m.num != null ? pctCls(m.num) : 'text-gray-800'}">${m.value}</p>
    </div>`).join('');

  // Reset to overview tab
  document.querySelectorAll('.dtab').forEach(b => b.classList.remove('tab-active'));
  document.querySelector('.dtab[data-tab="overview"]').classList.add('tab-active');
  renderOverviewTab(d);
  panel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

/* ======================================================= tab rendering */
function renderOverviewTab(d) {
  const cards = [
    ['1M Return',     pct(d.return_1m),  d.return_1m],
    ['3M Return',     pct(d.return_3m),  d.return_3m],
    ['1Y Return',     pct(d.return_1y),  d.return_1y],
    ['3Y Return',     pct(d.return_3y),  d.return_3y],
    ['52W High',      money(d.year_high)],
    ['52W Low',       money(d.year_low)],
    ['Bid/Ask',       d.bid_ask_spread_pct != null ? d.bid_ask_spread_pct + '%' : '—'],
    ['Inception',     d.inception_date || '—'],
    ['Asset Class',   d.asset_class || '—'],
    ['Benchmark',     d.benchmark || '—'],
    ['Exchange',      d.exchange || '—'],
    ['Currency',      'AUD'],
  ];
  document.getElementById('tab-content').innerHTML = `
    <div class="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
      ${cards.map(([label, val, num]) => `
        <div class="bg-slate-50 rounded-lg p-3 border border-gray-100">
          <p class="text-gray-400 text-xs">${label}</p>
          <p class="font-semibold text-sm mt-0.5 ${num != null ? pctCls(num) : 'text-gray-800'}">${val}</p>
        </div>`).join('')}
    </div>
    ${d.description ? `<p class="mt-4 text-sm text-gray-600 leading-relaxed border-t pt-4">${d.description}</p>` : ''}
    ${d.issuer_url ? `<a href="${d.issuer_url}" target="_blank" rel="noopener"
       class="mt-3 inline-flex items-center gap-1 text-blue-600 hover:underline text-sm">
       View on issuer site &#8594;</a>` : ''}`;
}

async function showTab(tab) {
  document.querySelectorAll('.dtab').forEach(b =>
    b.classList.toggle('tab-active', b.dataset.tab === tab)
  );
  const el = document.getElementById('tab-content');

  if (tab === 'overview') {
    el.innerHTML = '<div class="flex justify-center py-10"><div class="spinner"></div></div>';
    const d = await api('/api/v1/etfs/' + selectedCode);
    renderOverviewTab(d);

  } else if (tab === 'holdings') {
    el.innerHTML = '<div class="flex justify-center py-10"><div class="spinner"></div></div>';
    const h = await api('/api/v1/etfs/' + selectedCode + '/holdings');
    if (!h.holdings || !h.holdings.length) {
      el.innerHTML = '<p class="text-gray-400 text-sm text-center py-10">No holdings data available.</p>';
      return;
    }
    const maxW = Math.max(...h.holdings.map(x => x.weight_pct || 0));
    el.innerHTML = `
      <p class="text-xs text-gray-400 mb-3">${h.holdings.length} holdings · sorted by weight</p>
      <div class="space-y-2">
        ${h.holdings.slice(0, 30).map(r => `
          <div class="flex items-center gap-3">
            <div class="w-14 text-xs font-mono text-gray-400 shrink-0 text-right">${r.ticker || ''}</div>
            <div class="flex-1 min-w-0">
              <div class="flex items-center justify-between mb-1">
                <span class="text-xs font-medium text-gray-700 truncate">${r.name || ''}</span>
                <span class="text-xs font-bold text-blue-700 ml-2 shrink-0">
                  ${r.weight_pct != null ? r.weight_pct.toFixed(2) + '%' : '—'}
                </span>
              </div>
              <div class="h-1.5 bg-gray-100 rounded-full overflow-hidden">
                <div class="h-full bg-blue-400 rounded-full pbar"
                     style="width:${maxW > 0 ? ((r.weight_pct || 0) / maxW * 100).toFixed(1) : 0}%"></div>
              </div>
            </div>
            <div class="text-xs text-gray-400 w-28 shrink-0 truncate">${r.sector || ''}</div>
          </div>`).join('')}
        ${h.holdings.length > 30 ?
          `<p class="text-xs text-gray-400 pt-2 text-center">+${h.holdings.length - 30} more holdings</p>` : ''}
      </div>`;

  } else if (tab === 'sectors') {
    el.innerHTML = '<div class="flex justify-center py-10"><div class="spinner"></div></div>';
    const s = await api('/api/v1/etfs/' + selectedCode + '/sectors');
    if (!s.sectors || !s.sectors.length) {
      el.innerHTML = '<p class="text-gray-400 text-sm text-center py-10">No sector data available.</p>';
      return;
    }
    const max = Math.max(...s.sectors.map(x => x.weight_pct || 0));
    el.innerHTML = '<div class="space-y-3">' + s.sectors.map((r, i) => `
      <div class="flex items-center gap-3">
        <div class="w-2.5 h-2.5 rounded-sm shrink-0" style="background:${PALETTE[i % PALETTE.length]}"></div>
        <span class="text-sm text-gray-700 w-44 truncate">${r.sector}</span>
        <div class="flex-1 bg-gray-100 rounded-full h-2.5 overflow-hidden">
          <div class="h-full rounded-full pbar"
               style="width:${(r.weight_pct / max * 100).toFixed(0)}%;background:${PALETTE[i % PALETTE.length]}">
          </div>
        </div>
        <span class="text-sm font-semibold text-gray-700 w-14 text-right">
          ${r.weight_pct != null ? r.weight_pct.toFixed(1) + '%' : '—'}
        </span>
      </div>`).join('') + '</div>';

  } else if (tab === 'dividends') {
    el.innerHTML = '<div class="flex justify-center py-10"><div class="spinner"></div></div>';
    const dv = await api('/api/v1/etfs/' + selectedCode + '/dividends');
    if (!dv.dividends || !dv.dividends.length) {
      el.innerHTML = '<p class="text-gray-400 text-sm text-center py-10">No dividend data available.</p>';
      return;
    }
    el.innerHTML = `<div class="overflow-x-auto">
      <table class="w-full text-sm">
        <thead><tr class="text-left text-xs text-gray-400 font-semibold uppercase tracking-wide border-b pb-2">
          <th class="pb-2">Ex Date</th>
          <th class="pb-2">Pay Date</th>
          <th class="pb-2 text-right">Amount</th>
          <th class="pb-2 text-right">Franking</th>
          <th class="pb-2">Type</th>
        </tr></thead>
        <tbody class="divide-y divide-gray-100">
          ${dv.dividends.map(r => `<tr>
            <td class="py-2">${r.ex_date || ''}</td>
            <td class="py-2 text-gray-500">${r.pay_date || ''}</td>
            <td class="py-2 text-right font-mono">${r.amount != null ? '$' + r.amount.toFixed(4) : '—'}</td>
            <td class="py-2 text-right">${r.franking_pct != null ? r.franking_pct + '%' : '—'}</td>
            <td class="py-2 text-gray-500">${r.type || ''}</td>
          </tr>`).join('')}
        </tbody>
      </table>
    </div>`;
  }
}

document.querySelectorAll('.dtab').forEach(b =>
  b.addEventListener('click', () => showTab(b.dataset.tab))
);
document.getElementById('d-close').addEventListener('click', () => {
  document.getElementById('detail-panel').classList.add('hidden');
  selectedCode = null;
  document.querySelectorAll('#etf-table tr').forEach(tr => tr.classList.remove('row-selected'));
});

/* ======================================================= search */
let searchTimer;
document.getElementById('search').addEventListener('input', function () {
  clearTimeout(searchTimer);
  const q = this.value.trim();
  const el = document.getElementById('search-results');
  if (!q) { el.classList.add('hidden'); return; }
  searchTimer = setTimeout(async () => {
    const d = await api('/api/v1/search?q=' + encodeURIComponent(q));
    if (!d.results || !d.results.length) {
      el.innerHTML = '<p class="p-3 text-sm text-gray-400">No results</p>';
      el.classList.remove('hidden');
      return;
    }
    el.innerHTML = d.results.slice(0, 10).map(r => `
      <div class="px-3 py-2.5 hover:bg-blue-50 cursor-pointer flex justify-between items-center border-b last:border-b-0"
           data-code="${r.code}">
        <div>
          <span class="font-bold text-gray-900">${r.code}</span>
          <span class="text-gray-500 text-xs ml-2">${r.name || ''}</span>
        </div>
        <span class="text-xs text-gray-400 bg-gray-100 px-1.5 py-0.5 rounded">${r.exchange || 'ASX'}</span>
      </div>`).join('');
    el.classList.remove('hidden');
    el.querySelectorAll('[data-code]').forEach(node =>
      node.addEventListener('click', () => {
        showDetail(node.dataset.code);
        el.classList.add('hidden');
        document.getElementById('search').value = '';
      })
    );
  }, 250);
});
document.addEventListener('click', e => {
  if (!e.target.closest('#search') && !e.target.closest('#search-results'))
    document.getElementById('search-results').classList.add('hidden');
});

/* ======================================================= filter wiring */
['f-exchange', 'f-issuer', 'f-asset', 'f-sort'].forEach(id =>
  document.getElementById(id).addEventListener('change', () => { page = 0; loadTable(); })
);
document.getElementById('btn-reset').addEventListener('click', () => {
  ['f-exchange', 'f-issuer', 'f-asset'].forEach(id => document.getElementById(id).value = '');
  document.getElementById('f-sort').value = 'rank';
  page = 0;
  loadTable();
});

/* ======================================================= analytics */
async function loadAnalytics() {
  const [flows, issuers, cats] = await Promise.all([
    api('/api/v1/analytics/fund-flows?limit=6'),
    api('/api/v1/issuers'),
    api('/api/v1/categories'),
  ]);

  // Issuer count stat card
  document.getElementById('c-issuers').textContent = (issuers.issuers || []).length;

  // Asset class doughnut
  const catData = (cats.categories || []).filter(c => c.total_fum > 0).slice(0, 9);
  if (chartAsset) chartAsset.destroy();
  chartAsset = new Chart(
    document.getElementById('chart-asset').getContext('2d'), {
      type: 'doughnut',
      data: {
        labels: catData.map(c => c.asset_class || 'Other'),
        datasets: [{
          data: catData.map(c => c.total_fum || 0),
          backgroundColor: PALETTE,
          borderWidth: 2,
          borderColor: '#fff',
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: '62%',
        plugins: {
          legend: {
            position: 'bottom',
            labels: { font: { size: 10 }, padding: 8, boxWidth: 10 },
          },
          tooltip: {
            callbacks: { label: ctx => ' ' + ctx.label + ': ' + fmtFum(ctx.parsed) },
          },
        },
      },
    }
  );

  // Issuer bars
  const topIss = (issuers.issuers || []).slice(0, 10);
  const totalFum = topIss.reduce((s, i) => s + (i.total_fum || 0), 0);
  document.getElementById('a-issuers').innerHTML = topIss.map((i, idx) => {
    const p = totalFum > 0 ? ((i.total_fum || 0) / totalFum * 100) : 0;
    return `<div class="flex items-center gap-2">
      <div class="w-2.5 h-2.5 rounded-sm shrink-0" style="background:${PALETTE[idx % PALETTE.length]}"></div>
      <span class="text-xs text-gray-700 w-24 truncate">${i.name}</span>
      <div class="flex-1 bg-gray-100 rounded-full h-2 overflow-hidden">
        <div class="h-full rounded-full" style="width:${p.toFixed(1)}%;background:${PALETTE[idx % PALETTE.length]}"></div>
      </div>
      <span class="text-xs text-gray-500 w-10 text-right">${p.toFixed(1)}%</span>
    </div>`;
  }).join('');

  // Flows bar chart (top inflows + outflows combined)
  const inflows  = (flows.top_inflows  || []).slice(0, 5);
  const outflows = (flows.top_outflows || []).slice(0, 5);
  const flowLabels = [...inflows.map(r => r.code), ...outflows.map(r => r.code)];
  const flowVals   = [...inflows.map(r => r.fund_flow_1m || 0),
                      ...outflows.map(r => r.fund_flow_1m || 0)];
  const flowBg = flowVals.map(v => v >= 0 ? 'rgba(16,185,129,.8)' : 'rgba(239,68,68,.8)');

  if (chartFlows) chartFlows.destroy();
  chartFlows = new Chart(
    document.getElementById('chart-flows').getContext('2d'), {
      type: 'bar',
      data: {
        labels: flowLabels,
        datasets: [{
          data: flowVals,
          backgroundColor: flowBg,
          borderRadius: 4,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: { label: ctx => ' ' + fmtFum(ctx.parsed.y) },
          },
        },
        scales: {
          y: {
            grid: { color: '#f1f5f9' },
            ticks: { font: { size: 10 }, callback: v => fmtFum(v) },
          },
          x: {
            grid: { display: false },
            ticks: { font: { size: 10 } },
          },
        },
      },
    }
  );
}

/* ======================================================= init */
async function init() {
  try {
    await Promise.all([loadFilters(), loadOverview(), loadTable(), loadAnalytics()]);
  } catch (e) {
    console.error('Init error:', e);
  }
}

init();
setInterval(() => { loadOverview(); loadTable(); }, 120000);
</script>
</body>
</html>'''


# ===================================================================== main
def run_server(port=None):
    if port is None:
        port = int(os.getenv('ETF_PORT', '8081'))

    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", port), ETFAPIHandler) as httpd:
        print(f"ETF Dashboard API running at http://localhost:{port}")
        print(f"  Dashboard: http://localhost:{port}/dashboard")
        print(f"  API:       http://localhost:{port}/api/v1/etfs")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down.")


if __name__ == "__main__":
    port = None
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print("Usage: python3 etf_server_with_dashboard.py [port]")
            sys.exit(1)
    run_server(port)
