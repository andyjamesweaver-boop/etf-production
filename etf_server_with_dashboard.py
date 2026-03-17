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

DB_PATH = os.environ.get('DATABASE_PATH',
          os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etf_data.db'))


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
            '/api/v1/screener': lambda: self.handle_screener(qs),
            '/api/v1/compare':         lambda: self.handle_compare(qs),
            '/api/v1/compare/overlap': lambda: self.handle_compare_overlap(qs),
            '/api/v1/holdings/search': lambda: self.handle_holdings_search(qs),
            # Insights API
            '/api/v1/insights/fum':      self.handle_insights_fum,
            '/api/v1/insights/listings': self.handle_insights_listings,
            '/api/v1/insights/returns':  self.handle_insights_returns,
            '/api/v1/insights/expense':  self.handle_insights_expense,
            '/api/v1/insights/issuers':  self.handle_insights_issuers,
            '/api/v1/insights/nav':      self.handle_insights_nav,
            # Insights pages
            '/insights/fum':      lambda: self.handle_insights_page('fum'),
            '/insights/listings': lambda: self.handle_insights_page('listings'),
            '/insights/returns':  lambda: self.handle_insights_page('returns'),
            '/insights/expense':  lambda: self.handle_insights_page('expense'),
            '/insights/issuers':  lambda: self.handle_insights_page('issuers'),
            '/insights/nav':      lambda: self.handle_insights_page('nav'),
            '/admin/sync-db':     self.handle_sync_db,
            # Articles
            '/articles':          self.handle_articles_list,
            # Preferences screener
            '/screener/preferences': self.handle_screener_preferences_page,
        }

        handler = routes.get(path)
        if handler:
            handler()
            return

        # Articles detail: /articles/{slug}
        m = re.match(r'^/articles/([a-z0-9\-]+)$', path)
        if m:
            self.handle_article_detail(m.group(1))
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
            elif sub == 'nav-history':
                self.handle_etf_nav_history(code, qs)
            elif sub == 'similar':
                self.handle_etf_similar(code)
            else:
                self.send_json({'error': f'Unknown sub-resource: {sub}'}, 404)
            return

        self.send_json({'error': 'Not found'}, 404)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path.rstrip('/')
        if path == '/admin/sync-db':
            self.handle_sync_db()
        elif path == '/api/v1/screener/preferences':
            self.handle_screener_preferences_api()
        else:
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
                '/api/v1/scrape-status', '/api/v1/screener',
                '/api/v1/compare', '/api/v1/holdings/search',
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
            benchmark = self._str(qs, 'benchmark')
            fund_type = self._str(qs, 'fund_type')

            if exchange:
                where.append("exchange = ?"); params.append(exchange.upper())
            if issuer:
                where.append("issuer = ?"); params.append(issuer)
            if asset_class or category:
                where.append("asset_class = ?"); params.append(asset_class or category)
            if benchmark:
                where.append("benchmark LIKE ?"); params.append(f'%{benchmark}%')
            if fund_type:
                where.append("fund_type = ?"); params.append(fund_type)

            sort_field_map = {
                'fum':       ('fund_size_aud_millions', 'DESC'),
                'price':     ('current_price',          'DESC'),
                'return_1y': ('return_1y',              'DESC'),
                'return_3y': ('return_3y',              'DESC'),
                'return_5y': ('return_5y',              'DESC'),
                'yield':     ('distribution_yield',     'DESC'),
                'expense':   ('expense_ratio',          'ASC'),
                'name':      ('name',                   'ASC'),
                'code':      ('code',                   'ASC'),
                'rank':      ('rank_by_fum',            'ASC'),
            }
            sort_by  = self._str(qs, 'sort_by',  'rank')
            sort_dir = self._str(qs, 'sort_dir', '').upper()
            field, default_dir = sort_field_map.get(sort_by, ('rank_by_fum', 'ASC'))
            direction = sort_dir if sort_dir in ('ASC', 'DESC') else default_dir
            order = f'{field} {direction} NULLS LAST'

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
            d = dict(row)
            # Compute units change from history (current minus previous record)
            hist = conn.execute(
                "SELECT units_on_issue, date FROM units_history "
                "WHERE etf_code = ? AND units_on_issue IS NOT NULL "
                "ORDER BY date DESC LIMIT 2",
                (code,)
            ).fetchall()
            if len(hist) >= 2:
                d['units_change'] = hist[0]['units_on_issue'] - hist[1]['units_on_issue']
                d['units_change_date'] = hist[0]['date']
                d['units_change_prev_date'] = hist[1]['date']
            else:
                d['units_change'] = None
                d['units_change_date'] = None
                d['units_change_prev_date'] = None
            self.send_json(d)
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Holdings -----
    def handle_etf_holdings(self, code):
        conn = get_db()
        try:
            rows = conn.execute(
                "SELECT name, ticker, weight_pct, sector, country, last_updated FROM etf_holdings "
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
                "SELECT sector, weight_pct, last_updated FROM etf_sectors "
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

    # ----- NAV history -----
    def handle_etf_nav_history(self, code, qs):
        period = self._str(qs, 'period', '1y')
        period_days = {'1m': 31, '3m': 92, '6m': 183, '1y': 365,
                       '3y': 1095, '5y': 1825, 'all': 9999}.get(period, 365)
        from datetime import date, timedelta
        since = (date.today() - timedelta(days=period_days)).isoformat()
        conn = get_db()
        try:
            rows = conn.execute(
                "SELECT date, nav, close_price, premium_discount_pct, source "
                "FROM nav_history WHERE etf_code=? AND date>=? ORDER BY date ASC",
                (code, since)
            ).fetchall()
            self.send_json({'code': code, 'nav_history': [dict(r) for r in rows]})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Price history -----
    def handle_etf_price_history(self, code, qs):
        # period: 1m | 3m | 6m | 1y | 3y | 5y  (default 1y)
        period = self._str(qs, 'period', '1y')
        period_days = {'1m': 31, '3m': 92, '6m': 183, '1y': 365,
                       '3y': 1095, '5y': 1825}.get(period, 365)
        from datetime import date, timedelta
        since = (date.today() - timedelta(days=period_days)).isoformat()

        conn = get_db()
        try:
            # ETF price series
            rows = conn.execute(
                "SELECT date, open, high, low, close, volume FROM price_history "
                "WHERE etf_code=? AND date>=? ORDER BY date ASC",
                (code, since)
            ).fetchall()

            # Benchmark for this ETF's asset class
            ac = conn.execute(
                "SELECT asset_class FROM etfs WHERE code=?", (code,)
            ).fetchone()
            ac = ac['asset_class'] if ac else None

            # Map asset class → primary benchmark (must match price_history_fetcher.py)
            AC_BENCHMARK = {
                'Australian Equities':    'ASX200',
                'International Equities': 'SP500',
                'US Equities':            'SP500',
                'Fixed Income':           'AU_BOND',
                'Property':               'ASX200',
                'Commodities':            'GOLD',
                'Cash':                   'AU_CASH',
                'Multi-Asset':            'SP500',
                'Thematic':              'SP500',
                'Currency':               'SP500',
                'Digital Assets':         'BTC',
                'Alternatives':           'SP500',
                'Diversified':            'SP500',
                'leveraged & inverse':    'ASX200',
            }
            bmark_name = AC_BENCHMARK.get(ac, 'ASX200')

            # All benchmark series for this period
            bmark_rows = conn.execute(
                "SELECT ticker, date, close FROM benchmark_history "
                "WHERE date>=? ORDER BY ticker, date ASC",
                (since,)
            ).fetchall()

            # Group benchmarks by ticker
            benchmarks = {}
            for r in bmark_rows:
                t = r['ticker']
                if t not in benchmarks:
                    benchmarks[t] = []
                benchmarks[t].append({'date': r['date'], 'close': r['close']})

            # Period return table from price history
            prices = [r['close'] for r in rows if r['close']]
            period_return = None
            if len(prices) >= 2:
                period_return = round((prices[-1] / prices[0] - 1) * 100, 2)

            self.send_json({
                'code': code,
                'asset_class': ac,
                'default_benchmark': bmark_name,
                'period': period,
                'prices': [dict(r) for r in rows],
                'benchmarks': benchmarks,
                'period_return': period_return,
            })
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

            avg_pd_row = conn.execute(
                "SELECT ROUND(AVG(premium_discount_pct), 2) FROM nav_history "
                "WHERE date = (SELECT MAX(date) FROM nav_history) "
                "AND premium_discount_pct IS NOT NULL"
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
                'avg_premium_discount': avg_pd_row[0] if avg_pd_row else None,
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
                "SELECT code, name, issuer, fund_flow_1m FROM etfs "
                "WHERE fund_flow_1m IS NOT NULL ORDER BY fund_flow_1m DESC LIMIT ?",
                (limit,)
            ).fetchall()
            outflows = conn.execute(
                "SELECT code, name, issuer, fund_flow_1m FROM etfs "
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
                "SELECT code, name, issuer, asset_class, exchange, benchmark, "
                "current_price, fund_size_aud_millions, return_1y "
                "FROM etfs WHERE code LIKE ? OR name LIKE ? OR issuer LIKE ? OR benchmark LIKE ? "
                "ORDER BY fund_size_aud_millions DESC LIMIT 50",
                (pattern, pattern, pattern, pattern)
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

    # ----- Screener -----
    def handle_screener(self, qs):
        conn = get_db()
        try:
            where, params = ['1=1'], []
            exchange     = self._str(qs, 'exchange')
            asset_class  = self._str(qs, 'asset_class')
            issuer       = self._str(qs, 'issuer')
            max_fee      = self._str(qs, 'max_fee')
            min_fum      = self._str(qs, 'min_fum')
            min_ret_1y   = self._str(qs, 'min_return_1y')
            max_ret_1y   = self._str(qs, 'max_return_1y')
            min_yield    = self._str(qs, 'min_yield')
            fx_hedged    = self._str(qs, 'fx_hedged')

            if exchange:
                where.append("exchange = ?"); params.append(exchange.upper())
            if asset_class:
                where.append("asset_class = ?"); params.append(asset_class)
            if issuer:
                where.append("issuer = ?"); params.append(issuer)
            if max_fee is not None:
                try: where.append("expense_ratio <= ?"); params.append(float(max_fee))
                except ValueError: pass
            if min_fum is not None:
                try: where.append("fund_size_aud_millions >= ?"); params.append(float(min_fum))
                except ValueError: pass
            if min_ret_1y is not None:
                try: where.append("return_1y >= ?"); params.append(float(min_ret_1y))
                except ValueError: pass
            if max_ret_1y is not None:
                try: where.append("return_1y <= ?"); params.append(float(max_ret_1y))
                except ValueError: pass
            if min_yield is not None:
                try: where.append("distribution_yield >= ?"); params.append(float(min_yield))
                except ValueError: pass
            if fx_hedged and fx_hedged.lower() in ('1', 'true', 'yes'):
                where.append("fx_hedged = 1")

            sort_map = {
                'fum': 'fund_size_aud_millions DESC',
                'return_1y': 'return_1y DESC',
                'yield': 'distribution_yield DESC',
                'expense': 'expense_ratio ASC',
                'name': 'name ASC',
                'code': 'code ASC',
            }
            sort_by = self._str(qs, 'sort_by', 'fum')
            order = sort_map.get(sort_by, 'fund_size_aud_millions DESC')
            limit  = min(self._int(qs, 'limit', 200), 500)
            offset = self._int(qs, 'offset', 0)

            w = ' AND '.join(where)
            total = conn.execute(f"SELECT COUNT(*) FROM etfs WHERE {w}", params).fetchone()[0]
            rows = conn.execute(
                f"SELECT * FROM etfs WHERE {w} ORDER BY {order} LIMIT ? OFFSET ?",
                params + [limit, offset]
            ).fetchall()
            self.send_json({'data': [dict(r) for r in rows], 'total': total,
                            'limit': limit, 'offset': offset})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Compare -----
    def handle_compare(self, qs):
        codes_str = self._str(qs, 'codes', '')
        if not codes_str:
            self.send_json({'error': 'codes parameter required'}, 400)
            return
        codes = [c.strip().upper() for c in codes_str.split(',') if c.strip()][:5]
        if not codes:
            self.send_json({'error': 'No valid codes provided'}, 400)
            return
        conn = get_db()
        try:
            placeholders = ','.join('?' * len(codes))
            rows = conn.execute(
                f"SELECT * FROM etfs WHERE code IN ({placeholders}) "
                "ORDER BY fund_size_aud_millions DESC",
                codes
            ).fetchall()
            self.send_json({'data': [dict(r) for r in rows]})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Compare overlap (shared holdings between selected ETFs) -----
    def handle_compare_overlap(self, qs):
        import re as _re
        codes_str = self._str(qs, 'codes', '')
        codes = [c.strip().upper() for c in codes_str.split(',') if c.strip()][:5]
        if len(codes) < 2:
            self.send_json({'error': 'Need at least 2 codes'}, 400)
            return

        conn = get_db()
        try:
            placeholders = ','.join('?' * len(codes))
            rows = conn.execute(
                f"SELECT etf_code, ticker, name, weight_pct FROM etf_holdings "
                f"WHERE etf_code IN ({placeholders}) AND weight_pct IS NOT NULL AND weight_pct > 0",
                codes
            ).fetchall()

            if not rows:
                self.send_json({'codes': codes, 'overlap': [], 'coverage': {}, 'total_overlap_count': 0})
                return

            # Build name→ticker lookup for canonicalisation (same logic as similar ETFs)
            name_to_ticker = {}
            for r in rows:
                if not r['ticker'] or not r['name']:
                    continue
                norm_t = _re.sub(r'-[A-Z]{2,3}$', '', r['ticker'].upper().strip())
                if not norm_t:
                    continue
                norm_n = _re.sub(r'\s+', ' ', r['name'].upper().strip())
                name_to_ticker[norm_n] = norm_t
                if len(norm_n) > 28:
                    name_to_ticker[norm_n[:28]] = norm_t

            def _key(ticker, name):
                if ticker:
                    return _re.sub(r'-[A-Z]{2,3}$', '', ticker.upper().strip())
                if name:
                    norm_n = _re.sub(r'\s+', ' ', name.upper().strip())
                    return name_to_ticker.get(norm_n, norm_n)
                return ''

            # Build per-ETF weight dicts and best display name per key
            etf_weights = {code: {} for code in codes}
            key_to_name = {}

            for r in rows:
                ec = r['etf_code']
                if ec not in etf_weights:
                    continue
                k = _key(r['ticker'], r['name'])
                if not k:
                    continue
                etf_weights[ec][k] = etf_weights[ec].get(k, 0) + r['weight_pct']
                if k not in key_to_name and r['name']:
                    key_to_name[k] = r['name']

            # Find holdings that appear in 2+ of the selected ETFs
            all_keys = set()
            for weights in etf_weights.values():
                all_keys.update(weights.keys())

            overlap_items = []
            for k in all_keys:
                etf_count = sum(1 for c in codes if k in etf_weights[c])
                if etf_count < 2:
                    continue
                weights_per_etf = {c: round(etf_weights[c].get(k, 0), 4) for c in codes}
                max_weight = max(w for w in weights_per_etf.values())
                overlap_items.append({
                    'key': k,
                    'name': key_to_name.get(k, k),
                    'etf_count': etf_count,
                    'weights': weights_per_etf,
                    'max_weight': max_weight,
                })

            # Sort: most ETFs first, then by max weight descending
            overlap_items.sort(key=lambda x: (-x['etf_count'], -x['max_weight']))

            # Coverage: sum of weights in overlap holdings for each ETF
            overlap_keys = {item['key'] for item in overlap_items}
            coverage = {
                code: round(sum(w for k, w in etf_weights[code].items() if k in overlap_keys), 2)
                for code in codes
            }

            self.send_json({
                'codes': codes,
                'overlap': overlap_items[:60],
                'coverage': coverage,
                'total_overlap_count': len(overlap_items),
            })
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Similar ETFs (by portfolio holdings overlap) -----
    def handle_etf_similar(self, code):
        import re as _re
        conn = get_db()
        try:
            # Fetch target ETF metadata
            target = conn.execute(
                "SELECT code, asset_class, name FROM etfs WHERE code=?", (code,)
            ).fetchone()
            if not target:
                self.send_json({'error': 'ETF not found'}, 404)
                return

            target_ac = target['asset_class']

            # Load all holdings
            rows = conn.execute(
                "SELECT etf_code, ticker, name, weight_pct FROM etf_holdings "
                "WHERE weight_pct IS NOT NULL AND weight_pct > 0"
            ).fetchall()

            # --- Step 1: Build name→ticker lookup from holdings that have tickers.
            # Also index first-28-char prefixes to match Dimensional ETFs that
            # truncate names at 28 characters (e.g. "COMMONWEALTH BANK OF AUSTRAL").
            name_to_ticker = {}
            for r in rows:
                if not r['ticker'] or not r['name']:
                    continue
                norm_t = _re.sub(r'-[A-Z]{2,3}$', '', r['ticker'].upper().strip())
                if not norm_t:
                    continue
                norm_n = _re.sub(r'\s+', ' ', r['name'].upper().strip())
                name_to_ticker[norm_n] = norm_t
                if len(norm_n) > 28:
                    name_to_ticker[norm_n[:28]] = norm_t

            # --- Step 2: Canonical key for a holding.
            # Prefer ticker; for name-only holdings resolve via lookup.
            def _key(ticker, name):
                if ticker:
                    return _re.sub(r'-[A-Z]{2,3}$', '', ticker.upper().strip())
                if name:
                    norm_n = _re.sub(r'\s+', ' ', name.upper().strip())
                    return name_to_ticker.get(norm_n, norm_n)
                return ''

            # --- Step 3: Build per-ETF weight dicts keyed by canonical key
            holdings_map = {}
            for r in rows:
                ec = r['etf_code']
                k = _key(r['ticker'], r['name'])
                if not k:
                    continue
                if ec not in holdings_map:
                    holdings_map[ec] = {}
                holdings_map[ec][k] = holdings_map[ec].get(k, 0) + r['weight_pct']

            target_h = holdings_map.get(code)
            if not target_h:
                self.send_json({'similar': [], 'message': 'No holdings data for this ETF'})
                return

            # --- Step 4: Compute weighted overlap for all other ETFs
            # Fetch asset classes for all ETFs so we can prioritise same-class
            ac_map = {r['code']: r['asset_class'] for r in conn.execute(
                "SELECT code, asset_class FROM etfs"
            ).fetchall()}

            same_class, other_class = [], []
            for other_code, other_h in holdings_map.items():
                if other_code == code:
                    continue
                shared = sum(
                    min(target_h[k], other_h[k])
                    for k in target_h
                    if k in other_h
                )
                if shared <= 0:
                    continue
                entry = (other_code, round(shared, 2))
                if ac_map.get(other_code) == target_ac:
                    same_class.append(entry)
                else:
                    other_class.append(entry)

            same_class.sort(key=lambda x: x[1], reverse=True)
            other_class.sort(key=lambda x: x[1], reverse=True)

            # Return up to 8 same-class, then fill remaining slots (up to 12 total)
            # with cross-class ETFs that have meaningful overlap (>= 5%)
            SAME_LIMIT  = 8
            TOTAL_LIMIT = 12
            top_same  = same_class[:SAME_LIMIT]
            top_other = [e for e in other_class if e[1] >= 5.0][: TOTAL_LIMIT - len(top_same)]
            combined  = top_same + top_other
            top_codes = [c for c, _ in combined]

            if not top_codes:
                self.send_json({'similar': [], 'same_class_count': 0})
                return

            # --- Step 5: Fetch metadata
            score_map = {c: s for c, s in combined}
            placeholders = ','.join('?' * len(top_codes))
            etf_rows = conn.execute(
                f"SELECT code, name, asset_class, issuer, fund_size_aud_millions, "
                f"       COALESCE(expense_ratio, management_fee) AS expense_ratio, "
                f"       return_1y "
                f"FROM etfs WHERE code IN ({placeholders})",
                top_codes
            ).fetchall()

            same_result, other_result = [], []
            for r in etf_rows:
                d = dict(r)
                d['overlap_pct'] = score_map.get(r['code'], 0)
                if r['asset_class'] == target_ac:
                    same_result.append(d)
                else:
                    other_result.append(d)

            same_result.sort(key=lambda x: x['overlap_pct'], reverse=True)
            other_result.sort(key=lambda x: x['overlap_pct'], reverse=True)

            self.send_json({
                'code': code,
                'asset_class': target_ac,
                'same_class': same_result,
                'other_class': other_result,
                # Legacy flat list for backwards compat
                'similar': same_result + other_result,
            })
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Holdings search -----
    def handle_holdings_search(self, qs):
        q = self._str(qs, 'q', '')
        if not q:
            self.send_json({'error': 'q parameter required'}, 400)
            return
        try:
            min_weight = float(self._str(qs, 'min_weight') or '0')
        except ValueError:
            min_weight = 0.0
        conn = get_db()
        try:
            pattern = f'%{q.lower()}%'
            rows = conn.execute(
                "SELECT h.etf_code, e.name AS etf_name, e.asset_class, e.issuer, "
                "       h.name AS holding_name, h.ticker, h.weight_pct, h.sector "
                "FROM etf_holdings h JOIN etfs e ON e.code = h.etf_code "
                "WHERE (LOWER(h.name) LIKE ? OR LOWER(h.ticker) LIKE ?) "
                "  AND COALESCE(h.weight_pct, 0) >= ? "
                "ORDER BY h.weight_pct DESC "
                "LIMIT 100",
                (pattern, pattern, min_weight)
            ).fetchall()
            etf_codes = list(dict.fromkeys(r['etf_code'] for r in rows))
            self.send_json({
                'query': q,
                'total': len(rows),
                'etf_count': len(etf_codes),
                'etfs': etf_codes,
                'results': [dict(r) for r in rows],
            })
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ================================================================== INSIGHTS PAGES
    def handle_insights_page(self, name):
        from insights_pages import get_insights_page
        html = get_insights_page(name)
        if html:
            self.send_html(html)
        else:
            self.send_json({'error': f'Unknown insight page: {name}'}, 404)

    # ----- Insights: FUM analysis -----
    def handle_insights_fum(self):
        conn = get_db()
        try:
            total = conn.execute(
                "SELECT COALESCE(SUM(fund_size_aud_millions),0) FROM etfs"
            ).fetchone()[0] or 1

            top_etfs = conn.execute(
                "SELECT code, name, issuer, asset_class, fund_size_aud_millions AS fum "
                "FROM etfs WHERE fund_size_aud_millions > 0 ORDER BY fum DESC LIMIT 50"
            ).fetchall()

            by_issuer = conn.execute(
                "SELECT issuer, COALESCE(SUM(fund_size_aud_millions),0) AS total_fum, "
                "COUNT(*) AS etf_count FROM etfs WHERE issuer IS NOT NULL "
                "GROUP BY issuer ORDER BY total_fum DESC"
            ).fetchall()

            by_ac = conn.execute(
                "SELECT asset_class, COALESCE(SUM(fund_size_aud_millions),0) AS total_fum, "
                "COUNT(*) AS etf_count FROM etfs WHERE asset_class IS NOT NULL "
                "GROUP BY asset_class ORDER BY total_fum DESC"
            ).fetchall()

            # Top 5 per asset class and per issuer
            top_per_ac, top_per_issuer = {}, {}
            for row in by_ac:
                ac = row['asset_class']
                rows = conn.execute(
                    "SELECT code, name, fund_size_aud_millions AS fum FROM etfs "
                    "WHERE asset_class=? AND fund_size_aud_millions>0 ORDER BY fum DESC LIMIT 5",
                    (ac,)
                ).fetchall()
                top_per_ac[ac] = [dict(r) for r in rows]
            for row in by_issuer:
                iss = row['issuer']
                rows = conn.execute(
                    "SELECT code, name, fund_size_aud_millions AS fum FROM etfs "
                    "WHERE issuer=? AND fund_size_aud_millions>0 ORDER BY fum DESC LIMIT 5",
                    (iss,)
                ).fetchall()
                top_per_issuer[iss] = [dict(r) for r in rows]

            fums = [r['fum'] for r in top_etfs]
            conc = {
                'top5_pct':  round(sum(fums[:5])  / total * 100, 1),
                'top10_pct': round(sum(fums[:10]) / total * 100, 1),
                'top20_pct': round(sum(fums[:20]) / total * 100, 1),
            }
            self.send_json({
                'total_fum': round(total, 2),
                'top_etfs': [dict(r) | {'pct': round(r['fum'] / total * 100, 2)} for r in top_etfs],
                'by_issuer': [dict(r) | {'pct': round(r['total_fum'] / total * 100, 2)} for r in by_issuer],
                'by_asset_class': [dict(r) | {'pct': round(r['total_fum'] / total * 100, 2)} for r in by_ac],
                'top_per_asset_class': top_per_ac,
                'top_per_issuer': top_per_issuer,
                'concentration': conc,
            })
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Insights: listings history -----
    def handle_insights_listings(self):
        import re as _re
        def year_from(d):
            if not d: return None
            m = _re.search(r'\b(20\d{2}|199\d)\b', str(d))
            return m.group(1) if m else None

        conn = get_db()
        try:
            all_etfs = conn.execute(
                "SELECT code, name, issuer, asset_class, exchange, inception_date, fund_type FROM etfs"
            ).fetchall()

            # Recent / oldest — sort lexicographically (ISO dates sort correctly)
            dated = [r for r in all_etfs if r['inception_date']]
            dated_sorted = sorted(dated, key=lambda r: str(r['inception_date']))

            # Build per-ETF list with year for client-side slicing
            etf_list = []
            for r in all_etfs:
                yr = year_from(r['inception_date'])
                if yr:
                    etf_list.append({
                        'code': r['code'],
                        'year': yr,
                        'exchange': r['exchange'] or 'Unknown',
                        'fund_type': r['fund_type'] or 'ETF',
                        'issuer': r['issuer'] or 'Other',
                        'asset_class': r['asset_class'] or 'Other',
                    })

            # Pre-aggregate totals for sidebar bars
            by_issuer = conn.execute(
                "SELECT issuer, COUNT(*) AS count FROM etfs WHERE issuer IS NOT NULL "
                "GROUP BY issuer ORDER BY count DESC"
            ).fetchall()
            by_ac = conn.execute(
                "SELECT asset_class, COUNT(*) AS count FROM etfs WHERE asset_class IS NOT NULL "
                "GROUP BY asset_class ORDER BY count DESC"
            ).fetchall()
            by_ex = conn.execute(
                "SELECT exchange, COUNT(*) AS count FROM etfs GROUP BY exchange ORDER BY count DESC"
            ).fetchall()
            by_ft = conn.execute(
                "SELECT COALESCE(fund_type,'ETF') AS fund_type, COUNT(*) AS count FROM etfs "
                "GROUP BY COALESCE(fund_type,'ETF') ORDER BY count DESC"
            ).fetchall()

            # Unique dimension values for filter dropdowns
            issuers = sorted({r['issuer'] for r in all_etfs if r['issuer']})
            exchanges = sorted({r['exchange'] for r in all_etfs if r['exchange']})
            fund_types = sorted({r['fund_type'] for r in all_etfs if r['fund_type']})
            asset_classes = sorted({r['asset_class'] for r in all_etfs if r['asset_class']})

            self.send_json({
                'total': len(all_etfs),
                'recent': [dict(r) for r in reversed(dated_sorted[-30:])],
                'oldest': [dict(r) for r in dated_sorted[:30]],
                'etf_list': etf_list,
                'by_issuer':      [dict(r) for r in by_issuer],
                'by_asset_class': [dict(r) for r in by_ac],
                'by_exchange':    [dict(r) for r in by_ex],
                'by_fund_type':   [dict(r) for r in by_ft],
                'filter_options': {
                    'issuers': issuers,
                    'exchanges': exchanges,
                    'fund_types': fund_types,
                    'asset_classes': asset_classes,
                },
            })
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Insights: returns analysis -----
    def handle_insights_returns(self):
        conn = get_db()
        try:
            avg = conn.execute(
                "SELECT AVG(return_1y) FROM etfs WHERE return_1y IS NOT NULL"
            ).fetchone()[0]

            top = conn.execute(
                "SELECT code, name, issuer, asset_class, return_1y, return_3y, return_5y, "
                "fund_size_aud_millions FROM etfs WHERE return_1y IS NOT NULL "
                "ORDER BY return_1y DESC LIMIT 30"
            ).fetchall()
            bottom = conn.execute(
                "SELECT code, name, issuer, asset_class, return_1y, return_3y, return_5y, "
                "fund_size_aud_millions FROM etfs WHERE return_1y IS NOT NULL "
                "ORDER BY return_1y ASC LIMIT 20"
            ).fetchall()
            by_ac = conn.execute(
                "SELECT asset_class, AVG(return_1y) AS avg_1y, AVG(return_3y) AS avg_3y, "
                "AVG(return_5y) AS avg_5y, COUNT(*) AS etf_count "
                "FROM etfs WHERE asset_class IS NOT NULL AND return_1y IS NOT NULL "
                "GROUP BY asset_class ORDER BY avg_1y DESC"
            ).fetchall()
            by_issuer = conn.execute(
                "SELECT issuer, AVG(return_1y) AS avg_1y, COUNT(*) AS etf_count "
                "FROM etfs WHERE issuer IS NOT NULL AND return_1y IS NOT NULL "
                "GROUP BY issuer ORDER BY avg_1y DESC"
            ).fetchall()

            buckets = ['<-40%','-40 to -20%','-20 to -10%','-10 to 0%',
                       '0 to 5%','5 to 10%','10 to 20%','20 to 30%','30 to 50%','>50%']
            counts = {b: 0 for b in buckets}
            for row in conn.execute("SELECT return_1y FROM etfs WHERE return_1y IS NOT NULL"):
                v = row[0]
                if   v < -40: counts['<-40%']        += 1
                elif v < -20: counts['-40 to -20%']  += 1
                elif v < -10: counts['-20 to -10%']  += 1
                elif v <   0: counts['-10 to 0%']    += 1
                elif v <   5: counts['0 to 5%']      += 1
                elif v <  10: counts['5 to 10%']     += 1
                elif v <  20: counts['10 to 20%']    += 1
                elif v <  30: counts['20 to 30%']    += 1
                elif v <  50: counts['30 to 50%']    += 1
                else:         counts['>50%']         += 1

            self.send_json({
                'avg_1y':          round(avg, 2) if avg is not None else None,
                'top_performers':  [dict(r) for r in top],
                'bottom_performers': [dict(r) for r in bottom],
                'by_asset_class':  [dict(r) for r in by_ac],
                'by_issuer':       [dict(r) for r in by_issuer],
                'distribution':    [{'bucket': b, 'count': counts[b]} for b in buckets],
            })
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Insights: expense / cost analysis -----
    def handle_insights_expense(self):
        # Use COALESCE(expense_ratio, management_fee) as effective MER.
        # Many issuers (iShares, DFA, StateStreet) only populate management_fee;
        # using expense_ratio alone produces misleadingly low averages for them.
        MER = "COALESCE(expense_ratio, management_fee)"
        conn = get_db()
        try:
            avg = conn.execute(
                f"SELECT AVG({MER}) FROM etfs WHERE {MER} > 0"
            ).fetchone()[0]

            # FUM-weighted average MER across full market
            fum_weighted_row = conn.execute(
                f"SELECT SUM(fund_size_aud_millions * {MER}) / "
                f"       SUM(CASE WHEN {MER} > 0 THEN fund_size_aud_millions END) "
                f"FROM etfs WHERE {MER} > 0 AND fund_size_aud_millions > 0"
            ).fetchone()[0]

            cheapest = conn.execute(
                f"SELECT code, name, issuer, asset_class, {MER} AS effective_mer, "
                f"expense_ratio, management_fee, fund_size_aud_millions "
                f"FROM etfs WHERE {MER} > 0 ORDER BY effective_mer ASC LIMIT 25"
            ).fetchall()
            priciest = conn.execute(
                f"SELECT code, name, issuer, asset_class, {MER} AS effective_mer, "
                f"expense_ratio, management_fee, fund_size_aud_millions "
                f"FROM etfs WHERE {MER} IS NOT NULL ORDER BY effective_mer DESC LIMIT 25"
            ).fetchall()

            by_ac = conn.execute(
                f"SELECT asset_class, AVG({MER}) AS avg_mer, MIN({MER}) AS min_mer, "
                f"MAX({MER}) AS max_mer, COUNT(*) AS etf_count, "
                f"SUM(fund_size_aud_millions * {MER}) / "
                f"  SUM(CASE WHEN {MER} > 0 THEN fund_size_aud_millions END) AS fum_weighted_mer "
                f"FROM etfs WHERE asset_class IS NOT NULL AND {MER} > 0 "
                f"GROUP BY asset_class ORDER BY avg_mer ASC"
            ).fetchall()

            by_issuer = conn.execute(
                f"SELECT issuer, AVG({MER}) AS avg_mer, MIN({MER}) AS min_mer, "
                f"COUNT(*) AS etf_count, "
                f"SUM(fund_size_aud_millions * {MER}) / "
                f"  SUM(CASE WHEN {MER} > 0 THEN fund_size_aud_millions END) AS fum_weighted_mer, "
                f"SUM(fund_size_aud_millions) AS total_fum "
                f"FROM etfs WHERE issuer IS NOT NULL AND {MER} > 0 "
                f"GROUP BY issuer ORDER BY avg_mer ASC"
            ).fetchall()

            buckets = ['0.00-0.10%','0.10-0.20%','0.20-0.30%','0.30-0.40%',
                       '0.40-0.50%','0.50-0.75%','0.75-1.00%','>1.00%']
            counts = {b: 0 for b in buckets}
            for row in conn.execute(f"SELECT {MER} FROM etfs WHERE {MER} > 0"):
                v = row[0]
                if   v < 0.10: counts['0.00-0.10%'] += 1
                elif v < 0.20: counts['0.10-0.20%'] += 1
                elif v < 0.30: counts['0.20-0.30%'] += 1
                elif v < 0.40: counts['0.30-0.40%'] += 1
                elif v < 0.50: counts['0.40-0.50%'] += 1
                elif v < 0.75: counts['0.50-0.75%'] += 1
                elif v < 1.00: counts['0.75-1.00%'] += 1
                else:          counts['>1.00%']     += 1

            self.send_json({
                'avg_mer':          round(avg, 3) if avg is not None else None,
                'fum_weighted_mer': round(fum_weighted_row, 3) if fum_weighted_row else None,
                'cheapest':         [dict(r) for r in cheapest],
                'most_expensive':   [dict(r) for r in priciest],
                'by_asset_class':   [dict(r) for r in by_ac],
                'by_issuer':        [dict(r) for r in by_issuer],
                'distribution':     [{'bucket': b, 'count': counts[b]} for b in buckets],
            })
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Insights: issuer analysis -----
    def handle_insights_issuers(self):
        conn = get_db()
        try:
            total_mkt = conn.execute(
                "SELECT COALESCE(SUM(fund_size_aud_millions),0) FROM etfs"
            ).fetchone()[0] or 1

            issuers = conn.execute(
                "SELECT issuer, COUNT(*) AS etf_count, "
                "COALESCE(SUM(fund_size_aud_millions),0) AS total_fum, "
                "AVG(CASE WHEN COALESCE(expense_ratio, management_fee) > 0 "
                "         THEN COALESCE(expense_ratio, management_fee) END) AS avg_mer, "
                "AVG(return_1y) AS avg_return_1y, AVG(return_3y) AS avg_return_3y "
                "FROM etfs WHERE issuer IS NOT NULL GROUP BY issuer ORDER BY total_fum DESC"
            ).fetchall()

            result = []
            for row in issuers:
                iss = row['issuer']
                ac_rows = conn.execute(
                    "SELECT asset_class, COUNT(*) AS cnt FROM etfs "
                    "WHERE issuer=? AND asset_class IS NOT NULL GROUP BY asset_class ORDER BY cnt DESC",
                    (iss,)
                ).fetchall()
                top_etf = conn.execute(
                    "SELECT code, name, fund_size_aud_millions FROM etfs "
                    "WHERE issuer=? ORDER BY fund_size_aud_millions DESC NULLS LAST LIMIT 1",
                    (iss,)
                ).fetchone()
                d = dict(row)
                d['market_share_pct'] = round(d['total_fum'] / total_mkt * 100, 2)
                d['asset_classes'] = {r['asset_class']: r['cnt'] for r in ac_rows}
                d['top_etf'] = dict(top_etf) if top_etf else None
                if d['avg_mer']:       d['avg_mer']       = round(d['avg_mer'], 3)
                if d['avg_return_1y']: d['avg_return_1y'] = round(d['avg_return_1y'], 2)
                if d['avg_return_3y']: d['avg_return_3y'] = round(d['avg_return_3y'], 2)
                result.append(d)

            self.send_json({'issuers': result, 'total_market_fum': round(total_mkt, 2)})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Insights: NAV premium/discount -----
    def handle_insights_nav(self):
        conn = get_db()
        try:
            from datetime import date
            today = date.today().isoformat()

            # Latest nav_history date with meaningful coverage
            latest_date = conn.execute(
                "SELECT date FROM nav_history GROUP BY date ORDER BY COUNT(*) DESC, date DESC LIMIT 1"
            ).fetchone()
            snap_date = latest_date['date'] if latest_date else today

            # Full snapshot for that date joined to etfs
            snapshot = conn.execute("""
                SELECT n.etf_code AS code, e.name, e.issuer, e.asset_class,
                       e.fund_size_aud_millions AS fum, e.fund_type,
                       n.nav, n.close_price, n.premium_discount_pct, n.source
                FROM nav_history n
                JOIN etfs e ON e.code = n.etf_code
                WHERE n.date = ?
                  AND n.premium_discount_pct IS NOT NULL
                ORDER BY n.premium_discount_pct DESC
            """, (snap_date,)).fetchall()

            rows = [dict(r) for r in snapshot]

            # Summary stats
            prems = [r['premium_discount_pct'] for r in rows if r['premium_discount_pct'] is not None]
            at_premium  = sum(1 for v in prems if v > 0.05)
            at_discount = sum(1 for v in prems if v < -0.05)
            near_par    = sum(1 for v in prems if -0.05 <= v <= 0.05)
            avg_pd      = round(sum(prems) / len(prems), 4) if prems else None
            max_prem    = max(prems) if prems else None
            max_disc    = min(prems) if prems else None

            # By asset class
            by_ac = conn.execute("""
                SELECT e.asset_class,
                       AVG(n.premium_discount_pct) AS avg_pd,
                       MIN(n.premium_discount_pct) AS min_pd,
                       MAX(n.premium_discount_pct) AS max_pd,
                       COUNT(*) AS count
                FROM nav_history n JOIN etfs e ON e.code = n.etf_code
                WHERE n.date = ? AND n.premium_discount_pct IS NOT NULL
                  AND e.asset_class IS NOT NULL
                GROUP BY e.asset_class ORDER BY avg_pd DESC
            """, (snap_date,)).fetchall()

            # By issuer
            by_issuer = conn.execute("""
                SELECT e.issuer,
                       AVG(n.premium_discount_pct) AS avg_pd,
                       MIN(n.premium_discount_pct) AS min_pd,
                       MAX(n.premium_discount_pct) AS max_pd,
                       COUNT(*) AS count
                FROM nav_history n JOIN etfs e ON e.code = n.etf_code
                WHERE n.date = ? AND n.premium_discount_pct IS NOT NULL
                  AND e.issuer IS NOT NULL
                GROUP BY e.issuer ORDER BY count DESC
            """, (snap_date,)).fetchall()

            # Historical avg premium/discount across all ETFs (last 90 days)
            hist = conn.execute("""
                SELECT date,
                       AVG(premium_discount_pct) AS avg_pd,
                       MIN(premium_discount_pct) AS min_pd,
                       MAX(premium_discount_pct) AS max_pd,
                       COUNT(*) AS etf_count
                FROM nav_history
                WHERE premium_discount_pct IS NOT NULL
                  AND date >= DATE(?, '-90 days')
                GROUP BY date ORDER BY date ASC
            """, (snap_date,)).fetchall()

            # ETFs with most historical data (for the history explorer)
            has_history = conn.execute("""
                SELECT n.etf_code AS code, e.name, e.issuer, COUNT(*) AS days
                FROM nav_history n JOIN etfs e ON e.code = n.etf_code
                WHERE n.premium_discount_pct IS NOT NULL
                GROUP BY n.etf_code HAVING days > 30
                ORDER BY days DESC LIMIT 60
            """).fetchall()

            self.send_json({
                'snapshot_date': snap_date,
                'summary': {
                    'total': len(prems),
                    'at_premium': at_premium,
                    'at_discount': at_discount,
                    'near_par': near_par,
                    'avg_pd': avg_pd,
                    'max_premium': round(max_prem, 4) if max_prem is not None else None,
                    'max_discount': round(max_disc, 4) if max_disc is not None else None,
                },
                'snapshot': rows,
                'by_asset_class': [dict(r) for r in by_ac],
                'by_issuer': [dict(r) for r in by_issuer],
                'market_history': [dict(r) for r in hist],
                'etfs_with_history': [dict(r) for r in has_history],
            })
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ================================================================== DB SYNC
    def handle_sync_db(self):
        """
        POST /admin/sync-db
        Accepts a gzipped SQLite database upload and atomically replaces the
        live database. Protected by SYNC_TOKEN env var.
        """
        import gzip, tempfile, shutil
        from scrapers.config import DB_PATH

        length = int(self.headers.get('Content-Length', 0))

        expected = os.getenv('SYNC_TOKEN', '')
        auth = self.headers.get('Authorization', '')

        # Always drain the request body before sending any error response —
        # closing the connection mid-upload causes Varnish to return 503.
        if length > 0 and (not expected or auth != f'Bearer {expected}'):
            self.rfile.read(length)

        if not expected:
            self.send_json({'error': 'SYNC_TOKEN not configured on server'}, 500)
            return

        if auth != f'Bearer {expected}':
            self.send_json({'error': 'Unauthorized'}, 401)
            return

        if length == 0:
            self.send_json({'error': 'Empty body'}, 400)
            return

        try:
            compressed = self.rfile.read(length)
            data = gzip.decompress(compressed)

            # Write to a temp file first, then atomically replace
            db_dir = os.path.dirname(DB_PATH)
            os.makedirs(db_dir, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=db_dir, delete=False, suffix='.tmp') as f:
                f.write(data)
                tmp_path = f.name

            shutil.move(tmp_path, DB_PATH)

            size_mb = len(data) / 1_048_576
            self.send_json({'ok': True, 'size_mb': round(size_mb, 2), 'path': DB_PATH})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)

    # ================================================================== DASHBOARD
    def handle_dashboard(self):
        from articles import get_all_articles
        _CAT_COLORS = {
            'Performance':    ('bg-green-100',  'text-green-800'),
            'Market Trends':  ('bg-blue-100',   'text-blue-800'),
            'Thematic':       ('bg-purple-100', 'text-purple-800'),
            'Research':       ('bg-amber-100',  'text-amber-800'),
            'Education':      ('bg-teal-100',   'text-teal-800'),
            'Issuer Profile': ('bg-orange-100', 'text-orange-800'),
        }
        cards = ''
        for a in get_all_articles():
            bg, fg = _CAT_COLORS.get(a['category'], ('bg-gray-100', 'text-gray-800'))
            cards += (
                f'<a href="/articles/{a["slug"]}" '
                f'class="flex-shrink-0 w-64 snap-start bg-white rounded-xl border border-gray-100 '
                f'shadow-sm hover:shadow-md transition-shadow p-4 block">'
                f'<span class="inline-block {bg} {fg} text-xs font-semibold px-2 py-0.5 rounded mb-2">'
                f'{a["category"]}</span>'
                f'<p class="text-sm font-bold text-gray-900 leading-snug mb-1 line-clamp-2">{a["title"]}</p>'
                f'<p class="text-xs text-gray-500 line-clamp-2">{a["subtitle"]}</p>'
                f'<p class="text-xs text-gray-400 mt-2">{a["date"]}</p>'
                f'</a>'
            )
        html = DASHBOARD_HTML.replace('<!--ARTICLE_CAROUSEL-->', cards)
        self.send_html(html)

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
<script src="https://cdn.jsdelivr.net/npm/luxon@3.4.4/build/global/luxon.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon@1.3.1/dist/chartjs-adapter-luxon.umd.min.js"></script>
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

  /* Main view tabs */
  .main-tab { color: #6b7280; padding: 10px 0; font-weight: 500; transition: color .15s;
              border-bottom: 2px solid transparent; white-space: nowrap; }
  .main-tab:hover { color: #2563eb; }
  .main-tab.active { color: #2563eb; border-bottom-color: #2563eb; }

  /* Screener */
  .sc-check-list { max-height: 130px; overflow-y: auto; }
  .sc-check-list label { display: flex; align-items: center; gap: 6px; padding: 3px 0;
                         cursor: pointer; font-size: .8125rem; color: #374151; }
  .sc-check-list label:hover { color: #2563eb; }

  /* Compare mini-bars */
  .cmp-bar-wrap { display: flex; align-items: center; gap: 4px; }
  .cmp-bar { height: 6px; border-radius: 3px; background: #3b82f6; min-width: 2px; }
  .cmp-bar-neg { background: #ef4444; }

  /* Screener table rows */
  #screener-table tr { border-bottom: 1px solid #f1f5f9; transition: background .1s; }
  #screener-table tr:hover { background: #eff6ff; }

  /* Holdings table */
  #holdings-table tr { border-bottom: 1px solid #f1f5f9; }
  #holdings-table tr:hover { background: #eff6ff; }

  /* Data freshness tooltip trigger */
  .dated { cursor: help; }
  .dated:hover { border-bottom: 1px dotted #94a3b8; }
</style>
</head>
<body class="bg-slate-100 min-h-screen text-sm text-gray-800 antialiased">

<!-- ── Header ── -->
<header class="bg-gradient-to-r from-green-900 to-green-700 shadow-xl">
  <div class="max-w-[1400px] mx-auto px-5 py-3 flex flex-wrap items-center gap-4">
    <div class="flex-1 min-w-[180px]">
      <h1 class="text-lg font-bold text-white tracking-tight leading-tight">
        ☘️ Australian ETF Dashboard
      </h1>
      <p id="subtitle" class="text-green-300 text-xs mt-0.5">Loading market data…</p>
    </div>

    <!-- Search -->
    <div class="relative w-80">
      <svg class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-green-300 pointer-events-none"
           fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
              d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/>
      </svg>
      <input id="search" type="text" placeholder="Search code or name…"
             autocomplete="off"
             class="w-full bg-white/10 border border-white/20 text-white placeholder-green-300
                    rounded-xl pl-9 pr-3 py-2 text-sm outline-none
                    focus:ring-2 focus:ring-white/40 focus:bg-white/20">
      <div id="search-results"
           class="hidden bg-white border border-gray-200 rounded-xl shadow-2xl max-h-72 overflow-y-auto"></div>
    </div>

    <!-- Live indicator -->
    <div class="flex items-center gap-2 text-xs text-green-300">
      <span class="w-2 h-2 bg-green-400 rounded-full animate-pulse"></span>
      <span id="last-refresh">Live</span>
    </div>
  </div>
</header>

<main class="max-w-[1400px] mx-auto px-5 py-5">

  <!-- ── Stat cards ── -->
  <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-7 gap-3 mb-5">
    <div class="stat-card bg-white rounded-xl shadow-sm border border-gray-100 p-4 cursor-pointer" onclick="goCard('fum')" title="View FUM breakdown by asset class">
      <p class="text-gray-400 text-xs font-semibold uppercase tracking-wider">Total FUM</p>
      <p id="c-fum" class="text-2xl font-bold text-gray-900 mt-1 leading-tight">—</p>
      <p class="text-gray-400 text-xs mt-1 flex items-center justify-between">AUD <span class="text-green-300">›</span></p>
    </div>
    <div class="stat-card bg-white rounded-xl shadow-sm border border-gray-100 p-4 cursor-pointer" onclick="goCard('count')" title="Browse all ETFs sorted by size">
      <p class="text-gray-400 text-xs font-semibold uppercase tracking-wider">ETFs Listed</p>
      <p id="c-count" class="text-2xl font-bold text-gray-900 mt-1 leading-tight">—</p>
      <p class="text-gray-400 text-xs mt-1 flex items-center justify-between">all exchanges <span class="text-green-300">›</span></p>
    </div>
    <div class="stat-card bg-white rounded-xl shadow-sm border border-gray-100 p-4 cursor-pointer" onclick="goCard('return')" title="See top performing ETFs">
      <p class="text-gray-400 text-xs font-semibold uppercase tracking-wider">Avg 1Y Return</p>
      <p id="c-ret" class="text-2xl font-bold mt-1 leading-tight">—</p>
      <p class="text-gray-400 text-xs mt-1 flex items-center justify-between">market average <span class="text-green-300">›</span></p>
    </div>
    <div class="stat-card bg-white rounded-xl shadow-sm border border-gray-100 p-4 cursor-pointer" onclick="goCard('expense')" title="See lowest cost ETFs">
      <p class="text-gray-400 text-xs font-semibold uppercase tracking-wider">Avg Expense</p>
      <p id="c-exp" class="text-2xl font-bold text-gray-900 mt-1 leading-tight">—</p>
      <p class="text-gray-400 text-xs mt-1 flex items-center justify-between">management fee <span class="text-green-300">›</span></p>
    </div>
    <div class="stat-card bg-white rounded-xl shadow-sm border border-gray-100 p-4 cursor-pointer" onclick="goCard('top')" title="Open top performer detail">
      <p class="text-gray-400 text-xs font-semibold uppercase tracking-wider">Top Performer</p>
      <p id="c-top" class="text-2xl font-bold text-green-600 mt-1 leading-tight">—</p>
      <p class="text-gray-400 text-xs mt-1 flex items-center justify-between">best 1Y return <span class="text-green-300">›</span></p>
    </div>
    <div class="stat-card bg-white rounded-xl shadow-sm border border-gray-100 p-4 cursor-pointer" onclick="goCard('issuers')" title="View issuer market share">
      <p class="text-gray-400 text-xs font-semibold uppercase tracking-wider">Issuers</p>
      <p id="c-issuers" class="text-2xl font-bold text-gray-900 mt-1 leading-tight">—</p>
      <p class="text-gray-400 text-xs mt-1 flex items-center justify-between">fund managers <span class="text-green-300">›</span></p>
    </div>
    <div class="stat-card bg-white rounded-xl shadow-sm border border-gray-100 p-4 cursor-pointer" onclick="goCard('nav')" title="Premium / Discount to NAV analysis">
      <p class="text-gray-400 text-xs font-semibold uppercase tracking-wider">Prem / Disc</p>
      <p id="c-nav" class="text-2xl font-bold text-gray-900 mt-1 leading-tight">—</p>
      <p class="text-gray-400 text-xs mt-1 flex items-center justify-between">vs NAV <span class="text-green-300">›</span></p>
    </div>
  </div>

  <!-- ── Articles carousel ── -->
  <div class="mb-5">
    <div class="flex items-center justify-between mb-2 px-0.5">
      <span class="text-xs font-semibold text-gray-500 uppercase tracking-wider">Latest Articles</span>
      <a href="/articles" class="text-xs text-green-500 hover:text-green-700 font-medium">View all →</a>
    </div>
    <div class="relative">
      <div id="article-carousel" class="flex gap-3 overflow-x-auto snap-x snap-mandatory scroll-smooth pb-1"
           style="scrollbar-width:none;-ms-overflow-style:none;">
        <!--ARTICLE_CAROUSEL-->
      </div>
      <button onclick="carouselScroll(-1)"
              class="hidden lg:flex absolute left-0 top-1/2 -translate-y-1/2 -translate-x-4
                     w-8 h-8 items-center justify-center rounded-full bg-white shadow border
                     border-gray-200 text-gray-500 hover:text-green-600 z-10">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="15 18 9 12 15 6"/></svg>
      </button>
      <button onclick="carouselScroll(1)"
              class="hidden lg:flex absolute right-0 top-1/2 -translate-y-1/2 translate-x-4
                     w-8 h-8 items-center justify-center rounded-full bg-white shadow border
                     border-gray-200 text-gray-500 hover:text-green-600 z-10">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="9 18 15 12 9 6"/></svg>
      </button>
    </div>
  </div>

  <!-- ── Main nav tabs ── -->
  <div class="bg-white rounded-xl shadow-sm border border-gray-100 mb-5 px-5">
    <div class="flex gap-8 text-sm overflow-x-auto">
      <button class="main-tab active" data-view="list">ETF List</button>
      <button class="main-tab" data-view="screener">Screener</button>
      <button class="main-tab" data-view="compare">Compare</button>
      <button class="main-tab" data-view="holdings">Holdings Search</button>
      <button class="main-tab" data-view="analytics">Analytics</button>
      <a href="/articles" class="main-tab flex items-center gap-1" style="text-decoration:none">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10 9 9 9 8 9"/></svg>
        Articles
      </a>
      <a href="/screener/preferences" class="main-tab flex items-center gap-1" style="text-decoration:none">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14"/><path d="M4.93 4.93a10 10 0 0 0 0 14.14"/></svg>
        Portfolio Builder
      </a>
    </div>
  </div>

  <!-- ══════════════════════════════ VIEW: ETF List ══════════════════════════════ -->
  <div id="view-list">

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
                           focus:ring-2 focus:ring-green-200 focus:border-green-400 outline-none">
              <option value="">All Exchanges</option>
            </select>
          </div>
          <div>
            <label class="block text-xs text-gray-500 mb-1">Issuer</label>
            <select id="f-issuer"
                    class="w-full border border-gray-200 rounded-lg px-2.5 py-2 text-sm bg-gray-50
                           focus:ring-2 focus:ring-green-200 focus:border-green-400 outline-none">
              <option value="">All Issuers</option>
            </select>
          </div>
          <div>
            <label class="block text-xs text-gray-500 mb-1">Asset Class</label>
            <select id="f-asset"
                    class="w-full border border-gray-200 rounded-lg px-2.5 py-2 text-sm bg-gray-50
                           focus:ring-2 focus:ring-green-200 focus:border-green-400 outline-none">
              <option value="">All Asset Classes</option>
            </select>
          </div>
          <div>
            <label class="block text-xs text-gray-500 mb-1">Management Style</label>
            <select id="f-type"
                    class="w-full border border-gray-200 rounded-lg px-2.5 py-2 text-sm bg-gray-50
                           focus:ring-2 focus:ring-green-200 focus:border-green-400 outline-none">
              <option value="">All Types</option>
              <option value="ETF">Passive / Index</option>
              <option value="Active">Active</option>
              <option value="Complex">Active (Complex)</option>
              <option value="SP">Physical / Structured</option>
              <option value="Index">Index Accumulation</option>
            </select>
          </div>
          <div>
            <label class="block text-xs text-gray-500 mb-1">Benchmark / Index</label>
            <input id="f-benchmark" type="text" placeholder="e.g. MSCI, S&amp;P/ASX…"
                   class="w-full border border-gray-200 rounded-lg px-2.5 py-2 text-sm bg-gray-50
                          focus:ring-2 focus:ring-green-200 focus:border-green-400 outline-none"/>
          </div>
          <div>
            <label class="block text-xs text-gray-500 mb-1">Sort By</label>
            <select id="f-sort"
                    class="w-full border border-gray-200 rounded-lg px-2.5 py-2 text-sm bg-gray-50
                           focus:ring-2 focus:ring-green-200 focus:border-green-400 outline-none">
              <option value="rank">Rank by FUM</option>
              <option value="fum">FUM (largest)</option>
              <option value="return_1y">1Y Return</option>
              <option value="yield">Distribution Yield</option>
              <option value="expense">Expense Ratio</option>
              <option value="price">Price</option>
              <option value="code">Code A–Z</option>
            </select>
          </div>
          <div>
            <label class="block text-xs text-gray-500 mb-1">Extra Columns</label>
            <div class="space-y-1">
              <label class="flex items-center gap-2 cursor-pointer text-sm text-gray-700">
                <input id="col-3y" type="checkbox" class="accent-green-600">
                3Y Return
              </label>
              <label class="flex items-center gap-2 cursor-pointer text-sm text-gray-700">
                <input id="col-5y" type="checkbox" class="accent-green-600">
                5Y Return
              </label>
            </div>
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
            <thead id="etf-thead" class="bg-gray-50 text-xs font-semibold text-gray-500 uppercase tracking-wide"
                   style="position:sticky;top:0;z-index:5">
              <tr>
                <th class="px-3 py-2.5 text-left w-8 cursor-pointer select-none hover:text-gray-700" data-sort="rank">#</th>
                <th class="px-3 py-2.5 text-left cursor-pointer select-none hover:text-gray-700" style="min-width:260px" data-sort="code">ETF</th>
                <th class="px-3 py-2.5 text-left select-none text-gray-400" style="width:80px;max-width:80px">Class</th>
                <th class="px-3 py-2.5 text-right cursor-pointer select-none hover:text-gray-700" data-sort="price">Price</th>
                <th class="px-3 py-2.5 text-right cursor-pointer select-none hover:text-gray-700" data-sort="fum">FUM</th>
                <th class="px-3 py-2.5 text-right cursor-pointer select-none hover:text-gray-700" data-sort="return_1y">1Y Rtn</th>
                <th class="px-3 py-2.5 text-right cursor-pointer select-none hover:text-gray-700" data-sort="yield">Yield</th>
                <th class="px-3 py-2.5 text-right cursor-pointer select-none hover:text-gray-700 col-3y hidden" data-sort="return_3y">3Y Rtn</th>
                <th class="px-3 py-2.5 text-right cursor-pointer select-none hover:text-gray-700 col-5y hidden" data-sort="return_5y">5Y Rtn</th>
                <th class="px-3 py-2.5 text-right cursor-pointer select-none hover:text-gray-700" data-sort="expense">MER</th>
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
          <span id="d-code" class="text-2xl font-bold text-green-700"></span>
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
        <button class="dtab" data-tab="performance">Performance</button>
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
    <!-- Issuer market share pie chart -->
    <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
      <h3 class="font-semibold text-gray-700 text-sm mb-3">Issuer Market Share</h3>
      <div class="relative" style="height:220px">
        <canvas id="chart-issuers"></canvas>
      </div>
    </div>
    <!-- Fund flows bar chart -->
    <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
      <h3 class="font-semibold text-gray-700 text-sm mb-3">Fund Flows — Top 1M</h3>
      <div class="relative" style="height:220px">
        <canvas id="chart-flows"></canvas>
      </div>
    </div>
  </div>

  </div><!-- /view-list -->

  <!-- ══════════════════════════════ VIEW: Screener ══════════════════════════════ -->
  <div id="view-screener" class="hidden">
    <div class="grid grid-cols-1 lg:grid-cols-4 gap-4">

      <!-- Screener filters sidebar -->
      <aside class="lg:col-span-1">
        <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4 sticky top-4 space-y-4">
          <div class="flex items-center justify-between">
            <h3 class="text-xs font-semibold text-gray-500 uppercase tracking-wider">Screener Filters</h3>
            <button id="sc-clear"
                    class="text-xs text-green-600 hover:underline">Clear All</button>
          </div>

          <!-- Exchange toggles -->
          <div>
            <p class="text-xs text-gray-500 mb-1.5">Exchange</p>
            <div class="flex gap-1.5">
              <button class="sc-exch active flex-1 py-1 rounded-lg border text-xs font-medium
                             bg-green-600 text-white border-green-600" data-exch="">All</button>
              <button class="sc-exch flex-1 py-1 rounded-lg border text-xs font-medium
                             text-gray-600 border-gray-200 hover:border-green-400" data-exch="ASX">ASX</button>
              <button class="sc-exch flex-1 py-1 rounded-lg border text-xs font-medium
                             text-gray-600 border-gray-200 hover:border-green-400" data-exch="CXA">CXA</button>
            </div>
          </div>

          <!-- Asset class checklist -->
          <div>
            <p class="text-xs text-gray-500 mb-1.5">Asset Class</p>
            <div id="sc-asset-list" class="sc-check-list space-y-0.5 border border-gray-100 rounded-lg p-2"></div>
          </div>

          <!-- Issuer checklist -->
          <div>
            <p class="text-xs text-gray-500 mb-1.5">Issuer</p>
            <div id="sc-issuer-list" class="sc-check-list space-y-0.5 border border-gray-100 rounded-lg p-2"></div>
          </div>

          <!-- Max fee slider -->
          <div>
            <p class="text-xs text-gray-500 mb-1.5">Max Management Fee: <span id="sc-fee-val" class="font-semibold text-gray-700">2.00%</span></p>
            <input id="sc-fee" type="range" min="0" max="2" step="0.05" value="2"
                   class="w-full accent-green-600">
          </div>

          <!-- Min FUM -->
          <div>
            <p class="text-xs text-gray-500 mb-1">Min Fund Size (AUD M)</p>
            <input id="sc-fum" type="number" min="0" placeholder="e.g. 100"
                   class="w-full border border-gray-200 rounded-lg px-2.5 py-1.5 text-sm bg-gray-50
                          focus:ring-2 focus:ring-green-200 focus:border-green-400 outline-none">
          </div>

          <!-- 1Y Return range -->
          <div>
            <p class="text-xs text-gray-500 mb-1">1Y Return (%)</p>
            <div class="flex gap-2">
              <input id="sc-ret-min" type="number" placeholder="Min"
                     class="w-full border border-gray-200 rounded-lg px-2 py-1.5 text-sm bg-gray-50
                            focus:ring-2 focus:ring-green-200 focus:border-green-400 outline-none">
              <input id="sc-ret-max" type="number" placeholder="Max"
                     class="w-full border border-gray-200 rounded-lg px-2 py-1.5 text-sm bg-gray-50
                            focus:ring-2 focus:ring-green-200 focus:border-green-400 outline-none">
            </div>
          </div>

          <!-- Min yield -->
          <div>
            <p class="text-xs text-gray-500 mb-1">Min Distribution Yield (%)</p>
            <input id="sc-yield" type="number" min="0" placeholder="e.g. 3"
                   class="w-full border border-gray-200 rounded-lg px-2.5 py-1.5 text-sm bg-gray-50
                          focus:ring-2 focus:ring-green-200 focus:border-green-400 outline-none">
          </div>

          <!-- FX hedged -->
          <label class="flex items-center gap-2 cursor-pointer text-sm text-gray-700">
            <input id="sc-hedged" type="checkbox" class="accent-green-600">
            FX Hedged only
          </label>

          <p id="sc-count" class="pt-2 border-t text-xs text-gray-400"></p>
        </div>
      </aside>

      <!-- Screener results -->
      <div class="lg:col-span-3">
        <div class="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
          <div class="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
            <h3 class="font-semibold text-gray-700">Screener Results</h3>
            <div class="flex items-center gap-3">
              <span id="sc-result-count" class="text-xs text-gray-400"></span>
              <button id="sc-export"
                      class="flex items-center gap-1 px-3 py-1.5 border border-gray-200 rounded-lg
                             text-xs text-gray-600 hover:border-green-400 hover:text-green-600 transition-colors">
                &#8595; CSV
              </button>
            </div>
          </div>
          <div class="overflow-x-auto" style="max-height:600px;overflow-y:auto">
            <table class="w-full">
              <thead class="bg-gray-50 text-xs font-semibold text-gray-500 uppercase tracking-wide"
                     style="position:sticky;top:0;z-index:5">
                <tr>
                  <th class="px-3 py-2.5 text-left">ETF</th>
                  <th class="px-3 py-2.5 text-left">Class</th>
                  <th class="px-3 py-2.5 text-left">Issuer</th>
                  <th class="px-3 py-2.5 text-right">Price</th>
                  <th class="px-3 py-2.5 text-right">FUM</th>
                  <th class="px-3 py-2.5 text-right">1Y Rtn</th>
                  <th class="px-3 py-2.5 text-right">Yield</th>
                  <th class="px-3 py-2.5 text-right">MER</th>
                </tr>
              </thead>
              <tbody id="screener-table" class="text-sm divide-y divide-gray-50"></tbody>
            </table>
          </div>
          <div id="sc-empty" class="hidden py-16 text-center text-gray-400 text-sm">No ETFs match the current filters.</div>
        </div>
      </div>
    </div>
  </div><!-- /view-screener -->

  <!-- ══════════════════════════════ VIEW: Compare ══════════════════════════════ -->
  <div id="view-compare" class="hidden">
    <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-5 mb-4">
      <h3 class="font-semibold text-gray-700 mb-3">Select ETFs to Compare <span class="text-gray-400 font-normal text-sm">(2–5)</span></h3>
      <div class="flex gap-3 items-start flex-wrap">
        <div class="relative w-72">
          <svg class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 pointer-events-none"
               fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                  d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/>
          </svg>
          <input id="cmp-search" type="text" placeholder="Search ETF code or name…"
                 autocomplete="off"
                 class="w-full border border-gray-200 rounded-xl pl-9 pr-3 py-2 text-sm bg-gray-50
                        focus:ring-2 focus:ring-green-200 focus:border-green-400 outline-none">
          <div id="cmp-dropdown"
               class="hidden absolute z-50 top-full mt-1 left-0 right-0 bg-white border border-gray-200
                      rounded-xl shadow-2xl max-h-60 overflow-y-auto"></div>
        </div>
        <div id="cmp-chips" class="flex flex-wrap gap-2 items-center"></div>
      </div>
    </div>

    <!-- Holdings overlap analysis -->
    <div id="cmp-overlap-wrap" class="mb-6 hidden">
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-5">
        <div class="flex items-center justify-between mb-1">
          <h3 class="font-semibold text-gray-700">Holdings Overlap</h3>
          <span id="cmp-overlap-badge" class="text-xs text-gray-400"></span>
        </div>
        <div id="cmp-overlap-coverage" class="flex flex-wrap gap-3 mb-4"></div>
        <div id="cmp-overlap-body"></div>
      </div>
    </div>

    <!-- Similar ETFs suggestions -->
    <div id="cmp-similar-wrap" class="mb-6">
      <h3 class="font-semibold text-gray-700 mb-3">Similar ETFs by Holdings Overlap</h3>
      <div id="cmp-similar-content">
        <p class="text-sm text-gray-400 bg-white rounded-xl shadow-sm border border-gray-100 py-8 text-center">
          Add an ETF above to see similar alternatives based on portfolio holdings.
        </p>
      </div>
    </div>

    <div id="cmp-table-wrap">
      <div id="cmp-hint" class="bg-white rounded-xl shadow-sm border border-gray-100 py-16
                                 text-center text-gray-400 text-sm">
        Add 2–5 ETFs above to compare them side by side.
      </div>
      <div id="cmp-table-container" class="hidden bg-white rounded-xl shadow-sm border border-gray-100 overflow-x-auto">
        <table id="cmp-table" class="w-full text-sm"></table>
      </div>
    </div>
  </div><!-- /view-compare -->

  <!-- ══════════════════════════════ VIEW: Holdings Search ══════════════════════ -->
  <div id="view-holdings" class="hidden">
    <!-- Search bar -->
    <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-5 mb-4">
      <h3 class="font-semibold text-gray-700 mb-3">Holdings Search</h3>
      <p class="text-sm text-gray-500 mb-3">Find which Australian ETFs hold a particular stock or asset.</p>
      <div class="flex gap-3 items-center">
        <div class="relative flex-1 max-w-lg">
          <svg class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 pointer-events-none"
               fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                  d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/>
          </svg>
          <input id="hs-input" type="text" placeholder="e.g. Apple, BHP, AAPL, NVDA…"
                 class="w-full border border-gray-200 rounded-xl pl-9 pr-3 py-2.5 text-sm bg-gray-50
                        focus:ring-2 focus:ring-green-200 focus:border-green-400 outline-none">
        </div>
        <div class="flex items-center gap-2 text-sm text-gray-500">
          <label class="text-xs">Min Weight %</label>
          <input id="hs-min-weight" type="number" min="0" step="0.1" value="0" placeholder="0"
                 class="w-20 border border-gray-200 rounded-lg px-2 py-2 text-sm bg-gray-50
                        focus:ring-2 focus:ring-green-200 focus:border-green-400 outline-none">
        </div>
      </div>
    </div>

    <!-- Summary + results -->
    <div id="hs-summary" class="hidden bg-green-50 border border-green-100 rounded-xl px-5 py-3 mb-4 text-sm text-green-800"></div>

    <div class="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
      <div id="hs-empty" class="py-16 text-center text-gray-400 text-sm">
        Enter a company name or ticker to search across all ETF holdings.
      </div>
      <div id="hs-table-wrap" class="hidden overflow-x-auto">
        <table class="w-full text-sm">
          <thead class="bg-gray-50 text-xs font-semibold text-gray-500 uppercase tracking-wide"
                 style="position:sticky;top:0;z-index:5">
            <tr>
              <th class="px-3 py-2.5 text-left">ETF</th>
              <th class="px-3 py-2.5 text-left">ETF Name</th>
              <th class="px-3 py-2.5 text-left">Holding</th>
              <th class="px-3 py-2.5 text-left">Ticker</th>
              <th class="px-3 py-2.5 text-right">Weight</th>
              <th class="px-3 py-2.5 text-left">Sector</th>
              <th class="px-3 py-2.5 text-left">Asset Class</th>
            </tr>
          </thead>
          <tbody id="holdings-table" class="divide-y divide-gray-50"></tbody>
        </table>
      </div>
    </div>
  </div><!-- /view-holdings -->

  <!-- ══════════════════════════════ VIEW: Analytics ══════════════════════════════ -->
  <div id="view-analytics" class="hidden">

    <!-- Leaderboard cards -->
    <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">

      <!-- Top Performers -->
      <div class="bg-white rounded-xl shadow-sm border border-gray-100">
        <div class="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
          <h3 class="font-semibold text-gray-700 text-sm">Top Performers</h3>
          <span class="text-xs text-gray-400 bg-gray-50 px-2 py-0.5 rounded-full">1Y Return</span>
        </div>
        <div id="an-performers" class="divide-y divide-gray-50">
          <div class="flex justify-center py-8"><div class="spinner"></div></div>
        </div>
      </div>

      <!-- Highest Yield -->
      <div class="bg-white rounded-xl shadow-sm border border-gray-100">
        <div class="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
          <h3 class="font-semibold text-gray-700 text-sm">Highest Yield</h3>
          <span class="text-xs text-gray-400 bg-gray-50 px-2 py-0.5 rounded-full">Distribution</span>
        </div>
        <div id="an-yield" class="divide-y divide-gray-50">
          <div class="flex justify-center py-8"><div class="spinner"></div></div>
        </div>
      </div>

      <!-- Lowest Cost -->
      <div class="bg-white rounded-xl shadow-sm border border-gray-100">
        <div class="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
          <h3 class="font-semibold text-gray-700 text-sm">Lowest Cost</h3>
          <span class="text-xs text-gray-400 bg-gray-50 px-2 py-0.5 rounded-full">MER</span>
        </div>
        <div id="an-cheapest" class="divide-y divide-gray-50">
          <div class="flex justify-center py-8"><div class="spinner"></div></div>
        </div>
      </div>

    </div>

    <!-- Fund Flows -->
    <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-5">
      <h3 class="font-semibold text-gray-700 text-sm mb-4">Fund Flows — Monthly (1M)</h3>
      <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div>
          <p class="text-xs font-semibold text-emerald-600 uppercase tracking-wide mb-2">Top Inflows</p>
          <div id="an-inflows" class="space-y-2">
            <div class="flex justify-center py-4"><div class="spinner"></div></div>
          </div>
        </div>
        <div>
          <p class="text-xs font-semibold text-red-500 uppercase tracking-wide mb-2">Top Outflows</p>
          <div id="an-outflows" class="space-y-2">
            <div class="flex justify-center py-4"><div class="spinner"></div></div>
          </div>
        </div>
      </div>
    </div>

  </div><!-- /view-analytics -->

</main>

<script>
/* ======================================================= state */
const API = '';
let page = 0, pageSize = 50, selectedCode = null;
let tableSortKey = 'rank', tableSortDir = 'asc';
let topPerformerCode = null;
let chartAsset = null, chartFlows = null, chartIssuers = null;

const PALETTE = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6',
                 '#ec4899','#06b6d4','#84cc16','#f97316','#6366f1'];

/* ─── Issuer brand identity ─── */
const ISSUER_COLORS = {
  'BetaShares':      '#FF6B35',
  'Vanguard':        '#8B1A1A',
  'iShares':         '#009CDE',
  'VanEck':          '#1A3C8F',
  'SPDR':            '#CC1122',
  'Global X':        '#00A651',
  'Magellan':        '#E8712A',
  'Dimensional':     '#005B8E',
  'JPMorgan':        '#003087',
  'Fidelity':        '#009A44',
  'Invesco':         '#1E5CAB',
  'Janus Henderson': '#E30613',
  'Perpetual':       '#6B2D8B',
  'Platinum':        '#888888',
  'Hyperion':        '#2D5F8A',
  'Monochrome':      '#111111',
  'Coolabah':        '#1C4F7C',
  'PIMCO':           '#00AEEF',
  'Schroders':       '#DB0011',
  'Lazard':          '#00448E',
  'Avantis':         '#004C97',
  'Australian Ethical': '#76B043',
};
const ISSUER_DOMAINS = {
  'BetaShares':      'betashares.com.au',
  'Vanguard':        'vanguard.com.au',
  'iShares':         'blackrock.com',
  'VanEck':          'vaneck.com.au',
  'SPDR':            'ssga.com',
  'Global X':        'globalxetfs.com.au',
  'Magellan':        'magellangroup.com.au',
  'Dimensional':     'dimensional.com',
  'JPMorgan':        'am.jpmorgan.com',
  'Fidelity':        'fidelity.com.au',
  'Invesco':         'invesco.com',
  'Janus Henderson': 'janushenderson.com',
  'Perpetual':       'perpetual.com.au',
  'Platinum':        'platinum.com.au',
  'Hyperion':        'hyperion.com.au',
  'Monochrome':      'monochrome.com.au',
  'Coolabah':        'coolabah.com.au',
  'PIMCO':           'pimco.com.au',
  'Schroders':       'schroders.com',
  'Lazard':          'lazardassetmanagement.com',
  'Avantis':         'au.avantis.com',
  'Australian Ethical': 'australianethical.com.au',
};
function issuerColor(name) {
  return ISSUER_COLORS[name] || '#94a3b8';
}
function issuerLogo(name) {
  const dom = ISSUER_DOMAINS[name];
  if (!dom) return null;
  return `https://www.google.com/s2/favicons?domain=${dom}&sz=32`;
}

/* Asset class colours — consistent across charts */
const ASSET_CLASS_COLORS = {
  'Australian Equities':    '#3b82f6',
  'International Equities': '#10b981',
  'Fixed Income':           '#f59e0b',
  'Diversified':            '#8b5cf6',
  'Property':               '#ec4899',
  'Cash':                   '#06b6d4',
  'Commodities':            '#f97316',
  'Infrastructure':         '#84cc16',
  'Thematic':               '#6366f1',
  'Digital Assets':         '#a855f7',
  'Alternatives':           '#64748b',
  'Currency':               '#14b8a6',
};
function assetColor(name) {
  return ASSET_CLASS_COLORS[name] || '#94a3b8';
}

/* ======================================================= formatters */
function fmtFum(v) {
  if (v == null) return '—';
  if (v >= 1e6) return '$' + (v / 1e6).toFixed(2) + 'T';
  if (v >= 1000) return '$' + (v / 1000).toFixed(1) + 'B';
  return '$' + Math.round(v) + 'M';
}
/* CHESS FUM: units on issue × last price (null when units unavailable) */
function chessFum(e) {
  if (e.units_on_issue && e.current_price)
    return e.units_on_issue * e.current_price / 1e6;
  return null;
}
/* Primary display FUM: CHESS preferred, fallback to ASX report total */
function calcFum(e) {
  return chessFum(e) ?? e.fund_size_aud_millions;
}
function fumTip(e) {
  return e.units_on_issue && e.current_price
    ? 'CHESS FUM = units on issue × last price · ' + (e.units_on_issue_date || 'ASX monthly report')
    : 'Total FUM · ASX Monthly Report · ' + fmtTs(tsFor('asx_report'));
}
/*
 * fumDisplay(e) — shows CHESS FUM as primary with total FUM below when both exist.
 * Used in list table and compare table.
 */
function fumDisplay(e) {
  const chess = chessFum(e);
  const total = e.fund_size_aud_millions;
  if (chess != null && total != null) {
    const chessTip = 'CHESS FUM = ' + (e.units_on_issue ? e.units_on_issue.toLocaleString() + ' units' : '') + ' × ' + (e.current_price ? '$' + e.current_price : 'last price');
    return `<div>
      <div class="font-medium text-gray-800 tabular-nums" title="${chessTip}">${fmtFum(chess)}</div>
      <div class="text-xs text-gray-400 tabular-nums" title="Total FUM · ASX Monthly Report">Total ${fmtFum(total)}</div>
    </div>`;
  }
  return `<span title="${fumTip(e)}">${fmtFum(chess ?? total)}</span>`;
}
function fmtUnits(v) {
  if (v == null) return '—';
  if (v >= 1e9) return (v / 1e9).toFixed(2) + 'B';
  if (v >= 1e6) return (v / 1e6).toFixed(1) + 'M';
  if (v >= 1e3) return (v / 1e3).toFixed(1) + 'K';
  return v.toLocaleString();
}
function fmtAud(v) {
  if (v == null) return '—';
  if (v >= 1e9) return '$' + (v / 1e9).toFixed(2) + 'B';
  if (v >= 1e6) return '$' + (v / 1e6).toFixed(1) + 'M';
  return '$' + Math.round(v).toLocaleString();
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
    topPerformerCode = m.top_performer.code;
  }
  if (m.avg_premium_discount != null) {
    const navEl = document.getElementById('c-nav');
    const v = m.avg_premium_discount;
    navEl.textContent = (v >= 0 ? '+' : '') + v.toFixed(2) + '%';
    navEl.className = 'text-2xl font-bold mt-1 leading-tight ' + (v >= 0 ? 'text-green-600' : 'text-red-500');
  }
  const now = new Date().toLocaleTimeString();
  document.getElementById('subtitle').textContent =
    `${(m.total_etfs || 0).toLocaleString()} ETFs · ${fmtFum(m.total_fum_millions)} FUM · ${now}`;
  document.getElementById('last-refresh').textContent = 'Live · ' + now;
}

/* ======================================================= stat card navigation */
function goCard(type) {
  switch (type) {
    case 'fum':      window.location.href = '/insights/fum';      break;
    case 'count':    window.location.href = '/insights/listings';  break;
    case 'return':   window.location.href = '/insights/returns';   break;
    case 'expense':  window.location.href = '/insights/expense';   break;
    case 'top':      window.location.href = '/insights/returns';   break;
    case 'issuers':  window.location.href = '/insights/issuers';   break;
    case 'nav':      window.location.href = '/insights/nav';       break;
  }
}

/* ======================================================= table */
async function loadTable() {
  const params = new URLSearchParams();
  const ex   = document.getElementById('f-exchange').value;
  const iss  = document.getElementById('f-issuer').value;
  const ac   = document.getElementById('f-asset').value;
  const ft   = document.getElementById('f-type').value;
  const bm   = document.getElementById('f-benchmark').value.trim();
  if (ex)  params.set('exchange',    ex);
  if (iss) params.set('issuer',      iss);
  if (ac)  params.set('asset_class', ac);
  if (ft)  params.set('fund_type',   ft);
  if (bm)  params.set('benchmark',   bm);
  params.set('sort_by',  tableSortKey);
  params.set('sort_dir', tableSortDir);
  params.set('limit',    pageSize);
  params.set('offset',   page * pageSize);

  // Keep sidebar dropdown in sync
  const fSort = document.getElementById('f-sort');
  if (fSort) fSort.value = tableSortKey;

  renderSortHeaders();

  const d = await api('/api/v1/etfs?' + params);
  renderTable(d.data || [], d.total || 0);
}

function renderSortHeaders() {
  document.querySelectorAll('#etf-thead th[data-sort]').forEach(th => {
    const key = th.dataset.sort;
    const isActive = key === tableSortKey;
    const arrow = isActive ? (tableSortDir === 'asc' ? ' ▲' : ' ▼') : ' ⇅';
    // Strip any existing indicator and re-add
    th.textContent = th.textContent.replace(/\s[▲▼⇅]$/, '') + arrow;
    th.classList.toggle('text-green-600', isActive);
    th.classList.toggle('text-gray-500', !isActive);
  });
}

function sortTable(key) {
  // Default directions per column
  const defaultDir = { rank: 'asc', code: 'asc', name: 'asc', expense: 'asc' };
  if (tableSortKey === key) {
    tableSortDir = tableSortDir === 'asc' ? 'desc' : 'asc';
  } else {
    tableSortKey = key;
    tableSortDir = defaultDir[key] || 'desc';
  }
  page = 0;
  loadTable();
}

function renderTable(etfs, total) {
  const show3y = document.getElementById('col-3y')?.checked ?? false;
  const show5y = document.getElementById('col-5y')?.checked ?? false;
  const label = total.toLocaleString() + ' ETF' + (total !== 1 ? 's' : '');
  document.getElementById('result-count').textContent = label;
  document.getElementById('result-count-sidebar').textContent = label + ' matching';

  const tbody = document.getElementById('etf-table');
  tbody.innerHTML = etfs.map(e => {
    const sel = e.code === selectedCode ? 'row-selected' : '';
    return `<tr class="cursor-pointer ${sel}" data-code="${e.code}">
      <td class="px-3 py-2.5 text-gray-300 font-mono text-xs">${e.rank_by_fum || '—'}</td>
      <td class="px-3 py-2.5" style="min-width:260px">
        <div class="font-bold text-gray-900">${e.code}</div>
        <div class="text-xs text-gray-400 truncate" style="max-width:240px" title="${e.name || ''}">${e.name || ''}</div>
        ${e.benchmark ? `<div class="text-xs text-indigo-400 truncate" style="max-width:240px" title="${e.benchmark}">&#8594; ${e.benchmark}</div>` : ''}
      </td>
      <td class="px-3 py-2.5" style="width:80px;max-width:80px">${acChip(e.asset_class)}</td>
      <td class="px-3 py-2.5 text-right font-mono">
        <span class="dated" title="Price · ${fmtTs(e.last_updated)}">${money(e.current_price)}</span>
      </td>
      <td class="px-3 py-2.5 text-right">
        ${fumDisplay(e)}
      </td>
      <td class="px-3 py-2.5 text-right font-semibold ${pctCls(e.return_1y)}">
        <span class="dated" title="1Y Return · ASX Monthly Report · ${fmtTs(tsFor('asx_report'))}">${pct(e.return_1y)}</span>
      </td>
      <td class="px-3 py-2.5 text-right font-semibold col-3y ${show3y ? '' : 'hidden'} ${pctCls(e.return_3y)}">
        <span class="dated" title="3Y Return · ASX Monthly Report · ${fmtTs(tsFor('asx_report'))}">${pct(e.return_3y)}</span>
      </td>
      <td class="px-3 py-2.5 text-right font-semibold col-5y ${show5y ? '' : 'hidden'} ${pctCls(e.return_5y)}">
        <span class="dated" title="5Y Return · ASX Monthly Report · ${fmtTs(tsFor('asx_report'))}">${pct(e.return_5y)}</span>
      </td>
      <td class="px-3 py-2.5 text-right text-gray-600">
        <span class="dated" title="Distribution Yield · ASX Monthly Report · ${fmtTs(tsFor('asx_report'))}">${e.distribution_yield != null ? e.distribution_yield.toFixed(1) + '%' : '—'}</span>
      </td>
      <td class="px-3 py-2.5 text-right text-gray-400">
        <span class="dated" title="MER · Issuer website · ${fmtTs(tsFor(ISSUER_SRC[e.issuer] || ''))}">${e.expense_ratio != null ? e.expense_ratio.toFixed(2) + '%' : '—'}</span>
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
  const _pSrc = 'ASX live data · ' + fmtTs(d.last_updated);
  const _aSrc = 'ASX Monthly Report · ' + fmtTs(tsFor('asx_report'));
  const _mSrc = 'Issuer website · ' + fmtTs(tsFor(ISSUER_SRC[d.issuer] || ''));
  const metrics = [
    { label: 'Price',     value: money(d.current_price),           tip: _pSrc },
    { label: 'NAV',       value: d.nav_per_unit != null ? '$' + d.nav_per_unit.toFixed(4) : '—',
                          tip: 'Net Asset Value per unit · Yahoo Finance / iShares' },
    { label: 'Prem/Disc', value: d.premium_discount_pct != null
                                 ? (d.premium_discount_pct >= 0 ? '+' : '') + d.premium_discount_pct.toFixed(3) + '%' : '—',
                          tip: '(Price − NAV) / NAV · positive = premium, negative = discount',
                          num: d.premium_discount_pct },
    { label: 'Day Chg',   value: pct(d.day_change_pct),            tip: _pSrc, num: d.day_change_pct },
    { label: 'CHESS FUM', value: chessFum(d) != null ? fmtFum(chessFum(d)) : '—',
                          tip: chessFum(d) != null
                               ? 'CHESS units on issue × last price · ' + (d.units_on_issue_date || 'ASX monthly report')
                               : 'Units on issue not available for this fund' },
    { label: 'Total FUM', value: fmtFum(d.fund_size_aud_millions),
                          tip: 'Total FUM · ASX Monthly Report · ' + fmtTs(tsFor('asx_report')) },
    { label: 'Units on Issue', value: fmtUnits(d.units_on_issue),
                          tip: d.units_on_issue_date ? 'ASX Monthly Report · ' + d.units_on_issue_date : _aSrc },
    { label: 'Units Change',  value: d.units_change != null
                                     ? (d.units_change >= 0 ? '+' : '-') + fmtUnits(Math.abs(d.units_change)) : '—',
                          tip: d.units_change_date
                               ? 'vs ' + d.units_change_prev_date + ' · Issuer website · ' + d.units_change_date : '',
                          num: d.units_change },
    { label: 'Net Assets', value: fmtAud(d.net_assets_aud),
                          tip: d.net_assets_date ? 'Issuer website · ' + d.net_assets_date : _aSrc },
    { label: 'Rank',      value: d.rank_by_fum ? '#' + d.rank_by_fum : '—',
                          tip: 'FUM ranking · ' + fmtTs(tsFor('master_list')) },
    { label: '1Y Return', value: pct(d.return_1y),                 tip: _aSrc, num: d.return_1y },
    { label: 'Yield',     value: d.distribution_yield != null ? d.distribution_yield.toFixed(1) + '%' : '—', tip: _aSrc },
    { label: 'MER',       value: (d.expense_ratio || d.management_fee) != null
                                 ? (d.expense_ratio || d.management_fee).toFixed(2) + '%' : '—', tip: _mSrc },
    { label: 'Issuer',    value: d.issuer || '—' },
  ];
  document.getElementById('d-metrics').innerHTML = metrics.map(m => `
    <div class="px-4 py-3">
      <p class="text-xs text-gray-400 font-medium">${m.label}</p>
      <p class="font-semibold text-sm mt-0.5 truncate ${m.num != null ? pctCls(m.num) : 'text-gray-800'} ${m.tip ? 'dated' : ''}"
         ${m.tip ? `title="${m.tip}"` : ''}>${m.value}</p>
    </div>`).join('');

  // Reset to overview tab
  document.querySelectorAll('.dtab').forEach(b => b.classList.remove('tab-active'));
  document.querySelector('.dtab[data-tab="overview"]').classList.add('tab-active');
  renderOverviewTab(d);
  panel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

/* ======================================================= tab rendering */
function renderOverviewTab(d) {
  const _ao = 'ASX Monthly Report · ' + fmtTs(tsFor('asx_report'));
  const _po = 'ASX live data · ' + fmtTs(d.last_updated);
  const cards = [
    ['1M Return',   pct(d.return_1m),  d.return_1m, _ao],
    ['3M Return',   pct(d.return_3m),  d.return_3m, _ao],
    ['1Y Return',   pct(d.return_1y),  d.return_1y, _ao],
    ['3Y Return',   pct(d.return_3y),  d.return_3y, _ao],
    ['52W High',    money(d.year_high), null,         _po],
    ['52W Low',     money(d.year_low),  null,         _po],
    ['Bid/Ask',     d.bid_ask_spread_pct != null ? d.bid_ask_spread_pct + '%' : '—', null, _po],
    ['Inception',   d.inception_date || '—'],
    ['Asset Class', d.asset_class || '—'],
    ['Exchange',    d.exchange || '—'],
    ['Currency',    'AUD'],
  ];

  // ── AI Summary card ──────────────────────────────────────────────────────
  let aiSummaryHtml = '';
  if (d.summary) {
    let s = null;
    try { s = typeof d.summary === 'string' ? JSON.parse(d.summary) : d.summary; } catch(e) {}
    if (s) {
      const risksHtml = Array.isArray(s.key_risks) && s.key_risks.length
        ? `<details class="mt-2">
             <summary class="text-xs text-green-600 cursor-pointer hover:underline select-none">
               Key risks (${s.key_risks.length})
             </summary>
             <ul class="mt-1.5 space-y-1 list-disc list-inside">
               ${s.key_risks.map(r => `<li class="text-xs text-gray-600">${r}</li>`).join('')}
             </ul>
           </details>`
        : '';
      aiSummaryHtml = `
        <div class="bg-green-50 border border-green-100 rounded-lg px-4 py-3 mb-3">
          <p class="text-green-500 text-xs font-medium uppercase tracking-wide mb-1.5">AI Summary</p>
          ${s.summary ? `<p class="text-sm text-gray-700 leading-relaxed">${s.summary}</p>` : ''}
          ${s.objective ? `<p class="mt-2 text-xs text-gray-500"><span class="font-medium text-gray-600">Objective:</span> ${s.objective}</p>` : ''}
          ${s.suitable_for ? `<p class="mt-1 text-xs text-gray-500"><span class="font-medium text-gray-600">Suitable for:</span> ${s.suitable_for}</p>` : ''}
          ${risksHtml}
          <p class="mt-2 text-xs text-gray-400 italic">Generated by AI from PDS — not financial advice.</p>
        </div>`;
    }
  }

  // ── Document links row ────────────────────────────────────────────────────
  const docLinks = [
    ['PDS',        d.pds_url],
    ['TMD',        d.tmd_url],
    ['Fact Sheet', d.factsheet_url],
  ].filter(([, url]) => url);
  const docLinksHtml = docLinks.length ? `
    <div class="flex flex-wrap gap-2 mb-3">
      ${docLinks.map(([label, url]) => `
        <a href="${url}" target="_blank" rel="noopener"
           class="inline-flex items-center gap-1 px-3 py-1.5 bg-white border border-gray-200
                  rounded-full text-xs font-medium text-gray-600 hover:border-green-400
                  hover:text-green-600 transition-colors">
          &#128196; ${label}
        </a>`).join('')}
    </div>` : '';

  // ── Benchmark banner ──────────────────────────────────────────────────────
  const benchmarkRow = d.benchmark ? `
    <div class="bg-indigo-50 border border-indigo-100 rounded-lg px-4 py-3 mb-3 flex items-center gap-2">
      <span class="text-indigo-300 text-lg">&#8594;</span>
      <div>
        <p class="text-indigo-400 text-xs font-medium uppercase tracking-wide">Tracked Index</p>
        <p class="font-semibold text-indigo-900 text-sm">${d.benchmark}</p>
      </div>
    </div>` : '';

  document.getElementById('tab-content').innerHTML = `
    ${aiSummaryHtml}
    ${docLinksHtml}
    ${benchmarkRow}
    <div class="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
      ${cards.map(([label, val, num, tip]) => `
        <div class="bg-slate-50 rounded-lg p-3 border border-gray-100${tip ? ' dated' : ''}"
             ${tip ? `title="${tip}"` : ''}>
          <p class="text-gray-400 text-xs">${label}</p>
          <p class="font-semibold text-sm mt-0.5 ${num != null ? pctCls(num) : 'text-gray-800'}">${val}</p>
        </div>`).join('')}
    </div>
    ${d.description ? `<p class="mt-4 text-sm text-gray-600 leading-relaxed border-t pt-4">${d.description}</p>` : ''}
    ${d.issuer_url ? `<a href="${d.issuer_url}" target="_blank" rel="noopener"
       class="mt-3 inline-flex items-center gap-1 text-green-600 hover:underline text-sm">
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

  } else if (tab === 'performance') {
    el.innerHTML = '<div class="flex justify-center py-10"><div class="spinner"></div></div>';
    await renderPerformanceTab(selectedCode);

  } else if (tab === 'holdings') {
    el.innerHTML = '<div class="flex justify-center py-10"><div class="spinner"></div></div>';
    const h = await api('/api/v1/etfs/' + selectedCode + '/holdings');
    if (!h.holdings || !h.holdings.length) {
      el.innerHTML = '<p class="text-gray-400 text-sm text-center py-10">No holdings data available for this ETF.</p>';
      return;
    }
    const all = h.holdings;
    const holdingsTs = all.reduce((mx, r) => r.last_updated > mx ? r.last_updated : mx, '');
    const maxW = Math.max(...all.map(x => x.weight_pct || 0));
    const top10Wt = all.slice(0, 10).reduce((s, x) => s + (x.weight_pct || 0), 0);
    const byCountry = {};
    all.forEach(r => { const c = r.country || 'Other'; byCountry[c] = (byCountry[c] || 0) + (r.weight_pct || 0); });
    const topCountries = Object.entries(byCountry).sort((a, b) => b[1] - a[1]).slice(0, 4);

    el.innerHTML = `
      <div class="flex flex-wrap items-center gap-2 mb-3">
        <span class="bg-green-50 text-green-700 px-2.5 py-1 rounded-full text-xs font-semibold">${all.length} holdings</span>
        <span class="text-xs text-gray-500">Top 10 concentration:
          <strong class="text-gray-700">${top10Wt.toFixed(1)}%</strong></span>
        ${topCountries.length > 1 ? `<span class="text-xs text-gray-400">·</span>
          <span class="text-xs text-gray-500">${topCountries.map(([c, w]) =>
            `<strong class="text-gray-600">${c}</strong> ${w.toFixed(0)}%`).join(' &middot; ')}</span>` : ''}
        ${holdingsTs ? `<span class="ml-auto text-xs text-gray-400" title="Holdings last updated by issuer scraper">As of ${fmtTs(holdingsTs)}</span>` : ''}
      </div>
      <input id="holding-filter" type="text" placeholder="Filter by name or ticker…"
        class="w-full border border-gray-200 rounded-lg px-3 py-1.5 text-sm mb-3 focus:outline-none focus:ring-2 focus:ring-green-200">
      <div class="text-xs text-gray-400 grid gap-x-3 mb-1 pr-1"
           style="grid-template-columns:3.5rem 1fr 7rem 5rem">
        <span class="text-right">Ticker</span><span>Name</span><span>Sector</span><span class="hidden sm:block">Country</span>
      </div>
      <div id="holdings-list" class="space-y-1 overflow-y-auto" style="max-height:440px">
        ${all.map((r, i) => `
          <div class="holding-row flex items-center gap-3 py-0.5 hover:bg-slate-50 rounded ${i >= 50 ? 'hidden extra-holding' : ''}"
               data-name="${(r.name || '').toLowerCase().replace(/"/g, '')}"
               data-ticker="${(r.ticker || '').toLowerCase()}">
            <div class="w-14 text-xs font-mono text-gray-400 shrink-0 text-right">${r.ticker || ''}</div>
            <div class="flex-1 min-w-0">
              <div class="flex items-center justify-between mb-0.5">
                <span class="text-xs font-medium text-gray-700 truncate">${r.name || ''}</span>
                <span class="text-xs font-bold text-green-700 ml-2 shrink-0">${r.weight_pct != null ? r.weight_pct.toFixed(2) + '%' : '—'}</span>
              </div>
              <div class="h-1.5 bg-gray-100 rounded-full overflow-hidden">
                <div class="h-full bg-green-400 rounded-full pbar"
                     style="width:${maxW > 0 ? ((r.weight_pct || 0) / maxW * 100).toFixed(1) : 0}%"></div>
              </div>
            </div>
            <div class="text-xs text-gray-400 w-28 shrink-0 truncate">${r.sector || ''}</div>
            <div class="text-xs text-gray-300 w-20 shrink-0 truncate hidden sm:block">${r.country || ''}</div>
          </div>`).join('')}
      </div>
      ${all.length > 50 ? `
        <button id="show-all-holdings"
          class="mt-2 w-full text-xs text-green-600 hover:text-green-800 hover:underline py-1.5 border-t border-gray-100">
          Show all ${all.length} holdings ↓
        </button>` : ''}`;

    document.getElementById('holding-filter')?.addEventListener('input', function () {
      const q = this.value.trim().toLowerCase();
      document.querySelectorAll('.holding-row').forEach(row => {
        const vis = !q || row.dataset.name.includes(q) || row.dataset.ticker.includes(q);
        row.classList.toggle('hidden', !vis);
      });
    });
    document.getElementById('show-all-holdings')?.addEventListener('click', function () {
      document.querySelectorAll('.extra-holding').forEach(r => r.classList.remove('hidden', 'extra-holding'));
      this.remove();
    });

  } else if (tab === 'sectors') {
    el.innerHTML = '<div class="flex justify-center py-10"><div class="spinner"></div></div>';
    const s = await api('/api/v1/etfs/' + selectedCode + '/sectors');
    if (!s.sectors || !s.sectors.length) {
      el.innerHTML = '<p class="text-gray-400 text-sm text-center py-10">No sector data available.</p>';
      return;
    }
    const totalWt = s.sectors.reduce((sum, r) => sum + (r.weight_pct || 0), 0);
    const sectorsTs = s.sectors.reduce((mx, r) => r.last_updated > mx ? r.last_updated : mx, '');
    el.innerHTML = `
      <div class="grid grid-cols-1 sm:grid-cols-2 gap-6 items-start">
        <div class="flex justify-center">
          <div style="position:relative;width:200px;height:200px">
            <canvas id="sector-chart" width="200" height="200"></canvas>
          </div>
        </div>
        <div>
          <div class="space-y-2.5">
            ${s.sectors.map((r, i) => `
              <div class="flex items-center gap-2.5">
                <div class="w-2.5 h-2.5 rounded-sm shrink-0" style="background:${PALETTE[i % PALETTE.length]}"></div>
                <span class="text-sm text-gray-700 flex-1 truncate min-w-0">${r.sector}</span>
                <div class="w-20 bg-gray-100 rounded-full h-2 overflow-hidden shrink-0">
                  <div class="h-full rounded-full pbar"
                       style="width:${totalWt > 0 ? (r.weight_pct / totalWt * 100).toFixed(0) : 0}%;background:${PALETTE[i % PALETTE.length]}"></div>
                </div>
                <span class="text-sm font-semibold text-gray-700 w-12 text-right shrink-0">
                  ${r.weight_pct != null ? r.weight_pct.toFixed(1) + '%' : '—'}
                </span>
              </div>`).join('')}
          </div>
          <p class="text-xs text-gray-400 mt-3 pt-2 border-t border-gray-100">
            Coverage: <strong>${totalWt.toFixed(1)}%</strong> of portfolio
            ${sectorsTs ? `· <span title="Sectors last updated by issuer scraper">As of ${fmtTs(sectorsTs)}</span>` : ''}
          </p>
        </div>
      </div>`;

    new Chart(document.getElementById('sector-chart'), {
      type: 'doughnut',
      data: {
        labels: s.sectors.map(r => r.sector),
        datasets: [{
          data: s.sectors.map(r => r.weight_pct),
          backgroundColor: s.sectors.map((_, i) => PALETTE[i % PALETTE.length]),
          borderWidth: 2, borderColor: '#fff'
        }]
      },
      options: {
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: ctx => ` ${ctx.label}: ${Number(ctx.raw).toFixed(1)}%` } }
        },
        cutout: '60%'
      }
    });

  } else if (tab === 'dividends') {
    el.innerHTML = '<div class="flex justify-center py-10"><div class="spinner"></div></div>';
    const [dv, etf] = await Promise.all([
      api('/api/v1/etfs/' + selectedCode + '/dividends'),
      api('/api/v1/etfs/' + selectedCode)
    ]);

    const yieldPct = etf.distribution_yield;
    const price    = etf.current_price;
    const incomePerUnit  = (yieldPct != null && price != null) ? (yieldPct / 100 * price) : null;
    const incomePerTenK  = (yieldPct != null && price != null && price > 0)
                           ? (10000 / price * incomePerUnit) : null;

    const yieldBar = (yieldPct != null) ? `
      <div class="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-5">
        <div class="bg-green-50 rounded-lg p-3 border border-green-100">
          <p class="text-xs text-green-600 font-medium">Distribution Yield</p>
          <p class="text-xl font-bold text-green-700 mt-0.5">${yieldPct.toFixed(2)}%</p>
        </div>
        ${price != null ? `<div class="bg-slate-50 rounded-lg p-3 border border-gray-100">
          <p class="text-xs text-gray-400 font-medium">Unit Price</p>
          <p class="text-xl font-bold text-gray-800 mt-0.5">${money(price)}</p>
        </div>` : ''}
        ${incomePerUnit != null ? `<div class="bg-slate-50 rounded-lg p-3 border border-gray-100">
          <p class="text-xs text-gray-400 font-medium">Income / Unit</p>
          <p class="text-xl font-bold text-gray-800 mt-0.5">${money(incomePerUnit)}</p>
        </div>` : ''}
        ${incomePerTenK != null ? `<div class="bg-green-50 rounded-lg p-3 border border-green-100">
          <p class="text-xs text-green-600 font-medium">Est. Income / $10K</p>
          <p class="text-xl font-bold text-green-700 mt-0.5">${money(incomePerTenK)}<span class="text-xs font-normal text-green-400">/yr</span></p>
        </div>` : ''}
      </div>` : '';

    if (!dv.dividends || !dv.dividends.length) {
      el.innerHTML = yieldBar + `
        <div class="text-center py-6 text-gray-400 text-sm border border-dashed border-gray-200 rounded-lg">
          <p class="font-medium text-gray-500 mb-1">No dividend history in database</p>
          <p class="text-xs">Historical distribution data isn't collected yet.</p>
        </div>`;
      return;
    }

    const totalAmt = dv.dividends.reduce((s, r) => s + (r.amount || 0), 0);
    el.innerHTML = yieldBar + `
      <div class="overflow-x-auto">
        <table class="w-full text-sm">
          <thead>
            <tr class="text-left text-xs text-gray-400 font-semibold uppercase tracking-wide border-b">
              <th class="pb-2 pr-3">Ex Date</th>
              <th class="pb-2 pr-3">Pay Date</th>
              <th class="pb-2 pr-3 text-right">Amount</th>
              <th class="pb-2 pr-3 text-right">Franking</th>
              <th class="pb-2">Type</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-100">
            ${dv.dividends.map(r => `
              <tr class="hover:bg-slate-50">
                <td class="py-2 pr-3 font-medium">${r.ex_date || ''}</td>
                <td class="py-2 pr-3 text-gray-500">${r.pay_date || ''}</td>
                <td class="py-2 pr-3 text-right font-mono font-medium">${r.amount != null ? '$' + r.amount.toFixed(4) : '—'}</td>
                <td class="py-2 pr-3 text-right">${r.franking_pct != null ? r.franking_pct + '%' : '—'}</td>
                <td class="py-2 text-gray-500 capitalize">${r.type || ''}</td>
              </tr>`).join('')}
          </tbody>
          <tfoot class="border-t-2 border-gray-200">
            <tr>
              <td colspan="2" class="pt-2 text-xs text-gray-400">${dv.dividends.length} distribution${dv.dividends.length !== 1 ? 's' : ''}</td>
              <td class="pt-2 text-right font-mono font-bold text-gray-700">$${totalAmt.toFixed(4)}</td>
              <td colspan="2"></td>
            </tr>
          </tfoot>
        </table>
      </div>`;
  }
}

/* ======================================================= performance tab */
const BENCHMARK_LABELS = {
  // Equities
  ASX200:  'S&P/ASX 200',
  AORD:    'All Ords',
  SP500:   'S&P 500',
  NDX100:  'Nasdaq 100',
  MSCIW:   'MSCI ACWI',
  // Fixed Income
  AU_BOND: 'AU Bonds (VAF)',
  US_BOND: 'US Bonds (AGG)',
  // Commodities
  GOLD:    'Gold (GLD)',
  COMMOD:  'Commodities (GSCI)',
  // Cash
  AU_CASH: 'AU Cash (BILL)',
  // Crypto
  BTC:     'Bitcoin',
  ETH:     'Ethereum',
};
const BENCHMARK_COLORS = {
  // Equities — blues/purples
  ASX200:  '#6366f1',
  AORD:    '#8b5cf6',
  SP500:   '#f59e0b',
  NDX100:  '#10b981',
  MSCIW:   '#ec4899',
  // Fixed Income — teals
  AU_BOND: '#0d9488',
  US_BOND: '#0891b2',
  // Commodities — ambers/oranges
  GOLD:    '#d97706',
  COMMOD:  '#b45309',
  // Cash — slate
  AU_CASH: '#64748b',
  // Crypto — rose/orange
  BTC:     '#f97316',
  ETH:     '#a855f7',
};

let perfChart = null;
let perfData  = null;   // last fetched payload
let perfCode  = null;

async function renderPerformanceTab(code) {
  const el = document.getElementById('tab-content');
  const periods = ['1m', '3m', '6m', '1y', '3y', '5y'];
  const labels  = { '1m':'1 Month','3m':'3 Months','6m':'6 Months',
                    '1y':'1 Year','3y':'3 Years','5y':'5 Years' };

  el.innerHTML = `
    <div class="flex flex-wrap gap-2 mb-4 items-center justify-between">
      <div class="flex gap-1" id="perf-period-btns">
        ${periods.map(p => `
          <button data-period="${p}"
                  class="perf-period px-3 py-1 text-xs rounded-lg border font-medium transition-colors
                         ${p === '1y' ? 'bg-green-600 text-white border-green-600' : 'border-gray-200 text-gray-500 hover:border-green-400 hover:text-green-600'}">
            ${labels[p]}
          </button>`).join('')}
      </div>
      <div class="flex gap-1 flex-wrap" id="perf-bmark-btns"></div>
    </div>

    <div class="relative mb-4" style="height:300px">
      <canvas id="perf-chart"></canvas>
    </div>

    <div id="perf-table-wrap" class="overflow-x-auto mb-2"></div>
    <p class="text-xs text-gray-400">Prices from Yahoo Finance · rebased to 100 at period start · weekly data</p>`;

  await loadPerfData(code, '1y');

  // Period button clicks
  document.getElementById('perf-period-btns').addEventListener('click', async e => {
    const btn = e.target.closest('[data-period]');
    if (!btn) return;
    document.querySelectorAll('.perf-period').forEach(b => {
      b.className = b.className.replace('bg-green-600 text-white border-green-600',
                                        'border-gray-200 text-gray-500 hover:border-green-400 hover:text-green-600');
    });
    btn.className = btn.className.replace('border-gray-200 text-gray-500 hover:border-green-400 hover:text-green-600',
                                          'bg-green-600 text-white border-green-600');
    await loadPerfData(code, btn.dataset.period);
  });
}

// Benchmark toggles state
const activeBmarks = new Set(['DEFAULT']);

async function loadPerfData(code, period) {
  const d = await api('/api/v1/etfs/' + code + '/price-history?period=' + period);
  perfData = d;
  perfCode = code;

  // Render benchmark toggle buttons
  const bmarkEl = document.getElementById('perf-bmark-btns');
  if (bmarkEl) {
    const allBmarks = Object.keys(d.benchmarks || {});
    // Default to asset-class benchmark
    if (d.default_benchmark) activeBmarks.add(d.default_benchmark);

    bmarkEl.innerHTML = allBmarks.map(b => {
      const active = activeBmarks.has(b) || b === d.default_benchmark;
      const col = BENCHMARK_COLORS[b] || '#6b7280';
      return `<button data-bmark="${b}"
        class="perf-bmark px-2.5 py-1 text-xs rounded-lg border font-medium transition-colors
               ${active ? 'text-white' : 'text-gray-500 bg-white border-gray-200 hover:border-gray-400'}"
        style="${active ? `background:${col};border-color:${col}` : ''}">
        ${BENCHMARK_LABELS[b] || b}
      </button>`;
    }).join('');

    bmarkEl.querySelectorAll('[data-bmark]').forEach(btn => {
      btn.addEventListener('click', () => {
        const b = btn.dataset.bmark;
        if (activeBmarks.has(b)) activeBmarks.delete(b);
        else activeBmarks.add(b);
        drawPerfChart(perfData, perfCode);
        // Update button style
        const col = BENCHMARK_COLORS[b] || '#6b7280';
        if (activeBmarks.has(b)) {
          btn.style.background = col; btn.style.borderColor = col; btn.classList.add('text-white');
          btn.classList.remove('text-gray-500', 'bg-white', 'border-gray-200', 'hover:border-gray-400');
        } else {
          btn.style.background = ''; btn.style.borderColor = ''; btn.classList.remove('text-white');
          btn.classList.add('text-gray-500', 'bg-white', 'border-gray-200', 'hover:border-gray-400');
        }
      });
    });
  }

  drawPerfChart(d, code);
}

function drawPerfChart(d, code) {
  const prices  = (d.prices || []).filter(r => r.close);
  if (!prices.length) {
    document.getElementById('perf-chart').parentElement.innerHTML =
      '<p class="text-center text-gray-400 text-sm py-16">No price history available for this ETF yet.<br><span class="text-xs">Data is fetched from Yahoo Finance — run the price history fetcher to populate.</span></p>';
    renderPerfTable(d, code, []);
    return;
  }

  // Rebase everything to 100 at first date
  const base0   = prices[0].close;
  const etfDates = prices.map(r => r.date);
  const etfVals  = prices.map(r => parseFloat((r.close / base0 * 100).toFixed(2)));

  const datasets = [{
    label: code,
    data: etfDates.map((dt, i) => ({ x: dt, y: etfVals[i] })),
    borderColor: '#2563eb',
    backgroundColor: 'rgba(37,99,235,0.08)',
    fill: true,
    tension: 0.3,
    pointRadius: 0,
    borderWidth: 2.5,
    order: 0,
  }];

  // Overlay selected benchmarks
  for (const [bName, bSeries] of Object.entries(d.benchmarks || {})) {
    if (!activeBmarks.has(bName) && bName !== d.default_benchmark) continue;
    if (!activeBmarks.has(bName)) continue;
    if (!bSeries.length) continue;
    // Find first benchmark point >= ETF start date
    const startDate = etfDates[0];
    const aligned = bSeries.filter(r => r.date >= startDate);
    if (!aligned.length) continue;
    const bBase = aligned[0].close;
    datasets.push({
      label: BENCHMARK_LABELS[bName] || bName,
      data: aligned.map(r => ({ x: r.date, y: parseFloat((r.close / bBase * 100).toFixed(2)) })),
      borderColor: BENCHMARK_COLORS[bName] || '#9ca3af',
      backgroundColor: 'transparent',
      fill: false,
      tension: 0.3,
      pointRadius: 0,
      borderWidth: 1.5,
      borderDash: [4, 3],
      order: 1,
    });
  }

  const ctx = document.getElementById('perf-chart');
  if (!ctx) return;
  if (perfChart) { perfChart.destroy(); perfChart = null; }

  perfChart = new Chart(ctx, {
    type: 'line',
    data: { datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { position: 'top', labels: { boxWidth: 12, font: { size: 11 } } },
        tooltip: {
          callbacks: {
            label: ctx => `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(2)}`,
            afterBody: items => {
              const etf = items.find(i => i.dataset.label === code);
              if (!etf) return [];
              return [`Return: ${(etf.parsed.y - 100).toFixed(2)}%`];
            },
          },
        },
      },
      scales: {
        x: {
          type: 'time',
          time: { unit: 'month', tooltipFormat: 'yyyy-MM-dd' },
          grid: { display: false },
          ticks: { font: { size: 10 }, maxTicksLimit: 12 },
        },
        y: {
          ticks: {
            font: { size: 10 },
            callback: v => v.toFixed(0),
          },
          grid: { color: 'rgba(0,0,0,0.04)' },
          title: { display: true, text: 'Rebased to 100', font: { size: 10 }, color: '#9ca3af' },
        },
      },
    },
  });

  renderPerfTable(d, code, prices);
}

function renderPerfTable(d, code, prices) {
  const el = document.getElementById('perf-table-wrap');
  if (!el) return;

  // Compute period returns from price series
  function periodReturn(days) {
    if (prices.length < 2) return null;
    const cutoff = new Date(Date.now() - days * 86400000).toISOString().slice(0, 10);
    const from = prices.find(r => r.date >= cutoff);
    if (!from) return null;
    const last = prices[prices.length - 1].close;
    return (last / from.close - 1) * 100;
  }

  const periods = [
    { label: '1M',  days: 31 },
    { label: '3M',  days: 92 },
    { label: '6M',  days: 183 },
    { label: '1Y',  days: 365 },
    { label: '3Y',  days: 1095 },
    { label: '5Y',  days: 1825 },
  ];

  // Build benchmark return columns for active benchmarks
  const activeBmarkList = Object.entries(d.benchmarks || {})
    .filter(([b]) => activeBmarks.has(b));

  const headerCols = activeBmarkList.map(([b]) =>
    `<th class="pb-2 px-3 text-right text-xs text-gray-400 font-semibold uppercase"
         style="color:${BENCHMARK_COLORS[b] || '#9ca3af'}">${BENCHMARK_LABELS[b] || b}</th>`
  ).join('');

  const rows = periods.map(({ label, days }) => {
    const etfRet = periodReturn(days);

    const bmarkCols = activeBmarkList.map(([b, bSeries]) => {
      const cutoff = new Date(Date.now() - days * 86400000).toISOString().slice(0, 10);
      const from = bSeries.find(r => r.date >= cutoff);
      const last = bSeries[bSeries.length - 1];
      const ret = (from && last) ? (last.close / from.close - 1) * 100 : null;
      const diff = (etfRet != null && ret != null) ? etfRet - ret : null;
      const diffStr = diff != null ? ` <span class="text-xs ${diff >= 0 ? 'text-emerald-600' : 'text-red-500'}">(${diff >= 0 ? '+' : ''}${diff.toFixed(2)}%)</span>` : '';
      return `<td class="py-2.5 px-3 text-right text-sm">${ret != null ? ret.toFixed(2) + '%' : '—'}</td>`;
    }).join('');

    const src1m = d.return_1m;  // from etfs table
    // Prefer live-computed from price series if available, fall back to DB
    const showRet = etfRet;
    const cls = showRet != null ? (showRet >= 0 ? 'text-emerald-600' : 'text-red-500') : 'text-gray-400';
    return `<tr class="border-b border-gray-50 hover:bg-slate-50">
      <td class="py-2.5 pr-3 text-xs font-semibold text-gray-500 uppercase">${label}</td>
      <td class="py-2.5 px-3 text-right font-semibold text-sm ${cls}">${showRet != null ? showRet.toFixed(2) + '%' : '—'}</td>
      ${bmarkCols}
    </tr>`;
  }).join('');

  el.innerHTML = `
    <table class="w-full text-sm mt-2">
      <thead>
        <tr class="border-b border-gray-200">
          <th class="pb-2 pr-3 text-left text-xs text-gray-400 font-semibold uppercase">Period</th>
          <th class="pb-2 px-3 text-right text-xs text-green-600 font-semibold uppercase">${code} Return</th>
          ${headerCols}
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>`;
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
      <div class="px-3 py-2.5 hover:bg-green-50 cursor-pointer flex justify-between items-center border-b last:border-b-0"
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
['f-exchange', 'f-issuer', 'f-asset', 'f-type'].forEach(id =>
  document.getElementById(id).addEventListener('change', () => { page = 0; loadTable(); })
);
// Benchmark text filter — debounced
let bmTimer;
document.getElementById('f-benchmark').addEventListener('input', () => {
  clearTimeout(bmTimer);
  bmTimer = setTimeout(() => { page = 0; loadTable(); }, 350);
});
// Sidebar sort dropdown — update column sort state then reload
document.getElementById('f-sort').addEventListener('change', function () {
  tableSortKey = this.value;
  const defaultDir = { rank: 'asc', code: 'asc', name: 'asc', expense: 'asc' };
  tableSortDir = defaultDir[tableSortKey] || 'desc';
  page = 0;
  loadTable();
});
document.getElementById('btn-reset').addEventListener('click', () => {
  ['f-exchange', 'f-issuer', 'f-asset', 'f-type'].forEach(id => document.getElementById(id).value = '');
  document.getElementById('f-benchmark').value = '';
  tableSortKey = 'rank';
  tableSortDir = 'asc';
  page = 0;
  loadTable();
});

// Column header click-to-sort
document.getElementById('etf-thead').addEventListener('click', e => {
  const th = e.target.closest('th[data-sort]');
  if (th) sortTable(th.dataset.sort);
});

/* ======================================================= analytics */
async function loadAnalytics() {
  const [flows, issuers, cats] = await Promise.all([
    api('/api/v1/analytics/fund-flows?limit=8'),
    api('/api/v1/issuers'),
    api('/api/v1/categories'),
  ]);

  // Issuer count stat card
  document.getElementById('c-issuers').textContent = (issuers.issuers || []).length;

  /* ── 1. Asset class doughnut ── */
  const catData = (cats.categories || []).filter(c => c.total_fum > 0).slice(0, 12);
  const totalAum = catData.reduce((s, c) => s + (c.total_fum || 0), 0);

  // Centre-text plugin
  const centreTextPlugin = {
    id: 'centreText',
    beforeDraw(chart) {
      const { ctx, chartArea: { top, left, width, height } } = chart;
      ctx.save();
      const cx = left + width / 2, cy = top + height / 2;
      ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
      ctx.font = 'bold 13px Inter,sans-serif';
      ctx.fillStyle = '#1e293b';
      ctx.fillText(fmtFum(totalAum), cx, cy - 7);
      ctx.font = '10px Inter,sans-serif';
      ctx.fillStyle = '#94a3b8';
      ctx.fillText('Total AUM', cx, cy + 8);
      ctx.restore();
    }
  };

  if (chartAsset) chartAsset.destroy();
  chartAsset = new Chart(
    document.getElementById('chart-asset').getContext('2d'), {
      type: 'doughnut',
      plugins: [centreTextPlugin],
      data: {
        labels: catData.map(c => c.asset_class || 'Other'),
        datasets: [{
          data: catData.map(c => c.total_fum || 0),
          backgroundColor: catData.map(c => assetColor(c.asset_class)),
          hoverBackgroundColor: catData.map(c => assetColor(c.asset_class)),
          borderWidth: 2,
          borderColor: '#fff',
          hoverBorderWidth: 3,
          hoverOffset: 8,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: '64%',
        animation: { animateRotate: true, duration: 600 },
        onClick(evt, els) {
          if (!els.length) return;
          const label = catData[els[0].index]?.asset_class;
          if (!label) return;
          // Navigate to list view filtered by this asset class
          document.querySelector('.main-tab[data-view=list]').click();
          setTimeout(() => {
            // Set the sort to FUM and apply asset class text filter
            const search = document.getElementById('search');
            if (search) { search.value = ''; search.dispatchEvent(new Event('input')); }
            // Trigger asset class filter in the screener — open screener with that class pre-filtered
            document.querySelector('.main-tab[data-view=screener]').click();
            setTimeout(() => {
              const cb = [...document.querySelectorAll('.sc-ac')].find(el => el.value === label);
              if (cb && !cb.checked) { cb.checked = true; cb.dispatchEvent(new Event('change')); }
            }, 50);
          }, 50);
        },
        plugins: {
          legend: {
            position: 'bottom',
            labels: {
              font: { size: 10 },
              padding: 8,
              boxWidth: 10,
              generateLabels(chart) {
                return chart.data.labels.map((label, i) => ({
                  text: label,
                  fillStyle: catData[i] ? assetColor(catData[i].asset_class) : '#ccc',
                  strokeStyle: '#fff',
                  lineWidth: 0,
                  index: i,
                }));
              },
            },
          },
          tooltip: {
            callbacks: {
              label(ctx) {
                const c = catData[ctx.dataIndex];
                const pct = totalAum > 0 ? ((c.total_fum / totalAum) * 100).toFixed(1) : '0';
                return [
                  ' ' + fmtFum(c.total_fum) + '  (' + pct + '%)',
                  ' ' + c.etf_count + ' ETFs',
                ];
              },
            },
          },
        },
      },
    }
  );

  /* ── 2. Issuer market share — pie/doughnut chart ── */
  const topIss = (issuers.issuers || []).filter(i => (i.total_fum || 0) > 0).slice(0, 10);
  const issTotal = topIss.reduce((s, i) => s + (i.total_fum || 0), 0);

  if (chartIssuers) chartIssuers.destroy();
  chartIssuers = new Chart(
    document.getElementById('chart-issuers').getContext('2d'), {
      type: 'doughnut',
      data: {
        labels: topIss.map(i => i.name),
        datasets: [{
          data: topIss.map(i => i.total_fum || 0),
          backgroundColor: topIss.map(i => issuerColor(i.name)),
          borderWidth: 2,
          borderColor: '#fff',
          hoverBorderWidth: 3,
          hoverOffset: 8,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: '60%',
        animation: { animateRotate: true, duration: 600 },
        onClick(evt, els) {
          if (!els.length) return;
          const iss = topIss[els[0].index];
          if (!iss) return;
          document.querySelector('.main-tab[data-view=screener]').click();
          setTimeout(() => {
            const cb = [...document.querySelectorAll('.sc-is')].find(el => el.value === iss.name);
            if (cb && !cb.checked) { cb.checked = true; cb.dispatchEvent(new Event('change')); }
          }, 80);
        },
        plugins: {
          legend: {
            position: 'bottom',
            labels: {
              font: { size: 10 },
              padding: 7,
              boxWidth: 10,
            },
          },
          tooltip: {
            callbacks: {
              label(ctx) {
                const iss = topIss[ctx.dataIndex];
                const pct = issTotal > 0 ? ((iss.total_fum / issTotal) * 100).toFixed(1) : '0';
                return [
                  ' ' + fmtFum(iss.total_fum) + '  (' + pct + '%)',
                  ' ' + iss.etf_count + ' ETFs',
                ];
              },
            },
          },
        },
      },
    }
  );

  /* ── 3. Fund flows bar chart — coloured by issuer, click to detail ── */
  const inflows  = (flows.top_inflows  || []).slice(0, 5);
  const outflows = (flows.top_outflows || []).slice(0, 5);
  const flowRows   = [...inflows, ...outflows];
  const flowLabels = flowRows.map(r => r.code);
  const flowVals   = flowRows.map(r => r.fund_flow_1m || 0);
  // Positive bars: issuer brand color; negative bars: muted red
  const flowBg = flowRows.map(r =>
    (r.fund_flow_1m || 0) >= 0
      ? issuerColor(r.issuer)
      : 'rgba(239,68,68,0.75)'
  );
  const flowBorder = flowRows.map(r =>
    (r.fund_flow_1m || 0) >= 0
      ? issuerColor(r.issuer)
      : '#ef4444'
  );

  if (chartFlows) chartFlows.destroy();
  chartFlows = new Chart(
    document.getElementById('chart-flows').getContext('2d'), {
      type: 'bar',
      data: {
        labels: flowLabels,
        datasets: [{
          data: flowVals,
          backgroundColor: flowBg,
          borderColor: flowBorder,
          borderWidth: 1,
          borderRadius: 5,
          borderSkipped: false,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 500 },
        onClick(evt, els) {
          if (!els.length) return;
          const row = flowRows[els[0].index];
          if (row) {
            showDetail(row.code);
            document.querySelector('.main-tab[data-view=list]').click();
          }
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              title: ctx => {
                const r = flowRows[ctx[0].dataIndex];
                return r ? r.code + (r.issuer ? '  ·  ' + r.issuer : '') : ctx[0].label;
              },
              label: ctx => {
                const r = flowRows[ctx.dataIndex];
                return [
                  ' Flow: ' + fmtFum(ctx.parsed.y),
                  r?.name ? ' ' + r.name.slice(0, 40) : '',
                ].filter(Boolean);
              },
            },
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

/* ======================================================= data freshness */
let scrapeTimes = {};

// Map issuer display names → scrape_log source keys
const ISSUER_SRC = {
  'BetaShares': 'betashares', 'Vanguard': 'vanguard', 'iShares': 'ishares',
  'VanEck': 'vaneck', 'Global X': 'globalx', 'SPDR': 'spdr',
  'StateStreet': 'statestreet',
};

function fmtTs(ts) {
  if (!ts) return 'unknown date';
  const d = new Date(String(ts).replace(' ', 'T'));
  if (isNaN(d)) return String(ts);
  return d.toLocaleDateString('en-AU', { weekday: 'short', day: 'numeric', month: 'short', year: 'numeric' })
       + ' ' + d.toLocaleTimeString('en-AU', { hour: '2-digit', minute: '2-digit', hour12: false });
}

function tsFor(source) { return scrapeTimes[source] || ''; }

async function loadScrapeTimes() {
  const d = await api('/api/v1/scrape-status');
  (d.log || []).forEach(row => {
    if (row.finished_at && (!scrapeTimes[row.source] || row.finished_at > scrapeTimes[row.source]))
      scrapeTimes[row.source] = row.finished_at;
  });
}

/* ======================================================= init */
async function init() {
  try {
    await Promise.all([loadFilters(), loadOverview(), loadTable(), loadAnalytics(), loadScrapeTimes()]);
  } catch (e) {
    console.error('Init error:', e);
  }
}

init();
setInterval(() => { loadOverview(); loadTable(); }, 120000);

/* ======================================================= main view tabs */
const VIEWS = ['list', 'screener', 'compare', 'holdings', 'analytics'];
document.querySelectorAll('.main-tab').forEach(btn => {
  btn.addEventListener('click', () => {
    const v = btn.dataset.view;
    document.querySelectorAll('.main-tab').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    VIEWS.forEach(id => document.getElementById('view-' + id).classList.add('hidden'));
    document.getElementById('view-' + v).classList.remove('hidden');
    if (v === 'screener'  && !screenerLoaded)  initScreener();
    if (v === 'compare'   && !compareLoaded)   initCompare();
    if (v === 'analytics' && !analyticsLoaded) initAnalytics();
  });
});

/* ============================================================ SCREENER */
let screenerLoaded = false;
let scLastData = [];
const scFilters = {
  exchange: '', assetClasses: new Set(), issuers: new Set(),
  maxFee: 2, minFum: '', minRet: '', maxRet: '', minYield: '', hedged: false,
};
let scTimer;

async function initScreener() {
  screenerLoaded = true;
  // Populate checklists from already-loaded filter data
  const [cats, issuers] = await Promise.all([
    api('/api/v1/categories'),
    api('/api/v1/issuers'),
  ]);

  const assetEl = document.getElementById('sc-asset-list');
  (cats.categories || []).forEach(c => {
    const id = 'sca-' + c.asset_class.replace(/\W/g, '_');
    const label = document.createElement('label');
    label.innerHTML = `<input type="checkbox" id="${id}" value="${c.asset_class}" class="sc-ac accent-green-600">
      <span class="truncate">${c.asset_class} <span class="text-gray-400">(${c.etf_count})</span></span>`;
    label.querySelector('input').addEventListener('change', e => {
      if (e.target.checked) scFilters.assetClasses.add(e.target.value);
      else scFilters.assetClasses.delete(e.target.value);
      scDebounceFetch();
    });
    assetEl.appendChild(label);
  });

  const issuerEl = document.getElementById('sc-issuer-list');
  (issuers.issuers || []).forEach(i => {
    const id = 'sci-' + i.name.replace(/\W/g, '_');
    const label = document.createElement('label');
    label.innerHTML = `<input type="checkbox" id="${id}" value="${i.name}" class="sc-is accent-green-600">
      <span class="truncate">${i.name} <span class="text-gray-400">(${i.etf_count})</span></span>`;
    label.querySelector('input').addEventListener('change', e => {
      if (e.target.checked) scFilters.issuers.add(e.target.value);
      else scFilters.issuers.delete(e.target.value);
      scDebounceFetch();
    });
    issuerEl.appendChild(label);
  });

  // Wire exchange toggles
  document.querySelectorAll('.sc-exch').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.sc-exch').forEach(b => {
        b.classList.remove('active', 'bg-green-600', 'text-white', 'border-green-600');
        b.classList.add('text-gray-600', 'border-gray-200');
      });
      btn.classList.add('active', 'bg-green-600', 'text-white', 'border-green-600');
      btn.classList.remove('text-gray-600', 'border-gray-200');
      scFilters.exchange = btn.dataset.exch;
      scDebounceFetch();
    });
  });

  // Fee slider
  const feeSlider = document.getElementById('sc-fee');
  const feeVal = document.getElementById('sc-fee-val');
  feeSlider.addEventListener('input', () => {
    scFilters.maxFee = parseFloat(feeSlider.value);
    feeVal.textContent = scFilters.maxFee.toFixed(2) + '%';
    scDebounceFetch();
  });

  // Numeric inputs
  const scInputs = {
    'sc-fum': 'minFum', 'sc-ret-min': 'minRet',
    'sc-ret-max': 'maxRet', 'sc-yield': 'minYield',
  };
  Object.entries(scInputs).forEach(([id, key]) => {
    document.getElementById(id).addEventListener('input', e => {
      scFilters[key] = e.target.value;
      scDebounceFetch();
    });
  });

  // Hedged checkbox
  document.getElementById('sc-hedged').addEventListener('change', e => {
    scFilters.hedged = e.target.checked;
    scDebounceFetch();
  });

  // Clear all
  document.getElementById('sc-clear').addEventListener('click', () => {
    scFilters.exchange = ''; scFilters.assetClasses.clear(); scFilters.issuers.clear();
    scFilters.maxFee = 2; scFilters.minFum = '';
    scFilters.minRet = ''; scFilters.maxRet = '';
    scFilters.minYield = ''; scFilters.hedged = false;
    document.getElementById('sc-fee').value = 2;
    document.getElementById('sc-fee-val').textContent = '2.00%';
    ['sc-fum','sc-ret-min','sc-ret-max','sc-yield'].forEach(id =>
      document.getElementById(id).value = '');
    document.getElementById('sc-hedged').checked = false;
    document.querySelectorAll('#sc-asset-list input, #sc-issuer-list input').forEach(cb =>
      cb.checked = false);
    document.querySelectorAll('.sc-exch').forEach(b => {
      b.classList.remove('active','bg-green-600','text-white','border-green-600');
      b.classList.add('text-gray-600','border-gray-200');
    });
    document.querySelector('.sc-exch[data-exch=""]').classList.add(
      'active','bg-green-600','text-white','border-green-600');
    scFetch();
  });

  scFetch();
}

function scDebounceFetch() {
  clearTimeout(scTimer);
  scTimer = setTimeout(scFetch, 300);
}

async function scFetch() {
  const p = new URLSearchParams();
  if (scFilters.exchange) p.set('exchange', scFilters.exchange);
  if (scFilters.assetClasses.size === 1)
    p.set('asset_class', [...scFilters.assetClasses][0]);
  if (scFilters.issuers.size === 1)
    p.set('issuer', [...scFilters.issuers][0]);
  if (scFilters.maxFee < 2) p.set('max_fee', scFilters.maxFee);
  if (scFilters.minFum)  p.set('min_fum',       scFilters.minFum);
  if (scFilters.minRet)  p.set('min_return_1y',  scFilters.minRet);
  if (scFilters.maxRet)  p.set('max_return_1y',  scFilters.maxRet);
  if (scFilters.minYield) p.set('min_yield',     scFilters.minYield);
  if (scFilters.hedged)  p.set('fx_hedged',      '1');

  const d = await api('/api/v1/screener?' + p);
  const data = d.data || [];

  // Client-side multi-filter for multiple asset classes / issuers
  let rows = data;
  if (scFilters.assetClasses.size > 1)
    rows = rows.filter(e => scFilters.assetClasses.has(e.asset_class));
  if (scFilters.issuers.size > 1)
    rows = rows.filter(e => scFilters.issuers.has(e.issuer));

  scLastData = rows;
  document.getElementById('sc-count').textContent =
    rows.length + ' ETF' + (rows.length !== 1 ? 's' : '') + ' matching';
  document.getElementById('sc-result-count').textContent =
    rows.length.toLocaleString() + ' results';

  const tbody = document.getElementById('screener-table');
  const emptyEl = document.getElementById('sc-empty');
  if (!rows.length) {
    tbody.innerHTML = '';
    emptyEl.classList.remove('hidden');
  } else {
    emptyEl.classList.add('hidden');
    tbody.innerHTML = rows.map(e => `
      <tr class="cursor-pointer" data-code="${e.code}"
          onclick="showDetail('${e.code}');document.querySelector('.main-tab[data-view=list]').click()">
        <td class="px-3 py-2.5">
          <div class="font-bold text-gray-900">${e.code}</div>
          <div class="text-xs text-gray-400 truncate max-w-[160px]">${e.name || ''}</div>
          ${e.benchmark ? `<div class="text-xs text-indigo-400 truncate max-w-[160px]" title="${e.benchmark}">&#8594; ${e.benchmark}</div>` : ''}
        </td>
        <td class="px-3 py-2.5">${acChip(e.asset_class)}</td>
        <td class="px-3 py-2.5 text-xs text-gray-500 max-w-[100px] truncate">${e.issuer || '—'}</td>
        <td class="px-3 py-2.5 text-right font-mono">${money(e.current_price)}</td>
        <td class="px-3 py-2.5 text-right" title="${fumTip(e)}">${fmtFum(calcFum(e))}</td>
        <td class="px-3 py-2.5 text-right font-semibold ${pctCls(e.return_1y)}">${pct(e.return_1y)}</td>
        <td class="px-3 py-2.5 text-right text-gray-600">
          ${e.distribution_yield != null ? e.distribution_yield.toFixed(1) + '%' : '—'}
        </td>
        <td class="px-3 py-2.5 text-right text-gray-400">
          ${e.expense_ratio != null ? e.expense_ratio.toFixed(2) + '%' : '—'}
        </td>
      </tr>`).join('');
  }
}

/* ============================================================ COMPARE */
let compareLoaded = false;
let analyticsLoaded = false;
const cmpSet = new Set();
let cmpTimer;

function initCompare() {
  compareLoaded = true;
  const inp = document.getElementById('cmp-search');
  const dd  = document.getElementById('cmp-dropdown');

  inp.addEventListener('input', () => {
    clearTimeout(cmpTimer);
    const q = inp.value.trim();
    if (!q) { dd.classList.add('hidden'); return; }
    cmpTimer = setTimeout(async () => {
      const d = await api('/api/v1/search?q=' + encodeURIComponent(q));
      if (!d.results || !d.results.length) {
        dd.innerHTML = '<p class="p-3 text-sm text-gray-400">No results</p>';
        dd.classList.remove('hidden');
        return;
      }
      dd.innerHTML = d.results.slice(0, 8).map(r => `
        <div class="px-3 py-2 hover:bg-green-50 cursor-pointer flex justify-between items-center
                    border-b last:border-b-0 ${cmpSet.has(r.code) ? 'bg-green-50' : ''}"
             data-code="${r.code}" data-name="${r.name || ''}">
          <div>
            <span class="font-bold text-gray-900">${r.code}</span>
            <span class="text-gray-500 text-xs ml-2">${(r.name || '').slice(0,40)}</span>
          </div>
          <span class="text-xs text-gray-400 bg-gray-100 px-1.5 py-0.5 rounded">${r.exchange || 'ASX'}</span>
        </div>`).join('');
      dd.classList.remove('hidden');
      dd.querySelectorAll('[data-code]').forEach(node =>
        node.addEventListener('click', () => {
          const code = node.dataset.code;
          if (cmpSet.size >= 5 && !cmpSet.has(code)) {
            alert('Maximum 5 ETFs for comparison.'); return;
          }
          if (cmpSet.has(code)) cmpSet.delete(code);
          else cmpSet.add(code);
          dd.classList.add('hidden');
          inp.value = '';
          renderCmpChips();
          if (cmpSet.size >= 2) cmpFetch();
          else renderCmpHint();
          cmpFetchSimilar();
        })
      );
    }, 250);
  });

  document.addEventListener('click', e => {
    if (!e.target.closest('#cmp-search') && !e.target.closest('#cmp-dropdown'))
      dd.classList.add('hidden');
  });
}

function renderCmpChips() {
  const el = document.getElementById('cmp-chips');
  el.innerHTML = [...cmpSet].map(code => `
    <span class="inline-flex items-center gap-1 bg-green-100 text-green-800 text-xs font-semibold
                 px-2.5 py-1 rounded-full">
      ${code}
      <button onclick="cmpRemove('${code}')"
              class="ml-0.5 text-green-500 hover:text-green-800 font-bold text-sm leading-none">×</button>
    </span>`).join('');
}

function cmpRemove(code) {
  cmpSet.delete(code);
  renderCmpChips();
  if (cmpSet.size >= 2) cmpFetch();
  else renderCmpHint();
  cmpFetchSimilar();
}

function renderCmpHint() {
  document.getElementById('cmp-hint').classList.remove('hidden');
  document.getElementById('cmp-table-container').classList.add('hidden');
  document.getElementById('cmp-overlap-wrap').classList.add('hidden');
}

async function cmpFetch() {
  if (cmpSet.size < 2) return;
  const codes = [...cmpSet].join(',');
  const d = await api('/api/v1/compare?codes=' + codes);
  renderCmpTable(d.data || []);
  cmpFetchOverlap();
}

async function cmpFetchOverlap() {
  const wrap = document.getElementById('cmp-overlap-wrap');
  const badge = document.getElementById('cmp-overlap-badge');
  const coverageEl = document.getElementById('cmp-overlap-coverage');
  const bodyEl = document.getElementById('cmp-overlap-body');

  if (cmpSet.size < 2) {
    wrap.classList.add('hidden');
    return;
  }

  wrap.classList.remove('hidden');
  bodyEl.innerHTML = '<p class="text-sm text-gray-400 text-center py-4">Loading overlap…</p>';
  coverageEl.innerHTML = '';
  badge.textContent = '';

  const codes = [...cmpSet];
  const d = await api('/api/v1/compare/overlap?codes=' + codes.join(','));

  if (d.error || !d.overlap) {
    bodyEl.innerHTML = '<p class="text-sm text-gray-400 text-center py-4">Overlap data unavailable.</p>';
    return;
  }

  const { overlap, coverage, total_overlap_count } = d;

  if (total_overlap_count === 0) {
    badge.textContent = '';
    coverageEl.innerHTML = '';
    bodyEl.innerHTML = '<p class="text-sm text-gray-400 text-center py-4">No shared holdings found — these ETFs have no constituents in common, or holdings data is unavailable for one or more funds.</p>';
    return;
  }

  badge.textContent = total_overlap_count + ' shared holding' + (total_overlap_count !== 1 ? 's' : '');

  // Coverage pills: "VAS — 94.2% covered by shared holdings"
  coverageEl.innerHTML = codes.map(code => {
    const pct = coverage[code] != null ? coverage[code].toFixed(1) + '%' : '—';
    const col = coverage[code] >= 50 ? 'bg-green-50 text-green-700 border-green-200'
              : coverage[code] >= 20 ? 'bg-yellow-50 text-yellow-700 border-yellow-200'
              : 'bg-gray-50 text-gray-600 border-gray-200';
    return `<div class="border rounded-lg px-3 py-1.5 text-xs ${col}">
      <span class="font-bold">${code}</span>
      <span class="ml-1">${pct} in shared holdings</span>
    </div>`;
  }).join('');

  // Table header
  const maxWeightPerCode = {};
  codes.forEach(c => {
    maxWeightPerCode[c] = Math.max(...overlap.map(item => item.weights[c] || 0));
  });

  const thCols = codes.map(c =>
    `<th class="px-3 py-2 text-right text-xs font-semibold text-green-700 uppercase tracking-wide whitespace-nowrap">${c}</th>`
  ).join('');

  const rows = overlap.map((item, i) => {
    const bg = i % 2 === 0 ? 'bg-white' : 'bg-gray-50';
    // Badge if it's in all selected ETFs
    const inAll = item.etf_count === codes.length;
    const countBadge = inAll
      ? '<span class="ml-1.5 text-xs bg-green-100 text-green-700 px-1.5 py-0.5 rounded-full font-medium">all</span>'
      : (codes.length > 2
          ? `<span class="ml-1.5 text-xs bg-gray-100 text-gray-500 px-1.5 py-0.5 rounded-full">${item.etf_count}/${codes.length}</span>`
          : '');

    const weightCells = codes.map(c => {
      const w = item.weights[c];
      if (!w) return `<td class="px-3 py-2 text-right text-xs text-gray-300">—</td>`;
      const barPct = maxWeightPerCode[c] > 0 ? Math.round(w / maxWeightPerCode[c] * 64) : 0;
      return `<td class="px-3 py-2 text-right text-xs">
        <div class="flex items-center justify-end gap-1.5">
          <div class="h-1.5 rounded-full bg-green-200" style="width:${barPct}px;min-width:2px"></div>
          <span class="font-medium text-gray-700 tabular-nums">${w.toFixed(2)}%</span>
        </div>
      </td>`;
    }).join('');

    return `<tr class="${bg} hover:bg-green-50 transition-colors">
      <td class="px-3 py-2 text-xs text-gray-800 font-medium max-w-xs truncate">
        ${item.name}${countBadge}
      </td>
      ${weightCells}
    </tr>`;
  }).join('');

  bodyEl.innerHTML = `
    <div class="overflow-x-auto">
      <table class="w-full text-sm">
        <thead class="border-b border-gray-100">
          <tr>
            <th class="px-3 py-2 text-left text-xs font-semibold text-gray-400 uppercase tracking-wide">Holding</th>
            ${thCols}
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
    ${total_overlap_count > 60 ? `<p class="text-xs text-gray-400 text-center mt-3">Showing top 60 of ${total_overlap_count} shared holdings.</p>` : ''}`;
}

// Fetch similar ETFs for all codes in the compare set
async function cmpFetchSimilar() {
  const el = document.getElementById('cmp-similar-content');
  if (cmpSet.size === 0) {
    el.innerHTML = '<p class="text-sm text-gray-400 bg-white rounded-xl shadow-sm border border-gray-100 py-8 text-center">Add an ETF above to see similar alternatives based on portfolio holdings.</p>';
    return;
  }
  el.innerHTML = '<p class="text-sm text-gray-400 py-4 text-center">Loading suggestions…</p>';

  const results = await Promise.all([...cmpSet].map(code =>
    api('/api/v1/etfs/' + code + '/similar').then(d => ({ code, ...d }))
  ));

  // Merge results: for each source ETF we get same_class + other_class.
  // Aggregate per source asset class, deduplicating by taking max overlap.
  // bySourceAC = { acName: { same: Map<code, etfObj>, other: Map<code, etfObj> } }
  const bySourceAC = {};

  for (const res of results) {
    const ac = res.asset_class || 'Other';
    if (!bySourceAC[ac]) bySourceAC[ac] = { same: new Map(), other: new Map() };
    const bucket = bySourceAC[ac];

    for (const s of (res.same_class || [])) {
      if (cmpSet.has(s.code)) continue;
      const cur = bucket.same.get(s.code);
      if (!cur || s.overlap_pct > cur.overlap_pct) bucket.same.set(s.code, s);
    }
    for (const s of (res.other_class || [])) {
      if (cmpSet.has(s.code)) continue;
      const cur = bucket.other.get(s.code);
      if (!cur || s.overlap_pct > cur.overlap_pct) bucket.other.set(s.code, s);
    }
  }

  const acs = Object.keys(bySourceAC);
  if (acs.length === 0) {
    el.innerHTML = '<p class="text-sm text-gray-400 bg-white rounded-xl shadow-sm border border-gray-100 py-8 text-center">No similar ETFs found (holdings data may be unavailable).</p>';
    return;
  }

  function similarCard(s, dimmed) {
    const overlapColor = s.overlap_pct >= 50 ? 'bg-green-100 text-green-700'
                       : s.overlap_pct >= 20 ? 'bg-yellow-100 text-yellow-700'
                       : 'bg-gray-100 text-gray-500';
    return `
      <div class="bg-white border ${dimmed ? 'border-dashed border-gray-200 opacity-80' : 'border-gray-200'} rounded-xl p-4 hover:shadow-md hover:border-green-200 transition-all">
        <div class="flex justify-between items-start mb-1">
          <span class="font-bold text-green-700 text-base">${s.code}</span>
          <span class="text-xs ${overlapColor} font-semibold px-2 py-0.5 rounded-full">${s.overlap_pct.toFixed(0)}% overlap</span>
        </div>
        <p class="text-xs text-gray-500 mb-1 leading-snug">${(s.name || '').slice(0, 50)}</p>
        ${dimmed ? `<p class="text-xs text-gray-400 italic mb-1">${s.asset_class || ''}</p>` : ''}
        <div class="flex flex-wrap gap-x-3 gap-y-1 text-xs text-gray-500">
          ${s.issuer ? `<span>${s.issuer}</span>` : ''}
          ${calcFum(s) != null ? `<span class="text-gray-400">FUM: ${fmtFum(calcFum(s))}</span>` : ''}
          ${s.expense_ratio ? `<span class="text-gray-400">MER: ${s.expense_ratio.toFixed(2)}%</span>` : ''}
          ${s.return_1y != null ? `<span class="${pctCls(s.return_1y)}">${pct(s.return_1y)} 1Y</span>` : ''}
        </div>
        <button onclick="cmpAdd('${s.code}')"
                class="mt-3 text-xs text-green-600 hover:text-green-800 font-medium border border-green-200 hover:border-green-400 rounded-lg px-3 py-1 transition-colors">
          + Add to Compare
        </button>
      </div>`;
  }

  const html = acs.map(ac => {
    const { same, other } = bySourceAC[ac];
    const sameList  = [...same.values()].sort((a, b) => b.overlap_pct - a.overlap_pct).slice(0, 5);
    const otherList = [...other.values()].sort((a, b) => b.overlap_pct - a.overlap_pct).slice(0, 3);

    const sameCards  = sameList.map(s => similarCard(s, false)).join('');
    const otherCards = otherList.map(s => similarCard(s, true)).join('');
    const hasOther   = otherCards.length > 0;

    return `
      <div class="mb-6">
        <h4 class="text-sm font-semibold text-gray-600 mb-3 flex items-center gap-2">${acChip(ac)}</h4>
        ${sameList.length > 0
          ? `<div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3 mb-3">${sameCards}</div>`
          : `<p class="text-xs text-gray-400 italic mb-3">No same-class ETFs with holdings overlap found.</p>`}
        ${hasOther ? `
          <details class="mt-1">
            <summary class="text-xs text-gray-400 cursor-pointer hover:text-gray-600 select-none mb-2">
              ▸ Other asset classes with overlap
            </summary>
            <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3 mt-2">${otherCards}</div>
          </details>` : ''}
      </div>`;
  }).join('');

  el.innerHTML = html;
}

function cmpAdd(code) {
  if (cmpSet.size >= 5) { alert('Maximum 5 ETFs for comparison.'); return; }
  cmpSet.add(code);
  renderCmpChips();
  if (cmpSet.size >= 2) cmpFetch();
  cmpFetchSimilar();
}

function miniBar(val, maxVal, neg) {
  if (val == null || maxVal == 0) return '—';
  const w = Math.max(4, Math.round(Math.abs(val) / maxVal * 80));
  const cls = neg && val < 0 ? 'cmp-bar cmp-bar-neg' : 'cmp-bar';
  return `<div class="cmp-bar-wrap">
    <div class="${cls}" style="width:${w}px"></div>
    <span class="${neg ? pctCls(val) : 'text-gray-700'} text-xs font-semibold whitespace-nowrap">
      ${neg ? pct(val) : val.toFixed(2) + '%'}
    </span>
  </div>`;
}

function renderCmpTable(etfs) {
  if (!etfs.length) return;
  document.getElementById('cmp-hint').classList.add('hidden');
  document.getElementById('cmp-table-container').classList.remove('hidden');

  const maxFee   = Math.max(...etfs.map(e => e.expense_ratio || 0));
  const maxYield = Math.max(...etfs.map(e => e.distribution_yield || 0));
  const maxRet1y = Math.max(...etfs.map(e => Math.abs(e.return_1y || 0)));

  const rows = [
    { label: 'Name',          fmt: e => `<span class="font-medium text-gray-800 text-xs">${e.name || '—'}</span>` },
    { label: 'Issuer',        fmt: e => e.issuer || '—' },
    { label: 'Exchange',      fmt: e => `<span class="badge-${(e.exchange||'asx').toLowerCase()} px-1.5 py-0.5 rounded text-xs font-medium">${e.exchange || '—'}</span>` },
    { label: 'Asset Class',   fmt: e => acChip(e.asset_class) },
    { label: 'Benchmark',     fmt: e => e.benchmark ? `<span class="text-xs text-indigo-700">${e.benchmark}</span>` : '—' },
    { label: 'CHESS FUM',     fmt: e => chessFum(e) != null ? `<span title="CHESS units on issue × last price">${fmtFum(chessFum(e))}</span>` : '<span class="text-gray-300">—</span>' },
    { label: 'Total FUM',     fmt: e => `<span title="ASX Monthly Report">${fmtFum(e.fund_size_aud_millions)}</span>` },
    { label: 'Mgmt Fee',      fmt: e => miniBar(e.expense_ratio, maxFee, false), bar: true },
    { label: 'Dist. Yield',   fmt: e => miniBar(e.distribution_yield, maxYield, false), bar: true },
    { label: '1M Return',     fmt: e => `<span class="${pctCls(e.return_1m)}">${pct(e.return_1m)}</span>` },
    { label: '1Y Return',     fmt: e => miniBar(e.return_1y, maxRet1y, true), bar: true },
    { label: '3Y Return',     fmt: e => `<span class="${pctCls(e.return_3y)}">${pct(e.return_3y)}</span>` },
    { label: '5Y Return',     fmt: e => `<span class="${pctCls(e.return_5y)}">${pct(e.return_5y)}</span>` },
    { label: 'Price',         fmt: e => money(e.current_price) },
    { label: '52W High',      fmt: e => money(e.year_high) },
    { label: '52W Low',       fmt: e => money(e.year_low) },
    { label: 'Spread',        fmt: e => e.bid_ask_spread_pct != null ? e.bid_ask_spread_pct + '%' : '—' },
    { label: 'Inception',     fmt: e => e.inception_date || '—' },
    { label: 'FX Hedged',     fmt: e => e.fx_hedged ? '<span class="text-emerald-600 font-medium">Yes</span>' : '<span class="text-gray-400">No</span>' },
    { label: 'Sec. Lending',  fmt: e => e.securities_lending ? '<span class="text-emerald-600 font-medium">Yes</span>' : '<span class="text-gray-400">No</span>' },
  ];

  const thCols = etfs.map(e => `
    <th class="px-4 py-3 text-center min-w-[140px]">
      <div class="font-bold text-green-700 text-base">${e.code}</div>
      <button onclick="cmpRemove('${e.code}')"
              class="text-xs text-gray-400 hover:text-red-500 mt-0.5">Remove</button>
    </th>`).join('');

  const bodyRows = rows.map((row, i) => {
    const bg = i % 2 === 0 ? 'bg-white' : 'bg-gray-50';
    const cells = etfs.map(e =>
      `<td class="px-4 py-2.5 text-sm text-center text-gray-700">${row.fmt(e)}</td>`
    ).join('');
    return `<tr class="${bg}">
      <td class="px-4 py-2.5 text-xs font-semibold text-gray-500 uppercase tracking-wide
                 bg-gray-50 border-r border-gray-100 whitespace-nowrap w-28">${row.label}</td>
      ${cells}
    </tr>`;
  }).join('');

  document.getElementById('cmp-table').innerHTML = `
    <thead class="bg-gray-50 border-b border-gray-200">
      <tr>
        <th class="px-4 py-3 text-left text-xs font-semibold text-gray-400 uppercase w-28">Metric</th>
        ${thCols}
      </tr>
    </thead>
    <tbody>${bodyRows}</tbody>`;
}

/* ============================================================ HOLDINGS SEARCH */
let hsTimer;
document.getElementById('hs-input').addEventListener('input', hsDebounce);
document.getElementById('hs-min-weight').addEventListener('input', hsDebounce);

function hsDebounce() {
  clearTimeout(hsTimer);
  hsTimer = setTimeout(hsFetch, 400);
}

async function hsFetch() {
  const q = document.getElementById('hs-input').value.trim();
  const minW = document.getElementById('hs-min-weight').value || '0';
  if (!q) {
    document.getElementById('hs-empty').classList.remove('hidden');
    document.getElementById('hs-table-wrap').classList.add('hidden');
    document.getElementById('hs-summary').classList.add('hidden');
    return;
  }

  const d = await api('/api/v1/holdings/search?q=' + encodeURIComponent(q)
                      + '&min_weight=' + encodeURIComponent(minW));

  const sumEl = document.getElementById('hs-summary');
  const tblWrap = document.getElementById('hs-table-wrap');
  const emptyEl = document.getElementById('hs-empty');

  if (!d.results || !d.results.length) {
    sumEl.classList.add('hidden');
    tblWrap.classList.add('hidden');
    emptyEl.textContent = `No ETF holdings match "${q}".`;
    emptyEl.classList.remove('hidden');
    return;
  }

  emptyEl.classList.add('hidden');
  sumEl.classList.remove('hidden');
  sumEl.innerHTML = `Found <strong>${d.total}</strong> holding${d.total !== 1 ? 's' : ''} across
    <strong>${d.etf_count}</strong> ETF${d.etf_count !== 1 ? 's' : ''}
    &nbsp;·&nbsp; ${d.etfs.map(c => `<span class="font-mono font-bold">${c}</span>`).join(', ')}`;

  tblWrap.classList.remove('hidden');
  const maxW = Math.max(...d.results.map(r => r.weight_pct || 0));
  document.getElementById('holdings-table').innerHTML = d.results.map(r => `
    <tr class="cursor-pointer" onclick="showDetail('${r.etf_code}');document.querySelector('.main-tab[data-view=list]').click()">
      <td class="px-3 py-2.5 font-bold text-green-700">${r.etf_code}</td>
      <td class="px-3 py-2.5 text-gray-600 max-w-[160px] truncate text-xs">${r.etf_name || ''}</td>
      <td class="px-3 py-2.5 font-medium text-gray-800 max-w-[180px] truncate">${r.holding_name || ''}</td>
      <td class="px-3 py-2.5 font-mono text-xs text-gray-500">${r.ticker || '—'}</td>
      <td class="px-3 py-2.5 text-right">
        <div class="flex items-center justify-end gap-2">
          <div class="w-16 bg-gray-100 rounded-full h-1.5 overflow-hidden">
            <div class="h-full bg-green-400 rounded-full"
                 style="width:${maxW > 0 ? (r.weight_pct / maxW * 100).toFixed(1) : 0}%"></div>
          </div>
          <span class="font-bold text-green-700 text-xs w-12 text-right">
            ${r.weight_pct != null ? r.weight_pct.toFixed(2) + '%' : '—'}
          </span>
        </div>
      </td>
      <td class="px-3 py-2.5 text-xs text-gray-500 max-w-[120px] truncate">${r.sector || '—'}</td>
      <td class="px-3 py-2.5">${acChip(r.asset_class)}</td>
    </tr>`).join('');
}

/* ============================================================ ANALYTICS */
async function initAnalytics() {
  analyticsLoaded = true;
  const [performers, yieldData, cheapest, flows] = await Promise.all([
    api('/api/v1/analytics/top-performers?limit=15'),
    api('/api/v1/analytics/highest-yield?limit=15'),
    api('/api/v1/analytics/cheapest?limit=15'),
    api('/api/v1/analytics/fund-flows?limit=10'),
  ]);

  function leaderRow(code, name, valueHtml, rank) {
    return `<div class="px-4 py-2.5 flex items-center gap-3 hover:bg-slate-50 cursor-pointer"
         onclick="showDetail('${code}');document.querySelector('.main-tab[data-view=list]').click()">
      <span class="text-gray-300 font-mono text-xs w-5 text-right shrink-0">${rank}</span>
      <div class="flex-1 min-w-0">
        <div class="font-bold text-gray-900 text-sm">${code}</div>
        <div class="text-xs text-gray-400 truncate">${name || ''}</div>
      </div>
      ${valueHtml}
    </div>`;
  }

  document.getElementById('an-performers').innerHTML =
    (performers.top_performers || []).map((e, i) =>
      leaderRow(e.code, e.name,
        `<span class="font-semibold text-sm shrink-0 ${pctCls(e.return_1y)}">${pct(e.return_1y)}</span>`,
        i + 1)
    ).join('');

  document.getElementById('an-yield').innerHTML =
    (yieldData.highest_yield || []).map((e, i) =>
      leaderRow(e.code, e.name,
        `<span class="font-semibold text-sm shrink-0 text-emerald-600">${e.distribution_yield != null ? e.distribution_yield.toFixed(1) + '%' : '—'}</span>`,
        i + 1)
    ).join('');

  document.getElementById('an-cheapest').innerHTML =
    (cheapest.cheapest || []).map((e, i) =>
      leaderRow(e.code, e.name,
        `<span class="font-semibold text-sm shrink-0 text-gray-700">${e.expense_ratio != null ? e.expense_ratio.toFixed(2) + '%' : '—'}</span>`,
        i + 1)
    ).join('');

  const allFlows = [...(flows.top_inflows || []), ...(flows.top_outflows || [])];
  const maxFlow = Math.max(...allFlows.map(r => Math.abs(r.fund_flow_1m || 0)), 1);

  function flowRow(r, dir) {
    const col   = dir === 'in' ? issuerColor(r.issuer) : '#ef4444';
    const barW  = (Math.abs(r.fund_flow_1m || 0) / maxFlow * 100).toFixed(1);
    return `<div class="flex items-center gap-2 cursor-pointer hover:bg-slate-50 px-2 py-1.5 rounded group"
         onclick="showDetail('${r.code}');document.querySelector('.main-tab[data-view=list]').click()">
      <div class="min-w-0 w-24 shrink-0">
        <div class="font-bold text-gray-900 text-xs">${r.code}</div>
        <div class="text-[10px] text-gray-400 truncate">${r.issuer || ''}</div>
      </div>
      <div class="flex-1 bg-gray-100 rounded-full h-2 overflow-hidden">
        <div class="h-full rounded-full transition-all" style="background:${col};width:${barW}%"></div>
      </div>
      <span class="text-xs font-semibold w-16 text-right shrink-0" style="color:${col}">${fmtFum(r.fund_flow_1m)}</span>
    </div>`;
  }

  document.getElementById('an-inflows').innerHTML =
    (flows.top_inflows || []).slice(0, 8).map(r => flowRow(r, 'in')).join('');
  document.getElementById('an-outflows').innerHTML =
    (flows.top_outflows || []).slice(0, 8).map(r => flowRow(r, 'out')).join('');
}

/* ============================================================ CSV EXPORT */
function scExportCSV() {
  if (!scLastData.length) return;
  const headers = ['Code','Name','Asset Class','Issuer','Price (AUD)','FUM (AUD M)',
                   '1Y Return (%)','Yield (%)','MER (%)'];
  const escape = v => '"' + String(v || '').replace(/"/g, '""') + '"';
  const csvRows = [headers.join(',')];
  scLastData.forEach(e => {
    csvRows.push([
      e.code,
      escape(e.name),
      escape(e.asset_class),
      escape(e.issuer),
      e.current_price         != null ? e.current_price.toFixed(2)          : '',
      calcFum(e) != null ? calcFum(e).toFixed(1) : '',
      e.return_1y             != null ? e.return_1y.toFixed(2)              : '',
      e.distribution_yield    != null ? e.distribution_yield.toFixed(2)     : '',
      e.expense_ratio         != null ? e.expense_ratio.toFixed(2)          : '',
    ].join(','));
  });
  const blob = new Blob([csvRows.join('\n')], { type: 'text/csv' });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href     = url;
  a.download = 'etf-screener-' + new Date().toISOString().slice(0, 10) + '.csv';
  a.click();
  URL.revokeObjectURL(url);
}
document.getElementById('sc-export').addEventListener('click', scExportCSV);

/* ============================================================ COLUMN TOGGLES */
document.getElementById('col-3y').addEventListener('change', function () {
  document.querySelectorAll('.col-3y').forEach(el => el.classList.toggle('hidden', !this.checked));
});
document.getElementById('col-5y').addEventListener('change', function () {
  document.querySelectorAll('.col-5y').forEach(el => el.classList.toggle('hidden', !this.checked));
});

/* ============================================================ ARTICLE CAROUSEL */
function carouselScroll(dir) {
  const el = document.getElementById('article-carousel');
  el.scrollBy({ left: dir * 280, behavior: 'smooth' });
}

/* ============================================================ KEYBOARD SHORTCUT */
document.addEventListener('keydown', e => {
  if (e.key === '/' && !e.ctrlKey && !e.metaKey &&
      !['INPUT', 'TEXTAREA', 'SELECT'].includes(document.activeElement.tagName)) {
    e.preventDefault();
    document.getElementById('search').focus();
  }
});
</script>
</body>
</html>'''


# ===================================================================== articles

def _articles_head(title):
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title} — Australian ETF Market</title>
<script src="https://cdn.tailwindcss.com"></script>
<style>
  body {{ font-family: Inter, system-ui, -apple-system, sans-serif; background: #f8fafc; color: #1e293b; }}
  article h2 {{ font-size: 1.15rem; font-weight: 700; margin: 1.5rem 0 .6rem; color: #1e293b; }}
  article p  {{ margin-bottom: 1rem; line-height: 1.7; font-size: .9rem; color: #374151; }}
  article table {{ width: 100%; border-collapse: collapse; margin: 1rem 0; font-size: .82rem; }}
  article th {{ text-align: left; padding: .4rem .7rem; background: #f1f5f9; border-bottom: 1px solid #e2e8f0;
                font-size: .7rem; font-weight: 700; text-transform: uppercase; letter-spacing: .04em; color: #64748b; }}
  article td {{ padding: .4rem .7rem; border-bottom: 1px solid #f1f5f9; vertical-align: middle; }}
  article tr:last-child td {{ border-bottom: none; }}
  article tr:hover td {{ background: #f8fafc; }}
  .pos {{ color: #16a34a; font-weight: 600; }}
  .neg {{ color: #dc2626; font-weight: 600; }}
</style>
</head>"""


def _articles_nav(active_slug=None):
    return """
<header class="bg-gradient-to-r from-green-900 to-green-700 shadow-xl">
  <div class="max-w-4xl mx-auto px-5 py-3 flex items-center gap-4">
    <a href="/dashboard" class="text-white/70 hover:text-white text-sm flex items-center gap-1.5">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="15 18 9 12 15 6"/></svg>
      Dashboard
    </a>
    <span class="text-white/30">|</span>
    <a href="/articles" class="text-white font-semibold text-sm">Articles</a>
  </div>
</header>"""


def _handle_articles_list(self):
    from articles import get_all_articles
    articles = get_all_articles()

    CAT_COLORS = {
        'Performance':    ('bg-green-100',  'text-green-800'),
        'Market Trends':  ('bg-blue-100',   'text-blue-800'),
        'Thematic':       ('bg-purple-100', 'text-purple-800'),
        'Research':       ('bg-amber-100',  'text-amber-800'),
        'Education':      ('bg-teal-100',   'text-teal-800'),
        'Issuer Profile': ('bg-orange-100', 'text-orange-800'),
    }

    NEWS_CATS    = {'Performance', 'Market Trends', 'Thematic', 'Research'}
    BASICS_CATS  = {'Education'}
    ISSUER_CATS  = {'Issuer Profile'}

    BASICS_ORDER = [
        'what-is-an-etf', 'what-is-an-index', 'etf-costs-explained',
        'passive-vs-active-etfs', 'what-is-active-management',
        'what-is-a-market-maker', 'building-a-portfolio-with-etfs',
    ]

    news_arts   = [a for a in articles if a['category'] in NEWS_CATS]
    basics_arts = sorted(
        [a for a in articles if a['category'] in BASICS_CATS],
        key=lambda a: BASICS_ORDER.index(a['slug']) if a['slug'] in BASICS_ORDER else 99
    )
    issuer_arts = [a for a in articles if a['category'] in ISSUER_CATS]

    def card(a, wide=False):
        bg, fg = CAT_COLORS.get(a['category'], ('bg-gray-100', 'text-gray-800'))
        cols = 'sm:col-span-2' if wide else ''
        return f"""
    <a href="/articles/{a['slug']}" class="block {cols} bg-white rounded-xl border border-gray-100 shadow-sm hover:shadow-md transition-shadow p-5">
      <span class="inline-block {bg} {fg} text-xs font-semibold px-2 py-0.5 rounded mb-2">{a['category']}</span>
      <h2 class="text-base font-bold text-gray-900 leading-snug mb-1">{a['title']}</h2>
      <p class="text-sm text-gray-500 line-clamp-2">{a['subtitle']}</p>
      <p class="text-xs text-gray-400 mt-3">{a['date']}</p>
    </a>"""

    def section(heading, subheading, arts, cols=2, wide_first=False):
        if not arts:
            return ''
        grid_cols = f'grid-cols-1 sm:grid-cols-{cols}'
        cards_html = ''.join(card(a, wide=(i == 0 and wide_first and cols == 2)) for i, a in enumerate(arts))
        return f"""
  <section class="mb-10">
    <div class="mb-4">
      <h2 class="text-lg font-bold text-gray-900">{heading}</h2>
      <p class="text-sm text-gray-500 mt-0.5">{subheading}</p>
    </div>
    <div class="grid {grid_cols} gap-4">{cards_html}</div>
  </section>"""

    news_html   = section('Latest Analysis', 'Market data, performance and thematic coverage.',
                           news_arts, cols=2, wide_first=True)
    basics_html = section('Learn the Basics', 'Everything you need to know about how ETFs work.',
                           basics_arts, cols=3)
    issuer_html = section('Issuer Profiles', 'Background, size and product range for each major ETF provider.',
                           issuer_arts, cols=2)

    html = _articles_head('Articles') + _articles_nav() + f"""
<body class="bg-slate-100 min-h-screen">
<main class="max-w-5xl mx-auto px-5 py-8">
  <h1 class="text-2xl font-bold text-gray-900 mb-1">Articles</h1>
  <p class="text-sm text-gray-500 mb-8">Analysis, education and issuer profiles for the Australian ETF market.</p>
  {news_html}
  {basics_html}
  {issuer_html}
</main>
</body>
</html>"""
    self.send_html(html)


def _handle_article_detail(self, slug):
    from articles import get_article
    a = get_article(slug)
    if not a:
        self.send_json({'error': 'Article not found'}, 404)
        return

    category_colors = {
        'Performance':    ('bg-green-100',  'text-green-800'),
        'Market Trends':  ('bg-blue-100',   'text-blue-800'),
        'Thematic':       ('bg-purple-100', 'text-purple-800'),
        'Research':       ('bg-amber-100',  'text-amber-800'),
        'Education':      ('bg-teal-100',   'text-teal-800'),
        'Issuer Profile': ('bg-orange-100', 'text-orange-800'),
    }
    bg, fg = category_colors.get(a['category'], ('bg-gray-100', 'text-gray-800'))

    html = _articles_head(a['title']) + _articles_nav(slug) + f"""
<body class="bg-slate-100 min-h-screen">
<main class="max-w-4xl mx-auto px-5 py-8">
  <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-7">
    <span class="inline-block {bg} {fg} text-xs font-semibold px-2 py-0.5 rounded mb-3">{a['category']}</span>
    <h1 class="text-2xl font-bold text-gray-900 leading-tight mb-2">{a['title']}</h1>
    <p class="text-gray-500 text-sm mb-1">{a['subtitle']}</p>
    <p class="text-xs text-gray-400 mb-6">{a['date']}</p>
    <article class="prose max-w-none">
      {a['body']}
    </article>
  </div>
  <div class="mt-5">
    <a href="/articles" class="text-sm text-green-600 hover:text-green-800 flex items-center gap-1">
      <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="15 18 9 12 15 6"/></svg>
      Back to all articles
    </a>
  </div>
</main>
</body>
</html>"""
    self.send_html(html)


def _handle_screener_preferences_page(self):
    from screener_preferences import SCREENER_HTML
    self.send_html(SCREENER_HTML)


def _handle_screener_preferences_api(self):
    import screener_preferences as sp
    try:
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length) if length else b'{}'
        prefs = json.loads(body.decode('utf-8'))
    except Exception:
        self.send_json({'error': 'Invalid JSON body'}, 400)
        return
    try:
        portfolio = sp.build_portfolio(prefs, DB_PATH)
        self.send_json(portfolio)
    except Exception as e:
        self.send_json({'error': str(e)}, 500)


# Attach to the real handler class (defined earlier in the file)
ETFAPIHandler.handle_articles_list = _handle_articles_list
ETFAPIHandler.handle_article_detail = _handle_article_detail
ETFAPIHandler.handle_screener_preferences_page = _handle_screener_preferences_page
ETFAPIHandler.handle_screener_preferences_api = _handle_screener_preferences_api


# ===================================================================== main
def run_server(port=None):
    if port is None:
        port = int(os.getenv('PORT') or os.getenv('ETF_PORT', '8081'))

    # Start the background PCF scheduler (daemon thread — exits with server)
    try:
        from scheduler import start_scheduler
        start_scheduler()
    except Exception as e:
        print(f"[warn] Scheduler failed to start: {e}")

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
