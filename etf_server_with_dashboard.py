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


def slugify(name: str) -> str:
    """Convert an issuer name to a URL slug."""
    import re as _re
    s = name.lower()
    s = _re.sub(r'\s*/\s*', '-', s)
    s = s.replace(' & ', '-').replace('&', '-')
    s = s.replace('.', '').replace("'", '')
    s = s.replace(' ', '-')
    s = _re.sub(r'[^a-z0-9-]', '', s)
    s = _re.sub(r'-+', '-', s)
    return s.strip('-')


def estimate_asset_class(name, fund_type='', benchmark='', issuer=''):
    """Keyword-based asset class estimator for ETFs without a stored classification."""
    text = ' '.join([str(s) for s in [name, fund_type, benchmark, issuer] if s]).upper()
    clean = (text.replace('(AUD HEDGED)', '').replace('AUD HEDGED', '')
             .replace('(HEDGED)', '').replace(' HEDGED', ''))
    if any(k in clean for k in ['BOND', 'FIXED INCOME', 'CREDIT', 'DEBT', 'YIELD', 'TREASURY',
            'GOVERNMENT', 'CORPORATE', 'INFLATION', 'DURATION', 'INTEREST RATE',
            'BDC', 'INCOME FUND', 'RMBS', 'MORTGAGE', 'SUBORDINATED',
            'HIGH YIELD', 'CASH PLUS', ' AAA ', 'SHORT TERM ACTIVE YIELD']):
        return 'Fixed Income'
    if any(k in clean for k in ['CASH ETF', 'MONEY MARKET', 'CASH FUND', 'SHORT TERM CASH']):
        return 'Money Market'
    if any(k in clean for k in ['GOLD', 'SILVER', 'PLATINUM', 'PALLADIUM', 'COPPER', 'OIL', 'GAS',
            'COMMODITY', 'COMMODITIES', 'METAL', 'MINER', 'URANIUM', 'AGRICULTURE', 'WHEAT', 'CRUDE']):
        return 'Commodity'
    if any(k in clean for k in ['MULTI-ASSET', 'MULTI ASSET', 'DIVERSIFIED', 'BALANCED']):
        return 'Mixed Allocation'
    if any(k in clean for k in ['ABSOLUTE RETURN', 'LONG SHORT', 'LONG/SHORT', 'HEDGE FUND',
            'ALPHA PLUS', 'PRIVATE EQUITY', 'PRIVATE CREDIT', 'MARKET NEUTRAL']):
        return 'Alternative'
    if any(k in clean for k in ['SHARE', 'EQUITY', 'STOCK', 'S&P', 'MSCI', 'NASDAQ', 'DOW',
            'RUSSELL', 'NIKKEI', 'FTSE', 'INDEX FUND', 'INDEX ETF', 'LISTED PROPERTY', 'REIT',
            'REAL ESTATE', 'TECHNOLOGY', 'HEALTHCARE', 'FINANCIAL', 'CONSUMER',
            'GROWTH FUND', 'VALUE FUND', 'MOMENTUM', 'SMALL CAP', 'MID CAP', 'SMALL COMPANIES', 'SMID']):
        aus = any(k in clean for k in ['AUSTRALIA', 'AUSTRALIAN', ' ASX', 'A200'])
        return 'Australian Equities' if aus else 'International Equities'
    if 'FIRETRAIL' in clean:
        return 'Australian Equities'
    return None


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
            '/api/v1/holdings/search':         lambda: self.handle_holdings_search(qs),
            '/api/v1/holdings/concentration':  self.handle_holdings_concentration,
            # History API
            '/api/v1/history/industry':      self.handle_history_industry,
            '/api/v1/history/issuers':       self.handle_history_issuers,
            '/api/v1/history/asset-classes': self.handle_history_asset_classes,
            '/api/v1/history/flows':         lambda: self.handle_history_flows(qs),
            '/api/v1/history/launches':      self.handle_history_launches,
            '/api/v1/history/geography':     self.handle_history_geography,
            '/api/v1/history/strategy':      self.handle_history_strategy,
            # Insights API
            '/api/v1/insights/fum':      self.handle_insights_fum,
            '/api/v1/insights/listings': self.handle_insights_listings,
            '/api/v1/insights/returns':  self.handle_insights_returns,
            '/api/v1/insights/expense':  self.handle_insights_expense,
            '/api/v1/insights/issuers':  self.handle_insights_issuers,
            '/api/v1/insights/nav':          self.handle_insights_nav,
            '/api/v1/insights/upcoming':     self.handle_insights_upcoming,
            '/api/v1/insights/asset-classes': self.handle_insights_asset_classes,
            # Insights pages
            '/insights/fum':      lambda: self.handle_insights_page('fum'),
            '/insights/listings': lambda: self.handle_insights_page('listings'),
            '/insights/returns':  lambda: self.handle_insights_page('returns'),
            '/insights/expense':  lambda: self.handle_insights_page('expense'),
            '/insights/issuers':  lambda: self.handle_insights_page('issuers'),
            '/insights/nav':      lambda: self.handle_insights_page('nav'),
            '/insights/upcoming':      lambda: self.handle_insights_page('upcoming'),
            '/insights/asset-classes': lambda: self.handle_insights_page('asset-classes'),
            '/insights/total-market':  lambda: self.handle_insights_page('total-market'),
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

        # History ETF detail: /api/v1/history/etf/{code}
        m2 = re.match(r'^/api/v1/history/etf/([A-Za-z0-9]+)$', path)
        if m2:
            self.handle_history_etf(m2.group(1).upper())
            return

        # Issuer page: /issuers/{slug}
        m_iss = re.match(r'^/issuers/([a-z0-9\-]+)$', path)
        if m_iss:
            self.handle_issuer_page(m_iss.group(1))
            return

        # Issuer API: /api/v1/issuers/{slug}
        m_iss_api = re.match(r'^/api/v1/issuers/([a-z0-9\-]+)$', path)
        if m_iss_api:
            self.handle_issuer_api(m_iss_api.group(1))
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
            max_fee      = self._str(qs, 'max_fee')
            min_fum      = self._str(qs, 'min_fum')
            min_ret_1y   = self._str(qs, 'min_return_1y')
            max_ret_1y   = self._str(qs, 'max_return_1y')
            min_yield    = self._str(qs, 'min_yield')
            fx_hedged    = self._str(qs, 'fx_hedged')

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
            if max_fee is not None:
                try: where.append("expense_ratio <= ?"); params.append(float(max_fee))
                except (ValueError, TypeError): pass
            if min_fum is not None:
                try: where.append("fund_size_aud_millions >= ?"); params.append(float(min_fum))
                except (ValueError, TypeError): pass
            if min_ret_1y is not None:
                try: where.append("return_1y >= ?"); params.append(float(min_ret_1y))
                except (ValueError, TypeError): pass
            if max_ret_1y is not None:
                try: where.append("return_1y <= ?"); params.append(float(max_ret_1y))
                except (ValueError, TypeError): pass
            if min_yield is not None:
                try: where.append("distribution_yield >= ?"); params.append(float(min_yield))
                except (ValueError, TypeError): pass
            if fx_hedged and fx_hedged.lower() in ('1', 'true', 'yes'):
                where.append("fx_hedged = 1")

            sort_field_map = {
                'fum':       ('fund_size_aud_millions', 'DESC'),
                'chess_fum': ('units_on_issue * current_price / 1e6', 'DESC'),
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
            # Latest etp_monthly snapshot for CHESS data + cross-reference AUM
            m_row = conn.execute(
                "SELECT chess_mc, chess_units, chess_holders, total_units, market_cap, "
                "transacted_value, num_trades, spread_pct "
                "FROM etp_monthly WHERE code=? ORDER BY date DESC LIMIT 1",
                (code,)
            ).fetchone()
            if m_row:
                mr = dict(m_row)
                if mr.get('chess_mc'):       d['chess_mc']       = mr['chess_mc']
                if mr.get('chess_units'):    d['chess_units']     = mr['chess_units']
                if mr.get('chess_holders'):  d['chess_holders']   = mr['chess_holders']
                if not d.get('units_on_issue') and mr.get('total_units'):
                    d['units_on_issue'] = mr['total_units']
            # Fall back to estimated asset class if none stored
            if not d.get('asset_class'):
                d['asset_class'] = estimate_asset_class(
                    d.get('name', ''), d.get('fund_type', ''), '', d.get('issuer', ''))
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
            meta = conn.execute(
                "SELECT holdings_disclosure, holdings_as_of FROM etfs WHERE code = ?", (code,)
            ).fetchone()
            resp = {'code': code, 'holdings': [dict(r) for r in rows]}
            if meta:
                resp['holdings_disclosure'] = meta['holdings_disclosure']
                resp['holdings_as_of'] = meta['holdings_as_of']
            self.send_json(resp)
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
                """
                SELECT
                    i.name,
                    i.website,
                    i.etf_count,
                    i.total_fum,
                    i.last_updated,
                    ROUND(
                        100.0 * i.total_fum / NULLIF((SELECT SUM(fund_size_aud_millions) FROM etfs WHERE fund_size_aud_millions > 0), 0),
                        2
                    ) AS market_share_pct,
                    (SELECT AVG(management_fee)
                     FROM etfs e2
                     WHERE e2.issuer = i.name AND e2.management_fee IS NOT NULL) AS avg_mer,
                    (SELECT SUM(fund_flow_1m)
                     FROM etfs e3
                     WHERE e3.issuer = i.name AND e3.fund_flow_1m IS NOT NULL) AS fund_flow_1m
                FROM issuers i
                WHERE i.etf_count > 0
                ORDER BY i.total_fum DESC
                """
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
                       COALESCE(SUM(CASE WHEN units_on_issue IS NOT NULL AND current_price IS NOT NULL
                                         THEN units_on_issue * current_price / 1e6
                                         ELSE fund_size_aud_millions END),0) as chess_fum_millions,
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

            largest_etf_row = conn.execute(
                "SELECT code, name, fund_size_aud_millions FROM etfs "
                "WHERE fund_size_aud_millions IS NOT NULL "
                "ORDER BY fund_size_aud_millions DESC LIMIT 1"
            ).fetchone()

            largest_issuer_fum_row = conn.execute(
                "SELECT issuer as name, SUM(fund_size_aud_millions) as total_fum "
                "FROM etfs WHERE fund_size_aud_millions IS NOT NULL AND issuer IS NOT NULL "
                "GROUP BY issuer ORDER BY total_fum DESC LIMIT 1"
            ).fetchone()

            total_fum_val = stats['total_fum_millions'] or 1
            largest_issuer_fum = None
            if largest_issuer_fum_row:
                share = round((largest_issuer_fum_row['total_fum'] / total_fum_val) * 100, 1)
                largest_issuer_fum = {
                    'name': largest_issuer_fum_row['name'],
                    'total_fum': largest_issuer_fum_row['total_fum'],
                    'market_share_pct': share,
                }

            most_etfs_issuer_row = conn.execute(
                "SELECT issuer as name, COUNT(*) as etf_count FROM etfs "
                "WHERE issuer IS NOT NULL GROUP BY issuer ORDER BY etf_count DESC LIMIT 1"
            ).fetchone()

            fum_weighted_mer_row = conn.execute(
                "SELECT SUM(fund_size_aud_millions * COALESCE(expense_ratio, management_fee)) / "
                "SUM(fund_size_aud_millions) as fwm FROM etfs "
                "WHERE fund_size_aud_millions > 0 "
                "AND COALESCE(expense_ratio, management_fee) > 0"
            ).fetchone()

            try:
                upcoming_count_row = conn.execute(
                    "SELECT COUNT(*) FROM upcoming_listings"
                ).fetchone()
                upcoming_count = upcoming_count_row[0] if upcoming_count_row else 0
            except Exception:
                upcoming_count = 0

            exchange_counts = conn.execute(
                "SELECT exchange, COUNT(*) as cnt FROM etfs "
                "WHERE exchange IS NOT NULL GROUP BY exchange"
            ).fetchall()
            asx_count = 0
            cxa_count = 0
            for row in exchange_counts:
                if row['exchange'] and row['exchange'].upper() == 'ASX':
                    asx_count = row['cnt']
                elif row['exchange'] and row['exchange'].upper() == 'CXA':
                    cxa_count = row['cnt']

            issuer_count_row = conn.execute(
                "SELECT COUNT(DISTINCT issuer) FROM etfs WHERE issuer IS NOT NULL"
            ).fetchone()
            issuer_count = issuer_count_row[0] if issuer_count_row else 0

            self.send_json({
                'total_fum_millions': stats['total_fum_millions'],
                'chess_fum_millions': stats['chess_fum_millions'],
                'total_etfs': stats['total_etfs'],
                'total_issuers': issuer_count,
                'avg_return_1y': round(stats['avg_return_1y'], 2),
                'avg_expense_ratio': round(stats['avg_expense_ratio'], 3),
                'top_performer': dict(top) if top else None,
                'avg_premium_discount': avg_pd_row[0] if avg_pd_row else None,
                'categories': [dict(c) for c in cats],
                'largest_etf': dict(largest_etf_row) if largest_etf_row else None,
                'largest_issuer_fum': largest_issuer_fum,
                'most_etfs_issuer': dict(most_etfs_issuer_row) if most_etfs_issuer_row else None,
                'fum_weighted_mer': round(fum_weighted_mer_row['fwm'], 3) if fum_weighted_mer_row and fum_weighted_mer_row['fwm'] else None,
                'upcoming_count': upcoming_count,
                'asx_count': asx_count,
                'cxa_count': cxa_count,
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

    # ----- History: Industry -----
    def handle_history_industry(self):
        conn = get_db()
        try:
            rows = conn.execute("""
                SELECT m.date,
                       COUNT(DISTINCT m.code) AS etf_count,
                       SUM(m.market_cap)/1e9   AS aum_b,
                       SUM(CASE WHEN l.admission_date IS NULL
                                  OR strftime('%Y-%m', m.date) != strftime('%Y-%m', l.admission_date)
                                THEN m.funds_flow ELSE 0 END)/1e6 AS flows_m,
                       SUM(m.funds_flow)/1e6 AS raw_flows_m,
                       SUM(m.transacted_value)/1e6 AS traded_m
                FROM etp_monthly m LEFT JOIN etp_list l ON l.ticker = m.code
                WHERE m.market_cap > 0
                GROUP BY m.date ORDER BY m.date ASC
            """).fetchall()
            data = [dict(r) for r in rows]
            # Build anomalies map: date -> [{code, name, flow_m}] for admission-month spikes >$500M
            anomaly_months = [r['date'] for r in data
                              if abs((r['raw_flows_m'] or 0) - (r['flows_m'] or 0)) > 500]
            anomalies = {}
            if anomaly_months:
                ph = ','.join('?' * len(anomaly_months))
                contribs = conn.execute(f"""
                    SELECT m.date, m.code, COALESCE(l.name, m.code) AS name,
                           m.funds_flow/1e6 AS flow_m
                    FROM etp_monthly m LEFT JOIN etp_list l ON l.ticker = m.code
                    WHERE m.date IN ({ph})
                      AND l.admission_date IS NOT NULL
                      AND strftime('%Y-%m', m.date) = strftime('%Y-%m', l.admission_date)
                      AND m.funds_flow > 100000000
                    ORDER BY m.date, m.funds_flow DESC
                """, anomaly_months).fetchall()
                for r in contribs:
                    anomalies.setdefault(r['date'], []).append(
                        {'code': r['code'], 'name': r['name'], 'flow_m': round(r['flow_m'] or 0, 0)}
                    )
            self.send_json({'data': data, 'anomalies': anomalies})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- History: Issuers -----
    def handle_history_issuers(self):
        conn = get_db()
        try:
            latest = conn.execute("SELECT MAX(date) FROM etp_monthly").fetchone()[0]
            top = conn.execute("""
                SELECT COALESCE(l.trim_issuer, l.issuer, 'Other') AS issuer,
                       SUM(m.market_cap) AS aum
                FROM etp_monthly m LEFT JOIN etp_list l ON l.ticker=m.code
                WHERE m.date=? AND m.market_cap>0
                GROUP BY issuer ORDER BY aum DESC LIMIT 10
            """, (latest,)).fetchall()
            top_names = [r['issuer'] for r in top]
            all_rows = conn.execute("""
                SELECT m.date,
                       COALESCE(l.trim_issuer, l.issuer, 'Other') AS issuer,
                       SUM(m.market_cap)/1e9 AS aum_b
                FROM etp_monthly m LEFT JOIN etp_list l ON l.ticker=m.code
                WHERE m.market_cap>0
                GROUP BY m.date, issuer ORDER BY m.date ASC
            """).fetchall()
            from collections import defaultdict
            pivot = defaultdict(dict)
            dates_set = []
            seen_dates = set()
            for r in all_rows:
                nm = r['issuer'] if r['issuer'] in top_names else 'Other'
                pivot[r['date']][nm] = pivot[r['date']].get(nm, 0) + (r['aum_b'] or 0)
                if r['date'] not in seen_dates:
                    seen_dates.add(r['date'])
                    dates_set.append(r['date'])
            series = []
            display_names = top_names + (['Other'] if any(r['issuer'] not in top_names for r in all_rows) else [])
            seen = set()
            ordered_names = []
            for n in display_names:
                if n not in seen:
                    seen.add(n)
                    ordered_names.append(n)
            for name in ordered_names:
                series.append({'name': name, 'data': [round(pivot[d].get(name, 0), 2) for d in dates_set]})
            self.send_json({'dates': dates_set, 'series': series})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- History: Asset Classes -----
    def handle_history_asset_classes(self):
        conn = get_db()
        try:
            latest = conn.execute("SELECT MAX(date) FROM etp_monthly").fetchone()[0]
            top = conn.execute("""
                SELECT CASE
                    WHEN l.asset_class = 'Equity' AND l.segment LIKE 'Equity - Australia%' THEN 'Australian Equities'
                    WHEN l.asset_class = 'Equity' THEN 'International Equities'
                    ELSE COALESCE(l.asset_class, 'Other')
                END AS cls, SUM(m.market_cap) AS aum
                FROM etp_monthly m LEFT JOIN etp_list l ON l.ticker=m.code
                WHERE m.date=? AND m.market_cap>0
                GROUP BY cls ORDER BY aum DESC LIMIT 8
            """, (latest,)).fetchall()
            top_names = [r['cls'] for r in top]
            all_rows = conn.execute("""
                SELECT m.date,
                       CASE
                           WHEN l.asset_class = 'Equity' AND l.segment LIKE 'Equity - Australia%' THEN 'Australian Equities'
                           WHEN l.asset_class = 'Equity' THEN 'International Equities'
                           ELSE COALESCE(l.asset_class, 'Other')
                       END AS cls,
                       SUM(m.market_cap)/1e9 AS aum_b
                FROM etp_monthly m LEFT JOIN etp_list l ON l.ticker=m.code
                WHERE m.market_cap>0
                GROUP BY m.date, cls ORDER BY m.date ASC
            """).fetchall()
            from collections import defaultdict
            pivot = defaultdict(dict)
            dates_set = []
            seen_dates = set()
            for r in all_rows:
                nm = r['cls'] if r['cls'] in top_names else 'Other'
                pivot[r['date']][nm] = pivot[r['date']].get(nm, 0) + (r['aum_b'] or 0)
                if r['date'] not in seen_dates:
                    seen_dates.add(r['date'])
                    dates_set.append(r['date'])
            ordered_names = list(dict.fromkeys(top_names + (['Other'] if any(r['cls'] not in top_names for r in all_rows) else [])))
            series = [{'name': n, 'data': [round(pivot[d].get(n, 0), 2) for d in dates_set]} for n in ordered_names]
            self.send_json({'dates': dates_set, 'series': series})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- History: ETF detail -----
    def handle_history_etf(self, code):
        conn = get_db()
        try:
            info = conn.execute("SELECT name, issuer, admission_date FROM etp_list WHERE ticker=?", (code,)).fetchone()
            rows = conn.execute("""
                SELECT date, last_price, market_cap/1e6 AS aum_m,
                       funds_flow/1e6 AS flow_m, return_1y, distribution_yield,
                       total_units/1e6 AS units_m, spread_pct, mer,
                       chess_units/1e6 AS chess_units_m,
                       chess_mc/1e6 AS chess_mc_m,
                       chess_funds_flow/1e6 AS chess_flow_m
                FROM etp_monthly WHERE code=? ORDER BY date ASC
            """, (code,)).fetchall()
            adm_ym = (info['admission_date'] or '')[:7] if info else ''
            data = []
            for r in rows:
                row = dict(r)
                row['is_anomaly'] = bool(adm_ym and row['date'][:7] == adm_ym)
                data.append(row)
            self.send_json({
                'code': code,
                'name': info['name'] if info else code,
                'issuer': info['issuer'] if info else None,
                'data': data,
            })
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- History: Flows -----
    def handle_history_flows(self, qs):
        months = self._int(qs, 'months', 12)
        limit = self._int(qs, 'limit', 20)
        conn = get_db()
        try:
            latest = conn.execute("SELECT MAX(date) FROM etp_monthly").fetchone()[0]
            sql = """
                SELECT m.code,
                       COALESCE(l.name, m.code) AS name,
                       COALESCE(l.trim_issuer, l.issuer) AS issuer,
                       CASE
                           WHEN l.asset_class = 'Equity' AND l.segment LIKE 'Equity - Australia%' THEN 'Australian Equities'
                           WHEN l.asset_class = 'Equity' THEN 'International Equities'
                           ELSE COALESCE(l.asset_class, 'Other')
                       END AS asset_class,
                       SUM(m.funds_flow)/1e6 AS flow_m,
                       MAX(m.market_cap)/1e6  AS latest_aum_m
                FROM etp_monthly m LEFT JOIN etp_list l ON l.ticker=m.code
                WHERE m.date <= ? AND m.date > date(?, ? || ' months')
                  AND (l.admission_date IS NULL
                       OR strftime('%Y-%m', m.date) != strftime('%Y-%m', l.admission_date))
                GROUP BY m.code HAVING flow_m IS NOT NULL
                ORDER BY flow_m {dir} LIMIT ?
            """
            inflows  = conn.execute(sql.format(dir='DESC'), (latest, latest, f'-{months}', limit)).fetchall()
            outflows = conn.execute(sql.format(dir='ASC'),  (latest, latest, f'-{months}', limit)).fetchall()
            self.send_json({
                'months': months,
                'as_of': latest,
                'inflows':  [dict(r) for r in inflows],
                'outflows': [dict(r) for r in outflows],
            })
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- History: Launches -----
    def handle_history_launches(self):
        conn = get_db()
        try:
            by_year = conn.execute("""
                SELECT strftime('%Y', admission_date) AS year, COUNT(*) AS count
                FROM etp_list
                WHERE admission_date IS NOT NULL AND admission_date != ''
                  AND strftime('%Y', admission_date) >= '2000'
                GROUP BY year ORDER BY year ASC
            """).fetchall()
            by_issuer = conn.execute("""
                SELECT COALESCE(trim_issuer, issuer, 'Unknown') AS issuer,
                       COUNT(*) AS total,
                       SUM(CASE WHEN is_listed=1 THEN 1 ELSE 0 END) AS active
                FROM etp_list
                WHERE admission_date IS NOT NULL AND strftime('%Y', admission_date) >= '2000'
                GROUP BY issuer ORDER BY total DESC LIMIT 15
            """).fetchall()
            self.send_json({
                'by_year':   [dict(r) for r in by_year],
                'by_issuer': [dict(r) for r in by_issuer],
            })
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- History: Geography -----
    def handle_history_geography(self):
        conn = get_db()
        try:
            # Group etp_list.country into cleaner buckets
            all_rows = conn.execute("""
                SELECT m.date,
                       CASE
                           WHEN l.country = 'Australia'                           THEN 'Australia'
                           WHEN l.country IN ('Global','International')           THEN 'Global'
                           WHEN l.country = 'U.S.'                               THEN 'United States'
                           WHEN l.country IN ('Asia','Asian Pacific Region','Asian Pacific Region ex Japan',
                                              'China','Hong Kong','India','Japan','Singapore',
                                              'South Korea','Taiwan')             THEN 'Asia-Pacific'
                           WHEN l.country IN ('Europe','U.K.')                   THEN 'Europe'
                           WHEN l.country = 'Emerging Markets'                   THEN 'Emerging Markets'
                           WHEN l.country IS NULL                                 THEN 'Unclassified'
                           ELSE 'Other'
                       END AS geo,
                       SUM(m.market_cap)/1e9 AS aum_b
                FROM etp_monthly m LEFT JOIN etp_list l ON l.ticker = m.code
                WHERE m.market_cap > 0
                GROUP BY m.date, geo ORDER BY m.date ASC
            """).fetchall()
            # Determine top geos by latest-month AUM
            latest = max(r['date'] for r in all_rows)
            top = sorted(
                [(r['geo'], r['aum_b']) for r in all_rows if r['date'] == latest],
                key=lambda x: -x[1]
            )
            top_names = [g for g, _ in top]
            from collections import defaultdict
            pivot = defaultdict(dict)
            dates_seen = []
            seen_set = set()
            for r in all_rows:
                nm = r['geo'] if r['geo'] in top_names else 'Other'
                pivot[r['date']][nm] = pivot[r['date']].get(nm, 0) + (r['aum_b'] or 0)
                if r['date'] not in seen_set:
                    seen_set.add(r['date'])
                    dates_seen.append(r['date'])
            ordered = list(dict.fromkeys(top_names + (['Other'] if any(r['geo'] not in top_names for r in all_rows) else [])))
            series = [{'name': n, 'data': [round(pivot[d].get(n, 0), 2) for d in dates_seen]} for n in ordered]
            self.send_json({'dates': dates_seen, 'series': series})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- History: Strategy & Factors -----
    def handle_history_strategy(self):
        conn = get_db()
        try:
            all_rows = conn.execute("""
                SELECT m.date,
                       CASE
                           WHEN l.investment_style = 'Active'                    THEN 'Active'
                           WHEN l.smart_beta = 'yes' AND l.strategy = 'Quality'  THEN 'Quality'
                           WHEN l.smart_beta = 'yes' AND l.strategy = 'Value'    THEN 'Value'
                           WHEN l.smart_beta = 'yes' AND l.strategy = 'Growth'   THEN 'Growth'
                           WHEN l.smart_beta = 'yes' AND l.strategy = 'Blend'    THEN 'Blend (Multi-factor)'
                           WHEN l.smart_beta = 'yes'                             THEN 'Other Smart Beta'
                           WHEN l.investment_style IN ('Index','Index-Tracking')  THEN 'Passive Index'
                           ELSE 'Unclassified'
                       END AS strategy,
                       SUM(m.market_cap)/1e9 AS aum_b
                FROM etp_monthly m LEFT JOIN etp_list l ON l.ticker = m.code
                WHERE m.market_cap > 0
                GROUP BY m.date, strategy ORDER BY m.date ASC
            """).fetchall()
            latest = max(r['date'] for r in all_rows)
            top = sorted(
                [(r['strategy'], r['aum_b']) for r in all_rows if r['date'] == latest],
                key=lambda x: -x[1]
            )
            top_names = [s for s, _ in top]
            from collections import defaultdict
            pivot = defaultdict(dict)
            dates_seen = []
            seen_set = set()
            for r in all_rows:
                nm = r['strategy'] if r['strategy'] in top_names else 'Other'
                pivot[r['date']][nm] = pivot[r['date']].get(nm, 0) + (r['aum_b'] or 0)
                if r['date'] not in seen_set:
                    seen_set.add(r['date'])
                    dates_seen.append(r['date'])
            ordered = list(dict.fromkeys(top_names))
            series = [{'name': n, 'data': [round(pivot[d].get(n, 0), 2) for d in dates_seen]} for n in ordered]
            self.send_json({'dates': dates_seen, 'series': series})
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
                "       h.name AS holding_name, h.ticker, h.weight_pct, h.sector, "
                "       e.holdings_disclosure, e.holdings_as_of "
                "FROM etf_holdings h JOIN etfs e ON e.code = h.etf_code "
                "WHERE (LOWER(h.name) LIKE ? OR LOWER(h.ticker) LIKE ?) "
                "  AND COALESCE(h.weight_pct, 0) >= ? "
                "ORDER BY h.weight_pct DESC "
                "LIMIT 100",
                (pattern, pattern, min_weight)
            ).fetchall()
            etf_codes = list(dict.fromkeys(r['etf_code'] for r in rows))
            # Build per-ETF disclosure metadata map
            disclosure_meta = {}
            for r in rows:
                code = r['etf_code']
                if code not in disclosure_meta:
                    disclosure_meta[code] = {
                        'holdings_disclosure': r['holdings_disclosure'],
                        'holdings_as_of': r['holdings_as_of'],
                    }
            self.send_json({
                'query': q,
                'total': len(rows),
                'etf_count': len(etf_codes),
                'etfs': etf_codes,
                'disclosure_meta': disclosure_meta,
                'results': [dict(r) for r in rows],
            })
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    # ----- Holdings concentration -----
    def handle_holdings_concentration(self):
        conn = get_db()
        try:
            rows = conn.execute("""
                WITH base AS (
                    SELECT
                        h.etf_code,
                        e.name      AS etf_name,
                        e.asset_class,
                        e.issuer,
                        COUNT(*)    AS holding_count,
                        COUNT(DISTINCT CASE WHEN h.country IS NOT NULL AND h.country != ''
                                            THEN h.country END) AS country_count,
                        COUNT(DISTINCT CASE WHEN h.sector  IS NOT NULL AND h.sector  != ''
                                            THEN h.sector  END) AS sector_count,
                        (SELECT COALESCE(SUM(w), 0) FROM (
                            SELECT weight_pct AS w FROM etf_holdings
                            WHERE etf_code = h.etf_code AND weight_pct IS NOT NULL
                            ORDER BY weight_pct DESC LIMIT 10
                        ) t) AS top10_conc
                    FROM etf_holdings h
                    JOIN etfs e ON e.code = h.etf_code
                    GROUP BY h.etf_code, e.name, e.asset_class, e.issuer
                )
                SELECT * FROM base WHERE holding_count >= 5
            """).fetchall()
            data = [dict(r) for r in rows]

            def topN(key, asc, n=8):
                f = [d for d in data if d.get(key) is not None]
                return sorted(f, key=lambda x: x[key], reverse=not asc)[:n]

            self.send_json({
                'by_count': {
                    'most_concentrated': topN('holding_count', asc=True),
                    'most_diversified':  topN('holding_count', asc=False),
                },
                'by_geography': {
                    'most_concentrated': topN('country_count', asc=True),
                    'most_diversified':  topN('country_count', asc=False),
                },
                'by_sector': {
                    'most_concentrated': topN('sector_count', asc=True),
                    'most_diversified':  topN('sector_count', asc=False),
                },
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
        from datetime import datetime as _dt

        _DATE_FMTS = ('%Y-%m-%d', '%d %b %Y', '%d-%b-%Y', '%d-%b-%y',
                      '%d/%m/%Y', '%d/%m/%y', '%b %d, %Y')

        def parse_iso(d):
            """Return YYYY-MM-DD string or None; handles all stored date formats."""
            if not d: return None
            s = str(d).strip()
            for fmt in _DATE_FMTS:
                try:
                    return _dt.strptime(s, fmt).strftime('%Y-%m-%d')
                except ValueError:
                    pass
            return None

        def year_from(d):
            iso = parse_iso(d)
            if iso: return iso[:4]
            m = _re.search(r'\b(20\d{2}|199\d)\b', str(d))
            return m.group(1) if m else None

        conn = get_db()
        try:
            all_etfs = conn.execute(
                "SELECT code, name, issuer, asset_class, exchange, inception_date, fund_type FROM etfs"
            ).fetchall()

            # Recent / oldest — parse all dates to ISO so sort is correct regardless of stored format
            dated = [(r, parse_iso(r['inception_date'])) for r in all_etfs if parse_iso(r['inception_date'])]
            dated_sorted = [r for r, _ in sorted(dated, key=lambda x: x[1])]

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

            def to_dict_norm(r):
                d = dict(r)
                d['inception_date'] = parse_iso(d.get('inception_date')) or d.get('inception_date')
                return d

            self.send_json({
                'total': len(all_etfs),
                'recent': [to_dict_norm(r) for r in reversed(dated_sorted[-30:])],
                'oldest': [to_dict_norm(r) for r in dated_sorted[:30]],
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

            COLS = (f"code, name, issuer, asset_class, {MER} AS effective_mer, "
                    f"COALESCE(bid_ask_spread_pct, 0) AS spread, fund_size_aud_millions")
            all_etfs = conn.execute(
                f"SELECT {COLS} FROM etfs WHERE {MER} > 0 ORDER BY effective_mer ASC"
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
                'all_etfs':         [dict(r) for r in all_etfs],
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
                top_etfs_rows = conn.execute(
                    "SELECT code, name, fund_size_aud_millions FROM etfs "
                    "WHERE issuer=? ORDER BY fund_size_aud_millions DESC NULLS LAST LIMIT 3",
                    (iss,)
                ).fetchall()
                d = dict(row)
                d['market_share_pct'] = round(d['total_fum'] / total_mkt * 100, 2)
                d['asset_classes'] = {r['asset_class']: r['cnt'] for r in ac_rows}
                d['top_etf'] = dict(top_etf) if top_etf else None
                d['top_etfs'] = [dict(r) for r in top_etfs_rows]
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

    def handle_insights_asset_classes(self):
        conn = get_db()
        try:
            # 1. Asset class + sub_category breakdown
            ac_rows = conn.execute('''
                SELECT asset_class, sub_category,
                       COUNT(*) etf_count,
                       ROUND(SUM(fund_size_aud_millions),1) total_fum,
                       ROUND(AVG(return_1y),2) avg_1y,
                       ROUND(AVG(return_3y),2) avg_3y,
                       ROUND(AVG(return_5y),2) avg_5y,
                       ROUND(AVG(COALESCE(expense_ratio,management_fee)),3) avg_mer,
                       ROUND(SUM(fund_flow_1m),1) net_flow_1m
                FROM etfs
                WHERE asset_class IS NOT NULL AND fund_size_aud_millions > 0
                GROUP BY asset_class, sub_category
                ORDER BY total_fum DESC
            ''').fetchall()

            # 2. GICS sectors — FUM-weighted across market (normalised)
            gics_rows = conn.execute("""
                SELECT
                    CASE s.sector
                        WHEN 'Healthcare'             THEN 'Health Care'
                        WHEN 'Financial'              THEN 'Financials'
                        WHEN 'Communication'          THEN 'Communication Services'
                        WHEN 'Communications'         THEN 'Communication Services'
                        WHEN 'Technology'             THEN 'Information Technology'
                        ELSE s.sector
                    END AS sector_norm,
                    ROUND(SUM(s.weight_pct * e.fund_size_aud_millions / 100.0),1) AS fum_weighted_m,
                    COUNT(DISTINCT s.etf_code) AS etf_count,
                    ROUND(AVG(e.return_1y),2) AS avg_return_1y
                FROM etf_sectors s
                JOIN etfs e ON e.code = s.etf_code
                WHERE s.sector NOT IN ('Cash and/or Derivatives','Cash','Derivatives','-','ETFs','Other','Unknown')
                  AND s.sector IS NOT NULL AND e.fund_size_aud_millions > 0
                GROUP BY sector_norm
                HAVING fum_weighted_m > 100
                ORDER BY fum_weighted_m DESC
                LIMIT 20
            """).fetchall()

            # 3. GICS sector → ETF drill (top 5 ETFs per sector by weight_pct * FUM)
            sector_etf_rows = conn.execute("""
                SELECT
                    CASE s.sector
                        WHEN 'Healthcare'   THEN 'Health Care'
                        WHEN 'Financial'    THEN 'Financials'
                        WHEN 'Communication' THEN 'Communication Services'
                        WHEN 'Communications' THEN 'Communication Services'
                        WHEN 'Technology'   THEN 'Information Technology'
                        ELSE s.sector
                    END AS sector_norm,
                    s.etf_code, e.name, e.issuer,
                    ROUND(s.weight_pct,2) AS weight_pct,
                    ROUND(e.fund_size_aud_millions,1) AS fum,
                    ROUND(e.return_1y,2) AS return_1y
                FROM etf_sectors s
                JOIN etfs e ON e.code = s.etf_code
                WHERE s.sector NOT IN ('Cash and/or Derivatives','Cash','Derivatives','-','ETFs','Other','Unknown')
                  AND e.fund_size_aud_millions > 50
                ORDER BY sector_norm, s.weight_pct DESC
            """).fetchall()

            # 4. Country exposure — FUM-weighted (normalised)
            country_rows = conn.execute("""
                SELECT
                    CASE h.country
                        WHEN 'US' THEN 'United States'  WHEN 'AU' THEN 'Australia'
                        WHEN 'GB' THEN 'United Kingdom' WHEN 'Britain' THEN 'United Kingdom'
                        WHEN 'JP' THEN 'Japan'          WHEN 'CN' THEN 'China'
                        WHEN 'IN' THEN 'India'          WHEN 'FR' THEN 'France'
                        WHEN 'KR' THEN 'South Korea'    WHEN 'Korea (South)' THEN 'South Korea'
                        WHEN 'DE' THEN 'Germany'        WHEN 'NL' THEN 'Netherlands'
                        WHEN 'CH' THEN 'Switzerland'    WHEN 'CA' THEN 'Canada'
                        WHEN 'HK' THEN 'Hong Kong'      WHEN 'SG' THEN 'Singapore'
                        WHEN 'SE' THEN 'Sweden'         WHEN 'NO' THEN 'Norway'
                        WHEN 'DK' THEN 'Denmark'        WHEN 'IT' THEN 'Italy'
                        WHEN 'ES' THEN 'Spain'          WHEN 'TW' THEN 'Taiwan'
                        WHEN 'IL' THEN 'Israel'         WHEN 'NZ' THEN 'New Zealand'
                        WHEN 'FI' THEN 'Finland'        WHEN 'BE' THEN 'Belgium'
                        WHEN 'AT' THEN 'Austria'        WHEN 'PT' THEN 'Portugal'
                        WHEN 'ZA' THEN 'South Africa'   WHEN 'BR' THEN 'Brazil'
                        WHEN 'MX' THEN 'Mexico'         WHEN 'TH' THEN 'Thailand'
                        WHEN 'ID' THEN 'Indonesia'      WHEN 'MY' THEN 'Malaysia'
                        WHEN 'PH' THEN 'Philippines'    WHEN 'PL' THEN 'Poland'
                        WHEN 'CZ' THEN 'Czech Republic' WHEN 'GR' THEN 'Greece'
                        ELSE h.country
                    END AS country_norm,
                    ROUND(SUM(h.weight_pct * e.fund_size_aud_millions / 100.0),1) AS fum_weighted_m,
                    COUNT(DISTINCT h.etf_code) AS etf_count
                FROM etf_holdings h
                JOIN etfs e ON e.code = h.etf_code
                WHERE h.country IS NOT NULL
                  AND h.country NOT IN ('','-','Europe','Cash')
                  AND h.country NOT LIKE '%Exchange%'
                  AND h.country NOT LIKE '%Market%'
                  AND h.country NOT LIKE '%NASDAQ%'
                  AND h.country NOT LIKE '%NYSE%'
                  AND h.country NOT LIKE '%Ireland%'
                  AND e.fund_size_aud_millions > 0
                GROUP BY country_norm
                HAVING fum_weighted_m > 20
                ORDER BY fum_weighted_m DESC
                LIMIT 30
            """).fetchall()

            # 5. Country → ETF drill (ETFs with most exposure to each country)
            country_etf_rows = conn.execute("""
                SELECT
                    CASE h.country
                        WHEN 'US' THEN 'United States'  WHEN 'AU' THEN 'Australia'
                        WHEN 'GB' THEN 'United Kingdom' WHEN 'Britain' THEN 'United Kingdom'
                        WHEN 'JP' THEN 'Japan'          WHEN 'CN' THEN 'China'
                        WHEN 'IN' THEN 'India'          WHEN 'FR' THEN 'France'
                        WHEN 'KR' THEN 'South Korea'    WHEN 'Korea (South)' THEN 'South Korea'
                        WHEN 'DE' THEN 'Germany'        WHEN 'NL' THEN 'Netherlands'
                        WHEN 'CH' THEN 'Switzerland'    WHEN 'CA' THEN 'Canada'
                        WHEN 'HK' THEN 'Hong Kong'      WHEN 'SG' THEN 'Singapore'
                        WHEN 'TW' THEN 'Taiwan'         WHEN 'IL' THEN 'Israel'
                        ELSE h.country
                    END AS country_norm,
                    h.etf_code, e.name, e.issuer,
                    ROUND(SUM(h.weight_pct),1) AS total_weight_pct,
                    ROUND(e.fund_size_aud_millions,1) AS fum,
                    ROUND(e.return_1y,2) AS return_1y
                FROM etf_holdings h
                JOIN etfs e ON e.code = h.etf_code
                WHERE h.country IS NOT NULL
                  AND h.country NOT IN ('','-','Europe','Cash')
                  AND h.country NOT LIKE '%Exchange%'
                  AND h.country NOT LIKE '%Market%'
                  AND e.fund_size_aud_millions > 50
                GROUP BY country_norm, h.etf_code
                HAVING total_weight_pct > 5
                ORDER BY country_norm, total_weight_pct DESC
            """).fetchall()

            # 6. Full ETF list for factor lens
            etf_list = conn.execute('''
                SELECT code, name, issuer, asset_class, sub_category,
                       ROUND(fund_size_aud_millions,1) fum,
                       return_1y, return_3y, return_5y,
                       ROUND(COALESCE(expense_ratio,management_fee),3) mer,
                       beta, sharpe_ratio, fx_hedged, fund_flow_1m,
                       benchmark, volatility_1y
                FROM etfs
                WHERE fund_size_aud_millions > 0
                ORDER BY fund_size_aud_millions DESC
            ''').fetchall()

            self.send_json({
                'by_asset_class': [dict(r) for r in ac_rows],
                'gics_sectors':   [dict(r) for r in gics_rows],
                'sector_etfs':    [dict(r) for r in sector_etf_rows],
                'countries':      [dict(r) for r in country_rows],
                'country_etfs':   [dict(r) for r in country_etf_rows],
                'etf_list':       [dict(r) for r in etf_list],
            })
        except Exception as e:
            self.send_json({'error': str(e)}, 500)
        finally:
            conn.close()

    def handle_insights_upcoming(self):
        conn = get_db()
        try:
            from datetime import date
            today = date.today().isoformat()
            ytd_start = date.today().replace(month=1, day=1).isoformat()
            d30 = date.today().replace(day=1).isoformat()  # approx; use actual -30d
            from datetime import timedelta
            d30 = (date.today() - timedelta(days=30)).isoformat()
            d90 = (date.today() - timedelta(days=90)).isoformat()

            # --- Upcoming: pending listings from upcoming_listings table ---
            has_upcoming_table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='upcoming_listings'"
            ).fetchone()

            upcoming = []
            if has_upcoming_table:
                rows = conn.execute("""
                    SELECT asic_doc_no, code, name, scheme_name, issuer, exchange,
                           asset_class, fund_type, arsn, expected_listing_date,
                           pds_lodged_date, offer_doc_url, asic_detail_url, status
                    FROM upcoming_listings
                    WHERE status = 'pending'
                    AND LOWER(name) LIKE '%etf%'
                    ORDER BY expected_listing_date ASC NULLS LAST
                """).fetchall()
                upcoming = [dict(r) for r in rows]
                for u in upcoming:
                    if not u.get('asset_class'):
                        u['asset_class'] = estimate_asset_class(
                            u.get('name', ''), u.get('fund_type', ''), '', u.get('issuer', ''))

            # --- Recently listed: from etfs table by inception_date ---
            # Guard: only match ISO-format dates (YYYY-MM-DD) to exclude ~51 VanEck ETFs
            # that have inception_date stored as DD-Mon-YY and sort incorrectly as strings.
            recent = conn.execute("""
                SELECT code, name, issuer, exchange, asset_class, fund_type,
                       inception_date, management_fee, expense_ratio,
                       fund_size_aud_millions
                FROM etfs
                WHERE inception_date >= ?
                  AND inception_date GLOB '20[0-9][0-9]-[0-1][0-9]-[0-3][0-9]'
                ORDER BY inception_date DESC
            """, (d90,)).fetchall()
            recent = [dict(r) for r in recent]

            # Stats
            upcoming_count = len(upcoming)
            listed_last_30 = sum(1 for r in recent if r['inception_date'] and r['inception_date'] >= d30)
            listed_last_90 = len(recent)
            listed_ytd = conn.execute(
                "SELECT COUNT(*) FROM etfs WHERE inception_date >= ? "
                "AND inception_date GLOB '20[0-9][0-9]-[0-1][0-9]-[0-3][0-9]'", (ytd_start,)
            ).fetchone()[0]

            self.send_json({
                'upcoming': upcoming,
                'recent': recent,
                'stats': {
                    'upcoming_count': upcoming_count,
                    'listed_last_30': listed_last_30,
                    'listed_last_90': listed_last_90,
                    'listed_ytd': listed_ytd,
                },
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
  /* ── Dark Navy Theme ── */
  html, body { background: #1a2e4a !important; color: #e2e8f0 !important; }

  /* Backgrounds */
  .bg-white                { background: #233d66 !important; }
  .bg-gray-50, .bg-slate-50 { background: #1e3354 !important; }
  .bg-gray-100, .bg-slate-100, .bg-slate-200 { background: #1a2e4a !important; }
  .bg-blue-50              { background: #1e3878 !important; }
  .bg-green-50             { background: #1e4030 !important; }
  .bg-red-50               { background: #3e1818 !important; }
  .bg-amber-50             { background: #302c00 !important; }
  .bg-sky-50               { background: #1c3860 !important; }

  /* Text */
  .text-gray-900, .text-gray-800, .text-gray-700 { color: #e2e8f0 !important; }
  .text-gray-600, .text-gray-500                 { color: #a8c4e0 !important; }
  .text-gray-400, .text-gray-300                 { color: #7fa3c8 !important; }
  .text-slate-800, .text-slate-700               { color: #e2e8f0 !important; }
  .text-slate-600, .text-slate-500               { color: #a8c4e0 !important; }
  .text-slate-400                                { color: #7fa3c8 !important; }
  .text-blue-900, .text-blue-800                 { color: #93c5fd !important; }
  .text-blue-700, .text-blue-600                 { color: #60a5fa !important; }
  .text-blue-500                                 { color: #3b82f6 !important; }
  .text-green-800, .text-green-700               { color: #4ade80 !important; }
  .text-red-800, .text-red-700                   { color: #f87171 !important; }
  .text-indigo-800, .text-indigo-700             { color: #a5b4fc !important; }
  .text-purple-800                               { color: #c4b5fd !important; }
  .text-amber-800                                { color: #fcd34d !important; }
  .text-teal-800                                 { color: #5eead4 !important; }
  .text-orange-800                               { color: #fb923c !important; }

  /* Borders */
  .border-gray-100, .border-gray-200, .border-gray-300 { border-color: #2e5285 !important; }
  .border-slate-100, .border-slate-200                 { border-color: #2e5285 !important; }
  .divide-gray-50, .divide-gray-100                    { --tw-divide-opacity: 1; }
  .divide-y > * + * { border-color: #2e5285 !important; }

  /* Badge chips */
  .bg-blue-50  { background: #1e3878 !important; }
  .bg-blue-100 { background: #1e3878 !important; }
  .bg-green-100 { background: #1e4030 !important; }
  .bg-red-100  { background: #3e1818 !important; }
  .bg-purple-100 { background: #2e2060 !important; }
  .bg-amber-100  { background: #302a00 !important; }
  .bg-teal-100   { background: #183830 !important; }
  .bg-orange-100 { background: #302000 !important; }
  .bg-gray-100, .bg-gray-200 { background: #2c4d78 !important; }
  .bg-indigo-100 { background: #202858 !important; }
  .bg-sky-100    { background: #183358 !important; }

  /* Inputs & forms */
  input[type=text], input[type=number], input[type=search], select, textarea {
    background: #1e3354 !important;
    border-color: #2e5285 !important;
    color: #e2e8f0 !important;
  }
  input::placeholder { color: #7fa3c8 !important; }
  select option { background: #233d66; color: #e2e8f0; }

  /* Cards with shadow */
  .shadow, .shadow-sm, .shadow-md, .shadow-lg, .shadow-xl {
    box-shadow: 0 4px 24px rgba(0,0,0,.5) !important;
  }

  /* Hover states on rows/cards */
  .hover\:bg-blue-50:hover   { background: #2c4d78 !important; }
  .hover\:bg-gray-50:hover   { background: #2c4d78 !important; }
  .hover\:bg-slate-50:hover  { background: #2c4d78 !important; }
  .hover\:text-blue-600:hover, .hover\:text-blue-700:hover { color: #60a5fa !important; }

  /* Range input */
  input[type=range] { accent-color: #3b82f6; }

  /* Exchange badges */
  .badge-asx  { background: #2d55cc; color: #fff; }
  .badge-cboe { background: #5b21b6; color: #fff; }

  /* Asset class colour chips */
  .ac-au   { background: #1e3878; color: #93c5fd; }
  .ac-int  { background: #1e4030; color: #4ade80; }
  .ac-fi   { background: #302a00; color: #fcd34d; }
  .ac-prop { background: #3e1830; color: #f9a8d4; }
  .ac-com  { background: #302800; color: #fde68a; }
  .ac-div  { background: #202858; color: #a5b4fc; }
  .ac-alt  { background: #142030; color: #94a3b8; }

  /* Table rows */
  #etf-table tr { border-bottom: 1px solid #2e5285; transition: background .1s; }
  #etf-table tr:hover { background: #2c4d78 !important; }
  #etf-table tr.row-selected { background: #345590 !important; }

  /* Detail tabs */
  .dtab { color: #7fa3c8; padding-bottom: 10px; font-weight: 500; transition: color .15s; border-bottom: 2px solid transparent; }
  .dtab:hover { color: #60a5fa; }
  .tab-active { color: #3b82f6 !important; border-bottom-color: #3b82f6; }

  /* Search dropdown */
  #search-results { position: absolute; z-index: 50; top: calc(100% + 6px); left: 0; right: 0; }
  #search-results .bg-white { background: #233d66 !important; }

  /* Spinner */
  .spinner { border: 3px solid #2e5285; border-top-color: #3b82f6; border-radius: 50%;
             width: 28px; height: 28px; animation: spin .7s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* Progress bars animate in */
  .pbar { transition: width .5s cubic-bezier(.4,0,.2,1); }

  /* Scrollbar */
  ::-webkit-scrollbar { width: 5px; height: 5px; }
  ::-webkit-scrollbar-track { background: #1a2e4a; }
  ::-webkit-scrollbar-thumb { background: #2e5285; border-radius: 4px; }

  /* Stat card hover lift */
  .stat-card { transition: transform .15s, box-shadow .15s; }
  .stat-card:hover { transform: translateY(-2px); box-shadow: 0 6px 16px rgba(0,0,0,.4); }

  /* Main view tabs */
  .main-tab { color: #7fa3c8; padding: 10px 0; font-weight: 500; transition: color .15s;
              border-bottom: 2px solid transparent; white-space: nowrap; }
  .main-tab:hover { color: #60a5fa; }
  .main-tab.active { color: #3b82f6; border-bottom-color: #3b82f6; }

  /* Tab bar background */
  .border-b { border-color: #2e5285 !important; }

  /* Screener */
  .sc-check-list { max-height: 130px; overflow-y: auto; }
  .sc-check-list label { display: flex; align-items: center; gap: 6px; padding: 3px 0;
                         cursor: pointer; font-size: .8125rem; color: #a8c4e0; }
  .sc-check-list label:hover { color: #60a5fa; }

  /* Compare mini-bars */
  .cmp-bar-wrap { display: flex; align-items: center; gap: 4px; }
  .cmp-bar { height: 6px; border-radius: 3px; background: #3b82f6; min-width: 2px; }
  .cmp-bar-neg { background: #ef4444; }

  /* Screener table rows */
  #screener-table tr { border-bottom: 1px solid #2e5285; transition: background .1s; }
  #screener-table tr:hover { background: #2c4d78 !important; }

  /* Holdings table */
  #holdings-table tr { border-bottom: 1px solid #2e5285; }
  #holdings-table tr:hover { background: #2c4d78 !important; }

  /* Data freshness tooltip trigger */
  .dated { cursor: help; }
  .dated:hover { border-bottom: 1px dotted #7fa3c8; }

  /* Article/content pages */
  .prose, .prose p, .prose h2, .prose h3 { color: #e2e8f0 !important; }
  table th { background: #1e3354 !important; color: #a8c4e0 !important; border-color: #2e5285 !important; }
  table td { border-color: #2e5285 !important; }
  .chart-box { background: #233d66; border: 1px solid #2e5285; border-radius: 8px; padding: 16px; margin: 16px 0; }

  /* Positive / negative text (returns) */
  .pos { color: #4ade80 !important; }
  .neg { color: #f87171 !important; }

  /* Buttons */
  .bg-blue-600 { background: #2563eb !important; }
  .bg-blue-600:hover, .hover\:bg-blue-700:hover { background: #1d4ed8 !important; }
</style>
</head>
<body class="bg-[#1a2e4a] min-h-screen text-sm text-slate-100 antialiased">

<!-- ── Header ── -->
<header class="bg-gradient-to-r from-[#071422] to-[#0d2860] shadow-xl">
  <div class="max-w-[1400px] mx-auto px-5 py-3 flex flex-wrap items-center gap-4">
    <div class="flex-1 min-w-[180px]">
      <h1 class="text-lg font-bold text-white tracking-tight leading-tight">
        Australian ETF Dashboard
      </h1>
      <p id="subtitle" class="text-slate-300 text-xs mt-0.5">Loading market data…</p>
    </div>

    <!-- Search -->
    <div class="relative w-80">
      <svg class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-300 pointer-events-none"
           fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
              d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/>
      </svg>
      <input id="search" type="text" placeholder="Search code or name…"
             autocomplete="off"
             class="w-full bg-white/10 border border-white/20 text-white placeholder-slate-300
                    rounded-xl pl-9 pr-3 py-2 text-sm outline-none
                    focus:ring-2 focus:ring-white/40 focus:bg-white/20">
      <div id="search-results"
           class="hidden bg-white border border-gray-200 rounded-xl shadow-2xl max-h-72 overflow-y-auto"></div>
    </div>

    <!-- Live indicator -->
    <div class="flex items-center gap-2 text-xs text-slate-300">
      <span class="w-2 h-2 bg-blue-400 rounded-full animate-pulse"></span>
      <span id="last-refresh">Live</span>
    </div>
  </div>
</header>

<main class="max-w-[1400px] mx-auto px-5 py-5">

  <!-- ── Stat cards ── -->
  <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3 mb-5">

    <!-- Tile 1: Market Size -->
    <div class="stat-card bg-[#233d66] rounded-xl border border-[#2e5285] p-5 cursor-pointer" onclick="goCard('fum')">
      <p class="text-xs font-semibold uppercase tracking-wider" style="color:#7fa3c8">MARKET SIZE</p>
      <p id="c-fum" class="text-2xl font-bold text-white mt-1 leading-tight">—</p>
      <hr class="border-[#2e5285] my-2">
      <div class="flex items-center justify-between text-xs" style="color:#a8c4e0">
        <span>Largest ETF</span>
        <span id="c-largest-etf" class="tabular-nums font-medium">—</span>
      </div>
      <div class="flex items-center justify-between text-xs mt-2" style="color:#7fa3c8">
        <span id="c-largest-issuer">—</span>
        <span>›</span>
      </div>
    </div>

    <!-- Tile 2: Listings -->
    <div class="stat-card bg-[#233d66] rounded-xl border border-[#2e5285] p-5 cursor-pointer" onclick="goCard('listings')">
      <p class="text-xs font-semibold uppercase tracking-wider" style="color:#7fa3c8">LISTINGS</p>
      <p id="c-count" class="text-2xl font-bold text-white mt-1 leading-tight">—</p>
      <hr class="border-[#2e5285] my-2">
      <div class="flex items-center justify-between text-xs" style="color:#a8c4e0">
        <span id="c-upcoming-count">—</span>
        <span>coming soon</span>
      </div>
      <div class="flex items-center justify-between text-xs mt-2" style="color:#7fa3c8">
        <span id="c-exchange-split">—</span>
        <span>›</span>
      </div>
    </div>

    <!-- Tile 3: Performance -->
    <div class="stat-card bg-[#233d66] rounded-xl border border-[#2e5285] p-5 cursor-pointer" onclick="goCard('returns')">
      <p class="text-xs font-semibold uppercase tracking-wider" style="color:#7fa3c8">PERFORMANCE</p>
      <p id="c-ret" class="text-2xl font-bold mt-1 leading-tight">—</p>
      <hr class="border-[#2e5285] my-2">
      <div class="flex items-center justify-between text-xs" style="color:#a8c4e0">
        <span>Best:</span>
        <span id="c-top" class="font-bold text-blue-400 tabular-nums">—</span>
      </div>
      <div class="flex items-center justify-between text-xs mt-2" style="color:#7fa3c8">
        <span>market avg 1Y return</span>
        <span>›</span>
      </div>
    </div>

    <!-- Tile 4: Costs -->
    <div class="stat-card bg-[#233d66] rounded-xl border border-[#2e5285] p-5 cursor-pointer" onclick="goCard('expense')">
      <p class="text-xs font-semibold uppercase tracking-wider" style="color:#7fa3c8">COSTS</p>
      <p id="c-exp" class="text-2xl font-bold text-white mt-1 leading-tight">—</p>
      <hr class="border-[#2e5285] my-2">
      <div class="flex items-center justify-between text-xs" style="color:#a8c4e0">
        <span>Avg spread</span>
        <span id="c-nav" class="tabular-nums font-medium">—</span>
      </div>
      <div class="flex items-center justify-between text-xs mt-2" style="color:#7fa3c8">
        <span>fee + implicit trading cost</span>
        <span>›</span>
      </div>
    </div>

    <!-- Tile 5: Issuers -->
    <div class="stat-card bg-[#233d66] rounded-xl border border-[#2e5285] p-5 cursor-pointer" onclick="goCard('issuers')">
      <p class="text-xs font-semibold uppercase tracking-wider" style="color:#7fa3c8">ISSUERS</p>
      <p id="c-issuers" class="text-2xl font-bold text-white mt-1 leading-tight">—</p>
      <hr class="border-[#2e5285] my-2">
      <div class="flex items-center justify-between text-xs" style="color:#a8c4e0">
        <span id="c-most-fum-issuer">—</span>
      </div>
      <div class="flex items-center justify-between text-xs mt-2" style="color:#7fa3c8">
        <span id="c-most-etfs-issuer">—</span>
        <span>›</span>
      </div>
    </div>

  </div>

  <!-- ── Articles carousel ── -->
  <div class="mb-5">
    <div class="flex items-center justify-between mb-2 px-0.5">
      <span class="text-xs font-semibold text-gray-500 uppercase tracking-wider">Latest Articles</span>
      <a href="/articles" class="text-xs text-blue-500 hover:text-blue-700 font-medium">View all →</a>
    </div>
    <div class="relative">
      <div id="article-carousel" class="flex gap-3 overflow-x-auto snap-x snap-mandatory scroll-smooth pb-1"
           style="scrollbar-width:none;-ms-overflow-style:none;">
        <!--ARTICLE_CAROUSEL-->
      </div>
      <button onclick="carouselScroll(-1)"
              class="hidden lg:flex absolute left-0 top-1/2 -translate-y-1/2 -translate-x-4
                     w-8 h-8 items-center justify-center rounded-full bg-white shadow border
                     border-gray-200 text-gray-500 hover:text-blue-600 z-10">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="15 18 9 12 15 6"/></svg>
      </button>
      <button onclick="carouselScroll(1)"
              class="hidden lg:flex absolute right-0 top-1/2 -translate-y-1/2 translate-x-4
                     w-8 h-8 items-center justify-center rounded-full bg-white shadow border
                     border-gray-200 text-gray-500 hover:text-blue-600 z-10">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="9 18 15 12 9 6"/></svg>
      </button>
    </div>
  </div>

  <!-- ── Main nav tabs ── -->
  <div class="bg-white rounded-xl shadow-sm border border-gray-100 mb-5 px-5">
    <div class="flex gap-8 text-sm overflow-x-auto">
      <button class="main-tab active" data-view="screener">Screener</button>
      <button class="main-tab" data-view="compare">Compare</button>
      <button class="main-tab" data-view="holdings">Holdings Search</button>
      <button class="main-tab" data-view="issuers">Issuers</button>
      <button class="main-tab" data-view="analytics">Analytics</button>
      <a href="/articles" class="main-tab flex items-center gap-1" style="text-decoration:none">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10 9 9 9 8 9"/></svg>
        Articles
      </a>
      <a href="/insights/upcoming" class="main-tab flex items-center gap-1" style="text-decoration:none">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>
        New Listings
      </a>
      <a href="/screener/preferences" class="main-tab flex items-center gap-1" style="text-decoration:none">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14"/><path d="M4.93 4.93a10 10 0 0 0 0 14.14"/></svg>
        Portfolio Builder
      </a>
    </div>
  </div>

  <!-- ══════════════════════════════ VIEW: Screener ══════════════════════════════ -->
  <div id="view-screener">

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
            <label class="block text-xs text-gray-500 mb-1">Management Style</label>
            <select id="f-type"
                    class="w-full border border-gray-200 rounded-lg px-2.5 py-2 text-sm bg-gray-50
                           focus:ring-2 focus:ring-blue-200 focus:border-blue-400 outline-none">
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
                          focus:ring-2 focus:ring-blue-200 focus:border-blue-400 outline-none"/>
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
          <div>
            <label class="block text-xs text-gray-500 mb-1">Extra Columns</label>
            <div class="space-y-1">
              <label class="flex items-center gap-2 cursor-pointer text-sm text-gray-700">
                <input id="col-3y" type="checkbox" class="accent-blue-600">
                3Y Return
              </label>
              <label class="flex items-center gap-2 cursor-pointer text-sm text-gray-700">
                <input id="col-5y" type="checkbox" class="accent-blue-600">
                5Y Return
              </label>
            </div>
          </div>
          <div>
            <label class="block text-xs text-gray-500 mb-1">Max Management Fee: <span id="f-fee-val" class="font-semibold text-gray-700">2.00%</span></label>
            <input id="f-max-fee" type="range" min="0" max="2" step="0.05" value="2"
                   class="w-full accent-blue-600">
          </div>
          <div>
            <label class="block text-xs text-gray-500 mb-1">Min Fund Size (AUD M)</label>
            <input id="f-min-fum" type="number" min="0" placeholder="e.g. 100"
                   class="w-full border border-gray-200 rounded-lg px-2.5 py-2 text-sm bg-gray-50
                          focus:ring-2 focus:ring-blue-200 focus:border-blue-400 outline-none">
          </div>
          <div>
            <label class="block text-xs text-gray-500 mb-1">1Y Return (%)</label>
            <div class="flex gap-2">
              <input id="f-ret-min" type="number" placeholder="Min"
                     class="w-full border border-gray-200 rounded-lg px-2 py-1.5 text-sm bg-gray-50
                            focus:ring-2 focus:ring-blue-200 focus:border-blue-400 outline-none">
              <input id="f-ret-max" type="number" placeholder="Max"
                     class="w-full border border-gray-200 rounded-lg px-2 py-1.5 text-sm bg-gray-50
                            focus:ring-2 focus:ring-blue-200 focus:border-blue-400 outline-none">
            </div>
          </div>
          <div>
            <label class="block text-xs text-gray-500 mb-1">Min Distribution Yield (%)</label>
            <input id="f-min-yield" type="number" min="0" placeholder="e.g. 3"
                   class="w-full border border-gray-200 rounded-lg px-2.5 py-1.5 text-sm bg-gray-50
                          focus:ring-2 focus:ring-blue-200 focus:border-blue-400 outline-none">
          </div>
          <label class="flex items-center gap-2 cursor-pointer text-sm text-gray-700">
            <input id="f-hedged" type="checkbox" class="accent-blue-600">
            FX Hedged only
          </label>
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
          <h3 class="font-semibold text-gray-700">Screener</h3>
          <div class="flex items-center gap-3">
            <span id="result-count" class="text-xs text-gray-400"></span>
            <button id="sc-export"
                    class="flex items-center gap-1 px-3 py-1.5 border border-gray-200 rounded-lg
                           text-xs text-gray-600 hover:border-blue-400 hover:text-blue-600 transition-colors">
              &#8595; CSV
            </button>
          </div>
        </div>
        <div class="overflow-x-auto" style="max-height:520px;overflow-y:auto">
          <table class="w-full">
            <thead id="etf-thead" class="bg-gray-50 text-xs font-semibold text-gray-500 uppercase tracking-wide"
                   style="position:sticky;top:0;z-index:5">
              <tr>
                <th class="px-3 py-2.5 text-left w-8 cursor-pointer select-none hover:text-gray-700" data-sort="rank">#</th>
                <th class="px-3 py-2.5 text-left cursor-pointer select-none hover:text-gray-700" style="min-width:260px" data-sort="code">ETF</th>
                <th class="px-3 py-2.5 text-center select-none" style="width:90px">Class</th>
                <th class="px-3 py-2.5 text-right cursor-pointer select-none hover:text-gray-700" data-sort="price">Price</th>
                <th class="px-3 py-2.5 text-right cursor-pointer select-none hover:text-gray-700" data-sort="fum">Total FUM</th>
                <th class="px-3 py-2.5 text-right cursor-pointer select-none hover:text-gray-700 text-gray-500" data-sort="chess_fum">CHESS FUM</th>
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
        <button class="dtab" data-tab="performance">Performance</button>
        <button class="dtab" data-tab="holdings">Holdings</button>
        <button class="dtab" data-tab="sectors">Sectors</button>
        <button class="dtab" data-tab="dividends">Dividends</button>
      </div>
      <div id="tab-content"></div>
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
                        focus:ring-2 focus:ring-blue-200 focus:border-blue-400 outline-none">
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

    <!-- ── Concentration widget ── -->
    <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-5 mb-4">
      <div class="flex items-center justify-between mb-4 flex-wrap gap-3">
        <div>
          <h3 class="font-semibold text-gray-700">Portfolio Concentration</h3>
          <p class="text-xs text-gray-400 mt-0.5">Most and least concentrated ETFs by holdings, geography and GICS sector</p>
        </div>
        <div class="flex gap-1 bg-gray-100 rounded-lg p-1">
          <button class="conc-tab-btn px-3 py-1.5 text-xs rounded-md font-medium bg-white text-blue-700 shadow-sm" data-conc="count">Holdings Count</button>
          <button class="conc-tab-btn px-3 py-1.5 text-xs rounded-md font-medium text-gray-500 hover:text-gray-700" data-conc="geo">Geography</button>
          <button class="conc-tab-btn px-3 py-1.5 text-xs rounded-md font-medium text-gray-500 hover:text-gray-700" data-conc="sector">GICS Sectors</button>
        </div>
      </div>
      <div id="conc-loading" class="py-8 text-center text-gray-400 text-sm">Loading concentration data…</div>
      <div id="conc-content" class="hidden">
        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
          <div>
            <div class="text-xs font-semibold text-orange-600 uppercase tracking-wide px-4 py-2 bg-orange-50 rounded-t-lg border border-orange-100 border-b-0">Most Concentrated</div>
            <div id="conc-left" class="border border-gray-100 rounded-b-lg overflow-hidden"></div>
          </div>
          <div>
            <div class="text-xs font-semibold text-green-700 uppercase tracking-wide px-4 py-2 bg-green-50 rounded-t-lg border border-green-100 border-b-0">Most Diversified</div>
            <div id="conc-right" class="border border-gray-100 rounded-b-lg overflow-hidden"></div>
          </div>
        </div>
      </div>
    </div>

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
                        focus:ring-2 focus:ring-blue-200 focus:border-blue-400 outline-none">
        </div>
        <div class="flex items-center gap-2 text-sm text-gray-500">
          <label class="text-xs">Min Weight %</label>
          <input id="hs-min-weight" type="number" min="0" step="0.1" value="0" placeholder="0"
                 class="w-20 border border-gray-200 rounded-lg px-2 py-2 text-sm bg-gray-50
                        focus:ring-2 focus:ring-blue-200 focus:border-blue-400 outline-none">
        </div>
      </div>
    </div>

    <!-- Summary + results -->
    <div id="hs-summary" class="hidden bg-blue-50 border border-blue-100 rounded-xl px-5 py-3 mb-4 text-sm text-blue-800"></div>

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
  <!-- ── Issuers tab ── -->
  <div id="view-issuers" class="hidden">
    <!-- Summary stats -->
    <div id="iss-summary" class="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-5"></div>
    <!-- Search/filter bar -->
    <div class="bg-white rounded-xl shadow-sm border border-gray-100 px-4 py-3 mb-5 flex items-center gap-3 flex-wrap">
      <input id="iss-search" type="text" placeholder="Search issuers…"
             class="border border-gray-200 rounded-lg px-3 py-1.5 text-sm bg-gray-50
                    focus:ring-2 focus:ring-blue-300 outline-none w-52">
      <label class="flex items-center gap-1.5 text-sm text-gray-600 cursor-pointer ml-auto">
        <input type="checkbox" id="iss-hide-small" class="rounded">
        Hide issuers with &lt;5 ETFs
      </label>
    </div>
    <!-- Issuer cards grid -->
    <div id="iss-grid" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4"></div>
  </div><!-- /view-issuers -->

  <div id="view-analytics" class="hidden">

    <!-- Sub-nav -->
    <div class="flex gap-2 mb-5 flex-wrap" id="an-subnav">
      <button class="an-sub-btn px-3 py-1.5 text-xs rounded-lg border font-medium bg-blue-600 text-white border-blue-600" data-asub="overview">Overview</button>
      <button class="an-sub-btn px-3 py-1.5 text-xs rounded-lg border font-medium border-gray-200 text-gray-600 hover:border-blue-400" data-asub="industry">Industry Growth</button>
      <button class="an-sub-btn px-3 py-1.5 text-xs rounded-lg border font-medium border-gray-200 text-gray-600 hover:border-blue-400" data-asub="issuers">Issuer Market Share</button>
      <button class="an-sub-btn px-3 py-1.5 text-xs rounded-lg border font-medium border-gray-200 text-gray-600 hover:border-blue-400" data-asub="assetclass">Asset Classes</button>
      <button class="an-sub-btn px-3 py-1.5 text-xs rounded-lg border font-medium border-gray-200 text-gray-600 hover:border-blue-400" data-asub="fund">Fund Deep Dive</button>
      <button class="an-sub-btn px-3 py-1.5 text-xs rounded-lg border font-medium border-gray-200 text-gray-600 hover:border-blue-400" data-asub="flows">Flows</button>
      <button class="an-sub-btn px-3 py-1.5 text-xs rounded-lg border font-medium border-gray-200 text-gray-600 hover:border-blue-400" data-asub="launches">Launches</button>
    </div>

    <!-- ── Overview ── -->
    <div id="asub-overview">
      <!-- Leaderboard cards -->
      <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
        <div class="bg-white rounded-xl shadow-sm border border-gray-100">
          <div class="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
            <h3 class="font-semibold text-gray-700 text-sm">Top Performers</h3>
            <span class="text-xs text-gray-400 bg-gray-50 px-2 py-0.5 rounded-full">1Y Return</span>
          </div>
          <div id="an-performers" class="divide-y divide-gray-50">
            <div class="flex justify-center py-8"><div class="spinner"></div></div>
          </div>
        </div>
        <div class="bg-white rounded-xl shadow-sm border border-gray-100">
          <div class="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
            <h3 class="font-semibold text-gray-700 text-sm">Highest Yield</h3>
            <span class="text-xs text-gray-400 bg-gray-50 px-2 py-0.5 rounded-full">Distribution</span>
          </div>
          <div id="an-yield" class="divide-y divide-gray-50">
            <div class="flex justify-center py-8"><div class="spinner"></div></div>
          </div>
        </div>
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
            <p class="text-xs font-semibold text-blue-600 uppercase tracking-wide mb-2">Top Inflows</p>
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
    </div><!-- /asub-overview -->

    <!-- ── Industry Growth ── -->
    <div id="asub-industry" class="hidden">
      <!-- ASX vs Cboe snapshot -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4">
        <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
          <h3 class="font-semibold text-gray-700 text-sm mb-1">Market Cap by Exchange</h3>
          <p class="text-xs text-gray-400 mb-3">Current FUM split between ASX and Cboe Australia</p>
          <div style="height:220px" class="flex items-center justify-center">
            <canvas id="exch-donut" style="max-height:220px"></canvas>
          </div>
        </div>
        <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4 flex flex-col justify-center">
          <div id="exch-stats" class="space-y-4"></div>
        </div>
      </div>
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4">
        <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
          <h3 class="font-semibold text-gray-700 text-sm mb-1">Industry AUM ($B)</h3>
          <p class="text-xs text-gray-400 mb-3">Total market cap of all Australian ETFs</p>
          <div style="height:260px"><canvas id="hist-industry-aum"></canvas></div>
        </div>
        <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
          <h3 class="font-semibold text-gray-700 text-sm mb-1">ETF Count &amp; Monthly Flows</h3>
          <p class="text-xs text-gray-400 mb-3">Number of listed ETFs and net fund flows</p>
          <div style="height:260px"><canvas id="hist-industry-count"></canvas></div>
        </div>
      </div>
      <div class="flex items-start gap-3 bg-slate-50 border border-slate-200 rounded-xl px-4 py-3 mb-4">
        <div class="mt-0.5 shrink-0">
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" class="text-slate-400">
            <line x1="2" y1="8" x2="14" y2="8" stroke="#94a3b8" stroke-width="1.5" stroke-dasharray="3 2"/>
            <circle cx="8" cy="8" r="2.5" fill="#94a3b8"/>
          </svg>
        </div>
        <div>
          <p class="text-xs font-semibold text-slate-600 mb-0.5">About the grey dashed line</p>
          <p class="text-xs text-slate-500 leading-relaxed">
            Several funds listed on the ASX as <strong>quoted managed funds</strong> (e.g. MGOC, DACE, DGCE) transferred existing investor assets onto the exchange at admission rather than raising new capital.
            This causes their admission-month funds flow to equal their entire starting AUM — a one-off structural event, not genuine investor inflows.
            The <span class="font-medium text-slate-600">orange line</span> excludes these admission-month figures so ordinary flow trends are clearly visible.
            The <span class="font-medium text-slate-600">grey dashed line</span> shows the raw figure including admission AUM — hover over a grey data point to see which fund caused the spike.
          </p>
        </div>
      </div>
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
        <h3 class="font-semibold text-gray-700 text-sm mb-1">Annual Net Flows &amp; Traded Value</h3>
        <p class="text-xs text-gray-400 mb-3">Calendar-year aggregates</p>
        <div style="height:220px"><canvas id="hist-industry-flows"></canvas></div>
      </div>
    </div><!-- /asub-industry -->

    <!-- ── Issuer Market Share ── -->
    <div id="asub-issuers" class="hidden">
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4 mb-4">
        <h3 class="font-semibold text-gray-700 text-sm mb-1">Issuer AUM Over Time ($B)</h3>
        <p class="text-xs text-gray-400 mb-3">Stacked by top 10 issuers · monthly data since Jul 2013</p>
        <div style="height:320px"><canvas id="hist-issuer-stacked"></canvas></div>
      </div>
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
        <h3 class="font-semibold text-gray-700 text-sm mb-1">Current Issuer Rankings</h3>
        <div id="hist-issuer-table" class="overflow-x-auto"></div>
      </div>
    </div><!-- /asub-issuers -->

    <!-- ── Asset Classes ── -->
    <div id="asub-assetclass" class="hidden">
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4 mb-4">
        <h3 class="font-semibold text-gray-700 text-sm mb-1">Asset Class AUM Over Time ($B)</h3>
        <p class="text-xs text-gray-400 mb-3">Stacked by asset class · monthly data since Jul 2013</p>
        <div style="height:320px"><canvas id="hist-assetclass-stacked"></canvas></div>
      </div>
    </div><!-- /asub-assetclass -->

    <!-- ── Fund Deep Dive ── -->
    <div id="asub-fund" class="hidden">
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4 mb-4">
        <div class="flex gap-3 items-center flex-wrap mb-4">
          <input id="hist-fund-input" type="text" placeholder="Enter ASX code e.g. VAS, NDQ, IVV&hellip;"
            class="border border-gray-200 rounded-lg px-3 py-2 text-sm flex-1 max-w-xs focus:outline-none focus:ring-2 focus:ring-blue-200">
          <button id="hist-fund-btn"
            class="px-4 py-2 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 font-medium">
            Load
          </button>
          <span class="text-xs text-gray-400">or click any ETF in the Screener to deep-dive here</span>
        </div>
        <div id="hist-fund-result"></div>
      </div>
    </div><!-- /asub-fund -->

    <!-- ── Flows ── -->
    <div id="asub-flows" class="hidden">
      <div class="flex gap-2 mb-4 flex-wrap items-center">
        <span class="text-xs text-gray-500 font-medium">Period:</span>
        <button class="hist-flow-period px-3 py-1 text-xs rounded-lg border font-medium border-gray-200 text-gray-600 hover:border-blue-400" data-months="3">3 months</button>
        <button class="hist-flow-period px-3 py-1 text-xs rounded-lg border font-medium border-gray-200 text-gray-600 hover:border-blue-400" data-months="6">6 months</button>
        <button class="hist-flow-period px-3 py-1 text-xs rounded-lg border font-medium bg-blue-600 text-white border-blue-600" data-months="12">12 months</button>
        <button class="hist-flow-period px-3 py-1 text-xs rounded-lg border font-medium border-gray-200 text-gray-600 hover:border-blue-400" data-months="24">24 months</button>
        <button class="hist-flow-period px-3 py-1 text-xs rounded-lg border font-medium border-gray-200 text-gray-600 hover:border-blue-400" data-months="36">36 months</button>
      </div>
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
          <h3 class="font-semibold text-gray-700 text-sm mb-3">Top Inflows</h3>
          <div id="hist-flows-in"></div>
        </div>
        <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
          <h3 class="font-semibold text-gray-700 text-sm mb-3">Top Outflows</h3>
          <div id="hist-flows-out"></div>
        </div>
      </div>
    </div><!-- /asub-flows -->

    <!-- ── Launches ── -->
    <div id="asub-launches" class="hidden">
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
          <h3 class="font-semibold text-gray-700 text-sm mb-1">ETF Launches by Year</h3>
          <p class="text-xs text-gray-400 mb-3">New ETFs admitted to ASX per calendar year</p>
          <div style="height:280px"><canvas id="hist-launches-bar"></canvas></div>
        </div>
        <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
          <h3 class="font-semibold text-gray-700 text-sm mb-3">All-Time Launches by Issuer</h3>
          <div id="hist-launches-issuer"></div>
        </div>
      </div>
    </div><!-- /asub-launches -->

  </div><!-- /view-analytics -->

</main>

<script>
/* ======================================================= state */
const API = '';
let page = 0, pageSize = 50, selectedCode = null;
let tableSortKey = 'rank', tableSortDir = 'asc';
let topPerformerCode = null;

const PALETTE = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6',
                 '#ec4899','#06b6d4','#84cc16','#f97316','#6366f1'];

/* ─── Issuer brand identity ─── */
const ISSUER_COLORS = {
  'BetaShares':      '#FF6B35',
  'Vanguard':        '#8B1A1A',
  'iShares':         '#009CDE',
  'VanEck':          '#1A3C8F',
  'State Street Investment Management': '#1a9dd9',
  'SPDR':            '#1a9dd9',
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
  'State Street Investment Management': 'ssga.com',
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
function slugify(name) {
  return name.toLowerCase()
    .replace(/\s*\/\s*/g, '-').replace(/\s*&\s*/g, '-')
    .replace(/\./g, '').replace(/'/g, '')
    .replace(/\s+/g, '-')
    .replace(/[^a-z0-9-]/g, '').replace(/-+/g, '-').replace(/^-|-$/g, '');
}
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
  'International Equities': '#6366f1',
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
  if (m.largest_etf) {
    document.getElementById('c-largest-etf').textContent =
      m.largest_etf.code + ' ' + fmtFum(m.largest_etf.fund_size_aud_millions);
  }
  if (m.largest_issuer_fum) {
    document.getElementById('c-largest-issuer').textContent =
      m.largest_issuer_fum.name + ' ' + m.largest_issuer_fum.market_share_pct + '%';
  }
  document.getElementById('c-count').textContent = (m.total_etfs || 0).toLocaleString();
  document.getElementById('c-exchange-split').textContent =
    'ASX ' + (m.asx_count || '—') + ' · CXA ' + (m.cxa_count || '—');
  const ret = document.getElementById('c-ret');
  ret.textContent = pct(m.avg_return_1y);
  ret.className = 'text-2xl font-bold mt-1 leading-tight ' + pctCls(m.avg_return_1y);
  if (m.top_performer) {
    document.getElementById('c-top').textContent =
      m.top_performer.code + ' ' + pct(m.top_performer.return_1y);
    topPerformerCode = m.top_performer.code;
  }
  document.getElementById('c-exp').textContent =
    (m.fum_weighted_mer || m.avg_expense_ratio || 0).toFixed(2) + '%';
  if (m.avg_premium_discount != null) {
    const navEl = document.getElementById('c-nav');
    const v = m.avg_premium_discount;
    navEl.textContent = (v >= 0 ? '+' : '') + v.toFixed(2) + '%';
  }
  if (m.largest_issuer_fum) {
    document.getElementById('c-most-fum-issuer').textContent =
      m.largest_issuer_fum.name + ' ' + fmtFum(m.largest_issuer_fum.total_fum);
  }
  if (m.most_etfs_issuer) {
    document.getElementById('c-most-etfs-issuer').textContent =
      m.most_etfs_issuer.name + ' ' + m.most_etfs_issuer.etf_count + ' ETFs';
  }
  if (m.total_issuers) {
    document.getElementById('c-issuers').textContent = m.total_issuers;
  }
  // Upcoming listings stat card (best-effort, non-blocking)
  try {
    const up = await api('/api/v1/insights/upcoming');
    const n = (up.stats || {}).upcoming_count || 0;
    document.getElementById('c-upcoming-count').textContent = n > 0 ? n : '—';
  } catch (_) {}
  const now = new Date().toLocaleTimeString();
  document.getElementById('subtitle').textContent =
    `${(m.total_etfs || 0).toLocaleString()} ETFs · ${fmtFum(m.total_fum_millions)} total FUM · ${now}`;
  document.getElementById('last-refresh').textContent = 'Live · ' + now;
}

/* ======================================================= stat card navigation */
function goCard(type) {
  switch (type) {
    case 'fum':           window.location.href = '/insights/total-market'; break;
    case 'asset-classes': window.location.href = '/insights/asset-classes'; break;
    case 'listings':      window.location.href = '/insights/listings';      break;
    case 'returns':       window.location.href = '/insights/returns';       break;
    case 'expense':       window.location.href = '/insights/expense';       break;
    case 'issuers':       window.location.href = '/insights/issuers';       break;
    // legacy aliases
    case 'count':    window.location.href = '/insights/listings';  break;
    case 'return':   window.location.href = '/insights/returns';   break;
    case 'top':      window.location.href = '/insights/returns';   break;
    case 'nav':      window.location.href = '/insights/expense';   break;
    case 'upcoming': window.location.href = '/insights/listings';  break;
  }
}

/* ======================================================= table */
let lastTableData = [];
async function loadTable() {
  const params = new URLSearchParams();
  const ex   = document.getElementById('f-exchange').value;
  const iss  = document.getElementById('f-issuer').value;
  const ac   = document.getElementById('f-asset').value;
  const ft   = document.getElementById('f-type').value;
  const bm   = document.getElementById('f-benchmark').value.trim();
  const maxFee  = parseFloat(document.getElementById('f-max-fee').value);
  const minFum  = document.getElementById('f-min-fum').value.trim();
  const retMin  = document.getElementById('f-ret-min').value.trim();
  const retMax  = document.getElementById('f-ret-max').value.trim();
  const minYld  = document.getElementById('f-min-yield').value.trim();
  const hedged  = document.getElementById('f-hedged').checked;
  if (ex)  params.set('exchange',    ex);
  if (iss) params.set('issuer',      iss);
  if (ac)  params.set('asset_class', ac);
  if (ft)  params.set('fund_type',   ft);
  if (bm)  params.set('benchmark',   bm);
  if (maxFee < 2) params.set('max_fee', maxFee);
  if (minFum)  params.set('min_fum',       minFum);
  if (retMin)  params.set('min_return_1y', retMin);
  if (retMax)  params.set('max_return_1y', retMax);
  if (minYld)  params.set('min_yield',     minYld);
  if (hedged)  params.set('fx_hedged',     '1');
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
    th.classList.toggle('text-blue-600', isActive);
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
  lastTableData = etfs;
  tbody.innerHTML = etfs.map(e => {
    const sel = e.code === selectedCode ? 'row-selected' : '';
    const chess = chessFum(e);
    const chessTip = chess != null
      ? 'CHESS FUM = ' + (e.units_on_issue ? e.units_on_issue.toLocaleString() + ' units' : '') + ' × $' + (e.current_price || '?')
      : 'Units on issue not available';
    return `<tr class="cursor-pointer ${sel}" data-code="${e.code}">
      <td class="px-3 py-2.5 text-gray-300 font-mono text-xs">${e.rank_by_fum || '—'}</td>
      <td class="px-3 py-2.5" style="min-width:260px">
        <div class="font-bold text-gray-900">${e.code}</div>
        <div class="text-xs text-gray-400 truncate" style="max-width:240px" title="${e.name || ''}">${e.name || ''}</div>
        ${e.benchmark ? `<div class="text-xs text-indigo-400 truncate" style="max-width:240px" title="${e.benchmark}">&#8594; ${e.benchmark}</div>` : ''}
      </td>
      <td class="px-3 py-2.5 text-center" style="width:90px">${acChip(e.asset_class)}</td>
      <td class="px-3 py-2.5 text-right font-mono">
        <span class="dated" title="Price · ${fmtTs(e.last_updated)}">${money(e.current_price)}</span>
      </td>
      <td class="px-3 py-2.5 text-right tabular-nums" title="Total FUM · ASX Monthly Report">
        ${fmtFum(e.fund_size_aud_millions)}
      </td>
      <td class="px-3 py-2.5 text-right tabular-nums text-indigo-500" title="${chessTip}">
        ${chess != null ? fmtFum(chess) : '<span class="text-gray-300">—</span>'}
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
    { label: 'Issuer',    value: d.issuer ? `<a href="/issuers/${slugify(d.issuer)}" class="text-blue-500 hover:underline" onclick="event.stopPropagation()">${d.issuer}</a>` : '—' },
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
  const _is = 'Issuer website';

  function card(label, val, numOrNull, tip) {
    const cls = numOrNull != null ? pctCls(numOrNull) : 'text-gray-800';
    return `<div class="bg-slate-50 rounded-lg p-3 border border-gray-100" ${tip ? `title="${tip}"` : ''}>
      <p class="text-gray-400 text-xs">${label}</p>
      <p class="font-semibold text-sm mt-0.5 ${cls}">${val}</p>
    </div>`;
  }

  function section(title, cardsHtml) {
    return `<div class="mb-4">
      <p class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">${title}</p>
      <div class="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-2.5">${cardsHtml}</div>
    </div>`;
  }

  // AI Summary
  let aiHtml = '';
  if (d.summary) {
    let s = null;
    try { s = typeof d.summary === 'string' ? JSON.parse(d.summary) : d.summary; } catch(e) {}
    if (s) {
      const risksHtml = Array.isArray(s.key_risks) && s.key_risks.length
        ? `<details class="mt-2"><summary class="text-xs text-blue-600 cursor-pointer hover:underline select-none">Key risks (${s.key_risks.length})</summary>
             <ul class="mt-1.5 space-y-1 list-disc list-inside">${s.key_risks.map(r=>`<li class="text-xs text-gray-600">${r}</li>`).join('')}</ul></details>` : '';
      aiHtml = `<div class="bg-blue-50 border border-blue-100 rounded-lg px-4 py-3 mb-3">
        <p class="text-blue-500 text-xs font-medium uppercase tracking-wide mb-1.5">AI Summary</p>
        ${s.summary ? `<p class="text-sm text-gray-700 leading-relaxed">${s.summary}</p>` : ''}
        ${s.objective ? `<p class="mt-2 text-xs text-gray-500"><span class="font-medium text-gray-600">Objective:</span> ${s.objective}</p>` : ''}
        ${s.suitable_for ? `<p class="mt-1 text-xs text-gray-500"><span class="font-medium text-gray-600">Suitable for:</span> ${s.suitable_for}</p>` : ''}
        ${risksHtml}
        <p class="mt-2 text-xs text-gray-400 italic">Generated by AI from PDS — not financial advice.</p>
      </div>`;
    }
  }

  // Doc links
  const docLinks = [['PDS',d.pds_url],['TMD',d.tmd_url],['Fact Sheet',d.factsheet_url]].filter(([,u])=>u);
  const docHtml = docLinks.length ? `<div class="flex flex-wrap gap-2 mb-3">
    ${docLinks.map(([lbl,url])=>`<a href="${url}" target="_blank" rel="noopener"
       class="inline-flex items-center gap-1 px-3 py-1.5 bg-[#233d66] border border-[#2e5285] rounded-full text-xs font-medium text-slate-300 hover:border-blue-400 hover:text-blue-400 transition-colors">&#128196; ${lbl}</a>`).join('')}
  </div>` : '';

  // Benchmark
  const bmHtml = d.benchmark ? `<div class="bg-indigo-50 border border-indigo-100 rounded-lg px-4 py-3 mb-3 flex items-center gap-2">
    <span class="text-indigo-300 text-lg">&#8594;</span>
    <div><p class="text-indigo-400 text-xs font-medium uppercase tracking-wide">Tracked Index</p>
    <p class="font-semibold text-indigo-900 text-sm">${d.benchmark}</p></div>
  </div>` : '';

  // Section: Price & Market
  const priceCards = [
    card('Price', money(d.current_price), null, _po),
    card('Day Change', pct(d.day_change_pct), d.day_change_pct, _po),
    card('NAV / Unit', d.nav_per_unit != null ? '$' + d.nav_per_unit.toFixed(4) : '—', null, _is),
    card('Prem / Disc', d.premium_discount_pct != null ? d.premium_discount_pct.toFixed(2) + '%' : '—', d.premium_discount_pct, _po),
    card('52W High', money(d.year_high), null, _po),
    card('52W Low', money(d.year_low), null, _po),
    card('Volume', d.volume != null ? d.volume.toLocaleString() : '—', null, _po),
  ].join('');

  // Section: Fund Size
  const fumFormatted = d.fund_size_aud_millions != null
    ? (d.fund_size_aud_millions >= 1000 ? '$' + (d.fund_size_aud_millions/1000).toFixed(2) + 'B' : '$' + d.fund_size_aud_millions.toFixed(0) + 'M')
    : '—';
  const netAssetsFormatted = d.net_assets_aud != null
    ? (d.net_assets_aud >= 1e9 ? '$' + (d.net_assets_aud/1e9).toFixed(2) + 'B' : '$' + (d.net_assets_aud/1e6).toFixed(0) + 'M')
    : '—';
  const chessMcFormatted = d.chess_mc != null
    ? (d.chess_mc >= 1e9 ? '$' + (d.chess_mc/1e9).toFixed(2) + 'B' : '$' + (d.chess_mc/1e6).toFixed(0) + 'M')
    : '—';
  const unitsChange = d.units_change != null
    ? (d.units_change > 0 ? '+' : '') + d.units_change.toLocaleString()
    : null;
  const sizeCards = [
    card('Total FUM', fumFormatted, null, 'Fund size from issuer or ASX report'),
    card('Net Assets', netAssetsFormatted, null, 'Net assets from issuer website'),
    card('CHESS Mkt Cap', chessMcFormatted, null, 'CHESS-settled market cap · ASX monthly data'),
    card('Units on Issue', d.units_on_issue != null ? d.units_on_issue.toLocaleString() : '—', null, _is),
    card('Units Change', unitsChange ? unitsChange : '—', null, d.units_change_date && d.units_change_prev_date ? `Change from ${d.units_change_prev_date} to ${d.units_change_date}` : 'Requires 2+ unit history records'),
    card('CHESS Holders', d.chess_holders != null ? d.chess_holders.toLocaleString() : '—', null, 'CHESS registered holders · ASX monthly data'),
  ].join('');

  // Section: Costs & Trading
  const costsCards = [
    card('MER', d.expense_ratio != null ? d.expense_ratio.toFixed(2) + '% p.a.' : '—', null, 'Management expense ratio'),
    card('Bid/Ask Spread', d.bid_ask_spread_pct != null ? d.bid_ask_spread_pct + '%' : '—', null, _po),
    card('Buy Spread', d.buy_spread != null ? d.buy_spread.toFixed(3) + '%' : '—', null, 'Issuer buy spread'),
    card('Sell Spread', d.sell_spread != null ? d.sell_spread.toFixed(3) + '%' : '—', null, 'Issuer sell spread'),
  ].join('');

  // Section: Returns
  const retCards = [
    card('1M Return',  pct(d.return_1m), d.return_1m, _ao),
    card('3M Return',  pct(d.return_3m), d.return_3m, _ao),
    card('6M Return',  pct(d.return_6m), d.return_6m, _ao),
    card('1Y Return',  pct(d.return_1y), d.return_1y, _ao),
    card('3Y Return',  pct(d.return_3y), d.return_3y, _ao),
    card('5Y Return',  pct(d.return_5y), d.return_5y, _ao),
    d.return_since_inception != null ? card('Since Inception', pct(d.return_since_inception), d.return_since_inception, _ao) : '',
  ].join('');

  // Section: Distributions
  const distCards = [
    card('Dist. Yield', d.distribution_yield != null ? d.distribution_yield.toFixed(2) + '%' : '—', null, _is),
    card('Frequency', d.distribution_frequency || '—', null),
    card('Last Dist.', d.last_distribution_amount != null ? '$' + d.last_distribution_amount.toFixed(4) : '—', null, d.last_distribution_date || ''),
    card('Franking', d.franking_pct != null ? d.franking_pct.toFixed(0) + '%' : '—', null),
  ].join('');

  // Section: Fund Info
  const infoCards = [
    card('Inception', d.inception_date || '—'),
    card('Asset Class', d.asset_class || '—'),
    card('Exchange', d.exchange || '—'),
    card('Replication', d.replication_method || '—'),
    card('FX Hedged', d.fx_hedged ? 'Yes' : 'No'),
  ].join('');

  document.getElementById('tab-content').innerHTML = `
    ${aiHtml}${docHtml}${bmHtml}
    ${section('Price & Market', priceCards)}
    ${section('Fund Size', sizeCards)}
    ${section('Costs & Trading', costsCards)}
    ${section('Returns', retCards)}
    ${section('Distributions', distCards)}
    ${section('Fund Info', infoCards)}
    ${d.description ? `<p class="mt-4 text-sm text-gray-600 leading-relaxed border-t pt-4">${d.description}</p>` : ''}
    ${d.issuer_url ? `<a href="${d.issuer_url}" target="_blank" rel="noopener"
       class="mt-3 inline-flex items-center gap-1 text-blue-600 hover:underline text-sm">View on issuer site &#8594;</a>` : ''}`;
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
      ${h.holdings_disclosure === 'quarterly' ? `
      <div class="flex items-start gap-2 bg-amber-50 border border-amber-200 rounded-lg px-3 py-2 mb-3 text-xs text-amber-800">
        <svg class="shrink-0 mt-0.5" width="14" height="14" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M8.485 2.495c.673-1.167 2.357-1.167 3.03 0l6.28 10.875c.673 1.167-.17 2.625-1.516 2.625H3.72c-1.347 0-2.189-1.458-1.515-2.625L8.485 2.495zM10 5a.75.75 0 01.75.75v3.5a.75.75 0 01-1.5 0v-3.5A.75.75 0 0110 5zm0 9a1 1 0 100-2 1 1 0 000 2z" clip-rule="evenodd"/></svg>
        <span><strong>Quarterly disclosure</strong> — This is an active/complex ETF listed on Cboe Australia. Full portfolio holdings are publicly disclosed once per quarter, up to 60 days after quarter-end.${h.holdings_as_of ? ` Holdings shown are as of <strong>${h.holdings_as_of}</strong>.` : ''}</span>
      </div>` : ''}
      <div class="flex flex-wrap items-center gap-2 mb-3">
        <span class="bg-blue-50 text-blue-700 px-2.5 py-1 rounded-full text-xs font-semibold">${all.length} holdings</span>
        <span class="text-xs text-gray-500">Top 10 concentration:
          <strong class="text-gray-700">${top10Wt.toFixed(1)}%</strong></span>
        ${topCountries.length > 1 ? `<span class="text-xs text-gray-400">·</span>
          <span class="text-xs text-gray-500">${topCountries.map(([c, w]) =>
            `<strong class="text-gray-600">${c}</strong> ${w.toFixed(0)}%`).join(' &middot; ')}</span>` : ''}
        ${holdingsTs ? `<span class="ml-auto text-xs text-gray-400" title="Holdings last updated by issuer scraper">As of ${fmtTs(holdingsTs)}</span>` : ''}
      </div>
      <input id="holding-filter" type="text" placeholder="Filter by name or ticker…"
        class="w-full border border-gray-200 rounded-lg px-3 py-1.5 text-sm mb-3 focus:outline-none focus:ring-2 focus:ring-blue-200">
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
                <span class="text-xs font-bold text-blue-700 ml-2 shrink-0">${r.weight_pct != null ? r.weight_pct.toFixed(2) + '%' : '—'}</span>
              </div>
              <div class="h-1.5 bg-gray-100 rounded-full overflow-hidden">
                <div class="h-full bg-blue-400 rounded-full pbar"
                     style="width:${maxW > 0 ? ((r.weight_pct || 0) / maxW * 100).toFixed(1) : 0}%"></div>
              </div>
            </div>
            <div class="text-xs text-gray-400 w-28 shrink-0 truncate">${r.sector || ''}</div>
            <div class="text-xs text-gray-300 w-20 shrink-0 truncate hidden sm:block">${r.country || ''}</div>
          </div>`).join('')}
      </div>
      ${all.length > 50 ? `
        <button id="show-all-holdings"
          class="mt-2 w-full text-xs text-blue-600 hover:text-blue-800 hover:underline py-1.5 border-t border-gray-100">
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
        <div class="bg-blue-50 rounded-lg p-3 border border-blue-100">
          <p class="text-xs text-green-600 font-medium">Distribution Yield</p>
          <p class="text-xl font-bold text-blue-700 mt-0.5">${yieldPct.toFixed(2)}%</p>
        </div>
        ${price != null ? `<div class="bg-slate-50 rounded-lg p-3 border border-gray-100">
          <p class="text-xs text-gray-400 font-medium">Unit Price</p>
          <p class="text-xl font-bold text-gray-800 mt-0.5">${money(price)}</p>
        </div>` : ''}
        ${incomePerUnit != null ? `<div class="bg-slate-50 rounded-lg p-3 border border-gray-100">
          <p class="text-xs text-gray-400 font-medium">Income / Unit</p>
          <p class="text-xl font-bold text-gray-800 mt-0.5">${money(incomePerUnit)}</p>
        </div>` : ''}
        ${incomePerTenK != null ? `<div class="bg-blue-50 rounded-lg p-3 border border-blue-100">
          <p class="text-xs text-green-600 font-medium">Est. Income / $10K</p>
          <p class="text-xl font-bold text-blue-700 mt-0.5">${money(incomePerTenK)}<span class="text-xs font-normal text-blue-400">/yr</span></p>
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
                         ${p === '1y' ? 'bg-blue-600 text-white border-blue-600' : 'border-gray-200 text-gray-500 hover:border-blue-400 hover:text-blue-600'}">
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

  // Load AUM history from etp_monthly (append after chart)
  try {
    const histD = await api('/api/v1/history/etf/' + code);
    if (histD.data && histD.data.length > 1) {
      const rows = histD.data;
      const dates = rows.map(r => r.date);
      const aums  = rows.map(r => r.aum_m || 0);
      const flows = rows.map(r => r.flow_m || 0);
      const aumSection = document.createElement('div');
      aumSection.className = 'mt-5 pt-4 border-t border-gray-100';
      aumSection.innerHTML = `
        <p class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Historical AUM &amp; Flows <span class="font-normal normal-case">(ASX monthly data)</span></p>
        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
          <div style="height:160px"><canvas id="perf-aum-hist"></canvas></div>
          <div style="height:160px"><canvas id="perf-flow-hist"></canvas></div>
        </div>`;
      document.getElementById('tab-content').appendChild(aumSection);
      setTimeout(() => {
        if (histCharts['perf-aum-hist']) { histCharts['perf-aum-hist'].destroy(); }
        if (histCharts['perf-flow-hist']){ histCharts['perf-flow-hist'].destroy(); }
        histCharts['perf-aum-hist'] = new Chart(document.getElementById('perf-aum-hist'), {
          type: 'line',
          data: { labels: dates, datasets: [{ data: aums, borderColor: '#3b82f6', backgroundColor: '#3b82f620', fill: true, tension: 0.3, pointRadius: 0, borderWidth: 2 }] },
          options: { responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}},
            scales:{ x:{ticks:{maxTicksLimit:8,font:{size:9}},grid:{display:false}},
                     y:{ticks:{font:{size:9},callback:v=>'$'+v.toFixed(0)+'M'},grid:{color:'#2e5285'}} } }
        });
        histCharts['perf-flow-hist'] = new Chart(document.getElementById('perf-flow-hist'), {
          type: 'bar',
          data: { labels: dates, datasets: [{ data: flows, backgroundColor: flows.map(v=>v>=0?'#3b82f680':'#ef444480'), borderRadius: 1 }] },
          options: { responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}},
            scales:{ x:{ticks:{maxTicksLimit:8,font:{size:9}},grid:{display:false}},
                     y:{ticks:{font:{size:9},callback:v=>'$'+v.toFixed(0)+'M'},grid:{color:'#2e5285'}} } }
        });
      }, 50);
    }
  } catch(e) { /* etp_monthly data not available for this fund */ }

  await loadPerfData(code, '1y');

  // Period button clicks
  document.getElementById('perf-period-btns').addEventListener('click', async e => {
    const btn = e.target.closest('[data-period]');
    if (!btn) return;
    document.querySelectorAll('.perf-period').forEach(b => {
      b.className = b.className.replace('bg-blue-600 text-white border-blue-600',
                                        'border-gray-200 text-gray-500 hover:border-blue-400 hover:text-blue-600');
    });
    btn.className = btn.className.replace('border-gray-200 text-gray-500 hover:border-blue-400 hover:text-blue-600',
                                          'bg-blue-600 text-white border-blue-600');
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
               ${active ? 'text-white' : 'text-slate-300 bg-[#233d66] border-[#2e5285] hover:border-blue-500'}"
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
          btn.classList.remove('text-slate-300', 'bg-[#233d66]', 'border-[#2e5285]', 'hover:border-blue-500');
        } else {
          btn.style.background = ''; btn.style.borderColor = ''; btn.classList.remove('text-white');
          btn.classList.add('text-slate-300', 'bg-[#233d66]', 'border-[#2e5285]', 'hover:border-blue-500');
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
          title: { display: true, text: 'Rebased to 100', font: { size: 10 }, color: '#a8c4e0' },
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
          <th class="pb-2 px-3 text-right text-xs text-blue-600 font-semibold uppercase">${code} Return</th>
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
// Fee slider
const feeSlider = document.getElementById('f-max-fee');
const feeVal    = document.getElementById('f-fee-val');
feeSlider.addEventListener('input', () => {
  feeVal.textContent = parseFloat(feeSlider.value).toFixed(2) + '%';
  page = 0; loadTable();
});
// Numeric range inputs
let numTimer;
['f-min-fum', 'f-ret-min', 'f-ret-max', 'f-min-yield'].forEach(id => {
  document.getElementById(id).addEventListener('input', () => {
    clearTimeout(numTimer);
    numTimer = setTimeout(() => { page = 0; loadTable(); }, 350);
  });
});
// Hedged checkbox
document.getElementById('f-hedged').addEventListener('change', () => { page = 0; loadTable(); });

document.getElementById('btn-reset').addEventListener('click', () => {
  ['f-exchange', 'f-issuer', 'f-asset', 'f-type'].forEach(id => document.getElementById(id).value = '');
  document.getElementById('f-benchmark').value = '';
  document.getElementById('f-max-fee').value = 2;
  document.getElementById('f-fee-val').textContent = '2.00%';
  ['f-min-fum', 'f-ret-min', 'f-ret-max', 'f-min-yield'].forEach(id => document.getElementById(id).value = '');
  document.getElementById('f-hedged').checked = false;
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

/* ======================================================= data freshness */
let scrapeTimes = {};

// Map issuer display names → scrape_log source keys
const ISSUER_SRC = {
  'BetaShares': 'betashares', 'Vanguard': 'vanguard', 'iShares': 'ishares',
  'VanEck': 'vaneck', 'Global X': 'globalx',
  'State Street Investment Management': 'statestreet', 'SPDR': 'spdr', 'StateStreet': 'statestreet',
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
    await Promise.all([loadFilters(), loadOverview(), loadTable(), loadScrapeTimes()]);
  } catch (e) {
    console.error('Init error:', e);
  }
}

init();
/* ======================================================= detail panel live refresh */
async function refreshDetailMetrics(code) {
  const metricsEl = document.getElementById('d-metrics');
  if (!metricsEl) return;
  try {
    const d = await api('/api/v1/etfs/' + code);
    const _pSrc = 'ASX live data · ' + fmtTs(d.last_updated);
    const updates = {
      'Price':     { value: money(d.current_price), tip: _pSrc },
      'Day Chg':   { value: pct(d.day_change_pct), tip: _pSrc, num: d.day_change_pct },
      'Prem/Disc': { value: d.premium_discount_pct != null
                             ? (d.premium_discount_pct >= 0 ? '+' : '') + d.premium_discount_pct.toFixed(3) + '%' : '—',
                     tip: '(Price − NAV) / NAV · positive = premium, negative = discount',
                     num: d.premium_discount_pct },
      'CHESS FUM': { value: chessFum(d) != null ? fmtFum(chessFum(d)) : '—',
                     tip: chessFum(d) != null
                          ? 'CHESS units on issue × last price · ' + (d.units_on_issue_date || 'ASX monthly report')
                          : 'Units on issue not available for this fund' },
      'Total FUM': { value: fmtFum(d.fund_size_aud_millions),
                     tip: 'Total FUM · ASX Monthly Report · ' + fmtTs(tsFor('asx_report')) },
    };
    metricsEl.querySelectorAll('div').forEach(cell => {
      const label = cell.querySelector('p:first-child')?.textContent?.trim();
      const upd = updates[label];
      if (!upd) return;
      const valEl = cell.querySelector('p:last-child');
      if (!valEl) return;
      valEl.textContent = upd.value;
      if (upd.tip) valEl.title = upd.tip;
      if (upd.num != null) {
        valEl.className = `font-semibold text-sm mt-0.5 truncate ${pctCls(upd.num)} dated`;
      }
    });
  } catch (e) { /* silent */ }
}

setInterval(() => {
  loadOverview();
  loadTable();
  if (selectedCode && !document.getElementById('detail-panel').classList.contains('hidden')) {
    refreshDetailMetrics(selectedCode);
  }
}, 120000);

/* ======================================================= main view tabs */
const VIEWS = ['screener', 'compare', 'holdings', 'issuers', 'analytics'];
document.querySelectorAll('.main-tab').forEach(btn => {
  btn.addEventListener('click', () => {
    const v = btn.dataset.view;
    document.querySelectorAll('.main-tab').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    VIEWS.forEach(id => document.getElementById('view-' + id).classList.add('hidden'));
    document.getElementById('view-' + v).classList.remove('hidden');
    if (v === 'compare'   && !compareLoaded)   initCompare();
    if (v === 'holdings'  && !holdingsLoaded)  initHoldingsConcentration();
    if (v === 'issuers'   && !issuersLoaded)   initIssuers();
    if (v === 'analytics' && !analyticsLoaded) initAnalytics();
  });
});

/* ============================================================ COMPARE */
let compareLoaded = false;
let holdingsLoaded = false;
let analyticsLoaded = false;
let issuersLoaded = false;
let historyLoaded = false;
let histCharts = {};
let histSubActive = 'industry';
let anSubActive = 'overview';
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
        <div class="px-3 py-2 hover:bg-blue-50 cursor-pointer flex justify-between items-center
                    border-b last:border-b-0 ${cmpSet.has(r.code) ? 'bg-blue-50' : ''}"
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
    <span class="inline-flex items-center gap-1 bg-blue-100 text-blue-800 text-xs font-semibold
                 px-2.5 py-1 rounded-full">
      ${code}
      <button onclick="cmpRemove('${code}')"
              class="ml-0.5 text-blue-500 hover:text-blue-800 font-bold text-sm leading-none">×</button>
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
    const col = coverage[code] >= 50 ? 'bg-blue-50 text-blue-700 border-blue-200'
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
    `<th class="px-3 py-2 text-right text-xs font-semibold text-blue-700 uppercase tracking-wide whitespace-nowrap">${c}</th>`
  ).join('');

  const rows = overlap.map((item, i) => {
    const bg = i % 2 === 0 ? 'bg-white' : 'bg-gray-50';
    // Badge if it's in all selected ETFs
    const inAll = item.etf_count === codes.length;
    const countBadge = inAll
      ? '<span class="ml-1.5 text-xs bg-blue-100 text-blue-700 px-1.5 py-0.5 rounded-full font-medium">all</span>'
      : (codes.length > 2
          ? `<span class="ml-1.5 text-xs bg-gray-100 text-gray-500 px-1.5 py-0.5 rounded-full">${item.etf_count}/${codes.length}</span>`
          : '');

    const weightCells = codes.map(c => {
      const w = item.weights[c];
      if (!w) return `<td class="px-3 py-2 text-right text-xs text-gray-300">—</td>`;
      const barPct = maxWeightPerCode[c] > 0 ? Math.round(w / maxWeightPerCode[c] * 64) : 0;
      return `<td class="px-3 py-2 text-right text-xs">
        <div class="flex items-center justify-end gap-1.5">
          <div class="h-1.5 rounded-full bg-blue-200" style="width:${barPct}px;min-width:2px"></div>
          <span class="font-medium text-gray-700 tabular-nums">${w.toFixed(2)}%</span>
        </div>
      </td>`;
    }).join('');

    return `<tr class="${bg} hover:bg-blue-50 transition-colors">
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
    const overlapColor = s.overlap_pct >= 50 ? 'bg-blue-100 text-blue-700'
                       : s.overlap_pct >= 20 ? 'bg-yellow-100 text-yellow-700'
                       : 'bg-gray-100 text-gray-500';
    return `
      <div class="bg-white border ${dimmed ? 'border-dashed border-gray-200 opacity-80' : 'border-gray-200'} rounded-xl p-4 hover:shadow-md hover:border-blue-200 transition-all">
        <div class="flex justify-between items-start mb-1">
          <span class="font-bold text-blue-700 text-base">${s.code}</span>
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
                class="mt-3 text-xs text-blue-600 hover:text-blue-800 font-medium border border-blue-200 hover:border-blue-400 rounded-lg px-3 py-1 transition-colors">
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
    { label: 'Issuer',        fmt: e => e.issuer ? `<a href="/issuers/${slugify(e.issuer)}" class="text-blue-500 hover:underline">${e.issuer}</a>` : '—' },
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
      <div class="font-bold text-blue-700 text-base">${e.code}</div>
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
  const discMeta = d.disclosure_meta || {};
  // Show quarterly notice if any result ETF has quarterly disclosure
  const quarterlyEtfs = Object.entries(discMeta)
    .filter(([, m]) => m.holdings_disclosure === 'quarterly')
    .map(([code, m]) => ({ code, as_of: m.holdings_as_of }));
  const qBanner = quarterlyEtfs.length ? `
    <tr><td colspan="7" class="px-3 py-2">
      <div class="flex items-start gap-2 bg-amber-50 border border-amber-200 rounded-lg px-3 py-2 text-xs text-amber-800">
        <svg class="shrink-0 mt-0.5" width="14" height="14" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M8.485 2.495c.673-1.167 2.357-1.167 3.03 0l6.28 10.875c.673 1.167-.17 2.625-1.516 2.625H3.72c-1.347 0-2.189-1.458-1.515-2.625L8.485 2.495zM10 5a.75.75 0 01.75.75v3.5a.75.75 0 01-1.5 0v-3.5A.75.75 0 0110 5zm0 9a1 1 0 100-2 1 1 0 000 2z" clip-rule="evenodd"/></svg>
        <span><strong>Quarterly disclosure:</strong> ${quarterlyEtfs.map(e => `<strong>${e.code}</strong>${e.as_of ? ` (as of ${e.as_of})` : ''}`).join(', ')} ${quarterlyEtfs.length === 1 ? 'is an' : 'are'} active/complex ETF${quarterlyEtfs.length > 1 ? 's' : ''} listed on Cboe Australia. Holdings are publicly disclosed once per quarter, up to 60 days after quarter-end.</span>
      </div>
    </td></tr>` : '';
  document.getElementById('holdings-table').innerHTML = qBanner + d.results.map(r => {
    const isQ = discMeta[r.etf_code]?.holdings_disclosure === 'quarterly';
    return `
    <tr class="cursor-pointer" onclick="showDetail('${r.etf_code}');document.querySelector('.main-tab[data-view=screener]').click()">
      <td class="px-3 py-2.5 font-bold text-blue-700">${r.etf_code}${isQ ? ' <span title="Quarterly portfolio disclosure" class="text-amber-500 text-xs">Q</span>' : ''}</td>
      <td class="px-3 py-2.5 text-gray-600 max-w-[160px] truncate text-xs">${r.etf_name || ''}</td>
      <td class="px-3 py-2.5 font-medium text-gray-800 max-w-[180px] truncate">${r.holding_name || ''}</td>
      <td class="px-3 py-2.5 font-mono text-xs text-gray-500">${r.ticker || '—'}</td>
      <td class="px-3 py-2.5 text-right">
        <div class="flex items-center justify-end gap-2">
          <div class="w-16 bg-gray-100 rounded-full h-1.5 overflow-hidden">
            <div class="h-full bg-blue-400 rounded-full"
                 style="width:${maxW > 0 ? (r.weight_pct / maxW * 100).toFixed(1) : 0}%"></div>
          </div>
          <span class="font-bold text-blue-700 text-xs w-12 text-right">
            ${r.weight_pct != null ? r.weight_pct.toFixed(2) + '%' : '—'}
          </span>
        </div>
      </td>
      <td class="px-3 py-2.5 text-xs text-gray-500 max-w-[120px] truncate">${r.sector || '—'}</td>
      <td class="px-3 py-2.5">${acChip(r.asset_class)}</td>
    </tr>`;
  }).join('');
}

/* ============================================================ HOLDINGS CONCENTRATION */
let _concData = null;
let _concTab  = 'count';

document.getElementById('view-holdings').addEventListener('click', e => {
  const btn = e.target.closest('.conc-tab-btn');
  if (!btn) return;
  _concTab = btn.dataset.conc;
  document.querySelectorAll('.conc-tab-btn').forEach(b => {
    const active = b.dataset.conc === _concTab;
    b.className = 'conc-tab-btn px-3 py-1.5 text-xs rounded-md font-medium'
      + (active ? ' bg-white text-blue-700 shadow-sm' : ' text-gray-500 hover:text-gray-700');
  });
  renderConcentration();
});

async function initHoldingsConcentration() {
  holdingsLoaded = true;
  const data = await api('/api/v1/holdings/concentration');
  _concData = data;
  document.getElementById('conc-loading').classList.add('hidden');
  document.getElementById('conc-content').classList.remove('hidden');
  renderConcentration();
}

function renderConcentration() {
  if (!_concData) return;
  const sectionMap = { count: _concData.by_count, geo: _concData.by_geography, sector: _concData.by_sector };
  const section = sectionMap[_concTab] || {};
  const keyMap  = { count: 'holding_count', geo: 'country_count', sector: 'sector_count' };
  const lblMap  = { count: 'holdings', geo: 'countries', sector: 'sectors' };
  const key   = keyMap[_concTab];
  const label = lblMap[_concTab];
  renderConcList('conc-left',  section.most_concentrated || [], key, label, 'concentrated');
  renderConcList('conc-right', section.most_diversified  || [], key, label, 'diversified');
}

function renderConcList(elId, items, key, label, side) {
  const el = document.getElementById(elId);
  if (!items.length) { el.innerHTML = '<p class="text-gray-400 text-xs p-4">No data</p>'; return; }
  const max = Math.max(...items.map(d => d[key] || 0)) || 1;
  el.innerHTML = items.map((d, i) => {
    const val  = d[key] || 0;
    const barW = (val / max * 100).toFixed(1);
    const sub  = (_concTab === 'count' && d.top10_conc != null)
      ? `<span class="text-gray-400 text-xs ml-1">· top 10: ${d.top10_conc.toFixed(1)}%</span>` : '';
    const barClr = side === 'concentrated' ? 'bg-orange-400' : 'bg-green-500';
    return `<div class="flex items-center gap-2.5 px-4 py-2.5 hover:bg-gray-50 cursor-pointer border-b border-gray-50 last:border-0"
         onclick="showDetail('${d.etf_code}');document.querySelector('.main-tab[data-view=screener]').click()">
      <span class="text-gray-300 font-mono text-xs w-5 shrink-0 text-right">${i+1}</span>
      <div class="flex-1 min-w-0">
        <div class="flex items-center gap-1.5 mb-1">
          <span class="font-bold text-blue-700 text-xs shrink-0">${d.etf_code}</span>
          <span class="text-gray-500 text-xs truncate">${d.etf_name || ''}</span>
        </div>
        <div class="flex items-center gap-2">
          <div class="flex-1 bg-gray-100 rounded-full h-1.5 overflow-hidden">
            <div class="h-full rounded-full ${barClr}" style="width:${barW}%"></div>
          </div>
          <span class="text-xs font-semibold text-gray-700 shrink-0 w-20 text-right">
            ${val.toLocaleString()} ${label}${sub}
          </span>
        </div>
      </div>
    </div>`;
  }).join('');
}

/* ======================================================= issuers tab */
let _issuersList = null;

async function initIssuers() {
  issuersLoaded = true;
  const data = await api('/api/v1/issuers');
  _issuersList = (data.issuers || []);

  // Summary stats
  const totalFum = _issuersList.reduce((s, i) => s + (i.total_fum || 0), 0);
  const totalEtfs = _issuersList.reduce((s, i) => s + (i.etf_count || 0), 0);
  const totalFlow = _issuersList.reduce((s, i) => s + (i.fund_flow_1m || 0), 0);
  document.getElementById('iss-summary').innerHTML = [
    ['Total AUM',     fmtFum(totalFum)],
    ['Issuers',       _issuersList.length + ' managers'],
    ['Total ETFs',    totalEtfs + ' products'],
    ['1M Net Flows',  fmtFum(totalFlow)],
  ].map(([l, v]) => `
    <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-4">
      <p class="text-xs text-gray-500 font-medium uppercase tracking-wide">${l}</p>
      <p class="text-xl font-bold text-gray-900 mt-1">${v}</p>
    </div>`).join('');

  renderIssuerGrid();
  document.getElementById('iss-search').addEventListener('input', renderIssuerGrid);
  document.getElementById('iss-hide-small').addEventListener('change', renderIssuerGrid);
}

function renderIssuerGrid() {
  const q = (document.getElementById('iss-search')?.value || '').toLowerCase();
  const hideSmall = document.getElementById('iss-hide-small')?.checked;
  const issuers = (_issuersList || []).filter(i =>
    (!q || (i.name || '').toLowerCase().includes(q)) &&
    (!hideSmall || (i.etf_count || 0) >= 5)
  );

  if (!issuers.length) {
    document.getElementById('iss-grid').innerHTML =
      '<p class="text-gray-400 text-sm col-span-3 py-8 text-center">No issuers matched</p>';
    return;
  }

  document.getElementById('iss-grid').innerHTML = issuers.map(iss => {
    const slug = slugify(iss.name);
    const color = issuerColor(iss.name);
    const flow = iss.fund_flow_1m;
    const flowStr = flow != null ? fmtFum(flow) : '—';
    const flowCls = flow > 0 ? 'text-green-600' : flow < 0 ? 'text-red-500' : 'text-gray-400';
    const share = iss.market_share_pct != null ? iss.market_share_pct.toFixed(1) + '%' : '—';
    return `
    <a href="/issuers/${slug}"
       class="bg-white rounded-xl border border-gray-100 shadow-sm hover:shadow-md
              hover:border-blue-200 transition-all p-5 block group">
      <div class="flex items-center gap-3 mb-3">
        <div class="w-3 h-8 rounded-sm shrink-0" style="background:${color}"></div>
        <div class="min-w-0">
          <h3 class="font-bold text-gray-900 text-sm leading-tight truncate group-hover:text-blue-600 transition-colors">
            ${iss.name}
          </h3>
          <p class="text-xs text-gray-400">${iss.etf_count} ETFs · ${share} market share</p>
        </div>
      </div>
      <div class="grid grid-cols-3 gap-2 text-center">
        <div>
          <p class="text-xs text-gray-400">AUM</p>
          <p class="text-sm font-semibold text-gray-800">${fmtFum(iss.total_fum)}</p>
        </div>
        <div>
          <p class="text-xs text-gray-400">Avg MER</p>
          <p class="text-sm font-semibold text-gray-800">${iss.avg_mer != null ? Number(iss.avg_mer).toFixed(2) + '%' : '—'}</p>
        </div>
        <div>
          <p class="text-xs text-gray-400">1M Flow</p>
          <p class="text-sm font-semibold ${flowCls}">${flowStr}</p>
        </div>
      </div>
    </a>`;
  }).join('');
}

/* ============================================================ ANALYTICS */
/* ============================================================ ANALYTICS */
async function initAnalytics() {
  analyticsLoaded = true;
  anSubActive = 'overview';

  /* ── Sub-nav switching ── */
  document.getElementById('an-subnav').addEventListener('click', async e => {
    const btn = e.target.closest('[data-asub]');
    if (!btn) return;
    const sub = btn.dataset.asub;
    document.querySelectorAll('.an-sub-btn').forEach(b => {
      const active = b.dataset.asub === sub;
      b.className = 'an-sub-btn px-3 py-1.5 text-xs rounded-lg border font-medium ' +
        (active ? 'bg-blue-600 text-white border-blue-600'
                : 'border-gray-200 text-gray-600 hover:border-blue-400');
    });
    ['overview','industry','issuers','assetclass','fund','flows','launches'].forEach(id => {
      document.getElementById('asub-' + id).classList.toggle('hidden', id !== sub);
    });
    anSubActive = sub;
    if (sub === 'industry'   && !histCharts['hist-industry-aum'])       await loadHistIndustry();
    if (sub === 'issuers'    && !histCharts['hist-issuer-stacked'])      await loadHistIssuers();
    if (sub === 'assetclass' && !histCharts['hist-assetclass-stacked'])  await loadHistAssetClass();
    if (sub === 'flows'      && !document.getElementById('hist-flows-in').children.length) await loadHistFlows(12);
    if (sub === 'launches'   && !histCharts['hist-launches-bar'])        await loadHistLaunches();
  });

  /* ── Fund lookup wiring ── */
  document.getElementById('hist-fund-btn').addEventListener('click', () => {
    const code = document.getElementById('hist-fund-input').value.trim();
    if (code) loadHistFund(code);
  });
  document.getElementById('hist-fund-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') document.getElementById('hist-fund-btn').click();
  });

  /* ── Flow period buttons ── */
  document.querySelectorAll('.hist-flow-period').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.hist-flow-period').forEach(b => {
        b.className = b.className.replace('bg-blue-600 text-white border-blue-600', 'border-gray-200 text-gray-600 hover:border-blue-400');
      });
      btn.className = btn.className.replace('border-gray-200 text-gray-600 hover:border-blue-400', 'bg-blue-600 text-white border-blue-600');
      loadHistFlows(btn.dataset.months);
    });
  });

  /* ── Overview: leaderboard + flows ── */
  const [performers, yieldData, cheapest, flows] = await Promise.all([
    api('/api/v1/analytics/top-performers?limit=15'),
    api('/api/v1/analytics/highest-yield?limit=15'),
    api('/api/v1/analytics/cheapest?limit=15'),
    api('/api/v1/analytics/fund-flows?limit=10'),
  ]);

  function leaderRow(code, name, valueHtml, rank) {
    return `<div class="px-4 py-2.5 flex items-center gap-3 hover:bg-slate-50 cursor-pointer"
         onclick="showDetail('${code}');document.querySelector('.main-tab[data-view=screener]').click()">
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
    const col  = dir === 'in' ? issuerColor(r.issuer) : '#ef4444';
    const barW = (Math.abs(r.fund_flow_1m || 0) / maxFlow * 100).toFixed(1);
    return `<div class="flex items-center gap-2 cursor-pointer hover:bg-slate-50 px-2 py-1.5 rounded group"
         onclick="showDetail('${r.code}');document.querySelector('.main-tab[data-view=screener]').click()">
      <div class="min-w-0 w-24 shrink-0">
        <div class="font-bold text-gray-900 text-xs">${r.code}</div>
        <div class="text-[10px] text-gray-400 truncate">
  ${r.issuer ? `<a href="/issuers/${slugify(r.issuer)}" class="hover:underline">${r.issuer}</a>` : ''}
</div>
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

/* ============================================================ HISTORY TAB */
const HIST_PALETTE = [
  '#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6',
  '#ec4899','#06b6d4','#84cc16','#f97316','#6366f1',
  '#14b8a6','#a855f7'
];

function makeStackedArea(canvasId, dates, series, colors) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;
  if (histCharts[canvasId]) { histCharts[canvasId].destroy(); delete histCharts[canvasId]; }
  const c = new Chart(ctx, {
    type: 'line',
    data: {
      labels: dates,
      datasets: series.map((s, i) => ({
        label: s.name,
        data: s.data,
        backgroundColor: (colors?.[i] ?? HIST_PALETTE[i % HIST_PALETTE.length]) + '99',
        borderColor:     colors?.[i] ?? HIST_PALETTE[i % HIST_PALETTE.length],
        borderWidth: 1.5,
        fill: 'stack',
        tension: 0.3,
        pointRadius: 0,
      }))
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { position: 'bottom', labels: { boxWidth: 10, font: { size: 10 } } } },
      scales: {
        x: { ticks: { maxTicksLimit: 12, font: { size: 10 } }, grid: { display: false } },
        y: { stacked: true, ticks: { font: { size: 10 },
             callback: v => '$' + v.toFixed(0) + 'B' }, grid: { color: '#2e5285' } }
      }
    }
  });
  histCharts[canvasId] = c;
  return c;
}

function makeLineChart(canvasId, labels, datasets, yLabel, inlinePlugins) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;
  if (histCharts[canvasId]) { histCharts[canvasId].destroy(); delete histCharts[canvasId]; }
  const c = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { display: datasets.length > 1, position: 'bottom', labels: { boxWidth: 10, font: { size: 10 } } } },
      scales: {
        x: { ticks: { maxTicksLimit: 12, font: { size: 10 } }, grid: { display: false } },
        y: { ticks: { font: { size: 10 } }, grid: { color: '#2e5285' } }
      }
    },
    plugins: inlinePlugins || []
  });
  histCharts[canvasId] = c;
  return c;
}

function makeBarChart(canvasId, labels, datasets) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;
  if (histCharts[canvasId]) { histCharts[canvasId].destroy(); delete histCharts[canvasId]; }
  const c = new Chart(ctx, {
    type: 'bar',
    data: { labels, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: datasets.length > 1, position: 'bottom', labels: { boxWidth: 10, font: { size: 10 } } } },
      scales: {
        x: { ticks: { font: { size: 10 } }, grid: { display: false } },
        y: { ticks: { font: { size: 10 } }, grid: { color: '#2e5285' } }
      }
    }
  });
  histCharts[canvasId] = c;
  return c;
}

async function loadHistIndustry() {
  // ── Exchange breakdown (ASX vs Cboe) ──
  const exData = await api('/api/v1/exchanges');
  if (exData.exchanges && exData.exchanges.length) {
    const exchanges = exData.exchanges.filter(e => e.exchange && e.total_fum > 0);
    const labels = exchanges.map(e => e.exchange === 'CXA' ? 'Cboe Australia' : e.exchange);
    const fums   = exchanges.map(e => +(e.total_fum / 1000).toFixed(2));
    const counts = exchanges.map(e => e.etf_count);
    const total  = fums.reduce((a, b) => a + b, 0);
    const COLORS = ['#3b82f6', '#f59e0b', '#10b981', '#8b5cf6'];

    const ctx = document.getElementById('exch-donut').getContext('2d');
    new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels,
        datasets: [{ data: fums, backgroundColor: COLORS.slice(0, exchanges.length),
                     borderWidth: 2, borderColor: '#fff' }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        cutout: '62%',
        plugins: {
          legend: { position: 'bottom', labels: { boxWidth: 10, font: { size: 11 }, padding: 12 } },
          tooltip: { callbacks: { label: c => ` A$${c.parsed.toFixed(1)}B (${(c.parsed/total*100).toFixed(1)}%)` } }
        }
      }
    });

    document.getElementById('exch-stats').innerHTML = exchanges.map((e, i) => {
      const pct = (e.total_fum / 1000 / total * 100).toFixed(1);
      const fum = (e.total_fum / 1000).toFixed(1);
      const label = e.exchange === 'CXA' ? 'Cboe Australia' : e.exchange;
      return `
        <div class="flex items-center gap-3">
          <div class="w-3 h-3 rounded-sm shrink-0" style="background:${COLORS[i]}"></div>
          <div class="flex-1">
            <div class="flex items-baseline justify-between">
              <span class="text-sm font-semibold text-gray-700">${label}</span>
              <span class="text-sm font-bold text-gray-800">A$${fum}B</span>
            </div>
            <div class="flex items-center justify-between text-xs text-gray-400 mt-0.5">
              <span>${e.etf_count} ETFs</span>
              <span>${pct}% of market</span>
            </div>
            <div class="mt-1 h-1.5 rounded-full bg-gray-100 overflow-hidden">
              <div class="h-full rounded-full" style="width:${pct}%;background:${COLORS[i]}"></div>
            </div>
          </div>
        </div>`;
    }).join('');
  }

  // ── Historical industry data ──
  const d = await api('/api/v1/history/industry');
  if (!d.data) return;
  const rows = d.data;
  const anomalies = d.anomalies || {};
  const dates  = rows.map(r => r.date);
  const aums   = rows.map(r => r.aum_b      ? +r.aum_b.toFixed(1)      : 0);
  const counts = rows.map(r => r.etf_count  || 0);
  const flows     = rows.map(r => r.flows_m     ? +r.flows_m.toFixed(0)     : 0);
  const rawFlows  = rows.map(r => r.raw_flows_m ? +r.raw_flows_m.toFixed(0) : 0);
  const THRESH = 500;

  // Grey line traces orange exactly, diverges only at anomaly spikes — visually connected
  const greyLine  = rawFlows.map((v, i) => Math.abs(v - flows[i]) > THRESH ? v : flows[i]);
  const isAnomaly = greyLine.map((v, i) => Math.abs(v - flows[i]) > THRESH);

  // Y-axis cap: max of clean (non-anomaly) flows + 10% headroom — shows all ordinary data in full
  const cleanMax = Math.max(...flows.filter((_, i) => !isAnomaly[i])) * 1.1;

  // Inline plugin: dynamic Y-axis + hover callout
  function makeCalloutPlugin(anomalyMap, datesList, dsIndex) {
    let _updating = false;
    return {
      id: 'anomalyCallout_' + dsIndex,
      // On first render, clamp the Y-axis to clean data range
      afterRender(chart) {
        const yScale = chart.options.scales.yFlow;
        if (yScale && yScale.max === undefined) {
          yScale.max = cleanMax;
          if (!_updating) { _updating = true; chart.update('none'); _updating = false; }
        }
      },
      // Expand/collapse Y-axis on hover
      afterEvent(chart, args) {
        if (_updating) return;
        const e = args.event;
        if (e.type !== 'mousemove' && e.type !== 'mouseleave') return;
        const yScale = chart.options.scales.yFlow;
        if (!yScale) return;
        let onAnomaly = false;
        if (e.type === 'mousemove') {
          const active = chart.tooltip._active || [];
          if (active.length && anomalyMap[datesList[active[0].index]]) onAnomaly = true;
        }
        const target = onAnomaly ? undefined : cleanMax;
        if (yScale.max !== target) {
          yScale.max = target;
          _updating = true;
          chart.update();
          _updating = false;
        }
      },
      // Draw callout only while hovering over an anomaly point
      afterDatasetsDraw(chart) {
        const active = chart.tooltip._active;
        if (!active || !active.length) return;
        const activeIdx = active[0].index;
        const date = datesList[activeIdx];
        if (!anomalyMap[date]) return;
        const meta = chart.getDatasetMeta(dsIndex);
        if (!meta || !meta.data) return;
        const pt = meta.data[activeIdx];
        if (!pt) return;
        const ctx = chart.ctx;
        const x = pt.x, y = pt.y;
        const etfs = anomalyMap[date];
        const label = etfs.map(e => `${e.code}  +$${(e.flow_m / 1000).toFixed(1)}B`).join('   ');
        const lineH = 30;
        ctx.save();
        ctx.beginPath();
        ctx.strokeStyle = '#94a3b8';
        ctx.lineWidth = 1;
        ctx.setLineDash([3, 3]);
        ctx.moveTo(x, y - 5);
        ctx.lineTo(x, y - lineH);
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.font = 'bold 9px ui-sans-serif, sans-serif';
        const tw = ctx.measureText(label).width;
        const pad = 6, bh = 15, bw = tw + pad * 2;
        const bx = Math.min(Math.max(x - bw / 2, 2), chart.width - bw - 2);
        const by = y - lineH - bh - 2;
        ctx.fillStyle = '#2e5285';
        ctx.beginPath();
        ctx.roundRect(bx, by, bw, bh, 4);
        ctx.fill();
        ctx.fillStyle = '#e2e8f0';
        ctx.textAlign = 'left';
        ctx.textBaseline = 'middle';
        ctx.fillText(label, bx + pad, by + bh / 2);
        ctx.restore();
      }
    };
  }

  makeLineChart('hist-industry-aum', dates, [{
    label: 'Industry AUM ($B)',
    data: aums,
    borderColor: '#3b82f6', backgroundColor: '#3b82f620',
    fill: true, tension: 0.3, pointRadius: 0, borderWidth: 2
  }]);

  makeLineChart('hist-industry-count', dates, [
    { label: 'ETF Count', data: counts, borderColor: '#3b82f6', backgroundColor: 'transparent',
      tension: 0.3, pointRadius: 0, borderWidth: 2, yAxisID: 'yCount' },
    { label: 'Net Flows ($M)', data: flows, borderColor: '#f59e0b', backgroundColor: '#f59e0b20',
      fill: true, tension: 0.3, pointRadius: 0, borderWidth: 1.5, yAxisID: 'yFlow' },
    { label: 'Flows incl. new admissions ($M)', data: greyLine,
      borderColor: '#94a3b8', backgroundColor: 'transparent',
      borderDash: [4, 4], tension: 0.3,
      pointRadius: isAnomaly.map(a => a ? 4 : 0),
      pointBackgroundColor: '#94a3b8', borderWidth: 1.5, yAxisID: 'yFlow' }
  ], undefined, [makeCalloutPlugin(anomalies, dates, 2)]);

  // Patch dual-axis scales after creation
  const c = histCharts['hist-industry-count'];
  if (c) {
    c.options.scales = {
      x: { ticks: { maxTicksLimit: 12, font: { size: 10 } }, grid: { display: false } },
      yCount: { type: 'linear', position: 'left',  ticks: { font: { size: 10 } }, grid: { color: '#2e5285' } },
      yFlow:  { type: 'linear', position: 'right', ticks: { font: { size: 10 } }, grid: { display: false } }
    };
    c.update();
  }

  // Annual bar chart
  const annual = {};
  rows.forEach(r => {
    const yr = r.date.slice(0, 4);
    if (!annual[yr]) annual[yr] = { flows: 0, rawFlows: 0, traded: 0, anomalyDates: [] };
    annual[yr].flows    += (r.flows_m     || 0);
    annual[yr].rawFlows += (r.raw_flows_m || 0);
    annual[yr].traded   += (r.traded_m    || 0);
    if (anomalies[r.date]) annual[yr].anomalyDates.push(r.date);
  });
  const yrs = Object.keys(annual).sort();

  // Merge admission ETFs per year for callout
  const annualAnomalyMap = {};
  yrs.forEach(yr => {
    const merged = [];
    (annual[yr].anomalyDates || []).forEach(dt => (anomalies[dt] || []).forEach(e => merged.push(e)));
    if (merged.length) annualAnomalyMap[yr] = merged;
  });

  // Grey line: traces clean bars, diverges at anomaly years
  const annualRaw      = yrs.map(y => Math.abs(annual[y].rawFlows - annual[y].flows) > 1000
    ? +annual[y].rawFlows.toFixed(0) : +annual[y].flows.toFixed(0));
  const annualIsAnom   = yrs.map(y => Math.abs(annual[y].rawFlows - annual[y].flows) > 1000);

  const annualCleanMax = Math.max(...yrs
    .filter(y => !annualIsAnom[yrs.indexOf(y)])
    .map(y => annual[y].flows)) * 1.1;
  let _barUpdating = false;
  const barCalloutPlugin = {
    id: 'annualAnomalyCallout',
    afterRender(chart) {
      const yScale = chart.options.scales.y;
      if (yScale && yScale.max === undefined) {
        yScale.max = annualCleanMax;
        if (!_barUpdating) { _barUpdating = true; chart.update('none'); _barUpdating = false; }
      }
    },
    afterEvent(chart, args) {
      if (_barUpdating) return;
      const e = args.event;
      if (e.type !== 'mousemove' && e.type !== 'mouseleave') return;
      const yScale = chart.options.scales.y;
      if (!yScale) return;
      let onAnomaly = false;
      if (e.type === 'mousemove') {
        const active = chart.tooltip._active || [];
        if (active.length && annualAnomalyMap[yrs[active[0].index]]) onAnomaly = true;
      }
      const target = onAnomaly ? undefined : annualCleanMax;
      if (yScale.max !== target) {
        yScale.max = target;
        _barUpdating = true;
        chart.update();
        _barUpdating = false;
      }
    },
    afterDatasetsDraw(chart) {
      const active = chart.tooltip._active;
      if (!active || !active.length) return;
      const i = active[0].index;
      const yr = yrs[i];
      if (!annualAnomalyMap[yr]) return;
      const meta = chart.getDatasetMeta(2);
      if (!meta || !meta.data) return;
      const pt = meta.data[i];
      if (!pt) return;
      const ctx = chart.ctx;
      const x = pt.x, y = pt.y;
      const label = annualAnomalyMap[yr].map(e => `${e.code}  +$${(e.flow_m / 1000).toFixed(1)}B`).join('   ');
      const lineH = 28;
      ctx.save();
      ctx.beginPath();
      ctx.strokeStyle = '#94a3b8';
      ctx.lineWidth = 1;
      ctx.setLineDash([3, 3]);
      ctx.moveTo(x, y - 5);
      ctx.lineTo(x, y - lineH);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.font = 'bold 9px ui-sans-serif, sans-serif';
      const tw = ctx.measureText(label).width;
      const pad = 6, bh = 15, bw = tw + pad * 2;
      const bx = Math.min(Math.max(x - bw / 2, 2), chart.width - bw - 2);
      const by = y - lineH - bh - 2;
      ctx.fillStyle = '#334155';
      ctx.beginPath();
      ctx.roundRect(bx, by, bw, bh, 4);
      ctx.fill();
      ctx.fillStyle = '#f1f5f9';
      ctx.textAlign = 'left';
      ctx.textBaseline = 'middle';
      ctx.fillText(label, bx + pad, by + bh / 2);
      ctx.restore();
    }
  };

  makeBarChart('hist-industry-flows', yrs, [
    { label: 'Net Flows ($M)',    data: yrs.map(y => +annual[y].flows.toFixed(0)),  backgroundColor: '#3b82f680' },
    { label: 'Traded Value ($M)', data: yrs.map(y => +annual[y].traded.toFixed(0)), type: 'line',
      borderColor: '#3b82f6', fill: false, pointRadius: 2, borderWidth: 2, tension: 0.3 },
    { label: 'Flows incl. new admissions ($M)', data: annualRaw, type: 'line',
      borderColor: '#94a3b8', borderDash: [4, 4], fill: false,
      pointRadius: annualIsAnom.map(a => a ? 5 : 0),
      pointBackgroundColor: '#94a3b8', borderWidth: 1.5, tension: 0.3 }
  ]);
  const bc = histCharts['hist-industry-flows'];
  if (bc) { bc.config.plugins = [barCalloutPlugin]; bc.update(); }
}

async function loadHistIssuers() {
  const ISSUER_BRAND = {
    'Vanguard':    '#7b1c2e',   // maroon
    'Betashares':  '#ea580c',   // orange
    'iShares':     '#16a34a',   // green
    'VanEck':      '#1e3a8a',   // navy blue
    'Global X':    '#0e7490',   // teal-blue
  };
  const d = await api('/api/v1/history/issuers');
  if (!d.dates) return;
  const brandColors = d.series.map((s, i) => ISSUER_BRAND[s.name] ?? HIST_PALETTE[i % HIST_PALETTE.length]);
  makeStackedArea('hist-issuer-stacked', d.dates, d.series, brandColors);

  // Latest ranking table
  const latest = d.dates[d.dates.length - 1];
  const lastIdx = d.dates.length - 1;
  const ranked = d.series.map(s => ({ name: s.name, aum: s.data[lastIdx] || 0 }))
    .sort((a, b) => b.aum - a.aum);
  const totalAum = ranked.reduce((s, r) => s + r.aum, 0);
  document.getElementById('hist-issuer-table').innerHTML = `
    <table class="w-full text-sm">
      <thead><tr class="text-xs text-gray-400 border-b border-gray-100">
        <th class="pb-2 text-left font-medium">Issuer</th>
        <th class="pb-2 text-right font-medium">AUM ($B)</th>
        <th class="pb-2 text-right font-medium">Share</th>
      </tr></thead>
      <tbody class="divide-y divide-gray-50">
        ${ranked.map((r, i) => `
          <tr>
            <td class="py-1.5 flex items-center gap-2">
              <span class="w-2.5 h-2.5 rounded-sm inline-block" style="background:${ISSUER_BRAND[r.name] ?? HIST_PALETTE[d.series.findIndex(s=>s.name===r.name)%HIST_PALETTE.length]}"></span>
              ${r.name}
            </td>
            <td class="py-1.5 text-right font-semibold">${r.aum.toFixed(1)}</td>
            <td class="py-1.5 text-right text-gray-500">${totalAum > 0 ? (r.aum/totalAum*100).toFixed(1)+'%' : '—'}</td>
          </tr>`).join('')}
      </tbody>
    </table>`;
}

async function loadHistAssetClass() {
  const d = await api('/api/v1/history/asset-classes');
  if (!d.dates) return;
  makeStackedArea('hist-assetclass-stacked', d.dates, d.series);
}

async function loadHistFlows(months) {
  document.getElementById('hist-flows-in').innerHTML  = '<div class="flex justify-center py-6"><div class="spinner"></div></div>';
  document.getElementById('hist-flows-out').innerHTML = '<div class="flex justify-center py-6"><div class="spinner"></div></div>';
  const d = await api('/api/v1/history/flows?months=' + months + '&limit=15');
  function flowRows(arr, dir) {
    if (!arr || !arr.length) return '<p class="text-xs text-gray-400 py-4 text-center">No data</p>';
    const maxF = Math.max(...arr.map(r => Math.abs(r.flow_m || 0)), 1);
    return arr.map((r, i) => {
      const col = dir === 'in' ? '#3b82f6' : '#ef4444';
      const w = (Math.abs(r.flow_m||0) / maxF * 100).toFixed(1);
      return `<div class="flex items-center gap-2 py-1.5 hover:bg-slate-50 rounded cursor-pointer px-1"
           onclick="showDetail('${r.code}');document.querySelector('.main-tab[data-view=screener]').click()">
        <div class="w-16 shrink-0">
          <div class="font-bold text-xs text-gray-900">${r.code}</div>
          <div class="text-[10px] text-gray-400 truncate">
  ${r.issuer ? `<a href="/issuers/${slugify(r.issuer)}" class="hover:underline">${r.issuer}</a>` : ''}
</div>
        </div>
        <div class="flex-1 bg-gray-100 rounded-full h-2 overflow-hidden">
          <div class="h-full rounded-full" style="background:${col};width:${w}%"></div>
        </div>
        <span class="text-xs font-semibold w-20 text-right shrink-0" style="color:${col}">
          ${dir==='in'?'+':''}${r.flow_m!=null?(r.flow_m/1000).toFixed(2)+'B':'—'}
        </span>
      </div>`;
    }).join('');
  }
  document.getElementById('hist-flows-in').innerHTML  = flowRows(d.inflows,  'in');
  document.getElementById('hist-flows-out').innerHTML = flowRows(d.outflows, 'out');
}

async function loadHistLaunches() {
  const d = await api('/api/v1/history/launches');
  if (!d.by_year) return;
  const years  = d.by_year.map(r => r.year);
  const counts = d.by_year.map(r => r.count);
  makeBarChart('hist-launches-bar', years, [{
    label: 'New ETF Listings',
    data: counts,
    backgroundColor: years.map((_, i) => HIST_PALETTE[i % HIST_PALETTE.length] + 'cc'),
    borderRadius: 4,
  }]);

  const maxTotal = Math.max(...d.by_issuer.map(r => r.total), 1);
  document.getElementById('hist-launches-issuer').innerHTML = (d.by_issuer || []).map((r, i) => `
    <div class="flex items-center gap-3 py-1.5 border-b border-gray-50 last:border-0">
      <a href="/issuers/${slugify(r.issuer)}" class="w-28 text-xs text-gray-700 font-medium truncate shrink-0 hover:text-blue-600 hover:underline">${r.issuer}</a>
      <div class="flex-1 bg-gray-100 rounded-full h-2 overflow-hidden">
        <div class="h-full rounded-full" style="background:${HIST_PALETTE[i%HIST_PALETTE.length]};width:${(r.total/maxTotal*100).toFixed(0)}%"></div>
      </div>
      <span class="text-xs font-semibold text-gray-700 w-8 text-right shrink-0">${r.total}</span>
      <span class="text-xs text-gray-400 w-16 text-right shrink-0">${r.active} active</span>
    </div>`).join('');
}

async function loadHistFund(rawCode) {
  const code = rawCode.toUpperCase();
  const el = document.getElementById('hist-fund-result');
  el.innerHTML = '<div class="flex justify-center py-8"><div class="spinner"></div></div>';
  const d = await api('/api/v1/history/etf/' + code);
  if (d.error || !d.data || !d.data.length) {
    el.innerHTML = `<p class="text-gray-400 text-sm py-4">No historical data found for ${code}.</p>`;
    return;
  }
  const rows  = d.data;
  const dates = rows.map(r => r.date);
  const anomalyFlags = rows.map(r => r.is_anomaly);

  /* ── summary stats ── */
  const last = rows[rows.length - 1];
  const first = rows[0];
  const statItem = (label, value) =>
    `<div class="text-center px-4 py-2 border-r border-gray-100 last:border-0">
       <div class="text-xs text-gray-400 mb-0.5">${label}</div>
       <div class="font-semibold text-gray-800 text-sm">${value}</div>
     </div>`;

  el.innerHTML = `
    <div class="mb-4">
      <h3 class="font-semibold text-gray-800 mb-0.5">${d.code} — ${d.name || ''}</h3>
      <p class="text-xs text-gray-400 mb-3">${d.issuer ? `<a href="/issuers/${slugify(d.issuer)}" class="hover:underline hover:text-blue-600">${d.issuer}</a>` : ''} · ${rows.length} months of data · from ${first.date} to ${last.date}</p>
      <div class="flex flex-wrap bg-gray-50 rounded-lg border border-gray-100 overflow-hidden mb-4">
        ${statItem('Current AUM', last.aum_m != null ? fmtFum(last.aum_m) : '—')}
        ${statItem('Unit Price', last.last_price != null ? '$' + last.last_price.toFixed(2) : '—')}
        ${statItem('MER', last.mer != null ? (last.mer * 100).toFixed(2) + '%' : '—')}
        ${statItem('Bid/Ask Spread', last.spread_pct != null ? last.spread_pct.toFixed(3) + '%' : '—')}
        ${statItem('CHESS Units', last.chess_units_m != null ? (last.chess_units_m).toFixed(1) + 'M' : '—')}
        ${statItem('1Y Return', last.return_1y != null ? (last.return_1y * 100).toFixed(1) + '%' : '—')}
        ${statItem('Yield', last.distribution_yield != null ? (last.distribution_yield * 100).toFixed(1) + '%' : '—')}
      </div>
    </div>
    <div class="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-4 gap-4">
      <div class="bg-gray-50 rounded-lg p-3">
        <p class="text-xs font-medium text-gray-500 mb-1">AUM ($M)</p>
        <div style="height:160px"><canvas id="hf-aum"></canvas></div>
      </div>
      <div class="bg-gray-50 rounded-lg p-3">
        <p class="text-xs font-medium text-gray-500 mb-1">Unit Price ($)</p>
        <div style="height:160px"><canvas id="hf-price"></canvas></div>
      </div>
      <div class="bg-gray-50 rounded-lg p-3">
        <p class="text-xs font-medium text-gray-500 mb-1">Net Monthly Flows ($M)</p>
        <div style="height:160px"><canvas id="hf-flows"></canvas></div>
      </div>
      <div class="bg-gray-50 rounded-lg p-3">
        <p class="text-xs font-medium text-gray-500 mb-1">CHESS Units on Issue (M)</p>
        <div style="height:160px"><canvas id="hf-chess"></canvas></div>
      </div>
      <div class="bg-gray-50 rounded-lg p-3">
        <p class="text-xs font-medium text-gray-500 mb-1">CHESS Net Flows ($M)</p>
        <div style="height:160px"><canvas id="hf-chess-flows"></canvas></div>
      </div>
      <div class="bg-gray-50 rounded-lg p-3">
        <p class="text-xs font-medium text-gray-500 mb-1">1Y Total Return (%)</p>
        <div style="height:160px"><canvas id="hf-ret"></canvas></div>
      </div>
      <div class="bg-gray-50 rounded-lg p-3">
        <p class="text-xs font-medium text-gray-500 mb-1">Distribution Yield (%)</p>
        <div style="height:160px"><canvas id="hf-yield"></canvas></div>
      </div>
      <div class="bg-gray-50 rounded-lg p-3">
        <p class="text-xs font-medium text-gray-500 mb-1">MER (%) &amp; Bid/Ask Spread (%)</p>
        <div style="height:160px"><canvas id="hf-costs"></canvas></div>
      </div>
    </div>`;

  setTimeout(() => {
    /* AUM */
    makeLineChart('hf-aum', dates, [{
      data: rows.map(r => r.aum_m || 0),
      borderColor: '#3b82f6', backgroundColor: '#3b82f620', fill: true,
      tension: 0.3, pointRadius: 0, borderWidth: 2,
    }]);
    /* Unit price */
    makeLineChart('hf-price', dates, [{
      data: rows.map(r => r.last_price),
      borderColor: '#8b5cf6', backgroundColor: '#8b5cf620', fill: true,
      tension: 0.3, pointRadius: 0, borderWidth: 2,
    }]);
    /* Net flows bar */
    const flows = rows.map(r => r.flow_m || 0);
    makeBarChart('hf-flows', dates, [{
      data: flows,
      backgroundColor: flows.map((v, i) =>
        anomalyFlags[i] ? '#94a3b880' : (v >= 0 ? '#3b82f680' : '#ef444480')),
      borderColor: flows.map((v, i) =>
        anomalyFlags[i] ? '#94a3b8' : (v >= 0 ? '#3b82f6' : '#ef4444')),
      borderWidth: 1, borderRadius: 2,
    }]);
    /* CHESS units */
    makeLineChart('hf-chess', dates, [{
      data: rows.map(r => r.chess_units_m),
      borderColor: '#10b981', backgroundColor: '#10b98120', fill: true,
      tension: 0.3, pointRadius: 0, borderWidth: 2,
    }]);
    /* CHESS net flows bar */
    const chessFlows = rows.map(r => r.chess_flow_m || 0);
    makeBarChart('hf-chess-flows', dates, [{
      data: chessFlows,
      backgroundColor: chessFlows.map(v => v >= 0 ? '#10b98180' : '#f9731680'),
      borderColor:     chessFlows.map(v => v >= 0 ? '#10b981'   : '#f97316'),
      borderWidth: 1, borderRadius: 2,
    }]);
    /* 1Y return */
    makeLineChart('hf-ret', dates, [{
      data: rows.map(r => r.return_1y != null ? +(r.return_1y * 100).toFixed(2) : null),
      borderColor: '#f59e0b', backgroundColor: '#f59e0b20', fill: true,
      tension: 0.3, pointRadius: 0, borderWidth: 2,
      spanGaps: true,
    }]);
    /* Dist yield */
    makeLineChart('hf-yield', dates, [{
      data: rows.map(r => r.distribution_yield != null ? +(r.distribution_yield * 100).toFixed(2) : null),
      borderColor: '#ef4444', backgroundColor: '#ef444420', fill: true,
      tension: 0.3, pointRadius: 0, borderWidth: 2,
      spanGaps: true,
    }]);
    /* MER + spread dual-series */
    makeLineChart('hf-costs', dates, [
      {
        label: 'MER (%)',
        data: rows.map(r => r.mer != null ? +(r.mer * 100).toFixed(3) : null),
        borderColor: '#6366f1', backgroundColor: 'transparent', fill: false,
        tension: 0, pointRadius: 0, borderWidth: 2, spanGaps: true,
      },
      {
        label: 'Spread (%)',
        data: rows.map(r => r.spread_pct != null ? +r.spread_pct.toFixed(4) : null),
        borderColor: '#f97316', backgroundColor: 'transparent', fill: false,
        tension: 0.3, pointRadius: 0, borderWidth: 2, spanGaps: true,
      },
    ]);
  }, 50);
}

function deepDiveFund(code) {
  document.querySelector('.main-tab[data-view=analytics]').click();
  setTimeout(() => {
    document.querySelector('[data-asub=fund]').click();
    setTimeout(() => {
      document.getElementById('hist-fund-input').value = code;
      loadHistFund(code);
    }, 50);
  }, 50);
}


/* ============================================================ CSV EXPORT */
function scExportCSV() {
  if (!lastTableData.length) return;
  const headers = ['Code','Name','Asset Class','Issuer','Price (AUD)','Total FUM (AUD M)','CHESS FUM (AUD M)',
                   '1Y Return (%)','Yield (%)','MER (%)'];
  const escape = v => '"' + String(v || '').replace(/"/g, '""') + '"';
  const csvRows = [headers.join(',')];
  lastTableData.forEach(e => {
    const chess = chessFum(e);
    csvRows.push([
      e.code,
      escape(e.name),
      escape(e.asset_class),
      escape(e.issuer),
      e.current_price         != null ? e.current_price.toFixed(2)          : '',
      e.fund_size_aud_millions != null ? e.fund_size_aud_millions.toFixed(1) : '',
      chess                   != null ? chess.toFixed(1)                    : '',
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
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  body {{ font-family: Inter, system-ui, -apple-system, sans-serif; background: #1a2e4a; color: #e2e8f0; }}
  article h2 {{ font-size: 1.15rem; font-weight: 700; margin: 1.5rem 0 .6rem; color: #e2e8f0; }}
  article p  {{ margin-bottom: 1rem; line-height: 1.7; font-size: .9rem; color: #c8ddf0; }}
  article table {{ width: 100%; border-collapse: collapse; margin: 1rem 0; font-size: .82rem; }}
  article th {{ text-align: left; padding: .4rem .7rem; background: #1e3354; border-bottom: 1px solid #2e5285;
                font-size: .7rem; font-weight: 700; text-transform: uppercase; letter-spacing: .04em; color: #a8c4e0; }}
  article td {{ padding: .4rem .7rem; border-bottom: 1px solid #2e5285; vertical-align: middle; color: #e2e8f0; }}
  article tr:last-child td {{ border-bottom: none; }}
  article tr:hover td {{ background: #2c4d78; }}
  .pos {{ color: #4ade80; font-weight: 600; }}
  .neg {{ color: #f87171; font-weight: 600; }}
  .chart-box {{ background: #233d66; border: 1px solid #2e5285; border-radius: .75rem; padding: 1.25rem; margin: 1.5rem 0; }}
  .chart-box h3 {{ font-size: .8rem; font-weight: 700; text-transform: uppercase; letter-spacing: .05em; color: #7fa3c8; margin-bottom: .75rem; }}
  .chart-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin: 1.5rem 0; }}
  @media (max-width: 640px) {{ .chart-grid {{ grid-template-columns: 1fr; }} }}
</style>
</head>"""


def _articles_nav(active_slug=None):
    return """
<header class="bg-gradient-to-r from-[#071422] to-[#0d2860] shadow-xl">
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
        'Performance':    ('bg-green-900/50',  'text-green-300'),
        'Market Trends':  ('bg-blue-900/50',   'text-blue-300'),
        'Thematic':       ('bg-purple-900/50', 'text-purple-300'),
        'Research':       ('bg-amber-900/50',  'text-amber-300'),
        'Education':      ('bg-teal-900/50',   'text-teal-300'),
        'Issuer Profile': ('bg-orange-900/50', 'text-orange-300'),
        'Annual Report':  ('bg-amber-900/50',  'text-amber-300'),
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
    annual_arts = sorted(
        [a for a in articles if a['category'] == 'Annual Report' and a['slug'] != 'etf-year-review-2025'],
        key=lambda a: a['date'], reverse=True
    )

    def card(a, wide=False):
        bg, fg = CAT_COLORS.get(a['category'], ('bg-slate-800', 'text-slate-300'))
        cols = 'sm:col-span-2' if wide else ''
        return f"""
    <a href="/articles/{a['slug']}" class="block {cols} bg-[#233d66] rounded-xl border border-[#2e5285] hover:border-blue-500/50 transition-colors p-5">
      <span class="inline-block {bg} {fg} text-xs font-semibold px-2 py-0.5 rounded mb-2">{a['category']}</span>
      <h2 class="text-base font-bold text-slate-100 leading-snug mb-1">{a['title']}</h2>
      <p class="text-sm text-slate-400 line-clamp-2">{a['subtitle']}</p>
      <p class="text-xs text-slate-500 mt-3">{a['date']}</p>
    </a>"""

    def section(heading, subheading, arts, cols=2, wide_first=False):
        if not arts:
            return ''
        grid_cols = f'grid-cols-1 sm:grid-cols-{cols}'
        cards_html = ''.join(card(a, wide=(i == 0 and wide_first and cols == 2)) for i, a in enumerate(arts))
        return f"""
  <section class="mb-10">
    <div class="mb-4">
      <h2 class="text-lg font-bold text-slate-100">{heading}</h2>
      <p class="text-sm text-slate-400 mt-0.5">{subheading}</p>
    </div>
    <div class="grid {grid_cols} gap-4">{cards_html}</div>
  </section>"""

    news_html   = section('Latest Analysis', 'Market data, performance and thematic coverage.',
                           news_arts, cols=2, wide_first=True)
    annual_html = section('Annual Reports', 'Year-in-review analysis of the Australian ETF market since 2020.',
                           annual_arts, cols=2, wide_first=False)
    basics_html = section('Learn the Basics', 'Everything you need to know about how ETFs work.',
                           basics_arts, cols=3)
    issuer_html = section('Issuer Profiles', 'Background, size and product range for each major ETF provider.',
                           issuer_arts, cols=2)

    html = _articles_head('Articles') + _articles_nav() + f"""
<body class="bg-[#1a2e4a] min-h-screen">
<main class="max-w-5xl mx-auto px-5 py-8">
  <h1 class="text-2xl font-bold text-slate-100 mb-1">Articles</h1>
  <p class="text-sm text-slate-400 mb-8">Analysis, education and issuer profiles for the Australian ETF market.</p>
  {news_html}
  {annual_html}
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
        'Performance':    ('bg-green-900/50',  'text-green-300'),
        'Market Trends':  ('bg-blue-900/50',   'text-blue-300'),
        'Thematic':       ('bg-purple-900/50', 'text-purple-300'),
        'Research':       ('bg-amber-900/50',  'text-amber-300'),
        'Education':      ('bg-teal-900/50',   'text-teal-300'),
        'Issuer Profile': ('bg-orange-900/50', 'text-orange-300'),
        'Annual Report':  ('bg-amber-900/50',  'text-amber-300'),
    }
    bg, fg = category_colors.get(a['category'], ('bg-slate-800', 'text-slate-300'))

    html = _articles_head(a['title']) + _articles_nav(slug) + f"""
<body class="bg-[#1a2e4a] min-h-screen">
<main class="max-w-4xl mx-auto px-5 py-8">
  <div class="bg-[#233d66] rounded-xl border border-[#2e5285] p-7">
    <span class="inline-block {bg} {fg} text-xs font-semibold px-2 py-0.5 rounded mb-3">{a['category']}</span>
    <h1 class="text-2xl font-bold text-slate-100 leading-tight mb-2">{a['title']}</h1>
    <p class="text-slate-400 text-sm mb-1">{a['subtitle']}</p>
    <p class="text-xs text-slate-500 mb-6">{a['date']}</p>
    <article>
      {a['body']}
    </article>
  </div>
  <div class="mt-5">
    <a href="/articles" class="text-sm text-blue-400 hover:text-blue-300 flex items-center gap-1">
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


def _handle_issuer_page(self, slug):
    from insights_pages import get_issuer_page
    html = get_issuer_page(slug)
    self.send_html(html)

def _handle_issuer_api(self, slug):
    conn = get_db()
    try:
        # Resolve slug → canonical issuer name from the etfs table
        issuer_names = [r[0] for r in conn.execute(
            "SELECT DISTINCT issuer FROM etfs WHERE issuer IS NOT NULL ORDER BY issuer"
        ).fetchall()]
        issuer_name = next((n for n in issuer_names if slugify(n) == slug), None)
        if not issuer_name:
            self.send_json({'error': f'Issuer not found: {slug}'}, 404)
            return

        # Website from issuers table
        meta_row = conn.execute(
            "SELECT website FROM issuers WHERE name=?", (issuer_name,)
        ).fetchone()
        website = meta_row['website'] if meta_row and meta_row['website'] else None

        # Aggregate stats
        stats_row = conn.execute("""
            SELECT
                COUNT(*) AS etf_count,
                COALESCE(SUM(fund_size_aud_millions), 0) AS total_fum,
                AVG(CASE WHEN COALESCE(expense_ratio, management_fee) > 0
                         THEN COALESCE(expense_ratio, management_fee) END) AS avg_mer,
                SUM(CASE WHEN fund_size_aud_millions > 0 AND COALESCE(expense_ratio, management_fee) > 0
                         THEN fund_size_aud_millions * COALESCE(expense_ratio, management_fee) END)
                / NULLIF(SUM(CASE WHEN fund_size_aud_millions > 0 AND COALESCE(expense_ratio, management_fee) > 0
                                   THEN fund_size_aud_millions END), 0) AS fum_weighted_mer,
                AVG(return_1y) AS avg_return_1y,
                AVG(distribution_yield) AS avg_yield,
                MIN(inception_date) AS inception_earliest,
                COALESCE(SUM(fund_flow_1m), 0) AS fund_flow_1m,
                COALESCE(SUM(fund_flow_1y), 0) AS fund_flow_1y
            FROM etfs WHERE issuer=?
        """, (issuer_name,)).fetchone()

        # Market total for share %
        total_mkt = conn.execute(
            "SELECT COALESCE(SUM(fund_size_aud_millions),1) FROM etfs"
        ).fetchone()[0] or 1
        market_share = round((stats_row['total_fum'] / total_mkt) * 100, 1)

        # Asset class mix — FUM per class
        ac_rows = conn.execute("""
            SELECT COALESCE(asset_class,'Other') AS ac,
                   COUNT(*) AS cnt,
                   COALESCE(SUM(fund_size_aud_millions),0) AS fum
            FROM etfs WHERE issuer=?
            GROUP BY ac ORDER BY fum DESC
        """, (issuer_name,)).fetchall()

        # All ETFs for this issuer
        etf_rows = conn.execute("""
            SELECT code, name, asset_class, exchange,
                   fund_size_aud_millions, expense_ratio, management_fee,
                   return_1y, return_3y, return_5y, distribution_yield,
                   fund_flow_1m, inception_date, fx_hedged, benchmark, rank_by_fum
            FROM etfs WHERE issuer=?
            ORDER BY fund_size_aud_millions DESC NULLS LAST
        """, (issuer_name,)).fetchall()

        # Related articles — match issuer name in title/slug
        from articles import get_all_articles
        issuer_lower = issuer_name.lower()
        # Also try the first word (e.g. "Betashares" from "Betashares / something")
        issuer_words = issuer_lower.split()[0] if ' ' in issuer_lower else issuer_lower
        all_articles = get_all_articles()
        related = [
            {'slug': a['slug'], 'title': a['title'],
             'category': a['category'], 'date': a['date'], 'subtitle': a.get('subtitle','')}
            for a in all_articles
            if (issuer_lower in a['title'].lower()
                or issuer_words in a['slug']
                or issuer_lower in a.get('subtitle', '').lower()
                or a.get('category') == 'Issuer Profile' and issuer_words in a['slug'])
        ][:6]

        self.send_json({
            'issuer': issuer_name,
            'slug': slug,
            'website': website,
            'stats': {
                'etf_count': stats_row['etf_count'],
                'total_fum': round(stats_row['total_fum'] or 0, 2),
                'market_share_pct': market_share,
                'avg_mer': round(stats_row['avg_mer'] or 0, 3),
                'fum_weighted_mer': round(stats_row['fum_weighted_mer'] or 0, 3),
                'avg_return_1y': round(stats_row['avg_return_1y'] or 0, 2) if stats_row['avg_return_1y'] is not None else None,
                'avg_yield': round(stats_row['avg_yield'] or 0, 2) if stats_row['avg_yield'] is not None else None,
                'inception_earliest': stats_row['inception_earliest'],
                'fund_flow_1m': round(stats_row['fund_flow_1m'] or 0, 2),
                'fund_flow_1y': round(stats_row['fund_flow_1y'] or 0, 2),
            },
            'asset_class_mix': [
                {'ac': r['ac'], 'cnt': r['cnt'], 'fum': round(r['fum'], 2)}
                for r in ac_rows
            ],
            'etfs': [
                {
                    'code': r['code'],
                    'name': r['name'],
                    'asset_class': r['asset_class'],
                    'exchange': r['exchange'],
                    'fund_size_aud_millions': r['fund_size_aud_millions'],
                    'expense_ratio': r['expense_ratio'] or r['management_fee'],
                    'return_1y': r['return_1y'],
                    'return_3y': r['return_3y'],
                    'return_5y': r['return_5y'],
                    'distribution_yield': r['distribution_yield'],
                    'fund_flow_1m': r['fund_flow_1m'],
                    'inception_date': r['inception_date'],
                    'fx_hedged': r['fx_hedged'],
                    'rank_by_fum': r['rank_by_fum'],
                }
                for r in etf_rows
            ],
            'related_articles': related,
        })
    except Exception as e:
        self.send_json({'error': str(e)}, 500)
    finally:
        conn.close()

ETFAPIHandler.handle_issuer_page = _handle_issuer_page
ETFAPIHandler.handle_issuer_api = _handle_issuer_api

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
