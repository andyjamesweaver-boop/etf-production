#!/usr/bin/env python3
"""
Historical ETP Analysis CLI

Uses the etp_monthly and etp_list tables ingested from ETPReportData.xlsx.

Commands:
    industry       Industry-wide AUM and ETF count over time
    issuers        Issuer market share at a given date (or over time)
    flows          Top inflows / outflows over a period
    returns        Best / worst performers by return period
    launches       ETF launch history by year
    fees           MER distribution and trends
    liquidity      Spread and trading activity
    fund           Full history for a single fund
    screener       Filter ETFs by various criteria
"""

import argparse
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scrapers.config import DB_PATH
from scrapers.db_writer import get_connection


# ── helpers ──────────────────────────────────────────────────────────────────

def _fmt_m(v, decimals=1):
    """Format a raw AUD value as millions/billions string."""
    if v is None:
        return 'N/A'
    if abs(v) >= 1e9:
        return f'${v/1e9:.{decimals}f}B'
    if abs(v) >= 1e6:
        return f'${v/1e6:.{decimals}f}M'
    return f'${v:,.0f}'


def _fmt_pct(v, decimals=2):
    if v is None:
        return 'N/A'
    return f'{v*100:.{decimals}f}%' if abs(v) < 10 else f'{v:.{decimals}f}%'


def _latest_date(conn) -> str:
    return conn.execute("SELECT MAX(date) FROM etp_monthly").fetchone()[0]


def _earliest_date(conn) -> str:
    return conn.execute("SELECT MIN(date) FROM etp_monthly").fetchone()[0]


def _month_before(conn, date_str: str) -> str | None:
    row = conn.execute(
        "SELECT MAX(date) FROM etp_monthly WHERE date < ?", (date_str,)
    ).fetchone()
    return row[0] if row else None


def _resolve_date(conn, date_arg: str | None) -> str:
    if date_arg:
        return date_arg
    return _latest_date(conn)


def _print_table(headers: list, rows: list, col_widths: list | None = None):
    if not rows:
        print("  (no data)")
        return
    if col_widths is None:
        col_widths = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0))
                      for i, h in enumerate(headers)]
    fmt = '  ' + '  '.join(f'{{:<{w}}}' for w in col_widths)
    sep = '  ' + '  '.join('-' * w for w in col_widths)
    print(fmt.format(*headers))
    print(sep)
    for row in rows:
        print(fmt.format(*[str(v) for v in row]))


# ── commands ──────────────────────────────────────────────────────────────────

def cmd_industry(conn, args):
    """Total industry AUM, ETF count, and net flows — monthly or annually."""
    freq = args.freq if hasattr(args, 'freq') else 'annual'
    limit = args.limit if hasattr(args, 'limit') else 20

    if freq == 'monthly':
        sql = """
            SELECT date,
                   COUNT(DISTINCT code) AS etfs,
                   SUM(market_cap) / 1e9 AS aum_b,
                   SUM(funds_flow) / 1e6 AS flows_m,
                   SUM(transacted_value) / 1e6 AS traded_m
            FROM etp_monthly
            WHERE market_cap > 0
            GROUP BY date
            ORDER BY date DESC
            LIMIT ?
        """
        rows = conn.execute(sql, (limit,)).fetchall()
        print(f"\nIndustry — Monthly (most recent {limit} months)\n")
        _print_table(
            ['Date', 'ETFs', 'AUM ($B)', 'Net Flows ($M)', 'Traded ($M)'],
            [(r[0], r[1], f'{r[2]:.1f}' if r[2] else 'N/A',
              f'{r[3]:+.0f}' if r[3] else 'N/A',
              f'{r[4]:.0f}' if r[4] else 'N/A') for r in rows],
        )
    else:
        sql = """
            SELECT strftime('%Y', date) AS year,
                   MAX(date) AS last_date,
                   MAX(CASE WHEN date = (SELECT MAX(d2.date) FROM etp_monthly d2 WHERE strftime('%Y',d2.date)=strftime('%Y',etp_monthly.date))
                       THEN COUNT(DISTINCT code) END) AS etfs,
                   MAX(CASE WHEN date = (SELECT MAX(d2.date) FROM etp_monthly d2 WHERE strftime('%Y',d2.date)=strftime('%Y',etp_monthly.date))
                       THEN total_aum END) AS year_end_aum,
                   SUM(funds_flow) / 1e6 AS annual_flows_m,
                   SUM(transacted_value) / 1e9 AS annual_traded_b
            FROM (
                SELECT date, code, market_cap, funds_flow, transacted_value,
                       SUM(market_cap) OVER (PARTITION BY date) AS total_aum,
                       COUNT(DISTINCT code) OVER (PARTITION BY date) AS etf_count
                FROM etp_monthly WHERE market_cap > 0
            )
            GROUP BY year
            ORDER BY year DESC
            LIMIT ?
        """
        # Simpler equivalent
        sql = """
            SELECT
                strftime('%Y', date) AS year,
                COUNT(DISTINCT code) AS peak_etfs,
                MAX(date) AS last_month,
                SUM(funds_flow) / 1e6 AS annual_flows_m,
                SUM(transacted_value) / 1e9 AS annual_traded_b
            FROM etp_monthly
            WHERE market_cap > 0
            GROUP BY year
            ORDER BY year DESC
            LIMIT ?
        """
        # Get year-end AUM separately
        aum_sql = """
            SELECT strftime('%Y', date) AS year, SUM(market_cap) / 1e9 AS aum_b
            FROM etp_monthly
            WHERE market_cap > 0
              AND date IN (SELECT MAX(date) FROM etp_monthly GROUP BY strftime('%Y', date))
            GROUP BY year
        """
        aum_by_year = {r[0]: r[1] for r in conn.execute(aum_sql).fetchall()}
        rows = conn.execute(sql, (limit,)).fetchall()

        print(f"\nIndustry — Annual (most recent {limit} years)\n")
        out = []
        for r in rows:
            year, etfs, last_mo, flows, traded = r
            aum = aum_by_year.get(year)
            out.append((year, etfs, f'{aum:.1f}' if aum else 'N/A',
                        f'{flows:+.0f}' if flows else 'N/A',
                        f'{traded:.1f}' if traded else 'N/A'))
        _print_table(
            ['Year', 'ETFs', 'Year-End AUM ($B)', 'Net Flows ($M)', 'Traded ($B)'], out
        )

    print()


def cmd_issuers(conn, args):
    """Issuer market share at a given date."""
    as_of = _resolve_date(conn, getattr(args, 'date', None))
    limit = getattr(args, 'limit', 15)

    sql = """
        SELECT COALESCE(l.trim_issuer, l.issuer, m.code) AS issuer,
               COUNT(DISTINCT m.code) AS funds,
               SUM(m.market_cap) / 1e9 AS aum_b,
               SUM(m.funds_flow) / 1e6 AS flow_m,
               100.0 * SUM(m.market_cap) / SUM(SUM(m.market_cap)) OVER () AS share_pct
        FROM etp_monthly m
        LEFT JOIN etp_list l ON l.ticker = m.code
        WHERE m.date = ? AND m.market_cap > 0
        GROUP BY issuer
        ORDER BY aum_b DESC
        LIMIT ?
    """
    rows = conn.execute(sql, (as_of, limit)).fetchall()

    total = sum(r[2] for r in rows if r[2])
    print(f"\nIssuer Rankings — {as_of}  (total: ${total:.1f}B)\n")
    _print_table(
        ['Issuer', 'Funds', 'AUM ($B)', 'Flows ($M)', 'Share (%)'],
        [(r[0] or '?', r[1], f'{r[2]:.2f}' if r[2] else '—',
          f'{r[3]:+.0f}' if r[3] else '—',
          f'{r[4]:.1f}%' if r[4] else '—') for r in rows],
    )
    print()


def cmd_flows(conn, args):
    """Top ETFs by net inflows or outflows over a period."""
    end = _resolve_date(conn, getattr(args, 'date', None))
    months = getattr(args, 'months', 12)
    direction = getattr(args, 'direction', 'in')
    limit = getattr(args, 'limit', 20)

    order = 'DESC' if direction in ('in', 'inflows') else 'ASC'
    sql = f"""
        SELECT m.code,
               COALESCE(l.name, m.code) AS name,
               COALESCE(l.trim_issuer, l.issuer) AS issuer,
               SUM(m.funds_flow) / 1e6 AS flow_m,
               MAX(m.market_cap) / 1e6 AS latest_aum_m
        FROM etp_monthly m
        LEFT JOIN etp_list l ON l.ticker = m.code
        WHERE m.date <= ?
          AND m.date > date(?, '-{months} months')
        GROUP BY m.code
        ORDER BY flow_m {order}
        LIMIT ?
    """
    rows = conn.execute(sql, (end, end, limit)).fetchall()

    label = 'Inflows' if direction in ('in', 'inflows') else 'Outflows'
    print(f"\nTop {limit} by Net {label} — last {months} months to {end}\n")
    _print_table(
        ['Code', 'Name', 'Issuer', 'Net Flow ($M)', 'Latest AUM ($M)'],
        [(r[0], (r[1] or '')[:35], (r[2] or '')[:15],
          f'{r[3]:+.1f}' if r[3] else '—',
          f'{r[4]:.0f}' if r[4] else '—') for r in rows],
    )
    print()


def cmd_returns(conn, args):
    """Best / worst performers by return period."""
    as_of = _resolve_date(conn, getattr(args, 'date', None))
    period = getattr(args, 'period', '1y')
    direction = getattr(args, 'direction', 'best')
    limit = getattr(args, 'limit', 20)
    min_aum = getattr(args, 'min_aum', 0)

    col_map = {'1m': 'return_1m', '1y': 'return_1y', '3y': 'return_3y', '5y': 'return_5y'}
    col = col_map.get(period, 'return_1y')
    order = 'DESC' if direction == 'best' else 'ASC'

    sql = f"""
        SELECT m.code,
               COALESCE(l.name, m.code) AS name,
               COALESCE(l.trim_issuer, l.issuer) AS issuer,
               m.{col} AS ret,
               m.market_cap / 1e6 AS aum_m
        FROM etp_monthly m
        LEFT JOIN etp_list l ON l.ticker = m.code
        WHERE m.date = ?
          AND m.{col} IS NOT NULL
          AND m.market_cap >= ?
        ORDER BY m.{col} {order}
        LIMIT ?
    """
    rows = conn.execute(sql, (as_of, min_aum * 1e6, limit)).fetchall()

    label = 'Best' if direction == 'best' else 'Worst'
    print(f"\n{label} {limit} performers — {period} return as of {as_of}\n")
    _print_table(
        ['Code', 'Name', 'Issuer', f'{period} Return', 'AUM ($M)'],
        [(r[0], (r[1] or '')[:40], (r[2] or '')[:15],
          _fmt_pct(r[3]), f'{r[4]:.0f}' if r[4] else '—') for r in rows],
    )
    print()


def cmd_launches(conn, args):
    """ETF launch history by year."""
    sql = """
        SELECT strftime('%Y', admission_date) AS year,
               COUNT(*) AS launches,
               GROUP_CONCAT(DISTINCT COALESCE(trim_issuer, issuer)) AS issuers
        FROM etp_list
        WHERE admission_date IS NOT NULL
          AND admission_date != ''
          AND strftime('%Y', admission_date) >= '2000'
        GROUP BY year
        ORDER BY year DESC
    """
    rows = conn.execute(sql).fetchall()
    print("\nETF Launches by Year\n")
    _print_table(
        ['Year', 'Launches', 'Issuers'],
        [(r[0], r[1], (r[2] or '')[:70]) for r in rows],
        col_widths=[6, 9, 72],
    )

    # Also show by issuer
    by_issuer = conn.execute("""
        SELECT COALESCE(trim_issuer, issuer, 'Unknown') AS issuer,
               COUNT(*) AS total,
               SUM(CASE WHEN is_listed = 1 THEN 1 ELSE 0 END) AS active
        FROM etp_list
        WHERE admission_date IS NOT NULL
          AND strftime('%Y', admission_date) >= '2000'
        GROUP BY issuer
        ORDER BY total DESC
        LIMIT 20
    """).fetchall()
    print("\nAll-Time Launches by Issuer\n")
    _print_table(['Issuer', 'Total Launched', 'Currently Active'], by_issuer)
    print()


def cmd_fees(conn, args):
    """MER distribution and trends."""
    as_of = _resolve_date(conn, getattr(args, 'date', None))
    min_aum = getattr(args, 'min_aum', 0)

    # Distribution by quartile bucket
    sql = """
        SELECT
            CASE
                WHEN mer <= 0.10 THEN '0–0.10%'
                WHEN mer <= 0.20 THEN '0.11–0.20%'
                WHEN mer <= 0.30 THEN '0.21–0.30%'
                WHEN mer <= 0.50 THEN '0.31–0.50%'
                WHEN mer <= 0.75 THEN '0.51–0.75%'
                WHEN mer <= 1.00 THEN '0.76–1.00%'
                ELSE '> 1.00%'
            END AS bucket,
            COUNT(*) AS funds,
            ROUND(AVG(market_cap) / 1e6, 0) AS avg_aum_m,
            ROUND(SUM(market_cap) / 1e9, 1) AS total_aum_b
        FROM etp_monthly
        WHERE date = ? AND mer IS NOT NULL AND mer > 0 AND market_cap >= ?
        GROUP BY bucket
        ORDER BY MIN(mer)
    """
    rows = conn.execute(sql, (as_of, min_aum * 1e6)).fetchall()
    print(f"\nMER Distribution — {as_of}\n")
    _print_table(['MER Bucket', 'Funds', 'Avg AUM ($M)', 'Total AUM ($B)'], rows)

    # AUM-weighted average MER trend (annually)
    trend = conn.execute("""
        SELECT strftime('%Y', date) AS year, MAX(date) AS last_month,
               ROUND(SUM(mer * market_cap) / NULLIF(SUM(market_cap), 0), 3) AS wtd_mer,
               ROUND(AVG(mer), 3) AS simple_avg_mer,
               COUNT(*) AS funds
        FROM etp_monthly
        WHERE mer IS NOT NULL AND mer > 0 AND market_cap > 0
          AND date IN (SELECT MAX(date) FROM etp_monthly GROUP BY strftime('%Y', date))
        GROUP BY year
        ORDER BY year DESC
        LIMIT 12
    """).fetchall()
    print("\nAUM-Weighted Average MER — Annual Trend\n")
    _print_table(['Year', 'Last Month', 'Wtd Avg MER', 'Simple Avg', 'Funds'], trend)
    print()


def cmd_liquidity(conn, args):
    """Spread, depth, and trading activity."""
    as_of = _resolve_date(conn, getattr(args, 'date', None))
    limit = getattr(args, 'limit', 20)

    # Most liquid by trading value
    sql = """
        SELECT m.code,
               COALESCE(l.name, m.code) AS name,
               m.transacted_value / 1e6 AS traded_m,
               m.num_trades,
               m.spread_pct * 100 AS spread_bp,
               m.market_cap / 1e6 AS aum_m
        FROM etp_monthly m
        LEFT JOIN etp_list l ON l.ticker = m.code
        WHERE m.date = ? AND m.transacted_value > 0
        ORDER BY m.transacted_value DESC
        LIMIT ?
    """
    rows = conn.execute(sql, (as_of, limit)).fetchall()
    print(f"\nMost Traded ETFs — {as_of}\n")
    _print_table(
        ['Code', 'Name', 'Traded ($M)', 'Trades', 'Spread (%)', 'AUM ($M)'],
        [(r[0], (r[1] or '')[:35], f'{r[2]:.1f}' if r[2] else '—',
          f'{r[3]:,}' if r[3] else '—',
          f'{r[4]:.3f}' if r[4] else '—',
          f'{r[5]:.0f}' if r[5] else '—') for r in rows],
    )

    # Tightest spreads
    tight = conn.execute("""
        SELECT m.code, COALESCE(l.name, m.code) AS name,
               m.spread_pct * 100 AS spread_pct,
               m.market_cap / 1e6 AS aum_m
        FROM etp_monthly m
        LEFT JOIN etp_list l ON l.ticker = m.code
        WHERE m.date = ? AND m.spread_pct > 0 AND m.market_cap > 100e6
        ORDER BY m.spread_pct ASC
        LIMIT 15
    """, (as_of,)).fetchall()
    print(f"\nTightest Bid-Ask Spreads (AUM > $100M) — {as_of}\n")
    _print_table(
        ['Code', 'Name', 'Spread (%)', 'AUM ($M)'],
        [(r[0], (r[1] or '')[:40], f'{r[2]:.4f}', f'{r[3]:.0f}') for r in tight],
    )
    print()


def cmd_fund(conn, args):
    """Full monthly history for a single fund."""
    code = args.code.upper()
    limit = getattr(args, 'limit', 36)

    info = conn.execute("SELECT * FROM etp_list WHERE ticker = ?", (code,)).fetchone()
    if info:
        print(f"\n{code} — {info['name']}")
        print(f"  Issuer: {info['issuer']}  |  Asset class: {info['asset_class']}"
              f"  |  Admitted: {info['admission_date']}  |  Listed: {'Yes' if info['is_listed'] else 'No'}")
        if info['reference_benchmark']:
            print(f"  Benchmark: {info['reference_benchmark']}")
        print()

    rows = conn.execute("""
        SELECT date, last_price, market_cap/1e6, funds_flow/1e6,
               return_1m*100, return_1y*100, distribution_yield*100,
               spread_pct*100, mer, total_units/1e6
        FROM etp_monthly
        WHERE code = ?
        ORDER BY date DESC
        LIMIT ?
    """, (code, limit)).fetchall()

    if not rows:
        print(f"  No data found for {code}")
        return

    print(f"Monthly history (most recent {len(rows)} months)\n")
    _print_table(
        ['Date', 'Price', 'AUM ($M)', 'Flow ($M)', '1m Ret%', '1y Ret%', 'Yield%', 'Spread%', 'MER', 'Units (M)'],
        [(r[0],
          f'{r[1]:.2f}' if r[1] else '—',
          f'{r[2]:.0f}' if r[2] else '—',
          f'{r[3]:+.1f}' if r[3] else '—',
          f'{r[4]:.2f}' if r[4] is not None else '—',
          f'{r[5]:.2f}' if r[5] is not None else '—',
          f'{r[6]:.2f}' if r[6] is not None else '—',
          f'{r[7]:.3f}' if r[7] else '—',
          f'{r[8]:.2f}' if r[8] else '—',
          f'{r[9]:.2f}' if r[9] else '—') for r in rows],
    )
    print()


def cmd_screener(conn, args):
    """Filter ETFs by AUM, returns, MER, asset class, issuer."""
    as_of = _resolve_date(conn, getattr(args, 'date', None))

    filters = ["m.date = ?", "m.market_cap > 0"]
    params = [as_of]

    min_aum = getattr(args, 'min_aum', None)
    if min_aum:
        filters.append("m.market_cap >= ?")
        params.append(float(min_aum) * 1e6)

    max_mer = getattr(args, 'max_mer', None)
    if max_mer:
        filters.append("m.mer <= ?")
        params.append(float(max_mer))

    min_1y = getattr(args, 'min_1y', None)
    if min_1y:
        filters.append("m.return_1y >= ?")
        params.append(float(min_1y) / 100)

    asset_class = getattr(args, 'asset_class', None)
    if asset_class:
        filters.append("""CASE
            WHEN l.asset_class = 'Equity' AND l.segment LIKE 'Equity - Australia%' THEN 'Australian Equities'
            WHEN l.asset_class = 'Equity' THEN 'International Equities'
            ELSE COALESCE(l.asset_class, 'Other')
        END LIKE ?""")
        params.append(f'%{asset_class}%')

    issuer = getattr(args, 'issuer', None)
    if issuer:
        filters.append("(l.trim_issuer LIKE ? OR l.issuer LIKE ?)")
        params.extend([f'%{issuer}%', f'%{issuer}%'])

    limit = getattr(args, 'limit', 30)
    sort = getattr(args, 'sort', 'aum')
    sort_col = {'aum': 'm.market_cap', 'flows': 'm.funds_flow',
                'return_1y': 'm.return_1y', 'mer': 'm.mer',
                'spread': 'm.spread_pct'}.get(sort, 'm.market_cap')

    sql = f"""
        SELECT m.code,
               COALESCE(l.name, m.code) AS name,
               COALESCE(l.trim_issuer, l.issuer) AS issuer,
               CASE
                   WHEN l.asset_class = 'Equity' AND l.segment LIKE 'Equity - Australia%' THEN 'Australian Equities'
                   WHEN l.asset_class = 'Equity' THEN 'International Equities'
                   ELSE COALESCE(l.asset_class, 'Other')
               END AS asset_class,
               m.market_cap / 1e6 AS aum_m,
               m.mer,
               m.return_1y * 100 AS ret_1y,
               m.funds_flow / 1e6 AS flow_m,
               m.distribution_yield * 100 AS yield_pct
        FROM etp_monthly m
        LEFT JOIN etp_list l ON l.ticker = m.code
        WHERE {' AND '.join(filters)}
        ORDER BY {sort_col} DESC
        LIMIT ?
    """
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()

    print(f"\nScreener results — {as_of} ({len(rows)} funds)\n")
    _print_table(
        ['Code', 'Name', 'Issuer', 'Asset Class', 'AUM ($M)', 'MER', '1y Ret%', 'Flow ($M)', 'Yield%'],
        [(r[0], (r[1] or '')[:30], (r[2] or '')[:12], (r[3] or '')[:18],
          f'{r[4]:.0f}' if r[4] else '—',
          f'{r[5]:.2f}' if r[5] else '—',
          f'{r[6]:.1f}' if r[6] is not None else '—',
          f'{r[7]:+.0f}' if r[7] else '—',
          f'{r[8]:.1f}' if r[8] is not None else '—') for r in rows],
    )
    print()


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Australian ETP Historical Analysis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest='command', required=True)

    # industry
    p = sub.add_parser('industry', help='Industry AUM and ETF count over time')
    p.add_argument('--freq', choices=['annual', 'monthly'], default='annual')
    p.add_argument('--limit', type=int, default=20)

    # issuers
    p = sub.add_parser('issuers', help='Issuer market share')
    p.add_argument('--date', help='As-of date (YYYY-MM-DD), default latest')
    p.add_argument('--limit', type=int, default=15)

    # flows
    p = sub.add_parser('flows', help='Top ETFs by net inflows/outflows')
    p.add_argument('--date', help='End date (default latest)')
    p.add_argument('--months', type=int, default=12)
    p.add_argument('--direction', choices=['in', 'out'], default='in')
    p.add_argument('--limit', type=int, default=20)

    # returns
    p = sub.add_parser('returns', help='Best/worst performers')
    p.add_argument('--date', help='As-of date (default latest)')
    p.add_argument('--period', choices=['1m', '1y', '3y', '5y'], default='1y')
    p.add_argument('--direction', choices=['best', 'worst'], default='best')
    p.add_argument('--min-aum', type=float, default=0, dest='min_aum',
                   help='Minimum AUM in $M')
    p.add_argument('--limit', type=int, default=20)

    # launches
    p = sub.add_parser('launches', help='ETF launch history')

    # fees
    p = sub.add_parser('fees', help='MER distribution and trends')
    p.add_argument('--date', help='As-of date (default latest)')
    p.add_argument('--min-aum', type=float, default=0, dest='min_aum',
                   help='Minimum AUM in $M')

    # liquidity
    p = sub.add_parser('liquidity', help='Spread and trading activity')
    p.add_argument('--date', help='As-of date (default latest)')
    p.add_argument('--limit', type=int, default=20)

    # fund
    p = sub.add_parser('fund', help='Full history for a single fund')
    p.add_argument('code', help='ASX ticker code')
    p.add_argument('--limit', type=int, default=36, help='Number of months to show')

    # screener
    p = sub.add_parser('screener', help='Filter ETFs by criteria')
    p.add_argument('--date', help='As-of date (default latest)')
    p.add_argument('--min-aum', type=float, dest='min_aum', help='Min AUM ($M)')
    p.add_argument('--max-mer', type=float, dest='max_mer', help='Max MER (%%)')
    p.add_argument('--min-1y', type=float, dest='min_1y', help='Min 1y return (%%)')
    p.add_argument('--asset-class', dest='asset_class', help='Asset class filter')
    p.add_argument('--issuer', help='Issuer name filter')
    p.add_argument('--sort', choices=['aum', 'flows', 'return_1y', 'mer', 'spread'],
                   default='aum')
    p.add_argument('--limit', type=int, default=30)

    args = parser.parse_args()
    conn = get_connection(DB_PATH)
    conn.row_factory = __import__('sqlite3').Row

    cmd_map = {
        'industry': cmd_industry,
        'issuers': cmd_issuers,
        'flows': cmd_flows,
        'returns': cmd_returns,
        'launches': cmd_launches,
        'fees': cmd_fees,
        'liquidity': cmd_liquidity,
        'fund': cmd_fund,
        'screener': cmd_screener,
    }
    cmd_map[args.command](conn, args)
    conn.close()


if __name__ == '__main__':
    main()
