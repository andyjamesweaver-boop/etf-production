"""
NAV (Net Asset Value) history fetcher for Australian ETFs.

Sources:
  - Yahoo Finance (yfinance): daily navPrice for most ETFs (~78% coverage)
  - BlackRock/iShares chart AJAX: full historical NAV + price back to inception

Premium/discount calculation:
  premium_discount_pct = (close_price - nav) / nav * 100
    positive = trading at a premium to NAV
    negative = trading at a discount to NAV

Usage:
    python3 -m scrapers.nav_fetcher                 # daily update (Yahoo Finance)
    python3 -m scrapers.nav_fetcher --backfill       # + iShares historical backfill
    python3 -m scrapers.nav_fetcher --code VAS NDQ   # specific ETFs only
"""

import re
import time
import logging
import argparse
from datetime import datetime, date, timedelta

from scrapers.db_writer import get_connection, log_scrape
from scrapers.config import DB_PATH
from scrapers.base_scraper import fetch

logger = logging.getLogger(__name__)

# iShares AU product ID map — imported from config where it exists
try:
    from scrapers.config import ISHARES_AU_PRODUCTS
except ImportError:
    ISHARES_AU_PRODUCTS = {}

_RATE_LIMIT = 1.0   # seconds between Yahoo Finance requests
_BLACKROCK_RATE = 2.0


# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────

def _today() -> str:
    return date.today().isoformat()


def _upsert_nav(conn, etf_code: str, nav_date: str, nav: float,
                close_price: float | None, source: str, *, commit: bool = False):
    prem = None
    if close_price and nav:
        prem = round((close_price - nav) / nav * 100, 4)

    conn.execute("""
        INSERT INTO nav_history (etf_code, date, nav, close_price, premium_discount_pct, source)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(etf_code, date) DO UPDATE SET
            nav                  = COALESCE(excluded.nav, nav),
            close_price          = COALESCE(excluded.close_price, close_price),
            premium_discount_pct = COALESCE(excluded.premium_discount_pct, premium_discount_pct),
            source               = COALESCE(excluded.source, source),
            last_updated         = CURRENT_TIMESTAMP
    """, (etf_code, nav_date, nav, close_price, prem, source))

    if commit:
        conn.commit()


def _get_close_price(conn, etf_code: str, nav_date: str) -> float | None:
    """Look up close price from price_history for a given date."""
    row = conn.execute(
        "SELECT close FROM price_history WHERE etf_code = ? AND date = ?",
        (etf_code, nav_date)
    ).fetchone()
    return row['close'] if row else None


def _get_current_price(conn, etf_code: str) -> float | None:
    """Fall back to current_price from etfs table for today."""
    row = conn.execute(
        "SELECT current_price FROM etfs WHERE code = ?", (etf_code,)
    ).fetchone()
    return row['current_price'] if row else None


# ────────────────────────────────────────────────────────────────────────────
# Yahoo Finance — daily NAV for all ETFs
# ────────────────────────────────────────────────────────────────────────────

def fetch_yahoo_nav(conn, codes: list[str]) -> int:
    """
    Fetch today's NAV from Yahoo Finance navPrice field for each ETF.
    Calculates premium/discount using today's close from price_history or
    current_price from etfs table.
    Returns count of ETFs updated.
    """
    try:
        import yfinance as yf
    except ImportError:
        logger.error("yfinance not installed — run: pip install yfinance")
        return 0

    today = _today()
    updated = 0

    for i, code in enumerate(codes):
        try:
            ticker = yf.Ticker(f"{code}.AX")
            nav = ticker.info.get('navPrice')
            if not nav:
                continue

            close = _get_close_price(conn, code, today)
            if close is None:
                close = _get_current_price(conn, code)

            _upsert_nav(conn, code, today, float(nav), close, 'yahoo_finance')
            updated += 1

        except Exception as e:
            logger.debug(f"Yahoo NAV: {code}: {e}")

        if (i + 1) % 50 == 0:
            conn.commit()
            logger.info(f"  Yahoo NAV progress: {i+1}/{len(codes)} ({updated} with NAV)")

        time.sleep(_RATE_LIMIT)

    conn.commit()
    logger.info(f"Yahoo Finance NAV: {updated}/{len(codes)} ETFs updated for {today}")
    return updated


# ────────────────────────────────────────────────────────────────────────────
# iShares — historical NAV backfill from BlackRock chart AJAX
# ────────────────────────────────────────────────────────────────────────────

_DATE_UTC_RE = re.compile(
    r'\{x:Date\.UTC\((\d+),(\d+),(\d+)\),y:Number\(\(([0-9.]+)\)\.toFixed\(\d+\)\)'
)


def _parse_blackrock_js_series(js_text: str) -> list[tuple[str, float]]:
    """
    Parse a JS array of {x:Date.UTC(year,month0,day),y:Number((val).toFixed(2))}
    objects into a list of (date_str, value) tuples.
    Note: month is 0-indexed in Date.UTC.
    """
    results = []
    for m in _DATE_UTC_RE.finditer(js_text):
        year, month0, day, val = int(m.group(1)), int(m.group(2)), int(m.group(3)), float(m.group(4))
        d = date(year, month0 + 1, day)
        results.append((d.isoformat(), val))
    return results


def _find_ishares_chart_ajax(product_id: int) -> str | None:
    """
    Fetch the iShares AU product page and extract the chart AJAX URL.
    Returns the full URL or None.
    """
    page_url = f"https://www.blackrock.com/au/products/{product_id}/"
    resp = fetch(page_url, headers={
        'User-Agent': 'Mozilla/5.0',
        'Referer': 'https://www.blackrock.com/au/',
    })
    if not resp or resp.status_code != 200:
        return None

    # Look for data-ajaxUri with tab=chart
    m = re.search(r'data-ajaxUri="(/au/products/\d+/[^"]+\.ajax)\?tab=chart"', resp.text)
    if m:
        return f"https://www.blackrock.com{m.group(1)}?tab=chart"

    # Fallback: find any .ajax endpoint and try tab=chart
    m2 = re.search(r'(/au/products/\d+/[^/"\']+/(\d+)\.ajax)', resp.text)
    if m2:
        return f"https://www.blackrock.com{m2.group(1)}?tab=chart"

    return None


def fetch_ishares_nav_history(conn, code: str, product_id: int) -> int:
    """
    Fetch full NAV + price history from BlackRock chart AJAX for one iShares ETF.
    Returns number of rows inserted/updated.
    """
    chart_url = _find_ishares_chart_ajax(product_id)
    if not chart_url:
        logger.warning(f"iShares {code}: could not find chart AJAX URL")
        return 0

    time.sleep(_BLACKROCK_RATE)
    resp = fetch(chart_url, headers={
        'Referer': f'https://www.blackrock.com/au/products/{product_id}/',
    })
    if not resp or resp.status_code != 200:
        logger.warning(f"iShares {code}: chart AJAX request failed")
        return 0

    text = resp.text

    # Extract navData and priceData JS arrays
    nav_m = re.search(r'var navData\s*=\s*(\[.*?\]);', text, re.S)
    price_m = re.search(r'var priceData\s*=\s*(\[.*?\]);', text, re.S)

    if not nav_m:
        logger.warning(f"iShares {code}: navData not found in chart response")
        return 0

    nav_series = _parse_blackrock_js_series(nav_m.group(1))
    price_series = dict(_parse_blackrock_js_series(price_m.group(1))) if price_m else {}

    if not nav_series:
        logger.warning(f"iShares {code}: no NAV data points parsed")
        return 0

    # Also pull close prices from our price_history table to supplement
    ph_rows = conn.execute(
        "SELECT date, close FROM price_history WHERE etf_code = ?", (code,)
    ).fetchall()
    ph_prices = {r['date']: r['close'] for r in ph_rows}

    count = 0
    for nav_date, nav in nav_series:
        close = price_series.get(nav_date) or ph_prices.get(nav_date)
        _upsert_nav(conn, code, nav_date, nav, close, 'ishares_chart')
        count += 1

        if count % 500 == 0:
            conn.commit()

    conn.commit()
    logger.info(f"iShares {code}: {count} NAV records backfilled (product {product_id})")
    return count


def backfill_ishares(conn) -> int:
    """Backfill full NAV history for all iShares AU ETFs."""
    total = 0
    for code, product_id in ISHARES_AU_PRODUCTS.items():
        if not isinstance(product_id, int):
            continue
        logger.info(f"iShares backfill: {code} (product {product_id})")
        try:
            n = fetch_ishares_nav_history(conn, code, product_id)
            total += n
        except Exception as e:
            logger.warning(f"iShares backfill: {code} failed: {e}")
        time.sleep(_BLACKROCK_RATE)
    return total


# ────────────────────────────────────────────────────────────────────────────
# Update premium/discount on existing rows that have close but no prem/disc
# ────────────────────────────────────────────────────────────────────────────

def fill_missing_premiums(conn) -> int:
    """
    For nav_history rows that have close_price but no premium_discount_pct,
    (re)calculate it. Also joins price_history to fill missing close prices.
    """
    # First: join price_history to fill missing close prices in nav_history
    conn.execute("""
        UPDATE nav_history SET close_price = (
            SELECT ph.close FROM price_history ph
            WHERE ph.etf_code = nav_history.etf_code
              AND ph.date     = nav_history.date
        )
        WHERE close_price IS NULL
    """)

    # Then: calculate premium/discount for all rows that now have both values
    result = conn.execute("""
        UPDATE nav_history
        SET premium_discount_pct = ROUND((close_price - nav) / nav * 100, 4)
        WHERE close_price IS NOT NULL
          AND nav IS NOT NULL
          AND nav > 0
    """)
    conn.commit()
    return result.rowcount


# ────────────────────────────────────────────────────────────────────────────
# Update etfs table with latest NAV and premium/discount
# ────────────────────────────────────────────────────────────────────────────

def update_etf_nav_fields(conn):
    """
    Update nav_per_unit and premium_discount_pct on the etfs table
    using the most recent nav_history row for each ETF.
    """
    conn.execute("""
        UPDATE etfs SET
            nav_per_unit         = nh.nav,
            premium_discount_pct = nh.premium_discount_pct
        FROM (
            SELECT etf_code, nav, premium_discount_pct
            FROM nav_history
            WHERE (etf_code, date) IN (
                SELECT etf_code, MAX(date) FROM nav_history GROUP BY etf_code
            )
        ) nh
        WHERE etfs.code = nh.etf_code
    """)
    conn.commit()


# ────────────────────────────────────────────────────────────────────────────
# Main entry point
# ────────────────────────────────────────────────────────────────────────────

def scrape_nav(db_path=None, codes: list[str] | None = None,
               backfill: bool = False) -> int:
    """
    Main NAV scraping function.
      - Daily: fetch today's NAV from Yahoo Finance for all (or specified) ETFs.
      - backfill=True: also run full iShares historical NAV backfill.
    Returns total rows updated.
    """
    started = datetime.utcnow()
    conn = get_connection(db_path)
    total = 0

    # Resolve codes
    if codes is None:
        rows = conn.execute(
            "SELECT code FROM etfs WHERE exchange = 'ASX' ORDER BY rank_by_fum"
        ).fetchall()
        codes = [r['code'] for r in rows]

    logger.info(f"NAV fetcher: {len(codes)} ETFs, backfill={backfill}")

    # 1. Yahoo Finance — today's NAV
    yahoo_count = fetch_yahoo_nav(conn, codes)
    total += yahoo_count

    # 2. iShares historical backfill (optional)
    if backfill and ISHARES_AU_PRODUCTS:
        logger.info("Running iShares full historical NAV backfill...")
        ishares_count = backfill_ishares(conn)
        total += ishares_count
        logger.info(f"iShares backfill: {ishares_count} total rows")

    # 3. Fill missing premium/discount calculations
    filled = fill_missing_premiums(conn)
    logger.info(f"Premium/discount: recalculated {filled} rows")

    # 4. Update etfs table with latest NAV
    update_etf_nav_fields(conn)

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, 'nav_fetcher', 'success', records_affected=total,
               duration_secs=duration, started_at=started.isoformat())
    conn.close()

    logger.info(f"NAV fetcher complete: {total} rows updated in {duration:.1f}s")
    return total


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')

    parser = argparse.ArgumentParser(description='Fetch daily NAV for ASX ETFs')
    parser.add_argument('--backfill', action='store_true',
                        help='Run iShares full historical NAV backfill')
    parser.add_argument('--code', nargs='+', metavar='CODE',
                        help='Only fetch for these ETF codes')
    args = parser.parse_args()

    scrape_nav(codes=args.code, backfill=args.backfill)
