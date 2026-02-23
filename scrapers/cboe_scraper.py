"""
Cboe Australia ETF scraper.

Fetches the list of Cboe-listed ETPs, with a fallback to marketindex.com.au.
"""

import os
import re
import logging
from datetime import datetime, date

from scrapers.config import (
    CBOE_FUNDS_URL, CBOE_API_URL, MARKETINDEX_ETFS_URL,
    CBOE_MONTHLY_REPORT_CDN_BASE, DATA_DIR,
    CXA_ISSUER_URLS,
    normalise_issuer, normalise_asset_class,
)
from scrapers.base_scraper import fetch, fetch_json, download_file
from scrapers.db_writer import get_connection, upsert_etfs, log_scrape, update_issuer_stats

logger = logging.getLogger(__name__)


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        s = str(val).replace(',', '').replace('$', '').replace('%', '').strip()
        if s in ('', '-', 'N/A'):
            return None
        return float(s)
    except (ValueError, TypeError):
        return None


def _from_decimal_pct(val) -> float | None:
    """Convert a decimal fraction (e.g. -0.0053) to percentage (-0.53). Returns None for None or zero."""
    if not val:
        return None
    return round(val * 100, 4)


def _format_date(val) -> str | None:
    """Return a YYYY-MM-DD string from an openpyxl date object or date string. Returns None for non-dates."""
    if val is None:
        return None
    if isinstance(val, (date, datetime)):
        return val.strftime('%Y-%m-%d')
    s = str(val).strip()
    if re.match(r'^\d{4}-\d{2}-\d{2}', s):
        return s[:10]
    for fmt in ('%d/%m/%Y', '%d-%m-%Y', '%d %b %Y', '%d/%m/%y'):
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None


# Priority-ordered prefix list for extracting issuer from Cboe fund names
_CBOE_NAME_PREFIXES = [
    ('australian ethical', 'Australian Ethical'),
    ('ab managed', 'AllianceBernstein'),
    ('avantis', 'Avantis'),
    ('coolabah', 'Coolabah'),
    ('elstree', 'Elstree'),
    ('global x', 'Global X'),
    ('iam ', 'IAM'),
    ('ishares', 'iShares'),
    ('india avenue', 'India Avenue'),
    ('janus henderson', 'Janus Henderson'),
    ('jpmorgan', 'JPMorgan'),
    ('kapstream', 'Kapstream'),
    ('lazard', 'Lazard'),
    ('magellan', 'Magellan'),
    ('monochrome', 'Monochrome'),
    ('paradice', 'Paradice'),
    ('pimco', 'PIMCO'),
    ('schroder', 'Schroders'),
    ('t8 ', 'T8'),
    ('talaria', 'Talaria'),
]


def _extract_issuer_from_cboe_name(name: str) -> str | None:
    """Infer issuer canonical name from a Cboe fund name using prefix matching."""
    if not name:
        return None
    lower = name.lower()
    for prefix, canonical in _CBOE_NAME_PREFIXES:
        if lower.startswith(prefix):
            return normalise_issuer(canonical)
    return None


def _scrape_cboe_api() -> list[dict]:
    """Try the Cboe Australia JSON API."""
    data = fetch_json(CBOE_API_URL)
    if not data:
        return []

    results = []
    items = data if isinstance(data, list) else data.get('data', data.get('results', []))
    for item in items:
        if not isinstance(item, dict):
            continue
        code = (item.get('ticker') or item.get('code') or item.get('symbol') or '').strip().upper()
        if not code or not re.match(r'^[A-Z0-9]{2,6}$', code):
            continue

        etf = {
            'code': code,
            'name': item.get('name') or item.get('fundName'),
            'issuer': normalise_issuer(item.get('issuer') or item.get('manager')),
            'asset_class': normalise_asset_class(item.get('assetClass') or item.get('category')),
            'exchange': 'CBOE',
            'current_price': _safe_float(item.get('lastPrice') or item.get('price')),
            'fund_size_aud_millions': _safe_float(item.get('marketCap') or item.get('fum')),
            'data_source': 'cboe_api',
        }
        etf = {k: v for k, v in etf.items() if v is not None}
        results.append(etf)

    return results


def _scrape_cboe_html() -> list[dict]:
    """Scrape the Cboe Australia products page via HTML."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.warning("beautifulsoup4 not installed, skipping Cboe HTML scrape")
        return []

    resp = fetch(CBOE_FUNDS_URL)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, 'html.parser')
    results = []

    # Look for tables with ETF data
    for table in soup.find_all('table'):
        headers = []
        for th in table.find_all('th'):
            headers.append(th.get_text(strip=True).lower())

        if not any('code' in h or 'ticker' in h or 'symbol' in h for h in headers):
            continue

        for tr in table.find_all('tr')[1:]:  # skip header
            cells = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
            if len(cells) < 2:
                continue

            code_idx = next((i for i, h in enumerate(headers) if 'code' in h or 'ticker' in h), 0)
            code = cells[code_idx].upper().strip() if code_idx < len(cells) else ''
            if not re.match(r'^[A-Z0-9]{2,6}$', code):
                continue

            name_idx = next((i for i, h in enumerate(headers) if 'name' in h or 'fund' in h), 1)
            issuer_idx = next((i for i, h in enumerate(headers) if 'issuer' in h or 'manager' in h), None)

            etf = {
                'code': code,
                'name': cells[name_idx] if name_idx < len(cells) else None,
                'issuer': normalise_issuer(cells[issuer_idx]) if issuer_idx and issuer_idx < len(cells) else None,
                'exchange': 'CBOE',
                'data_source': 'cboe_html',
            }
            etf = {k: v for k, v in etf.items() if v is not None}
            results.append(etf)

    return results


def _scrape_marketindex_fallback() -> list[dict]:
    """Fallback: scrape marketindex.com.au for Cboe-listed ETFs."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return []

    resp = fetch(MARKETINDEX_ETFS_URL)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, 'html.parser')
    results = []

    for table in soup.find_all('table'):
        for tr in table.find_all('tr')[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all('td')]
            if len(cells) < 3:
                continue

            code = cells[0].upper().strip()
            if not re.match(r'^[A-Z0-9]{2,6}$', code):
                continue

            # MarketIndex sometimes marks exchange
            exchange = 'ASX'
            for cell in cells:
                if 'cboe' in cell.lower() or 'chi-x' in cell.lower():
                    exchange = 'CBOE'
                    break

            if exchange != 'CBOE':
                continue

            etf = {
                'code': code,
                'name': cells[1] if len(cells) > 1 else None,
                'exchange': 'CBOE',
                'data_source': 'marketindex',
            }
            etf = {k: v for k, v in etf.items() if v is not None}
            results.append(etf)

    return results


def _discover_cboe_report_url() -> str | None:
    """
    Fetch the Cboe AU funds report index page and extract the latest xlsx URL
    from the embedded CTX.fundsReports JSON array.
    Falls back to guessing the URL from the current/previous months.
    """
    import json as _json
    resp = fetch('https://www-api.cboe.com/au/funds_reports/')
    if resp:
        # Page embeds: CTX.fundsReports = [{..., "xls": "https://cdn.cboe.com/...xlsx"}, ...]
        m = re.search(r'CTX\.fundsReports\s*=\s*(\[.*?\]);', resp.text, re.DOTALL)
        if m:
            try:
                reports = _json.loads(m.group(1))
                if reports and isinstance(reports, list):
                    # First entry is most recent
                    url = reports[0].get('xls') or reports[0].get('xlsx')
                    if url and url.endswith('.xlsx'):
                        logger.info(f"Discovered Cboe report URL: {url}")
                        return url
            except (_json.JSONDecodeError, (IndexError, KeyError)):
                pass
        # Fallback: find any cdn.cboe.com xlsx link in the page
        m = re.search(r'https://cdn\.cboe\.com/[^"\']+Monthly-Funds-Report-\d{4}-\d{2}\.xlsx', resp.text)
        if m:
            logger.info(f"Discovered Cboe report URL (regex): {m.group(0)}")
            return m.group(0)

    # Last resort: guess from current/prior months
    now = datetime.now()
    for months_back in range(4):
        year = now.year
        month = now.month - months_back
        while month <= 0:
            month += 12
            year -= 1
        url = CBOE_MONTHLY_REPORT_CDN_BASE.format(year=year, month=month)
        # Check the cached file first before attempting a HEAD-like download
        ts = f'{year}{month:02d}'
        dest = os.path.join(DATA_DIR, f'cboe_funds_report_{ts}.xlsx')
        if os.path.exists(dest) and os.path.getsize(dest) > 1000:
            return url
    return None


def _scrape_cboe_monthly_report() -> list[dict]:
    """
    Download and parse the Cboe Australia monthly funds report Excel file.
    Discovers the latest URL from the Cboe report index, with month-guessing fallback.
    Returns all funds (both ASX and CXA-exclusive).
    """
    try:
        import openpyxl
    except ImportError:
        logger.error("openpyxl required for Cboe monthly report")
        return []

    os.makedirs(DATA_DIR, exist_ok=True)

    # Discover the latest report URL
    latest_url = _discover_cboe_report_url()

    xlsx_path = None
    urls_to_try = []

    # Build ordered list: discovered URL first, then month fallbacks
    if latest_url:
        urls_to_try.append(latest_url)
    now = datetime.now()
    for months_back in range(4):
        year = now.year
        month = now.month - months_back
        while month <= 0:
            month += 12
            year -= 1
        url = CBOE_MONTHLY_REPORT_CDN_BASE.format(year=year, month=month)
        if url not in urls_to_try:
            urls_to_try.append(url)

    for url in urls_to_try:
        # Derive local filename from the YYYY-MM in the URL
        m = re.search(r'(\d{4})-(\d{2})\.xlsx', url)
        ts = f'{m.group(1)}{m.group(2)}' if m else 'unknown'
        dest = os.path.join(DATA_DIR, f'cboe_funds_report_{ts}.xlsx')

        if os.path.exists(dest) and os.path.getsize(dest) > 1000:
            age_hours = (datetime.now().timestamp() - os.path.getmtime(dest)) / 3600
            if age_hours < 12:
                xlsx_path = dest
                logger.info(f"Using cached Cboe report: {dest}")
                break

        logger.info(f"Downloading Cboe monthly report: {url}")
        if download_file(url, dest) and os.path.getsize(dest) > 1000:
            xlsx_path = dest
            break
        if os.path.exists(dest) and os.path.getsize(dest) <= 1000:
            os.remove(dest)

    if not xlsx_path:
        logger.error("Could not download Cboe monthly report")
        return []

    logger.info(f"Parsing Cboe monthly report: {xlsx_path}")
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active  # single sheet named by month (e.g. 'January')

    results = []
    current_asset_class = None
    header_row_idx = None
    col_map = {}

    for row_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
        row_vals = list(row)

        # Find header row by 'Ticker' in first cell
        if header_row_idx is None:
            first = str(row_vals[0] or '').strip().lower()
            if first in ('ticker', 'code', 'asx code'):
                header_row_idx = row_idx
                for i, h in enumerate(row_vals):
                    h_str = str(h or '').strip().lower()
                    if 'code' not in col_map and ('ticker' in h_str or h_str in ('code', 'asx code')):
                        col_map['code'] = i
                    elif 'name' not in col_map and 'name' in h_str:
                        col_map['name'] = i
                    elif 'exchange' not in col_map and 'exchange' in h_str:
                        col_map['exchange'] = i
                    elif 'mer' not in col_map and ('mer' in h_str or 'management fee' in h_str):
                        col_map['mer'] = i
                    elif 'aum' not in col_map and 'aum' in h_str:
                        col_map['aum'] = i
                    elif 'flow_1m' not in col_map and ('inflow' in h_str or 'outflow' in h_str):
                        col_map['flow_1m'] = i
                    elif 'inception_date' not in col_map and ('listing date' in h_str or 'inception date' in h_str):
                        col_map['inception_date'] = i
                    elif 'spread' not in col_map and 'spread' in h_str:
                        col_map['spread'] = i
                    elif 'price' not in col_map and re.match(r'^last(\s+price)?$', h_str):
                        col_map['price'] = i
                    elif 'year_high' not in col_map and ('year high' in h_str or re.search(r'52.+high', h_str)):
                        col_map['year_high'] = i
                    elif 'year_low' not in col_map and ('year low' in h_str or re.search(r'52.+low', h_str)):
                        col_map['year_low'] = i
                    elif 'return_1m' not in col_map and re.search(r'1\s*m\s+total\s+return', h_str):
                        col_map['return_1m'] = i
                    elif 'return_1y' not in col_map and re.search(r'1\s*y\s+total\s+return', h_str):
                        col_map['return_1y'] = i
                    elif 'return_3y' not in col_map and re.search(r'3\s*y\s+total\s+return', h_str):
                        col_map['return_3y'] = i
                    elif 'return_5y' not in col_map and re.search(r'5\s*y\s+total\s+return', h_str):
                        col_map['return_5y'] = i
            continue

        if not col_map:
            continue

        code_idx = col_map.get('code', 0)
        first_val = row_vals[code_idx] if code_idx < len(row_vals) else None

        # Section header rows: no ticker but col 1 has category text
        if first_val is None:
            second_val = str(row_vals[1] or '').strip() if len(row_vals) > 1 else ''
            if second_val:
                mapped = normalise_asset_class(second_val)
                if mapped != second_val:  # successful mapping
                    current_asset_class = mapped
            continue

        code = str(first_val).strip().upper()
        if not re.match(r'^[A-Z][A-Z0-9]{1,5}$', code) or code.isdigit():
            continue

        def get_col(key):
            idx = col_map.get(key)
            if idx is None or idx >= len(row_vals):
                return None
            return row_vals[idx]

        exchange_raw = str(get_col('exchange') or '').strip().upper()
        exchange = 'CXA' if 'CXA' in exchange_raw else 'ASX'

        name_val = str(get_col('name') or '').strip() or None
        issuer = _extract_issuer_from_cboe_name(name_val)
        issuer_url = CXA_ISSUER_URLS.get(issuer) if issuer else None

        etf = {
            'code': code,
            'name': name_val,
            'exchange': exchange,
            'asset_class': current_asset_class,
            'management_fee': _safe_float(get_col('mer')),
            'fund_size_aud_millions': _safe_float(get_col('aum')),
            'fund_flow_1m': _safe_float(get_col('flow_1m')),
            'inception_date': _format_date(get_col('inception_date')),
            'bid_ask_spread_pct': _safe_float(get_col('spread')),
            'current_price': _safe_float(get_col('price')),
            'year_high': _safe_float(get_col('year_high')),
            'year_low': _safe_float(get_col('year_low')),
            'return_1m': _from_decimal_pct(_safe_float(get_col('return_1m'))),
            'return_1y': _from_decimal_pct(_safe_float(get_col('return_1y'))),
            'return_3y': _from_decimal_pct(_safe_float(get_col('return_3y'))),
            'return_5y': _from_decimal_pct(_safe_float(get_col('return_5y'))),
            'issuer': issuer,
            'issuer_url': issuer_url,
            'data_source': 'cboe_report',
        }
        etf = {k: v for k, v in etf.items() if v is not None}
        results.append(etf)

    wb.close()

    cxa_count = sum(1 for e in results if e.get('exchange') == 'CXA')
    logger.info(f"Cboe monthly report: found {len(results)} ETFs ({cxa_count} CXA-exclusive)")
    return results


def scrape_cboe(db_path=None) -> int:
    """
    Main entry point: try Cboe API, then HTML, then marketindex fallback.
    Returns count of ETFs upserted.
    """
    started = datetime.utcnow()
    source = 'cboe'

    # Primary: official Cboe monthly Excel report (comprehensive, includes CXA-exclusive ETFs)
    etfs = _scrape_cboe_monthly_report()
    if not etfs:
        logger.info("Cboe monthly report returned no results, trying API...")
        etfs = _scrape_cboe_api()
    if not etfs:
        logger.info("Cboe API returned no results, trying HTML scrape...")
        etfs = _scrape_cboe_html()
    if not etfs:
        logger.info("Cboe HTML returned no results, trying marketindex fallback...")
        etfs = _scrape_marketindex_fallback()

    # Deduplicate
    seen = set()
    unique = []
    for e in etfs:
        if e['code'] not in seen:
            seen.add(e['code'])
            unique.append(e)

    logger.info(f"Cboe scraper: found {len(unique)} ETFs")

    conn = get_connection(db_path)
    if unique:
        upsert_etfs(conn, unique)
        update_issuer_stats(conn)

    duration = (datetime.utcnow() - started).total_seconds()
    status = 'success' if unique else 'no_data'
    log_scrape(conn, source, status, records_affected=len(unique),
               duration_secs=duration, started_at=started.isoformat())
    conn.close()

    return len(unique)
