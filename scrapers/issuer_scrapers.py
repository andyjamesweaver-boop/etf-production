"""
Per-issuer website scrapers.

Each function scrapes an issuer's website for ETF data that isn't available
from ASX/Cboe sources: holdings, sector allocations, returns, fees, etc.
"""

import re
import io
import json
import html as html_module
import logging
from datetime import datetime, date

from scrapers.config import ISSUER_URLS, ISHARES_AU_PRODUCTS, SPDR_AU_FUNDS, VANGUARD_AU_FUNDS, VANGUARD_AU_PORT_IDS, normalise_asset_class, CXA_ISSUER_URLS
from scrapers.base_scraper import fetch, fetch_json
from scrapers.db_writer import (
    get_connection, upsert_etf, upsert_holdings, upsert_sectors,
    upsert_units_history, log_scrape, update_issuer_stats,
)

logger = logging.getLogger(__name__)


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        s = str(val).replace(',', '').replace('$', '').replace('%', '').strip()
        if s in ('', '-', 'N/A', 'n/a'):
            return None
        return float(s)
    except (ValueError, TypeError):
        return None


def _parse_fum_str(s) -> float | None:
    """Parse abbreviated FUM strings like '$7.76B', '$123.4M', '$1.5K' into raw float."""
    if not s:
        return None
    s = str(s).strip().replace(',', '').replace('$', '').replace(' ', '')
    mult = 1
    if s and s[-1] in ('B', 'b'):
        mult, s = 1_000_000_000, s[:-1]
    elif s and s[-1] in ('M', 'm'):
        mult, s = 1_000_000, s[:-1]
    elif s and s[-1] in ('K', 'k'):
        mult, s = 1_000, s[:-1]
    val = _safe_float(s)
    return val * mult if val is not None else None


def _find_json_in_script(soup, key_hints: list[str]) -> dict | list | None:
    """Search <script> tags for inline JSON containing any of the given key strings."""
    for tag in soup.find_all('script'):
        text = tag.string or ''
        if not any(h in text for h in key_hints):
            continue
        # Try to extract the largest JSON object/array in the block
        for match in re.finditer(r'(\{[\s\S]{20,}\}|\[[\s\S]{20,}\])', text):
            try:
                return json.loads(match.group(0))
            except (ValueError, json.JSONDecodeError):
                continue
    return None


# ====================================================================
# BetaShares
# ====================================================================

def _parse_betashares_csv(csv_text: str) -> tuple[list[dict], list[dict]]:
    """
    Parse a BetaShares Portfolio Holdings CSV.

    Format:
      Rows 1-6: metadata (Fund Name, ASX Code, Date)
      Row 7: column headers — Ticker, Name, Asset Class, Sector, Country,
                               Currency, Weight (%), Shares/Units (#), ...
      Remaining: data rows, then empty/disclaimer footer rows

    Returns (holdings, sectors) where sectors are aggregated from holding rows.
    """
    import csv as csv_module

    reader = csv_module.reader(io.StringIO(csv_text))
    rows = list(reader)

    # Find the header row — first row where col 0 is 'Ticker'
    header_idx = None
    for i, row in enumerate(rows):
        if row and row[0].strip() == 'Ticker':
            header_idx = i
            break
    if header_idx is None:
        return [], []

    headers = [h.strip() for h in rows[header_idx]]
    try:
        idx_ticker = headers.index('Ticker')
        idx_name   = headers.index('Name')
        idx_weight = headers.index('Weight (%)')
    except ValueError:
        return [], []
    # Sector and Country are absent in fund-of-funds CSVs (e.g. DHHF, DZZF)
    idx_sector  = headers.index('Sector')  if 'Sector'  in headers else None
    idx_country = headers.index('Country') if 'Country' in headers else None

    holdings: list[dict] = []
    sector_totals: dict[str, float] = {}

    for row in rows[header_idx + 1:]:
        if len(row) <= idx_weight:
            continue
        weight = _safe_float(row[idx_weight])
        if weight is None or weight <= 0:
            continue
        name_raw = row[idx_name].strip()
        if not name_raw:
            continue
        name = name_raw.title()

        # Strip exchange suffix from ticker  e.g. 'CBA AT' → 'CBA', 'NVDA UW' → 'NVDA'
        ticker_raw = row[idx_ticker].strip()
        if ticker_raw:
            parts = ticker_raw.rsplit(' ', 1)
            ticker = parts[0] if len(parts) == 2 and len(parts[1]) <= 3 else ticker_raw
        else:
            ticker = None

        sector  = (row[idx_sector].strip()  or None) if idx_sector  is not None and len(row) > idx_sector  else None
        country = (row[idx_country].strip() or None) if idx_country is not None and len(row) > idx_country else None

        holdings.append({
            'name':       name,
            'ticker':     ticker,
            'weight_pct': weight,
            'sector':     sector,
            'country':    country,
        })
        if sector:
            sector_totals[sector] = sector_totals.get(sector, 0.0) + weight

    sectors = [
        {'sector': s, 'weight_pct': round(w, 6)}
        for s, w in sorted(sector_totals.items(), key=lambda x: -x[1])
    ]
    return holdings, sectors


def _parse_betashares_csv_names_only(csv_text: str) -> list[dict]:
    """
    Fallback: parse BetaShares CSV where all weights are empty (e.g. HBRD active ETF).
    Returns holdings with weight_pct=None.
    """
    import csv as _csv
    rows = list(_csv.reader(csv_text.splitlines()))
    header_idx = None
    for i, row in enumerate(rows):
        if row and row[0].strip() == 'Ticker':
            header_idx = i
            break
    if header_idx is None:
        return []
    headers = [h.strip() for h in rows[header_idx]]
    try:
        idx_name = headers.index('Name')
    except ValueError:
        return []
    holdings = []
    for row in rows[header_idx + 1:]:
        if len(row) <= idx_name:
            continue
        name = row[idx_name].strip()
        if not name or name.startswith('"') or name.lower() in ('name', 'total'):
            continue
        holdings.append({'name': name.title(), 'ticker': None,
                         'weight_pct': None, 'sector': None, 'country': None})
    return holdings


def _scrape_betashares_holdings_csv(conn, code: str) -> int:
    """
    Download and parse the BetaShares Portfolio Holdings CSV for a given ETF code.
    Replaces all holdings and sectors for the ETF in the database.
    Returns the number of holdings inserted (0 if CSV not found or empty).
    """
    url = f"https://www.betashares.com.au/files/csv/{code}_Portfolio_Holdings.csv"
    resp = fetch(url)
    if not resp or resp.status_code != 200:
        return 0

    holdings, sectors = _parse_betashares_csv(resp.text)
    if not holdings:
        # For active ETFs (e.g. HBRD), BetaShares publishes names but no weights.
        # Fall back to parsing names-only from the CSV.
        holdings = _parse_betashares_csv_names_only(resp.text)
        if not holdings:
            return 0

    # Merge duplicate names (e.g. GOOGL + GOOG both appear as "Alphabet Inc")
    merged: dict[str, dict] = {}
    for h in holdings:
        n = h['name']
        if n in merged:
            merged[n]['weight_pct'] = (merged[n]['weight_pct'] or 0) + (h['weight_pct'] or 0)
        else:
            merged[n] = h
    holdings = list(merged.values())

    upsert_holdings(conn, code, holdings, commit=False)
    if sectors:
        upsert_sectors(conn, code, sectors, commit=False)

    return len(holdings)


def _scrape_betashares_fund_page(conn, slug: str, soup, raw_html: str = '') -> str | None:
    """
    Parse a BetaShares individual fund page.
    Returns the ASX code, or None if not found.
    Title format: "NDQ ASX | Nasdaq 100 ETF | Betashares"
    Holdings table rows: <th>COMPANY NAME</th><td>weight</td>
    """
    # ASX code from <title> — handles multiple formats:
    #   "NDQ ASX | Nasdaq 100 ETF | Betashares"  (old)
    #   "ASX A200 | Australia 200 ETF | Betashares"  (new)
    #   "AGVT ETF | Australian Government Bond ETF | Betashares"  (new alt)
    title = soup.title.string if soup.title else ''
    code_match = (
        re.match(r'^([A-Z0-9]{2,6})\s+ASX\s*\|', title) or   # old: CODE ASX |
        re.match(r'^ASX\s+([A-Z0-9]{2,6})\s*[\|\s]', title) or  # new: ASX CODE |
        re.match(r'^([A-Z0-9]{2,6})\s+ETF\s*\|', title)          # new alt: CODE ETF |
    )
    if not code_match:
        return None
    code = code_match.group(1)

    # Fund name from title
    name_match = re.search(r'(?:ASX\s+)?[A-Z0-9]+\s+(?:ASX\s+)?\|\s+(.+?)\s+\|\s+', title)
    name = name_match.group(1) if name_match else None

    # MER from the "Management fee and cost" table row
    mer = None
    for table in soup.find_all('table'):
        headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
        if 'management fee and cost** (p.a.)' in headers or 'management fee and costs (p.a.)' in headers:
            # MER is in the first data td in this table
            for row in table.find_all('tr')[1:]:
                tds = [td.get_text(strip=True) for td in row.find_all('td')]
                if tds:
                    val = _safe_float(tds[0])
                    if val and 0 < val < 5:
                        mer = val
                    break
            break

    # Benchmark from the "Index" table row
    benchmark = None
    for table in soup.find_all('table'):
        for row in table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True).lower()
                if label in ('index', 'underlying index', 'benchmark index', 'benchmark'):
                    val = cells[1].get_text(strip=True)
                    if val and len(val) > 3:
                        benchmark = val
                        break
        if benchmark:
            break

    # Holdings — rows are <tr><th>COMPANY NAME</th><td>weight%</td></tr>
    holdings = []
    for table in soup.find_all('table'):
        first_row_headers = [th.get_text(strip=True).lower() for th in table.find_all('tr')[0].find_all('th')] if table.find_all('tr') else []
        if 'name' in first_row_headers and 'weight (%)' in first_row_headers:
            for row in table.find_all('tr')[1:]:
                th_els = row.find_all('th')
                td_els = row.find_all('td')
                if th_els and td_els:
                    h_name = th_els[0].get_text(strip=True).title()
                    weight = _safe_float(td_els[0].get_text(strip=True))
                    if h_name and weight:
                        holdings.append({'name': h_name, 'weight_pct': weight})
            break

    # ── Returns, NAV, FUM, units on issue, distribution yield ──────────────────
    returns: dict = {}
    nav_per_unit = None
    fum_aud = None
    units_on_issue = None
    distribution_yield = None

    # Returns from the fund-performance table
    # Rows look like: <tr><th>1 month</th><td>-3.96%</td><td>-3.94%</td></tr>
    # Label is in <th>, fund return is first <td>, index return is second <td>
    _RETURN_LABEL_MAP = {
        '1 month':      'return_1m',
        '3 months':     'return_3m',
        '6 months':     'return_6m',
        '1 year':       'return_1y',
        '3 year':       'return_3y',
        '3 years':      'return_3y',
        '5 year':       'return_5y',
        '5 years':      'return_5y',
    }
    for row in soup.find_all('tr'):
        th_els = row.find_all('th')
        td_els = row.find_all('td')
        if not th_els or not td_els:
            continue
        label = th_els[0].get_text(strip=True).lower()
        val = _safe_float(td_els[0].get_text(strip=True))
        if val is None:
            continue
        # strip trailing p.a. / (p.a.) for matching
        base_label = re.sub(r'\s*\(?\s*p\.?a\.?\s*\)?$', '', label).strip()
        field = _RETURN_LABEL_MAP.get(base_label)
        if field:
            returns[field] = val
        elif 'since inception' in base_label:
            returns['return_since_inception'] = val

    # Distribution yield from th/td table row: <th>12 mth distribution yield*</th><td>0.9%</td>
    for th in soup.find_all('th'):
        if '12 mth distribution yield' in th.get_text(strip=True).lower():
            td = th.find_next_sibling('td')
            if td:
                yld = _safe_float(td.get_text(strip=True))
                if yld and 0 < yld < 50:
                    distribution_yield = yld
            break

    if raw_html:
        # NAV per unit: "NAV/unit </span>...<div class="v2-fund-value ...">$51.45</div>"
        nav_m = re.search(
            r'NAV/unit\s*</span>[\s\S]{0,400}?\$([0-9,\.]+)',
            raw_html
        )
        if nav_m:
            nav = _safe_float(nav_m.group(1))
            if nav and nav > 0:
                nav_per_unit = nav

        # Net assets (FUM): look for "$7,185,630,356" following "Net assets" label
        na_m = re.search(
            r"Net assets\*?</div>.*?<div[^>]*>\$?([\d,]+)</div>",
            raw_html, re.DOTALL | re.IGNORECASE
        )
        if na_m:
            fum_raw = _safe_float(na_m.group(1).replace(',', ''))
            if fum_raw and fum_raw > 1_000:  # sanity: > $1000 AUD
                fum_aud = fum_raw

        # Units on issue: Units outstanding* (#)</th>\n<td>139,671,576</td>
        units_m = re.search(
            r'Units outstanding\*?\s*\(#\)</th>\s*<td>([\d,]+)</td>',
            raw_html, re.IGNORECASE
        )
        if units_m:
            units_str = units_m.group(1).replace(',', '')
            units_on_issue = int(units_str) if units_str else None

    etf = {
        'code': code,
        'name': name,
        'issuer': 'BetaShares',
        'expense_ratio': mer,
        'management_fee': mer,
        'benchmark': benchmark,
        'data_source': 'betashares',
        'issuer_url': f"https://www.betashares.com.au/fund/{slug}/",
        'nav_per_unit': nav_per_unit,
        'fund_size_aud_millions': round(fum_aud / 1_000_000, 1) if fum_aud else None,
        'net_assets_aud': fum_aud,
        'net_assets_date': date.today().isoformat() if fum_aud else None,
        'units_on_issue': units_on_issue,
        'units_on_issue_date': date.today().isoformat() if units_on_issue else None,
        'distribution_yield': distribution_yield,
        **returns,
    }
    etf = {k: v for k, v in etf.items() if v is not None}
    upsert_etf(conn, etf)
    upsert_units_history(conn, code, etf.get('units_on_issue'), etf.get('net_assets_aud'),
                         date.today().isoformat(), commit=False)

    # Download the full portfolio holdings CSV (includes ticker, sector, country, all positions)
    csv_count = _scrape_betashares_holdings_csv(conn, code)
    if not csv_count and holdings:
        # Fall back to HTML table holdings if CSV unavailable (top ~10 only, no sector/country)
        merged: dict[str, dict] = {}
        for h in holdings:
            n = h['name']
            if n in merged:
                merged[n]['weight_pct'] = (merged[n]['weight_pct'] or 0) + (h['weight_pct'] or 0)
            else:
                merged[n] = h
        upsert_holdings(conn, code, list(merged.values()), commit=False)

    return code


def scrape_betashares(db_path=None) -> int:
    """Scrape BetaShares fund list (/fund/) and individual fund pages for MER + holdings."""
    started = datetime.utcnow()
    source = 'betashares'
    updated = 0

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.error("beautifulsoup4 required for issuer scrapers")
        return 0

    conn = get_connection(db_path)
    try:
        fund_list_url = ISSUER_URLS['BetaShares']['fund_list']

        # Step 1: Get all fund slugs from the listing page
        resp = fetch(fund_list_url)
        if not resp:
            log_scrape(conn, source, 'error', error='Failed to fetch fund list',
                       duration_secs=(datetime.utcnow() - started).total_seconds(),
                       started_at=started.isoformat())
            return 0

        soup = BeautifulSoup(resp.text, 'html.parser')
        slugs: list[str] = []
        seen_slugs: set[str] = set()
        for a in soup.find_all('a', href=True):
            href = a['href']
            m = re.match(r'https://www\.betashares\.com\.au/fund/([^/?#]+)/?$', href)
            if m:
                slug = m.group(1)
                if slug not in seen_slugs:
                    seen_slugs.add(slug)
                    slugs.append(slug)

        logger.info(f"BetaShares: found {len(slugs)} fund slugs")

        # Step 2: Scrape each individual fund page
        for slug in slugs:
            fund_url = f"https://www.betashares.com.au/fund/{slug}/"
            resp = fetch(fund_url)
            if not resp:
                continue
            page_soup = BeautifulSoup(resp.text, 'html.parser')
            try:
                code = _scrape_betashares_fund_page(conn, slug, page_soup, resp.text)
            except Exception as e:
                logger.warning(f"BetaShares: error scraping {slug}: {e}")
                conn.rollback()
                continue
            if code:
                updated += 1

        conn.commit()
        duration = (datetime.utcnow() - started).total_seconds()
        log_scrape(conn, source, 'success' if updated else 'no_data',
                   records_affected=updated, duration_secs=duration,
                   started_at=started.isoformat())
        logger.info(f"BetaShares: updated {updated} ETFs")
        return updated
    finally:
        conn.close()


# ====================================================================
# VanEck
# ====================================================================

def _scrape_vaneck_snapshot(conn, code: str, snapshot_url: str) -> None:
    """
    Scrape a VanEck fund snapshot page for MER, inception date, and holdings.
    Primary path: extract the embedded FundDatasetBlock/Get/ JSON API URL from the
    page HTML and call it directly (returns structured JSON with holdings + metadata).
    Fallback: BeautifulSoup table parsing for any missing fields.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return

    resp = fetch(snapshot_url)
    if not resp:
        return

    update = {'code': code}
    holdings = []

    # --- Primary: FundDatasetBlock JSON API ---
    # VanEck embeds a URL like:
    #   FundDatasetBlock/Get/?blockId=352386&pageId=248665&ticker=QUALAU
    # in the page HTML which returns full fund metadata + holdings as JSON.
    api_match = re.search(
        r'FundDatasetBlock/Get/\?blockId=(\d+)&pageId=(\d+)&ticker=(\w+)',
        resp.text
    )
    if api_match:
        block_id, page_id, ticker = api_match.groups()
        api_url = (
            f'https://www.vaneck.com.au/Main/FundDatasetBlock/Get/'
            f'?blockId={block_id}&pageId={page_id}&ticker={ticker}'
        )
        api_resp = fetch(api_url)
        if api_resp:
            try:
                data = api_resp.json()
                # Management fee — stored as "0.40%" string
                fee_str = data.get('Management fee (p.a.)', '') or ''
                fee_val = _safe_float(re.sub(r'[^0-9.]', '', fee_str))
                if fee_val and 0 < fee_val < 5:
                    update['expense_ratio'] = fee_val
                # Inception date
                inc = (data.get('Inception Date') or '').strip()
                if inc:
                    update['inception_date'] = inc
                # NAV per unit
                nav_val = _safe_float(data.get('NAV'))
                if nav_val and nav_val > 0:
                    update['nav_per_unit'] = nav_val
                # FUM (Total Net Assets) — may be abbreviated e.g. "$7.76B"
                fum = _parse_fum_str(data.get('Total Net Assets'))
                if fum and fum > 1_000:
                    update['net_assets_aud'] = fum
                    update['net_assets_date'] = date.today().isoformat()
                    update['fund_size_aud_millions'] = round(fum / 1_000_000, 1)
                    if nav_val and nav_val > 0:
                        update['units_on_issue'] = int(round(fum / nav_val))
                        update['units_on_issue_date'] = date.today().isoformat()
                # Holdings — HoldingsList is a list of date-bucketed snapshots;
                # use the first (most recent) entry.
                for entry in data.get('HoldingsList', []):
                    for h in entry.get('Holdings', []):
                        name = (h.get('HoldingName') or '').strip()
                        label = (h.get('Label') or '').strip()  # e.g. "META US"
                        weight = _safe_float(h.get('Weight'))
                        if name and weight is not None and 0 < weight < 100:
                            holdings.append({
                                'name': name,
                                'ticker': label,
                                'weight_pct': weight,
                            })
                    if holdings:
                        break  # only need the most recent snapshot
            except (ValueError, KeyError, AttributeError):
                pass

    # --- Fallback: BeautifulSoup table parsing ---
    soup = BeautifulSoup(resp.text, 'html.parser')

    if 'expense_ratio' not in update:
        for label in soup.find_all(string=re.compile(r'Management\s+(Fee|Cost|Expense|MER)', re.I)):
            parent = label.parent
            for candidate in [parent.find_next_sibling(), parent.find_next('td')]:
                if candidate:
                    text = re.sub(r'[^0-9.]', '', candidate.get_text())
                    val = _safe_float(text)
                    if val and 0 < val < 5:
                        update['expense_ratio'] = val
                        break
            if 'expense_ratio' in update:
                break

    if 'inception_date' not in update:
        for label in soup.find_all(string=re.compile(r'Inception\s+Date', re.I)):
            parent = label.parent
            for candidate in [parent.find_next_sibling(), parent.find_next('td')]:
                if candidate:
                    text = candidate.get_text(strip=True)
                    if re.match(r'\d{1,2}[/\-]\w+[/\-]\d{2,4}', text) or re.match(r'\d{4}-\d{2}-\d{2}', text):
                        update['inception_date'] = text
                        break
            if 'inception_date' in update:
                break

    # Benchmark — two patterns:
    # 1. Structured: <h4>Underlying Index:</h4><p>{name}</p>
    # 2. Prose: "the benchmark, the {Index Name}"
    if 'benchmark' not in update:
        for el in soup.find_all(string=re.compile(r'Underlying\s+Index', re.I)):
            parent = el.parent
            for sib in parent.next_siblings:
                sib_text = sib.get_text(strip=True) if hasattr(sib, 'get_text') else ''
                if sib_text and len(sib_text) > 3:
                    update['benchmark'] = sib_text
                    break
            if 'benchmark' in update:
                break
    if 'benchmark' not in update:
        bench_m = re.search(
            r'the benchmark,\s+the\s+([A-Z][^\n<.]{5,100}Index)',
            resp.text,
        )
        if bench_m:
            bench_val = bench_m.group(1).strip()
            if len(bench_val) > 5 and 'Index' in bench_val:
                update['benchmark'] = bench_val

    # Returns table fallback
    for table in soup.find_all('table'):
        headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
        has_returns = any(
            any(h in hdr for h in ['1 yr', '1yr', '1 year', '3 yr', '3yr', 'ytd'])
            for hdr in headers
        )
        if not has_returns:
            continue
        for row in table.find_all('tr')[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all('td')]
            if not cells:
                continue
            if any(kw in cells[0].lower() for kw in ('fund', 'nav', 'etf')):
                col_map = {}
                for i, h in enumerate(headers):
                    if 'month' in h or '1m' in h:
                        col_map[i] = 'return_1m'
                    elif '1' in h and ('yr' in h or 'year' in h):
                        col_map[i] = 'return_1y'
                    elif '3' in h and ('yr' in h or 'year' in h):
                        col_map[i] = 'return_3y'
                    elif '5' in h and ('yr' in h or 'year' in h):
                        col_map[i] = 'return_5y'
                for i, field in col_map.items():
                    if i < len(cells):
                        update[field] = _safe_float(cells[i])
                break

    if not holdings:
        for table in soup.find_all('table'):
            headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
            if not (any(h in ' '.join(headers) for h in ['holding', 'security', 'name']) and
                    any(h in ' '.join(headers) for h in ['weight', '%', 'allocation'])):
                continue
            for row in table.find_all('tr')[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all('td')]
                if len(cells) < 2:
                    continue
                name = cells[0] if cells[0] and cells[0] != '-' else None
                weight = None
                for cell in reversed(cells):
                    w = _safe_float(cell)
                    if w and 0 < w < 100:
                        weight = w
                        break
                if name and weight:
                    holdings.append({'name': name, 'weight_pct': weight})
            if holdings:
                break

    if len(update) > 1:
        upsert_etf(conn, update)
        upsert_units_history(conn, code, update.get('units_on_issue'), update.get('net_assets_aud'),
                             date.today().isoformat(), commit=False)
    if holdings:
        # Deduplicate by name (merge weights for same-name duplicates e.g. Alphabet Inc)
        merged: dict[str, dict] = {}
        for h in holdings:
            name = h['name']
            if name in merged:
                merged[name]['weight_pct'] = (merged[name]['weight_pct'] or 0) + (h.get('weight_pct') or 0)
            else:
                merged[name] = dict(h)
        upsert_holdings(conn, code, list(merged.values()), commit=False)


def scrape_vaneck(db_path=None) -> int:
    """Scrape VanEck Australia ETF list and individual snapshot pages."""
    started = datetime.utcnow()
    source = 'vaneck'
    updated = 0

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return 0

    conn = get_connection(db_path)

    # Fund list — VanEck /etf/ lists all ETFs via snapshot links
    resp = fetch('https://www.vaneck.com.au/etf/')
    if not resp:
        log_scrape(conn, source, 'error', error='Failed to fetch fund list',
                   duration_secs=(datetime.utcnow() - started).total_seconds(),
                   started_at=started.isoformat())
        conn.close()
        return 0

    soup = BeautifulSoup(resp.text, 'html.parser')

    # Extract ETFs from links like /etf/equity/qual/snapshot
    seen: dict[str, str] = {}  # code -> snapshot URL
    for link in soup.find_all('a', href=True):
        href = link['href']
        match = re.match(r'^/etf/[^/]+/([a-z0-9]{2,6})/snapshot/?$', href, re.I)
        if not match:
            continue

        code = match.group(1).upper()
        if code in seen:
            continue

        text = link.get_text(strip=True)
        name = None
        if text.startswith(code):
            name = text[len(code):].strip()
        elif text:
            name = text

        category_segment = href.split('/')[2] if len(href.split('/')) > 2 else None
        asset_class = None
        if category_segment:
            cat_map = {
                'equity': 'International Equities',
                'fixed-income': 'Fixed Income',
                'income': 'Fixed Income',
                'alternatives': 'Alternatives',
                'multi-asset': 'Diversified',
                'australian-equities': 'Australian Equities',
                'australian': 'Australian Equities',
            }
            asset_class = cat_map.get(category_segment)

        snapshot_url = f"https://www.vaneck.com.au{href}"
        seen[code] = snapshot_url

        etf = {
            'code': code,
            'name': name,
            'issuer': 'VanEck',
            'asset_class': asset_class,
            'data_source': 'vaneck',
            'issuer_url': snapshot_url,
        }
        etf = {k: v for k, v in etf.items() if v is not None}
        upsert_etf(conn, etf)
        updated += 1

    conn.commit()

    # Scrape individual snapshot pages for MER, returns, holdings
    if seen:
        # Prioritise ETFs already in DB with FUM ranking, then add any remaining
        ranked = conn.execute(
            "SELECT code FROM etfs WHERE issuer = 'VanEck' AND code IN ({}) ORDER BY rank_by_fum".format(
                ','.join('?' * len(seen))
            ),
            list(seen.keys())
        ).fetchall()
        priority_codes = [r[0] for r in ranked]
        # Add any remaining codes not yet ranked
        remaining = [c for c in seen if c not in priority_codes]
        codes_to_scrape = priority_codes + remaining

        detail_updated = 0
        for code in codes_to_scrape:
            try:
                _scrape_vaneck_snapshot(conn, code, seen[code])
                detail_updated += 1
            except Exception as e:
                logger.warning(f"VanEck: error scraping {code}: {e}")
                conn.rollback()

        conn.commit()
        logger.info(f"VanEck: scraped detail pages for {detail_updated} ETFs")

    duration = (datetime.utcnow() - started).total_seconds()
    try:
        log_scrape(conn, source, 'success' if updated else 'no_data',
                   records_affected=updated, duration_secs=duration,
                   started_at=started.isoformat())
    finally:
        conn.close()
    logger.info(f"VanEck: updated {updated} ETFs")
    return updated


# ====================================================================
# Vanguard
# ====================================================================

_VANGUARD_API_BASE = 'https://www.vanguard.com.au/personal/api/data/products'
_VANGUARD_OVERVIEW_BASE = 'https://www.vanguard.com.au/personal/api/products/personal/fund'
_VANGUARD_API_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'application/json',
}
# Max holdings rows to store per ETF (caps very large bond funds like VBND with 14k rows)
_VANGUARD_MAX_HOLDINGS = 500
# Fallback benchmarks for US-domiciled funds where benchMarkNameFromECS is absent
_VANGUARD_BENCHMARK_FALLBACK = {
    '0991': 'FTSE All-World ex-US Index',       # VEU
    '0970': 'CRSP US Total Market Index',        # VTS
}


def scrape_vanguard(db_path=None) -> int:
    """
    Scrape Vanguard Australia ETF holdings and sector data via the public REST API.

    API base: https://www.vanguard.com.au/personal/api/data/products/{endpoint}/{portId}
    Key endpoints used:
      - /fund-profile-type/{portId}  → fund name confirmation
      - /holdings-download/{portId}  → full holdings list with weight, sector, country

    Sectors are aggregated from holdings (sectorName field).
    portId map is maintained in config.VANGUARD_AU_PORT_IDS.
    """
    started = datetime.utcnow()
    source = 'vanguard'
    updated = 0
    holdings_count = 0

    conn = get_connection(db_path)

    # Deduplicate portIds (e.g. VLUE and VVLU share portId 8202)
    seen_port_ids: set[str] = set()
    port_id_benchmarks: dict[str, str] = {}
    port_id_primary: dict[str, str] = {}   # portId → first code processed
    # Order: process codes in VANGUARD_AU_PORT_IDS order
    for code, port_id in VANGUARD_AU_PORT_IDS.items():
        issuer_url = f'https://www.vanguard.com.au/personal/invest-with-us/etf?portId={port_id}'

        # Always upsert basic metadata for every code
        etf_base = {
            'code':       code,
            'issuer':     'Vanguard',
            'exchange':   'ASX',
            'data_source': 'vanguard',
            'issuer_url': issuer_url,
        }

        # If this portId has already been fetched for another code (share-class pair),
        # copy the benchmark and holdings from the primary code and skip the API fetch
        if port_id in seen_port_ids:
            if port_id in port_id_benchmarks:
                etf_base['benchmark'] = port_id_benchmarks[port_id]
            upsert_etf(conn, etf_base)
            updated += 1
            # Mirror holdings and sectors from the primary code to this share class
            primary = port_id_primary.get(port_id)
            if primary and primary != code:
                conn.execute('DELETE FROM etf_holdings WHERE etf_code=?', (code,))
                conn.execute(
                    'INSERT INTO etf_holdings (etf_code, name, ticker, weight_pct, sector, country) '
                    'SELECT ?, name, ticker, weight_pct, sector, country FROM etf_holdings WHERE etf_code=?',
                    (code, primary)
                )
                conn.execute('DELETE FROM etf_sectors WHERE etf_code=?', (code,))
                conn.execute(
                    'INSERT INTO etf_sectors (etf_code, sector, weight_pct) '
                    'SELECT ?, sector, weight_pct FROM etf_sectors WHERE etf_code=?',
                    (code, primary)
                )
                conn.commit()
            continue
        seen_port_ids.add(port_id)
        port_id_primary[port_id] = code

        try:
            # --- Fund profile: confirmed name ---
            profile_url = f'{_VANGUARD_API_BASE}/fund-profile-type/{port_id}'
            profile_resp = fetch_json(profile_url, headers=_VANGUARD_API_HEADERS)
            if profile_resp:
                profiles = profile_resp.get('data', [])
                if profiles and isinstance(profiles, list):
                    api_name = profiles[0].get('fundName') or profiles[0].get('longName')
                    if api_name:
                        etf_base['name'] = api_name

            # --- Overview: benchmark, management fee, FUM, distribution frequency ---
            overview_url = f'{_VANGUARD_OVERVIEW_BASE}/{port_id}/overview'
            overview_resp = fetch_json(overview_url, headers=_VANGUARD_API_HEADERS)
            if overview_resp:
                overview_data = overview_resp.get('data', [])
                if overview_data and isinstance(overview_data, list):
                    ov = overview_data[0]
                    bm_raw = (ov.get('benchMarkNameFromECS')
                              or ov.get('benchmarkName')
                              or '')
                    if bm_raw:
                        etf_base['benchmark'] = html_module.unescape(bm_raw.strip())
                    # Management fee / expense ratio from fundFees → expenseType ADJEXPRTPC
                    for fee_group in (ov.get('fundFees') or []):
                        for exp in (fee_group.get('expenseType') or []):
                            if exp.get('eipcode') == 'ADJEXPRTPC':
                                val = _safe_float(exp.get('value'))
                                if val is not None:
                                    etf_base['management_fee'] = val
                                    etf_base['expense_ratio'] = val
                    # Distribution frequency
                    dist_freq = ov.get('distFrequency')
                    if dist_freq:
                        etf_base['distribution_frequency'] = dist_freq
                    # NAV per unit from navPrices (most recent entry)
                    nav_prices = ov.get('navPrices') or []
                    if nav_prices:
                        nav_val = _safe_float(nav_prices[0].get('price'))
                        if nav_val and nav_val > 0:
                            etf_base['nav_per_unit'] = nav_val
                    # Distribution yield — sum last 12 months of CASH distributions / current NAV
                    distributions = ov.get('periodicDistributions') or []
                    if distributions and nav_prices:
                        nav_for_yield = _safe_float(nav_prices[0].get('price')) or 0
                        if nav_for_yield > 0:
                            from datetime import date as _date
                            one_year_ago = _date.today().replace(year=_date.today().year - 1).isoformat()
                            dist_total = 0.0
                            for dist in distributions:
                                dist_date = (dist.get('asOfDate') or '')[:10]
                                if dist_date < one_year_ago:
                                    break
                                for td in (dist.get('taxDetails') or []):
                                    if td.get('distributionType', {}).get('distCode') == 'CASH':
                                        dist_total += _safe_float(td.get('distributionAmount')) or 0
                            if dist_total > 0:
                                etf_base['distribution_yield'] = round(dist_total / nav_for_yield * 100, 2)
                    # Last distribution date and amount
                    if distributions:
                        last_dist = distributions[0]
                        last_dist_date = (last_dist.get('asOfDate') or '')[:10] or None
                        if last_dist_date:
                            etf_base['last_distribution_date'] = last_dist_date
                        for td in (last_dist.get('taxDetails') or []):
                            if td.get('distributionType', {}).get('distCode') == 'CASH':
                                amt = _safe_float(td.get('distributionAmount'))
                                if amt:
                                    etf_base['last_distribution_amount'] = amt
                                break
            # --- ETF-specific units on issue and FUM (nav × units) ---
            # The overview API's aumAmountWhole is the total managed fund AUM across
            # all share classes (ETF + managed fund), so we use the /prices endpoint
            # which provides OSCLTSHQTY (outstanding share quantity) for this ETF only.
            prices_url = f'https://www.vanguard.com.au/api/products/personal/fund/{port_id}/prices'
            prices_resp = fetch_json(prices_url, headers=_VANGUARD_API_HEADERS)
            if prices_resp:
                for fund in (prices_resp.get('data') or []):
                    ui = fund.get('unitsOnIssue', {})
                    records = (ui.get('OSCLTSHQTY') or [])
                    if records:
                        units = records[0].get('outstandingShare')
                        units_date = (records[0].get('effectiveDate') or '')[:10] or None
                        if units:
                            etf_base['units_on_issue'] = int(units)
                            etf_base['units_on_issue_date'] = units_date or date.today().isoformat()
                            nav = etf_base.get('nav_per_unit')
                            if nav:
                                net_assets = float(nav) * int(units)
                                etf_base['net_assets_aud'] = net_assets
                                etf_base['net_assets_date'] = units_date or date.today().isoformat()
                                etf_base['fund_size_aud_millions'] = round(net_assets / 1_000_000, 1)
                    break

            # Fallback for US-domiciled funds where benchMarkNameFromECS is absent
            if 'benchmark' not in etf_base and port_id in _VANGUARD_BENCHMARK_FALLBACK:
                etf_base['benchmark'] = _VANGUARD_BENCHMARK_FALLBACK[port_id]
            # Track benchmark for dedup cases (portIds shared across codes)
            if 'benchmark' in etf_base:
                port_id_benchmarks[port_id] = etf_base['benchmark']

            upsert_etf(conn, etf_base)
            upsert_units_history(conn, code, etf_base.get('units_on_issue'),
                                 etf_base.get('net_assets_aud'),
                                 date.today().isoformat(), commit=False)
            updated += 1

            # --- Holdings: ticker, name, weight, sector, country ---
            holdings_url = f'{_VANGUARD_API_BASE}/holdings-download/{port_id}'
            holdings_resp = fetch_json(holdings_url, headers=_VANGUARD_API_HEADERS)
            if not holdings_resp:
                conn.commit()
                continue

            raw_holdings = holdings_resp.get('data', [])
            if not isinstance(raw_holdings, list) or not raw_holdings:
                conn.commit()
                continue

            # Sort by weight descending and cap at max
            raw_holdings = sorted(
                raw_holdings,
                key=lambda h: h.get('marketValPercent') or 0,
                reverse=True
            )[:_VANGUARD_MAX_HOLDINGS]

            holdings = []
            sector_weights: dict[str, float] = {}

            for h in raw_holdings:
                weight = _safe_float(h.get('marketValPercent'))
                if weight is None or weight <= 0:
                    continue
                name = (h.get('name') or h.get('longName') or '').strip()
                if not name:
                    continue
                ticker  = (h.get('ticker') or '').strip() or None
                sec_raw = (h.get('sectorName') or '').strip()
                # '—' means no sector (common in bond funds)
                sector  = sec_raw if sec_raw and sec_raw != '—' else None
                country = (h.get('countryCode') or '').strip() or None

                holdings.append({
                    'name':       name,
                    'ticker':     ticker,
                    'weight_pct': weight,
                    'sector':     sector,
                    'country':    country,
                })

                if sector:
                    sector_weights[sector] = sector_weights.get(sector, 0) + weight

            if holdings:
                # Deduplicate by name (merge weights for any same-name entries)
                merged: dict[str, dict] = {}
                for h in holdings:
                    key = h['name']
                    if key in merged:
                        merged[key]['weight_pct'] = (merged[key]['weight_pct'] or 0) + (h['weight_pct'] or 0)
                    else:
                        merged[key] = h
                upsert_holdings(conn, code, list(merged.values()), commit=False)
                holdings_count += len(merged)

            if sector_weights:
                sectors = [
                    {'sector': s, 'weight_pct': round(w, 6)}
                    for s, w in sector_weights.items()
                ]
                upsert_sectors(conn, code, sectors, commit=False)

            conn.commit()
            logger.info(f"Vanguard: {code} — {len(holdings)} holdings, {len(sector_weights)} sectors")

        except Exception as e:
            logger.warning(f"Vanguard: error scraping {code} (portId={port_id}): {e}")
            conn.rollback()
            upsert_etf(conn, etf_base)
            upsert_units_history(conn, code, etf_base.get('units_on_issue'),
                                 etf_base.get('net_assets_aud'),
                                 date.today().isoformat(), commit=False)
            conn.commit()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f"Vanguard: updated {updated} ETFs, {holdings_count} holdings total")
    return updated


# ====================================================================
# iShares (BlackRock)
# ====================================================================

# Feeder fund map: AU code → (underlying_ticker, product_page_url)
# url=None means no look-through (e.g. physical gold ETC).
# Underlying product pages can be US (/us/individual/) or UK (/uk/individual/).
_ISHARES_FEEDER_MAP = {
    'IVV':  ('IVV', 'https://www.blackrock.com/us/individual/products/239726/'),
    'IJH':  ('IJH', 'https://www.blackrock.com/us/individual/products/239763/'),
    'IJR':  ('IJR', 'https://www.blackrock.com/us/individual/products/239761/'),
    # IOO AU feeds into iShares Global 100 ETF Trust → look through to US IOO product page
    'IOO':  ('IOO', 'https://www.blackrock.com/us/individual/products/239737/'),
    'IVE':  ('EFA', 'https://www.blackrock.com/us/individual/products/239600/'),
    'IXJ':  ('IXJ', 'https://www.blackrock.com/us/individual/products/239744/'),
    'IXI':  ('KXI', 'https://www.blackrock.com/us/individual/products/239740/'),
    # UCITS feeders — UK product pages
    'AGGG': ('AGGH', 'https://www.blackrock.com/uk/individual/products/291770/'),
    # Physical commodity — no equity look-through
    'GLDN': (None, None),
}

# Regex matching BlackRock holdings AJAX endpoints across all regional sites:
#   AU:  /au/products/{id}/fund/{ajax_id}.ajax
#   US:  /us/individual/products/{id}/{slug}/{ajax_id}.ajax
#   UK:  /uk/individual/products/{id}/fund/{ajax_id}.ajax
_BLACKROCK_HOLDINGS_AJAX_RE = re.compile(
    r'(/(?:au|us/individual|uk/individual)/products/\d+/[^/\'"]+/(\d+)\.ajax)'
    r'[^\'"]*fileName=[^\'"]*[Hh]olding'
)


def _detect_ishares_col_layout(rows: list) -> tuple[int, int]:
    """
    Detect (sector_idx, weight_idx) from the first AJAX data row.

    BlackRock product pages use two different column layouts:
      Format A (AU pages and some US pages like IVV):
        [2]=sector, [5]=weight (dict with 'raw' key, or plain float)
      Format B (most US/UK pages like IJH):
        [3]=sector, [17]=weight (plain float string, e.g. '1.41')

    Distinguish by checking if row[5] holds a weight-like value.
    """
    if not rows:
        return 2, 5
    row = rows[0]
    v5 = row[5] if len(row) > 5 else None
    if isinstance(v5, dict) and 'raw' in v5:
        try:
            if 0 < float(v5['raw']) < 100:
                return 2, 5
        except (TypeError, ValueError):
            pass
    elif isinstance(v5, (int, float)) and 0 < v5 < 100:
        return 2, 5
    return 3, 17


def _fetch_ishares_holdings_from_url(product_url: str) -> list[dict]:
    """
    Generic BlackRock holdings fetcher. Fetches a product page (AU, US, or UK),
    discovers the embedded holdings AJAX endpoint, and returns parsed holdings.

    Column layout is detected dynamically from the AJAX data (see _detect_ishares_col_layout).
    """
    resp = fetch(product_url)
    if not resp or resp.status_code != 200:
        return []

    m = _BLACKROCK_HOLDINGS_AJAX_RE.search(resp.text)
    if not m:
        logger.debug(f"iShares look-through: no holdings AJAX found at {product_url}")
        return []

    json_url = f'https://www.blackrock.com{m.group(1)}?tab=all&fileType=json'
    json_resp = fetch(json_url)
    if not json_resp:
        return []

    try:
        data = json.loads(json_resp.text.lstrip('\ufeff'))
    except (ValueError, json.JSONDecodeError):
        return []

    if 'aaData' not in data:
        return []

    sector_idx, weight_idx = _detect_ishares_col_layout(data['aaData'])
    logger.debug(f"iShares AJAX layout: sector={sector_idx}, weight={weight_idx} ({product_url})")

    holdings = []
    for row in data['aaData']:
        if len(row) <= weight_idx:
            continue
        ticker = str(row[0]).strip() or None
        name   = str(row[1]).strip() or None
        sector = str(row[sector_idx]).strip() if len(row) > sector_idx else None
        weight_raw = row[weight_idx]
        if isinstance(weight_raw, dict):
            weight = _safe_float(weight_raw.get('raw') or weight_raw.get('display'))
        else:
            weight = _safe_float(weight_raw)
        country = None
        for idx in range(12, min(16, len(row))):
            val = row[idx]
            if isinstance(val, str) and val not in ('-', 'N/A', '') and not val.isdigit():
                country = val.strip()
                break
        if not name or name == '-':
            continue
        if weight is None or not (0 < weight < 100):
            continue
        holdings.append({
            'ticker':     ticker if ticker and ticker != '-' else None,
            'name':       name,
            'sector':     sector if sector and sector not in ('-', 'N/A') else None,
            'weight_pct': weight,
            'country':    country if country and country not in ('-', 'N/A') else None,
        })
    return holdings


def _detect_ishares_feeders(conn) -> list[tuple[str, str, float]]:
    """
    Return list of (au_code, underlying_ticker, feeder_weight_pct) for iShares
    ETFs whose holdings table contains a single dominant position (>= 90%) that
    is itself an ETF-like security (i.e. the au_code is in _ISHARES_FEEDER_MAP).
    """
    rows = conn.execute(
        "SELECT etf_code, ticker, weight_pct FROM etf_holdings "
        "WHERE etf_code IN ({}) AND weight_pct >= 90".format(
            ','.join(f'"{c}"' for c in _ISHARES_FEEDER_MAP)
        )
    ).fetchall()
    return [(r['etf_code'], r['ticker'] or '', r['weight_pct']) for r in rows]


def _apply_ishares_lookthrough(conn, au_code: str, underlying_url: str, feeder_weight: float):
    """
    Fetch look-through holdings from underlying_url, scale by feeder_weight,
    and replace the feeder's holdings + sectors.
    """
    holdings = _fetch_ishares_holdings_from_url(underlying_url)
    if not holdings:
        logger.warning(f"iShares look-through {au_code}: no holdings from {underlying_url}")
        return

    scale = feeder_weight / 100.0
    for h in holdings:
        h['weight_pct'] = round((h['weight_pct'] or 0) * scale, 6)

    # Merge duplicates
    merged: dict[str, dict] = {}
    for h in holdings:
        key = h['name']
        if key in merged:
            merged[key]['weight_pct'] = (merged[key]['weight_pct'] or 0) + (h['weight_pct'] or 0)
        else:
            merged[key] = dict(h)

    upsert_holdings(conn, au_code, list(merged.values()), commit=False)

    sector_weights: dict[str, float] = {}
    for h in merged.values():
        s = h.get('sector')
        w = h.get('weight_pct') or 0
        if s and s != 'Cash and/or Derivatives':
            sector_weights[s] = sector_weights.get(s, 0) + w
    if sector_weights:
        sectors = [{'sector': s, 'weight_pct': round(w, 6)} for s, w in sector_weights.items()]
        upsert_sectors(conn, au_code, sectors, commit=False)

    conn.commit()
    logger.info(f"iShares look-through {au_code}: {len(merged)} holdings from underlying")


def _scrape_ishares_product_page(conn, code: str, product_url: str) -> bool:
    """
    Fetch all holdings for a single iShares ETF from its BlackRock product page.

    BlackRock product pages embed AJAX endpoint URLs of the form:
        /au/products/{productId}/fund/{ajaxId}.ajax
    We find the holdings-specific endpoint (identified by a 'holdings' filename
    in the download link) and call it with tab=all&fileType=json.

    JSON response shape:
        {"aaData": [[ticker, name, sector, asset_class, market_value, weight,
                     country, currency], ...]}
    where weight is either a plain float or {"raw": 10.83, "display": "10.83"}.

    Returns True if any holdings were stored.
    """
    resp = fetch(product_url)
    if not resp:
        return False

    # Find the holdings AJAX base path from the CSV download link embedded in the page.
    # Standard:  /au/products/251852/fund/1478358644060.ajax?fileType=csv&fileName=IOZ_holdings
    # Slug-based: /au/products/251979/ishares-ubs-treasury-fund/1478358644060.ajax?...
    holdings_match = re.search(
        r'(/au/products/\d+/[^/"\' ]+/(\d+)\.ajax)[^"\']*fileName=[^"\']*[Hh]olding',
        resp.text,
    )
    if not holdings_match:
        logger.debug(f"iShares {code}: holdings ajax URL not found in product page")
        return False

    base_ajax_path = holdings_match.group(1)
    json_url = f'https://www.blackrock.com{base_ajax_path}?tab=all&fileType=json'

    # BlackRock returns UTF-8 with BOM — strip it before JSON parsing
    json_resp = fetch(json_url)
    if not json_resp:
        logger.debug(f"iShares {code}: no response from holdings URL")
        return False
    try:
        data = json.loads(json_resp.text.lstrip('\ufeff'))
    except (ValueError, json.JSONDecodeError):
        logger.debug(f"iShares {code}: invalid JSON in holdings response")
        return False
    if 'aaData' not in data:
        logger.debug(f"iShares {code}: no aaData in holdings response")
        return False

    # Row format (15 columns):
    #   0:ticker  1:name  2:sector  3:asset_class  4:market_value  5:weight
    #   6:notional  7:shares  8:security_code  9:ISIN  10:exchange_code
    #   11:price  12:country  13:exchange  14:currency
    holdings = []
    for row in data['aaData']:
        if len(row) < 6:
            continue
        ticker = str(row[0]).strip() or None
        name   = str(row[1]).strip() or None
        sector = str(row[2]).strip() or None
        # Weight is normally at index 5, but cash/money-market funds insert an extra
        # category column at index 4, pushing weight to index 6 (e.g. BILL, ISEC).
        # Detect by checking whether index 5 looks like a large market-value number.
        def _extract_raw(v):
            if isinstance(v, dict):
                return _safe_float(v.get('raw') or v.get('display'))
            return _safe_float(v)
        weight = _extract_raw(row[5]) if len(row) > 5 else None
        if weight is None or weight >= 100:
            # Fall back to index 6 for extra-column layouts
            weight = _extract_raw(row[6]) if len(row) > 6 else None
        # Country position varies by fund type (equity: index 12, fixed income: index 13+)
        # Scan from index 12 for the first plain string that looks like a country name
        country = None
        for idx in range(12, min(16, len(row))):
            val = row[idx]
            if isinstance(val, str) and val not in ('-', 'N/A', '') and not val.isdigit():
                country = val.strip()
                break

        if not name or name == '-':
            continue
        if weight is None or not (0 < weight < 100):
            continue

        holdings.append({
            'ticker':     ticker if ticker and ticker != '-' else None,
            'name':       name,
            'sector':     sector if sector and sector not in ('-', 'N/A') else None,
            'weight_pct': weight,
            'country':    country if country and country not in ('-', 'N/A') else None,
        })

    if not holdings:
        return False

    # Deduplicate by name (merge weights for same-name entries e.g. dual-listed shares)
    merged: dict[str, dict] = {}
    for h in holdings:
        key = h['name']
        if key in merged:
            merged[key]['weight_pct'] = (merged[key]['weight_pct'] or 0) + (h['weight_pct'] or 0)
        else:
            merged[key] = dict(h)

    upsert_holdings(conn, code, list(merged.values()), commit=False)

    # Aggregate sector allocations from holdings (skip cash/derivatives for sector summary)
    sector_weights: dict[str, float] = {}
    for h in merged.values():
        s = h.get('sector')
        w = h.get('weight_pct') or 0
        if s and s != 'Cash and/or Derivatives':
            sector_weights[s] = sector_weights.get(s, 0) + w
    if sector_weights:
        sectors = [{'sector': s, 'weight_pct': round(w, 6)} for s, w in sector_weights.items()]
        upsert_sectors(conn, code, sectors, commit=False)

    logger.debug(f"iShares {code}: stored {len(merged)} holdings, {len(sector_weights)} sectors")
    return True


def scrape_ishares(db_path=None) -> int:
    """
    Scrape iShares/BlackRock Australia ETF holdings via individual product pages.

    Phase 1: Upsert base ETF records using the hardcoded ISHARES_AU_PRODUCTS map,
             setting issuer_url = https://www.blackrock.com/au/products/{product_id}/
    Phase 2: For each ETF with a blackrock.com/au/products/ URL, scrape holdings
             via the AJAX endpoint embedded in the product page HTML.
    """
    started = datetime.utcnow()
    source = 'ishares'
    updated = 0

    conn = get_connection(db_path)

    # ---- Phase 1: upsert base records with correct product page URLs ----
    for code, product_id in ISHARES_AU_PRODUCTS.items():
        issuer_url = f'https://www.blackrock.com/au/products/{product_id}/'
        upsert_etf(conn, {
            'code': code,
            'issuer': 'iShares',
            'issuer_url': issuer_url,
            'data_source': 'ishares',
        })
        updated += 1
    conn.commit()

    # ---- Phase 2: holdings from individual product pages ----
    holdings_count = 0
    for code, product_id in ISHARES_AU_PRODUCTS.items():
        url = f'https://www.blackrock.com/au/products/{product_id}/'
        try:
            if _scrape_ishares_product_page(conn, code, url):
                holdings_count += 1
                logger.info(f"iShares: got holdings for {code}")
        except Exception as e:
            logger.warning(f"iShares holdings {code}: {e}")
        conn.commit()

    logger.info(f"iShares: fetched holdings for {holdings_count}/{updated} ETFs")

    # ---- Phase 3: feeder fund look-through ----
    # Iterate the feeder map directly rather than detecting from holdings, because some
    # AU product pages show only a cash row (e.g. IVV) rather than the feeder ETF unit,
    # preventing detection-based approaches from finding them.
    for au_code, (underlying_ticker, underlying_url) in _ISHARES_FEEDER_MAP.items():
        if not underlying_url:
            # No look-through available (unlisted trust or closed underlying fund).
            # Clear feeder-unit holdings so the dashboard shows "no holdings data"
            # rather than a misleading single trust/ETF row.
            conn.execute("DELETE FROM etf_holdings WHERE etf_code=?", (au_code,))
            conn.execute("DELETE FROM etf_sectors WHERE etf_code=?", (au_code,))
            conn.commit()
            logger.info(f"iShares look-through: cleared feeder holdings for {au_code} (no underlying)")
            continue
        try:
            _apply_ishares_lookthrough(conn, au_code, underlying_url, 100.0)
        except Exception as e:
            logger.warning(f"iShares look-through {au_code}: {e}")

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if (updated or holdings_count) else 'no_data',
               records_affected=updated + holdings_count, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f"iShares: updated {updated} ETFs")
    return updated


# ====================================================================
# SSGA Holdings (shared by SPDR + StateStreet)
# ====================================================================

def _parse_ssga_holdings_excel(content: bytes) -> list[dict]:
    """
    Parse an SSGA holdings-daily Excel file.
    URL pattern: https://www.ssga.com/library-content/products/fund-data/etfs/apac/holdings-daily-au-en-{ticker}.xlsx
    Structure:
      Row 0-2: metadata (Fund Name, Ticker, Holdings date)
      Row 4:   header (ISIN, SEDOL, Ticker, Name, Currency, Shares, Weight(%), Country, Price, Sector, Industry)
      Row 5+:  holdings rows
    Returns list of {name, ticker, isin, weight_pct, sector, country}.
    """
    try:
        import openpyxl
    except ImportError:
        logger.error("openpyxl required for SSGA holdings parsing")
        return []

    try:
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    except Exception as e:
        logger.warning(f"SSGA Excel parse error: {e}")
        return []

    ws = wb.active
    header_row = None
    col = {}
    holdings = []

    for row_idx, row in enumerate(ws.iter_rows(values_only=True)):
        # Find header row (contains 'Weight' or 'Name')
        if header_row is None:
            row_lower = [str(v or '').strip().lower() for v in row]
            if any('weight' in c for c in row_lower):
                header_row = row_idx
                for i, h in enumerate(row_lower):
                    if 'name' in h and 'fund' not in h:
                        col.setdefault('name', i)
                    elif 'ticker' in h or 'symbol' in h:
                        col.setdefault('ticker', i)
                    elif 'isin' in h:
                        col.setdefault('isin', i)
                    elif 'weight' in h:
                        col.setdefault('weight', i)
                    elif 'sector' in h and 'industry' not in h:
                        col.setdefault('sector', i)
                    elif 'country' in h or 'trade country' in h:
                        col.setdefault('country', i)
            continue

        if header_row is None or not col:
            continue

        def gcol(key):
            idx = col.get(key)
            return row[idx] if idx is not None and idx < len(row) else None

        name = str(gcol('name') or '').strip()
        if not name or name.lower() in ('cash', '-', 'n/a', ''):
            continue

        weight = _safe_float(gcol('weight'))
        if weight is None or weight <= 0:
            continue

        holdings.append({
            'name':       name,
            'ticker':     str(gcol('ticker') or '').strip() or None,
            'isin':       str(gcol('isin') or '').strip() or None,
            'weight_pct': weight,
            'sector':     str(gcol('sector') or '').strip() or None,
            'country':    str(gcol('country') or '').strip() or None,
        })

    return holdings


def _scrape_ssga_holdings(conn, code: str) -> bool:
    """
    Download and upsert SSGA holdings for a single ETF code.
    Returns True if holdings were found and saved.
    """
    url = f'https://www.ssga.com/library-content/products/fund-data/etfs/apac/holdings-daily-au-en-{code.lower()}.xlsx'
    resp = fetch(url)
    if not resp or resp.status_code != 200:
        return False

    rows = _parse_ssga_holdings_excel(resp.content)
    if not rows:
        return False

    # Deduplicate by name
    merged: dict[str, dict] = {}
    for h in rows:
        n = h['name']
        if n in merged:
            merged[n]['weight_pct'] = (merged[n]['weight_pct'] or 0) + (h['weight_pct'] or 0)
        else:
            merged[n] = h

    upsert_holdings(conn, code, list(merged.values()), commit=False)

    # Aggregate sector allocations
    sector_weights: dict[str, float] = {}
    for h in merged.values():
        s = h.get('sector')
        if s and s.lower() not in ('cash', 'cash and/or derivatives', '-', 'n/a', ''):
            sector_weights[s] = sector_weights.get(s, 0) + (h.get('weight_pct') or 0)
    if sector_weights:
        sectors = [{'sector': s, 'weight_pct': round(w, 4)} for s, w in sector_weights.items()]
        upsert_sectors(conn, code, sectors, commit=False)

    return True


# ====================================================================
# SPDR (State Street)
# ====================================================================

def scrape_spdr(db_path=None) -> int:
    """
    Scrape SPDR/State Street Australia ETF data from individual fund pages.
    (SSGA fund finder is JS-rendered; individual pages serve static HTML with JSON-LD.)
    Uses SPDR_AU_FUNDS from config as the known fund index.
    """
    started = datetime.utcnow()
    source = 'spdr'
    updated = 0

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return 0

    conn = get_connection(db_path)
    base_url = 'https://www.ssga.com/au/en_gb/intermediary/etfs/funds/'

    for code, slug in SPDR_AU_FUNDS.items():
        url = base_url + slug
        resp = fetch(url)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Name and ticker from JSON-LD schema
        name = None
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                ld = json.loads(script.string.strip())
                raw_name = ld.get('name', '')
                # Strip HTML entities and trademark symbols
                name = re.sub(r'[®™]|&\w+;', '', raw_name).strip() or None
                # Verify this page is for the right ticker
                ticker_symbol = ld.get('tickerSymbol', '')
                if code not in ticker_symbol:
                    name = None  # Wrong page
                break
            except (ValueError, json.JSONDecodeError):
                continue

        if not name:
            # Try page title: "STW: SPDR S&P ASX 200 ETF | State Street ETFs"
            title = soup.title.string if soup.title else ''
            if code in title:
                name_match = re.match(rf'^{code}:\s*(.+?)\s*\|', title)
                if name_match:
                    name = name_match.group(1).strip()

        # SSGA pages embed data as HTML-entity-encoded JSON — decode once for all extractions
        decoded = html_module.unescape(resp.text)

        # MER from "total-expense-ratio" embedded JSON key
        mer = None
        mer_match = re.search(
            r'"total-expense-ratio"\s*:\s*\{[^}]*"value"\s*:\s*"([0-9.]+)\s*%',
            decoded,
        )
        if mer_match:
            mer = _safe_float(mer_match.group(1))
            if mer and not (0 < mer < 5):
                mer = None

        # Inception date
        inception = None
        inc_match = re.search(r'"inception-date"[^}]*"value"\s*:\s*"([^"]+)"', decoded)
        if inc_match:
            inception = inc_match.group(1).strip() or None

        # Benchmark from HTML table row: <td>Benchmark</td><td>{index name}</td>
        benchmark = None
        for td in soup.find_all('td'):
            if td.get_text(strip=True) == 'Benchmark':
                next_td = td.find_next_sibling('td')
                if next_td:
                    val = next_td.get_text(strip=True)
                    if val and len(val) > 3:
                        benchmark = val
                        break

        # Sector allocations from attrArray (picks the first occurrence)
        sectors = []
        sector_block = re.search(r'"attrArray"\s*:\s*(\[[^\]]+\])', decoded)
        if sector_block:
            try:
                attr_list = json.loads(sector_block.group(1))
                for entry in attr_list:
                    sector_name = (entry.get('name') or {}).get('value', '').strip()
                    weight_str  = (entry.get('weight') or {}).get('originalValue', '')
                    weight = _safe_float(weight_str)
                    if sector_name and weight is not None and 0 < weight < 100:
                        sectors.append({'sector': sector_name, 'weight_pct': weight})
            except (ValueError, json.JSONDecodeError, AttributeError):
                pass

        # ── NEW: FUM, NAV, units, returns, distribution yield ───────────────────
        # SSGA embeds fund data as decoded JSON embedded in attribute values.
        # Key: "nav":{"originalValue":"78.1705"}
        # Key: "aum":{"originalValue":"6299437556.17"}
        fum_aud = None
        nav_per_unit = None
        spdr_returns: dict = {}
        spdr_yield = None

        aum_m = re.search(r'"aum"\s*:\s*\{[^}]*"originalValue"\s*:\s*"([0-9.]+)"', decoded)
        if aum_m:
            fum_raw = _safe_float(aum_m.group(1))
            if fum_raw and fum_raw > 1_000:
                fum_aud = fum_raw

        nav_m = re.search(r'"nav"\s*:\s*\{[^}]*"originalValue"\s*:\s*"([0-9.]+)"', decoded)
        if nav_m:
            nav = _safe_float(nav_m.group(1))
            if nav and nav > 0:
                nav_per_unit = nav

        # Units on issue derived from FUM / NAV (both available above)
        units_on_issue = None
        if fum_aud and nav_per_unit and nav_per_unit > 0:
            units_on_issue = int(round(fum_aud / nav_per_unit))

        # Distribution yield from HTML table
        spdr_yield_m = re.search(
            r'[Dd]istribution\s+[Yy]ield[^<>%]*?([0-9]+\.?[0-9]*)\s*%', decoded
        )
        if spdr_yield_m:
            yld = _safe_float(spdr_yield_m.group(1))
            if yld and 0 < yld < 50:
                spdr_yield = yld

        # Returns from the performance table: find the "Fund Total Return" row
        # Table structure: first <th> is "As Of" (row-label column), then data columns.
        # Data row: td[0]=label, td[1]=date (skipped), td[2+]=values aligned to th[1+].
        _SPDR_HEADER_MAP = {
            '1 month':       'return_1m',
            '3 month':       'return_3m',
            '1 year':        'return_1y',
            '3 year (p.a.)': 'return_3y',
            '3 years (p.a.)': 'return_3y',
            '5 year (p.a.)': 'return_5y',
            '5 years (p.a.)': 'return_5y',
        }
        for table in soup.find_all('table'):
            th_texts = [th.get_text(strip=True).lower() for th in table.find_all('th')]
            if '1 year' not in th_texts:
                continue
            # Build col_index → field map.  th[0] is the row-label header ("As Of"),
            # so th[i] aligns with data_vals[i-1] (offset of 1).
            col_map: dict[int, str] = {}
            for col_i, h in enumerate(th_texts):
                if col_i == 0:
                    continue  # row-label column has no data counterpart
                field = _SPDR_HEADER_MAP.get(h)
                if field:
                    col_map[col_i] = field
                elif 'since inception' in h or ('inception' in h and 'date' not in h):
                    col_map[col_i] = 'return_since_inception'
            # Find "Fund Total Return" data row
            for row in table.find_all('tr'):
                tds = row.find_all('td')
                if not tds:
                    continue
                row_label = tds[0].get_text(strip=True).lower()
                if 'total return' not in row_label:
                    continue
                # data_vals: skip td[0] (label) and td[1] (date), collect remaining % values
                data_vals = []
                for td in tds[2:]:
                    txt = td.get_text(strip=True)
                    pct = _safe_float(txt)
                    data_vals.append(pct)  # None if not a number
                # col_i is th index (1-based); data_vals is 0-based → offset = col_i - 1
                for col_i, field in col_map.items():
                    dv_i = col_i - 1
                    if dv_i < len(data_vals) and data_vals[dv_i] is not None:
                        spdr_returns[field] = data_vals[dv_i]
                break
            break  # only process first matching table

        etf = {
            'code': code,
            'name': name,
            'issuer': 'SPDR',
            'expense_ratio': mer,
            'management_fee': mer,
            'inception_date': inception,
            'benchmark': benchmark,
            'exchange': 'ASX',
            'data_source': 'spdr',
            'issuer_url': url,
            'nav_per_unit': nav_per_unit,
            'fund_size_aud_millions': round(fum_aud / 1_000_000, 1) if fum_aud else None,
            'net_assets_aud': fum_aud,
            'net_assets_date': date.today().isoformat() if fum_aud else None,
            'units_on_issue': units_on_issue,
            'units_on_issue_date': date.today().isoformat() if units_on_issue else None,
            'distribution_yield': spdr_yield,
            **spdr_returns,
        }
        etf = {k: v for k, v in etf.items() if v is not None}
        upsert_etf(conn, etf)
        upsert_units_history(conn, code, etf.get('units_on_issue'), etf.get('net_assets_aud'),
                             date.today().isoformat(), commit=False)
        if sectors:
            upsert_sectors(conn, code, sectors, commit=False)

        # Holdings from SSGA daily Excel
        _scrape_ssga_holdings(conn, code)
        conn.commit()
        updated += 1

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f"SPDR: updated {updated} ETFs")
    return updated


# ====================================================================
# StateStreet
# ====================================================================

# StateStreet ETFs listed on ASX/Cboe (not branded as SPDR)
_STATESTREET_AU_CODES = [
    'SPY', 'SSO', 'E200', 'SYI', 'QMIX', 'WXOZ', 'WXHG', 'OZR', 'OZF', 'WEMG',
]


def scrape_statestreet(db_path=None) -> int:
    """
    Download SSGA daily holdings Excel for each StateStreet AU ETF.
    Metadata (name, MER) comes from the ASX/Cboe reports; this adds holdings + sectors.
    """
    started = datetime.utcnow()
    source = 'statestreet'
    updated = 0

    conn = get_connection(db_path)

    # Fetch known StateStreet codes from DB (may be more than the hardcoded list)
    db_codes = [r[0] for r in conn.execute(
        "SELECT code FROM etfs WHERE issuer='StateStreet' ORDER BY code"
    ).fetchall()]
    codes = db_codes or _STATESTREET_AU_CODES

    for code in codes:
        if _scrape_ssga_holdings(conn, code):
            conn.commit()
            updated += 1
            logger.info(f"StateStreet: got holdings for {code}")
        else:
            logger.debug(f"StateStreet: no SSGA holdings for {code}")

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f"StateStreet: updated {updated} ETFs")
    return updated


# ====================================================================
# Global X
# ====================================================================

_GLOBALX_STRAPI_BASE = 'https://d255kjlej81hb4.cloudfront.net'
_GLOBALX_FUNDS_PAGE  = 'https://www.globalxetfs.com.au/funds/ndq/'
_GLOBALX_CHUNK_RE    = re.compile(r'/_next/static/chunks/([^"\'?\s]+\.js)')
_GLOBALX_TOKEN_RE    = re.compile(
    r'Authorization.*?Bearer.*?concat\("([a-f0-9]{60,})', re.DOTALL
)

# Global X Strapi category name → canonical asset class
_GLOBALX_CATEGORY_MAP = {
    'core':              'Australian Equities',
    'income':            'Fixed Income',
    'international':     'International Equities',
    'thematic':          'Thematic',
    'commodities':       'Commodities',
    'crypto':            'Digital Assets',
    'leveraged and inverse': 'Alternatives',
    'leveraged':         'Alternatives',
}


def _globalx_get_token() -> str | None:
    """
    Discover the Global X Strapi bearer token from the Next.js JS bundles.
    The token is embedded in one of the static chunks (stable across page loads).
    """
    page = fetch(_GLOBALX_FUNDS_PAGE)
    if not page:
        return None
    chunks = _GLOBALX_CHUNK_RE.findall(page.text)
    for chunk in chunks:
        chunk_url = f'https://www.globalxetfs.com.au/_next/static/chunks/{chunk}'
        resp = fetch(chunk_url)
        if not resp:
            continue
        m = _GLOBALX_TOKEN_RE.search(resp.text)
        if m:
            return m.group(1)
    return None


def _parse_globalx_pcf(content: bytes, code: str) -> list[dict]:
    """
    Parse a Global X PCF (Portfolio Composition File) Excel.
    Structure:
      Header row: '#', 'Component Name', 'ISIN', 'SEDOL', 'Bloomberg Ticker',
                  'Number of Shares', 'Local CCY', 'Local CCY Price',
                  'Market Value (Base CCY)', 'Weight', 'Sector', 'Country'
    Weight is a decimal fraction (0.121 = 12.1%) — multiply by 100.
    """
    try:
        import openpyxl
    except ImportError:
        return []
    try:
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    except Exception as e:
        logger.warning(f"Global X PCF parse error for {code}: {e}")
        return []

    ws = wb.active
    col = {}
    holdings = []
    for row in ws.iter_rows(values_only=True):
        vals = list(row)
        # Header row detection
        if not col:
            row_lower = [str(v or '').strip().lower() for v in vals]
            if 'component name' in row_lower or 'name' in row_lower:
                for i, h in enumerate(row_lower):
                    if h in ('component name', 'name'):
                        col.setdefault('name', i)
                    elif 'weight' in h and 'market' not in h:
                        col.setdefault('weight', i)
                    elif h == 'sector':
                        col.setdefault('sector', i)
                    elif h == 'country':
                        col.setdefault('country', i)
                    elif 'isin' in h:
                        col.setdefault('isin', i)
                    elif 'bloomberg' in h or 'ticker' in h:
                        col.setdefault('ticker', i)
            continue

        def gcol(key):
            idx = col.get(key)
            return vals[idx] if idx is not None and idx < len(vals) else None

        name = str(gcol('name') or '').strip()
        if not name or name.lower() in ('cash', '-', 'n/a', ''):
            continue
        weight_raw = _safe_float(gcol('weight'))
        if weight_raw is None or weight_raw <= 0:
            continue
        # Weight is stored as a decimal fraction (e.g. 0.121 = 12.1%)
        weight_pct = weight_raw * 100 if weight_raw < 1.5 else weight_raw

        holdings.append({
            'name':       name,
            'ticker':     str(gcol('ticker') or '').strip() or None,
            'isin':       str(gcol('isin') or '').strip() or None,
            'weight_pct': round(weight_pct, 4),
            'sector':     str(gcol('sector') or '').strip() or None,
            'country':    str(gcol('country') or '').strip() or None,
        })
    return holdings


def scrape_globalx(db_path=None) -> int:
    """
    Scrape Global X Australia via their Strapi CMS API + PCF holdings files.
    API:  GET https://d255kjlej81hb4.cloudfront.net/api/products?populate=*
    PCF:  each product.pcf is a direct xlsx download URL
    """
    started = datetime.utcnow()
    source = 'globalx'
    updated = 0

    conn = get_connection(db_path)

    # Step 1: Get auth token
    token = _globalx_get_token()
    if not token:
        logger.error("Global X: could not discover bearer token — skipping")
        log_scrape(conn, source, 'error', records_affected=0,
                   duration_secs=0, started_at=started.isoformat())
        conn.close()
        return 0

    auth_headers = {'Authorization': f'Bearer {token}'}

    # Step 2: Fetch all products from Strapi
    api_url = f'{_GLOBALX_STRAPI_BASE}/api/products?populate=*&pagination[limit]=100'
    resp = fetch(api_url, headers=auth_headers)
    if not resp:
        logger.error("Global X: Strapi API request failed")
        conn.close()
        return 0

    try:
        payload = resp.json()
        products = payload.get('data', [])
    except Exception as e:
        logger.error(f"Global X: Strapi JSON parse error: {e}")
        conn.close()
        return 0

    logger.info(f"Global X: {len(products)} products from Strapi API")

    for product in products:
        ticker = str(product.get('ticker') or '').strip().upper()
        if not ticker or not re.match(r'^[A-Z][A-Z0-9]{1,5}$', ticker):
            continue

        name = str(product.get('name') or '').strip() or None
        mer  = _safe_float(product.get('manageCosts'))
        inception = str(product.get('inceptionDate') or '').strip() or None
        slug = str(product.get('pageSlug') or ticker.lower())
        issuer_url = f'https://www.globalxetfs.com.au/funds/{slug}/'

        # Asset class from Strapi category
        cat_name = ((product.get('category') or {}).get('name') or '').strip().lower()
        asset_class = _GLOBALX_CATEGORY_MAP.get(cat_name) or normalise_asset_class(cat_name) or None

        # NAV, AUM, units from the NAV history Excel embedded in fundOverview.fundNav
        nav_per_unit = None
        fum_aud = None
        units_on_issue = None
        units_date = None
        dist_yield = None

        fund_overview = product.get('fundOverview') or {}
        fund_nav_block = fund_overview.get('fundNav') or {}
        nav_history_url = (fund_nav_block.get('navHistoryLink') or '').strip()
        if nav_history_url:
            nav_xl = fetch(nav_history_url)
            if nav_xl and nav_xl.status_code == 200:
                try:
                    import openpyxl as _openpyxl
                    wb = _openpyxl.load_workbook(io.BytesIO(nav_xl.content), read_only=True, data_only=True)
                    ws = wb.active
                    last_row = None
                    for row in ws.iter_rows(values_only=True):
                        # Data rows have a datetime in col index 2 and numeric NAV in col 4
                        if row[2] and hasattr(row[2], 'year') and row[4] is not None:
                            last_row = row
                    if last_row is not None:
                        nav_per_unit = _safe_float(last_row[4])
                        fum_aud = _safe_float(last_row[5])
                        units_raw = last_row[6]
                        if units_raw is not None:
                            units_on_issue = int(units_raw)
                        if hasattr(last_row[2], 'date'):
                            units_date = last_row[2].date().isoformat()
                except Exception as _e:
                    logger.debug(f"Global X {ticker}: NAV Excel parse error: {_e}")

        # Distribution yield from fundDistribution otherInfoList
        fund_dist = fund_overview.get('fundDistribution') or {}
        for item in (fund_dist.get('otherInfoList') or []):
            if '12-month yield' in (item.get('label') or '').lower():
                dist_yield = _safe_float(item.get('textValue'))
                break

        etf = {
            'code':         ticker,
            'name':         name,
            'issuer':       'Global X',
            'expense_ratio': mer,
            'management_fee': mer,
            'asset_class':  asset_class,
            'inception_date': inception,
            'data_source':  'globalx',
            'issuer_url':   issuer_url,
            'exchange':     'ASX',
            'nav_per_unit': nav_per_unit,
            'net_assets_aud': fum_aud,
            'net_assets_date': units_date if fum_aud else None,
            'fund_size_aud_millions': round(fum_aud / 1_000_000, 1) if fum_aud else None,
            'units_on_issue': units_on_issue,
            'units_on_issue_date': units_date if units_on_issue else None,
            'distribution_yield': dist_yield,
        }
        etf = {k: v for k, v in etf.items() if v is not None}
        upsert_etf(conn, etf)
        upsert_units_history(conn, ticker, units_on_issue, fum_aud,
                             units_date or date.today().isoformat(), commit=False)

        # Step 3: Download PCF for holdings
        pcf_url = str(product.get('pcf') or '').strip()
        if pcf_url:
            pcf_resp = fetch(pcf_url)
            if pcf_resp and pcf_resp.status_code == 200:
                rows = _parse_globalx_pcf(pcf_resp.content, ticker)
                if rows:
                    # Deduplicate by name
                    merged: dict[str, dict] = {}
                    for h in rows:
                        n = h['name']
                        if n in merged:
                            merged[n]['weight_pct'] = round(
                                (merged[n]['weight_pct'] or 0) + (h['weight_pct'] or 0), 4
                            )
                        else:
                            merged[n] = h
                    upsert_holdings(conn, ticker, list(merged.values()), commit=False)

                    # Sector aggregation
                    sector_wt: dict[str, float] = {}
                    for h in merged.values():
                        s = h.get('sector')
                        if s and s.lower() not in ('cash', '-', 'n/a', ''):
                            sector_wt[s] = round(sector_wt.get(s, 0) + (h.get('weight_pct') or 0), 4)
                    if sector_wt:
                        upsert_sectors(conn, ticker,
                                       [{'sector': s, 'weight_pct': w} for s, w in sector_wt.items()],
                                       commit=False)

        conn.commit()
        updated += 1

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f"Global X: updated {updated} ETFs")
    return updated


# ====================================================================
# CXA Issuer Scrapers (distribution yield)
# ====================================================================

def _extract_distribution_yield(soup, raw_text: str) -> float | None:
    """
    Try to extract a distribution yield percentage from a fund page.
    Checks JSON-LD structured data, meta tags, HTML tables, then raw-text regex.
    Returns the yield as a float (e.g. 4.5 for 4.5%), or None if not found.
    """
    # JSON-LD structured data
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string.strip())
            for key in ('distributionYield', 'distribution_yield', 'dividendYield', 'yield'):
                val = _safe_float(data.get(key))
                if val and 0 < val < 50:
                    return val
        except (ValueError, json.JSONDecodeError, AttributeError):
            continue

    # Meta tags
    for meta in soup.find_all('meta'):
        name_attr = (meta.get('name') or meta.get('property') or '').lower()
        if 'yield' in name_attr or 'distribution' in name_attr:
            val = _safe_float(meta.get('content'))
            if val and 0 < val < 50:
                return val

    # HTML tables with distribution/yield headers
    for table in soup.find_all('table'):
        headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
        if not any('distribution' in h or 'yield' in h for h in headers):
            continue
        for row in table.find_all('tr'):
            cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
            if len(cells) < 2:
                continue
            label = cells[0].lower()
            if 'distribution yield' in label or ('yield' in label and 'distribution' in label):
                val = _safe_float(cells[1])
                if val and 0 < val < 50:
                    return val

    # Raw text regex fallback — ordered most-to-least specific
    for pattern in [
        # Coolabah "Currently yielding 6.2% p.a." callout
        r'currently\s+yielding\s+([0-9]+\.?[0-9]*)\s*%',
        # Generic distribution yield label
        r'distribution\s+yield[^<>0-9]*([0-9]+\.?[0-9]*)\s*%',
        r'"distributionYield"\s*:\s*"?([0-9]+\.?[0-9]*)',
        # Running yield / gross yield table rows
        r'(?:running|gross)\s+(?:running\s+)?yield[^<>0-9]*([0-9]+\.?[0-9]*)\s*%',
    ]:
        m = re.search(pattern, raw_text, re.I)
        if m:
            val = _safe_float(m.group(1))
            if val and 0 < val < 50:
                return val

    return None


def _make_slug(name: str) -> str:
    """Convert a fund name to a URL-friendly slug."""
    slug = re.sub(r'[^a-z0-9\s]', '', name.lower())
    return re.sub(r'\s+', '-', slug).strip('-')


# Hardcoded URL slugs for CXA issuers where auto-generation from name doesn't match
# the actual site structure.

# Coolabah: domain is coolabahcapital.com (not coolabah.com.au); slugs are non-standard
_COOLABAH_SLUGS = {
    'CBNX': 'coolabah-global-carbon-leaders-complex-etf',
    'FIXD': 'active-composite-bond-strategy',
    'FRNS': 'coolabah-short-term-income-fund-managed-fund',
    'YLDX': 'coolabah-global-floating-rate-high-yield-fund',
}

# JPMorgan: fund detail URLs require the ISIN appended to the name slug
_JPMORGAN_ISINS = {
    'JPGB': 'au0000302440',
    'JPIE': 'au0000282345',
}

# Schroders: URL uses the ASX code (lowercase) under a per-fund category segment
_SCHRODERS_CATEGORIES = {
    'HIGH': 'active-etf',
    'PAYS': 'fixed-income',
}


def scrape_cxa_issuers(db_path=None) -> int:
    """
    Enrich CXA ETFs with distribution_yield scraped from issuer websites.
    Targets Coolabah, PIMCO, JPMorgan, Janus Henderson, and Schroders.
    PIMCO/JPMorgan/Janus Henderson/Schroders load fund data dynamically so
    will typically return no yield; Coolabah embeds yield in static HTML.
    Logs failures without crashing.
    """
    started = datetime.utcnow()
    source = 'cxa_issuers'
    updated = 0

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.warning("beautifulsoup4 required for CXA issuer scrapers")
        return 0

    conn = get_connection(db_path)

    rows = conn.execute(
        "SELECT code, name, issuer FROM etfs WHERE exchange = 'CXA' AND issuer IS NOT NULL"
    ).fetchall()

    if not rows:
        logger.info("CXA issuers: no CXA ETFs in DB")
        conn.close()
        return 0

    # Group by issuer
    by_issuer: dict[str, list[dict]] = {}
    for row in rows:
        issuer = row['issuer']
        if issuer not in by_issuer:
            by_issuer[issuer] = []
        by_issuer[issuer].append({'code': row['code'], 'name': row['name'] or ''})

    # --- Per-issuer URL builders ---

    def _coolabah_url(code, name):
        slug = _COOLABAH_SLUGS.get(code) or _make_slug(name)
        return f"https://coolabahcapital.com/{slug}/"

    def _pimco_url(code, name):
        # Replace "Active ETF" suffix with "Fund" (keep "PIMCO " prefix in slug)
        slug_name = re.sub(r'\s+active\s+etf\s*$', ' Fund', name, flags=re.I)
        slug = _make_slug(slug_name)
        return f"https://www.pimco.com/au/en/investments/etf/{slug}/ausetf-aud"

    def _jpmorgan_url(code, name):
        isin = _JPMORGAN_ISINS.get(code)
        if not isin:
            return None
        slug = _make_slug(name)
        return f"https://am.jpmorgan.com/au/en/asset-management/adv/products/{slug}-{isin}"

    def _janus_url(code, name):
        # Strip "Janus Henderson " prefix from the name slug
        slug_name = re.sub(r'^janus\s+henderson\s+', '', name, flags=re.I)
        slug = _make_slug(slug_name)
        return f"https://www.janushenderson.com/en-au/adviser/{slug}/"

    def _schroders_url(code, name):
        category = _SCHRODERS_CATEGORIES.get(code, 'active-etf')
        return f"https://www.schroders.com/en-au/au/adviser/funds/{category}/{code.lower()}/"

    issuer_configs = [
        ('Coolabah', _coolabah_url),
        ('PIMCO', _pimco_url),
        ('JPMorgan', _jpmorgan_url),
        ('Janus Henderson', _janus_url),
        ('Schroders', _schroders_url),
    ]

    for issuer_name, url_fn in issuer_configs:
        funds = by_issuer.get(issuer_name, [])
        if not funds:
            continue

        logger.info(f"CXA issuers: scraping {issuer_name} ({len(funds)} funds)")
        for fund in funds:
            code = fund['code']
            name = fund['name']
            try:
                url = url_fn(code, name)
                if not url:
                    logger.debug(f"  {code}: no URL available, skipping")
                    continue

                resp = fetch(url)
                if not resp or resp.status_code >= 400:
                    logger.debug(f"  {code}: no usable response from {url}")
                    continue

                soup = BeautifulSoup(resp.text, 'html.parser')
                dist_yield = _extract_distribution_yield(soup, resp.text)

                if dist_yield is not None:
                    upsert_etf(conn, {
                        'code': code,
                        'distribution_yield': dist_yield,
                        'data_source': f'cxa_{issuer_name.lower().replace(" ", "_")}',
                    })
                    updated += 1
                    logger.info(f"  {code}: distribution_yield={dist_yield}")
                else:
                    logger.debug(f"  {code}: distribution yield not found at {url}")

            except Exception as e:
                logger.warning(f"CXA issuers: error scraping {issuer_name} {code}: {e}")

        conn.commit()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f"CXA issuers: updated {updated} ETFs with distribution yield")
    return updated


# ====================================================================
# Dimensional (DFA)
# ====================================================================

_DIMENSIONAL_PDF_URL = 'https://dimensionaltools.blob.core.windows.net/etf/dual-access/{code}.pdf'

# Patterns stripped from the end of security names before matching
# e.g. "ALPHABET INC-CL A" → "ALPHABET INC", "NESTLE SA-REG" → "NESTLE SA"
_NAME_SUFFIX_RE = re.compile(
    r'\s*[-/]\s*('
    r'CL\s*[A-Z\d]|CLASS\s*[A-Z\d]|[A-Z]\s+SHS|SHS\s+[A-Z]|'
    r'REG|GENUSSCHEIN|CVA|ADR|GDR|CDI|PREF|ORD|'
    r'THE|AU|NZ|US|UK'
    r')\s*$',
    re.IGNORECASE,
)
_PUNCT_RE  = re.compile(r"[^\w\s]")
_SPACE_RE  = re.compile(r"\s+")


def _norm_name(name: str) -> str:
    """Normalise a security name for fuzzy matching across data sources."""
    n = name.upper().strip()
    # Strip trailing share-class / market suffixes (up to 3 passes for stacked suffixes)
    for _ in range(3):
        prev = n
        n = _NAME_SUFFIX_RE.sub('', n).strip()
        if n == prev:
            break
    n = _PUNCT_RE.sub(' ', n)
    n = _SPACE_RE.sub(' ', n).strip()
    return n


def infer_dimensional_sectors(db_path=None) -> int:
    """
    For Dimensional (DFA) ETF holdings that have no sector, look up the security
    name in other ETFs' holdings where sector IS NOT NULL and copy the sector.

    Matching strategy (tried in order):
      1. Exact normalised name match
      2. First-3-word prefix match (catches truncated / abbreviated names)

    Returns the number of holdings rows updated.
    """
    conn = get_connection(db_path)

    dfa_codes = [r['code'] for r in conn.execute(
        "SELECT code FROM etfs WHERE issuer = 'DFA'"
    ).fetchall()]
    if not dfa_codes:
        conn.close()
        return 0

    ph = ','.join('?' * len(dfa_codes))

    # ── Build sector lookup from non-DFA holdings ──
    # Load all (name, sector) pairs from non-DFA ETFs that have a sector.
    # For each normalised name keep the most frequently assigned sector.
    from collections import Counter
    sector_freq: dict[str, Counter] = {}

    for name, sector in conn.execute(
        f"SELECT name, sector FROM etf_holdings "
        f"WHERE sector IS NOT NULL AND etf_code NOT IN ({ph})",
        dfa_codes,
    ).fetchall():
        norm = _norm_name(name)
        if norm not in sector_freq:
            sector_freq[norm] = Counter()
        sector_freq[norm][sector] += 1

    # Also build a prefix lookup: first 3 words → Counter
    prefix_sector: dict[str, Counter] = {}
    for norm, ctr in sector_freq.items():
        prefix = ' '.join(norm.split()[:3])
        if prefix not in prefix_sector:
            prefix_sector[prefix] = Counter()
        prefix_sector[prefix].update(ctr)

    # ── Fetch DFA holdings missing sectors ──
    missing = conn.execute(
        f"SELECT id, name FROM etf_holdings "
        f"WHERE etf_code IN ({ph}) AND sector IS NULL",
        dfa_codes,
    ).fetchall()

    updates: list[tuple[str, int]] = []
    for row_id, name in missing:
        norm = _norm_name(name)

        # 1. Exact normalised match
        if norm in sector_freq:
            sector = sector_freq[norm].most_common(1)[0][0]
            updates.append((sector, row_id))
            continue

        # 2. First-3-word prefix match
        prefix = ' '.join(norm.split()[:3])
        if len(prefix.split()) >= 3 and prefix in prefix_sector:
            sector = prefix_sector[prefix].most_common(1)[0][0]
            updates.append((sector, row_id))

    if updates:
        conn.executemany(
            "UPDATE etf_holdings SET sector = ? WHERE id = ?", updates
        )
        conn.commit()

    conn.close()
    logger.info(f"Dimensional sector inference: {len(updates)}/{len(missing)} holdings assigned")
    return len(updates)

# ISO currency → country (best-effort; EUR covers many countries)
_CURRENCY_COUNTRY = {
    'AUD': 'Australia', 'USD': 'United States', 'JPY': 'Japan',
    'GBP': 'United Kingdom', 'EUR': 'Europe', 'CAD': 'Canada',
    'CHF': 'Switzerland', 'SEK': 'Sweden', 'NOK': 'Norway',
    'DKK': 'Denmark', 'HKD': 'Hong Kong', 'SGD': 'Singapore',
    'NZD': 'New Zealand', 'KRW': 'South Korea', 'TWD': 'Taiwan',
    'INR': 'India', 'BRL': 'Brazil', 'ZAR': 'South Africa',
    'MXN': 'Mexico', 'IDR': 'Indonesia', 'MYR': 'Malaysia',
    'THB': 'Thailand', 'PHP': 'Philippines', 'PLN': 'Poland',
    'HUF': 'Hungary', 'CZK': 'Czech Republic', 'ILS': 'Israel',
    'TRY': 'Turkey', 'CLP': 'Chile', 'COP': 'Colombia',
    'PEN': 'Peru', 'ARS': 'Argentina', 'QAR': 'Qatar',
    'SAR': 'Saudi Arabia', 'AED': 'UAE', 'EGP': 'Egypt',
}

def _parse_dimensional_pdf(content: bytes) -> list[dict]:
    """
    Parse a Dimensional pricing-basket PDF.
    Each data line format: NAME [CURRENCY] WEIGHT%
    e.g. 'BHP GROUP LTD AUD 10.107%'
    Returns list of dicts: {name, weight_pct, country}.
    """
    import pdfplumber
    import io

    holdings = []
    in_data = False
    line_pattern = re.compile(r'^(.+?)\s+([A-Z]{3})\s+([\d.]+)%\s*$')

    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ''
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                # Detect start of data section
                if 'Name of Security' in line:
                    in_data = True
                    continue
                if not in_data:
                    continue
                # Stop at footer disclaimer
                if line.startswith('* Excludes') or line.startswith('This report'):
                    in_data = False
                    continue
                m = line_pattern.match(line)
                if not m:
                    continue
                name, currency, weight_str = m.group(1).strip(), m.group(2), m.group(3)
                weight = _safe_float(weight_str)
                if weight is None or weight < 0:
                    continue
                holdings.append({
                    'name':       name,
                    'ticker':     None,
                    'weight_pct': weight,
                    'sector':     None,
                    'country':    _CURRENCY_COUNTRY.get(currency),
                })
    return holdings


def scrape_dimensional(db_path=None) -> int:
    """
    Scrape Dimensional (DFA) ETF holdings from their daily pricing-basket PDFs.
    URL pattern: https://dimensionaltools.blob.core.windows.net/etf/dual-access/{CODE}.pdf
    """
    started = datetime.utcnow()
    source = 'dimensional'
    updated = 0
    holdings_count = 0

    conn = get_connection(db_path)

    # All DFA ETFs in the database
    rows = conn.execute(
        "SELECT code FROM etfs WHERE issuer = 'DFA' ORDER BY code"
    ).fetchall()
    codes = [r['code'] for r in rows]

    for code in codes:
        url = _DIMENSIONAL_PDF_URL.format(code=code)
        try:
            resp = fetch(url)
            if not resp or resp.status_code != 200:
                logger.warning(f"Dimensional: PDF not found for {code} ({url})")
                continue

            holdings = _parse_dimensional_pdf(resp.content)
            if not holdings:
                logger.warning(f"Dimensional: no holdings parsed from {code} PDF")
                continue

            # Merge duplicate names (e.g. multiple share classes of same company)
            merged: dict[str, dict] = {}
            for h in holdings:
                key = h['name']
                if key in merged:
                    merged[key]['weight_pct'] = (merged[key]['weight_pct'] or 0) + (h['weight_pct'] or 0)
                else:
                    merged[key] = h

            upsert_holdings(conn, code, list(merged.values()), commit=False)
            conn.commit()
            holdings_count += len(holdings)
            updated += 1
            logger.info(f"Dimensional: {code} — {len(holdings)} holdings")

        except Exception as e:
            logger.warning(f"Dimensional: error scraping {code}: {e}")
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f"Dimensional: updated {updated} ETFs, {holdings_count} holdings total")

    if updated:
        infer_dimensional_sectors(db_path)

    return updated


# ====================================================================
# Macquarie
# ====================================================================

# Maps ASX code → (internal fund code, URL-encoded fund name, file type)
# File type is 'PCF' (PDF) or 'DPB' (CSV)
_MACQUARIE_FUNDS = {
    'MQAE': ('ACEETF', 'Macquarie%20Core%20Australian%20Equity%20Active%20ETF', 'PCF'),
    'MQEG': ('CGEETF', 'Macquarie%20Core%20Global%20Equity%20Active%20ETF', 'PCF'),
    'MQDB': ('DBFETF', 'Macquarie%20Dynamic%20Bond%20Active%20ETF', 'PCF'),
    'MQIO': ('MIOETF', 'Macquarie%20Income%20Opportunities%20Active%20ETF', 'PCF'),
    'MQSD': ('SUBETF', 'Macquarie%20Subordinated%20Debt%20Active%20ETF', 'PCF'),
    'MQYM': ('GHYETF', 'Macquarie%20Global%20Yield%20Maximiser%20Active%20ETF', 'PCF'),
    'MQWS': ('MWSETF', 'Macquarie%20Walter%20Scott%20Global%20Equity%20Active%20ETF', 'DPB'),
    'MQHG': ('CGHETF', 'Macquarie%20Core%20Global%20Equity%20(Hedged)%20Active%20ETF', 'PCF'),
}
_MACQUARIE_BASE = 'https://etf.macquarie.com/assets/etf/data/fund'


def _parse_macquarie_pcf_pdf(content: bytes) -> list[dict]:
    """Parse a Macquarie PCF PDF. Returns list of holding dicts."""
    try:
        import pdfplumber
    except ImportError:
        logger.error('pdfplumber not installed — pip install pdfplumber')
        return []

    holdings = []
    # Column indices discovered from the header page
    name_col = ticker_col = country_col = wgt_col = None

    try:
        import io as _io
        pdf = pdfplumber.open(_io.BytesIO(content))
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue

            first_row = [str(c or '').strip() for c in table[0]]

            # Detect header row (first row of first data page)
            if 'Short Security Description' in first_row:
                name_col = first_row.index('Short Security Description')
                ticker_col = first_row.index('Security Ticker')
                country_col = first_row.index('Trade Country')
                wgt_col = first_row.index('WGT')
                data_rows = table[1:]  # skip header
            else:
                # Continuation page — use previously found column indices
                if name_col is None:
                    continue
                data_rows = table  # all rows are data

            for row in data_rows:
                if len(row) <= wgt_col:
                    continue
                name = str(row[name_col] or '').strip()
                if not name:
                    continue
                ticker = str(row[ticker_col] or '').strip() or None
                country = str(row[country_col] or '').strip() or None
                weight = _safe_float(row[wgt_col])
                holdings.append({
                    'name': name,
                    'ticker': ticker,
                    'weight_pct': weight,
                    'country': country,
                })
    except Exception as e:
        logger.warning(f'Macquarie PDF parse error: {e}')
    return holdings


def _parse_macquarie_dpb_csv(content: str) -> list[dict]:
    """Parse a Macquarie DPB CSV. Returns list of holding dicts."""
    import csv as _csv
    import io as _io

    holdings = []
    try:
        reader = _csv.DictReader(_io.StringIO(content))
        for row in reader:
            name = (row.get('Short Security Description') or '').strip()
            if not name:
                continue
            ticker = (row.get('Security Ticker') or '').strip() or None
            country = (row.get('Trade Country') or '').strip() or None
            weight = _safe_float(row.get('WGT'))
            holdings.append({
                'name': name,
                'ticker': ticker,
                'weight_pct': weight,
                'country': country,
            })
    except Exception as e:
        logger.warning(f'Macquarie DPB CSV parse error: {e}')
    return holdings


def scrape_macquarie(db_path=None) -> int:
    """
    Scrape Macquarie ETF holdings from their daily PCF PDFs and DPB CSVs.
    PCF URL: https://etf.macquarie.com/assets/etf/data/fund/{FundCode}/{Name}_PCF.pdf
    DPB URL: https://etf.macquarie.com/assets/etf/data/fund/{FundCode}/{Name}_DPB.csv
    """
    started = datetime.utcnow()
    source = 'macquarie'
    updated = 0

    conn = get_connection(db_path)

    for asx_code, (fund_code, name_encoded, file_type) in _MACQUARIE_FUNDS.items():
        ext = 'pdf' if file_type == 'PCF' else 'csv'
        url = f'{_MACQUARIE_BASE}/{fund_code}/{name_encoded}_{file_type}.{ext}'
        try:
            resp = fetch(url)
            if not resp or resp.status_code != 200:
                logger.warning(f'Macquarie: no data for {asx_code} ({url})')
                continue

            if file_type == 'PCF':
                holdings = _parse_macquarie_pcf_pdf(resp.content)
            else:
                holdings = _parse_macquarie_dpb_csv(resp.text)

            if not holdings:
                logger.warning(f'Macquarie: no holdings parsed for {asx_code}')
                continue

            # Merge duplicate names (same security, different share classes)
            merged: dict[str, dict] = {}
            for h in holdings:
                key = h['name']
                if key in merged:
                    merged[key]['weight_pct'] = (merged[key]['weight_pct'] or 0) + (h['weight_pct'] or 0)
                else:
                    merged[key] = h

            upsert_holdings(conn, asx_code, list(merged.values()))
            updated += 1
            logger.info(f'Macquarie: {asx_code} — {len(merged)} holdings')

        except Exception as e:
            logger.warning(f'Macquarie: error scraping {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Macquarie: updated {updated} ETFs')
    return updated


def scrape_macquarie_metadata(db_path=None) -> int:
    """
    Scrape benchmark and investment objective (summary) from Macquarie ETF
    fund pages. The pages use a static <dl>/<dt>/<dd> structure in a
    "Fund facts" section — no JavaScript rendering required.
    """
    started = datetime.utcnow()
    source = 'macquarie_metadata'
    updated = 0

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.warning('beautifulsoup4 required for Macquarie metadata scraper')
        return 0

    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT code, issuer_url FROM etfs WHERE issuer = 'Macquarie' AND issuer_url IS NOT NULL"
    ).fetchall()

    for row in rows:
        code = row['code']
        url = row['issuer_url']
        try:
            resp = fetch(url)
            if not resp or resp.status_code != 200:
                logger.warning(f'Macquarie metadata: no response for {code} ({url})')
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')

            # Macquarie fund pages use a <table> with <b>Label</b> in the first
            # <td> and the value in the second <td> of each row.
            table_data: dict[str, str] = {}
            for tr in soup.find_all('tr'):
                cells = tr.find_all(['td', 'th'])
                if len(cells) >= 2:
                    key = cells[0].get_text(separator=' ', strip=True).lower()
                    val = cells[1].get_text(separator=' ', strip=True)
                    if key and val:
                        table_data[key] = val

            benchmark = table_data.get('benchmark') or None
            objective = table_data.get('investment objective') or None

            if not benchmark and not objective:
                logger.debug(f'Macquarie metadata: no data found for {code}')
                continue

            update = {'code': code}
            if benchmark:
                update['benchmark'] = benchmark
            if objective:
                update['summary'] = objective

            upsert_etf(conn, update)
            conn.commit()
            updated += 1
            logger.info(f'Macquarie metadata: {code} benchmark={benchmark!r}')

        except Exception as e:
            logger.warning(f'Macquarie metadata: error scraping {code}: {e}')

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Macquarie metadata: updated {updated} ETFs')
    return updated


# ====================================================================
# Dimensional (DFA) — static metadata
# ====================================================================
# Dimensional's fund pages are fully JavaScript-rendered; benchmark and
# summary are hard-coded from their PDSs and fund factsheets.

_DFA_METADATA = {
    'DACE': {
        'benchmark': 'S&P/ASX 300 Accumulation Index',
        'summary': (
            'Seeks long-term capital appreciation and income through broadly diversified '
            'exposure to Australian equities. Uses a systematic, factor-based approach '
            'targeting the size, relative price, and profitability premiums.'
        ),
    },
    'DAVA': {
        'benchmark': 'S&P/ASX 200 Index',
        'summary': (
            'Seeks long-term capital appreciation by investing in Australian equities with '
            'a strong value tilt, targeting companies with lower relative prices and higher '
            'profitability characteristics.'
        ),
    },
    'DFGH': {
        'benchmark': 'MSCI World ex Australia Index (AUD Hedged)',
        'summary': (
            'Provides broadly diversified exposure to developed-market equities excluding '
            'Australia, with AUD currency hedging. Uses a systematic, factor-based approach '
            'targeting the size, value, and profitability premiums.'
        ),
    },
    'DGCE': {
        'benchmark': 'MSCI World ex Australia Index (AUD)',
        'summary': (
            'Provides broadly diversified exposure to developed-market equities excluding '
            'Australia in unhedged AUD terms. Uses a systematic, factor-based approach '
            'targeting the size, value, and profitability premiums.'
        ),
    },
    'DGSM': {
        'benchmark': 'MSCI World Small Cap Index (AUD)',
        'summary': (
            'Seeks long-term capital appreciation through diversified exposure to small-cap '
            'equities across global developed markets. Emphasises the small, value, and '
            'profitability premiums to target higher expected returns.'
        ),
    },
    'DGVA': {
        'benchmark': 'MSCI World Index (AUD)',
        'summary': (
            'Seeks long-term capital appreciation by investing in global developed-market '
            'equities with a strong value tilt, targeting companies with lower relative '
            'prices and higher profitability characteristics.'
        ),
    },
}


def scrape_dfa_metadata(db_path=None) -> int:
    """Apply hard-coded benchmark and summary for Dimensional (DFA) ETFs."""
    started = datetime.utcnow()
    source = 'dfa_metadata'
    updated = 0
    conn = get_connection(db_path)

    for code, meta in _DFA_METADATA.items():
        try:
            upsert_etf(conn, {'code': code, **meta})
            updated += 1
            logger.info(f'DFA metadata: {code} — benchmark={meta["benchmark"]!r}')
        except Exception as e:
            logger.warning(f'DFA metadata: error updating {code}: {e}')

    conn.commit()
    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'DFA metadata: updated {updated} ETFs')
    return updated


# ====================================================================
# JPMorgan — static metadata
# ====================================================================
# JPMorgan's fund pages load data via JavaScript; benchmark and summary
# are hard-coded from their fund factsheets and PDSs.

_JPMORGAN_METADATA = {
    # ASX-listed (issuer = 'JPMorgan')
    'JEPI': {
        'benchmark': 'S&P 500 Index (AUD Unhedged)',
        'summary': (
            'Seeks to deliver monthly income and lower volatility than the S&P 500 by '
            'investing in large-cap US equities and writing out-of-the-money covered call '
            'options via equity-linked notes.'
        ),
    },
    'JHPI': {
        'benchmark': 'S&P 500 Index (AUD Hedged)',
        'summary': (
            'Seeks to deliver monthly income and lower volatility by investing in large-cap '
            'US equities and writing covered call options, with AUD currency hedging.'
        ),
    },
    'JPEQ': {
        'benchmark': 'Nasdaq-100 Index (AUD Unhedged)',
        'summary': (
            'Seeks to deliver monthly income with lower volatility than the Nasdaq-100 by '
            'investing in large-cap US technology-oriented equities and writing covered call '
            'options via equity-linked notes.'
        ),
    },
    'JPHQ': {
        'benchmark': 'Nasdaq-100 Index (AUD Hedged)',
        'summary': (
            'Seeks to deliver monthly income with lower volatility than the Nasdaq-100 by '
            'investing in large-cap US technology-oriented equities and writing covered call '
            'options, with AUD currency hedging.'
        ),
    },
    'JREG': {
        'benchmark': 'MSCI World Index (AUD Unhedged)',
        'summary': (
            'Targets modest outperformance of the MSCI World Index by combining broad '
            'global equity diversification with systematic, research-driven tilts toward '
            'securities with the most favourable return prospects.'
        ),
    },
    'JRHG': {
        'benchmark': 'MSCI World Index (AUD Hedged)',
        'summary': (
            'Targets modest outperformance of the MSCI World Index through systematic, '
            'research-driven security selection across global developed markets, with AUD '
            'currency hedging.'
        ),
    },
    'JEME': {
        'benchmark': 'MSCI Emerging Markets Index (AUD Unhedged)',
        'summary': (
            'Targets modest outperformance of the MSCI Emerging Markets Index by combining '
            'broad diversification with systematic, research-driven tilts toward '
            'higher-quality emerging market equities.'
        ),
    },
    'JGLO': {
        'benchmark': 'MSCI ACWI (AUD Unhedged)',
        'summary': (
            'Concentrated, high-conviction portfolio of 50–90 global equities selected for '
            'their long-term growth potential through fundamental, bottom-up research across '
            'developed and emerging markets.'
        ),
    },
    'JHLO': {
        'benchmark': 'MSCI ACWI (AUD Hedged)',
        'summary': (
            'Concentrated, high-conviction portfolio of global equities selected for their '
            'long-term growth potential through fundamental, bottom-up research, with AUD '
            'currency hedging.'
        ),
    },
    'T3MP': {
        'benchmark': 'MSCI ACWI (AUD Unhedged)',
        'summary': (
            'Invests in global companies developing solutions to climate change, including '
            'clean energy, energy efficiency, sustainable transportation, and water '
            'management. Actively managed with a high-conviction, concentrated approach.'
        ),
    },
    'JPGB': {
        'benchmark': 'Bloomberg Global Aggregate Index (AUD Hedged)',
        'summary': (
            'Actively managed global investment-grade fixed income ETF seeking income and '
            'capital preservation by investing across government and corporate bonds '
            'worldwide, hedged to AUD.'
        ),
    },
    'JPIE': {
        'benchmark': 'Bloomberg Global Aggregate Index (AUD Hedged)',
        'summary': (
            'Seeks above-benchmark income by investing across a broad range of fixed income '
            'sectors including high yield, securitised assets, and emerging market debt, '
            'hedged to AUD.'
        ),
    },
    # ASX-listed (issuer = 'JPMAM / Perpetual')
    'JEGA': {
        'benchmark': 'MSCI World Index (AUD Unhedged)',
        'summary': (
            'Complex ETF seeking monthly income through dividends and option premiums with '
            'lower equity market volatility, by investing in a diversified portfolio of '
            'global equities and systematically writing covered call options.'
        ),
    },
    'JHGA': {
        'benchmark': 'MSCI World Index (AUD Hedged)',
        'summary': (
            'Complex ETF seeking monthly income through dividends and option premiums with '
            'lower equity market volatility, investing in global equities and writing covered '
            'call options with AUD currency hedging.'
        ),
    },
}


def scrape_jpmorgan_metadata(db_path=None) -> int:
    """Apply hard-coded benchmark and summary for JPMorgan ETFs."""
    started = datetime.utcnow()
    source = 'jpmorgan_metadata'
    updated = 0
    conn = get_connection(db_path)

    for code, meta in _JPMORGAN_METADATA.items():
        try:
            upsert_etf(conn, {'code': code, **meta})
            updated += 1
            logger.info(f'JPMorgan metadata: {code} — benchmark={meta["benchmark"]!r}')
        except Exception as e:
            logger.warning(f'JPMorgan metadata: error updating {code}: {e}')

    conn.commit()
    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'JPMorgan metadata: updated {updated} ETFs')
    return updated


# ====================================================================
# Schroders
# ====================================================================

# Maps ASX code → (API code, has_security_names)
# has_security_names: True = individual holdings; False = sector/country only
_SCHRODERS_FUNDS = {
    'ALPH': ('ALPH', False),  # country/GICS breakdown only
    'CORE': ('CORE', True),   # individual holdings with Short Security Description
    'GROW': ('GROW', True),
    'HIGH': ('HIGH', False),  # category breakdown only (bond fund)
    'PAYS': ('PAYS', True),
}
_SCHRODERS_PDF_URL = 'https://api.schroders.com/document-store/{code}_Material_Portfolio_Information.pdf'


def _parse_schroders_pdf(content: bytes, has_security_names: bool) -> tuple[list[dict], list[dict]]:
    """
    Parse a Schroders Material Portfolio Information PDF.
    Returns (holdings, sectors).
    """
    try:
        import pdfplumber
    except ImportError:
        logger.error('pdfplumber not installed — pip install pdfplumber')
        return [], []

    holdings = []
    sectors = []

    try:
        import io as _io
        pdf = pdfplumber.open(_io.BytesIO(content))
        for page in pdf.pages:
            table = page.extract_table()
            if not table or len(table) < 2:
                continue
            header = [str(h or '').strip() for h in table[0]]

            if has_security_names and 'Short Security Description' in header:
                name_col = header.index('Short Security Description')
                wgt_col = header.index('WGT') if 'WGT' in header else None
                country_col = header.index('Country Description') if 'Country Description' in header else None
                sector_col = header.index('GICS Industry Group') if 'GICS Industry Group' in header else None

                for row in table[1:]:
                    if len(row) <= name_col:
                        continue
                    name = str(row[name_col] or '').strip()
                    if not name:
                        continue
                    weight = _safe_float(row[wgt_col]) if wgt_col is not None else None
                    country = str(row[country_col] or '').strip() or None if country_col is not None else None
                    sector = str(row[sector_col] or '').strip() or None if sector_col is not None else None
                    holdings.append({
                        'name': name,
                        'ticker': None,
                        'weight_pct': weight,
                        'country': country,
                        'sector': sector,
                    })

            elif 'GICS Industry Group' in header or 'Category' in header:
                # Sector/category-level breakdown
                wgt_col = header.index('WGT') if 'WGT' in header else None
                sector_col = (header.index('GICS Industry Group') if 'GICS Industry Group' in header
                              else header.index('Category') if 'Category' in header else None)
                if sector_col is None or wgt_col is None:
                    continue
                for row in table[1:]:
                    if len(row) <= max(sector_col, wgt_col):
                        continue
                    sector = str(row[sector_col] or '').strip()
                    weight = _safe_float(row[wgt_col])
                    if sector and weight:
                        sectors.append({'sector': sector, 'weight_pct': weight})

    except Exception as e:
        logger.warning(f'Schroders PDF parse error: {e}')

    return holdings, sectors


def scrape_schroders(db_path=None) -> int:
    """
    Scrape Schroders ETF holdings from their Material Portfolio Information PDFs.
    URL: https://api.schroders.com/document-store/{CODE}_Material_Portfolio_Information.pdf
    """
    started = datetime.utcnow()
    source = 'schroders'
    updated = 0

    conn = get_connection(db_path)

    for asx_code, (api_code, has_security_names) in _SCHRODERS_FUNDS.items():
        url = _SCHRODERS_PDF_URL.format(code=api_code)
        try:
            resp = fetch(url)
            if not resp or resp.status_code != 200:
                logger.warning(f'Schroders: no data for {asx_code}')
                continue

            holdings, sectors = _parse_schroders_pdf(resp.content, has_security_names)

            if holdings:
                # Merge duplicate names
                merged: dict[str, dict] = {}
                for h in holdings:
                    key = h['name']
                    if key in merged:
                        merged[key]['weight_pct'] = (merged[key]['weight_pct'] or 0) + (h['weight_pct'] or 0)
                    else:
                        merged[key] = h
                upsert_holdings(conn, asx_code, list(merged.values()))
                updated += 1
                logger.info(f'Schroders: {asx_code} — {len(merged)} holdings')
            elif sectors:
                # Aggregate duplicate sector names
                agg: dict[str, float] = {}
                for s in sectors:
                    agg[s['sector']] = agg.get(s['sector'], 0) + (s['weight_pct'] or 0)
                deduped = [{'sector': k, 'weight_pct': v} for k, v in agg.items()]
                upsert_sectors(conn, asx_code, deduped)
                updated += 1
                logger.info(f'Schroders: {asx_code} — {len(deduped)} sectors (no individual holdings)')
            else:
                logger.warning(f'Schroders: no data parsed for {asx_code}')

        except Exception as e:
            logger.warning(f'Schroders: error scraping {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Schroders: updated {updated} ETFs')
    return updated


# ====================================================================
# Russell Investments
# ====================================================================

# Maps ASX code → Russell fund-centre URL slug
_RUSSELL_FUNDS = {
    'RDV':  '/au/fund-centre-etf-embed/performance-pricing/exchange-traded-funds/etf/equity-income/rdv-russell-high-dividend-australian-shares',
    'RARI': '/au/fund-centre-etf-embed/performance-pricing/exchange-traded-funds/etf/responsible-investing/rari-russell-australian-responsible-investment',
    'RGOS': '/au/fund-centre-etf-embed/performance-pricing/exchange-traded-funds/etf/responsible-investing/rgos-russell-investments-sustainable-global-opportunities-complex',
    'RCB':  '/au/fund-centre-etf-embed/performance-pricing/exchange-traded-funds/etf/fixed-income/rcb-russell-australian-select-corporate-bond',
    'RGB':  '/au/fund-centre-etf-embed/performance-pricing/exchange-traded-funds/etf/fixed-income/rgb-russell-investments-australian-government-bond',
    'RSM':  '/au/fund-centre-etf-embed/performance-pricing/exchange-traded-funds/etf/fixed-income/rsm-russell-investments-australian-semi-government-bond',
}
_RUSSELL_BASE = 'https://russellinvestments.com'


def _parse_russell_page(html: str) -> list[dict]:
    """
    Extract top-10 holdings embedded as double-escaped JSON in the Russell
    fund-centre embed page.
    Holdings structure: {"Security": "...", "Value": 7.02}
    """
    import html as _html_mod

    # Unescape the double-escaped content: \\&quot; → &quot; → "
    step1 = html.replace('\\&quot;', '"').replace('\\/', '/')
    step2 = _html_mod.unescape(step1)

    # Find the Holdings array using bracket counting
    marker = '"Holdings":['
    idx = step2.find(marker)
    if idx == -1:
        return []

    start = idx + len(marker) - 1  # position of '['
    depth = 0
    end = start
    for i, c in enumerate(step2[start:], start):
        if c == '[':
            depth += 1
        elif c == ']':
            depth -= 1
            if depth == 0:
                end = i
                break

    try:
        holdings_raw = json.loads(step2[start:end + 1])
    except (ValueError, json.JSONDecodeError):
        return []

    holdings = []
    for h in holdings_raw:
        name = str(h.get('Security') or '').strip()
        weight = _safe_float(h.get('Value'))
        if name:
            holdings.append({'name': name, 'ticker': None, 'weight_pct': weight})
    return holdings


def scrape_russell(db_path=None) -> int:
    """
    Scrape Russell Investments ETF top-10 holdings from their fund-centre
    embed pages (embedded as JSON in HTML).
    """
    started = datetime.utcnow()
    source = 'russell'
    updated = 0

    conn = get_connection(db_path)

    for asx_code, slug in _RUSSELL_FUNDS.items():
        url = _RUSSELL_BASE + slug
        try:
            resp = fetch(url)
            if not resp or resp.status_code != 200:
                logger.warning(f'Russell: no data for {asx_code}')
                continue

            holdings = _parse_russell_page(resp.text)
            if not holdings:
                logger.warning(f'Russell: no holdings parsed for {asx_code}')
                continue

            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Russell: {asx_code} — {len(holdings)} holdings')

        except Exception as e:
            logger.warning(f'Russell: error scraping {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Russell: updated {updated} ETFs')
    return updated


# ====================================================================
# Magellan / Airlie
# ====================================================================

# Maps ASX code → (issuer_page_url, holdings_url_pattern_hint)
# The holdings PDF URL is discovered dynamically from the fund page
_MAGELLAN_FUND_PAGES = {
    'MGOC': 'https://www.magellangroup.com.au/funds/magellan-global-fund-open-class-asx-mgoc/',
    'MHG':  'https://www.magellangroup.com.au/funds/magellan-global-equities-fund-currency-hedged-asx-mhg/',
    'OPPT': 'https://www.magellangroup.com.au/funds/magellan-global-opportunities-fund/',
    'MCSI': 'https://www.magellangroup.com.au/funds/magellan-core-infrastructure-cboe-mcsi/',
    'MICH': 'https://www.magellangroup.com.au/funds/magellan-infrastructure-fund-currency-hedged-managed-fund-asx-mich/',
}

# AASF (Airlie) publishes quarterly portfolio disclosures as ASX announcements via weblink
_AASF_REPORTS_PAGE = 'https://www.magellangroup.com.au/funds/airlie-australian-share-fund/reports/'


_MAGELLAN_SKIP_NAMES = frozenset({
    'total', 'total equity position', 'total liquidity',
    'total securities', 'total portfolio', 'total investments',
})


def _parse_magellan_holdings_pdf(content: bytes) -> list[dict]:
    """
    Parse a Magellan quarterly portfolio holdings PDF.
    Handles two formats:
      1. Structured table with 'Security' and 'Weight (%)' columns (MGOC, MHG, OPPT, MICH)
      2. Two-column text layout "Company Name X.X%" (MCSI quarterly announcement)
    """
    try:
        import pdfplumber
    except ImportError:
        logger.error('pdfplumber not installed — pip install pdfplumber')
        return []

    holdings = []
    try:
        import io as _io
        pdf = pdfplumber.open(_io.BytesIO(content))

        for page in pdf.pages:
            table = page.extract_table()
            if table and len(table) > 1:
                header = [str(h or '').strip().lower() for h in table[0]]
                name_col = next((i for i, h in enumerate(header) if 'security' in h), None)
                weight_col = next((i for i, h in enumerate(header) if 'weight' in h), None)
                if name_col is not None:
                    for row in table[1:]:
                        if len(row) <= name_col:
                            continue
                        name = str(row[name_col] or '').strip()
                        if not name:
                            continue
                        # Skip subtotal / aggregate rows
                        if name.lower() in _MAGELLAN_SKIP_NAMES:
                            continue
                        weight = _safe_float(row[weight_col]) if weight_col is not None and len(row) > weight_col else None
                        holdings.append({'name': name, 'ticker': None, 'weight_pct': weight})
                    continue

            # Fallback: text extraction — handles two-column "Name X.X%" layout
            text = page.extract_text() or ''
            # Match "Company Name 1.2%" anywhere in the text (handles both column layouts)
            for m in re.finditer(r'([A-Z][^\d\n]+?)\s+(\d+\.\d+)\s*%', text):
                name = m.group(1).strip()
                weight = _safe_float(m.group(2))
                # Skip header lines and aggregate rows
                if name and weight and not any(skip in name.lower() for skip in
                                               ['security', 'weight', 'fund', 'portfolio', 'listing', 'quarter']) \
                        and name.lower() not in _MAGELLAN_SKIP_NAMES:
                    holdings.append({'name': name, 'ticker': None, 'weight_pct': weight})

    except Exception as e:
        logger.warning(f'Magellan PDF parse error: {e}')

    return holdings


def _parse_aasf_quarterly_pdf(content: bytes) -> list[dict]:
    """
    Parse AASF Quarterly Portfolio Disclosure PDF.
    Two-column layout: "Company Name X.X%  Company Name X.X%" per line.
    Allows digits in company names (e.g. Life360).
    """
    try:
        import pdfplumber, io as _io
    except ImportError:
        logger.error('pdfplumber not installed')
        return []

    _SKIP = {'cash'}
    holdings = []
    seen = set()
    try:
        with pdfplumber.open(_io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ''
                for m in re.finditer(r'([A-Z][^%\n]+?)\s+(\d{1,2}\.\d)\s*%', text):
                    name = m.group(1).strip()
                    weight = _safe_float(m.group(2))
                    if not name or name.lower() in _SKIP or name in seen:
                        continue
                    seen.add(name)
                    holdings.append({'name': name, 'ticker': None, 'weight_pct': weight})
    except Exception as e:
        logger.warning(f'AASF quarterly PDF parse error: {e}')
    return holdings


def scrape_magellan(db_path=None) -> int:
    """
    Scrape Magellan/Airlie ETF holdings from their quarterly portfolio holdings PDFs.
    The PDF URL is discovered dynamically from the fund page.
    """
    started = datetime.utcnow()
    source = 'magellan'
    updated = 0

    conn = get_connection(db_path)

    for asx_code, page_url in _MAGELLAN_FUND_PAGES.items():
        try:
            # Step 1: Load fund page to find latest portfolio holdings PDF link
            page_resp = fetch(page_url)
            if not page_resp or page_resp.status_code != 200:
                logger.warning(f'Magellan: fund page not found for {asx_code} ({page_url})')
                continue

            # Find the most recent portfolio holdings link
            # Pattern: href="/funds/.../docs/fund-updates/...portfolio-holdings.../"
            base = page_url.split('/')[0] + '//' + page_url.split('/')[2]
            links = re.findall(
                r'href="(/[^"]*portfolio-holdings[^"]*)"',
                page_resp.text,
                re.I,
            )
            if not links:
                logger.warning(f'Magellan: no portfolio holdings link found for {asx_code}')
                continue

            # Take the most recently listed link (first match is usually most recent)
            holdings_url = base + links[0]

            # Step 2: Download and parse the PDF
            pdf_resp = fetch(holdings_url)
            if not pdf_resp or pdf_resp.status_code != 200:
                logger.warning(f'Magellan: holdings PDF not found for {asx_code}')
                continue

            holdings = _parse_magellan_holdings_pdf(pdf_resp.content)
            if not holdings:
                logger.warning(f'Magellan: no holdings parsed for {asx_code}')
                continue

            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Magellan: {asx_code} — {len(holdings)} holdings')

        except Exception as e:
            logger.warning(f'Magellan: error scraping {asx_code}: {e}')
            conn.rollback()

    # --- AASF (Airlie Australian Share Fund) ---
    # Holdings are published as quarterly portfolio disclosures via ASX/weblink
    try:
        page_resp = fetch(_AASF_REPORTS_PAGE)
        if page_resp and page_resp.status_code == 200:
            # Find the most recent quarterly portfolio disclosure weblink URL
            m = re.search(
                r'href="(https://wcsecure\.weblink\.com\.au[^"]*headlineid=\d+)"[^>]*>[^<]*quarterly\s+portfolio\s+disclosure',
                page_resp.text,
                re.I,
            )
            if m:
                pdf_url = m.group(1)
                pdf_resp = fetch(pdf_url)
                if pdf_resp and pdf_resp.status_code == 200:
                    holdings = _parse_aasf_quarterly_pdf(pdf_resp.content)
                    if holdings:
                        upsert_holdings(conn, 'AASF', holdings)
                        updated += 1
                        logger.info(f'Magellan: AASF — {len(holdings)} holdings')
                    else:
                        logger.warning('Magellan: AASF — no holdings parsed from quarterly PDF')
                else:
                    logger.warning('Magellan: AASF — quarterly PDF download failed')
            else:
                logger.warning('Magellan: AASF — no quarterly portfolio disclosure link found')
        else:
            logger.warning(f'Magellan: AASF — reports page not accessible')
    except Exception as e:
        logger.warning(f'Magellan: AASF error: {e}')
        conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Magellan: updated {updated} ETFs')
    return updated


# ====================================================================
# Coolabah
# ====================================================================

# Funds that publish a "Top 10 Holdings Report" PDF — these are fetched by
# scraping the fund page to find the latest dated file link.
_COOLABAH_TOP10_SLUGS = {
    'FIXD': 'active-composite-bond-strategy',
    'FRNS': 'coolabah-short-term-income-fund-managed-fund',
}
_COOLABAH_DOWNLOAD_BASE = (
    'https://coolabahcapital.com/wp-content/plugins/helcci_azure_downloader/'
    'helcci_azure_downloader.php?download={path}'
)
_COOLABAH_FUND_PAGE_BASE = 'https://coolabahcapital.com/{slug}/'


def _parse_coolabah_top10_pdf(content: bytes) -> list[dict]:
    """
    Parse a Coolabah 'Top 10 Portfolio Holdings' PDF.
    Format:
      Header lines (fund name, date, exchange code, 'Security Weight (%)')
      Data rows: 'SECURITY_NAME WEIGHT'  — weight is the last numeric token
      Footer lines (company name, ABN, phone, website)
    """
    try:
        import pdfplumber
    except ImportError:
        logger.error('pdfplumber not installed')
        return []

    _SKIP = frozenset({'coolabah', 'abn', '1300', 'info@', 'top 10', 'exchange code',
                       'security weight', 'portfolio holdings'})
    holdings = []
    try:
        import io as _io
        with pdfplumber.open(_io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ''
                in_data = False
                for line in text.split('\n'):
                    line = line.strip()
                    if not line:
                        continue
                    if 'Security' in line and 'Weight' in line:
                        in_data = True
                        continue
                    if not in_data:
                        continue
                    if any(skip in line.lower() for skip in _SKIP):
                        continue
                    # Last token is the weight; everything before is the security name
                    parts = line.rsplit(None, 1)
                    if len(parts) != 2:
                        continue
                    name, raw_weight = parts[0].strip(), parts[1]
                    weight = _safe_float(raw_weight)
                    if name and weight is not None and weight > 0:
                        holdings.append({'name': name, 'ticker': None, 'weight_pct': weight})
    except Exception as e:
        logger.warning(f'Coolabah top-10 PDF parse error: {e}')
    return holdings


def scrape_coolabah(db_path=None) -> int:
    """
    Scrape Coolabah ETF holdings from their 'Top 10 Portfolio Holdings' PDFs.
    The fund page is scraped to discover the latest dated PDF link dynamically.
    Funds without a top-10 report are skipped (no MPI basket data stored).
    """
    import urllib.parse as _urlparse
    started = datetime.utcnow()
    source = 'coolabah'
    updated = 0

    conn = get_connection(db_path)

    for asx_code, slug in _COOLABAH_TOP10_SLUGS.items():
        try:
            # Step 1: scrape fund page to find the latest top-10 holdings PDF path
            page_url = _COOLABAH_FUND_PAGE_BASE.format(slug=slug)
            page_resp = fetch(page_url)
            if not page_resp or page_resp.status_code != 200:
                logger.warning(f'Coolabah: cannot load fund page for {asx_code}')
                continue

            matches = re.findall(
                r'download=([^"\']*Top[^"\']*Holdings[^"\']*\.pdf)',
                page_resp.text, re.IGNORECASE,
            )
            if not matches:
                logger.warning(f'Coolabah: no Top 10 Holdings PDF link found for {asx_code}')
                continue

            # Take the first (most recent) match; URL-decode the path
            pdf_path = _urlparse.unquote(matches[0])
            pdf_url = _COOLABAH_DOWNLOAD_BASE.format(path=_urlparse.quote(pdf_path))

            # Step 2: download and parse
            pdf_resp = fetch(pdf_url, headers={'Referer': page_url})
            if not pdf_resp or pdf_resp.status_code != 200:
                logger.warning(f'Coolabah: could not download Top 10 PDF for {asx_code}')
                continue

            holdings = _parse_coolabah_top10_pdf(pdf_resp.content)
            if not holdings:
                logger.warning(f'Coolabah: no holdings parsed for {asx_code}')
                continue

            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Coolabah: {asx_code} — {len(holdings)} holdings (top-10 report)')

        except Exception as e:
            logger.warning(f'Coolabah: error scraping {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Coolabah: updated {updated} ETFs')
    return updated


# ====================================================================
# Fidelity
# ====================================================================

# Maps ASX code → URL slug on fidelity.com.au/funds/
_FIDELITY_FUNDS = {
    'FHCO': 'fidelity-australian-high-conviction-active-etf',
    'FCAP': 'fidelity-global-future-leaders-active-etf',
    'FEMX': 'fidelity-global-emerging-markets-active-etf',
    'FIIN': 'fidelity-india-active-etf',
    'FASI': 'fidelity-asia-active-etf',
}
_FIDELITY_BASE = 'https://www.fidelity.com.au/funds'


def scrape_fidelity(db_path=None) -> int:
    """
    Scrape Fidelity Australia ETF top-10 holdings and sector allocations
    from their fund pages (data is embedded as static HTML tables/chart data).
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.error('beautifulsoup4 not installed')
        return 0

    started = datetime.utcnow()
    source = 'fidelity'
    updated = 0

    conn = get_connection(db_path)

    for asx_code, slug in _FIDELITY_FUNDS.items():
        url = f'{_FIDELITY_BASE}/{slug}/'
        try:
            resp = fetch(url)
            if not resp or resp.status_code != 200:
                logger.warning(f'Fidelity: no page for {asx_code}')
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')

            # --- Top-10 Holdings ---
            # Div ID is either fund-holdings{CODE} or fund-holdings{numeric_id}
            holding_div = soup.find(id=re.compile(r'fund-holdings'))
            holdings = []
            if holding_div:
                for row in holding_div.find_all('tr'):
                    cols = [td.get_text(strip=True) for td in row.find_all('td')]
                    if len(cols) >= 2 and cols[0]:
                        weight = _safe_float(cols[1].replace('%', ''))
                        holdings.append({'name': cols[0], 'ticker': None, 'weight_pct': weight})

            # --- Sector Allocations (from Highcharts JS variable) ---
            sectors = []
            idx_s = resp.text.find('dataChartindustry =')
            if idx_s == -1:
                idx_s = resp.text.find('dataChartindustry=')
            if idx_s >= 0:
                bracket_start = resp.text.find('[', idx_s)
                if bracket_start >= 0:
                    depth = 0
                    bracket_end = bracket_start
                    for i, c in enumerate(resp.text[bracket_start:], bracket_start):
                        if c == '[':
                            depth += 1
                        elif c == ']':
                            depth -= 1
                            if depth == 0:
                                bracket_end = i
                                break
                    content = resp.text[bracket_start + 1:bracket_end]
                    for m in re.finditer(r"name:\s*['\"]([^'\"]+)['\"].*?y:\s*([\d.]+)", content, re.DOTALL):
                        sectors.append({'sector': m.group(1), 'weight_pct': float(m.group(2))})

            if holdings:
                upsert_holdings(conn, asx_code, holdings)
                updated += 1
                logger.info(f'Fidelity: {asx_code} — {len(holdings)} holdings')
            if sectors:
                upsert_sectors(conn, asx_code, sectors)
                logger.info(f'Fidelity: {asx_code} — {len(sectors)} sectors')

            if not holdings and not sectors:
                logger.warning(f'Fidelity: no data for {asx_code}')

        except Exception as e:
            logger.warning(f'Fidelity: error scraping {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Fidelity: updated {updated} ETFs')
    return updated


# ====================================================================
# Morningstar
# ====================================================================

_MORNINGSTAR_FUNDS = {
    'MSTR': 'https://morningstarinvestments.com.au/holdings/mstr/PCF.csv',
}


def _parse_morningstar_pcf_csv(text: str) -> tuple[list[dict], list[dict]]:
    """
    Parse Morningstar PCF CSV.

    Format:
      Rows 1-9: fund metadata (key,value pairs)
      Row 10: blank
      Row 11: data header (Portfolio_ID, Category_Description, ..., Symbol, Security_Long_Name,
                           ..., Percentage_of_Holdings, ..., Sector_Level_2_Name, Sector_Level_4_Name)
      Row 12+: data rows

    Returns (holdings, sectors) where holdings are aggregated by name
    and sectors aggregate weight by Sector_Level_2_Name (excluding cash/liquidity).
    """
    import csv as _csv_mod

    # The CSV has a metadata section at the top (rows 1-9) with key/value pairs,
    # then a blank row, then the actual data starting with a header containing Portfolio_ID.
    # Skip ahead to find the data header row.
    lines = text.splitlines()
    data_start = None
    for i, line in enumerate(lines):
        if 'Portfolio_ID' in line or 'Security_Long_Name' in line:
            data_start = i
            break
    if data_start is None:
        return [], []

    data_text = '\n'.join(lines[data_start:])
    reader = _csv_mod.DictReader(io.StringIO(data_text))
    holdings_by_name: dict[str, dict] = {}
    sectors_by_name: dict[str, float] = {}

    for row in reader:
        name = (row.get('Security_Long_Name') or '').strip()
        symbol = (row.get('Symbol') or '').strip()
        pct_str = row.get('Percentage_of_Holdings') or ''
        sector = (row.get('Sector_Level_2_Name') or '').strip()
        category = (row.get('Category_Description') or '').strip().lower()

        # Skip cash rows (symbol starts with CASH-)
        if symbol.startswith('CASH-') or category == 'cash' or not name:
            continue

        weight = _safe_float(pct_str)
        if weight is None or weight <= 0:
            continue

        ticker = symbol if symbol and not symbol.startswith('CASH') else None
        if name in holdings_by_name:
            holdings_by_name[name]['weight_pct'] = (holdings_by_name[name]['weight_pct'] or 0) + weight
        else:
            holdings_by_name[name] = {
                'name': name,
                'ticker': ticker,
                'weight_pct': weight,
                'sector': sector or None,
                'country': None,
            }
        if sector and sector.lower() not in ('liquidity', 'cash', ''):
            sectors_by_name[sector] = sectors_by_name.get(sector, 0) + weight

    holdings = list(holdings_by_name.values())
    sectors = [{'sector': s, 'weight_pct': round(w, 6)} for s, w in sectors_by_name.items()]
    return holdings, sectors


def scrape_morningstar(db_path=None) -> int:
    """Scrape Morningstar ETF holdings from their PCF CSV files."""
    started = datetime.utcnow()
    source = 'morningstar'
    updated = 0

    conn = get_connection(db_path)

    for asx_code, url in _MORNINGSTAR_FUNDS.items():
        try:
            resp = fetch(url)
            if not resp or resp.status_code != 200:
                logger.warning(f'Morningstar: no response for {asx_code}')
                continue

            holdings, sectors = _parse_morningstar_pcf_csv(resp.text)
            if not holdings:
                logger.warning(f'Morningstar: no holdings parsed for {asx_code}')
                continue

            upsert_etf(conn, {'code': asx_code, 'issuer': 'Morningstar', 'data_source': 'morningstar'})
            upsert_holdings(conn, asx_code, holdings)
            if sectors:
                upsert_sectors(conn, asx_code, sectors)
            updated += 1
            logger.info(f'Morningstar: {asx_code} — {len(holdings)} holdings, {len(sectors)} sectors')
        except Exception as e:
            logger.warning(f'Morningstar: error scraping {asx_code}: {e}')

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Morningstar: updated {updated} ETFs')
    return updated


# ====================================================================
# Munro / GSFM
# ====================================================================

# Fund pages used to discover the latest portfolio disclosure PDF URL.
# Each page is scraped to find the most recent "Portfolio-Disclosure" PDF link.
_MUNRO_FUND_PAGES = {
    'MAET': 'https://www.munropartners.com/funds/maet-asx/',
}


def _parse_munro_disclosure_pdf(content: bytes) -> list[dict]:
    """
    Parse a Munro monthly portfolio disclosure PDF.

    Format: two-column table on page 1 with entries like:
        NVIDIA CORP  6.79%   TENCENT HOLDINGS LTD  1.84%

    Returns list of {name, ticker, weight_pct}.
    """
    try:
        import pdfplumber as _pdfplumber
        import io as _io
        with _pdfplumber.open(_io.BytesIO(content)) as pdf:
            cell = pdf.pages[0].extract_table()[0][0] or ''
    except Exception as e:
        logger.warning(f'Munro PDF parse error: {e}')
        return []

    holdings = []
    seen: set[str] = set()
    for line in cell.split('\n'):
        if 'SECURITY WEIGHTING' in line or not line.strip():
            continue
        for name, weight_str in re.findall(r'([A-Z][^\d\n]+?)\s+([\d]+\.[\d]+)%', line):
            name = name.strip()
            if not name or 'WEIGHTING' in name or 'IMPORTANT' in name:
                continue
            weight = _safe_float(weight_str)
            if weight is None or weight < 0:
                continue
            if name in seen:
                continue
            seen.add(name)
            holdings.append({'name': name, 'ticker': None, 'weight_pct': weight})
    return holdings


def scrape_munro(db_path=None) -> int:
    """Scrape Munro Partners ETF holdings from their portfolio disclosure PDFs."""
    started = datetime.utcnow()
    source = 'munro'
    updated = 0

    conn = get_connection(db_path)

    for asx_code, page_url in _MUNRO_FUND_PAGES.items():
        try:
            page_resp = fetch(page_url)
            if not page_resp or page_resp.status_code != 200:
                logger.warning(f'Munro: no response for {asx_code} fund page')
                continue

            # Find latest "Portfolio-Disclosure" or "Portfolio_Disclosure" PDF link
            links = re.findall(
                r'href=\"(https://[^\"]*(?:Portfolio.Disclosure|Portfolio_Disclosure)[^\"]*)\"',
                page_resp.text, re.I
            )
            if not links:
                logger.warning(f'Munro: no portfolio disclosure PDF found for {asx_code}')
                continue

            pdf_url = links[0]
            pdf_resp = fetch(pdf_url)
            if not pdf_resp or pdf_resp.status_code != 200:
                logger.warning(f'Munro: no PDF response for {asx_code}')
                continue

            holdings = _parse_munro_disclosure_pdf(pdf_resp.content)
            if not holdings:
                logger.warning(f'Munro: no holdings parsed for {asx_code}')
                continue

            upsert_etf(conn, {'code': asx_code, 'issuer': 'Munro / GSFM', 'data_source': 'munro'})
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Munro: {asx_code} — {len(holdings)} holdings')
        except Exception as e:
            logger.warning(f'Munro: error scraping {asx_code}: {e}')

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Munro: updated {updated} ETFs')
    return updated


# ====================================================================
# ETF Shares
# ====================================================================

_ETFSHARES_FUNDS = {
    'HUGE': 'https://www.etfshares.com.au/products/huge',
    'WWWW': 'https://www.etfshares.com.au/products/wwww',
    'BEST': 'https://www.etfshares.com.au/products/best',
}


def _parse_etfshares_pcf_excel(content: bytes) -> list[dict]:
    """
    Parse an ETF Shares PCF Excel file.

    Structure:
      Rows 0-15: metadata
      Row 16:    header (#, Component Name, Bloomberg Ticker, Position, Currency,
                          Local Price, Market Value (AUD), Weight)
      Row 17+:   data rows; Weight is a decimal fraction (e.g. 0.1224 = 12.24%)
    """
    try:
        import openpyxl
    except ImportError:
        logger.error('openpyxl required for ETF Shares PCF parsing')
        return []

    try:
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    except Exception as e:
        logger.warning(f'ETF Shares Excel parse error: {e}')
        return []

    ws = wb.active
    header_row = None
    col = {}
    holdings = []

    for row in ws.iter_rows(values_only=True):
        if header_row is None:
            # Detect header row: contains 'Component Name'
            if any(str(c or '').strip() == 'Component Name' for c in row):
                header_row = row
                for i, h in enumerate(row):
                    h_clean = str(h or '').strip()
                    if h_clean == 'Component Name':
                        col['name'] = i
                    elif h_clean == 'Bloomberg Ticker':
                        col['ticker'] = i
                    elif h_clean == 'Weight':
                        col['weight'] = i
            continue

        if 'name' not in col or 'weight' not in col:
            continue
        name = str(row[col['name']] or '').strip()
        if not name:
            continue
        weight_raw = row[col['weight']]
        if weight_raw is None:
            continue
        weight = _safe_float(weight_raw)
        if weight is None or weight <= 0:
            continue
        # Weight may be a decimal fraction (0.122) or already a percentage (12.2)
        if weight < 1:
            weight = round(weight * 100, 6)

        ticker_raw = str(row[col.get('ticker', -1)] or '').strip() if col.get('ticker') is not None else None
        # Bloomberg ticker is like "NVDA UW Equity" — extract first word
        ticker = ticker_raw.split()[0] if ticker_raw and ' ' in ticker_raw else ticker_raw or None

        holdings.append({'name': name, 'ticker': ticker, 'weight_pct': weight, 'sector': None, 'country': None})

    wb.close()
    return holdings


def scrape_etfshares(db_path=None) -> int:
    """Scrape ETF Shares (ETFS) PCF Excel files from their product pages."""
    started = datetime.utcnow()
    source = 'etfshares'
    updated = 0

    conn = get_connection(db_path)

    for asx_code, page_url in _ETFSHARES_FUNDS.items():
        try:
            page_resp = fetch(page_url)
            if not page_resp or page_resp.status_code != 200:
                logger.warning(f'ETF Shares: no response for {asx_code} page')
                continue

            # Find the PCF Excel link
            links = re.findall(
                r'href=\"(https://[^\"]*PCF_[^\"]*\.xlsx)\"',
                page_resp.text, re.I
            )
            if not links:
                logger.warning(f'ETF Shares: no PCF Excel found for {asx_code}')
                continue

            xlsx_url = links[0]
            xlsx_resp = fetch(xlsx_url)
            if not xlsx_resp or xlsx_resp.status_code != 200:
                logger.warning(f'ETF Shares: no Excel response for {asx_code}')
                continue

            holdings = _parse_etfshares_pcf_excel(xlsx_resp.content)
            if not holdings:
                logger.warning(f'ETF Shares: no holdings for {asx_code}')
                continue

            upsert_etf(conn, {'code': asx_code, 'issuer': 'ETF Shares', 'data_source': 'etfshares'})
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'ETF Shares: {asx_code} — {len(holdings)} holdings')
        except Exception as e:
            logger.warning(f'ETF Shares: error scraping {asx_code}: {e}')

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'ETF Shares: updated {updated} ETFs')
    return updated


# ====================================================================
# Resolution Capital
# ====================================================================

_RESCAP_FUNDS = {
    'RCAP': 'https://rescap.com/wp-content/uploads/Resolution-Capital-Global-Property-Securities-Fund-Active-ETF-Monthly-Report.pdf',
    'RIIF': 'https://rescap.com/wp-content/uploads/Resolution-Capital-Global-Listed-Infrastructure-Fund-Active-ETF-Monthly-Report.pdf',
}


def _parse_rescap_monthly_pdf(content: bytes) -> tuple[list[dict], list[dict]]:
    """
    Parse a Resolution Capital monthly report PDF.

    Page 3 has three side-by-side tables rendered on a single text line:
        "Top 5 Weights  Top 5 Contributors  Top 5 Detractors"
    Data rows:  "Welltower Inc. 7.88  Sun Hung Kai Properties 0.91  Welltower Inc. -0.29"

    We extract only the FIRST name+weight from each data line (Top 5 Weights column).

    Sector Allocation block on the same page uses the same layout:
        "Retail 18.91  US 52.40"
    We extract only the first sector+weight from each line.

    Returns (holdings, sectors).
    """
    try:
        import pdfplumber as _pdfplumber
        import io as _io
    except ImportError:
        logger.error('pdfplumber required for Resolution Capital PDF parsing')
        return [], []

    holdings: list[dict] = []
    sectors: list[dict] = []

    try:
        with _pdfplumber.open(_io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ''
                in_top5 = False
                in_sector = False
                for line in text.splitlines():
                    line_raw = line.strip()
                    if not line_raw:
                        continue
                    line_lower = line_raw.lower()

                    # Section headers
                    if 'top 5 weights' in line_lower:
                        in_top5 = True
                        in_sector = False
                        continue
                    if re.search(r's\s*ector\s+(?:name|allocation)', line_lower):
                        in_top5 = False
                        in_sector = True
                        continue
                    # End markers
                    if re.search(r'contact\s+us|disclaimer|assigned\s+as\s+of|analyst.driven', line_lower):
                        in_top5 = False
                        in_sector = False
                        continue
                    # Skip column header lines
                    if re.match(r'^security\s+name', line_lower) or re.match(r'^sector\s+name', line_lower) or re.match(r'^region\s+name', line_lower):
                        continue

                    if in_top5:
                        # Three-column layout: "NAME weight  CONTRIB_NAME weight  DETRACT_NAME weight"
                        # Extract first NAME + first weight (positive decimal)
                        m = re.match(r'^(.+?)\s+([\d]+\.[\d]+)\s+', line_raw)
                        if not m:
                            # Last row might only have left column
                            m = re.match(r'^(.+?)\s+([\d]+\.[\d]+)\s*$', line_raw)
                        if m:
                            name = m.group(1).strip()
                            weight = _safe_float(m.group(2))
                            if name and weight and weight > 0 and len(holdings) < 10:
                                holdings.append({
                                    'name': name,
                                    'ticker': None,
                                    'weight_pct': weight,
                                    'sector': None,
                                    'country': None,
                                })
                    elif in_sector:
                        # Three-column layout: "SECTOR weight  REGION weight"
                        # Extract first sector name + weight
                        m = re.match(r'^(.+?)\s+([\d]+\.[\d]+)\s+', line_raw)
                        if not m:
                            m = re.match(r'^(.+?)\s+([\d]+\.[\d]+)\s*$', line_raw)
                        if m:
                            sector = m.group(1).strip()
                            weight = _safe_float(m.group(2))
                            if sector and weight and weight > 0 and sector.lower() not in ('cash', 'cash & hedge', 'total'):
                                sectors.append({'sector': sector, 'weight_pct': weight})
    except Exception as e:
        logger.warning(f'Resolution Capital PDF parse error: {e}')

    return holdings, sectors


def scrape_rescap(db_path=None) -> int:
    """Scrape Resolution Capital ETF holdings from their monthly report PDFs."""
    started = datetime.utcnow()
    source = 'rescap'
    updated = 0

    conn = get_connection(db_path)

    for asx_code, pdf_url in _RESCAP_FUNDS.items():
        try:
            resp = fetch(pdf_url)
            if not resp or resp.status_code != 200:
                logger.warning(f'Resolution Capital: no PDF for {asx_code}')
                continue

            holdings, sectors = _parse_rescap_monthly_pdf(resp.content)
            if not holdings:
                logger.warning(f'Resolution Capital: no holdings parsed for {asx_code}')
                continue

            upsert_etf(conn, {'code': asx_code, 'issuer': 'Resolution / Pinnacle', 'data_source': 'rescap'})
            upsert_holdings(conn, asx_code, holdings)
            if sectors:
                upsert_sectors(conn, asx_code, sectors)
            updated += 1
            logger.info(f'Resolution Capital: {asx_code} — {len(holdings)} holdings, {len(sectors)} sectors')
        except Exception as e:
            logger.warning(f'Resolution Capital: error scraping {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Resolution Capital: updated {updated} ETFs')
    return updated


# ====================================================================
# Hyperion Asset Management
# ====================================================================

# WP REST API page IDs for each fund (from wp-json/wp/v2/pages/{id})
_HYPERION_WP_PAGES = {
    'HYGG': 'https://www.hyperion.com.au/wp-json/wp/v2/pages/10540',
}
# Regex to extract the quarterly holdings PDF URL from WP page content
_HYPERION_HOLDINGS_PDF_RE = re.compile(
    r'href="(https://www\.hyperion\.com\.au/app/uploads/[^"]*quarterly[^"]*holdings[^"]*\.pdf)"',
    re.I,
)


def _parse_hyperion_holdings_pdf(content: bytes) -> list[dict]:
    """
    Parse a Hyperion quarterly portfolio holdings PDF.

    Format (single page):
        Code  Security Name
        AMZN-US  Amazon.com, Inc.
        ...
        (no weights published)

    Returns holdings with weight_pct=None.
    """
    try:
        import pdfplumber as _pdfplumber
        import io as _io
    except ImportError:
        logger.error('pdfplumber required for Hyperion PDF parsing')
        return []

    holdings = []
    in_data = False
    try:
        with _pdfplumber.open(_io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ''
                for line in text.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    # Header line detection
                    if re.match(r'^Code\s+Security\s+Name', line, re.I):
                        in_data = True
                        continue
                    if not in_data:
                        continue
                    # Footer / disclaimer line
                    if re.search(r'as\s+at|subject\s+to\s+change|copyright|hyperion\s+global', line, re.I):
                        break
                    # Data line: "AMZN-US  Amazon.com, Inc."
                    m = re.match(r'^([A-Z0-9]{1,8}-[A-Z]{2})\s+(.+)$', line)
                    if m:
                        ticker = m.group(1).strip()
                        name = m.group(2).strip()
                        if name:
                            holdings.append({
                                'name': name,
                                'ticker': ticker,
                                'weight_pct': None,
                                'sector': None,
                                'country': None,
                            })
    except Exception as e:
        logger.warning(f'Hyperion PDF parse error: {e}')

    return holdings


def scrape_hyperion(db_path=None) -> int:
    """
    Scrape Hyperion Asset Management ETF holdings from quarterly holdings PDFs.
    The PDF URL is discovered dynamically via the WordPress REST API.
    Holdings are published without weights (quarterly disclosure requirement).
    """
    started = datetime.utcnow()
    source = 'hyperion'
    updated = 0

    conn = get_connection(db_path)

    for asx_code, wp_api_url in _HYPERION_WP_PAGES.items():
        try:
            # Step 1: fetch WP REST API page to find the latest quarterly holdings PDF URL
            api_resp = fetch(wp_api_url)
            if not api_resp or api_resp.status_code != 200:
                logger.warning(f'Hyperion: no WP API response for {asx_code}')
                continue

            try:
                import json as _json
                page_data = _json.loads(api_resp.text)
                content_html = page_data.get('content', {}).get('rendered', '')
            except Exception:
                content_html = api_resp.text

            pdf_urls = _HYPERION_HOLDINGS_PDF_RE.findall(content_html)
            if not pdf_urls:
                logger.warning(f'Hyperion: no quarterly holdings PDF link found for {asx_code}')
                continue

            pdf_url = pdf_urls[0]
            logger.info(f'Hyperion: {asx_code} quarterly PDF: {pdf_url}')

            # Step 2: download and parse the PDF
            pdf_resp = fetch(pdf_url)
            if not pdf_resp or pdf_resp.status_code != 200:
                logger.warning(f'Hyperion: PDF download failed for {asx_code}')
                continue

            holdings = _parse_hyperion_holdings_pdf(pdf_resp.content)
            if not holdings:
                logger.warning(f'Hyperion: no holdings parsed for {asx_code}')
                continue

            upsert_etf(conn, {'code': asx_code, 'issuer': 'Hyperion / Pinnacle', 'data_source': 'hyperion'})
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Hyperion: {asx_code} — {len(holdings)} holdings (no weights, quarterly disclosure)')
        except Exception as e:
            logger.warning(f'Hyperion: error scraping {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Hyperion: updated {updated} ETFs')
    return updated


# ====================================================================
# Talaria Capital
# ====================================================================

_TALARIA_FUND_PAGES = {
    'TLRA': 'https://www.talariacapital.com.au/our-funds/talaria-global-equity-fund-complex-etf/',
    'TLRH': 'https://www.talariacapital.com.au/our-funds/talaria-global-equity-fund-currency-hedged-complex-etf/',
}
# Pattern to find quarterly fund update PDF — matches "0738-TAL-Quarterly-DEC25-Managed-FA_2.pdf" style
_TALARIA_QUARTERLY_PDF_RE = re.compile(
    r'href="(/app/uploads/[^"]*(?:quarterly|Quarterly)[^"]*\.pdf)"',
    re.I,
)


def _parse_talaria_quarterly_pdf(content: bytes) -> tuple[list[dict], list[dict]]:
    """
    Parse a Talaria quarterly update PDF.

    Page 10 layout — two side-by-side tables on same text lines:
      Col 1 (Top 10 Holdings):  "Newmont  6.0%  1 month  0.13%  56%"
      Col 2 (Performance):      mixed performance stats on same line

    Holdings are extracted by matching the first "Name  weight%" pattern
    on each data line, ignoring trailing performance data.
    Weight always ends with '%' so the format is: NAME DECIMAL%

    Sector Allocation uses an inverted layout: "22% Health Care"
    (weight% precedes the sector name on same line, or separate lines).

    Returns (holdings, sectors).
    """
    try:
        import pdfplumber as _pdfplumber
        import io as _io
    except ImportError:
        logger.error('pdfplumber required for Talaria PDF parsing')
        return [], []

    holdings: list[dict] = []
    sectors: list[dict] = []

    # Performance keywords that appear on same line as holdings data — skip if first token
    _PERF_SKIP_RE = re.compile(
        r'^\d+\s+(?:month|year|months|years)|^since\s+inception|^p\.a\.|^\d+%$|^table\s+title',
        re.I,
    )
    # Sector patterns: "22% Health Care" (weight first) or "Health Care 22%"
    _SECTOR_LEAD_RE = re.compile(r'^([\d]+)%\s+(.+)$')
    _SECTOR_TRAIL_RE = re.compile(r'^(.+?)\s+([\d]+)%\s*$')

    try:
        with _pdfplumber.open(_io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ''
                if 'top 10' not in text.lower() and 'top 5' not in text.lower():
                    continue

                in_holdings = False
                in_sector = False
                for line in text.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    line_lower = line.lower()

                    # Section detection
                    if re.search(r'top\s+(?:10|5)\s+holdings', line_lower):
                        in_holdings = True
                        in_sector = False
                        continue
                    if re.search(r'sector\s+allocation', line_lower):
                        in_holdings = False
                        in_sector = True
                        continue
                    # End of sections — require the keyword to START the line to avoid
                    # false triggers from mid-line footnotes like "1 Fund Returns are..."
                    if re.match(r'^(?:regional\s+allocation|asset\s+alloc|portfolio\s+contributor|quarterly\s+distribution|fund\s+snapshot)', line_lower):
                        in_holdings = False
                        in_sector = False
                        continue
                    # Skip column header line
                    if re.match(r'^(?:table\s+title|security|name)\s+%', line_lower):
                        continue
                    # Skip footnote lines (start with digit + space + word)
                    if re.match(r'^\d+\s+[a-zA-Z]', line) and in_holdings:
                        continue

                    if in_holdings:
                        # Holdings line: "Newmont 6.0% 1 month 0.13% 56%"
                        # Extract first "NAME  DECIMAL%" — the weight ends with literal %
                        m = re.match(r'^(.+?)\s+([\d]+\.[\d]+)%', line)
                        if m:
                            name = m.group(1).strip()
                            weight = _safe_float(m.group(2))
                            # Reject performance metric lines (e.g., "1 month 0.13%")
                            # Also reject footnote lines that begin with a digit
                            if (name and weight and weight > 0
                                    and not _PERF_SKIP_RE.match(name)
                                    and not re.match(r'^\d', name)
                                    and len(name) >= 2
                                    and len(holdings) < 12):
                                holdings.append({
                                    'name': name,
                                    'ticker': None,
                                    'weight_pct': weight,
                                    'sector': None,
                                    'country': None,
                                })

                    elif in_sector:
                        # Two layouts: "22% Health Care" or "Health Care 22%"
                        # Lines may also have trailing region data (e.g., "20% Cash Japan 3%") — skip complex ones
                        m_lead = _SECTOR_LEAD_RE.match(line)
                        m_trail = _SECTOR_TRAIL_RE.match(line)
                        sector_name = weight_val = None
                        if m_lead:
                            weight_val = _safe_float(m_lead.group(1))
                            rest = m_lead.group(2).strip()
                            # Skip lines with multiple numbers (column-merged regional data)
                            if re.search(r'\d', rest):
                                continue
                            sector_name = rest
                        elif m_trail:
                            sector_name = m_trail.group(1).strip()
                            weight_val = _safe_float(m_trail.group(2))
                            # Skip lines that contain digits inside the sector name (merged columns)
                            if re.search(r'\d', sector_name):
                                continue
                        if (sector_name and weight_val and weight_val > 0
                                and sector_name.lower() not in ('cash', 'total', 'cash – put option cover')
                                and not re.match(r'^\d', sector_name)):
                            sectors.append({'sector': sector_name, 'weight_pct': weight_val})

    except Exception as e:
        logger.warning(f'Talaria PDF parse error: {e}')

    return holdings, sectors


def scrape_talaria(db_path=None) -> int:
    """
    Scrape Talaria Capital ETF holdings from their quarterly update PDFs.
    The PDF URL is discovered from each fund's page on talariacapital.com.au.
    Top 10 holdings with weights + sector allocations are extracted.
    """
    started = datetime.utcnow()
    source = 'talaria'
    updated = 0

    conn = get_connection(db_path)

    for asx_code, page_url in _TALARIA_FUND_PAGES.items():
        try:
            page_resp = fetch(page_url)
            if not page_resp or page_resp.status_code != 200:
                logger.warning(f'Talaria: no fund page for {asx_code}')
                continue

            pdf_paths = _TALARIA_QUARTERLY_PDF_RE.findall(page_resp.text)
            if not pdf_paths:
                logger.warning(f'Talaria: no quarterly PDF link found for {asx_code}')
                continue

            pdf_url = 'https://www.talariacapital.com.au' + pdf_paths[0]
            logger.info(f'Talaria: {asx_code} quarterly PDF: {pdf_url}')

            pdf_resp = fetch(pdf_url)
            if not pdf_resp or pdf_resp.status_code != 200:
                logger.warning(f'Talaria: PDF download failed for {asx_code}')
                continue

            holdings, sectors = _parse_talaria_quarterly_pdf(pdf_resp.content)
            if not holdings:
                logger.warning(f'Talaria: no holdings parsed for {asx_code}')
                continue

            upsert_etf(conn, {'code': asx_code, 'issuer': 'Talaria', 'data_source': 'talaria'})
            upsert_holdings(conn, asx_code, holdings)
            if sectors:
                upsert_sectors(conn, asx_code, sectors)
            updated += 1
            logger.info(f'Talaria: {asx_code} — {len(holdings)} holdings, {len(sectors)} sectors')
        except Exception as e:
            logger.warning(f'Talaria: error scraping {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Talaria: updated {updated} ETFs')
    return updated


# ====================================================================
# Ausbil
# ====================================================================

_AUSBIL_FUNDS = {
    'DIVI': 'https://www.ausbil.com.au/our-products/australian-equities/ausbil-active-dividend-income-fund',
    'GSCF': 'https://www.ausbil.com.au/our-products/global-equities/ausbil-global-smallcap-fund',
    'GHIF': 'https://www.ausbil.com.au/our-products/global-equities/ausbil-global-essential-infrastructure-fund-hedged',
}
# Regex to find the fact sheet PDF link on each fund page (e.g. 2512-Ausbil-...-Fact-Sheet...-DIVI.pdf)
_AUSBIL_FACTSHEET_RE = re.compile(r'/getmedia/[^"]*\d{4}-Ausbil[^"]*Fact[^"]*Sheet[^"]*\.pdf', re.I)


def _parse_ausbil_factsheet_pdf(content: bytes) -> list[dict]:
    """
    Parse an Ausbil fact sheet PDF to extract top 10 stock holdings.
    Uses word-position extraction to isolate the right-column holdings table,
    avoiding bleed-in from the left-column investment description text.
    Handles two table formats:
      1. Three-column: "Name Fund% Index% Tilt%" — uses first number (fund weight)
      2. Two-column: "Name Tilt%" — uses first number (fund weight)
    """
    try:
        import pdfplumber, io as _io
        from collections import defaultdict
    except ImportError:
        logger.error('pdfplumber not installed')
        return []

    holdings = []
    try:
        with pdfplumber.open(_io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                # Extract only right-column words (x > 47% of page width)
                right_x = page.width * 0.47
                words = page.extract_words(x_tolerance=3, y_tolerance=3)
                right_words = [w for w in words if w['x0'] > right_x]

                # Group words by row (y-coordinate, rounded to 3pt tolerance)
                rows: dict = defaultdict(list)
                for w in right_words:
                    rows[round(w['top'] / 3) * 3].append(w)

                # Reconstruct lines from right column
                lines = []
                for y in sorted(rows.keys()):
                    line_words = sorted(rows[y], key=lambda w: w['x0'])
                    lines.append(' '.join(w['text'] for w in line_words))

                in_section = False
                skip_lines = 2  # skip header rows: "Name Fund Benchmark Tilt" + "% % %"
                for line in lines:
                    if 'top 10 stock' in line.lower():
                        in_section = True
                        skip_lines = 2
                        continue
                    if not in_section:
                        continue
                    # End of section
                    if re.match(r'^sector\s+alloc', line, re.I) or re.match(r'^style\s+tilt', line, re.I):
                        break
                    # Skip header/percentage-only lines
                    if re.match(r'^[%\s]+$', line) or re.match(r'^Name\s+', line, re.I):
                        if skip_lines > 0:
                            skip_lines -= 1
                        continue
                    if skip_lines > 0:
                        skip_lines -= 1
                        continue
                    # Parse "Company Name XX.XX ..." — first number is fund weight
                    m = re.match(r'^(.+?)\s+(\d+\.\d+)\s*', line)
                    if m:
                        name = m.group(1).strip()
                        # Strip leading lowercase words (left-column bleed-in artefact)
                        name = re.sub(r'^(?:[a-z]+\s+)+', '', name).strip()
                        weight = _safe_float(m.group(2))
                        if name and weight and 0.1 < weight < 50:
                            holdings.append({'name': name, 'ticker': None, 'weight_pct': weight})
    except Exception as e:
        logger.warning(f'Ausbil fact sheet parse error: {e}')
    return holdings


def scrape_ausbil(db_path=None) -> int:
    """Scrape Ausbil ETF top 10 holdings from their fund fact sheet PDFs."""
    started = datetime.utcnow()
    source = 'ausbil'
    updated = 0

    conn = get_connection(db_path)

    for asx_code, page_url in _AUSBIL_FUNDS.items():
        try:
            page_resp = fetch(page_url)
            if not page_resp or page_resp.status_code != 200:
                logger.warning(f'Ausbil: fund page not accessible for {asx_code}')
                continue

            # Find the latest fact sheet PDF link
            m = _AUSBIL_FACTSHEET_RE.search(page_resp.text)
            if not m:
                logger.warning(f'Ausbil: no fact sheet PDF found for {asx_code}')
                continue

            fact_url = 'https://www.ausbil.com.au' + m.group(0)
            pdf_resp = fetch(fact_url)
            if not pdf_resp or pdf_resp.status_code != 200:
                logger.warning(f'Ausbil: fact sheet PDF download failed for {asx_code}')
                continue

            holdings = _parse_ausbil_factsheet_pdf(pdf_resp.content)
            if not holdings:
                logger.warning(f'Ausbil: no holdings parsed for {asx_code}')
                continue

            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Ausbil: {asx_code} — {len(holdings)} holdings')

        except Exception as e:
            logger.warning(f'Ausbil: error scraping {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Ausbil: updated {updated} ETFs')
    return updated


# ====================================================================
# Munro Partners
# ====================================================================

_MUNRO_FUND_PAGES = {
    'MCCL': 'https://www.munropartners.com/funds/mccl-asx/',
    'MCGG': 'https://www.munropartners.com/funds/mcgg-asx/',
}
_MUNRO_PORTFOLIO_PDF_RE = re.compile(
    r'https://www\.munropartners\.com/wp-content/uploads/[^\s"\'<>]*Portfolio-Disclosure[^\s"\'<>]*\.pdf',
    re.IGNORECASE,
)


def _parse_munro_portfolio_pdf(content: bytes) -> list[dict]:
    """
    Parse a Munro Partners monthly portfolio holdings disclosure PDF.

    Handles two formats:
      Single-column (MCCL):
        SECURITY WEIGHTING
        GE VERNOVA INC 9.22%
        ...
      Two-column (MCGG):
        SECURITY WEIGHTING SECURITY WEIGHTING
        NVIDIA CORP 6.85% CONSTELLATION ENERGY 1.93%
        ...
    Cash lines are skipped.
    """
    try:
        import pdfplumber
        import io as _io
    except ImportError:
        logger.error('pdfplumber not installed')
        return []

    holdings = []
    try:
        with pdfplumber.open(_io.BytesIO(content)) as pdf:
            full_text = '\n'.join(page.extract_text() or '' for page in pdf.pages)

        in_section = False
        for line in full_text.splitlines():
            line = line.strip()
            if re.match(r'^security\s+weighting', line, re.IGNORECASE):
                in_section = True
                continue
            if not in_section:
                continue
            if not line or re.match(r'^(important\s+information|past\s+performance|gsfm|munro|fund)', line, re.IGNORECASE):
                continue
            # Match one or two "NAME XX.XX%" pairs on the line
            pairs = re.findall(r'([A-Z][A-Z0-9 &\-\./\(\)\']+?)\s+([\d.]+)%', line)
            for name, weight_str in pairs:
                name = name.strip()
                weight = _safe_float(weight_str)
                if name and weight and 0.1 < weight < 50:
                    if 'cash' not in name.lower():
                        holdings.append({'name': name, 'ticker': None, 'weight_pct': weight})
    except Exception as e:
        logger.warning(f'Munro PDF parse error: {e}')

    # Deduplicate
    seen: dict[str, dict] = {}
    for h in holdings:
        seen.setdefault(h['name'], h)
    return list(seen.values())


def scrape_munro(db_path=None) -> int:
    """
    Scrape Munro Partners ETF holdings from their monthly portfolio disclosure PDFs.
    PDF URL is discovered from each fund's page on munropartners.com.
    """
    started = datetime.utcnow()
    source = 'munro'
    updated = 0

    conn = get_connection(db_path)

    for asx_code, page_url in _MUNRO_FUND_PAGES.items():
        try:
            page_resp = fetch(page_url)
            if not page_resp or page_resp.status_code != 200:
                logger.warning(f'Munro: fund page not accessible for {asx_code}')
                continue

            m = _MUNRO_PORTFOLIO_PDF_RE.search(page_resp.text)
            if not m:
                logger.warning(f'Munro: no portfolio disclosure PDF found for {asx_code}')
                continue

            pdf_url = m.group(0)
            logger.info(f'Munro: {asx_code} portfolio PDF: {pdf_url}')

            pdf_resp = fetch(pdf_url)
            if not pdf_resp or pdf_resp.status_code != 200:
                logger.warning(f'Munro: PDF download failed for {asx_code}')
                continue

            holdings = _parse_munro_portfolio_pdf(pdf_resp.content)
            if not holdings:
                logger.warning(f'Munro: no holdings parsed for {asx_code}')
                continue

            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Munro: {asx_code} — {len(holdings)} holdings')

        except Exception as e:
            logger.warning(f'Munro: error scraping {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Munro: updated {updated} ETFs')
    return updated


# ====================================================================
# Platinum
# ====================================================================

# Maps ASX code → fund page URL on platinum.com.au
_PLATINUM_FUND_PAGES = {
    'PIXX': 'https://www.platinum.com.au/active-etfs/pixx',
    'PAXX': 'https://www.platinum.com.au/active-etfs/paxx',
}
# Match quarterly portfolio disclosure PDF links in the ASX-Releases table
_PLATINUM_DISC_PDF_RE = re.compile(
    r'/media/Platinum/ASX-Releases/[^\s"\'<]+_asx_disc_[^\s"\'<]+\.pdf',
    re.IGNORECASE,
)


def _parse_platinum_disclosure_pdf(content: bytes) -> list[dict]:
    """
    Parse a Platinum quarterly portfolio disclosure PDF.

    Format (text-based):
      Security Weighting
      AerCap Holdings NV 10.0%
      Amazon.com Inc 8.0%
      ...
      Cash 1.8%

    Some funds hold an underlying managed fund (100%) which then lists
    the sub-fund's holdings on subsequent lines. We extract whichever
    block has >1 stock-level entry (weight < 100%).
    """
    try:
        import pdfplumber
        import io as _io
    except ImportError:
        logger.error('pdfplumber not installed')
        return []

    holdings = []
    try:
        with pdfplumber.open(_io.BytesIO(content)) as pdf:
            full_text = '\n'.join(
                page.extract_text() or '' for page in pdf.pages
            )

        in_section = False
        for line in full_text.splitlines():
            line = line.strip()
            # Section header
            if re.match(r'^security\s+weighting', line, re.IGNORECASE):
                in_section = True
                holdings = []   # reset — take the last/deepest section
                continue
            if not in_section:
                continue
            # End markers: blank lines or non-data lines after section started
            if not line or re.match(r'^(we\s+advise|authorised|investor\s+contact|platinum\s+investment|tel:|fax:|by\s+e-lodg)', line, re.IGNORECASE):
                if holdings:
                    in_section = False
                continue
            # Parse: "Company Name XX.X%"  or  "Company Name XX.X% [extra]"
            m = re.match(r'^(.+?)\s+([\d.]+)%\s*$', line)
            if m:
                name = m.group(1).strip()
                weight = _safe_float(m.group(2))
                if name and weight is not None and name.lower() not in ('cash',):
                    # Skip 100% wrapper entries (e.g. "Platinum International Fund 100.0%")
                    if weight < 99:
                        holdings.append({'name': name, 'ticker': None, 'weight_pct': weight})
    except Exception as e:
        logger.warning(f'Platinum PDF parse error: {e}')

    return holdings


def scrape_platinum(db_path=None) -> int:
    """
    Scrape Platinum active ETF holdings from their quarterly portfolio
    disclosure PDFs, discovered from each fund's page on platinum.com.au.
    """
    started = datetime.utcnow()
    source = 'platinum'
    updated = 0

    conn = get_connection(db_path)

    for asx_code, page_url in _PLATINUM_FUND_PAGES.items():
        try:
            page_resp = fetch(page_url)
            if not page_resp or page_resp.status_code != 200:
                logger.warning(f'Platinum: fund page not accessible for {asx_code}')
                continue

            m = _PLATINUM_DISC_PDF_RE.search(page_resp.text)
            if not m:
                logger.warning(f'Platinum: no disclosure PDF link found for {asx_code}')
                continue

            pdf_url = 'https://www.platinum.com.au' + m.group(0)
            logger.info(f'Platinum: {asx_code} disclosure PDF: {pdf_url}')

            pdf_resp = fetch(pdf_url)
            if not pdf_resp or pdf_resp.status_code != 200:
                logger.warning(f'Platinum: PDF download failed for {asx_code}')
                continue

            holdings = _parse_platinum_disclosure_pdf(pdf_resp.content)
            if not holdings:
                logger.warning(f'Platinum: no holdings parsed for {asx_code}')
                continue

            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Platinum: {asx_code} — {len(holdings)} holdings')

        except Exception as e:
            logger.warning(f'Platinum: error scraping {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Platinum: updated {updated} ETFs')
    return updated


# ====================================================================
# Loftus Peak
# ====================================================================

# Both ETFs (LPGD = unhedged, LPHD = hedged) share the same underlying portfolio.
# Holdings are published monthly on the advisers page as a single report.
_LOFTUSPEAK_ADVISERS_URL = 'https://www.loftuspeak.com.au/advisers/'
_LOFTUSPEAK_FUNDS = ['LPGD', 'LPHD']


def _parse_loftuspeak_holdings(html_text: str) -> list[dict]:
    """
    Parse Loftus Peak monthly stock holding report from the #stockholding-report
    section of their advisers page.

    Format in section text:
      Ticker: NVDA Weight: 10.35%
      Ticker: 2330/TSM Weight: 8.11%
      ...

    The section lists each holding twice (once sorted by weight, once alphabetically).
    We deduplicate by ticker/name, keeping the first (weight-sorted) occurrence.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html_text, 'html.parser')
    section = soup.find(id='stockholding-report')
    if not section:
        return []

    text = section.get_text()
    seen: set[str] = set()
    holdings = []

    for m in re.finditer(r'Ticker:\s*([^\s]+)\s+Weight:\s*([\d.]+)%', text):
        ticker_raw = m.group(1).strip()
        weight = _safe_float(m.group(2))
        if ticker_raw in seen:
            continue   # second (alphabetical) occurrence — skip
        seen.add(ticker_raw)

        # Normalise compound tickers like "2330/TSM" — keep the simpler form
        ticker = ticker_raw.split('/')[-1] if '/' in ticker_raw else ticker_raw

        if weight is not None:
            holdings.append({
                'name': ticker,
                'ticker': ticker,
                'weight_pct': weight,
            })

    return holdings


def scrape_loftuspeak(db_path=None) -> int:
    """
    Scrape Loftus Peak ETF holdings from their monthly stock holding report,
    embedded in the static HTML of their advisers page.
    Both LPGD (unhedged) and LPHD (hedged) share the same portfolio.
    """
    started = datetime.utcnow()
    source = 'loftuspeak'
    updated = 0

    conn = get_connection(db_path)

    resp = fetch(_LOFTUSPEAK_ADVISERS_URL)
    if not resp or resp.status_code != 200:
        logger.error('Loftus Peak: advisers page not accessible')
        log_scrape(conn, source, 'error', records_affected=0,
                   duration_secs=0, started_at=started.isoformat())
        conn.close()
        return 0

    holdings = _parse_loftuspeak_holdings(resp.text)
    if not holdings:
        logger.warning('Loftus Peak: no holdings found on advisers page')
        conn.close()
        return 0

    for asx_code in _LOFTUSPEAK_FUNDS:
        try:
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Loftus Peak: {asx_code} — {len(holdings)} holdings')
        except Exception as e:
            logger.warning(f'Loftus Peak: error for {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Loftus Peak: updated {updated} ETFs')
    return updated


# ====================================================================
# Montaka
# ====================================================================

# Maps ASX code → (fund page URL, factsheet URL on HubSpot CDN)
_MONTAKA_FUNDS = {
    'MKAX': 'https://7041279.fs1.hubspotusercontent-ap1.net/hubfs/7041279/PDS%20and%20Factsheets/MKAX%20Factsheet.pdf',
    'MOGL': 'https://7041279.fs1.hubspotusercontent-ap1.net/hubfs/7041279/PDS%20and%20Factsheets/MOGL%20Factsheet.pdf',
}


def _parse_montaka_factsheet_pdf(content: bytes) -> list[dict]:
    """
    Parse a Montaka fund fact sheet PDF for top-10 holdings.

    Format — two-column table on a single page:
      TOP 10 HOLDINGS (net, % of NAV)
      1 Amazon 12.8% 6 Tencent 6.6%
      2 Microsoft 12.4% 7 ServiceNow 6.1%
      ...

    Holdings are numbered 1-10, arranged in two columns of five.
    We parse each line as: rank name weight% rank name weight%
    """
    try:
        import pdfplumber
        import io as _io
    except ImportError:
        logger.error('pdfplumber not installed')
        return []

    holdings = []
    try:
        with pdfplumber.open(_io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ''
                in_section = False
                for line in text.splitlines():
                    line = line.strip()
                    if re.match(r'^top\s+10\s+hold', line, re.IGNORECASE):
                        in_section = True
                        continue
                    if not in_section:
                        continue
                    # End of section
                    if re.match(r'^(key\s+transform|geographic|industry|fund\s+stat|performance|total\s+top)', line, re.IGNORECASE):
                        break
                    # Match pairs: "N Name XX.X% N Name XX.X%"
                    # Each half: rank (digit), name (words), weight%
                    parts = re.findall(r'\d+\s+(.+?)\s+([\d.]+)%', line)
                    for name, weight_str in parts:
                        weight = _safe_float(weight_str)
                        name = name.strip()
                        if name and weight and 0.5 < weight < 50:
                            holdings.append({'name': name, 'ticker': None, 'weight_pct': weight})
    except Exception as e:
        logger.warning(f'Montaka factsheet parse error: {e}')

    # Deduplicate — same security can appear from multi-page/multi-section
    seen: dict[str, dict] = {}
    for h in holdings:
        seen.setdefault(h['name'], h)
    return list(seen.values())[:10]


def scrape_montaka(db_path=None) -> int:
    """
    Scrape Montaka ETF top-10 holdings from their HubSpot-hosted factsheet PDFs.
    MKAX = Montaka Global Extension Fund (long/short)
    MOGL = Montaka Global Long Only Fund
    """
    started = datetime.utcnow()
    source = 'montaka'
    updated = 0

    conn = get_connection(db_path)

    for asx_code, pdf_url in _MONTAKA_FUNDS.items():
        try:
            pdf_resp = fetch(pdf_url)
            if not pdf_resp or pdf_resp.status_code != 200:
                logger.warning(f'Montaka: factsheet PDF not accessible for {asx_code}')
                continue

            holdings = _parse_montaka_factsheet_pdf(pdf_resp.content)
            if not holdings:
                logger.warning(f'Montaka: no holdings parsed for {asx_code}')
                continue

            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Montaka: {asx_code} — {len(holdings)} holdings')

        except Exception as e:
            logger.warning(f'Montaka: error scraping {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Montaka: updated {updated} ETFs')
    return updated


# ====================================================================
# Associate Global Partners (SWTZ, WCMQ)
# ====================================================================

_AGP_FUNDS = {
    'SWTZ': 'https://www.associateglobal.com/funds/swtz/',
    'WCMQ': 'https://www.associateglobal.com/funds/wcmq/',
}
_AGP_WEBLINK_BASE = 'https://wcsecure.weblink.com.au/clients/associateglobal/{ticker}/headline.aspx?headlineid={id}'
_AGP_HEADLINE_RE = re.compile(r'headlineid=(\d+)', re.IGNORECASE)
_AGP_CHROME_HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/131.0.0.0 Safari/537.36'),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-AU,en;q=0.9',
}


def _parse_agp_portfolio_pdf(content: bytes) -> list[dict]:
    """
    Parse an AGP quarterly portfolio disclosure PDF.
    Format (page 2): two-column table with "Holdings Weight" headers.
    Each line: "NAME1 WEIGHT1 NAME2 WEIGHT2" or "NAME1 WEIGHT1"
    Skips CASH, options/derivatives lines.
    """
    import io as _io
    import pdfplumber

    holdings = []
    skip_re = re.compile(r'^(?:CASH|Holdings Weight)', re.IGNORECASE)
    # Options/derivatives, or footer lines
    option_re = re.compile(r'\d{2}/\d{2}/\d{2}|\bEquity\b|\bComdty\b', re.IGNORECASE)
    footer_re = re.compile(
        r'Associate Global|AGP Investment|ABN \d|AFSL \d|Level \d|About the|E invest@',
        re.IGNORECASE,
    )

    try:
        with pdfplumber.open(_io.BytesIO(content)) as pdf:
            # Holdings are on page 2 (index 1)
            for page in pdf.pages[1:]:
                text = page.extract_text() or ''
                for line in text.splitlines():
                    line = line.strip()
                    if not line or skip_re.match(line) or option_re.search(line):
                        continue
                    if footer_re.search(line):
                        break  # Stop at footer text
                    # Try two-holding line: NAME WEIGHT NAME WEIGHT
                    m2 = re.match(
                        r'^(.+?)\s+([\d.]+)\s+(.+?)\s+([\d.]+)\s*$', line
                    )
                    if m2:
                        for name, w_str in [(m2.group(1), m2.group(2)),
                                            (m2.group(3), m2.group(4))]:
                            name = name.strip()
                            if skip_re.match(name) or option_re.search(name):
                                continue
                            try:
                                holdings.append({
                                    'name': name,
                                    'ticker': None,
                                    'weight_pct': float(w_str),
                                    'sector': None,
                                    'country': None,
                                })
                            except ValueError:
                                pass
                        continue
                    # Try single-holding line
                    m1 = re.match(r'^(.+?)\s+([\d.]+)\s*$', line)
                    if m1:
                        name = m1.group(1).strip()
                        if skip_re.match(name) or option_re.search(name):
                            continue
                        try:
                            holdings.append({
                                'name': name,
                                'ticker': None,
                                'weight_pct': float(m1.group(2)),
                                'sector': None,
                                'country': None,
                            })
                        except ValueError:
                            pass
    except Exception as e:
        logger.warning(f'AGP: PDF parse error: {e}')

    return holdings


def scrape_agp(db_path=None) -> int:
    source = 'Associate Global Partners'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    for asx_code, fund_url in _AGP_FUNDS.items():
        try:
            resp = fetch(fund_url, headers=_AGP_CHROME_HEADERS)
            if not resp or resp.status_code != 200:
                logger.warning(f'AGP: fund page not accessible for {asx_code}')
                continue

            # Find all headlineids in page, looking for quarterly portfolio context
            ticker = asx_code.lower()
            latest_id = None
            for m in _AGP_HEADLINE_RE.finditer(resp.text):
                start = max(0, m.start() - 300)
                context = resp.text[start:m.end()]
                if 'portfolio' in context.lower() and 'quarterly' in context.lower():
                    latest_id = m.group(1)
                    break

            if not latest_id:
                logger.warning(f'AGP: no quarterly portfolio disclosure found for {asx_code}')
                continue

            pdf_url = _AGP_WEBLINK_BASE.format(ticker=ticker, id=latest_id)
            pdf_resp = fetch(pdf_url)
            if not pdf_resp or pdf_resp.status_code != 200:
                logger.warning(f'AGP: PDF download failed for {asx_code} ({pdf_url})')
                continue

            holdings = _parse_agp_portfolio_pdf(pdf_resp.content)
            if not holdings:
                logger.warning(f'AGP: no holdings parsed for {asx_code}')
                continue

            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'AGP: {asx_code} — {len(holdings)} holdings')

        except Exception as e:
            logger.warning(f'AGP: error scraping {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'AGP: updated {updated} ETFs')
    return updated


# ====================================================================
# Apostle Dundas
# ====================================================================

_APOSTLE_K2_FORMS_URL = 'https://www.k2am.com.au/forms-apostle-dundas'
_APOSTLE_FACTSHEET_RE = re.compile(
    r'/cms_uploads/docs/adgef-class-c-monthly[^\s"\'<>]*\.pdf',
    re.IGNORECASE,
)


def _parse_apostle_factsheet_pdf(content: bytes) -> list[dict]:
    """
    Parse Apostle Dundas ADGEF monthly factsheet PDF.
    Extracts the "Top Ten Holdings by Capital (% weight)" table.

    The PDF is two-column; we crop the right half of page 1 to isolate the
    holdings table and avoid left-column text (manager info) bleeding in.
    """
    import io as _io
    import pdfplumber

    holdings = []
    try:
        with pdfplumber.open(_io.BytesIO(content)) as pdf:
            page = pdf.pages[0]
            # Crop right half of page (holdings table lives there)
            right = page.crop((page.width / 2, 0, page.width, page.height))
            text = right.extract_text() or ''
    except Exception as e:
        logger.warning(f'Apostle: PDF parse error: {e}')
        return []

    # Find the top-10 holdings block
    m = re.search(r'Top Ten Holdings by Capital[^\n]*\n', text)
    if not m:
        logger.warning('Apostle: Top Ten Holdings section not found in right half')
        return []

    block = text[m.end():]
    total_m = re.search(r'^TOTAL\b', block, re.MULTILINE)
    if total_m:
        block = block[:total_m.start()]

    # Each data line: "Company Name FUND_PCT ACTIVE_WEIGHT"
    for line in block.splitlines():
        line = line.strip()
        if not line or line.startswith('Stock') or line.startswith('Source:'):
            continue
        # Match trailing two floats (active weight may be negative)
        m2 = re.match(r'^(.+?)\s+([\d.]+)\s+([-\d.]+)\s*$', line)
        if not m2:
            continue
        name = m2.group(1).strip()
        try:
            weight_pct = float(m2.group(2))
        except ValueError:
            continue
        if not name or weight_pct <= 0:
            continue
        holdings.append({
            'name': name,
            'ticker': None,
            'weight_pct': weight_pct,
            'sector': None,
            'country': None,
        })

    return holdings


def scrape_apostle(db_path=None) -> int:
    source = 'Apostle Dundas'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0
    asx_code = 'ADEF'

    try:
        resp = fetch(_APOSTLE_K2_FORMS_URL)
        if not resp or resp.status_code != 200:
            logger.warning('Apostle: forms page not accessible')
        else:
            m = _APOSTLE_FACTSHEET_RE.search(resp.text)
            if not m:
                logger.warning('Apostle: no Class C factsheet PDF link found')
            else:
                pdf_url = 'https://www.k2am.com.au' + m.group(0)
                pdf_resp = fetch(pdf_url)
                if not pdf_resp or pdf_resp.status_code != 200:
                    logger.warning(f'Apostle: PDF download failed: {pdf_url}')
                else:
                    holdings = _parse_apostle_factsheet_pdf(pdf_resp.content)
                    if not holdings:
                        logger.warning('Apostle: no holdings parsed')
                    else:
                        upsert_holdings(conn, asx_code, holdings)
                        updated += 1
                        logger.info(f'Apostle: {asx_code} — {len(holdings)} holdings')

    except Exception as e:
        logger.warning(f'Apostle: error: {e}')
        conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Apostle: updated {updated} ETFs')
    return updated


# ====================================================================
# Australian Ethical (AEAE)
# ====================================================================

_AUS_ETHICAL_FUND_URL = (
    'https://www.australianethical.com.au/managed-funds/investment-options/high-conviction/'
)
_AUS_ETHICAL_CHROME_HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}


def _parse_aus_ethical_holdings(html: str) -> list[dict]:
    """Parse top-10 holdings from Australian Ethical High Conviction fund page."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    for table in soup.find_all('table'):
        headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
        if 'top 10 holdings' in headers and 'allocation' in headers:
            holdings = []
            for row in table.find_all('tr')[1:]:  # skip header row
                cells = [td.get_text(strip=True) for td in row.find_all('td')]
                if len(cells) < 2:
                    continue
                name, w_str = cells[0], cells[1].replace('%', '')
                if not name or name.lower() in ('top 10 holdings', 'allocation'):
                    continue
                try:
                    holdings.append({
                        'name': name,
                        'ticker': None,
                        'weight_pct': float(w_str),
                        'sector': None,
                        'country': 'AU',
                    })
                except ValueError:
                    pass
            return holdings
    return []


def scrape_aus_ethical(db_path=None) -> int:
    source = 'Australian Ethical'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0
    asx_code = 'AEAE'

    try:
        resp = fetch(_AUS_ETHICAL_FUND_URL, headers=_AUS_ETHICAL_CHROME_HEADERS)
        if not resp or resp.status_code != 200:
            logger.warning('Australian Ethical: page not accessible')
        else:
            holdings = _parse_aus_ethical_holdings(resp.text)
            if not holdings:
                logger.warning('Australian Ethical: no holdings parsed')
            else:
                upsert_holdings(conn, asx_code, holdings)
                updated += 1
                logger.info(f'Australian Ethical: {asx_code} — {len(holdings)} holdings')
    except Exception as e:
        logger.warning(f'Australian Ethical: error: {e}')
        conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Australian Ethical: updated {updated} ETFs')
    return updated


# ====================================================================
# India Avenue (IAEF)
# ====================================================================

_INDIA_AVENUE_URL = 'https://indiaavenue.com.au/india-avenue-equity-fund-etf/'


def _parse_india_avenue_holdings(html: str) -> list[dict]:
    """Parse top-10 holdings from the India Avenue IAEF fund page HTML."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    holdings = []
    for table in soup.find_all('table'):
        headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
        if 'stock name' in headers and 'weight' in headers:
            name_idx = headers.index('stock name')
            wgt_idx = headers.index('weight')
            sector_idx = headers.index('sector') if 'sector' in headers else None
            for row in table.find('tbody').find_all('tr'):
                cells = [td.get_text(strip=True) for td in row.find_all('td')]
                if len(cells) <= max(name_idx, wgt_idx):
                    continue
                name = cells[name_idx]
                w_str = cells[wgt_idx].replace('%', '')
                try:
                    weight_pct = float(w_str)
                except ValueError:
                    continue
                sector = cells[sector_idx] if sector_idx is not None else None
                holdings.append({
                    'name': name,
                    'ticker': None,
                    'weight_pct': weight_pct,
                    'sector': sector or None,
                    'country': 'IN',
                })
            break
    return holdings


def scrape_india_avenue(db_path=None) -> int:
    source = 'India Avenue'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0
    asx_code = 'IAEF'

    try:
        resp = fetch(_INDIA_AVENUE_URL, headers={
            'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                           'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'),
        })
        if not resp or resp.status_code != 200:
            logger.warning('India Avenue: page not accessible')
        else:
            holdings = _parse_india_avenue_holdings(resp.text)
            if not holdings:
                logger.warning('India Avenue: no holdings parsed')
            else:
                upsert_holdings(conn, asx_code, holdings)
                updated += 1
                logger.info(f'India Avenue: {asx_code} — {len(holdings)} holdings')
    except Exception as e:
        logger.warning(f'India Avenue: error: {e}')
        conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'India Avenue: updated {updated} ETFs')
    return updated


# ====================================================================
# GCQ Funds (GCQF)
# ====================================================================

_GCQ_FUND_URL = 'https://www.gcqfunds.com/gcq-global-equities-complex-etf-asx-gcqf/'
_GCQ_DISCLOSURE_RE = re.compile(
    r'https://www\.gcqfunds\.com/content/uploads/GCQF-Quarterly-Portfolio-Disclosure[^\s"\'<>]+\.pdf',
    re.IGNORECASE,
)


def _parse_gcq_disclosure_pdf(content: bytes) -> list[dict]:
    """
    Parse a GCQ Quarterly Portfolio Disclosure PDF.
    Format: two-column, "Name X.XX%  Name X.XX%"
    Skips negative weights (short positions).
    """
    import io as _io
    import pdfplumber

    holdings = []
    try:
        with pdfplumber.open(_io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ''
                # Match all "Name X.XX%" patterns
                for m in re.finditer(r'([A-Z][^\d\n]+?)\s+([\d.]+)%', text):
                    name = m.group(1).strip().rstrip('.,')
                    try:
                        weight_pct = float(m.group(2))
                    except ValueError:
                        continue
                    if weight_pct <= 0 or not name:
                        continue
                    # Skip very generic or short names
                    if len(name) < 3:
                        continue
                    holdings.append({
                        'name': name,
                        'ticker': None,
                        'weight_pct': weight_pct,
                        'sector': None,
                        'country': None,
                    })
    except Exception as e:
        logger.warning(f'GCQ: PDF parse error: {e}')

    # Deduplicate by name (keep highest weight)
    seen = {}
    for h in holdings:
        n = h['name']
        if n not in seen or h['weight_pct'] > seen[n]['weight_pct']:
            seen[n] = h
    return list(seen.values())


def scrape_gcq(db_path=None) -> int:
    source = 'GCQ Funds'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0
    asx_code = 'GCQF'

    try:
        resp = fetch(_GCQ_FUND_URL)
        if not resp or resp.status_code != 200:
            logger.warning('GCQ: fund page not accessible')
        else:
            m = _GCQ_DISCLOSURE_RE.search(resp.text)
            if not m:
                logger.warning('GCQ: no quarterly portfolio disclosure URL found')
            else:
                pdf_url = m.group(0)
                pdf_resp = fetch(pdf_url)
                if not pdf_resp or pdf_resp.status_code != 200:
                    logger.warning(f'GCQ: PDF download failed: {pdf_url}')
                else:
                    holdings = _parse_gcq_disclosure_pdf(pdf_resp.content)
                    if not holdings:
                        logger.warning('GCQ: no holdings parsed')
                    else:
                        upsert_holdings(conn, asx_code, holdings)
                        updated += 1
                        logger.info(f'GCQ: {asx_code} — {len(holdings)} holdings')
    except Exception as e:
        logger.warning(f'GCQ: error: {e}')
        conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'GCQ: updated {updated} ETFs')
    return updated


# ====================================================================
# IML (Investors Mutual)
# ====================================================================

_IML_FUND_PAGES = {
    'EQIN': 'https://iml.com.au/funds/equity-income-fund/',
    'IMLC': 'https://iml.com.au/funds/concentrated-australian-share-fund-etf/',
}


def _parse_iml_holdings(html: str) -> list[dict]:
    """Parse top-10 holdings from an IML fund page HTML."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    holdings = []
    for table in soup.find_all('table'):
        headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
        if 'company name' in headers and '% weight' in headers:
            for row in table.find('tbody').find_all('tr'):
                cells = [td.get_text(strip=True) for td in row.find_all('td')]
                if len(cells) < 4:
                    continue
                name, ticker, sector, weight_str = cells[0], cells[1], cells[2], cells[3]
                try:
                    weight_pct = float(weight_str.replace('%', ''))
                except ValueError:
                    continue
                holdings.append({
                    'name': name,
                    'ticker': ticker or None,
                    'weight_pct': weight_pct,
                    'sector': sector or None,
                    'country': 'AU',
                })
            break
    return holdings


def scrape_iml(db_path=None) -> int:
    source = 'IML'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    for asx_code, page_url in _IML_FUND_PAGES.items():
        try:
            resp = fetch(page_url)
            if not resp or resp.status_code != 200:
                logger.warning(f'IML: page not accessible for {asx_code}')
                continue

            holdings = _parse_iml_holdings(resp.text)
            if not holdings:
                logger.warning(f'IML: no holdings parsed for {asx_code}')
                continue

            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'IML: {asx_code} — {len(holdings)} holdings')

        except Exception as e:
            logger.warning(f'IML: error scraping {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'IML: updated {updated} ETFs')
    return updated


# ====================================================================
# JPMorgan Asset Management
# ====================================================================

# ====================================================================
# Plato Investment Management
# ====================================================================

_PLATO_FUNDS = {
    'PGA1': 'https://plato.com.au/wp-content/uploads/Plato-Global-Alpha-Fund.pdf',
}
_PLATO_SKIP_RE = re.compile(
    r'^(Source:|Region|Fund Manager|Stock Weight|Past performance)', re.IGNORECASE
)
_PLATO_WEIGHT_RE = re.compile(r'^(.+?)\s+([\d]+\.[\d]+)\s*$')


def _parse_plato_factsheet_pdf(content: bytes) -> list[dict]:
    """Parse 'Top 10 holdings (%)' section from a Plato factsheet PDF."""
    import pdfplumber
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ''
            if 'Top 10 holdings' not in text:
                continue
            lines = [ln.strip() for ln in text.split('\n') if ln.strip()]
            in_holdings = False
            holdings = []
            for line in lines:
                if 'Stock Weight' in line:
                    in_holdings = True
                    continue
                if not in_holdings:
                    continue
                if len(holdings) >= 10:
                    break
                if _PLATO_SKIP_RE.match(line):
                    continue
                m = _PLATO_WEIGHT_RE.match(line)
                if m:
                    name = m.group(1).strip()
                    try:
                        weight_pct = float(m.group(2))
                    except ValueError:
                        continue
                    if weight_pct > 0:
                        holdings.append({'name': name, 'ticker': None,
                                         'weight_pct': weight_pct, 'sector': None, 'country': None})
            if holdings:
                return holdings
    return []


def scrape_plato(db_path=None) -> int:
    source = 'Plato'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    for asx_code, pdf_url in _PLATO_FUNDS.items():
        try:
            resp = fetch(pdf_url)
            if not resp or resp.status_code != 200:
                logger.warning(f'Plato: PDF not accessible for {asx_code}')
                continue
            holdings = _parse_plato_factsheet_pdf(resp.content)
            if not holdings:
                logger.warning(f'Plato: no holdings parsed for {asx_code}')
                continue
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Plato: {asx_code} — {len(holdings)} holdings')
        except Exception as e:
            logger.warning(f'Plato: error scraping {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Plato: updated {updated} ETFs')
    return updated


# ====================================================================
# Janus Henderson (AU ETFs)
# ====================================================================

_JH_BASE = 'https://www.janushenderson.com'
# Only equity fund has a public Top Holdings section in HTML
_JH_FUNDS = {
    'GOOD': '/en-au/investor/product/global-sustainable-equity-fund',
}
# FUTR factsheet PDF on Janus Henderson CDN — overwritten monthly with latest data
_JH_PDF_FUNDS = {
    'FUTR': 'https://cdn.janushenderson.com/webdocs/Janus+Henderson+Global+Sustainable+Equity+Active+ETF+Factsheet.pdf',
}
_JH_HOLDINGS_RE = re.compile(
    r'([A-Z][A-Za-z0-9\s\&\.\,\(\)\-\/]{1,50}?)\s+([\d]{1,3}\.[\d]{1,2})(?=\s|$)'
)


def _parse_jh_holdings(text: str) -> list[dict]:
    """
    Parse 'Top Holdings' section from Janus Henderson AU fund page text.
    Format: 'Top Holdings (As of ...) % of Fund NAME1 X.XX NAME2 X.XX ... TOTAL ...'
    """
    holdings = []
    idx = text.find('Top Holdings')
    if idx < 0:
        return holdings
    section = text[idx:idx + 600]
    # Skip '% of Fund' header
    header_end = section.find('% of Fund')
    content = section[header_end + len('% of Fund'):] if header_end >= 0 else section
    # Stop at TOTAL or Sector sections
    for stopper in ('TOTAL', 'Sector Market'):
        stop_idx = content.find(stopper)
        if stop_idx >= 0:
            content = content[:stop_idx]
            break
    for m in _JH_HOLDINGS_RE.finditer(content):
        name = m.group(1).strip()
        if any(x in name for x in ('of Fund', 'of Index', '%', 'As of')):
            continue
        try:
            weight_pct = float(m.group(2))
        except ValueError:
            continue
        if weight_pct > 0 and name:
            holdings.append({'name': name, 'ticker': None,
                             'weight_pct': weight_pct, 'sector': None, 'country': None})
    return holdings[:10]


_JH_PDF_WEIGHT_RE = re.compile(r'^(.+?)\s+([\d]+\.[\d]+)\s*$')
_JH_PDF_SKIP = frozenset({'Total', 'TOTAL', 'Cash', 'Other'})


def _parse_jh_pdf_holdings(content: bytes) -> list[dict]:
    """
    Parse Janus Henderson factsheet PDF for Top 10 Holdings.
    Format on page 2: 'Top 10 Holdings (%) Fund' header followed by
    lines like 'STOCK NAME X.XX'.
    """
    import pdfplumber
    holdings = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ''
            if 'Top 10 Holdings' not in text:
                continue
            lines = [ln.strip() for ln in text.split('\n') if ln.strip()]
            in_section = False
            for line in lines:
                if 'Top 10 Holdings' in line:
                    in_section = True
                    continue
                if not in_section:
                    continue
                if len(holdings) >= 10:
                    break
                m = _JH_PDF_WEIGHT_RE.match(line)
                if not m:
                    continue
                name = m.group(1).strip()
                if name in _JH_PDF_SKIP or not name[0].isupper():
                    continue
                try:
                    weight_pct = float(m.group(2))
                except ValueError:
                    continue
                if weight_pct > 0:
                    holdings.append({'name': name, 'ticker': None,
                                     'weight_pct': weight_pct, 'sector': None, 'country': None})
            if holdings:
                break
    return holdings


def scrape_janus_henderson(db_path=None) -> int:
    from bs4 import BeautifulSoup
    source = 'Janus Henderson'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    for asx_code, path in _JH_FUNDS.items():
        url = _JH_BASE + path
        try:
            resp = fetch(url)
            if not resp or resp.status_code != 200:
                logger.warning(f'Janus Henderson: page not accessible for {asx_code}')
                continue
            text = BeautifulSoup(resp.text, 'html.parser').get_text(' ', strip=True)
            holdings = _parse_jh_holdings(text)
            if not holdings:
                logger.warning(f'Janus Henderson: no holdings parsed for {asx_code}')
                continue
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Janus Henderson: {asx_code} — {len(holdings)} holdings')
        except Exception as e:
            logger.warning(f'Janus Henderson: error scraping {asx_code}: {e}')
            conn.rollback()

    for asx_code, url in _JH_PDF_FUNDS.items():
        try:
            resp = fetch(url)
            if not resp or resp.status_code != 200:
                logger.warning(f'Janus Henderson: PDF not accessible for {asx_code}')
                continue
            holdings = _parse_jh_pdf_holdings(resp.content)
            if not holdings:
                logger.warning(f'Janus Henderson: no holdings in PDF for {asx_code}')
                continue
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Janus Henderson: {asx_code} — {len(holdings)} holdings (PDF)')
        except Exception as e:
            logger.warning(f'Janus Henderson: error scraping PDF for {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Janus Henderson: updated {updated} ETFs')
    return updated


# ====================================================================
# JPMorgan Asset Management
# ====================================================================

_JPMAM_BASE_URL = (
    'https://am.jpmorgan.com/content/dam/jpm-am-aem/asiapacific/au/en/literature/fact-sheet/'
)
_JPMAM_FUNDS = {
    'JEPI':  'factsheet-jpmorgan-equity-premium-income-active-etf.pdf',
    'JHPI':  'factsheet-jpmorgan-equity-premium-income-active-etf-hedged.pdf',
    'JGLO':  'factsheet-jpmorgan-global-select-equity-active-etf.pdf',
    'JHLO':  'factsheet-jpmorgan-global-select-equity-active-etf-hedged.pdf',
    'JREG':  'factsheet-jpmorgan-global-research-enhanced-index-equity-active-etf.pdf',
    'JRHG':  'factsheet-jpmorgan-global-research-enhanced-index-equity-active-etf-hedged.pdf',
    'JEME':  'factsheet-jpmorgan-emerging-markets-research-enhanced-index-equity-active-etf.pdf',
    'T3MP':  'factsheet-jpmorgan-climate-change-solutions-active-etf.pdf',
    'JPGB':  'factsheet-jpmorgan-global-bond-fund-active-etf.pdf',
    'JPIE':  'factsheet-jpmorgan-income-active-etf.pdf',
    'JPEQ':  'factsheet-jpmorgan-us-100q-equity-premium-income-active-etf.pdf',
    'JPHQ':  'factsheet-jpmorgan-us-100q-equity-premium-income-active-etf-hedged.pdf',
}
# Match decimal numbers NOT immediately followed by % or another digit
# (avoids matching partial numbers from coupon rates like 4.75% → avoid matching 4.7)
_JPMAM_DECIMAL_RE = re.compile(r'(\d+\.\d+)(?![%\d])')
_JPMAM_CURRENCY_SUFFIX_RE = re.compile(r'\s*/[A-Z]{3}/\s*$')
_JPMAM_SKIP_NAMES = frozenset({'CASH', 'NET CASH', 'CASH AND EQUIVALENTS'})


def _parse_jpmam_factsheet_pdf(content: bytes) -> list[dict]:
    """
    Parse JPMorgan AU factsheet PDF to extract top-10 underlying holdings.

    Factsheets use a two-column page layout. The 'TOP 10 %' header marks the
    start of the holdings table (under 'Underlying Holdings' for wrapper funds).
    Each data line has the format: 'HOLDING NAME X.XX [right-column-bleed]'
    """
    import pdfplumber
    holdings = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ''
            if 'TOP 10' not in text:
                continue
            lines = [ln.strip() for ln in text.split('\n') if ln.strip()]
            in_top10 = False
            for line in lines:
                if re.search(r'TOP\s+10\s+%', line):
                    in_top10 = True
                    continue
                if not in_top10:
                    continue
                if len(holdings) >= 10:
                    break
                matches = list(_JPMAM_DECIMAL_RE.finditer(line))
                if not matches:
                    continue
                # First non-%-terminated decimal is the holding's weight
                m = matches[0]
                try:
                    weight_pct = float(m.group(1))
                except ValueError:
                    continue
                if weight_pct <= 0:
                    continue
                # Name = everything before the weight value
                name = line[:m.start()].strip()
                # Strip currency suffix e.g. '/TWD/'
                name = _JPMAM_CURRENCY_SUFFIX_RE.sub('', name).strip()
                if not name or name.upper() in _JPMAM_SKIP_NAMES:
                    continue
                # Merge duplicate names (e.g. Alphabet Class A + C)
                existing = next((h for h in holdings if h['name'] == name), None)
                if existing:
                    existing['weight_pct'] = round(existing['weight_pct'] + weight_pct, 4)
                else:
                    holdings.append({
                        'name': name,
                        'ticker': None,
                        'weight_pct': weight_pct,
                        'sector': None,
                        'country': None,
                    })
            if holdings:
                break
    return holdings


def scrape_jpmam(db_path=None) -> int:
    source = 'JPMorgan'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    for asx_code, slug in _JPMAM_FUNDS.items():
        pdf_url = _JPMAM_BASE_URL + slug
        try:
            resp = fetch(pdf_url)
            if not resp or resp.status_code != 200:
                logger.warning(f'JPMorgan: PDF not accessible for {asx_code}: {pdf_url}')
                continue
            holdings = _parse_jpmam_factsheet_pdf(resp.content)
            if not holdings:
                logger.warning(f'JPMorgan: no holdings parsed for {asx_code}')
                continue
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'JPMorgan: {asx_code} — {len(holdings)} holdings')
        except Exception as e:
            logger.warning(f'JPMorgan: error scraping {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'JPMorgan: updated {updated} ETFs')
    return updated


# ====================================================================
# Antipodes Partners
# ====================================================================

# Static URLs — overwritten each month with the latest monthly update
_ANTIPODES_FUNDS = {
    'AGX1': 'https://antipodes.com/wp-content/uploads/Antipodes-Global-Value-Active-ETF-AGX1-Monthly-Update.pdf',
    'MIDS': 'https://antipodes.com/wp-content/uploads/Antipodes-Global-SMID-Active-ETF-Monthly-Update.pdf',
}
_ANTIPODES_DECIMAL_RE = re.compile(r'([\d]+\.[\d]+)')
# Two-word countries that appear as last two tokens before weight
_ANTIPODES_TWO_WORD_COUNTRIES = frozenset({
    'United States', 'United Kingdom', 'Hong Kong', 'South Korea',
    'New Zealand', 'Saudi Arabia', 'South Africa',
})
_ANTIPODES_SKIP_LINES = frozenset({'Name Country Weight Asset value', 'Fund AUM', 'Strategy AUM'})


def _parse_antipodes_pdf(content: bytes) -> list[dict]:
    """
    Parse Antipodes monthly update PDF for 'Top 10 equity longs'.
    Two-column layout bleeds right-column text onto holding lines.
    Strategy: find FIRST decimal in [0.1, 20.0] range per line — that's the weight.
    Text before it is 'Name Country'; we strip the last 1-2 words as country.
    """
    import pdfplumber
    holdings = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ''
            if 'Top 10 equity' not in text:
                continue
            lines = [ln.strip() for ln in text.split('\n') if ln.strip()]
            in_section = False
            for line in lines:
                if 'Top 10 equity' in line:
                    in_section = True
                    continue
                if not in_section:
                    continue
                if len(holdings) >= 10:
                    break
                if any(line.startswith(s) for s in _ANTIPODES_SKIP_LINES):
                    continue
                # Find first decimal weight in plausible holding-weight range
                weight_pct = None
                name_country = None
                for m in _ANTIPODES_DECIMAL_RE.finditer(line):
                    val = float(m.group(1))
                    if 0.1 <= val <= 20.0:
                        weight_pct = val
                        name_country = line[:m.start()].strip()
                        break
                if weight_pct is None or not name_country:
                    continue
                # Strip country: try two-word first, then one-word
                country = None
                parts = name_country.rsplit(' ', 2)
                if len(parts) >= 3:
                    candidate = f'{parts[-2]} {parts[-1]}'
                    if candidate in _ANTIPODES_TWO_WORD_COUNTRIES:
                        country = candidate
                        name_country = parts[0]
                if country is None:
                    parts = name_country.rsplit(' ', 1)
                    if len(parts) == 2 and parts[-1][0].isupper():
                        country = parts[-1]
                        name_country = parts[0]
                name = name_country.strip()
                if not name or name[0].isdigit():
                    continue
                holdings.append({'name': name, 'ticker': None,
                                 'weight_pct': weight_pct, 'sector': None, 'country': country})
            if holdings:
                break
    return holdings


def scrape_antipodes(db_path=None) -> int:
    source = 'Antipodes'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    for asx_code, url in _ANTIPODES_FUNDS.items():
        try:
            resp = fetch(url)
            if not resp or resp.status_code != 200:
                logger.warning(f'Antipodes: PDF not accessible for {asx_code}')
                continue
            holdings = _parse_antipodes_pdf(resp.content)
            if not holdings:
                logger.warning(f'Antipodes: no holdings parsed for {asx_code}')
                continue
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Antipodes: {asx_code} — {len(holdings)} holdings')
        except Exception as e:
            logger.warning(f'Antipodes: error for {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Antipodes: updated {updated} ETFs')
    return updated


# ====================================================================
# Lakehouse Capital
# ====================================================================

# stockanalysis.com hosts clean HTML holdings tables for these equity ETFs
_STOCKANALYSIS_EQUITY_FUNDS = {
    'LHGG': 'https://stockanalysis.com/quote/asx/LHGG/holdings/',
    'FSML': 'https://stockanalysis.com/quote/asx/FSML/holdings/',
}
# Keep old name for compat
_LAKEHOUSE_FUNDS = _STOCKANALYSIS_EQUITY_FUNDS


def scrape_lakehouse(db_path=None) -> int:
    from bs4 import BeautifulSoup
    source = 'Lakehouse'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    for asx_code, url in _LAKEHOUSE_FUNDS.items():
        try:
            resp = fetch(url, headers={'Accept': 'text/html'})
            if not resp or resp.status_code != 200:
                logger.warning(f'Lakehouse: page not accessible for {asx_code}')
                continue
            soup = BeautifulSoup(resp.text, 'html.parser')
            table = soup.find('table')
            if not table:
                logger.warning(f'Lakehouse: no table found for {asx_code}')
                continue
            holdings = []
            for row in table.find_all('tr')[1:]:  # skip header
                cells = row.find_all('td')
                if len(cells) < 4:
                    continue
                # Columns: No. | Symbol | Name | Weight | Shares
                ticker = cells[1].get_text(strip=True) or None
                name = cells[2].get_text(strip=True)
                weight_str = cells[3].get_text(strip=True).rstrip('%')
                try:
                    weight_pct = float(weight_str)
                except ValueError:
                    continue
                if name and weight_pct > 0:
                    holdings.append({'name': name, 'ticker': ticker,
                                     'weight_pct': weight_pct, 'sector': None, 'country': None})
                if len(holdings) >= 10:
                    break
            if not holdings:
                logger.warning(f'Lakehouse: no holdings parsed for {asx_code}')
                continue
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Lakehouse: {asx_code} — {len(holdings)} holdings')
        except Exception as e:
            logger.warning(f'Lakehouse: error for {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Lakehouse: updated {updated} ETFs')
    return updated


# ====================================================================
# Alphinity Investment Management
# ====================================================================

_ALPHINITY_FUNDS = {
    # Factsheet PDFs served from Fidante CDN — static URLs, refreshed monthly
    'XALG': 'https://fidante.com/au/ALPH-FS-GEF.pdf',
    'XASG': 'https://fidante.com/au/ALPH-FS-GSEF.pdf',
}
# Known GICS sector names (1 or 2 words) to strip from end of name+sector string
_ALPHINITY_SECTORS = {
    'Financials', 'Health Care', 'Information Technology', 'Consumer Staples',
    'Consumer Discretionary', 'Real Estate', 'Industrials', 'Energy',
    'Materials', 'Utilities', 'Communication Services', 'Resources',
    'Technology', 'Info. Technology',
}
_ALPHINITY_WEIGHT_RE = re.compile(r'([\d]+\.[\d]+)\s*$')
# Detect PDF text-interleaving garble: word with 2+ uppercase then lowercase, or lowercase+uppercase
_ALPHINITY_GARBLE_WORD_RE = re.compile(r'^[A-Z]{2}[a-z]|^[a-z][A-Z]')


def _has_garbled_words(text: str) -> bool:
    """Return True if any word in text looks like interleaved PDF garbage."""
    for word in text.split():
        if len(word) > 5 and _ALPHINITY_GARBLE_WORD_RE.match(word):
            return True
    return False


def _strip_alphinity_sector(name_sector: str) -> tuple[str, str | None]:
    """Remove trailing GICS sector name from 'Company Sector' string."""
    parts = name_sector.rsplit(' ', 2)
    if len(parts) >= 3:
        two_word = f'{parts[-2]} {parts[-1]}'
        if two_word in _ALPHINITY_SECTORS:
            return parts[0].strip(), two_word
    parts = name_sector.rsplit(' ', 1)
    if len(parts) == 2 and parts[-1] in _ALPHINITY_SECTORS:
        return parts[0].strip(), parts[-1]
    return name_sector.strip(), None


def _parse_alphinity_factsheet_pdf(content: bytes) -> list[dict]:
    """
    Parse Alphinity monthly factsheet PDF for top 10 holdings.
    Format: 'Company Sector %' lines under 'Top 10 positions' header.
    Handles PDFs with duplicate/interleaved columns by skipping garbled lines.
    """
    import pdfplumber
    holdings = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ''
            if 'Top 10 positions' not in text:
                continue
            lines = [ln.strip() for ln in text.split('\n') if ln.strip()]
            in_section = False
            for line in lines:
                if 'Top 10 positions' in line:
                    in_section = True
                    continue
                if not in_section:
                    continue
                if len(holdings) >= 10:
                    break
                if line.startswith(('Company', 'Total', 'Data Source')):
                    continue
                m = _ALPHINITY_WEIGHT_RE.search(line)
                if not m:
                    continue
                try:
                    weight_pct = float(m.group(1))
                except ValueError:
                    continue
                if weight_pct <= 0:
                    continue
                name_sector = line[:m.start()].strip()
                # Skip garbled lines (PDF column interleaving artifacts)
                if _has_garbled_words(name_sector):
                    continue
                if not name_sector or name_sector[0].isdigit() or len(name_sector) < 3:
                    continue
                name, sector = _strip_alphinity_sector(name_sector)
                if not name:
                    continue
                holdings.append({'name': name, 'ticker': None,
                                 'weight_pct': weight_pct, 'sector': sector, 'country': None})
            if holdings:
                break
    return holdings


def scrape_alphinity(db_path=None) -> int:
    source = 'Alphinity'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    for asx_code, url in _ALPHINITY_FUNDS.items():
        try:
            resp = fetch(url)
            if not resp or resp.status_code != 200:
                logger.warning(f'Alphinity: PDF not accessible for {asx_code}')
                continue
            holdings = _parse_alphinity_factsheet_pdf(resp.content)
            if not holdings:
                logger.warning(f'Alphinity: no holdings parsed for {asx_code}')
                continue
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Alphinity: {asx_code} — {len(holdings)} holdings')
        except Exception as e:
            logger.warning(f'Alphinity: error for {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Alphinity: updated {updated} ETFs')
    return updated


# ====================================================================
# Loomis Sayles (IML)
# ====================================================================

_LOOMIS_FUNDS = {
    'LSGE': 'https://www.loomissayles.com.au/files/Latest-LSGE_FactSheet.pdf',
}
_LOOMIS_HOLDING_RE = re.compile(r'^(.+?)\s+([\d]+\.[\d]+)%')
_LOOMIS_SKIP = frozenset({
    'HOLDINGS', 'Fund', 'Total', 'Benchmark', 'SECTOR', 'WEIGHTS', 'REGIONAL',
    'Cash', 'Energy', 'Materials', 'Utilities', 'Financials', 'Industrials',
    'Communication', 'Consumer', 'Information', 'Health', 'Real',
})


def _parse_loomis_factsheet_pdf(content: bytes) -> list[dict]:
    """
    Parse Loomis Sayles factsheet PDF for top 10 holdings.
    Format: 'Company Name X.X%' followed by sector/benchmark bleed.
    Find first percentage in each line — that's the holding weight.
    """
    import pdfplumber
    holdings = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ''
            if 'HOLDINGS' not in text:
                continue
            lines = [ln.strip() for ln in text.split('\n') if ln.strip()]
            in_section = False
            for line in lines:
                if 'HOLDINGS' in line and 'SECTOR' in line:
                    in_section = True
                    continue
                if not in_section:
                    continue
                if len(holdings) >= 10:
                    break
                m = _LOOMIS_HOLDING_RE.match(line)
                if not m:
                    continue
                name = m.group(1).strip()
                if any(name.startswith(s) for s in _LOOMIS_SKIP):
                    continue
                try:
                    weight_pct = float(m.group(2))
                except ValueError:
                    continue
                if weight_pct <= 0 or not name or name[0].isdigit():
                    continue
                holdings.append({'name': name, 'ticker': None,
                                 'weight_pct': weight_pct, 'sector': None, 'country': None})
            if holdings:
                break
    return holdings


def scrape_loomis(db_path=None) -> int:
    source = 'Loomis Sayles'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    for asx_code, url in _LOOMIS_FUNDS.items():
        try:
            resp = fetch(url)
            if not resp or resp.status_code != 200:
                logger.warning(f'Loomis Sayles: PDF not accessible for {asx_code}')
                continue
            holdings = _parse_loomis_factsheet_pdf(resp.content)
            if not holdings:
                logger.warning(f'Loomis Sayles: no holdings parsed for {asx_code}')
                continue
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Loomis Sayles: {asx_code} — {len(holdings)} holdings')
        except Exception as e:
            logger.warning(f'Loomis Sayles: error for {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Loomis Sayles: updated {updated} ETFs')
    return updated


# ====================================================================
# AllianceBernstein — AMVE monthly factsheet PDF
# ====================================================================

_AB_FUNDS = {
    'AMVE': 'https://www.alliancebernstein.com/content/dam/alliancebernstein/apac/au/au-pdfs/fund-literature/MVE-Monthly-Fact-Sheet.pdf',
}

# Matches lines like: "BHP Group 5.6 9.3"  (name, portfolio %, index %)
_AB_HOLDING_RE = re.compile(r'^(.+?)\s+([\d]+\.[\d]+)\s+([\d]+\.[\d]+)\s*$')
_AB_SKIP = frozenset({'Stock Name', 'Total', 'Cash', 'Other'})


def _parse_ab_factsheet_pdf(content: bytes) -> list[dict]:
    """Parse AB monthly factsheet PDF for Top 10 Holdings.
    Format: 'Stock Name Portfolio Index' header then 'Name X.X X.X' rows."""
    import pdfplumber
    holdings = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ''
            if 'Top 10 Holdings' not in text:
                continue
            lines = [ln.strip() for ln in text.split('\n') if ln.strip()]
            in_section = False
            for line in lines:
                if 'Top 10 Holdings' in line:
                    in_section = True
                    continue
                if not in_section:
                    continue
                if len(holdings) >= 10:
                    break
                if line in _AB_SKIP or line.startswith('Stock Name'):
                    continue
                m = _AB_HOLDING_RE.match(line)
                if not m:
                    continue
                name = m.group(1).strip()
                if not name or name[0].isdigit():
                    continue
                try:
                    weight_pct = float(m.group(2))
                except ValueError:
                    continue
                if weight_pct > 0:
                    holdings.append({'name': name, 'ticker': None,
                                     'weight_pct': weight_pct, 'sector': None, 'country': None})
            if holdings:
                break
    return holdings


def scrape_ab(db_path=None) -> int:
    source = 'AllianceBernstein'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    for asx_code, url in _AB_FUNDS.items():
        try:
            resp = fetch(url)
            if not resp or resp.status_code != 200:
                logger.warning(f'AllianceBernstein: PDF not accessible for {asx_code}')
                continue
            holdings = _parse_ab_factsheet_pdf(resp.content)
            if not holdings:
                logger.warning(f'AllianceBernstein: no holdings parsed for {asx_code}')
                continue
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'AllianceBernstein: {asx_code} — {len(holdings)} holdings')
        except Exception as e:
            logger.warning(f'AllianceBernstein: error for {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'AllianceBernstein: updated {updated} ETFs')
    return updated


# ====================================================================
# Nanuk — monthly report PDF (NNUK + NNWH share same holdings)
# ====================================================================

_NANUK_REPORT_PAGE = 'https://www.nanukasset.com/monthly-reports/'
# Both codes get the same holdings (unhedged vs hedged of same fund)
_NANUK_CODES = ['NNUK', 'NNWH']

# Matches "Taiwan Semiconductor Manufacturing Co., Ltd. 4.8 TAIWAN Sustainable & Efficient Industry"
# Weight is a bare decimal; country is all-caps; sector follows
_NANUK_HOLDING_RE = re.compile(r'^(.+?)\s+([\d]+\.[\d]+)\s+([A-Z][A-Z\s]+)\s+(.+)$')
_NANUK_SKIP = frozenset({'Security Name', 'Total'})


def _parse_nanuk_report_pdf(content: bytes) -> list[dict]:
    """Parse Nanuk monthly report PDF for Top 10 Holdings on page 3."""
    import pdfplumber
    holdings = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ''
            if 'Top 10 Holdings' not in text:
                continue
            lines = [ln.strip() for ln in text.split('\n') if ln.strip()]
            in_section = False
            for line in lines:
                if 'Top 10 Holdings' in line:
                    in_section = True
                    continue
                if not in_section:
                    continue
                if len(holdings) >= 10:
                    break
                if line in _NANUK_SKIP or line.startswith('Security Name'):
                    continue
                # Format: "Name Weight(%) COUNTRY Sector"
                m = _NANUK_HOLDING_RE.match(line)
                if not m:
                    continue
                name = m.group(1).strip()
                if not name or name[0].isdigit():
                    continue
                try:
                    weight_pct = float(m.group(2))
                except ValueError:
                    continue
                if weight_pct <= 0:
                    continue
                country = m.group(3).strip().title()
                holdings.append({'name': name, 'ticker': None,
                                 'weight_pct': weight_pct, 'sector': None, 'country': country})
            if holdings:
                break
    return holdings


def scrape_nanuk(db_path=None) -> int:
    from bs4 import BeautifulSoup
    source = 'Nanuk'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    # Fetch the monthly reports page to find the latest PDF URL
    resp = fetch(_NANUK_REPORT_PAGE)
    if not resp:
        logger.warning('Nanuk: could not fetch monthly reports page')
        duration = (datetime.utcnow() - started).total_seconds()
        log_scrape(conn, source, 'no_data', duration_secs=duration, started_at=started.isoformat())
        conn.close()
        return 0

    soup = BeautifulSoup(resp.text, 'html.parser')
    pdf_links = [a['href'] for a in soup.find_all('a', href=True)
                 if a['href'].endswith('.pdf') and 'NNWF' in a['href']]
    if not pdf_links:
        logger.warning('Nanuk: no PDF links found on monthly reports page')
        duration = (datetime.utcnow() - started).total_seconds()
        log_scrape(conn, source, 'no_data', duration_secs=duration, started_at=started.isoformat())
        conn.close()
        return 0

    latest_url = pdf_links[0]
    logger.info(f'Nanuk: latest report URL: {latest_url}')

    pdf_resp = fetch(latest_url)
    if not pdf_resp:
        logger.warning(f'Nanuk: could not download PDF from {latest_url}')
        duration = (datetime.utcnow() - started).total_seconds()
        log_scrape(conn, source, 'no_data', duration_secs=duration, started_at=started.isoformat())
        conn.close()
        return 0

    holdings = _parse_nanuk_report_pdf(pdf_resp.content)
    if not holdings:
        logger.warning(f'Nanuk: no holdings parsed from {latest_url}')
        duration = (datetime.utcnow() - started).total_seconds()
        log_scrape(conn, source, 'no_data', duration_secs=duration, started_at=started.isoformat())
        conn.close()
        return 0

    for asx_code in _NANUK_CODES:
        try:
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Nanuk: {asx_code} — {len(holdings)} holdings')
        except Exception as e:
            logger.warning(f'Nanuk: error for {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Nanuk: updated {updated} ETFs')
    return updated


# ====================================================================
# ClearBridge / Franklin Templeton — factsheet PDFs
# ====================================================================

# R3AL: static GUID-based URL on franklintempleton.com.au
# CIIH/CIVH/CUIV: dynamic date-based URL fetched from etf.franklintempleton.com/{code}/
_CB_STATIC_FUNDS = {
    'R3AL': 'https://www.franklintempleton.com.au/download/en-au/factsheet/18a9f4c8-3381-4380-a4f9-308391a82e0f/Factsheet-ClearBridge-Real-Income-Fund-A.pdf',
}
_CB_DYNAMIC_FUNDS = {
    'CIIH': 'https://etf.franklintempleton.com/ciih/',
    'CIVH': 'https://etf.franklintempleton.com/civh/',
    'CUIV': 'https://etf.franklintempleton.com/cuiv/',
}

# "TOP 10 POSITIONS END WEIGHT %" and "Top Equity Issuers (% of Total)"
_CB_SECTION_RE = re.compile(r'TOP 10 POSITIONS|Top Equity Issuers', re.I)
_CB_DECIMAL_RE = re.compile(r'([\d]+\.[\d]+)')
_CB_SECTOR_WORDS = frozenset({
    'Electric', 'Renewables', 'Gas', 'Water', 'Energy', 'Infra', 'Toll',
    'Roads', 'Road', 'Rail', 'Airports', 'Airport', 'Other', 'Cash',
    'Latin', 'America', 'USA', 'Canada', 'Western', 'Europe', 'Asia',
    'Pacific', 'Pac', 'Dev', 'Emerging', 'Markets', 'Infrastructure',
    'Utilities', 'Contracted', 'Regulated', 'User', 'Pays', 'Fund',
    'Years', 'Firm', 'Experience', 'Portfolio', 'Management',
    '&', '-', '/', '|',
})
_CB_SKIP_LINES = frozenset({'Top 10 Total', 'END WEIGHT', 'TOP 10', 'Cash'})


def _parse_cb_factsheet_pdf(content: bytes) -> list[dict]:
    """
    Parse ClearBridge factsheet PDF for Top 10 holdings.
    Two-column bleed: sector labels (left) interleave with company names (right).
    Strategy: find first decimal in [1, 20], strip leading sector-noise words from name.
    """
    import pdfplumber
    holdings = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ''
            m = _CB_SECTION_RE.search(text)
            if not m:
                continue
            section = text[m.start():]
            lines = [ln.strip() for ln in section.split('\n') if ln.strip()]
            in_section = True  # we're already in the section
            for line in lines[1:]:  # skip the header line
                if len(holdings) >= 10:
                    break
                if any(skip in line for skip in _CB_SKIP_LINES):
                    continue
                weight_pct = None
                name_part = None
                for dm in _CB_DECIMAL_RE.finditer(line):
                    val = float(dm.group(1))
                    if 1.0 <= val <= 20.0:
                        weight_pct = val
                        name_part = line[:dm.start()].strip()
                        break
                if weight_pct is None or not name_part:
                    continue
                # Strip leading sector-noise words
                words = name_part.split()
                start = 0
                for i, w in enumerate(words):
                    w_clean = w.strip('.,&/-|')
                    # Skip if empty, single-char symbol, or known sector word
                    if not w_clean or len(w_clean) == 1 or w_clean in _CB_SECTOR_WORDS or w_clean.isdigit():
                        continue
                    start = i
                    break
                name = ' '.join(words[start:]).strip()
                if not name or name[0].isdigit() or len(name) < 2:
                    continue
                holdings.append({'name': name, 'ticker': None,
                                 'weight_pct': weight_pct, 'sector': None, 'country': None})
            if holdings:
                break
    return holdings


def scrape_clearbridge(db_path=None) -> int:
    source = 'ClearBridge / Franklin Templeton'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    # Static URL funds
    for asx_code, pdf_url in _CB_STATIC_FUNDS.items():
        try:
            resp = fetch(pdf_url)
            if not resp or resp.status_code != 200 or b'%PDF' not in resp.content[:10]:
                logger.warning(f'ClearBridge: PDF not accessible for {asx_code}')
                continue
            holdings = _parse_cb_factsheet_pdf(resp.content)
            if not holdings:
                logger.warning(f'ClearBridge: no holdings parsed for {asx_code}')
                continue
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'ClearBridge: {asx_code} — {len(holdings)} holdings')
        except Exception as e:
            logger.warning(f'ClearBridge: error for {asx_code}: {e}')
            conn.rollback()

    # Dynamic URL funds: fetch ETF page to find current factsheet URL
    for asx_code, etf_page_url in _CB_DYNAMIC_FUNDS.items():
        try:
            page_resp = fetch(etf_page_url)
            if not page_resp:
                logger.warning(f'ClearBridge: could not fetch ETF page for {asx_code}')
                continue
            # Find factsheet PDF link
            pdf_match = re.search(
                r'href="(/FormBuilder/_Resource/_module/[^"]+/file[s]?/FACTSHEET[^"]+\.pdf)"',
                page_resp.text
            )
            if not pdf_match:
                logger.warning(f'ClearBridge: no factsheet PDF link found for {asx_code}')
                continue
            pdf_url = 'https://etf.franklintempleton.com' + pdf_match.group(1)
            pdf_resp = fetch(pdf_url)
            if not pdf_resp or b'%PDF' not in pdf_resp.content[:10]:
                logger.warning(f'ClearBridge: could not download PDF for {asx_code}')
                continue
            holdings = _parse_cb_factsheet_pdf(pdf_resp.content)
            if not holdings:
                logger.warning(f'ClearBridge: no holdings parsed for {asx_code}')
                continue
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'ClearBridge: {asx_code} — {len(holdings)} holdings')
        except Exception as e:
            logger.warning(f'ClearBridge: error for {asx_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'ClearBridge: updated {updated} ETFs')
    return updated


# ====================================================================
# Aoris Investment Management
# ====================================================================
# DAOR = Aoris International Fund (Hedged) Class D
# BAOR = Aoris International Fund (Unhedged) Class B
# Both hold the same ~15-stock concentrated global equity portfolio.
# Monthly reports at: https://www.aoris.com.au/performance

_AORIS_PERF_PAGE = 'https://www.aoris.com.au/performance'
_AORIS_CODES = ['DAOR', 'BAOR']


_AORIS_REGION_MAP = {
    'US': 'United States', 'UK': 'United Kingdom',
    'EUR': 'Europe', 'AU': 'Australia', 'JP': 'Japan',
}
# Known region codes and skip phrases
_AORIS_SKIP = frozenset({'Company', 'Region', 'Sector', 'Portfolio holdings', 'Monthly Report'})


def _parse_aoris_monthly_pdf(content: bytes) -> list[dict]:
    """
    Parse Aoris monthly report PDF for portfolio holdings.
    The PDF has no table lines; uses text extraction from cropped left column.
    Line format: "Company  REGION  Sector"
    No weights published; returns weight_pct=None.
    """
    import pdfplumber
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            page = pdf.pages[0]
            # Crop to left column to avoid revenue exposure pie chart
            left_crop = page.crop((0, 480, 295, page.height))
            text = left_crop.extract_text() or ''

        holdings = []
        for line in text.split('\n'):
            line = line.strip()
            if not line or line in _AORIS_SKIP:
                continue
            # Lines look like: "Accenture US Business Services"
            # Split on whitespace; region is the second token (short code)
            parts = line.split()
            if len(parts) < 2:
                continue
            # Find the region code (one of our known 2-3 char codes)
            region_idx = None
            for idx, tok in enumerate(parts):
                if tok in _AORIS_REGION_MAP and idx > 0:
                    region_idx = idx
                    break
            if region_idx is None:
                continue
            name = ' '.join(parts[:region_idx])
            region = parts[region_idx]
            sector = ' '.join(parts[region_idx + 1:]) or None
            country = _AORIS_REGION_MAP.get(region, region)
            if not name:
                continue
            holdings.append({
                'name': name,
                'ticker': None,
                'weight_pct': None,
                'sector': sector,
                'country': country,
            })
    except Exception as e:
        logger.warning(f'Aoris: PDF parse error: {e}')
        return []
    return holdings


def scrape_aoris(db_path=None) -> int:
    from bs4 import BeautifulSoup
    source = 'Aoris'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    try:
        page_resp = fetch(_AORIS_PERF_PAGE)
        if not page_resp:
            logger.warning('Aoris: could not fetch performance page')
            log_scrape(conn, source, 'error', error='page fetch failed',
                       started_at=started.isoformat())
            conn.close()
            return 0

        soup = BeautifulSoup(page_resp.text, 'html.parser')
        # Find unhedged monthly report URL (latest)
        pdf_url = None
        for a in soup.find_all('a', href=True):
            href = a['href']
            if 'Monthly_Report' in href and 'Unhedged' in href and href.endswith('.pdf'):
                pdf_url = href
                break

        if not pdf_url:
            logger.warning('Aoris: no monthly report PDF found on performance page')
            log_scrape(conn, source, 'no_data', started_at=started.isoformat())
            conn.close()
            return 0

        pdf_resp = fetch(pdf_url)
        if not pdf_resp or b'%PDF' not in pdf_resp.content[:10]:
            logger.warning(f'Aoris: could not download PDF from {pdf_url}')
            log_scrape(conn, source, 'error', error='PDF download failed',
                       started_at=started.isoformat())
            conn.close()
            return 0

        holdings = _parse_aoris_monthly_pdf(pdf_resp.content)
        if not holdings:
            logger.warning('Aoris: no holdings parsed from PDF')
            log_scrape(conn, source, 'no_data', started_at=started.isoformat())
            conn.close()
            return 0

        for asx_code in _AORIS_CODES:
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Aoris: {asx_code} — {len(holdings)} holdings')

    except Exception as e:
        logger.warning(f'Aoris: error: {e}')
        conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Aoris: updated {updated} ETFs')
    return updated


# ====================================================================
# Vaughan Nelson (VNGS)
# ====================================================================
# VNGS = Vaughan Nelson Global Equity SMID Fund Active ETF
# Monthly fund update PDF at stable URL; page 2 has TOP HOLDINGS table.

_VN_FUND_UPDATE_URL = 'https://vaughannelson.com.au/files/FundUpdate-VN-GE-SMID-FUND.pdf'
_VN_CODES = ['VNGS']


def _parse_vn_fund_update(content: bytes) -> list[dict]:
    """
    Parse Vaughan Nelson fund update PDF for Top Holdings.
    Page 2, left column (x: 0–300): table with columns [Name, Country, Sector, Weight%].
    """
    import pdfplumber
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            if len(pdf.pages) < 2:
                return []
            page = pdf.pages[1]
            # Crop to TOP HOLDINGS section on left side
            left_crop = page.crop((0, 85, 300, 370))
            table = left_crop.extract_table()
            if not table:
                return []
            holdings = []
            for row in table:
                if not row or not row[0]:
                    continue
                name = str(row[0]).strip().replace('\n', ' ')
                if not name or 'Company' in name or 'TOP HOLDINGS' in name:
                    continue
                country = str(row[1] or '').strip().replace('\n', ' ') if len(row) > 1 else None
                sector = str(row[2] or '').strip().replace('\n', ' ') if len(row) > 2 else None
                weight_str = str(row[3] or '').strip() if len(row) > 3 else ''
                try:
                    weight_pct = float(weight_str.replace('%', '').strip())
                except (ValueError, AttributeError):
                    weight_pct = None
                holdings.append({
                    'name': name,
                    'ticker': None,
                    'weight_pct': weight_pct,
                    'sector': sector or None,
                    'country': country or None,
                })
    except Exception as e:
        logger.warning(f'Vaughan Nelson: PDF parse error: {e}')
        return []
    return holdings


def scrape_vaughan_nelson(db_path=None) -> int:
    source = 'Vaughan Nelson'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    try:
        resp = fetch(_VN_FUND_UPDATE_URL)
        if not resp or b'%PDF' not in resp.content[:10]:
            logger.warning('Vaughan Nelson: could not download fund update PDF')
            log_scrape(conn, source, 'error', error='PDF download failed',
                       started_at=started.isoformat())
            conn.close()
            return 0

        holdings = _parse_vn_fund_update(resp.content)
        if not holdings:
            logger.warning('Vaughan Nelson: no holdings parsed from PDF')
            log_scrape(conn, source, 'no_data', started_at=started.isoformat())
            conn.close()
            return 0

        for asx_code in _VN_CODES:
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Vaughan Nelson: {asx_code} — {len(holdings)} holdings')

    except Exception as e:
        logger.warning(f'Vaughan Nelson: error: {e}')
        conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Vaughan Nelson: updated {updated} ETFs')
    return updated


# ====================================================================
# Firetrail (S3GO, FIRE)
# ====================================================================
# S3GO = Firetrail S3 Global Opportunities Fund Active ETF
# Quarterly ASX portfolio disclosure PDF at stable URL.
# Format: alternating lines of security name and weight% (e.g. "Apple Inc\n7.3%")
# or single-line "Cash 9.1%"

_FIRETRAIL_HOLDINGS_URL = 'https://firetrail.com/wp-content/uploads/S3GO-Portfolio-Holdings.pdf'
_FIRETRAIL_CODES = ['S3GO']

# FIRE metadata — static fields known from the fund page (no holdings PDF available yet)
_FIRETRAIL_FIRE_META = {
    'code': 'FIRE',
    'name': 'Firetrail Alpha Plus Fund - Complex ETF',
    'issuer': 'Firetrail',
    'exchange': 'ASX',
    'fund_type': 'Complex ETF',
    'benchmark': 'S&P/ASX 200 Accumulation Index',
    'distribution_frequency': 'Semi-annually',
    'inception_date': '2026-03-04',
    'issuer_url': 'https://firetrail.com/funds/firetrail-alpha-plus-fund-complex-etf/',
    'data_source': 'firetrail',
}
_FIRETRAIL_WEIGHT_RE = re.compile(r'^([\d]+\.[\d]+)%$')


def _parse_firetrail_holdings_pdf(content: bytes) -> list[dict]:
    """
    Parse Firetrail S3GO quarterly ASX portfolio holdings PDF.
    Lines alternate: security name then weight percentage (e.g. "Apple Inc" / "7.3%"),
    or appear on a single line for "Cash X.X%".
    """
    import pdfplumber
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            all_lines = []
            for page in pdf.pages:
                text = page.extract_text() or ''
                # Find the holdings section
                idx = text.find('Security Weighting')
                if idx >= 0:
                    text = text[idx + len('Security Weighting'):]
                for ln in text.split('\n'):
                    ln = ln.strip()
                    if ln:
                        all_lines.append(ln)

        holdings = []
        i = 0
        while i < len(all_lines):
            line = all_lines[i]
            # Check if this line ends with a weight (e.g. "Cash 9.1%")
            m_inline = re.search(r'\s+([\d]+\.[\d]+)%$', line)
            if m_inline:
                name = line[:m_inline.start()].strip()
                try:
                    weight_pct = float(m_inline.group(1))
                except ValueError:
                    weight_pct = None
                if name and name.lower() != 'cash' and weight_pct:
                    holdings.append({'name': name, 'ticker': None,
                                     'weight_pct': weight_pct, 'sector': None, 'country': None})
                i += 1
            elif i + 1 < len(all_lines) and _FIRETRAIL_WEIGHT_RE.match(all_lines[i + 1]):
                # Next line is the weight
                name = line
                try:
                    weight_pct = float(all_lines[i + 1].replace('%', ''))
                except ValueError:
                    weight_pct = None
                if name and weight_pct:
                    holdings.append({'name': name, 'ticker': None,
                                     'weight_pct': weight_pct, 'sector': None, 'country': None})
                i += 2
            else:
                i += 1

        return holdings
    except Exception as e:
        logger.warning(f'Firetrail: PDF parse error: {e}')
        return []


def scrape_firetrail(db_path=None) -> int:
    source = 'Firetrail'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    try:
        # Always upsert static metadata for FIRE (no holdings PDF available)
        upsert_etf(conn, _FIRETRAIL_FIRE_META)
        conn.commit()
        updated += 1
        logger.info('Firetrail: upserted FIRE metadata')

        # S3GO holdings from quarterly PDF
        resp = fetch(_FIRETRAIL_HOLDINGS_URL)
        if not resp or b'%PDF' not in resp.content[:10]:
            logger.warning('Firetrail: could not download S3GO portfolio holdings PDF')
        else:
            holdings = _parse_firetrail_holdings_pdf(resp.content)
            if not holdings:
                logger.warning('Firetrail: no holdings parsed from S3GO PDF')
            else:
                for asx_code in _FIRETRAIL_CODES:
                    upsert_holdings(conn, asx_code, holdings)
                    updated += 1
                    logger.info(f'Firetrail: {asx_code} — {len(holdings)} holdings')

    except Exception as e:
        logger.warning(f'Firetrail: error: {e}')
        conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Firetrail: updated {updated} ETFs')
    return updated


# ====================================================================
# Milford Asset Management (MFOA)
# ====================================================================
# MFOA = Milford Australian Absolute Growth Complex ETF
# Monthly factsheets for the R-Class/W-Class fund use the same portfolio.
# The fund-reports page links to the latest monthly PDF.
# Page 2 has "Top Security Holdings" table: "Name  X.XX%"

_MILFORD_REPORTS_PAGE = 'https://milfordasset.com.au/fund-reports/'
_MILFORD_CODES = ['MFOA']
# Matches first "Name  X.XX%" on a line (right-column bleed is after the first %)
_MILFORD_WEIGHT_RE = re.compile(r'^([A-Za-z].*?)\s+([\d]+\.[\d]+)%')
_MILFORD_SKIP = frozenset({'Holdings % of Fund Actual', 'Top Security Holdings Current Asset Allocation',
                            'Typical Maximum'})


def _parse_milford_monthly_pdf(content: bytes) -> list[dict]:
    """
    Parse Milford monthly factsheet PDF for Top Security Holdings.
    Page 2 has rows like 'BHP Group 4.17% Australasian Equities 67.03%...' (two-column bleed).
    Strategy: find first X.XX% per line; name is everything before it.
    """
    import pdfplumber
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ''
                if 'Top Security Holdings' not in text:
                    continue
                holdings = []
                in_section = False
                for line in text.split('\n'):
                    line = line.strip()
                    if 'Top Security Holdings' in line:
                        in_section = True
                        continue
                    if not in_section:
                        continue
                    if not line:
                        continue
                    # Stop at clearly non-holdings sections
                    if any(kw in line for kw in ['Sector Allocation', 'Fund Changes', 'Past performance']):
                        break
                    # Skip known header/noise lines
                    if line in _MILFORD_SKIP or line.startswith('Range') or line.startswith('Typical'):
                        continue
                    m = _MILFORD_WEIGHT_RE.match(line)
                    if not m:
                        continue
                    name = m.group(1).strip()
                    # Skip asset-class lines (e.g. "Australasian Equities", "Cash and Other")
                    if any(kw in name for kw in ['Equities', 'Derivatives', 'International', 'Cash', 'Holdings']):
                        continue
                    if not name or name[0].isdigit():
                        continue
                    try:
                        weight_pct = float(m.group(2))
                    except ValueError:
                        continue
                    if weight_pct > 0:
                        holdings.append({'name': name, 'ticker': None,
                                         'weight_pct': weight_pct, 'sector': None, 'country': None})
                if holdings:
                    return holdings
    except Exception as e:
        logger.warning(f'Milford: PDF parse error: {e}')
    return []


def scrape_milford(db_path=None) -> int:
    from bs4 import BeautifulSoup
    source = 'Milford'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    try:
        page_resp = fetch(_MILFORD_REPORTS_PAGE)
        if not page_resp:
            logger.warning('Milford: could not fetch fund reports page')
            log_scrape(conn, source, 'error', error='page fetch failed',
                       started_at=started.isoformat())
            conn.close()
            return 0

        soup = BeautifulSoup(page_resp.text, 'html.parser')
        # Find the latest R-Class monthly report (same portfolio as MFOA)
        pdf_url = None
        for a in soup.find_all('a', href=True):
            href = a['href']
            if 'R_Class' in href and href.endswith('.pdf') and 'Absolute_Growth' in href:
                pdf_url = href
                break

        if not pdf_url:
            logger.warning('Milford: no R-Class monthly report PDF found')
            log_scrape(conn, source, 'no_data', started_at=started.isoformat())
            conn.close()
            return 0

        pdf_resp = fetch(pdf_url)
        if not pdf_resp or b'%PDF' not in pdf_resp.content[:10]:
            logger.warning(f'Milford: could not download PDF from {pdf_url}')
            log_scrape(conn, source, 'error', error='PDF download failed',
                       started_at=started.isoformat())
            conn.close()
            return 0

        holdings = _parse_milford_monthly_pdf(pdf_resp.content)
        if not holdings:
            logger.warning('Milford: no holdings parsed from PDF')
            log_scrape(conn, source, 'no_data', started_at=started.isoformat())
            conn.close()
            return 0

        for asx_code in _MILFORD_CODES:
            upsert_holdings(conn, asx_code, holdings)
            updated += 1
            logger.info(f'Milford: {asx_code} — {len(holdings)} holdings')

    except Exception as e:
        logger.warning(f'Milford: error: {e}')
        conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'Milford: updated {updated} ETFs')
    return updated


# ================================================================ Franklin Templeton
_FT_ETF_PAGES = {
    'FRGG': 'https://etf.franklintempleton.com/frgg/',
}

_FT_SKIP_STARTS = frozenset({
    'Franklin', 'Fund', 'Top', 'Portfolio', 'Class', 'Performance',
    'Total', 'Emerging', 'North', 'Europe', 'Japan', 'United',
    'Asia', 'Cash', 'Allinvest', 'Past', 'Issued',
})


def _parse_ft_factsheet_pdf(content: bytes) -> list[dict]:
    """
    Parse a Franklin Templeton factsheet PDF for Top Equity Issuers.
    The holdings table is on page 2, left column; format: 'NAME  X.XX'.
    Crops the left column (x: 0–270) to avoid right-column bleed.
    """
    import pdfplumber
    weight_re = re.compile(r'^([A-Z][A-Z0-9 &.,\'-]+?)\s+([\d]+\.[\d]+)')
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                full_text = page.extract_text() or ''
                if 'Top Equity Issuers' not in full_text:
                    continue
                left = page.crop((0, 50, 270, page.height * 0.45))
                text = left.extract_text() or ''
                holdings = []
                in_section = False
                for line in text.split('\n'):
                    line = line.strip()
                    if 'Top Equity Issuers' in line:
                        in_section = True
                        continue
                    if not in_section or not line:
                        continue
                    if 'Performance Attribution' in line:
                        break
                    m = weight_re.match(line)
                    if m:
                        name = m.group(1).strip()
                        if any(name.startswith(w) for w in _FT_SKIP_STARTS):
                            continue
                        try:
                            weight = float(m.group(2))
                            if 0 < weight < 20:
                                holdings.append({
                                    'name': name.title(), 'ticker': None,
                                    'weight_pct': weight, 'sector': None, 'country': None,
                                })
                        except ValueError:
                            pass
                if holdings:
                    return holdings
    except Exception as e:
        logger.warning(f'Franklin Templeton: PDF parse error: {e}')
    return []


def scrape_franklin_templeton(db_path=None) -> int:
    """
    Scrape Franklin Templeton ETF factsheets (FRGG) for top holdings.
    Fetches the ETF page → Widen viewer → PDF download → parses holdings.
    """
    source = 'franklin_templeton'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    for code, page_url in _FT_ETF_PAGES.items():
        try:
            resp = fetch(page_url)
            if not resp or resp.status_code != 200:
                logger.warning(f'FT: could not load ETF page for {code}')
                continue

            widen_urls = re.findall(
                r'https://franklintempletonprod\.widen\.net/s/[^"\'<>\s]+',
                resp.text,
            )
            factsheet_url = next(
                (u for u in widen_urls
                 if 'factsheet' in u.lower() and 'modern-slavery' not in u.lower()),
                None,
            )
            if not factsheet_url:
                logger.warning(f'FT: no Widen factsheet URL for {code}')
                continue

            viewer_resp = fetch(factsheet_url)
            if not viewer_resp or viewer_resp.status_code != 200:
                logger.warning(f'FT: Widen viewer failed for {code}')
                continue

            dl_matches = re.findall(
                r'href="(/content/[^"]+/original/[^"]+\.pdf[^"]*)"',
                viewer_resp.text,
            )
            if not dl_matches:
                logger.warning(f'FT: no PDF download link in Widen viewer for {code}')
                continue

            pdf_url = 'https://franklintempletonprod.widen.net' + dl_matches[0].replace('&amp;', '&')
            pdf_resp = fetch(pdf_url)
            if not pdf_resp or pdf_resp.status_code != 200:
                logger.warning(f'FT: PDF download failed for {code}')
                continue

            holdings = _parse_ft_factsheet_pdf(pdf_resp.content)
            if not holdings:
                logger.warning(f'FT: no holdings parsed for {code}')
                continue

            upsert_holdings(conn, code, holdings, commit=True)
            updated += 1
            logger.info(f'FT: {code}: {len(holdings)} holdings')
        except Exception as e:
            logger.warning(f'FT: error for {code}: {e}')

    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, started_at=started.isoformat())
    conn.close()
    logger.info(f'Franklin Templeton: updated {updated} ETFs')
    return updated


# ================================================================ InvestSMART
_INVESTSMART_CODES = ['IIGF', 'INIF', 'INES', 'IISV']

_INVESTSMART_EXCHANGE_COUNTRY = {
    'ASX': 'Australia', 'NYS': 'United States', 'NAS': 'United States',
    'TSX': 'Canada', 'LSE': 'United Kingdom', 'NZX': 'New Zealand',
    'AMS': 'Netherlands', 'EPA': 'France', 'HKG': 'Hong Kong',
    'TYO': 'Japan', 'SGX': 'Singapore',
}

_INVESTSMART_ROW_RE = re.compile(r'^(\S+)\s+(.+?)\s+([A-Z]{2,4})\s+[\d,]+\s+([\d.]+)%$')


def _parse_investsmart_portfolio_pdf(content: bytes) -> list[dict]:
    """
    Parse an InvestSMART monthly portfolio disclosure PDF.
    Format: header 'Code Name Exchange Units Weight', then rows:
    'TICKER  Full Company Name  EXCHANGE  1,234  5.6%'
    """
    import pdfplumber
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            all_text = '\n'.join(p.extract_text() or '' for p in pdf.pages)
    except Exception as e:
        logger.warning(f'InvestSMART: PDF parse error: {e}')
        return []

    holdings = []
    in_section = False
    for line in all_text.split('\n'):
        line = line.strip()
        if line.startswith('Code Name Exchange'):
            in_section = True
            continue
        if not in_section or not line:
            continue
        if line.startswith(('A currency', 'Yours', 'About', 'Pursuant')):
            continue
        m = _INVESTSMART_ROW_RE.match(line)
        if m:
            ticker = m.group(1)
            name = m.group(2).strip()
            exchange = m.group(3)
            weight = float(m.group(4))
            if ticker.upper() == 'CASH':
                continue
            holdings.append({
                'name': name.title(), 'ticker': ticker,
                'weight_pct': weight, 'sector': None,
                'country': _INVESTSMART_EXCHANGE_COUNTRY.get(exchange),
            })
    return holdings


def scrape_investsmart(db_path=None) -> int:
    """
    Scrape InvestSMART monthly portfolio disclosures for IIGF, INIF, INES, IISV.
    Fetches the InvestSMART fund page to find the latest 'Portfolio Update' PDF,
    then downloads and parses it.
    """
    source = 'investsmart'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    for code in _INVESTSMART_CODES:
        try:
            page_url = f'https://www.investsmart.com.au/shares/asx-{code.lower()}/'
            resp = fetch(page_url)
            if not resp or resp.status_code != 200:
                logger.warning(f'InvestSMART: could not load page for {code}')
                continue

            # Find latest 'Portfolio Update' announcement PDF
            anchors = re.findall(
                r'<a[^>]*href="(https://www\.aspecthuntley\.com\.au/asxdata/[^"]+\.pdf)"[^>]*>(.*?)</a>',
                resp.text, re.S,
            )
            portfolio_url = None
            for href, anchor_text in anchors:
                clean = re.sub(r'\s+', ' ', anchor_text).lower()
                if 'portfolio update' in clean or 'portfolio disclosure' in clean:
                    portfolio_url = href
                    break

            if not portfolio_url:
                logger.warning(f'InvestSMART: no portfolio PDF found for {code}')
                continue

            pdf_resp = fetch(portfolio_url)
            if not pdf_resp or pdf_resp.status_code != 200 or len(pdf_resp.content) < 1000:
                logger.warning(f'InvestSMART: portfolio PDF download failed for {code}')
                continue

            holdings = _parse_investsmart_portfolio_pdf(pdf_resp.content)
            if not holdings:
                logger.warning(f'InvestSMART: no holdings parsed for {code}')
                continue

            upsert_holdings(conn, code, holdings, commit=True)
            updated += 1
            logger.info(f'InvestSMART: {code}: {len(holdings)} holdings')
        except Exception as e:
            logger.warning(f'InvestSMART: error for {code}: {e}')

    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, started_at=started.isoformat())
    conn.close()
    logger.info(f'InvestSMART: updated {updated} ETFs')
    return updated


# ================================================================ Barrow Hanley (Perpetual)
# GLOB publishes Quarterly Portfolio Disclosures as ASX announcements via aspecthuntley.com.au
_BARROW_HANLEY_CODES = ['GLOB']

_BARROW_HANLEY_WEIGHT_RE = re.compile(r'^([A-Z][A-Z0-9 &.,\'\-]+?)\s+([\d]+\.[\d]+)%$')
_BARROW_HANLEY_SKIP = frozenset({'Cash', 'Cash And Equivalents', 'Security Name'})


def _parse_barrow_hanley_quarterly_pdf(content: bytes) -> list[dict]:
    """
    Parse a Barrow Hanley quarterly portfolio disclosure PDF.
    Format: 'Security name  Portfolio weight %' header, then 'NAME  X.X%' rows.
    """
    import pdfplumber
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            all_text = '\n'.join(p.extract_text() or '' for p in pdf.pages)
    except Exception as e:
        logger.warning(f'Barrow Hanley: PDF parse error: {e}')
        return []

    holdings = []
    in_section = False
    for line in all_text.split('\n'):
        line = line.strip()
        if 'Security name' in line and 'Portfolio weight' in line:
            in_section = True
            continue
        if not in_section or not line:
            continue
        if line.startswith(('Perpetual', 'Barrow', 'ASX', 'This document')):
            continue
        m = _BARROW_HANLEY_WEIGHT_RE.match(line)
        if m:
            name = m.group(1).strip().title()
            if name in _BARROW_HANLEY_SKIP or name.lower() == 'cash':
                continue
            try:
                weight = float(m.group(2))
                if weight > 0:
                    holdings.append({
                        'name': name, 'ticker': None,
                        'weight_pct': weight, 'sector': None, 'country': None,
                    })
            except ValueError:
                pass
    return holdings


def scrape_barrow_hanley(db_path=None) -> int:
    """
    Scrape Barrow Hanley GLOB quarterly portfolio disclosure from ASX announcements.
    Fetches the InvestSMART page to find the latest 'Quarterly Portfolio Disclosure' PDF.
    """
    source = 'barrow_hanley'
    started = datetime.utcnow()
    conn = get_connection(db_path)
    updated = 0

    for code in _BARROW_HANLEY_CODES:
        try:
            page_url = f'https://www.investsmart.com.au/shares/asx-{code.lower()}/'
            resp = fetch(page_url)
            if not resp or resp.status_code != 200:
                logger.warning(f'Barrow Hanley: could not load page for {code}')
                continue

            anchors = re.findall(
                r'<a[^>]*href="(https://www\.aspecthuntley\.com\.au/asxdata/[^"]+\.pdf)"[^>]*>(.*?)</a>',
                resp.text, re.S,
            )
            portfolio_url = None
            for href, anchor_text in anchors:
                clean = re.sub(r'\s+', ' ', anchor_text).lower()
                if 'quarterly portfolio disclosure' in clean or 'portfolio disclosure' in clean:
                    portfolio_url = href
                    break

            if not portfolio_url:
                logger.warning(f'Barrow Hanley: no portfolio disclosure PDF found for {code}')
                continue

            pdf_resp = fetch(portfolio_url)
            if not pdf_resp or pdf_resp.status_code != 200 or len(pdf_resp.content) < 1000:
                logger.warning(f'Barrow Hanley: portfolio PDF download failed for {code}')
                continue

            holdings = _parse_barrow_hanley_quarterly_pdf(pdf_resp.content)
            if not holdings:
                logger.warning(f'Barrow Hanley: no holdings parsed for {code}')
                continue

            upsert_holdings(conn, code, holdings, commit=True)
            updated += 1
            logger.info(f'Barrow Hanley: {code}: {len(holdings)} holdings')
        except Exception as e:
            logger.warning(f'Barrow Hanley: error for {code}: {e}')

    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, started_at=started.isoformat())
    conn.close()
    logger.info(f'Barrow Hanley: updated {updated} ETFs')
    return updated


# ====================================================================
# Orchestrator
# ====================================================================

def scrape_all_issuers(db_path=None) -> int:
    """Run all issuer scrapers. Returns total ETFs updated."""
    total = 0
    scrapers = [
        ('BetaShares', scrape_betashares),
        ('VanEck', scrape_vaneck),
        ('Vanguard', scrape_vanguard),
        ('iShares', scrape_ishares),
        ('SPDR', scrape_spdr),
        ('StateStreet', scrape_statestreet),
        ('Global X', scrape_globalx),
        ('CXA Issuers', scrape_cxa_issuers),
        ('Dimensional', scrape_dimensional),
        ('Dimensional Metadata', scrape_dfa_metadata),
        ('Macquarie', scrape_macquarie),
        ('Macquarie Metadata', scrape_macquarie_metadata),
        ('Schroders', scrape_schroders),
        ('Russell', scrape_russell),
        ('Magellan', scrape_magellan),
        ('Fidelity', scrape_fidelity),
        ('Coolabah', scrape_coolabah),
        ('Morningstar', scrape_morningstar),
        ('Munro', scrape_munro),
        ('ETF Shares', scrape_etfshares),
        ('Resolution Capital', scrape_rescap),
        ('Hyperion', scrape_hyperion),
        ('Talaria', scrape_talaria),
        ('Ausbil', scrape_ausbil),
        ('Platinum', scrape_platinum),
        ('Loftus Peak', scrape_loftuspeak),
        ('Montaka', scrape_montaka),
        ('IML', scrape_iml),
        ('Apostle Dundas', scrape_apostle),
        ('Associate Global Partners', scrape_agp),
        ('Australian Ethical', scrape_aus_ethical),
        ('India Avenue', scrape_india_avenue),
        ('GCQ Funds', scrape_gcq),
        ('Plato', scrape_plato),
        ('Janus Henderson', scrape_janus_henderson),
        ('JPMorgan', scrape_jpmam),
        ('JPMorgan Metadata', scrape_jpmorgan_metadata),
        ('Antipodes', scrape_antipodes),
        ('Lakehouse', scrape_lakehouse),
        ('Alphinity', scrape_alphinity),
        ('Loomis Sayles', scrape_loomis),
        ('AllianceBernstein', scrape_ab),
        ('Nanuk', scrape_nanuk),
        ('ClearBridge', scrape_clearbridge),
        ('Aoris', scrape_aoris),
        ('Vaughan Nelson', scrape_vaughan_nelson),
        ('Firetrail', scrape_firetrail),
        ('Milford', scrape_milford),
        ('Franklin Templeton', scrape_franklin_templeton),
        ('InvestSMART', scrape_investsmart),
        ('Barrow Hanley', scrape_barrow_hanley),
    ]

    for name, func in scrapers:
        try:
            count = func(db_path)
            total += count
        except Exception as e:
            logger.error(f"Issuer scraper '{name}' failed: {e}", exc_info=True)

    # Update issuer stats after all scrapers run
    conn = get_connection(db_path)
    update_issuer_stats(conn)
    conn.close()

    logger.info(f"All issuer scrapers complete: {total} total ETFs updated")
    return total
