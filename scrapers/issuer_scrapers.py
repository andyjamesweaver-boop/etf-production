"""
Per-issuer website scrapers.

Each function scrapes an issuer's website for ETF data that isn't available
from ASX/Cboe sources: holdings, sector allocations, returns, fees, etc.
"""

import re
import json
import logging
from datetime import datetime

from scrapers.config import ISSUER_URLS, SPDR_AU_FUNDS, VANGUARD_AU_FUNDS, VANGUARD_AU_PORT_IDS, normalise_asset_class, CXA_ISSUER_URLS
from scrapers.base_scraper import fetch, fetch_json
from scrapers.db_writer import (
    get_connection, upsert_etf, upsert_holdings, upsert_sectors,
    log_scrape, update_issuer_stats,
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

def _scrape_betashares_fund_page(conn, slug: str, soup) -> str | None:
    """
    Parse a BetaShares individual fund page.
    Returns the ASX code, or None if not found.
    Title format: "NDQ ASX | Nasdaq 100 ETF | Betashares"
    Holdings table rows: <th>COMPANY NAME</th><td>weight</td>
    """
    # ASX code from <title>
    title = soup.title.string if soup.title else ''
    code_match = re.match(r'^([A-Z0-9]{2,6})\s+ASX', title)
    if not code_match:
        return None
    code = code_match.group(1)

    # Fund name from title
    name_match = re.match(r'^[A-Z0-9]+\s+ASX\s+\|\s+(.+?)\s+\|\s+', title)
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

    etf = {
        'code': code,
        'name': name,
        'issuer': 'BetaShares',
        'expense_ratio': mer,
        'management_fee': mer,
        'data_source': 'betashares',
        'issuer_url': f"https://www.betashares.com.au/fund/{slug}/",
    }
    etf = {k: v for k, v in etf.items() if v is not None}
    upsert_etf(conn, etf)

    if holdings:
        # Deduplicate by name — merge weights when the same name appears twice
        # (e.g. Alphabet Inc appears as both GOOGL and GOOG share classes)
        merged: dict[str, dict] = {}
        for h in holdings:
            name = h['name']
            if name in merged:
                merged[name]['weight_pct'] = (merged[name]['weight_pct'] or 0) + (h['weight_pct'] or 0)
            else:
                merged[name] = h
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
                code = _scrape_betashares_fund_page(conn, slug, page_soup)
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

    # Scrape individual snapshot pages for MER, returns, holdings (top 30 by priority)
    if seen:
        # Prioritise ETFs already in DB with FUM ranking
        ranked = conn.execute(
            "SELECT code FROM etfs WHERE issuer = 'VanEck' AND code IN ({}) ORDER BY rank_by_fum LIMIT 30".format(
                ','.join('?' * len(seen))
            ),
            list(seen.keys())
        ).fetchall()
        priority_codes = [r[0] for r in ranked]
        # Add any remaining codes not yet ranked
        remaining = [c for c in seen if c not in priority_codes]
        codes_to_scrape = (priority_codes + remaining)[:30]

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

def scrape_vanguard(db_path=None) -> int:
    """
    Seed Vanguard Australia ETF data from the known fund list.
    (Vanguard AU website is an Angular SPA — no accessible server-side API.)
    Upserts the known fund list and preserves any richer data already in DB.
    """
    started = datetime.utcnow()
    source = 'vanguard'
    updated = 0

    conn = get_connection(db_path)

    # Seed from known list (preserves existing data via COALESCE upsert)
    for code, name in VANGUARD_AU_FUNDS.items():
        port_id = VANGUARD_AU_PORT_IDS.get(code)
        issuer_url = (
            f"https://www.vanguard.com.au/personal/invest-with-us/etf?portId={port_id}"
            if port_id else None
        )
        etf = {
            'code': code,
            'name': name,
            'issuer': 'Vanguard',
            'exchange': 'ASX',
            'data_source': 'vanguard',
        }
        if issuer_url:
            etf['issuer_url'] = issuer_url
        upsert_etf(conn, etf)
        updated += 1

    # Fix issuer_url for all Vanguard ETFs already in the DB using the portId mapping
    for code, port_id in VANGUARD_AU_PORT_IDS.items():
        conn.execute("""
            UPDATE etfs
            SET issuer_url = ?
            WHERE code = ? AND issuer = 'Vanguard'
        """, (f"https://www.vanguard.com.au/personal/invest-with-us/etf?portId={port_id}", code))

    conn.commit()
    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success', records_affected=updated,
               duration_secs=duration, started_at=started.isoformat())
    conn.close()
    logger.info(f"Vanguard: seeded {updated} ETFs from known fund list")
    return updated


# ====================================================================
# iShares (BlackRock)
# ====================================================================

def scrape_ishares(db_path=None) -> int:
    """Scrape iShares/BlackRock Australia ETF data."""
    started = datetime.utcnow()
    source = 'ishares'
    updated = 0

    conn = get_connection(db_path)

    # Try multiple known API patterns for BlackRock AU
    api_attempts = [
        ('https://www.blackrock.com/au/individual/products/product-list?productView=etf&iShares=true&country=au&domicileCountry=au&type=ishares&dataType=fund', {}),
        ('https://www.blackrock.com/au/individual/products/fund-list', {'Accept': 'application/json'}),
        ('https://www.blackrock.com/cache/api/fund/getfundlist/?productView=etf&country=au&iShares=true', {}),
    ]

    for api_url, extra_headers in api_attempts:
        headers = {'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'}
        headers.update(extra_headers)
        data = fetch_json(api_url, headers=headers)
        if not data:
            continue

        funds_raw = None
        if isinstance(data, list):
            funds_raw = data
        elif isinstance(data, dict):
            for key in ('data', 'funds', 'products', 'fundData', 'aaData'):
                if key in data:
                    funds_raw = data[key]
                    break

        if not funds_raw:
            continue

        for fund in funds_raw:
            if not isinstance(fund, dict):
                continue
            code = (fund.get('localExchangeTicker') or fund.get('ticker') or
                    fund.get('fundCode') or fund.get('asx_code') or '').strip().upper()
            if not code or not re.match(r'^[A-Z0-9]{2,6}$', code) or code.isdigit():
                continue

            etf = {
                'code': code,
                'name': fund.get('fundName') or fund.get('name'),
                'issuer': 'iShares',
                'expense_ratio': _safe_float(fund.get('totalExpenseRatio') or fund.get('mer') or fund.get('managementFee')),
                'asset_class': normalise_asset_class(fund.get('assetClass') or fund.get('asset_class')),
                'benchmark': fund.get('benchmark') or fund.get('benchmarkName'),
                'inception_date': fund.get('inceptionDate') or fund.get('fundInceptionDate'),
                'fund_size_aud_millions': _safe_float(fund.get('totalNetAssets') or fund.get('aum')),
                'return_1y': _safe_float(fund.get('return1yr') or fund.get('return1y') or fund.get('annualReturn1yr')),
                'return_3y': _safe_float(fund.get('return3yr') or fund.get('return3y')),
                'return_5y': _safe_float(fund.get('return5yr') or fund.get('return5y')),
                'data_source': 'ishares',
            }
            etf = {k: v for k, v in etf.items() if v is not None}
            upsert_etf(conn, etf)
            updated += 1

        if updated:
            break  # Stop trying other API endpoints

    # HTML fallback — parse fund list page and extract inline JSON
    if updated == 0:
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            pass
        else:
            fund_list_url = 'https://www.blackrock.com/au/individual/products/investment-funds#categoryId=702702&tab=overview'
            resp = fetch(fund_list_url)
            if resp:
                soup = BeautifulSoup(resp.text, 'html.parser')

                # BlackRock sometimes embeds fund data in a JS variable
                json_data = _find_json_in_script(soup, ['localExchangeTicker', 'fundName', 'iShares'])
                if json_data:
                    funds = json_data if isinstance(json_data, list) else json_data.get('data', [])
                    for fund in funds:
                        if not isinstance(fund, dict):
                            continue
                        code = (fund.get('localExchangeTicker') or fund.get('ticker') or '').strip().upper()
                        if not re.match(r'^[A-Z0-9]{2,6}$', code) or code.isdigit():
                            continue
                        etf = {
                            'code': code,
                            'name': fund.get('fundName') or fund.get('name'),
                            'issuer': 'iShares',
                            'expense_ratio': _safe_float(fund.get('totalExpenseRatio') or fund.get('mer')),
                            'asset_class': normalise_asset_class(fund.get('assetClass')),
                            'data_source': 'ishares',
                        }
                        etf = {k: v for k, v in etf.items() if v is not None}
                        upsert_etf(conn, etf)
                        updated += 1
                else:
                    # Last resort: extract ticker-like codes from links to fund pages.
                    # Require at least one letter — BlackRock numeric product IDs like
                    # /products/251852/ must not be treated as ASX ticker codes.
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        if '/products/' not in href and '/funds/' not in href:
                            continue
                        # iShares AU product page patterns — [A-Z] anchor prevents
                        # matching pure-numeric BlackRock internal product IDs.
                        for pattern in [
                            r'/products/([A-Z][A-Z0-9]{1,5})/',
                            r'ticker=([A-Z][A-Z0-9]{1,5})',
                            r'/etf/([A-Z][A-Z0-9]{1,5})$',
                        ]:
                            match = re.search(pattern, href, re.IGNORECASE)
                            if match:
                                code = match.group(1).upper()
                                name = link.get_text(strip=True)
                                if name and 3 < len(name) < 100:
                                    etf = {
                                        'code': code,
                                        'name': name,
                                        'issuer': 'iShares',
                                        'data_source': 'ishares',
                                    }
                                    upsert_etf(conn, etf)
                                    updated += 1
                                break

    conn.commit()
    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f"iShares: updated {updated} ETFs")
    return updated


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

        # MER from fee percentages on the page
        mer = None
        fee_matches = re.findall(r'management.fee[^0-9]*(\d+\.\d+)\s*%', resp.text, re.I)
        for val_str in fee_matches:
            val = _safe_float(val_str)
            if val and 0 < val < 3:
                mer = val
                break

        # Inception date from URL-encoded JSON embedded in the page
        inception = None
        inc_match = re.search(r'inception-date[^:]*:.*?value.*?(\d{1,2}[- ]\w{3}[- ]\d{4})', resp.text, re.I)
        if inc_match:
            inception = inc_match.group(1)

        etf = {
            'code': code,
            'name': name,
            'issuer': 'SPDR',
            'expense_ratio': mer,
            'management_fee': mer,
            'inception_date': inception,
            'exchange': 'ASX',
            'data_source': 'spdr',
            'issuer_url': url,
        }
        etf = {k: v for k, v in etf.items() if v is not None}
        upsert_etf(conn, etf)
        updated += 1

    conn.commit()
    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f"SPDR: updated {updated} ETFs")
    return updated


# ====================================================================
# Global X
# ====================================================================

def scrape_globalx(db_path=None) -> int:
    """Scrape Global X Australia ETF list and fund details."""
    started = datetime.utcnow()
    source = 'globalx'
    updated = 0

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.error("beautifulsoup4 required for issuer scrapers")
        return 0

    conn = get_connection(db_path)
    urls = ISSUER_URLS.get('Global X', {})

    # --- Try WordPress REST API (Global X uses WordPress) ---
    wp_endpoints = [
        'https://www.globalxetfs.com.au/wp-json/globalx/v1/funds?per_page=100',
        'https://www.globalxetfs.com.au/wp-json/wp/v2/fund?per_page=100&_fields=id,title,link,acf',
        'https://www.globalxetfs.com.au/wp-json/wp/v2/etf?per_page=100&_fields=id,title,link,acf',
        'https://www.globalxetfs.com.au/wp-json/wp/v2/posts?categories=etf&per_page=100',
    ]

    for api_url in wp_endpoints:
        data = fetch_json(api_url)
        if not data or not isinstance(data, list):
            continue

        for fund in data:
            if not isinstance(fund, dict):
                continue
            acf = fund.get('acf') or {}

            code = (
                acf.get('asx_code') or acf.get('ticker') or acf.get('exchange_code') or
                fund.get('asx_code') or fund.get('ticker') or ''
            ).strip().upper()
            if not code or not re.match(r'^[A-Z0-9]{2,6}$', code):
                continue

            raw_name = (
                (fund.get('title') or {}).get('rendered') or
                fund.get('name') or acf.get('fund_name') or ''
            )
            name = re.sub(r'<[^>]+>', '', raw_name).strip() or None

            mer = _safe_float(
                acf.get('management_fee') or acf.get('mer') or acf.get('expense_ratio') or
                acf.get('annual_fee') or acf.get('mgt_fee')
            )

            asset_class_raw = (
                acf.get('asset_class') or acf.get('category') or acf.get('fund_type') or
                fund.get('category')
            )

            etf = {
                'code': code,
                'name': name,
                'issuer': 'Global X',
                'expense_ratio': mer,
                'asset_class': normalise_asset_class(asset_class_raw),
                'inception_date': acf.get('inception_date') or acf.get('listing_date'),
                'benchmark': acf.get('benchmark') or acf.get('index'),
                'distribution_yield': _safe_float(acf.get('distribution_yield') or acf.get('yield')),
                'data_source': 'globalx',
                'issuer_url': fund.get('link') or f"https://www.globalxetfs.com.au/funds/{code.lower()}/",
            }
            etf = {k: v for k, v in etf.items() if v is not None}
            upsert_etf(conn, etf)
            updated += 1

        if updated:
            conn.commit()
            break  # Stop trying other endpoints

    # --- HTML fallback: fund list page ---
    if updated == 0:
        resp = fetch(urls.get('fund_list', 'https://www.globalxetfs.com.au/funds/'))
        if resp:
            soup = BeautifulSoup(resp.text, 'html.parser')

            # Try script-embedded JSON first
            json_data = _find_json_in_script(soup, ['asx_code', 'globalx', 'management_fee'])
            if json_data:
                funds = json_data if isinstance(json_data, list) else json_data.get('funds', json_data.get('data', []))
                for fund in funds:
                    if not isinstance(fund, dict):
                        continue
                    code = (fund.get('asx_code') or fund.get('ticker') or '').strip().upper()
                    if not re.match(r'^[A-Z0-9]{2,6}$', code):
                        continue
                    etf = {
                        'code': code,
                        'name': fund.get('name') or fund.get('fund_name'),
                        'issuer': 'Global X',
                        'expense_ratio': _safe_float(fund.get('management_fee') or fund.get('mer')),
                        'asset_class': normalise_asset_class(fund.get('asset_class') or fund.get('category')),
                        'data_source': 'globalx',
                    }
                    etf = {k: v for k, v in etf.items() if v is not None}
                    upsert_etf(conn, etf)
                    updated += 1
            else:
                # Parse fund cards — Global X uses card-based layout
                card_selectors = [
                    '.fund-card', '.etf-card', '[class*="fund-item"]',
                    'article.fund', '.product-card', '[data-asx-code]',
                    '.funds-listing article', '.fund-listing__item',
                    '[class*="FundCard"]', '[class*="fund_card"]',
                ]
                cards = []
                for sel in card_selectors:
                    cards = soup.select(sel)
                    if cards:
                        break

                # If no cards found, try rows with data attributes
                if not cards:
                    cards = [el for el in soup.find_all(attrs={'data-asx-code': True})]
                    cards += [el for el in soup.find_all(attrs={'data-ticker': True})]

                seen_codes: set[str] = set()
                for card in cards:
                    # Find ASX code
                    code = ''
                    code_el = card.select_one(
                        '.asx-code, [class*="ticker"], [class*="asx-code"], '
                        '.code-badge, [data-asx-code], [data-ticker]'
                    )
                    if code_el:
                        code = (code_el.get('data-asx-code') or code_el.get('data-ticker') or
                                code_el.get_text(strip=True))
                    else:
                        code = card.get('data-asx-code', card.get('data-ticker', ''))

                    code = re.sub(r'[^A-Z0-9]', '', code.upper())
                    if not re.match(r'^[A-Z0-9]{2,6}$', code) or code in seen_codes:
                        continue
                    seen_codes.add(code)

                    # Name
                    name_el = card.select_one('h2, h3, h4, [class*="fund-name"], [class*="title"]')
                    name = name_el.get_text(strip=True) if name_el else None
                    if name and name.upper() == code:
                        name = None  # Skip if name is just the ticker

                    # MER
                    mer = None
                    for el in card.select('[class*="fee"], [class*="mer"], [class*="expense"], [class*="cost"]'):
                        text = el.get_text(strip=True)
                        val = _safe_float(re.sub(r'[^0-9.]', '', text))
                        if val and 0 < val < 5:
                            mer = val
                            break

                    # Asset class from card text
                    card_text = card.get_text(separator=' ', strip=True).lower()
                    asset_class = None
                    for keyword, ac in [
                        ('australian equit', 'Australian Equities'),
                        ('fixed income', 'Fixed Income'), ('bond', 'Fixed Income'),
                        ('international equit', 'International Equities'),
                        ('global equit', 'International Equities'),
                        ('commodit', 'Commodities'), ('gold', 'Commodities'),
                        ('crypto', 'Digital Assets'), ('bitcoin', 'Digital Assets'),
                        ('digital asset', 'Digital Assets'),
                        ('property', 'Property'), ('reit', 'Property'),
                        ('cash', 'Cash'), ('alternative', 'Alternatives'),
                    ]:
                        if keyword in card_text:
                            asset_class = ac
                            break

                    # Link
                    issuer_url = None
                    link_el = card.find('a', href=True)
                    if link_el:
                        href = link_el['href']
                        issuer_url = href if href.startswith('http') else f"https://www.globalxetfs.com.au{href}"

                    etf = {
                        'code': code,
                        'name': name,
                        'issuer': 'Global X',
                        'expense_ratio': mer,
                        'asset_class': asset_class,
                        'data_source': 'globalx',
                        'issuer_url': issuer_url,
                    }
                    etf = {k: v for k, v in etf.items() if v is not None}
                    upsert_etf(conn, etf)
                    updated += 1

            conn.commit()

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
        ('Global X', scrape_globalx),
        ('CXA Issuers', scrape_cxa_issuers),
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
