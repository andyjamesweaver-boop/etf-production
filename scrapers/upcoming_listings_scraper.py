#!/usr/bin/env python3
"""
Upcoming ETF Listings Scraper
==============================
Queries the ASIC Offer Notice Board API for new PDS lodgements (764K) and
parses detail pages to build a list of upcoming ASX/Cboe ETF listings.

Also reconciles pending listings against the etfs table to detect when an
upcoming ETF has actually listed (marks it as 'listed' and records its code).

API endpoint (discovered from client.js):
  GET https://regulatoryportal.asic.gov.au/offer-list/search
  Params: documentType, startDate.Day/Month/Year, endDate.Day/Month/Year,
          pageSize, pageNumber
"""

import logging
import os
import re
import ssl
import time
import urllib.request
import urllib.error
import urllib.parse
import json
from datetime import date, datetime, timedelta

logger = logging.getLogger(__name__)

_BASE = 'https://regulatoryportal.asic.gov.au'
_SEARCH_URL = _BASE + '/offer-list/search'
_DETAIL_URL = _BASE + '/offer-notice-board/detail/?docNo={}'

_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    ),
    'Accept': 'application/json, text/html, */*',
    'Referer': _BASE + '/offer-notice-board',
}

# SSL context: ASIC portal cert chain uses an intermediate not in all stores
_SSL = ssl.create_default_context()
_SSL.check_hostname = False
_SSL.verify_mode = ssl.CERT_NONE

# ASIC document type code for new PDS lodgements
_NEW_PDS_CODE = '764K'

# Offer type text that identifies exchange-listed managed funds (ETFs)
_LISTING_OFFER_TYPES = ('listing management investments scheme',)

# Exchanges we care about
_EXCHANGES = {'ASX', 'CXA', 'CBOE'}

# How far back to search (captures PDSs for funds still pending + recently listed)
_LOOKBACK_MONTHS = 6

# Pause between detail page fetches (be respectful)
_RATE_LIMIT_SECS = 1.5

# ---------------------------------------------------------------------------
# ASX Online admission notice constants
# ---------------------------------------------------------------------------

_ASXONLINE_BASE = 'https://www.asxonline.com/public/notices'
_ASXONLINE_STATE_FILE = os.path.join(os.path.dirname(__file__), '.asxonline_state.json')
_MONTH_NAMES = ['', 'january', 'february', 'march', 'april', 'may', 'june',
                'july', 'august', 'september', 'october', 'november', 'december']

_ASXONLINE_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,*/*',
}


# ---------------------------------------------------------------------------
# Name normalisation (shared by reconciler + existing-ETF filter)
# ---------------------------------------------------------------------------

def _clean_name(s: str) -> str:
    """Normalise a fund name for fuzzy comparison.
    HEDGED/UNHEDGED are intentionally kept — they distinguish different products
    (e.g. hedged vs unhedged share classes of the same fund family).
    """
    s = re.sub(r'[-–—]', ' ', s)
    s = re.sub(r'\b(ACTIVE|COMPLEX|ETFS?|FUND|TRUST|UNITS?|CLASS [A-Z]|OPEN CLASS|OPEN)\b', '', s)
    return re.sub(r'\s+', ' ', s).strip()


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get(url, as_json=False):
    req = urllib.request.Request(url, headers=_HEADERS)
    resp = urllib.request.urlopen(req, timeout=20, context=_SSL)
    body = resp.read().decode('utf-8', errors='ignore')
    if as_json:
        return json.loads(body)
    return body


# ---------------------------------------------------------------------------
# Already-listed ETF filter
# ---------------------------------------------------------------------------

def _matches_existing_etf(detail: dict, all_etfs: list) -> str | None:
    """Return the code of an already-listed ETF if this PDS is for it, else None."""
    name = (detail.get('name') or '').upper()
    scheme = (detail.get('scheme_name') or '').upper()
    candidates = [n for n in (name, scheme) if n]

    for etf in all_etfs:
        etf_name = (etf['name'] or '').upper()
        ec = _clean_name(etf_name)
        for cand in candidates:
            uc = _clean_name(cand)
            if _name_matches(ec, uc, strict=True):
                return etf['code']
    return None


# ---------------------------------------------------------------------------
# ASX Online admission notice scraper
# ---------------------------------------------------------------------------

def _load_asxonline_state() -> dict:
    try:
        with open(_ASXONLINE_STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_asxonline_state(state: dict):
    try:
        with open(_ASXONLINE_STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.warning(f'Could not save ASX Online state: {e}')


def _fetch_asxonline_notice(year: int, month: int, num: int, timeout: float = 2.0) -> str | None:
    """Fetch a single ASX Online notice. Returns HTML or None if not found/error.
    Non-existent notices send HTTP 404 within ~1.5s; real pages arrive in ~0.2s.
    """
    month_name = _MONTH_NAMES[month]
    notice_ref = f'{num:04d}.{year % 100:02d}.{month:02d}'
    url = f'{_ASXONLINE_BASE}/{year}/{month_name}/{notice_ref}.html'
    try:
        req = urllib.request.Request(url, headers=_ASXONLINE_HEADERS)
        resp = urllib.request.urlopen(req, timeout=timeout, context=_SSL)
        return resp.read().decode('utf-8', errors='ignore')
    except urllib.error.HTTPError as e:
        if e.code != 404:
            logger.debug(f'ASX Online {notice_ref}: HTTP {e.code}')
        return None
    except Exception:
        return None


def _probe_month_start(year: int, month: int, step: int = 50, max_num: int = 800) -> int:
    """
    Step-probe to find the approximate first notice number for a given month.
    Uses large steps to quickly skip the gap before the month's notices begin.
    Returns the start of the step window containing the first hit, or 1 if none found.
    """
    for probe in range(step, max_num + 1, step):
        time.sleep(0.05)  # small delay to avoid rate-limiting between probe requests
        if _fetch_asxonline_notice(year, month, probe) is not None:
            return max(1, probe - step)
    return 1


def _parse_asxonline_notice(html: str) -> list[dict] | None:
    """
    Parse an ASX Online notice page.
    Returns a list of dicts (one per product) with code/name/commencement_date,
    or None if not an AQUA ETF admission notice.
    A single notice may cover multiple ETFs (e.g. unhedged + hedged share class).
    """
    if 'AQUA' not in html:
        return None

    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'\s+', ' ', text).strip()

    if 'AQUA' not in text:
        return None

    # Expected trading commencement date (shared across all products in the notice)
    commencement_date = None
    m = re.search(
        r'(?:Expected trading commencement|Commencement date)[^\d]{0,30}(\d{1,2}\s+\w+\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})',
        text, re.IGNORECASE,
    )
    if m:
        date_str = m.group(1).strip()
        for fmt in ['%d %b %Y', '%d %B %Y', '%d/%m/%Y']:
            try:
                commencement_date = datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
                break
            except ValueError:
                continue

    # Find the block listing AQUA products — may be a numbered list
    products_block = ''
    m = re.search(r'Admission to Trading Status[:\s]+(.+?)(?=Expected trading|Commencement|$)', text, re.IGNORECASE)
    if m:
        products_block = m.group(1)

    # Try to split on numbered items: "1. Product A 2. Product B"
    numbered = re.findall(r'\d+\.\s+(.+?)(?=\s+\d+\.|$)', products_block)
    product_texts = [p.strip() for p in numbered] if numbered else [products_block.strip()]

    results = []
    for ptext in product_texts:
        if not ptext:
            continue
        # ASX code may appear inline: "Fund Name (ASX Code: XXXX)" or separately.
        # Codes are 3-5 chars, uppercase letters and digits (e.g. FIRE, V500, V5AH, NDQ).
        code = None
        for pat in [
            r'\(ASX[:\s]+([A-Z][A-Z0-9]{2,4})\)',
            r'ASX\s+Code[:\s]+([A-Z][A-Z0-9]{2,4})\b',
            r'\bASX:\s*([A-Z][A-Z0-9]{2,4})\b',
        ]:
            mc = re.search(pat, ptext)
            if mc:
                code = mc.group(1)
                break
        # If not in the product text, try the full notice (for single-product notices)
        if not code and len(product_texts) == 1:
            for pat in [
                r'ASX\s+Code[:\s]+([A-Z][A-Z0-9]{2,4})\b',
                r'\(ASX[:\s]+([A-Z][A-Z0-9]{2,4})\)',
                r'\bASX:\s*([A-Z][A-Z0-9]{2,4})\b',
            ]:
                mc = re.search(pat, text)
                if mc:
                    code = mc.group(1)
                    break

        # Strip any trailing code/parenthetical from the name
        name = re.sub(r'\s*\([^)]*\)\s*$', '', ptext).strip().rstrip('.,')
        if not name:
            continue

        results.append({'code': code, 'name': name, 'commencement_date': commencement_date})

    return results if results else None


def _scan_asxonline_notices(conn) -> int:
    """
    Scan ASX Online admission notices for the current and previous month.
    For any AQUA ETF admission found, match to pending listings and update
    the expected_listing_date and code.

    Notice numbers are sequential per calendar year (not per month), so the
    state file tracks both a per-year high-watermark and a per-month last-seen
    to avoid re-scanning. On first run the scan uses a higher 404-tolerance
    to bridge the gap from 1 to wherever the current month's notices begin.
    """
    state = _load_asxonline_state()
    today = date.today()
    year_key = str(today.year)
    year_high = state.get(year_key, 0)  # highest notice # seen anywhere this year

    pending = conn.execute(
        "SELECT id, name, scheme_name FROM upcoming_listings WHERE status='pending'"
    ).fetchall()
    if not pending:
        return 0

    total_matches = 0
    new_year_high = year_high

    for delta in range(2):  # current month, then previous
        m = today.month - delta
        y = today.year
        if m <= 0:
            m += 12
            y -= 1

        month_key = f'{y}-{m:02d}'
        month_last = state.get(month_key, 0)

        if month_last:
            # We've seen notices in this month before — start from where we left off
            start_num = month_last + 1
        elif year_high:
            # Notices are numbered sequentially year-wide — start right after last known
            start_num = year_high + 1
        else:
            # Completely fresh — step-probe to skip the gap before this month's notices
            logger.info(f'    First scan of {y}/{m:02d}: probing for notice range…')
            start_num = _probe_month_start(y, m)

        # Tolerance: larger on first scan (probe puts us within ~50 of first notice),
        # tight on subsequent scans (already positioned just after last known notice)
        gap_tolerance = 15 if month_last else 60

        logger.info(f'  Scanning ASX Online notices for {y}/{m:02d} starting at #{start_num}…')

        consecutive_404s = 0
        last_successful = month_last

        for num in range(start_num, start_num + 500):
            html = _fetch_asxonline_notice(y, m, num)
            if html is None:
                consecutive_404s += 1
                if consecutive_404s >= gap_tolerance:
                    break
                continue

            consecutive_404s = 0
            gap_tolerance = 15  # tighten once we've found at least one notice
            last_successful = num
            new_year_high = max(new_year_high, num)

            products = _parse_asxonline_notice(html)
            if not products:
                continue

            for product in products:
                notice_name = (product.get('name') or '').upper()
                notice_code = product.get('code')
                notice_date = product.get('commencement_date')

                logger.info(
                    f'    AQUA admission #{num:04d}: {notice_code} '
                    f'"{product.get("name")}" → {notice_date}'
                )

                for row in pending:
                    candidates = [n.upper() for n in (row['name'], row['scheme_name']) if n]
                    matched = False
                    for cand in candidates:
                        nc = _clean_name(notice_name)
                        uc = _clean_name(cand)
                        if nc and uc and _name_matches(nc, uc):
                            matched = True
                            break

                    if matched:
                        update_fields, update_vals = [], []
                        if notice_code:
                            update_fields.append('code=?')
                            update_vals.append(notice_code)
                        if notice_date:
                            update_fields.append('expected_listing_date=?')
                            update_vals.append(notice_date)
                        # Auto-mark as listed when the commencement date has passed
                        # and we have a confirmed ASX code from the admission notice
                        already_listed = (
                            notice_code
                            and notice_date
                            and notice_date <= date.today().isoformat()
                        )
                        if already_listed:
                            update_fields.append('status=?')
                            update_vals.append('listed')
                        if update_fields:
                            update_vals.append(row['id'])
                            conn.execute(
                                f"UPDATE upcoming_listings SET {', '.join(update_fields)}, "
                                f"updated_at=CURRENT_TIMESTAMP WHERE id=?",
                                update_vals,
                            )
                            conn.commit()
                            total_matches += 1
                            status_tag = ' [LISTED]' if already_listed else ''
                            logger.info(
                                f'      → Matched pending "{row["name"]}" '
                                f'(code={notice_code}, date={notice_date}){status_tag}'
                            )
                        break

        if last_successful > month_last:
            state[month_key] = last_successful

    state[year_key] = new_year_high
    _save_asxonline_state(state)
    return total_matches


# ---------------------------------------------------------------------------
# ASIC search API
# ---------------------------------------------------------------------------

def _search_new_pds(start: date, end: date, page: int = 1, page_size: int = 100) -> dict:
    """Query ASIC offer-list/search for all document types in the date range."""
    params = urllib.parse.urlencode({
        'documentType': 'ALL',
        'startDate.Day':   start.day,
        'startDate.Month': start.month,
        'startDate.Year':  start.year,
        'endDate.Day':     end.day,
        'endDate.Month':   end.month,
        'endDate.Year':    end.year,
        'pageSize':        page_size,
        'pageNumber':      page,
    })
    url = f'{_SEARCH_URL}?{params}'
    return _get(url, as_json=True)


def _collect_new_pds_entries(lookback_months: int = _LOOKBACK_MONTHS) -> list[dict]:
    """Paginate through ASIC search results and return all 764K PDS entries."""
    end = date.today()
    # Approximate 'n months ago' by subtracting 30*n days
    start = end - timedelta(days=30 * lookback_months)

    entries = []
    page = 1
    while True:
        data = _search_new_pds(start, end, page=page, page_size=100)
        results = data.get('result', [])
        total = data.get('pagination', {}).get('total', 0)

        for r in results:
            doc_html = r.get('document', {}).get('html', '')
            if _NEW_PDS_CODE not in doc_html:
                continue  # skip supplementaries, replacements, prospectuses

            title_href = r.get('title', {}).get('href', '')
            doc_no = title_href.split('docNo=')[-1] if 'docNo=' in title_href else None
            if not doc_no:
                continue

            issuer_html = r.get('issuer', {}).get('html', '')
            issuer_text = re.sub(r'<[^>]+>', ' ', issuer_html)
            issuer_text = re.sub(r'\s+', ' ', issuer_text).strip()
            # Remove ACN prefix (9 digits at the start)
            issuer_name = re.sub(r'^\d{3}\s*\d{3}\s*\d{3}\s*', '', issuer_text).strip()

            received = r.get('received', '')
            received_date = received[:10] if received else None  # ISO yyyy-mm-dd

            entries.append({
                'doc_no': doc_no,
                'issuer_raw': issuer_name,
                'received_date': received_date,
            })

        logger.info(f'  Page {page}: {len(results)} results, {len(entries)} 764K so far (total={total})')

        next_url = data.get('pagination', {}).get('next')
        if not next_url or len(entries) >= total or not results:
            break
        page += 1

    return entries


# ---------------------------------------------------------------------------
# Detail page parser
# ---------------------------------------------------------------------------

def _fund_type_from_name(name: str) -> str:
    n = name.upper()
    if 'ACTIVE ETF' in n or 'ACTIVE E' in n:
        return 'Active ETF'
    if 'COMPLEX ETF' in n:
        return 'Complex ETF'
    if 'ETF' in n:
        return 'ETF'
    return 'Active ETF'  # most new listings are active ETFs


def _parse_detail_page(doc_no: str) -> list[dict] | None:
    """
    Fetch and parse an ASIC offer notice detail page.
    Returns a list of dicts (one per scheme/ETF in the PDS), or None if the
    notice is not for an exchange-listed managed scheme we care about.
    Multi-scheme PDSs (e.g. unhedged + hedged class in one filing) return
    multiple entries so each gets its own upcoming_listings record.
    """
    url = _DETAIL_URL.format(doc_no)
    try:
        html = _get(url)
    except Exception as e:
        logger.warning(f'  Failed to fetch detail for {doc_no}: {e}')
        return None

    # Strip HTML tags and normalise whitespace
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'\s+', ' ', text).strip()

    def after(label, chars=300):
        idx = text.find(label)
        if idx == -1:
            return ''
        return text[idx + len(label): idx + len(label) + chars].strip()

    # Offer type — must be a listing scheme for us to care
    offer_type = after('Type of offer').lower()
    if not any(t in offer_type for t in _LISTING_OFFER_TYPES):
        return None

    # Exchange
    exchange_ctx = after('On what exchange is the company seeking quotation of their securities?', 50).upper()
    if 'ASX' in exchange_ctx:
        exchange = 'ASX'
    elif 'CBOE' in exchange_ctx or 'CXA' in exchange_ctx:
        exchange = 'CXA'
    elif 'ASX' in text[text.find('exchange'):text.find('exchange')+200].upper():
        exchange = 'ASX'
    else:
        return None

    # Effective date (earliest listing date, after 7-day exposure period)
    eff_str = after('Effective date', 30)
    m = re.search(r'(\d{2})/(\d{2})/(\d{4})', eff_str)
    effective_date = f'{m.group(3)}-{m.group(2)}-{m.group(1)}' if m else None

    # Offer document URL
    offer_doc_section = after('Where will the offer document be available?', 400)
    urls = re.findall(r'https?://\S+', offer_doc_section)
    offer_doc_url = urls[0].rstrip('.,)') if urls else None

    # All ARSN numbers and scheme names — a single PDS may cover multiple ETFs
    arsns = re.findall(r'ARSN\s+(\d{9})', text)
    scheme_names = re.findall(
        r'Scheme name\s+([A-Z][A-Z0-9 \-&(),/\.\']+?)(?:\s{2,}|ARSN|For more)', text
    )
    scheme_names = [s.strip() for s in scheme_names]

    # Build one record per scheme when there are multiple
    if len(scheme_names) > 1:
        records = []
        for i, sname in enumerate(scheme_names):
            arsn = arsns[i] if i < len(arsns) else None
            records.append({
                'name': sname,
                'scheme_name': sname,
                'exchange': exchange,
                'effective_date': effective_date,
                'arsn': arsn,
                'offer_doc_url': offer_doc_url,
                'fund_type': _fund_type_from_name(sname),
                'asic_detail_url': url,
            })
        return records

    # Single-scheme PDS — derive name from "Name of offer" field, fall back to scheme name
    scheme_name = scheme_names[0] if scheme_names else None
    arsn = arsns[0] if arsns else None

    raw_name = after('Name of offer', 400)
    name_end = raw_name.find('Type of offer')
    name = raw_name[:name_end].strip().rstrip('.') if name_end > 0 else raw_name.split('  ')[0].strip().rstrip('.')

    _generic = ('initial public offer', 'offer of units in the', 'product disclosure statement', 'pds')
    if not name or any(g in name.lower() for g in _generic):
        name = scheme_name

    return [{
        'name': name or scheme_name,
        'scheme_name': scheme_name,
        'exchange': exchange,
        'effective_date': effective_date,
        'arsn': arsn,
        'offer_doc_url': offer_doc_url,
        'fund_type': _fund_type_from_name(name or scheme_name or ''),
        'asic_detail_url': url,
    }]


# ---------------------------------------------------------------------------
# Promote: insert newly-listed ETFs into the main etfs table
# ---------------------------------------------------------------------------

def _promote_to_etfs(conn) -> int:
    """
    For each upcoming listing with status='listed' and a known code that is
    NOT yet in the etfs table, fetch basic data from the ASX API and insert it.
    Returns the number of ETFs promoted.
    """
    from scrapers.asx_etf_scraper import fetch_etf_price
    from scrapers.db_writer import upsert_etf

    rows = conn.execute(
        "SELECT ul.code, ul.name, ul.issuer, ul.exchange, ul.fund_type, "
        "       ul.expected_listing_date, ul.arsn "
        "FROM upcoming_listings ul "
        "WHERE ul.status = 'listed' AND ul.code IS NOT NULL "
        "  AND NOT EXISTS (SELECT 1 FROM etfs WHERE code = ul.code)"
    ).fetchall()

    if not rows:
        return 0

    promoted = 0
    for row in rows:
        code = row['code']
        logger.info(f'  Promoting {code} ({row["name"]}) to etfs table…')

        # Start with what we know from the upcoming_listings record
        etf_data = {
            'code': code,
            'name': row['name'],
            'issuer': row['issuer'],
            'exchange': row['exchange'] or 'ASX',
            'fund_type': row['fund_type'],
            'inception_date': row['expected_listing_date'],
            'data_source': 'upcoming_listings',
        }

        # Enrich with live ASX API data (price, FUM, etc.)
        api_data = fetch_etf_price(code)
        if api_data:
            etf_data.update(api_data)
            etf_data['data_source'] = 'asx_api'

        upsert_etf(conn, etf_data)
        conn.commit()
        promoted += 1
        logger.info(f'    → Inserted {code}: price={etf_data.get("current_price")}, '
                    f'FUM={etf_data.get("fund_size_aud_millions")}M')

    return promoted


# ---------------------------------------------------------------------------
# Reconcile: mark pending listings as 'listed' when they appear in etfs table
# ---------------------------------------------------------------------------

def _reconcile_listed(conn) -> int:
    """
    For each pending upcoming listing whose expected_listing_date has passed,
    check if an ETF with a matching name has been added to the etfs table.
    Mark as 'listed' and record the code.
    """
    today = date.today().isoformat()
    # Reconcile ALL pending entries (not just ones past their date)
    pending = conn.execute(
        "SELECT id, name, scheme_name, expected_listing_date "
        "FROM upcoming_listings WHERE status = 'pending'"
    ).fetchall()

    # Load all ETFs for name matching
    all_etfs = conn.execute("SELECT code, name FROM etfs WHERE name IS NOT NULL").fetchall()

    updated = 0
    for row in pending:
        ul_id = row['id']
        candidates = [n.upper() for n in (row['name'], row['scheme_name']) if n]
        if not candidates:
            continue

        matched_code = None
        for etf in all_etfs:
            etf_name = (etf['name'] or '').upper()
            ec = _clean_name(etf_name)
            for cand in candidates:
                uc = _clean_name(cand)
                if _name_matches(ec, uc, strict=True):
                    matched_code = etf['code']
                    break
            if matched_code:
                break

        if matched_code:
            conn.execute(
                "UPDATE upcoming_listings SET status='listed', code=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (matched_code, ul_id)
            )
            updated += 1
            logger.info(f'  Marked {row["name"]} as listed → {matched_code}')

    conn.commit()
    return updated


def _similarity(a: str, b: str) -> float:
    """Simple word-overlap similarity between two strings."""
    wa = set(a.split())
    wb = set(b.split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / max(len(wa), len(wb))


def _name_matches(a: str, b: str, strict: bool = False) -> bool:
    """True if two normalised fund names refer to the same fund.

    strict=False (for ASX Online matching): allows subset match and uses
        similarity > 0.75. Designed for cases where the admission notice name
        adds qualifiers not present in the ASIC PDS name, e.g.
        "VANGUARD US SHARES INDEX" ⊂ "VANGUARD S&P 500 US SHARES INDEX".

    strict=True (for reconcile / existing-ETF filter): uses similarity > 0.80
        and no subset check, reducing false positives between similar-sounding
        but distinct fund families (e.g. International vs Australian Shares).
    """
    if not a or not b:
        return False
    if a in b or b in a:
        return True
    threshold = 0.80 if strict else 0.75
    if _similarity(a, b) > threshold:
        return True
    if not strict:
        # Subset: every word in the shorter set appears in the longer
        wa = set(a.split())
        wb = set(b.split())
        shorter, longer = (wa, wb) if len(wa) <= len(wb) else (wb, wa)
        return shorter <= longer
    return False


# ---------------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------------

def _upsert(conn, doc_no: str, issuer: str, received_date: str, detail: dict) -> bool:
    """Upsert a record into upcoming_listings. Returns True if new/changed."""
    existing = conn.execute(
        "SELECT id, status FROM upcoming_listings WHERE asic_doc_no = ?",
        (doc_no,)
    ).fetchone()

    if existing and existing['status'] != 'pending':
        return False  # already listed or withdrawn, don't overwrite

    fields = {
        'asic_doc_no':           doc_no,
        'name':                  detail.get('name'),
        'scheme_name':           detail.get('scheme_name'),
        'issuer':                issuer,
        'exchange':              detail.get('exchange'),
        'fund_type':             detail.get('fund_type'),
        'arsn':                  detail.get('arsn'),
        'expected_listing_date': detail.get('effective_date'),
        'pds_lodged_date':       received_date,
        'offer_doc_url':         detail.get('offer_doc_url'),
        'asic_detail_url':       detail.get('asic_detail_url'),
    }

    if existing:
        set_clause = ', '.join(f'{k}=?' for k in fields if k != 'asic_doc_no')
        vals = [v for k, v in fields.items() if k != 'asic_doc_no']
        vals.append(doc_no)
        conn.execute(
            f"UPDATE upcoming_listings SET {set_clause}, updated_at=CURRENT_TIMESTAMP WHERE asic_doc_no=?",
            vals
        )
    else:
        cols = ', '.join(fields.keys())
        placeholders = ', '.join(['?'] * len(fields))
        conn.execute(
            f"INSERT INTO upcoming_listings ({cols}) VALUES ({placeholders})",
            list(fields.values())
        )

    return True


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def scrape_upcoming_listings(db_path=None) -> int:
    from scrapers.config import DB_PATH as DEFAULT_DB
    import sqlite3

    db_path = db_path or DEFAULT_DB
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')

    # Ensure table exists (safe to run on every call)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS upcoming_listings (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            asic_doc_no             TEXT UNIQUE NOT NULL,
            code                    TEXT,
            name                    TEXT,
            scheme_name             TEXT,
            issuer                  TEXT,
            exchange                TEXT,
            asset_class             TEXT,
            fund_type               TEXT,
            arsn                    TEXT,
            expected_listing_date   TEXT,
            pds_lodged_date         TEXT,
            offer_doc_url           TEXT,
            asic_detail_url         TEXT,
            status                  TEXT DEFAULT 'pending',
            notes                   TEXT,
            created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()

    start_time = time.time()
    new_count = 0

    try:
        logger.info('Fetching ASIC offer notice board — new PDS lodgements…')
        entries = _collect_new_pds_entries()
        logger.info(f'Found {len(entries)} new PDS lodgements to evaluate')

        # Load existing ETFs once for already-listed filtering
        all_etfs = conn.execute("SELECT code, name FROM etfs WHERE name IS NOT NULL").fetchall()

        for i, entry in enumerate(entries):
            doc_no = entry['doc_no']

            # Skip already-processed entries.
            # For multi-scheme PDSs, check whether the base doc_no OR any suffixed variant
            # has been processed. A single 'pending' or 'existing_etf' on ANY variant means
            # the detail fetch already happened — skip it.
            existing_rows = conn.execute(
                "SELECT status FROM upcoming_listings WHERE asic_doc_no=? OR asic_doc_no LIKE ?",
                (doc_no, f'{doc_no}:%'),
            ).fetchall()
            if existing_rows:
                statuses = {r['status'] for r in existing_rows}
                # If anything is pending, reconcile handles it; if all are terminal, skip entirely
                if 'pending' in statuses or statuses <= {'listed', 'existing_etf'}:
                    continue

            logger.info(f'  [{i+1}/{len(entries)}] Fetching detail for {doc_no} ({entry["issuer_raw"][:40]})')
            time.sleep(_RATE_LIMIT_SECS)

            details = _parse_detail_page(doc_no)
            if details is None:
                logger.debug(f'    → Skipped (not an exchange-listed managed scheme)')
                continue

            if len(details) > 1:
                logger.info(f'    → Multi-scheme PDS: {len(details)} ETFs in one filing')

            for idx, detail in enumerate(details):
                if not detail.get('name'):
                    continue

                # For multi-scheme PDSs, secondary schemes get a synthetic doc_no suffix
                effective_doc_no = doc_no if idx == 0 else f'{doc_no}:{idx}'

                # Check if this is just a new PDS for an already-listed ETF
                matched_code = _matches_existing_etf(detail, all_etfs)
                if matched_code:
                    logger.info(
                        f'    → [{idx+1}/{len(details)}] {detail["name"]} already listed as '
                        f'{matched_code} — suppressing'
                    )
                    conn.execute(
                        "INSERT OR IGNORE INTO upcoming_listings "
                        "(asic_doc_no, name, issuer, status, pds_lodged_date) VALUES (?, ?, ?, 'existing_etf', ?)",
                        (effective_doc_no, detail.get('name'), entry['issuer_raw'], entry['received_date']),
                    )
                    conn.commit()
                    continue

                logger.info(
                    f'    → [{idx+1}/{len(details)}] {detail["name"]} | {detail["exchange"]} | '
                    f'effective {detail.get("effective_date", "?")} | {detail["fund_type"]}'
                )

                changed = _upsert(conn, effective_doc_no, entry['issuer_raw'], entry['received_date'], detail)
                conn.commit()
                if changed:
                    new_count += 1

        # Reconcile: mark pending listings that have now appeared in etfs table
        logger.info('Reconciling pending listings against etfs table…')
        promoted = _reconcile_listed(conn)
        logger.info(f'  Marked {promoted} listings as listed')

        # Validate/update listing dates and codes from ASX Online admission notices
        logger.info('Scanning ASX Online admission notices for listing dates…')
        asx_matches = _scan_asxonline_notices(conn)
        logger.info(f'  Updated {asx_matches} pending listings from ASX Online')

        # Promote newly-listed ETFs into the main etfs table
        logger.info('Promoting newly-listed ETFs into main etfs table…')
        etfs_added = _promote_to_etfs(conn)
        logger.info(f'  Promoted {etfs_added} new ETFs')

    except Exception as e:
        logger.error(f'Upcoming listings scraper failed: {e}', exc_info=True)
        raise
    finally:
        conn.close()

    elapsed = time.time() - start_time
    total = new_count + (promoted if 'promoted' in dir() else 0)
    logger.info(f'Upcoming listings complete: {new_count} new/updated, {elapsed:.1f}s')
    return total
