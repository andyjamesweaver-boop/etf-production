"""
Scraper for CBOE Australia quarterly portfolio disclosures.

Active/Complex ETFs listed on CBOE are required to publicly disclose their full
portfolio holdings once per quarter, typically 45-60 days after quarter-end.
These are filed as PDF announcements on CBOE's CDN:
  https://cdn.cboe.com/data/au/equities/issuer_announcements/{PREFIX}{DDMMYY}{SEQ}.pdf

URL format:
  PREFIX  = 3-char CBOE announcement code (derived from ticker, see _CBOE_PREFIXES)
  DDMMYY  = filing date in day-month-year format (e.g. 190226 = 19 Feb 2026)
  SEQ     = 3-digit sequence (001 = first announcement on that day)

Two PDF formats are currently supported:
  1. Coolabah format  — "Quarterly portfolio disclosure notification"
     Two-column table layout; each cell contains multiple holdings as newline-separated
     "SECURITY_NAME WEIGHT" lines.
  2. AMVE/AB format   — "Quarterly Holdings Disclosure Notification"
     Clean 3-column pdfplumber table: [Name, ISIN, Weight]

Funds that file OTHER announcement types on CBOE (MPI correlation reports, factsheets,
monthly unit reports, etc.) are NOT processed here.
"""

import re
import io
import logging
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

# ── Known CBOE announcement prefixes that file genuine portfolio disclosures ──
# Format: etf_code -> (cboe_prefix, format_hint)
# format_hint: 'coolabah' | 'amve' | 'auto'
_CBOE_PREFIXES = {
    'FIXD': ('FIX', 'coolabah'),  # Coolabah Active Composite Bond Complex ETF
    'YLDX': ('YLD', 'coolabah'),  # Coolabah Global Floating-Rate High Yield Complex ETF
    'FRNS': ('FRN', 'coolabah'),  # Coolabah Short Term Income Active ETF
    'AMVE': ('AMV', 'amve'),      # AB Managed Volatility Equities Fund
}

_CDN_BASE = 'https://cdn.cboe.com/data/au/equities/issuer_announcements/'

# Keywords that confirm a PDF is a genuine portfolio disclosure
_PORTFOLIO_KEYWORDS = [
    'quarterly portfolio disclosure notification',
    'quarterly holdings disclosure',
    'comprised of the below securities',
]

# Rows/lines to skip when parsing holdings
_SKIP_NAMES = frozenset({
    'sub-total', 'grand total', 'total', 'cash & collateral',
    'repurchase agreements', 'cash and collateral', 'cash',
})


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get(url, timeout=15):
    """Fetch a URL using requests. Returns response or None."""
    try:
        import requests, urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        s = requests.Session()
        s.headers['User-Agent'] = 'Mozilla/5.0'
        r = s.get(url, timeout=timeout, verify=False)
        return r if r.status_code == 200 else None
    except Exception as e:
        logger.debug(f'HTTP error {url}: {e}')
        return None


def _head_size(url, timeout=5):
    """Return content-length for a HEAD request, or 0 on failure."""
    try:
        import requests, urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        s = requests.Session()
        s.headers['User-Agent'] = 'Mozilla/5.0'
        r = s.head(url, timeout=timeout, verify=False)
        if r.status_code == 200:
            return int(r.headers.get('content-length', 0))
    except Exception:
        pass
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# URL discovery
# ─────────────────────────────────────────────────────────────────────────────

def _discover_portfolio_url(prefix: str, max_days_back: int = 120) -> str | None:
    """
    Scan backwards from today to find the most recent quarterly portfolio PDF
    for the given CBOE prefix. Uses parallel HEAD requests to find existing files,
    then verifies content (newest-first) until a genuine portfolio disclosure is found.

    Returns the filename (not full URL) of the latest genuine portfolio PDF found,
    or None if nothing found within max_days_back.
    """
    today = date.today()
    candidates = []
    for i in range(max_days_back):
        d = today - timedelta(days=i)
        ds = f'{d.day:02d}{d.month:02d}{str(d.year)[2:]}'
        for seq in ('001', '002', '003'):
            candidates.append(f'{prefix}{ds}{seq}.pdf')

    found = {}  # date_str -> [filenames]  (multiple seqs on same date)

    def probe(fname):
        sz = _head_size(_CDN_BASE + fname)
        return fname if sz > 20_000 else None  # real PDFs are >20KB

    with ThreadPoolExecutor(max_workers=40) as ex:
        future_map = {ex.submit(probe, f): f for f in candidates}
        for fut in as_completed(future_map):
            fname = fut.result()
            if fname:
                date_part = fname[len(prefix):len(prefix) + 6]
                found.setdefault(date_part, []).append(fname)

    if not found:
        return None

    # Sort date keys newest-first (DDMMYY → sort key YY,MM,DD)
    sorted_dates = sorted(found.keys(), key=lambda s: (s[4:6], s[2:4], s[0:2]), reverse=True)

    # Iterate newest-to-oldest; download and verify each candidate
    for date_key in sorted_dates:
        for fname in sorted(found[date_key]):  # seq 001 before 002
            resp = _get(_CDN_BASE + fname)
            if not resp:
                continue
            first_text = ''
            try:
                import pdfplumber, io as _io
                with pdfplumber.open(_io.BytesIO(resp.content)) as pdf:
                    first_text = (pdf.pages[0].extract_text() or '').lower()
            except Exception:
                continue
            if any(kw in first_text for kw in _PORTFOLIO_KEYWORDS):
                logger.debug(f'Confirmed portfolio PDF: {fname}')
                return fname
            else:
                logger.debug(f'Not a portfolio disclosure: {fname} — skipping')

    return None


# ─────────────────────────────────────────────────────────────────────────────
# PDF parsing
# ─────────────────────────────────────────────────────────────────────────────

def _extract_portfolio_date(text: str) -> str | None:
    """
    Extract the portfolio-as-of date from the announcement text.
    Looks for patterns like "as of 31 December 2025" or "31 December 2025".
    Returns 'YYYY-MM-DD' string or None.
    """
    months = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12,
    }
    date_pat = (
        r'\b(\d{1,2})\s+(january|february|march|april|may|june|july|august|'
        r'september|october|november|december)\s+(20\d{2})\b'
    )
    # Try context-specific patterns first (most reliable)
    for prefix_pat in (
        r'as of\s+',                              # "as of 31 December 2025"
        r'quarterly holdings disclosure notification\s+',  # AMVE title
        r'portfolio for the fund as of\s+',
    ):
        m = re.search(prefix_pat + date_pat, text, re.IGNORECASE)
        if m:
            g = m.groups()
            day, month_str, year = int(g[-3]), g[-2].lower(), int(g[-1])
            return f'{year}-{months[month_str]:02d}-{day:02d}'
    # Fallback: find all dates and return the latest one (excludes filing dates
    # that appear first but are typically more recent than the portfolio date)
    all_dates = []
    for m in re.finditer(date_pat, text, re.IGNORECASE):
        day, month_str, year = int(m.group(1)), m.group(2).lower(), int(m.group(3))
        all_dates.append((year, months[month_str], day))
    if all_dates:
        y, mo, d = min(all_dates)  # portfolio date is the earlier one
        return f'{y}-{mo:02d}-{d:02d}'
    return None


def _parse_coolabah_format(pdf) -> list[dict]:
    """
    Parse the Coolabah two-column quarterly portfolio PDF.
    Table cells contain multiple holdings as "NAME WEIGHT" lines (newline-separated).
    """
    holdings = []
    seen = set()

    for page in pdf.pages:
        tbl = page.extract_table()
        if not tbl:
            # Fallback: text extraction
            text = page.extract_text() or ''
            in_data = False
            for line in text.split('\n'):
                line = line.strip()
                if not line:
                    continue
                if 'asset name' in line.lower() and 'weight' in line.lower():
                    in_data = True
                    continue
                if not in_data:
                    continue
                parts = line.rsplit(None, 1)
                if len(parts) != 2:
                    continue
                name, raw_w = parts
                name = name.strip()
                if name.lower() in _SKIP_NAMES:
                    continue
                w = _safe_float(raw_w)
                if w is not None and name and name not in seen:
                    seen.add(name)
                    holdings.append({'name': name, 'ticker': None, 'weight_pct': w})
            continue

        for row in tbl:
            for cell in row:
                if not cell:
                    continue
                cell_text = str(cell)
                # Skip header cells
                if 'asset name' in cell_text.lower() or 'weight' in cell_text.lower():
                    continue
                for line in cell_text.split('\n'):
                    line = line.strip()
                    if not line:
                        continue
                    # Skip sub/grand total lines
                    if any(skip in line.lower() for skip in _SKIP_NAMES):
                        continue
                    # Last token is weight; rest is name
                    parts = line.rsplit(None, 1)
                    if len(parts) != 2:
                        continue
                    name, raw_w = parts[0].strip(), parts[1]
                    # Filter out header remnants
                    if 'sub-total' in name.lower() or name.lower() == 'total':
                        continue
                    w = _safe_float(raw_w)
                    if w is not None and name and name not in seen:
                        seen.add(name)
                        holdings.append({'name': name, 'ticker': None, 'weight_pct': w})

    return holdings


def _parse_amve_format(pdf) -> list[dict]:
    """
    Parse the AllianceBernstein 3-column quarterly portfolio PDF.
    Table format: [Portfolio Holdings, ISIN, Weight]
    """
    holdings = []
    seen = set()

    for page in pdf.pages:
        tbl = page.extract_table()
        if not tbl:
            continue
        for row in tbl:
            if len(row) < 2:
                continue
            name = str(row[0] or '').strip()
            isin = str(row[1] or '').strip() if len(row) > 1 else None
            raw_w = str(row[2] or '').strip() if len(row) > 2 else None

            if not name or name.lower() in ('portfolio holdings', 'isin', 'weight', ''):
                continue
            if name.lower() in _SKIP_NAMES:
                continue

            w = _safe_float(raw_w) if raw_w else None
            if name not in seen:
                seen.add(name)
                holdings.append({
                    'name': name,
                    'ticker': isin if isin and len(isin) >= 10 else None,
                    'weight_pct': w,
                })

    return holdings


def _parse_cboe_quarterly_pdf(content: bytes) -> tuple[list[dict], str | None]:
    """
    Parse a CBOE quarterly portfolio PDF.
    Returns (holdings, portfolio_as_of_date) or ([], None) if not a portfolio doc.
    """
    try:
        import pdfplumber
    except ImportError:
        logger.error('pdfplumber not installed — pip install pdfplumber')
        return [], None

    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            first_text = (pdf.pages[0].extract_text() or '').lower()

            # Verify this is actually a portfolio disclosure
            if not any(kw in first_text for kw in _PORTFOLIO_KEYWORDS):
                logger.debug('Not a portfolio disclosure PDF — skipping')
                return [], None

            as_of = _extract_portfolio_date(first_text)

            # Detect format
            if 'quarterly portfolio disclosure notification' in first_text or \
               'comprised of the below securities' in first_text:
                holdings = _parse_coolabah_format(pdf)
                fmt = 'coolabah'
            elif 'quarterly holdings disclosure' in first_text or \
                 'portfolio holdings' in first_text:
                holdings = _parse_amve_format(pdf)
                fmt = 'amve'
            else:
                logger.warning('Unknown quarterly portfolio PDF format — skipping')
                return [], None

            logger.debug(f'Parsed {fmt} format: {len(holdings)} holdings, as_of={as_of}')
            return holdings, as_of

    except Exception as e:
        logger.warning(f'CBOE quarterly PDF parse error: {e}')
        return [], None


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        s = str(val).replace(',', '').replace('%', '').strip()
        if s in ('', '-', 'N/A', 'n/a'):
            return None
        return float(s)
    except (ValueError, TypeError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Main scraper
# ─────────────────────────────────────────────────────────────────────────────

def scrape_cboe_quarterly_portfolios(db_path=None) -> int:
    """
    Discover and ingest quarterly portfolio disclosures for all CBOE-listed ETFs
    in _CBOE_PREFIXES.

    For each fund:
      1. Scan the past 120 days to find the latest portfolio PDF.
      2. Download and verify it's a genuine disclosure.
      3. Parse holdings.
      4. Upsert into etf_holdings.
      5. Mark etf.holdings_disclosure = 'quarterly' and etf.holdings_as_of = date.
    """
    from datetime import datetime
    from scrapers.db_writer import get_connection, upsert_holdings, log_scrape

    started = datetime.utcnow()
    source = 'cboe_quarterly'
    updated = 0
    conn = get_connection(db_path)

    for etf_code, (prefix, _fmt) in _CBOE_PREFIXES.items():
        try:
            logger.info(f'CBOE quarterly: discovering {etf_code} (prefix={prefix})...')
            fname = _discover_portfolio_url(prefix)
            if not fname:
                logger.warning(f'CBOE quarterly: no portfolio PDF found for {etf_code}')
                continue

            url = _CDN_BASE + fname
            resp = _get(url)
            if not resp:
                logger.warning(f'CBOE quarterly: download failed for {etf_code}: {fname}')
                continue

            holdings, as_of = _parse_cboe_quarterly_pdf(resp.content)
            if not holdings:
                logger.warning(f'CBOE quarterly: no holdings parsed for {etf_code} ({fname})')
                continue

            upsert_holdings(conn, etf_code, holdings)
            conn.execute(
                "UPDATE etfs SET holdings_disclosure=?, holdings_as_of=? WHERE code=?",
                ('quarterly', as_of, etf_code),
            )
            conn.commit()
            updated += 1
            logger.info(
                f'CBOE quarterly: {etf_code} — {len(holdings)} holdings '
                f'(as_of={as_of}, file={fname})'
            )

        except Exception as e:
            logger.warning(f'CBOE quarterly: error for {etf_code}: {e}')
            conn.rollback()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success' if updated else 'no_data',
               records_affected=updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'CBOE quarterly: updated {updated}/{len(_CBOE_PREFIXES)} ETFs')
    return updated
