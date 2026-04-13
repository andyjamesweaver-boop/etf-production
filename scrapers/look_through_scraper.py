"""
Look-through holdings scraper for feeder/wrapper ETFs.

For ETFs that invest substantially in a single underlying fund, this scraper
fetches the underlying fund's holdings and stores them as layer=2 in etf_holdings.

Supports:
  - Avantis UCITS ETFs  (avantisinvestors.com)
  - iShares UCITS ETFs  (ishares.com/uk)
  - DB-derived:  feeder ETFs whose underlying is another ASX-listed ETF already in DB
"""

import logging
import re
import io

logger = logging.getLogger(__name__)

# ── Feeder map: ASX code → underlying ASX code (already in DB) ──────────────
# When the underlying is another ASX ETF we already scrape, we just copy its
# layer-1 holdings as this ETF's layer-2.
_DB_FEEDERS = {
    'G200':  'A200',   # BetaShares WealthBuilder Aus 200 Geared → A200
    'GNDQ':  'NDQ',    # BetaShares WealthBuilder Nasdaq Geared → NDQ
    'GGBL':  'BGBL',   # BetaShares WealthBuilder Global Geared → BGBL
    'GGUS':  'IVV',    # BetaShares Geared US Equity → IVV (S&P 500)
    'UMAX':  'IVV',    # BetaShares S&P 500 Yield Maximiser → IVV
    'IHVV':  'IVV',    # iShares S&P 500 AUD Hedged → IVV
    'WXHG':  'WXOZ',   # SPDR World ex Aus Hedged → WXOZ (same portfolio, FX hedged)
}

# ── Avantis UCITS ETFs: AU ETF code → UCITS fund page slug ──────────────────
_AVANTIS_UCITS = {
    'AVNG': 'avantis-global-equity-ucits-etf',
    'AVTE': 'avantis-emerging-markets-equity-ucits-etf',
    'AVSV': 'avantis-global-small-cap-value-ucits-etf',
}
_AVANTIS_UCITS_BASE = 'https://www.avantisinvestors.com/ucits/'

# ── iShares UCITS: AU ETF code → iShares UK product ID ──────────────────────
# Product IDs from ishares.com/uk/individual/en/products/{id}/
# Format: ASX code → (product_id, slug, tier, ticker)
_ISHARES_UCITS = {
    'ULTB': ('272124', 'ishares-usd-treasury-bond-20-yr-ucits-etf',              'individual',   'IDTL'),
    'AESG': ('319887', 'ishares-global-aggregate-bond-esg-ucits-etf',             'professional', 'AGGE'),
    'EMXC': ('315592', 'ishares-msci-em-ex-china-ucits-etf',                      'individual',   'EXCS'),
    'ITEK': ('340307', 'ishares-nasdaq-100-top-30-ucits-etf',                     'individual',   'QTOP'),
    'IEM':  ('264182', 'ishares-msci-emerging-markets-ucits-etf',                 'individual',   'EIMI'),
    'IHEB': ('251854', 'ishares-jp-morgan-usd-emerging-markets-bond-ucits-etf',   'individual',   'SEMB'),
}
_ISHARES_HOLDINGS_URL = (
    'https://www.ishares.com/uk/{tier}/en/products/{pid}/{slug}/'
    '1506575576011.ajax?fileType=csv&fileName={ticker}_holdings&dataType=fund'
)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get(url, timeout=20):
    try:
        import requests, urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        s = requests.Session()
        s.headers['User-Agent'] = (
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        )
        r = s.get(url, timeout=timeout, verify=False)
        return r if r.status_code == 200 else None
    except Exception as e:
        logger.debug(f'HTTP error {url}: {e}')
        return None


# ─────────────────────────────────────────────────────────────────────────────
# DB-derived look-throughs (copy underlying ASX ETF's layer-1 as layer-2)
# ─────────────────────────────────────────────────────────────────────────────

def _sync_db_feeders(conn) -> int:
    """
    For each feeder in _DB_FEEDERS, replace its layer-2 holdings with the
    current layer-1 holdings of the underlying ETF.
    Returns number of feeder ETFs updated.
    """
    updated = 0
    for feeder_code, source_code in _DB_FEEDERS.items():
        source_count = conn.execute(
            "SELECT COUNT(*) FROM etf_holdings WHERE etf_code=? AND COALESCE(layer,1)=1",
            (source_code,)
        ).fetchone()[0]
        if not source_count:
            logger.debug(f'look-through: no layer-1 holdings for {source_code}, skipping {feeder_code}')
            continue

        conn.execute(
            "DELETE FROM etf_holdings WHERE etf_code=? AND layer=2", (feeder_code,)
        )
        conn.execute(
            "INSERT INTO etf_holdings (etf_code, name, ticker, weight_pct, sector, country, layer) "
            "SELECT ?, name, ticker, weight_pct, sector, country, 2 "
            "FROM etf_holdings WHERE etf_code=? AND COALESCE(layer,1)=1",
            (feeder_code, source_code)
        )
        inserted = conn.execute(
            "SELECT COUNT(*) FROM etf_holdings WHERE etf_code=? AND layer=2", (feeder_code,)
        ).fetchone()[0]
        logger.info(f'look-through: {feeder_code} ← {source_code}: {inserted} rows')
        updated += 1

    conn.commit()
    return updated


# ─────────────────────────────────────────────────────────────────────────────
# Avantis UCITS
# ─────────────────────────────────────────────────────────────────────────────

def _safe_float(val):
    if val is None:
        return None
    try:
        s = str(val).replace(',', '').replace('%', '').strip()
        return float(s) if s not in ('', '-', 'N/A') else None
    except (ValueError, TypeError):
        return None


def _fetch_avantis_ucits_holdings(slug: str) -> list[dict]:
    """
    Fetch holdings for an Avantis UCITS ETF from their website.
    Returns list of {name, ticker, weight_pct, sector, country} dicts.
    """
    url = _AVANTIS_UCITS_BASE + slug + '/'
    resp = _get(url)
    if not resp:
        logger.warning(f'Avantis UCITS: could not fetch {url}')
        return []

    # Look for a holdings CSV link in the page
    csv_pat = re.search(
        r'(https://res\.avantisinvestors\.com/[^\'"]+holdings[^\'"]*\.csv)',
        resp.text, re.IGNORECASE
    )
    if csv_pat:
        csv_url = csv_pat.group(1)
        csv_resp = _get(csv_url)
        if csv_resp:
            return _parse_avantis_csv(csv_resp.text)

    # Fallback: parse holdings table from HTML
    return _parse_avantis_html_holdings(resp.text)


def _parse_avantis_csv(text: str) -> list[dict]:
    """Parse Avantis holdings CSV (if available)."""
    import csv
    holdings = []
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        name = (row.get('Security Name') or row.get('Name') or '').strip()
        ticker = (row.get('Ticker') or row.get('Symbol') or '').strip() or None
        weight = _safe_float(row.get('Weight') or row.get('Weight (%)') or row.get('% of Portfolio'))
        sector = (row.get('Sector') or '').strip() or None
        country = (row.get('Country') or row.get('Location') or '').strip() or None
        if name and weight is not None:
            holdings.append({'name': name, 'ticker': ticker, 'weight_pct': weight,
                             'sector': sector, 'country': country})
    return holdings


def _parse_avantis_html_holdings(html: str) -> list[dict]:
    """Parse holdings table from Avantis fund page HTML."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.warning('beautifulsoup4 not installed — cannot parse Avantis HTML holdings')
        return []

    soup = BeautifulSoup(html, 'html.parser')
    holdings = []

    # Look for a table with "Company" or "Weight" headers
    for tbl in soup.find_all('table'):
        headers = [th.get_text(strip=True).lower() for th in tbl.find_all('th')]
        name_idx = next((i for i, h in enumerate(headers) if 'company' in h or 'name' in h or 'security' in h), None)
        weight_idx = next((i for i, h in enumerate(headers) if 'weight' in h or '%' in h), None)
        ticker_idx = next((i for i, h in enumerate(headers) if 'ticker' in h or 'symbol' in h), None)
        sector_idx = next((i for i, h in enumerate(headers) if 'sector' in h), None)
        country_idx = next((i for i, h in enumerate(headers) if 'country' in h or 'location' in h), None)

        if name_idx is None or weight_idx is None:
            continue

        for row in tbl.find_all('tr')[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all('td')]
            if len(cells) <= max(name_idx, weight_idx):
                continue
            name = cells[name_idx].strip()
            weight = _safe_float(cells[weight_idx])
            ticker = cells[ticker_idx].strip() if ticker_idx is not None and len(cells) > ticker_idx else None
            sector = cells[sector_idx].strip() if sector_idx is not None and len(cells) > sector_idx else None
            country = cells[country_idx].strip() if country_idx is not None and len(cells) > country_idx else None
            if name and weight is not None:
                holdings.append({'name': name, 'ticker': ticker or None,
                                 'weight_pct': weight, 'sector': sector, 'country': country})

    return holdings


# ─────────────────────────────────────────────────────────────────────────────
# iShares UCITS
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_ishares_ucits_holdings(pid: str, slug: str, tier: str, ticker: str) -> list[dict]:
    """
    Fetch holdings CSV from the iShares UK website.
    Returns list of {name, ticker, weight_pct, sector, country} dicts.
    """
    url = _ISHARES_HOLDINGS_URL.format(pid=pid, slug=slug, tier=tier, ticker=ticker)
    resp = _get(url)
    if not resp:
        logger.warning(f'iShares UCITS: could not fetch holdings for {ticker} (pid={pid})')
        return []
    return _parse_ishares_csv(resp.text)


def _parse_ishares_csv(text: str) -> list[dict]:
    """
    Parse iShares UK holdings CSV. Format: BOM + metadata rows, then blank line,
    then Ticker,Name,Sector,Asset Class,Market Value,Weight (%),...
    """
    import csv
    # Strip BOM
    text = text.lstrip('\ufeff')
    lines = text.splitlines()

    # Find the header row: must contain both 'Name' and 'Weight'
    header_idx = None
    for i, line in enumerate(lines):
        if 'Name' in line and 'Weight' in line:
            header_idx = i
            break
    if header_idx is None:
        logger.warning('iShares CSV: could not find header row')
        return []

    data_text = '\n'.join(lines[header_idx:])
    reader = csv.DictReader(io.StringIO(data_text))

    holdings = []
    for row in reader:
        name = (row.get('Name') or '').strip()
        tkr = (row.get('Ticker') or '').strip() or None
        weight = _safe_float(row.get('Weight (%)') or row.get('Weight'))
        sector = (row.get('Sector') or '').strip() or None
        country = (row.get('Location') or row.get('Country') or '').strip() or None
        asset_class = (row.get('Asset Class') or '').strip()

        # Skip cash, money market, FX, futures
        if asset_class.lower() in ('cash', 'futures', 'fx', 'money market'):
            continue
        if not name or name.lower() in ('', '-', 'n/a'):
            continue
        if weight is None or weight == 0:
            continue

        holdings.append({
            'name': name,
            'ticker': tkr,
            'weight_pct': weight,
            'sector': sector,
            'country': country,
        })

    return holdings


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def scrape_look_through_holdings(db_path=None) -> int:
    """
    Update layer-2 (look-through) holdings for all known feeder ETFs.
    Returns total number of ETFs updated.
    """
    from datetime import datetime
    from scrapers.db_writer import get_connection, log_scrape

    started = datetime.utcnow()
    conn = get_connection(db_path)
    total_updated = 0

    # 1. DB-derived feeders (copy from underlying ASX ETF)
    db_updated = _sync_db_feeders(conn)
    total_updated += db_updated
    logger.info(f'look-through: DB-derived feeders updated: {db_updated}')

    # 2. Avantis UCITS
    for etf_code, slug in _AVANTIS_UCITS.items():
        try:
            holdings = _fetch_avantis_ucits_holdings(slug)
            if holdings:
                conn.execute("DELETE FROM etf_holdings WHERE etf_code=? AND layer=2", (etf_code,))
                for h in holdings:
                    conn.execute(
                        "INSERT OR IGNORE INTO etf_holdings "
                        "(etf_code, name, ticker, weight_pct, sector, country, layer) "
                        "VALUES (?,?,?,?,?,?,2)",
                        (etf_code, h['name'], h.get('ticker'), h.get('weight_pct'),
                         h.get('sector'), h.get('country'))
                    )
                conn.commit()
                total_updated += 1
                logger.info(f'look-through: {etf_code} (Avantis UCITS) → {len(holdings)} holdings')
            else:
                logger.warning(f'look-through: no holdings found for {etf_code} ({slug})')
        except Exception as e:
            logger.warning(f'look-through: error for {etf_code}: {e}')

    # 3. iShares UCITS
    for etf_code, (pid, slug, tier, ticker) in _ISHARES_UCITS.items():
        try:
            holdings = _fetch_ishares_ucits_holdings(pid, slug, tier, ticker)
            if holdings:
                conn.execute("DELETE FROM etf_holdings WHERE etf_code=? AND layer=2", (etf_code,))
                for h in holdings:
                    conn.execute(
                        "INSERT OR IGNORE INTO etf_holdings "
                        "(etf_code, name, ticker, weight_pct, sector, country, layer) "
                        "VALUES (?,?,?,?,?,?,2)",
                        (etf_code, h['name'], h.get('ticker'), h.get('weight_pct'),
                         h.get('sector'), h.get('country'))
                    )
                conn.commit()
                total_updated += 1
                logger.info(f'look-through: {etf_code} (iShares UCITS pid={pid}) → {len(holdings)} holdings')
            else:
                logger.warning(f'look-through: no holdings found for {etf_code} (pid={pid})')
        except Exception as e:
            logger.warning(f'look-through: error for {etf_code}: {e}')

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, 'look_through', 'success' if total_updated else 'no_data',
               records_affected=total_updated, duration_secs=duration,
               started_at=started.isoformat())
    conn.close()
    logger.info(f'look-through: complete — {total_updated} ETFs updated')
    return total_updated
