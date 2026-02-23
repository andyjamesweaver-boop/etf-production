"""
ASX JSON API scraper — fetches live price data for ETFs.

Uses the ASX Research API endpoint for individual ETF headers.
"""

import re
import logging
from datetime import datetime

from scrapers.config import ASX_ETF_API_BASE
from scrapers.base_scraper import fetch_json
from scrapers.db_writer import get_connection, upsert_etf, log_scrape

logger = logging.getLogger(__name__)


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    if val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def fetch_etf_price(code: str) -> dict | None:
    """
    Fetch live price data from ASX API for a single ETF.
    Returns a dict of fields to upsert, or None on failure.
    """
    url = ASX_ETF_API_BASE.format(code=code)
    data = fetch_json(url, headers={
        'Accept': 'application/json',
        'Origin': 'https://www2.asx.com.au',
        'Referer': 'https://www2.asx.com.au/',
    })

    if not data:
        return None

    # ASX API typically wraps data in a 'data' key
    info = data.get('data', data)
    if not isinstance(info, dict):
        return None

    result = {
        'code': code.upper(),
        'current_price': _safe_float(info.get('priceLast') or info.get('lastPrice')),
        'day_change_pct': _safe_float(info.get('priceChangePercent') or info.get('changePercent')),
        'bid_price': _safe_float(info.get('priceBid') or info.get('bidPrice')),
        'offer_price': _safe_float(info.get('priceAsk') or info.get('offerPrice')),
        'year_high': _safe_float(info.get('yearHighPrice')),
        'year_low': _safe_float(info.get('yearLowPrice')),
        'volume': _safe_int(info.get('volume')),
        'inception_date': info.get('dateListed'),
        'data_source': 'asx_api',
    }

    # Market cap -> FUM estimate (for ETFs, market cap ~= FUM)
    mktcap = _safe_float(info.get('marketCap'))
    if mktcap and mktcap > 0:
        result['fund_size_aud_millions'] = round(mktcap / 1_000_000, 1)

    # Calculate bid-ask spread
    if result.get('bid_price') and result.get('offer_price') and result['bid_price'] > 0:
        mid = (result['bid_price'] + result['offer_price']) / 2
        if mid > 0:
            result['bid_ask_spread_pct'] = round(
                (result['offer_price'] - result['bid_price']) / mid * 100, 4
            )

    # Try to get name if available
    name = info.get('displayName') or info.get('desc_full') or info.get('name')
    if name:
        result['name'] = name.strip()

    return {k: v for k, v in result.items() if v is not None}


def scrape_asx_prices(db_path=None, codes: list[str] | None = None) -> int:
    """
    Fetch live prices from ASX API for all ETFs in the database
    (or a specific list of codes).  Returns count updated.
    """
    started = datetime.utcnow()
    source = 'asx_api'

    conn = get_connection(db_path)

    if codes is None:
        rows = conn.execute(
            "SELECT code FROM etfs WHERE exchange = 'ASX' OR exchange IS NULL ORDER BY rank_by_fum"
        ).fetchall()
        codes = [r['code'] for r in rows]

    if not codes:
        logger.warning("No ETF codes to fetch prices for")
        conn.close()
        return 0

    logger.info(f"Fetching ASX prices for {len(codes)} ETFs...")
    updated = 0
    errors = 0

    for i, code in enumerate(codes):
        data = fetch_etf_price(code)
        if data:
            upsert_etf(conn, data)
            updated += 1
        else:
            errors += 1

        if (i + 1) % 50 == 0:
            conn.commit()
            logger.info(f"  Progress: {i+1}/{len(codes)} (updated: {updated}, errors: {errors})")

    conn.commit()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success', records_affected=updated,
               error=f"{errors} failures" if errors else None,
               duration_secs=duration, started_at=started.isoformat())
    conn.close()

    logger.info(f"ASX prices: updated {updated}/{len(codes)} ETFs in {duration:.1f}s")
    return updated
