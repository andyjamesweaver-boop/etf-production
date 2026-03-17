#!/usr/bin/env python3
"""
intraday_prices.py — fetch live ASX prices during market hours.

Called by launchd (com.etf.prices.plist) on a 20-minute StartInterval.
Acts as a no-op outside Mon–Fri 09:40–16:40 Sydney time so that launchd
can fire unconditionally every 20 minutes without log noise at night.

Coverage: ASX-listed ETFs only (436 funds).
CXA (Cboe Australia) ETFs do not have a live price API source yet.
"""

import logging
import os
import sys
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

os.makedirs(os.path.join(ROOT, 'logs'), exist_ok=True)
logging.basicConfig(
    filename=os.path.join(ROOT, 'logs', 'intraday_prices.log'),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

SYDNEY       = ZoneInfo('Australia/Sydney')
MARKET_OPEN  = dtime(9, 40)
MARKET_CLOSE = dtime(16, 40)


def within_trading_hours() -> bool:
    now = datetime.now(SYDNEY)
    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    return MARKET_OPEN <= now.time() <= MARKET_CLOSE


def main():
    if not within_trading_hours():
        return  # silent exit — no log noise outside market hours

    now_str = datetime.now(SYDNEY).strftime('%Y-%m-%d %H:%M %Z')
    logger.info(f'Intraday price refresh starting — {now_str}')
    try:
        from scrapers.run_all import run_asx_api
        updated = run_asx_api()
        logger.info(f'Price refresh complete — {updated} ASX ETFs updated')
    except Exception as exc:
        logger.error(f'Price refresh failed: {exc}', exc_info=True)


if __name__ == '__main__':
    main()
