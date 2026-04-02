"""
Background scheduler for the ETF platform.

Runs the PCF / holdings refresh twice daily at Sydney local time:
  - 12:00 noon  (Australia/Sydney)
  -  8:00 PM    (Australia/Sydney)

Start from the server with:
    from scheduler import start_scheduler
    start_scheduler()

Uses Python's `schedule` library for job timing and `zoneinfo` for
Sydney-aware next-run calculation so DST transitions are handled correctly.
"""

import logging
import os
import sys
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import schedule

# Ensure the project root is importable from wherever this module is loaded
_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

logger = logging.getLogger(__name__)

SYDNEY = ZoneInfo("Australia/Sydney")

# Times to run the PCF scraper each day (Sydney local time, 24-hour)
PCF_SCHEDULE_TIMES = ["12:00", "20:00"]


def _run_pcf_job():
    """Execute the PCF scraper pipeline; log start/finish to stdout."""
    now_sydney = datetime.now(SYDNEY).strftime("%Y-%m-%d %H:%M %Z")
    logger.info(f"[scheduler] PCF job starting — Sydney time {now_sydney}")
    try:
        from scrapers.run_all import run_pcf
        count = run_pcf()
        logger.info(f"[scheduler] PCF job complete — {count} records updated")
    except Exception as e:
        logger.error(f"[scheduler] PCF job failed: {e}", exc_info=True)


def _run_nav_job():
    """Fetch daily NAV + premium/discount from Yahoo Finance after ASX close."""
    now_sydney = datetime.now(SYDNEY).strftime("%Y-%m-%d %H:%M %Z")
    logger.info(f"[scheduler] NAV/premium-discount job starting — {now_sydney}")
    try:
        from scrapers.nav_fetcher import scrape_nav
        count = scrape_nav()
        logger.info(f"[scheduler] NAV job complete — {count} records updated")
    except Exception as e:
        logger.error(f"[scheduler] NAV job failed: {e}", exc_info=True)


def _run_price_history_job():
    """Update price history from Yahoo Finance (weekly, Sunday evenings)."""
    now_sydney = datetime.now(SYDNEY).strftime("%Y-%m-%d %H:%M %Z")
    logger.info(f"[scheduler] Price history update starting — {now_sydney}")
    try:
        import subprocess, sys
        result = subprocess.run(
            [sys.executable, "scrapers/price_history_fetcher.py", "--update"],
            capture_output=True, text=True, timeout=1800
        )
        logger.info(f"[scheduler] Price history update complete — exit {result.returncode}")
        if result.stdout:
            logger.info(result.stdout[-2000:])
        if result.returncode != 0 and result.stderr:
            logger.error(result.stderr[-2000:])
    except Exception as e:
        logger.error(f"[scheduler] Price history update failed: {e}", exc_info=True)


def _schedule_loop():
    """
    Convert Sydney schedule times to UTC, register jobs, then run the
    pending-check loop.  Re-registers jobs daily so that DST changes
    (AEST ↔ AEDT) are picked up automatically on the next midnight cycle.
    """
    logger.info(f"[scheduler] Starting — PCF runs scheduled at {PCF_SCHEDULE_TIMES} Sydney time")

    def _register_today():
        schedule.clear("pcf")
        schedule.clear("nav")
        today = datetime.now(SYDNEY).date()
        from datetime import timezone as _tz

        for hhmm in PCF_SCHEDULE_TIMES:
            h, m = map(int, hhmm.split(":"))
            sydney_dt = datetime(today.year, today.month, today.day, h, m, tzinfo=SYDNEY)
            utc_hhmm = sydney_dt.astimezone(_tz.utc).strftime("%H:%M")
            logger.info(f"[scheduler] Registering PCF job at {hhmm} Sydney = {utc_hhmm} UTC")
            schedule.every().day.at(utc_hhmm, "UTC").do(_run_pcf_job).tag("pcf")

        # NAV / premium-discount: daily at 17:30 Sydney (ASX closes 16:00, NAV published ~17:00)
        nav_sydney = datetime(today.year, today.month, today.day, 17, 30, tzinfo=SYDNEY)
        nav_utc = nav_sydney.astimezone(_tz.utc).strftime("%H:%M")
        logger.info(f"[scheduler] Registering NAV job at 17:30 Sydney = {nav_utc} UTC")
        schedule.every().day.at(nav_utc, "UTC").do(_run_nav_job).tag("nav")

    # Re-register at midnight UTC so any DST shift is absorbed automatically
    _register_today()
    schedule.every().day.at("00:01", "UTC").do(_register_today).tag("pcf-reregister")

    # Price history: update weekly on Sunday at 18:00 Sydney (market closed)
    from datetime import timezone as _tz
    _ph_sydney = datetime(2000, 1, 2, 18, 0, tzinfo=SYDNEY)  # Sunday
    _ph_utc = _ph_sydney.astimezone(_tz.utc).strftime("%H:%M")
    schedule.every().sunday.at(_ph_utc, "UTC").do(_run_price_history_job).tag("price-history")
    logger.info(f"[scheduler] Price history update scheduled weekly Sunday 18:00 Sydney = {_ph_utc} UTC")

    while True:
        schedule.run_pending()
        time.sleep(30)


def start_scheduler():
    """
    Launch the scheduler in a daemon background thread.
    Safe to call multiple times — only starts one thread.
    """
    t = threading.Thread(target=_schedule_loop, name="pcf-scheduler", daemon=True)
    t.start()
    logger.info("[scheduler] Background scheduler thread started")
    return t
