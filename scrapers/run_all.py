#!/usr/bin/env python3
"""
CLI orchestrator for the ETF scraping pipeline.

Usage:
    python -m scrapers.run_all                    # run everything
    python -m scrapers.run_all --source asx_report
    python -m scrapers.run_all --source cboe
    python -m scrapers.run_all --source asx_api
    python -m scrapers.run_all --source issuers
    python -m scrapers.run_all --source master    # just rebuild ranks
    python -m scrapers.run_all --source all       # same as no flag
"""

import argparse
import logging
import sys
import os
import time
from datetime import datetime

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.config import DB_PATH
from scrapers.db_writer import get_connection, log_scrape


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%H:%M:%S',
        handlers=[logging.StreamHandler()],
    )


def ensure_db():
    """Make sure the database schema exists."""
    # Import setup_db from project root
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, project_dir)
    from setup_db import setup_database
    setup_database(DB_PATH)


def run_asx_report():
    from scrapers.asx_report_scraper import scrape_asx_report
    return scrape_asx_report(DB_PATH)


def run_cboe():
    from scrapers.cboe_scraper import scrape_cboe
    return scrape_cboe(DB_PATH)


def run_asx_api():
    from scrapers.asx_etf_scraper import scrape_asx_prices
    return scrape_asx_prices(DB_PATH)


def run_issuers():
    from scrapers.issuer_scrapers import scrape_all_issuers
    return scrape_all_issuers(DB_PATH)


def run_master():
    from scrapers.master_list import build_master_list
    return build_master_list(DB_PATH)


def run_prices_and_fum():
    """
    Twice-daily price refresh pipeline:
      1. Fetch live prices + market-cap FUM from ASX API for all ETFs.
      2. Recalculate fund_size_aud_millions = units_on_issue * current_price / 1e6
         for any ETF where the ASX API did not return a market cap (newly listed,
         Cboe-only, or API gap).
      3. Rebuild FUM ranks and issuer totals via build_master_list.

    Run at 10:30 and 16:20 Sydney time to capture the open and post-close snapshot.
    """
    logger = logging.getLogger('run_prices_and_fum')

    from scrapers.asx_etf_scraper import scrape_asx_prices
    from scrapers.master_list import build_master_list
    from scrapers.db_writer import get_connection

    # Step 1 — fetch live prices (also writes fund_size_aud_millions from marketCap)
    updated = scrape_asx_prices(DB_PATH)
    logger.info(f"Price scrape: {updated} ETFs updated")

    # Step 2 — backfill FUM for ETFs the ASX API didn't return a market cap for,
    # using the most recent known units_on_issue * fresh current_price
    conn = get_connection(DB_PATH)
    result = conn.execute('''
        UPDATE etfs
        SET fund_size_aud_millions = ROUND(units_on_issue * current_price / 1000000.0, 2),
            last_updated = STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE units_on_issue IS NOT NULL
          AND current_price IS NOT NULL
          AND current_price > 0
          AND (
              fund_size_aud_millions IS NULL
              OR ABS(fund_size_aud_millions - ROUND(units_on_issue * current_price / 1000000.0, 2))
                 / fund_size_aud_millions > 0.10
          )
    ''')
    backfilled = result.rowcount
    conn.commit()
    conn.close()
    logger.info(f"FUM backfill: {backfilled} ETFs recalculated from units × price")

    # Step 3 — rebuild ranks and issuer totals
    build_master_list(DB_PATH)

    return updated


def run_documents():
    from scrapers.document_scraper import scrape_documents
    return scrape_documents(DB_PATH)


def run_nav():
    from scrapers.nav_fetcher import scrape_nav
    return scrape_nav(DB_PATH)


def run_pcf():
    """
    Run only the issuer scrapers that download Portfolio Composition Files (PCFs).
    This is a targeted run intended for twice-daily scheduling: it fetches the
    latest PCF/holdings files from Global X, Macquarie, Dimensional and other
    issuers that publish intraday-updated basket files, then rebuilds FUM ranks.
    """
    from scrapers.issuer_scrapers import scrape_all_issuers
    count = scrape_all_issuers(DB_PATH)
    from scrapers.master_list import build_master_list
    build_master_list(DB_PATH)
    return count


def run_upcoming():
    from scrapers.upcoming_listings_scraper import scrape_upcoming_listings
    return scrape_upcoming_listings(DB_PATH)


def run_cboe_quarterly():
    from scrapers.cboe_quarterly_scraper import scrape_cboe_quarterly_portfolios
    return scrape_cboe_quarterly_portfolios(DB_PATH)


def run_look_through():
    from scrapers.look_through_scraper import scrape_look_through_holdings
    return scrape_look_through_holdings(DB_PATH)


SOURCES = {
    'asx_report': ('ASX Monthly Report', run_asx_report),
    'cboe': ('Cboe Australia', run_cboe),
    'asx_api': ('ASX Live Prices', run_asx_api),
    'issuers': ('Issuer Websites', run_issuers),
    'master': ('Master List Builder', run_master),
    'nav': ('NAV & Premium/Discount', run_nav),
    'documents': ('Document Ingestion & AI Summaries', run_documents),
    'pcf': ('PCF / Holdings Refresh', run_pcf),
    'upcoming': ('Upcoming ETF Listings', run_upcoming),
    'cboe_quarterly': ('CBOE Quarterly Portfolio Disclosures', run_cboe_quarterly),
    'prices': ('Daily Price + FUM Refresh', run_prices_and_fum),
    'look_through': ('Look-through Holdings (Feeder Funds)', run_look_through),
}


def run_pipeline(sources: list[str]):
    """Run the specified sources in order."""
    logger = logging.getLogger('pipeline')
    overall_start = time.time()

    results = {}
    for key in sources:
        if key not in SOURCES:
            logger.error(f"Unknown source: {key}")
            continue

        label, func = SOURCES[key]
        logger.info(f"{'='*60}")
        logger.info(f"Starting: {label}")
        logger.info(f"{'='*60}")

        start = time.time()
        try:
            count = func()
            elapsed = time.time() - start
            results[key] = {'status': 'success', 'count': count, 'time': elapsed}
            logger.info(f"Completed {label}: {count} records in {elapsed:.1f}s")
        except Exception as e:
            elapsed = time.time() - start
            results[key] = {'status': 'error', 'error': str(e), 'time': elapsed}
            logger.error(f"Failed {label}: {e}", exc_info=True)

            # Log error to database
            try:
                conn = get_connection(DB_PATH)
                log_scrape(conn, key, 'error', error=str(e),
                           duration_secs=elapsed,
                           started_at=datetime.utcnow().isoformat())
                conn.close()
            except Exception:
                pass

    total_time = time.time() - overall_start

    # Print summary
    logger.info(f"\n{'='*60}")
    logger.info("PIPELINE SUMMARY")
    logger.info(f"{'='*60}")
    for key, res in results.items():
        label = SOURCES[key][0]
        if res['status'] == 'success':
            logger.info(f"  {label}: {res['count']} records ({res['time']:.1f}s)")
        else:
            logger.info(f"  {label}: FAILED - {res.get('error', 'unknown')} ({res['time']:.1f}s)")
    logger.info(f"  Total time: {total_time:.1f}s")

    # Print DB stats
    try:
        conn = get_connection(DB_PATH)
        etf_count = conn.execute("SELECT COUNT(*) FROM etfs").fetchone()[0]
        issuer_count = conn.execute("SELECT COUNT(*) FROM issuers WHERE etf_count > 0").fetchone()[0]
        conn.close()
        logger.info(f"  Database: {etf_count} ETFs from {issuer_count} issuers")
    except Exception:
        pass

    return results


def main():
    parser = argparse.ArgumentParser(
        description='Australian ETF Data Collection Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Sources:
  asx_report  Download and parse ASX monthly investment products Excel report
  cboe        Fetch Cboe Australia ETF listings
  asx_api     Fetch live prices from ASX JSON API
  issuers     Scrape issuer websites (BetaShares, VanEck, Vanguard, iShares, SPDR, Global X)
  master      Rebuild FUM rankings and issuer stats
  documents   Discover PDS/TMD/factsheet URLs and generate AI summaries (slow, costs money)
  pcf         Download PCF/holdings files from issuers + rebuild FUM ranks (twice-daily)
  upcoming    Scrape ASIC Offer Notice Board for upcoming ETF listings
  all         Run all of the above except 'documents' (default)
        """
    )
    parser.add_argument(
        '--source', '-s',
        choices=list(SOURCES.keys()) + ['all'],
        default='all',
        help='Which scraper(s) to run (default: all)',
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable debug logging',
    )
    parser.add_argument(
        '--skip-db-setup',
        action='store_true',
        help='Skip database schema setup (assumes it already exists)',
    )

    args = parser.parse_args()
    setup_logging(args.verbose)

    logger = logging.getLogger('pipeline')
    logger.info("Australian ETF Data Collection Pipeline")
    logger.info(f"Database: {DB_PATH}")

    if not args.skip_db_setup:
        logger.info("Ensuring database schema...")
        ensure_db()

    if args.source == 'all':
        # documents excluded from default daily run (slow + API costs)
        sources = ['asx_report', 'cboe', 'issuers', 'asx_api', 'master', 'nav', 'upcoming', 'cboe_quarterly']
    else:
        sources = [args.source]
        # Always run master list after individual scrapers, except for
        # standalone sources
        if args.source not in ('master', 'documents', 'nav', 'upcoming') and 'master' not in sources:
            sources.append('master')

    run_pipeline(sources)


if __name__ == '__main__':
    main()
