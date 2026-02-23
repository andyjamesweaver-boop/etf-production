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


SOURCES = {
    'asx_report': ('ASX Monthly Report', run_asx_report),
    'cboe': ('Cboe Australia', run_cboe),
    'asx_api': ('ASX Live Prices', run_asx_api),
    'issuers': ('Issuer Websites', run_issuers),
    'master': ('Master List Builder', run_master),
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
  all         Run all of the above in order (default)
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
        sources = ['asx_report', 'cboe', 'issuers', 'asx_api', 'master']
    else:
        sources = [args.source]
        # Always run master list after individual scrapers
        if args.source not in ('master',) and 'master' not in sources:
            sources.append('master')

    run_pipeline(sources)


if __name__ == '__main__':
    main()
