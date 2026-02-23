"""
Master list builder — merges ASX + Cboe sources, deduplicates, assigns FUM ranks.
"""

import logging
from datetime import datetime

from scrapers.db_writer import get_connection, log_scrape, update_issuer_stats

logger = logging.getLogger(__name__)


def build_master_list(db_path=None) -> int:
    """
    Post-processing step that runs after all scrapers:
      1. Assigns rank_by_fum based on fund_size_aud_millions DESC
      2. Updates issuer stats (etf_count, total_fum)
      3. Fills in any missing exchange values

    Returns total ETF count.
    """
    started = datetime.utcnow()
    source = 'master_list'

    conn = get_connection(db_path)

    # Default exchange to ASX if missing
    conn.execute("UPDATE etfs SET exchange = 'ASX' WHERE exchange IS NULL")

    # Assign FUM ranks (NULLs go to the end)
    conn.execute('''
        UPDATE etfs SET rank_by_fum = (
            SELECT COUNT(*) + 1
            FROM etfs AS e2
            WHERE e2.fund_size_aud_millions > etfs.fund_size_aud_millions
               OR (e2.fund_size_aud_millions = etfs.fund_size_aud_millions AND e2.code < etfs.code)
        )
        WHERE fund_size_aud_millions IS NOT NULL
    ''')

    # ETFs without FUM get rank after all ranked ones
    max_rank_row = conn.execute(
        "SELECT MAX(rank_by_fum) FROM etfs WHERE rank_by_fum IS NOT NULL"
    ).fetchone()
    max_rank = (max_rank_row[0] or 0) if max_rank_row else 0

    conn.execute(f'''
        UPDATE etfs SET rank_by_fum = {max_rank} + (
            SELECT COUNT(*) + 1
            FROM etfs AS e2
            WHERE e2.fund_size_aud_millions IS NULL
              AND e2.code < etfs.code
        )
        WHERE fund_size_aud_millions IS NULL AND rank_by_fum IS NULL
    ''')

    # Update issuer stats
    update_issuer_stats(conn)

    # Count totals
    total_row = conn.execute("SELECT COUNT(*) FROM etfs").fetchone()
    total = total_row[0] if total_row else 0

    asx_row = conn.execute("SELECT COUNT(*) FROM etfs WHERE exchange = 'ASX'").fetchone()
    cboe_row = conn.execute("SELECT COUNT(*) FROM etfs WHERE exchange IN ('CBOE', 'CXA')").fetchone()
    asx_count = asx_row[0] if asx_row else 0
    cboe_count = cboe_row[0] if cboe_row else 0

    conn.commit()

    duration = (datetime.utcnow() - started).total_seconds()
    log_scrape(conn, source, 'success', records_affected=total,
               duration_secs=duration, started_at=started.isoformat())
    conn.close()

    logger.info(f"Master list: {total} ETFs (ASX: {asx_count}, CXA: {cboe_count})")
    return total
