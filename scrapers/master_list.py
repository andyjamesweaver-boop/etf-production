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

    # Propagate sector allocations from holdings for ETFs that don't yet have sector rows.
    # Aggregates sector weights from etf_holdings.sector for each fund, skipping cash.
    conn.execute('''
        INSERT OR REPLACE INTO etf_sectors (etf_code, sector, weight_pct)
        SELECT etf_code, sector, ROUND(SUM(weight_pct), 6)
        FROM etf_holdings
        WHERE sector IS NOT NULL
          AND sector NOT IN ('Cash and/or Derivatives', 'Cash', 'Derivatives')
          AND etf_code NOT IN (SELECT DISTINCT etf_code FROM etf_sectors)
        GROUP BY etf_code, sector
    ''')

    # ── Sync units_on_issue from etp_monthly ──────────────────────────────────
    # For every ETF, take the most-recent etp_monthly row and propagate
    # total_units → etfs.units_on_issue where it is currently missing.
    # Also fill fund_size_aud_millions from chess_mc where FUM is absent.
    conn.execute('''
        UPDATE etfs
        SET units_on_issue = (
            SELECT CAST(m.total_units AS INTEGER)
            FROM etp_monthly m
            WHERE m.code = etfs.code
              AND m.total_units IS NOT NULL
            ORDER BY m.date DESC
            LIMIT 1
        )
        WHERE units_on_issue IS NULL
          AND EXISTS (
            SELECT 1 FROM etp_monthly m
            WHERE m.code = etfs.code AND m.total_units IS NOT NULL
          )
    ''')
    conn.execute('''
        UPDATE etfs
        SET fund_size_aud_millions = ROUND((
            SELECT m.chess_mc / 1000000.0
            FROM etp_monthly m
            WHERE m.code = etfs.code
              AND m.chess_mc IS NOT NULL
            ORDER BY m.date DESC
            LIMIT 1
        ), 2)
        WHERE fund_size_aud_millions IS NULL
          AND EXISTS (
            SELECT 1 FROM etp_monthly m
            WHERE m.code = etfs.code AND m.chess_mc IS NOT NULL
          )
    ''')
    synced = conn.execute(
        "SELECT COUNT(*) FROM etfs WHERE units_on_issue IS NOT NULL"
    ).fetchone()[0]
    logger.info(f"Master list: {synced} ETFs have units_on_issue after etp_monthly sync")

    # ── Estimate units for ETFs still missing (CXA / newer listings) ─────────
    # Derive units_on_issue = fund_size_aud_millions * 1e6 / current_price
    # for any ETF that still has no units but has both FUM and a price.
    conn.execute('''
        UPDATE etfs
        SET units_on_issue = CAST(
            ROUND(fund_size_aud_millions * 1000000.0 / current_price, 0) AS INTEGER
        )
        WHERE units_on_issue IS NULL
          AND fund_size_aud_millions IS NOT NULL
          AND current_price IS NOT NULL
          AND current_price > 0
    ''')
    estimated = conn.execute('''
        SELECT COUNT(*) FROM etfs
        WHERE units_on_issue IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM etp_monthly m
            WHERE m.code = etfs.code AND m.total_units IS NOT NULL
          )
          AND fund_size_aud_millions IS NOT NULL
    ''').fetchone()[0]
    logger.info(f"Master list: {estimated} ETFs got estimated units_on_issue from FUM/price")

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
