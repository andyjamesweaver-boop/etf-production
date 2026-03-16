"""
Migration 0004 — fund_type column
==================================
Adds fund_type TEXT column to etfs, using ASX product type codes:
  ETF      — passive, index-tracking ETF
  Active   — actively managed ETF
  Complex  — complex/alternative actively managed ETF
  SP       — structured product (physical commodity)
  Index    — index accumulation product

Backfills from fund name heuristics for rows not yet populated by the scraper.
"""


def up(conn):
    existing = {row[1] for row in conn.execute("PRAGMA table_info(etfs)").fetchall()}

    if 'fund_type' not in existing:
        conn.execute("ALTER TABLE etfs ADD COLUMN fund_type TEXT")

    # Backfill from name keywords for rows with no fund_type yet
    conn.execute("""
        UPDATE etfs SET fund_type = 'Active'
        WHERE fund_type IS NULL AND (
            name LIKE '%Active ETF%'
        )
    """)
    conn.execute("""
        UPDATE etfs SET fund_type = 'Complex'
        WHERE fund_type IS NULL AND (
            name LIKE '%Complex ETF%'
        )
    """)
    conn.execute("""
        UPDATE etfs SET fund_type = 'SP'
        WHERE fund_type IS NULL AND (
            name LIKE '%Structured%'
        )
    """)
    conn.execute("""
        UPDATE etfs SET fund_type = 'Index'
        WHERE fund_type IS NULL AND (
            name LIKE '%Accumulation%' AND name NOT LIKE '%Active ETF%'
        )
    """)
    # Remaining unknowns default to passive ETF
    conn.execute("""
        UPDATE etfs SET fund_type = 'ETF'
        WHERE fund_type IS NULL AND name IS NOT NULL
    """)
