"""
Migration 0005 — nav_history table
====================================
Stores daily NAV (Net Asset Value) per unit for each ETF, alongside the
closing price for that day, so premium/discount can be calculated historically.

premium_discount_pct = (close_price - nav) / nav * 100
  positive = trading at a premium to NAV
  negative = trading at a discount to NAV
"""


def up(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nav_history (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            etf_code              TEXT NOT NULL,
            date                  TEXT NOT NULL,
            nav                   REAL NOT NULL,
            close_price           REAL,
            premium_discount_pct  REAL,
            source                TEXT,
            last_updated          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(etf_code, date),
            FOREIGN KEY (etf_code) REFERENCES etfs(code) ON DELETE CASCADE
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_nav_history_etf_date ON nav_history(etf_code, date DESC)"
    )
