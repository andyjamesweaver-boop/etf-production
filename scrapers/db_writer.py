"""
Database writer — COALESCE-based upserts so no field is overwritten with NULL.
"""

import sqlite3
import logging
from datetime import datetime
from scrapers.config import DB_PATH

logger = logging.getLogger(__name__)


def get_connection(db_path=None):
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


# ------------------------------------------------------------------ etfs
def upsert_etf(conn, data: dict):
    """
    Insert or update an ETF row.  Uses COALESCE so existing non-NULL values
    are preserved when the incoming value is None.
    """
    code = data.get('code')
    if not code:
        return

    # Filter to non-None values, excluding 'code' from the update fields
    fields = {k: v for k, v in data.items() if k != 'code' and v is not None}
    if not fields:
        return
    data = fields

    # Build the SET clause: col = COALESCE(excluded.col, col)
    columns = ['code'] + list(data.keys()) + ['last_updated']
    placeholders = ['?'] * len(columns)
    values = [code] + list(data.values()) + [datetime.utcnow().isoformat()]

    set_parts = []
    for col in list(data.keys()) + ['last_updated']:
        set_parts.append(f"{col} = COALESCE(excluded.{col}, {col})")

    sql = (
        f"INSERT INTO etfs ({','.join(columns)}) "
        f"VALUES ({','.join(placeholders)}) "
        f"ON CONFLICT(code) DO UPDATE SET {','.join(set_parts)}"
    )

    conn.execute(sql, values)


def upsert_etfs(conn, rows: list[dict], *, commit: bool = True):
    """Batch upsert multiple ETF rows."""
    for row in rows:
        if 'code' in row:
            upsert_etf(conn, row)
    if commit:
        conn.commit()


# -------------------------------------------------------------- holdings
def upsert_holdings(conn, etf_code: str, holdings: list[dict], *, commit: bool = True):
    """Replace holdings for an ETF (delete + insert)."""
    conn.execute("DELETE FROM etf_holdings WHERE etf_code = ?", (etf_code,))
    for h in holdings:
        conn.execute(
            "INSERT INTO etf_holdings (etf_code, name, ticker, weight_pct, sector, country) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (etf_code, h.get('name'), h.get('ticker'), h.get('weight_pct'),
             h.get('sector'), h.get('country'))
        )
    if commit:
        conn.commit()


# --------------------------------------------------------------- sectors
def upsert_sectors(conn, etf_code: str, sectors: list[dict], *, commit: bool = True):
    """Replace sector allocations for an ETF."""
    conn.execute("DELETE FROM etf_sectors WHERE etf_code = ?", (etf_code,))
    for s in sectors:
        conn.execute(
            "INSERT INTO etf_sectors (etf_code, sector, weight_pct) VALUES (?, ?, ?)",
            (etf_code, s.get('sector'), s.get('weight_pct'))
        )
    if commit:
        conn.commit()


# ------------------------------------------------------------- dividends
def upsert_dividend(conn, etf_code: str, dividend: dict, *, commit: bool = True):
    """Upsert a single dividend record."""
    conn.execute(
        "INSERT INTO etf_dividends (etf_code, ex_date, pay_date, amount, franking_pct, type) "
        "VALUES (?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(etf_code, ex_date) DO UPDATE SET "
        "pay_date = COALESCE(excluded.pay_date, pay_date), "
        "amount = COALESCE(excluded.amount, amount), "
        "franking_pct = COALESCE(excluded.franking_pct, franking_pct), "
        "type = COALESCE(excluded.type, type), "
        "last_updated = CURRENT_TIMESTAMP",
        (etf_code, dividend.get('ex_date'), dividend.get('pay_date'),
         dividend.get('amount'), dividend.get('franking_pct'),
         dividend.get('type', 'ordinary'))
    )
    if commit:
        conn.commit()


# --------------------------------------------------------- price history
def upsert_price(conn, etf_code: str, price: dict, *, commit: bool = True):
    """Upsert a single price history record."""
    conn.execute(
        "INSERT INTO price_history (etf_code, date, open, high, low, close, volume) "
        "VALUES (?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(etf_code, date) DO UPDATE SET "
        "open = COALESCE(excluded.open, open), "
        "high = COALESCE(excluded.high, high), "
        "low = COALESCE(excluded.low, low), "
        "close = COALESCE(excluded.close, close), "
        "volume = COALESCE(excluded.volume, volume), "
        "last_updated = CURRENT_TIMESTAMP",
        (etf_code, price.get('date'), price.get('open'), price.get('high'),
         price.get('low'), price.get('close'), price.get('volume'))
    )
    if commit:
        conn.commit()


def upsert_prices_batch(conn, etf_code: str, prices: list[dict], *, commit: bool = True):
    """Batch upsert price history."""
    for p in prices:
        upsert_price(conn, etf_code, p, commit=False)
    if commit:
        conn.commit()


# --------------------------------------------------------------- issuers
def update_issuer_stats(conn, *, commit: bool = True):
    """Recalculate issuer etf_count and total_fum from the etfs table."""
    conn.execute('''
        INSERT OR REPLACE INTO issuers (name, website, fund_list_url, etf_count, total_fum, last_updated)
        SELECT
            e.issuer,
            COALESCE(i.website, ''),
            COALESCE(i.fund_list_url, ''),
            COUNT(*),
            COALESCE(SUM(e.fund_size_aud_millions), 0),
            CURRENT_TIMESTAMP
        FROM etfs e
        LEFT JOIN issuers i ON i.name = e.issuer
        WHERE e.issuer IS NOT NULL
        GROUP BY e.issuer
    ''')
    if commit:
        conn.commit()


# ------------------------------------------------------------ scrape log
def log_scrape(conn, source: str, status: str, records_affected: int = 0,
               error: str | None = None, duration_secs: float | None = None,
               started_at: str | None = None, *, commit: bool = True):
    """Write an entry to the scrape_log table."""
    conn.execute(
        "INSERT INTO scrape_log (source, status, records_affected, error, duration_secs, started_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (source, status, records_affected, error, duration_secs, started_at)
    )
    if commit:
        conn.commit()
