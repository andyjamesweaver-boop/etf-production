#!/usr/bin/env python3
"""
Database Setup & Migration for Australian ETF Data Platform
============================================================
Creates 7 tables: etfs, etf_holdings, etf_sectors, etf_dividends,
price_history, issuers, scrape_log.

Handles migration from the old single-table schema.
"""

import sqlite3
import os
import shutil
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etf_data.db')


def get_connection(db_path=None):
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def backup_database(db_path=None):
    db_path = db_path or DB_PATH
    if os.path.exists(db_path):
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = f"{db_path}.backup_{ts}"
        shutil.copy2(db_path, backup_path)
        print(f"Backed up existing database to {backup_path}")
        return backup_path
    return None


def detect_old_schema(conn):
    """Check if the old schema (simple etfs table with ~18 columns) is present."""
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='etfs'")
    if not cursor.fetchone():
        return False
    cursor = conn.execute("PRAGMA table_info(etfs)")
    columns = {row[1] for row in cursor.fetchall()}
    # Old schema markers: has 'category' but not 'exchange', not 'asset_class'
    return 'category' in columns and 'exchange' not in columns


def migrate_old_data(conn):
    """Migrate data from the old schema into the new schema."""
    cursor = conn.execute("PRAGMA table_info(etfs)")
    old_columns = {row[1] for row in cursor.fetchall()}

    rows = conn.execute("SELECT * FROM etfs").fetchall()
    if not rows:
        conn.execute("DROP TABLE IF EXISTS etfs")
        return []

    col_cursor = conn.execute("SELECT * FROM etfs LIMIT 0")
    col_names = [d[0] for d in col_cursor.description]

    migrated = []
    for row in rows:
        d = dict(zip(col_names, row))
        migrated.append({
            'code': d.get('code'),
            'name': d.get('name'),
            'issuer': d.get('issuer'),
            'asset_class': d.get('category', 'Unknown'),
            'exchange': 'ASX',
            'fund_size_aud_millions': d.get('fund_size_aud_millions'),
            'current_price': d.get('current_price'),
            'day_change_pct': d.get('day_change_percent'),
            'expense_ratio': d.get('expense_ratio'),
            'distribution_yield': d.get('distribution_yield'),
            'return_1y': d.get('return_1y') or d.get('volatility_1y'),
            'return_3y': d.get('return_3y'),
            'beta': d.get('beta'),
            'sharpe_ratio': d.get('sharpe_ratio'),
            'max_drawdown': d.get('max_drawdown'),
            'rank_by_fum': d.get('rank_by_fum'),
        })

    conn.execute("DROP TABLE IF EXISTS etfs")
    return migrated


def create_tables(conn):
    """Create all 7 tables."""

    # 1. etfs — expanded master table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS etfs (
            code                    TEXT PRIMARY KEY,
            name                    TEXT,
            issuer                  TEXT,
            exchange                TEXT DEFAULT 'ASX',
            asset_class             TEXT,
            sub_category            TEXT,

            -- Size & flows
            fund_size_aud_millions  REAL,
            fund_flow_1m            REAL,
            fund_flow_3m            REAL,
            fund_flow_1y            REAL,
            rank_by_fum             INTEGER,

            -- Pricing
            current_price           REAL,
            day_change_pct          REAL,
            bid_price               REAL,
            offer_price             REAL,
            bid_ask_spread_pct      REAL,
            nav_per_unit            REAL,
            premium_discount_pct    REAL,
            year_high               REAL,
            year_low                REAL,
            volume                  INTEGER,

            -- Returns
            return_1m               REAL,
            return_3m               REAL,
            return_6m               REAL,
            return_1y               REAL,
            return_3y               REAL,
            return_5y               REAL,
            return_since_inception  REAL,

            -- Fees
            management_fee          REAL,
            expense_ratio           REAL,
            buy_spread              REAL,
            sell_spread             REAL,

            -- Distributions
            distribution_yield      REAL,
            distribution_frequency  TEXT,
            last_distribution_date  TEXT,
            last_distribution_amount REAL,
            franking_pct            REAL,

            -- Risk
            beta                    REAL,
            sharpe_ratio            REAL,
            max_drawdown            REAL,
            volatility_1y           REAL,
            tracking_error          REAL,

            -- Fund info
            benchmark               TEXT,
            inception_date          TEXT,
            domicile                TEXT DEFAULT 'Australia',
            replication_method      TEXT,
            securities_lending      INTEGER DEFAULT 0,
            fx_hedged               INTEGER DEFAULT 0,

            -- Links
            issuer_url              TEXT,
            asx_url                 TEXT,
            pds_url                 TEXT,

            -- Meta
            data_source             TEXT,
            last_scraped            TIMESTAMP,
            last_updated            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 2. etf_holdings
    conn.execute('''
        CREATE TABLE IF NOT EXISTS etf_holdings (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            etf_code    TEXT NOT NULL,
            name        TEXT NOT NULL,
            ticker      TEXT,
            weight_pct  REAL,
            sector      TEXT,
            country     TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (etf_code) REFERENCES etfs(code) ON DELETE CASCADE,
            UNIQUE(etf_code, name)
        )
    ''')

    # 3. etf_sectors
    conn.execute('''
        CREATE TABLE IF NOT EXISTS etf_sectors (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            etf_code    TEXT NOT NULL,
            sector      TEXT NOT NULL,
            weight_pct  REAL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (etf_code) REFERENCES etfs(code) ON DELETE CASCADE,
            UNIQUE(etf_code, sector)
        )
    ''')

    # 4. etf_dividends
    conn.execute('''
        CREATE TABLE IF NOT EXISTS etf_dividends (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            etf_code        TEXT NOT NULL,
            ex_date         TEXT NOT NULL,
            pay_date        TEXT,
            amount          REAL,
            franking_pct    REAL,
            type            TEXT DEFAULT 'ordinary',
            last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (etf_code) REFERENCES etfs(code) ON DELETE CASCADE,
            UNIQUE(etf_code, ex_date)
        )
    ''')

    # 5. price_history
    conn.execute('''
        CREATE TABLE IF NOT EXISTS price_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            etf_code    TEXT NOT NULL,
            date        TEXT NOT NULL,
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL,
            volume      INTEGER,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (etf_code) REFERENCES etfs(code) ON DELETE CASCADE,
            UNIQUE(etf_code, date)
        )
    ''')

    # 6. issuers
    conn.execute('''
        CREATE TABLE IF NOT EXISTS issuers (
            name            TEXT PRIMARY KEY,
            website         TEXT,
            fund_list_url   TEXT,
            etf_count       INTEGER DEFAULT 0,
            total_fum       REAL DEFAULT 0,
            last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 7. scrape_log
    conn.execute('''
        CREATE TABLE IF NOT EXISTS scrape_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            source          TEXT NOT NULL,
            status          TEXT NOT NULL,
            records_affected INTEGER DEFAULT 0,
            error           TEXT,
            duration_secs   REAL,
            started_at      TIMESTAMP,
            finished_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    conn.commit()


def create_indexes(conn):
    indexes = [
        'CREATE INDEX IF NOT EXISTS idx_etfs_exchange ON etfs(exchange)',
        'CREATE INDEX IF NOT EXISTS idx_etfs_issuer ON etfs(issuer)',
        'CREATE INDEX IF NOT EXISTS idx_etfs_asset_class ON etfs(asset_class)',
        'CREATE INDEX IF NOT EXISTS idx_etfs_fum ON etfs(fund_size_aud_millions DESC)',
        'CREATE INDEX IF NOT EXISTS idx_etfs_rank ON etfs(rank_by_fum)',
        'CREATE INDEX IF NOT EXISTS idx_etfs_last_scraped ON etfs(last_scraped)',
        'CREATE INDEX IF NOT EXISTS idx_holdings_etf ON etf_holdings(etf_code)',
        'CREATE INDEX IF NOT EXISTS idx_sectors_etf ON etf_sectors(etf_code)',
        'CREATE INDEX IF NOT EXISTS idx_dividends_etf ON etf_dividends(etf_code)',
        'CREATE INDEX IF NOT EXISTS idx_dividends_date ON etf_dividends(ex_date DESC)',
        'CREATE INDEX IF NOT EXISTS idx_price_history_etf_date ON price_history(etf_code, date DESC)',
        'CREATE INDEX IF NOT EXISTS idx_scrape_log_source ON scrape_log(source)',
        'CREATE INDEX IF NOT EXISTS idx_scrape_log_finished ON scrape_log(finished_at DESC)',
    ]
    for sql in indexes:
        conn.execute(sql)
    conn.commit()


def insert_migrated_data(conn, migrated):
    """Re-insert migrated rows from old schema."""
    for row in migrated:
        cols = [k for k, v in row.items() if v is not None]
        vals = [row[k] for k in cols]
        placeholders = ','.join(['?'] * len(cols))
        col_str = ','.join(cols)
        conn.execute(
            f"INSERT OR IGNORE INTO etfs ({col_str}) VALUES ({placeholders})",
            vals
        )
    conn.commit()


def seed_issuers(conn):
    """Seed known Australian ETF issuers."""
    issuers = [
        ('Vanguard', 'https://www.vanguard.com.au', 'https://www.vanguard.com.au/personal/invest-with-us/etf'),
        ('BetaShares', 'https://www.betashares.com.au', 'https://www.betashares.com.au/fund-list/'),
        ('iShares', 'https://www.blackrock.com/au', 'https://www.blackrock.com/au/individual/products/investment-funds'),
        ('VanEck', 'https://www.vaneck.com.au', 'https://www.vaneck.com.au/etf/'),
        ('SPDR', 'https://www.ssga.com/au', 'https://www.ssga.com/au/en_gb/intermediary/etfs/fund-finder'),
        ('Global X', 'https://www.globalxetfs.com.au', 'https://www.globalxetfs.com.au/funds/'),
        ('Magellan', 'https://www.magellangroup.com.au', None),
        ('Dimensional', 'https://www.dimensional.com/au-en', None),
        ('JPMorgan', 'https://am.jpmorgan.com/au', None),
        ('Fidelity', 'https://www.fidelity.com.au', None),
        ('Invesco', 'https://www.invesco.com/au', None),
        ('Janus Henderson', 'https://www.janushenderson.com/en-au/', None),
        ('Perpetual', 'https://www.perpetual.com.au', None),
        ('Platinum', 'https://www.platinum.com.au', None),
        ('Hyperion', 'https://www.hyperion.com.au', None),
        ('Monochrome', 'https://www.monochrome.com.au', None),
    ]
    for name, website, fund_list_url in issuers:
        conn.execute(
            "INSERT OR IGNORE INTO issuers (name, website, fund_list_url) VALUES (?, ?, ?)",
            (name, website, fund_list_url)
        )
    conn.commit()


def setup_database(db_path=None):
    """Main entry point: backup, detect old schema, migrate, create new tables."""
    db_path = db_path or DB_PATH
    print(f"Setting up database at {db_path}")

    backup_database(db_path)

    conn = get_connection(db_path)

    migrated = []
    if detect_old_schema(conn):
        print("Detected old schema — migrating data...")
        migrated = migrate_old_data(conn)
        print(f"  Extracted {len(migrated)} ETF records for migration")

    create_tables(conn)
    create_indexes(conn)
    print("Created 7 tables + indexes")

    if migrated:
        insert_migrated_data(conn, migrated)
        print(f"  Re-inserted {len(migrated)} migrated records")

    seed_issuers(conn)
    print("Seeded issuer data")

    # Verify
    counts = {}
    for table in ['etfs', 'etf_holdings', 'etf_sectors', 'etf_dividends',
                   'price_history', 'issuers', 'scrape_log']:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        counts[table] = row[0]

    conn.close()

    print("\nDatabase setup complete:")
    for table, count in counts.items():
        print(f"  {table}: {count} rows")


if __name__ == '__main__':
    setup_database()
