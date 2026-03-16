"""
Migration 0003 — Document URLs and AI summaries
================================================
Adds columns for PDS/TMD/factsheet URLs, AI-generated summaries, and
scrape timestamps to the etfs table.
"""


def up(conn):
    existing = {row[1] for row in conn.execute("PRAGMA table_info(etfs)").fetchall()}

    columns = [
        ("tmd_url",              "TEXT"),
        ("factsheet_url",        "TEXT"),
        ("summary",              "TEXT"),
        ("summary_generated_at", "TIMESTAMP"),
        ("docs_last_scraped",    "TIMESTAMP"),
    ]

    for col_name, col_type in columns:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE etfs ADD COLUMN {col_name} {col_type}")
