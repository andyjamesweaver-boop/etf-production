#!/usr/bin/env python3
"""
Database migration runner for the Australian ETF platform.

Usage:
    python3 migrate.py           # apply all pending migrations
    python3 migrate.py --list    # show migration status

Migrations live in migrations/NNNN_*.py, each exporting an up(conn) function.
Applied migrations are recorded in the schema_migrations table so re-running
is always safe.
"""

import sqlite3
import importlib.util
import os
import sys
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etf_data.db')
MIGRATIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'migrations')


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def ensure_migrations_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            migration   TEXT    NOT NULL UNIQUE,
            applied_at  TEXT    NOT NULL
        )
    """)
    conn.commit()


def applied_migrations(conn):
    return {row[0] for row in conn.execute("SELECT migration FROM schema_migrations").fetchall()}


def available_migrations():
    files = sorted(
        f for f in os.listdir(MIGRATIONS_DIR)
        if f.endswith('.py') and not f.startswith('_')
    )
    return files


def load_migration(filename):
    path = os.path.join(MIGRATIONS_DIR, filename)
    spec = importlib.util.spec_from_file_location(filename[:-3], path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run(list_only=False):
    if not os.path.exists(DB_PATH):
        print(f"Error: database not found at {DB_PATH}")
        sys.exit(1)

    conn = get_connection()
    ensure_migrations_table(conn)

    done = applied_migrations(conn)
    pending = [f for f in available_migrations() if f not in done]

    if list_only:
        all_files = available_migrations()
        if not all_files:
            print("No migrations found.")
            return
        for f in all_files:
            status = 'applied' if f in done else 'pending'
            print(f"  {'✓' if f in done else '○'} {f}  [{status}]")
        return

    if not pending:
        print("Nothing to migrate — all migrations already applied.")
        conn.close()
        return

    for filename in pending:
        print(f"  Applying {filename}…", end=' ', flush=True)
        mod = load_migration(filename)
        try:
            mod.up(conn)
            conn.execute(
                "INSERT INTO schema_migrations (migration, applied_at) VALUES (?, ?)",
                (filename, datetime.now().isoformat())
            )
            conn.commit()
            print("done.")
        except Exception as e:
            conn.rollback()
            print(f"FAILED: {e}")
            conn.close()
            sys.exit(1)

    print(f"\n{len(pending)} migration(s) applied.")
    conn.close()


if __name__ == '__main__':
    list_only = '--list' in sys.argv
    run(list_only=list_only)
