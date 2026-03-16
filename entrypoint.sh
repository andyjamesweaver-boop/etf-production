#!/bin/bash
set -e

DB_PATH="${DATABASE_PATH:-/data/etf_data.db}"

if [ ! -f "$DB_PATH" ]; then
    echo "No database found at $DB_PATH — initialising empty schema..."
    python3 -c "
import sys
sys.path.insert(0, '/app')
from setup_db import setup_database
setup_database('$DB_PATH')
print('Schema initialised.')
"
else
    # DB exists — run only the lightweight migration scripts (no backup, no table recreation)
    echo "Database found at $DB_PATH — applying any pending migrations..."
    python3 -c "
import sys, os, sqlite3
sys.path.insert(0, '/app')

db_path = '$DB_PATH'
conn = sqlite3.connect(db_path)
conn.execute('PRAGMA journal_mode=WAL')
conn.execute('PRAGMA foreign_keys=ON')

# Ensure migration tracking table exists with correct schema
conn.execute('''CREATE TABLE IF NOT EXISTS schema_migrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)''')
conn.commit()

# Check if name column exists (old DBs may have a schema_migrations table with different columns)
cols = {row[1] for row in conn.execute('PRAGMA table_info(schema_migrations)').fetchall()}
if 'name' not in cols:
    print('  schema_migrations missing name column — rebuilding...')
    conn.execute('ALTER TABLE schema_migrations RENAME TO _schema_migrations_old')
    conn.execute('''CREATE TABLE schema_migrations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    # Mark all known migration files as already applied so they do not re-run
    mig_dir_check = '/app/migrations'
    if os.path.isdir(mig_dir_check):
        for f in sorted(os.listdir(mig_dir_check)):
            if f.endswith('.py') and f[0].isdigit():
                conn.execute('INSERT OR IGNORE INTO schema_migrations (name) VALUES (?)', (f[:-3],))
    conn.commit()
    print('  schema_migrations rebuilt.')

# Run any migration files not yet applied
mig_dir = '/app/migrations'
if os.path.isdir(mig_dir):
    files = sorted(f for f in os.listdir(mig_dir) if f.endswith('.py') and f[0].isdigit())
    for fname in files:
        name = fname[:-3]
        already = conn.execute('SELECT 1 FROM schema_migrations WHERE name=?', (name,)).fetchone()
        if not already:
            print(f'  Applying migration: {name}')
            import importlib.util
            spec = importlib.util.spec_from_file_location(name, os.path.join(mig_dir, fname))
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            mod.up(conn)
            conn.execute('INSERT INTO schema_migrations (name) VALUES (?)', (name,))
            conn.commit()
            print(f'  Done: {name}')

conn.close()
print('Migrations complete.')
"
fi

echo "Starting ETF dashboard server on port ${PORT:-8080}..."
exec python3 /app/etf_server_with_dashboard.py
