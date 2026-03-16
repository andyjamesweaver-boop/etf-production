#!/bin/bash
set -e

DB_PATH="${DATABASE_PATH:-/data/etf_data.db}"

# Initialise schema if no database exists yet
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
    # Run any pending migrations on existing DB
    python3 -c "
import sys
sys.path.insert(0, '/app')
from setup_db import setup_database
setup_database('$DB_PATH')
print('Migrations complete.')
"
fi

echo "Starting ETF dashboard server on port ${PORT:-8080}..."
exec python3 /app/etf_server_with_dashboard.py
