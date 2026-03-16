#!/usr/bin/env python3
"""
Push the local SQLite database to the Railway-hosted dashboard.

Usage:
    python3 sync_to_railway.py

Environment variables (or edit defaults below):
    RAILWAY_URL    https://your-app.up.railway.app
    SYNC_TOKEN     shared secret matching the server's SYNC_TOKEN env var
    DB_PATH        path to local database (default: etf_data.db in project root)
"""

import gzip
import io
import os
import sys
import urllib.request
import urllib.error

# ── configure ────────────────────────────────────────────────────────────────
RAILWAY_URL = os.environ.get("RAILWAY_URL", "").rstrip("/")
SYNC_TOKEN  = os.environ.get("SYNC_TOKEN", "")
DB_PATH     = os.environ.get(
    "DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "etf_data.db"),
)
# ─────────────────────────────────────────────────────────────────────────────


def main():
    if not RAILWAY_URL:
        sys.exit("Error: set RAILWAY_URL env var (e.g. https://your-app.up.railway.app)")
    if not SYNC_TOKEN:
        sys.exit("Error: set SYNC_TOKEN env var")
    if not os.path.exists(DB_PATH):
        sys.exit(f"Error: database not found at {DB_PATH}")

    url = f"{RAILWAY_URL}/admin/sync-db"
    print(f"Compressing {DB_PATH} ...")

    buf = io.BytesIO()
    with open(DB_PATH, "rb") as f_in, gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(f_in.read())
    compressed = buf.getvalue()

    size_mb = len(compressed) / 1_048_576
    print(f"Uploading {size_mb:.1f} MB to {url} ...")

    req = urllib.request.Request(
        url,
        data=compressed,
        method="POST",
        headers={
            "Authorization": f"Bearer {SYNC_TOKEN}",
            "Content-Type": "application/octet-stream",
            "Content-Encoding": "gzip",
            "Content-Length": str(len(compressed)),
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read().decode()
            print(f"Success ({resp.status}): {body}")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        sys.exit(f"HTTP {e.code}: {body}")
    except urllib.error.URLError as e:
        sys.exit(f"Connection error: {e.reason}")


if __name__ == "__main__":
    main()
