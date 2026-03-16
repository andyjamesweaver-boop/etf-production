"""
Migration 0006 — sub_category column + asset_class consistency
==============================================================
1. Ensures the sub_category column exists on etfs.
2. Re-normalises inconsistent asset_class values using the canonical map
   (e.g. old stray values like "Unknown", raw Cboe strings, etc.).
3. Fixes the Infrastructure / Property conflation introduced by the Cboe
   "Infrastructure & Property" section label — Infrastructure ETFs are
   reclassified from Property → Infrastructure by name keywords.
4. Populates sub_category for all rows using classify_sub_category().
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def up(conn):
    # ── 1. Add sub_category column if missing ────────────────────────────────
    existing = {row[1] for row in conn.execute("PRAGMA table_info(etfs)").fetchall()}
    if 'sub_category' not in existing:
        conn.execute("ALTER TABLE etfs ADD COLUMN sub_category TEXT")

    # ── 2. Normalise stray / non-canonical asset_class values ────────────────
    from scrapers.config import ASSET_CLASS_MAP, CANONICAL_ASSET_CLASSES

    rows = conn.execute("SELECT code, asset_class FROM etfs WHERE asset_class IS NOT NULL").fetchall()
    for code, ac in rows:
        mapped = ASSET_CLASS_MAP.get(ac.strip().lower())
        if mapped and mapped != ac:
            conn.execute("UPDATE etfs SET asset_class = ? WHERE code = ?", (mapped, code))
        elif ac.strip() not in CANONICAL_ASSET_CLASSES:
            # Unknown value — try title-case lookup, otherwise leave for manual review
            title = ac.strip().title()
            if title in CANONICAL_ASSET_CLASSES:
                conn.execute("UPDATE etfs SET asset_class = ? WHERE code = ?", (title, code))

    # ── 3. Fix Infrastructure ETFs mis-classified as Property ────────────────
    # The Cboe "Infrastructure & Property" section maps everything to Property.
    # Reclassify the infrastructure-named ones.
    infra_keywords = [
        '%infrastructure%', '%toll road%', '%airport%',
        '%global infrastructure%', '%listed infrastructure%',
        '%core infrastructure%', '%essential infrastructure%',
    ]
    for kw in infra_keywords:
        conn.execute(
            "UPDATE etfs SET asset_class = 'Infrastructure' "
            "WHERE asset_class = 'Property' AND LOWER(name) LIKE ?",
            (kw,)
        )

    conn.commit()

    # ── 4. Backfill NULL asset_class by name keywords ────────────────────────
    backfills = [
        # Fixed Term Corporate Bond series
        ("UPDATE etfs SET asset_class='Fixed Income' WHERE asset_class IS NULL "
         "AND LOWER(name) LIKE '%fixed term%bond%'", []),
        # Cash
        ("UPDATE etfs SET asset_class='Cash' WHERE code='CASH' AND asset_class IS NULL", []),
        # Infrastructure accumulation indices
        ("UPDATE etfs SET asset_class='Infrastructure' WHERE asset_class IS NULL "
         "AND LOWER(name) LIKE '%infrastructure%'", []),
        # ASX accumulation indices — A-REIT
        ("UPDATE etfs SET asset_class='Property' WHERE asset_class IS NULL "
         "AND LOWER(name) LIKE '%a-reit%'", []),
        # ASX accumulation indices — broad AU equity
        ("UPDATE etfs SET asset_class='Australian Equities' WHERE asset_class IS NULL "
         "AND (LOWER(name) LIKE '%asx 200 accum%' OR LOWER(name) LIKE '%small ords%')", []),
        # Global fund catch-all
        ("UPDATE etfs SET asset_class='International Equities' WHERE asset_class IS NULL "
         "AND (LOWER(name) LIKE '%global%' OR LOWER(name) LIKE '%international%' "
         "OR LOWER(name) LIKE '%momentum%')", []),
    ]
    for sql, params in backfills:
        conn.execute(sql, params)
    conn.commit()

    # ── 5. Populate sub_category for all rows ────────────────────────────────
    from scrapers.config import classify_sub_category

    rows = conn.execute("SELECT code, name, asset_class FROM etfs").fetchall()
    updated = 0
    for code, name, ac in rows:
        sub = classify_sub_category(code, name, ac)
        conn.execute(
            "UPDATE etfs SET sub_category = ? WHERE code = ?",
            (sub, code)
        )
        if sub:
            updated += 1

    conn.commit()
    print(f"  sub_category populated for {updated} / {len(rows)} ETFs")
