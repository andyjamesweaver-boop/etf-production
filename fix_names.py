#!/usr/bin/env python3
"""
Fix all-caps ETF names to proper title case and normalise issuer names.

Rules:
  - Smart title-case: capitalise first letter of each word, lowercase the rest
  - Preserve known acronyms (S&P, MSCI, ETF, AUD, etc.) in uppercase
  - Keep minor words (of, and, the, in, ...) lowercase in mid-title
  - Apply brand-name fixes AFTER title-casing (iShares, VanEck, JPMorgan, etc.)
  - Fix issuer name spellings in the issuer column
"""

import re
import sqlite3

DB_PATH = 'etf_data.db'

# ── Acronyms / terms that always stay ALL-CAPS ────────────────────────────────
KEEP_UPPER = {
    # Fund types
    'ETF', 'ETFS', 'ETP',
    # Indices / exchanges
    'MSCI', 'FTSE', 'NASDAQ', 'NYSE', 'ASX', 'CBOE', 'LBMA', 'S&P',
    # S&P combos with slash
    'S&P/ASX',
    # J.P. Morgan abbreviation
    'J.P.',
    # U.S. abbreviation
    'U.S.',
    # Currencies
    'AUD', 'USD', 'EUR', 'GBP', 'JPY', 'CHF', 'NZD', 'CAD', 'SGD', 'HKD',
    'TWD', 'KRW', 'CNY', 'INR',
    # Country / region codes used as abbreviations in index names
    'US', 'UK', 'EU', 'EM', 'USA', 'APAC', 'EAFE',
    # Asset / strategy acronyms
    'ESG', 'SRI', 'HY', 'IG', 'REIT', 'REITS', 'BDC', 'GDP', 'VIX',
    'AI', 'IT',
    # Fund admin acronyms
    'NAV', 'MER', 'PCF', 'LIC', 'LIT', 'CHESS', 'PDS', 'TMD', 'FUM', 'FX',
    'ABN', 'ARSN', 'APRA', 'ASIC',
    # Brands kept uppercase
    'SPDR', 'PIMCO', 'GICS', 'OTC', 'PLC',
    # Bond ratings
    'AAA', 'AA', 'BBB', 'BB', 'CCC',
    # Other common abbreviations in fund names
    'IPO', 'ESG', 'RAFI', 'MOAT',
    # Commonly abbreviated but well-known
    'JPM',
    # Dotted abbreviations
    'J.P.', 'U.S.',
    # ASX company codes used in bond fund names
    'WBC', 'CBA', 'ANZ', 'NAB', 'MQG',
    # Financial benchmarks
    'BBSW',
    # Issuer abbreviations
    'IAM', 'AB',
}

# ── Minor words kept lowercase when in the middle of a title ─────────────────
LOWERCASE_MID = {
    'a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to',
    'for', 'of', 'with', 'by', 'from', 'as', 'ex', 'vs',
}

# ── Brand-name regex fixes applied AFTER title-casing ────────────────────────
# Each tuple: (compiled_regex, replacement_string)
BRAND_FIXES = [
    # iShares: "Ishares" → "iShares"
    (re.compile(r'\bIshares\b'), 'iShares'),
    # VanEck: "Vaneck" → "VanEck"
    (re.compile(r'\bVaneck\b', re.IGNORECASE), 'VanEck'),
    # JPMorgan: "Jpmorgan" → "JPMorgan"
    (re.compile(r'\bJpmorgan\b'), 'JPMorgan'),
    # abrdn: "Abrdn" → "abrdn"
    (re.compile(r'\bAbrdn\b'), 'abrdn'),
    # ETFS (ETF Securities brand prefix): "Etfs" → "ETFS"
    (re.compile(r'\bEtfs\b'), 'ETFS'),
    # S&P 500, S&P/ASX etc. – belt-and-braces
    (re.compile(r'\bS&p\b', re.IGNORECASE), 'S&P'),
    # Dotted abbreviations: \b doesn't work after dots, so use letter lookaround
    (re.compile(r'(?<![A-Za-z])J\.p\.(?![A-Za-z])', re.IGNORECASE), 'J.P.'),
    (re.compile(r'(?<![A-Za-z])U\.s\.(?![A-Za-z])', re.IGNORECASE), 'U.S.'),
    # BlackRock (appears in some iShares fund names)
    (re.compile(r'\bBlackrock\b', re.IGNORECASE), 'BlackRock'),
    # 21Shares brand name (starts with digit, capitalize() lowercases "Shares")
    (re.compile(r'\b21shares\b', re.IGNORECASE), '21Shares'),
]

# ── Issuer column fixes (exact string replacements) ───────────────────────────
ISSUER_FIXES = {
    'BetaShares':                          'Betashares',
    'StateStreet':                         'State Street',
    'DFA':                                 'Dimensional',
    'JPMorgan':                            'J.P. Morgan',
    'JPMAM / Perpetual':                   'J.P. Morgan / Perpetual',
    'VANGUARD INVESTMENTS AUSTRALIA LTD':  'Vanguard',
    'abrdn / MSC':                         'abrdn',
}


def fix_word(word: str, position: int) -> str:
    """Return a single space-separated token with corrected casing."""
    # Capture any leading/trailing brackets or punctuation
    m = re.match(r'^([(\[]*)(.*?)([)\].,;:]*)$', word, re.DOTALL)
    if not m:
        return word
    prefix, core, suffix = m.group(1), m.group(2), m.group(3)

    if not core:
        return word

    core_upper = core.upper()

    # Known uppercase acronym (including slash variants like S&P/ASX)
    if core_upper in KEEP_UPPER:
        return prefix + core_upper + suffix

    # Hyphenated compound: fix each part individually
    if '-' in core:
        parts = core.split('-')
        fixed_parts = []
        for j, p in enumerate(parts):
            pu = p.upper()
            if pu in KEEP_UPPER:
                fixed_parts.append(pu)
            elif j == 0:
                fixed_parts.append(p.capitalize())
            elif p.lower() in LOWERCASE_MID:
                fixed_parts.append(p.lower())
            else:
                fixed_parts.append(p.capitalize())
        return prefix + '-'.join(fixed_parts) + suffix

    # Slash-separated compound (e.g. S&P/ASX already handled above, but catch others)
    if '/' in core:
        parts = core.split('/')
        fixed_parts = []
        for p in parts:
            pu = p.upper()
            if pu in KEEP_UPPER:
                fixed_parts.append(pu)
            else:
                fixed_parts.append(p.capitalize())
        return prefix + '/'.join(fixed_parts) + suffix

    # Lowercase minor word in the middle of a title
    # (exclude single-char tokens — they're likely class labels like "A", "B")
    if position > 0 and len(core) > 1 and core.lower() in LOWERCASE_MID:
        return prefix + core.lower() + suffix

    # Default: title-capitalise
    return prefix + core.capitalize() + suffix


def smart_title(name: str) -> str:
    """Convert an all-caps ETF name to smart title case."""
    if not name:
        return name
    name = name.strip()
    # Insert a space before "(" when directly attached to a word (data quality fix)
    name = re.sub(r'([A-Za-z0-9])(\()', r'\1 \2', name)

    words = name.split()
    fixed = [fix_word(w, i) for i, w in enumerate(words)]
    result = ' '.join(fixed)

    # Apply brand-name post-processing
    for pattern, replacement in BRAND_FIXES:
        result = pattern.sub(replacement, result)

    return result


def fix_database():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # ── 1. Fix ETF names ──────────────────────────────────────────────────────
    rows = cursor.execute("SELECT code, name FROM etfs").fetchall()
    name_updates = []
    for row in rows:
        original = row['name']
        if not original:
            continue
        # Apply to all names — smart_title is idempotent on already-correct names
        # and also catches partially-title-cased names (e.g. from issuer scrapers)
        fixed = smart_title(original)
        if fixed != original:
            name_updates.append((fixed, row['code']))

    print(f"ETF names to fix: {len(name_updates)}")
    for fixed, code in name_updates[:10]:
        orig = next(r['name'] for r in rows if r['code'] == code)
        print(f"  {code}: {orig!r:60s} → {fixed!r}")
    if len(name_updates) > 10:
        print(f"  ... and {len(name_updates) - 10} more")

    cursor.executemany("UPDATE etfs SET name = ? WHERE code = ?", name_updates)

    # ── 2. Fix issuer names ───────────────────────────────────────────────────
    issuer_updates = []
    for old, new in ISSUER_FIXES.items():
        count = cursor.execute(
            "SELECT COUNT(*) FROM etfs WHERE issuer = ?", (old,)
        ).fetchone()[0]
        if count > 0:
            issuer_updates.append((new, old, count))

    print(f"\nIssuer fixes ({len(issuer_updates)} issuers):")
    for new, old, count in issuer_updates:
        print(f"  {old!r:45s} → {new!r}  ({count} ETFs)")
        cursor.execute("UPDATE etfs SET issuer = ? WHERE issuer = ?", (new, old))

    # ── 3. Fix upcoming_listings names + issuers ──────────────────────────────
    try:
        ul_rows = cursor.execute("SELECT asic_doc_no, name, issuer FROM upcoming_listings").fetchall()
        ul_name_updates = []
        for row in ul_rows:
            original = row['name']
            if not original:
                continue
            fixed = smart_title(original)
            if fixed != original:
                ul_name_updates.append((fixed, row['asic_doc_no']))

        print(f"\nUpcoming listings names to fix: {len(ul_name_updates)}")
        cursor.executemany(
            "UPDATE upcoming_listings SET name = ? WHERE asic_doc_no = ?",
            ul_name_updates
        )

        # Also fix issuers in upcoming_listings (they use raw ASIC names)
        # Normalise them to sentence case since they're full legal names like
        # "EQUITY TRUSTEES LIMITED" → "Equity Trustees Limited"
        ul_issuer_rows = cursor.execute(
            "SELECT DISTINCT issuer FROM upcoming_listings WHERE issuer IS NOT NULL"
        ).fetchall()
        ul_issuer_updates = []
        for row in ul_issuer_rows:
            orig = row['issuer']
            if not orig:
                continue
            upper_ratio = sum(1 for c in orig if c.isupper()) / max(len(orig), 1)
            if upper_ratio > 0.7:
                fixed = smart_title(orig)
                if fixed != orig:
                    ul_issuer_updates.append((fixed, orig))

        print(f"Upcoming listings issuers to fix: {len(ul_issuer_updates)}")
        for fixed, orig in ul_issuer_updates:
            print(f"  {orig!r:50s} → {fixed!r}")
        cursor.executemany(
            "UPDATE upcoming_listings SET issuer = ? WHERE issuer = ?",
            ul_issuer_updates
        )
    except Exception as e:
        print(f"  (upcoming_listings skipped: {e})")

    conn.commit()
    conn.close()
    print("\nDone.")


if __name__ == '__main__':
    fix_database()
