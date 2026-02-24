"""
Migration 0001 — Fix and add ETF issuer URLs
=============================================
- IVV:  generic BlackRock AU page → specific iShares S&P 500 product page
- GOLD: generic Global X home    → specific GOLD fund page
- MGOC: old Magellan Group URL   → Magellan Investment Partners MGOC page
- DACE: (none)                   → Dimensional AU DACE fund page
- DAVA: (none)                   → Dimensional AU DAVA fund page
- DFGH: (none)                   → Dimensional AU DFGH fund page
- DGCE: (none)                   → Dimensional AU DGCE fund page
- DGSM: (none)                   → Dimensional AU DGSM fund page
- DGVA: (none)                   → Dimensional AU DGVA fund page
- issuers.DFA: (blank website)   → dimensional.com/au-en
"""

ETF_URLS = {
    'IVV':  'https://www.blackrock.com/au/products/275304/ishares-s-p-500-etf',
    'GOLD': 'https://www.globalxetfs.com.au/funds/gold/',
    'MGOC': 'https://magellaninvestmentpartners.com/funds/magellan-global-fund-open-class-asx-mgoc/',
    'DACE': 'https://www.dimensional.com/au-en/funds/dfa0003au/dimensional-australian-core-equity-trust-active-etf',
    'DAVA': 'https://www.dimensional.com/au-en/funds/dfa0101au/australian-value-trust',
    'DFGH': 'https://www.dimensional.com/au-en/funds/dfa0009au/dimensional-global-core-equity-trust-managed-fund-aud-hedged-class',
    'DGCE': 'https://www.dimensional.com/au-en/funds/dfa0004au/dimensional-global-core-equity-trust-managed-fund-unhedged-class',
    'DGSM': 'https://www.dimensional.com/au-en/funds/dfa0106au/global-small-company-trust',
    'DGVA': 'https://www.dimensional.com/au-en/funds/dfa0102au/global-value-trust',
}

ISSUER_URLS = {
    'DFA': 'https://www.dimensional.com/au-en',
}


def up(conn):
    for code, url in ETF_URLS.items():
        conn.execute('UPDATE etfs SET issuer_url = ? WHERE code = ?', (url, code))
    for name, website in ISSUER_URLS.items():
        conn.execute('UPDATE issuers SET website = ? WHERE name = ?', (website, name))
