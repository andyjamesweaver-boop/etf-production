"""
Configuration for all scrapers — URLs, rate limits, issuer mappings.
"""

import os

# ---------- Paths ----------
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.environ.get('DATABASE_PATH', os.path.join(PROJECT_DIR, 'etf_data.db'))
DATA_DIR = os.path.join(PROJECT_DIR, 'data')

# ---------- Rate limiting (seconds between requests per domain) ----------
RATE_LIMITS = {
    'asx.com.au': 3.0,
    'cboe.com.au': 2.0,
    'betashares.com.au': 2.0,
    'vanguard.com.au': 2.0,
    'blackrock.com': 2.0,
    'vaneck.com.au': 2.0,
    'ssga.com': 2.0,
    'globalxetfs.com.au': 2.0,
    'marketindex.com.au': 2.0,
    '_default': 2.0,
}

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # seconds — retries at 2s, 4s, 8s

USER_AGENT = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/120.0.0.0 Safari/537.36'
)

REQUEST_TIMEOUT = 30  # seconds

# ---------- ASX ----------
ASX_INVESTMENT_PRODUCTS_INDEX_URL = (
    'https://www2.asx.com.au/content/asx/home/issuers/investment-products/'
    'asx-investment-products-monthly-report.html'
)
ASX_INVESTMENT_PRODUCTS_BASE_URL = 'https://www2.asx.com.au'
ASX_ETF_API_BASE = 'https://asx.api.markitdigital.com/asx-research/1.0/companies/{code}/header'
ASX_ETF_PAGE = 'https://www2.asx.com.au/markets/etp/{code}'

# ---------- Cboe Australia ----------
CBOE_FUNDS_URL = 'https://www.cboe.com.au/products/asx-listed-etps'
CBOE_API_URL = 'https://www.cboe.com.au/mdx/api/etps'
# Monthly funds report — URL template (use .format(year=..., month=...))
CBOE_MONTHLY_REPORT_CDN_BASE = 'https://cdn.cboe.com/resources/au/reports/funds_monthly/Monthly-Funds-Report-{year}-{month:02d}.xlsx'

# ---------- MarketIndex fallback ----------
MARKETINDEX_ETFS_URL = 'https://www.marketindex.com.au/asx-etfs'

# ---------- Issuer scraping ----------
ISSUER_URLS = {
    'BetaShares': {
        # /fund-list/ is gone; the current fund listing is at /fund/
        'fund_list': 'https://www.betashares.com.au/fund/',
        'fund_page': 'https://www.betashares.com.au/fund/{slug}/',
    },
    'VanEck': {
        'fund_list': 'https://www.vaneck.com.au/etf/',
        'fund_page': 'https://www.vaneck.com.au/etf/{slug}/',
    },
    'Vanguard': {
        # Angular SPA — direct API calls not accessible; individual product pages work
        'fund_list': 'https://www.vanguard.com.au/personal/invest-with-us/etf',
        'fund_page': 'https://www.vanguard.com.au/personal/products/en/detail/{code}/overview',
    },
    'iShares': {
        'fund_list': 'https://www.blackrock.com/au/individual/products/investment-funds#categoryId=702702&tab=overview',
        'fund_page': 'https://www.blackrock.com/au/individual/products/{product_id}/',
        'api': 'https://www.blackrock.com/au/individual/products/fund-list',
    },
    'SPDR': {
        # Fund finder is JS-rendered; individual fund pages work
        'fund_list': 'https://www.ssga.com/au/en_gb/intermediary/etfs/fund-finder',
        'fund_page': 'https://www.ssga.com/au/en_gb/intermediary/etfs/funds/{slug}',
    },
    'Global X': {
        'fund_list': 'https://www.globalxetfs.com.au/funds/',
        'fund_page': 'https://www.globalxetfs.com.au/funds/{slug}/',
    },
}

# Known iShares AU ETFs: {ASX code: BlackRock numeric product ID}
# Product page URL: https://www.blackrock.com/au/products/{product_id}/
ISHARES_AU_PRODUCTS = {
    'AESG': 328186,
    'AGGG': 346452,
    'ALTB': 337679,
    'AUMF': 284664,
    'BILL': 287045,
    'EMXC': 337684,
    'GLDN': 332696,
    'GLIN': 331650,
    'GLPR': 331647,
    'IAA':  273416,
    'IACT': 343627,
    'IAF':  251977,
    'IBAL': 327628,
    'IBIT': 346455,
    'ICME': 347181,
    'ICOR': 313534,
    'IEM':  273417,
    'IESG': 318831,
    'IEU':  273427,
    'IGB':  251979,
    'IGRO': 327642,
    'IHCB': 275246,
    'IHD':  251922,
    'IHEB': 275254,
    'IHHY': 275248,
    'IHOO': 271031,
    'IHQL': 335483,
    'IHVV': 271027,
    'IHWL': 283119,
    'IJH':  273425,
    'IJP':  273434,
    'IJR':  273426,
    'IKO':  273436,
    'ILB':  251978,
    'ILC':  251921,
    'IMTM': 335486,
    'IOO':  273428,
    'IOZ':  251852,
    'IQLT': 335480,
    'ISEC': 287042,
    'ISO':  251923,
    'ITEK': 328189,
    'IUSG': 331211,
    'IVE':  273432,
    'IVHG': 335523,
    'IVLU': 335478,
    'IVV':  275304,
    'IWLD': 283117,
    'IXI':  273429,
    'IXJ':  273430,
    'IYLD': 313537,
    'IZZ':  273424,
    'MVOL': 284666,
    'ULTB': 339056,
    'WDMF': 284665,
    'WVOL': 284667,
}

# Known SPDR AU ETFs: {ASX code: URL slug}
SPDR_AU_FUNDS = {
    'STW': 'spdr-sp-asx-200-fund-stw',
    'SFY': 'spdr-sp-asx-50-fund-sfy',
    'SLF': 'spdr-sp-asx-200-listed-property-fund-slf',
    'DJRE': 'spdr-dow-jones-global-real-estate-fund-djre',
    'WDIV': 'spdr-sp-global-dividend-fund-wdiv',
    'GOVT': 'spdr-s-p-asx-australian-government-bond-etf-govt',
    'BOND': 'spdr-s-p-asx-australian-bond-etf-bond',
    'SPUK': 'spdr-morningstar-multi-sector-bond-etf-spuk',
    'ZOZI': 'spdr-sp-asx-200-resources-fund-ozr',
}

# Known Vanguard AU ETFs (site is Angular SPA — use as fallback seed)
VANGUARD_AU_FUNDS = {
    'VAS': 'Vanguard Australian Shares Index ETF',
    'VGS': 'Vanguard MSCI Index International Shares ETF',
    'VAP': 'Vanguard Australian Property Securities Index ETF',
    'VHY': 'Vanguard Australian Shares High Yield ETF',
    'VGAD': 'Vanguard MSCI Index International Shares (Hedged) ETF',
    'VGB': 'Vanguard Australian Government Bond Index ETF',
    'VIF': 'Vanguard International Fixed Interest Index ETF (Hedged)',
    'VSO': 'Vanguard MSCI Australian Small Companies Index ETF',
    'VGE': 'Vanguard FTSE Emerging Markets Shares ETF',
    'VESG': 'Vanguard Ethically Conscious International Shares Index ETF',
    'VBLD': 'Vanguard Diversified Growth Index ETF',
    'VDCO': 'Vanguard Diversified Conservative Index ETF',
    'VDBA': 'Vanguard Diversified Balanced Index ETF',
    'VDGR': 'Vanguard Diversified Growth Index ETF',
    'VDHG': 'Vanguard Diversified High Growth Index ETF',
    'VEQ': 'Vanguard Australian Shares Index ETF',
    'VLUE': 'Vanguard Global Value Equity Active ETF',
    'VMIN': 'Vanguard Global Minimum Volatility Active ETF',
    'VISM': 'Vanguard International Small Companies Index ETF',
    'VACF': 'Vanguard Australian Corporate Fixed Interest Index ETF',
    'VVLU': 'Vanguard Global Value Equity Active ETF',
    'VEU': 'Vanguard All-World ex-US Shares Index ETF',
}

# Vanguard AU ETF portId values — used to build correct issuer_url links.
# URL format: https://www.vanguard.com.au/personal/invest-with-us/etf?portId={portId}
VANGUARD_AU_PORT_IDS = {
    'VBND': '8200',   # Vanguard Global Aggregate Bond Index (Hedged) ETF
    'VMIN': '8201',   # Vanguard Global Minimum Volatility Active ETF
    'VLUE': '8202',   # Vanguard Global Value Equity Active ETF (same fund as VVLU)
    'VVLU': '8202',   # Vanguard Global Value Equity Active ETF
    'VACF': '8203',   # Vanguard Australian Corporate Fixed Interest Index ETF
    'VGE':  '8204',   # Vanguard FTSE Emerging Markets Shares ETF
    'VAS':  '8205',   # Vanguard Australian Shares Index ETF
    'VAP':  '8206',   # Vanguard Australian Property Securities Index ETF
    'VAF':  '8207',   # Vanguard Australian Fixed Interest Index ETF
    'VGB':  '8208',   # Vanguard Australian Government Bond Index ETF
    'VLC':  '8209',   # Vanguard MSCI Australian Large Companies Index ETF
    'VHY':  '8210',   # Vanguard Australian Shares High Yield ETF
    'VSO':  '8211',   # Vanguard MSCI Australian Small Companies Index ETF
    'VGS':  '8212',   # Vanguard MSCI Index International Shares ETF
    'VGAD': '8213',   # Vanguard MSCI Index International Shares (Hedged) ETF
    'VEQ':  '8214',   # Vanguard FTSE Europe Shares ETF
    'VAE':  '8215',   # Vanguard FTSE Asia ex Japan Shares Index ETF
    'VIF':  '8216',   # Vanguard International Fixed Interest Index (Hedged) ETF
    'VCF':  '8217',   # Vanguard International Credit Securities Index (Hedged) ETF
    'VDBA': '8218',   # Vanguard Diversified Balanced Index ETF
    'VDCO': '8219',   # Vanguard Diversified Conservative Index ETF
    'VDGR': '8220',   # Vanguard Diversified Growth Index ETF
    'VDHG': '8221',   # Vanguard Diversified High Growth Index ETF
    'VEFI': '8224',   # Vanguard Ethically Conscious Global Aggregate Bond Index (Hedged) ETF
    'VESG': '8225',   # Vanguard Ethically Conscious International Shares Index ETF
    'VETH': '8226',   # Vanguard Ethically Conscious Australian Shares ETF
    'VISM': '8227',   # Vanguard MSCI International Small Companies Index ETF
    'VBLD': '8228',   # Vanguard Global Infrastructure Index ETF
    'VEU':  '0991',   # Vanguard All-World ex-US Shares Index ETF
    'VTS':  '0970',   # Vanguard US Total Market Shares Index ETF
    'VDAL': 'F100',   # Vanguard Diversified All Growth Index ETF
    'VDIF': 'F101',   # Vanguard Diversified Income ETF
}

# Map common issuer name variants to canonical names
ISSUER_ALIASES = {
    'betashares': 'BetaShares',
    'beta shares': 'BetaShares',
    'vanguard': 'Vanguard',
    'vanguard investments australia': 'Vanguard',
    'ishares': 'iShares',
    'blackrock': 'iShares',
    'vaneck': 'VanEck',
    'van eck': 'VanEck',
    'spdr': 'SPDR',
    'state street': 'SPDR',
    'state street global advisors': 'SPDR',
    'global x': 'Global X',
    'magellan': 'Magellan',
    'dimensional': 'Dimensional',
    'jpmorgan': 'JPMorgan',
    'fidelity': 'Fidelity',
    'invesco': 'Invesco',
    'janus henderson': 'Janus Henderson',
    'perpetual': 'Perpetual',
    'platinum': 'Platinum',
    'hyperion': 'Hyperion',
    'monochrome': 'Monochrome',
    # Cboe-listed issuers
    'coolabah': 'Coolabah',
    'talaria': 'Talaria',
    'lazard': 'Lazard',
    'kapstream': 'Kapstream',
    'elstree': 'Elstree',
    'avantis': 'Avantis',
    'india avenue': 'India Avenue',
    'alliancebernstein': 'AllianceBernstein',
    'ab investments': 'AllianceBernstein',
    'ab asset management': 'AllianceBernstein',
    'paradice': 'Paradice',
    't8': 'T8',
    'iam': 'IAM',
    'australian ethical': 'Australian Ethical',
    'schroders': 'Schroders',
    'pimco': 'PIMCO',
}

# Base fund-page URLs for CXA-exclusive issuers (used by cboe_scraper)
CXA_ISSUER_URLS = {
    'AllianceBernstein': 'https://www.alliancebernstein.com/au/advisor/en/funds.html',
    'Australian Ethical': 'https://www.australianethical.com.au/wholesale/etfs/',
    'Avantis':           'https://au.avantis.com/',
    'Coolabah':          'https://coolabah.com.au/our-funds/',
    'Elstree':           'https://www.elstreemanagement.com.au/',
    'Global X':          'https://www.globalxetfs.com.au/',
    'IAM':               'https://www.iamadvisory.com.au/',
    'India Avenue':      'https://indiaavenue.com.au/funds/',
    'iShares':           'https://www.blackrock.com/au/individual/products/investment-funds',
    'Janus Henderson':   'https://www.janushenderson.com/en-au/advisor/funds/',
    'JPMorgan':          'https://am.jpmorgan.com/au/en/asset-management/adv/products/',
    'Kapstream':         'https://kapstream.com.au/',
    'Lazard':            'https://www.lazardassetmanagement.com/au/en_au/individual/',
    'Magellan':          'https://www.magellangroup.com.au/funds/',
    'Monochrome':        'https://monochrome.com.au/',
    'Paradice':          'https://paradice.com/',
    'PIMCO':             'https://www.pimco.com.au/en-au/our-solutions/all-funds/',
    'Schroders':         'https://www.schroders.com/en/au/asset-management/products/',
    'T8':                'https://www.t8asset.com.au/',
    'Talaria':           'https://www.talariacapital.com.au/',
}

# Map raw source category strings to canonical asset_class values.
# Keys are lowercased; values are the exact canonical strings used throughout the platform.
ASSET_CLASS_MAP = {
    # ASX report categories
    'australian equities':          'Australian Equities',
    'australian shares':            'Australian Equities',
    'international equities':       'International Equities',
    'international shares':         'International Equities',
    'global equities':              'International Equities',
    'global shares':                'International Equities',
    'fixed income':                 'Fixed Income',
    'bonds':                        'Fixed Income',
    'australian fixed interest':    'Fixed Income',
    'international fixed interest': 'Fixed Income',
    'fixed interest':               'Fixed Income',
    'cash':                         'Cash',
    'cash and fixed income':        'Fixed Income',
    'commodities':                  'Commodities',
    'commodity':                    'Commodities',
    'precious metals':              'Commodities',
    'property':                     'Property',
    'real estate':                  'Property',
    'listed property':              'Property',
    'infrastructure':               'Infrastructure',
    'listed infrastructure':        'Infrastructure',
    'diversified':                  'Diversified',
    'multi-asset':                  'Diversified',
    'multi asset':                  'Diversified',
    'balanced':                     'Diversified',
    'currency':                     'Currency',
    'currencies':                   'Currency',
    'alternatives':                 'Alternatives',
    'alternative':                  'Alternatives',
    'leveraged & inverse':          'Alternatives',
    'leveraged and inverse':        'Alternatives',
    'crypto':                       'Digital Assets',
    'cryptocurrency':               'Digital Assets',
    'digital assets':               'Digital Assets',
    'crypto assets':                'Digital Assets',
    'thematic':                     'Thematic',
    'sector':                       'Thematic',
    # Cboe monthly report section names
    'equity - domestic':            'Australian Equities',
    'equity - international':       'International Equities',
    # Note: Cboe lumps these together; the migration re-separates by name keywords
    'infrastructure & property':    'Property',
    'fixed income - domestic':      'Fixed Income',
    'fixed income - international': 'Fixed Income',
    'cash products':                'Cash',
    'mixed asset':                  'Diversified',
    'crypto-assets':                'Digital Assets',
}

# Canonical asset_class values — the only strings that should appear in the DB
CANONICAL_ASSET_CLASSES = {
    'Australian Equities',
    'International Equities',
    'Fixed Income',
    'Commodities',
    'Property',
    'Infrastructure',
    'Diversified',
    'Cash',
    'Digital Assets',
    'Currency',
    'Alternatives',
    'Thematic',
}


def normalise_issuer(raw):
    """Map a raw issuer string to its canonical name."""
    if not raw:
        return None
    key = raw.strip().lower()
    return ISSUER_ALIASES.get(key, raw.strip())


def normalise_asset_class(raw):
    """Map a raw category / asset class string to a canonical asset_class."""
    if not raw:
        return None
    key = raw.strip().lower()
    mapped = ASSET_CLASS_MAP.get(key)
    if mapped:
        return mapped
    # Already a canonical value?
    stripped = raw.strip()
    if stripped in CANONICAL_ASSET_CLASSES:
        return stripped
    # Fall back to title-cased raw value rather than raw garbage
    return stripped


def classify_sub_category(code, name, asset_class):
    """
    Derive a sub_category for an ETF given its code, name, and (canonical) asset_class.

    Rules are keyword-based on the lower-cased ETF name + code combined.
    More specific patterns must appear before general fallbacks within each block.
    Returns None if asset_class is unrecognised.
    """
    if not asset_class:
        return None

    text = ((name or '') + ' ' + (code or '')).lower()

    def has(*keywords):
        return any(k in text for k in keywords)

    # ── Australian Equities ──────────────────────────────────────────────────
    if asset_class == 'Australian Equities':
        if has('small cap', 'smallcap', 'small-cap', 'mid cap', 'micro cap',
               'emerging companies', 'ex-20', 'ex 20', 'ex20', 'small companies',
               'smaller companies', 'small & mid'):
            return 'Small / Mid Cap'
        if has('esg', 'ethical', 'responsible investing', 'sustainable', 'sustainability',
               'climate', 'impact', 'carbon', 'net zero', 'environmental'):
            return 'ESG & Responsible'
        if has('dividend', 'high yield', 'income fund', 'dividend income', 'dividend yield'):
            return 'Dividend & Income'
        if has('financ', ' bank', 'banking'):
            return 'Sector — Financials'
        if has('resource', 'mining', 'materials', 'energy sector', 'oil', 'gas'):
            return 'Sector — Resources'
        if has('property', 'reit', 'real estate'):
            return 'Sector — Property'
        if has('health', 'biotech', 'pharma', 'medical', 'life science'):
            return 'Sector — Healthcare'
        if has('tech', 'digital', 'innovation', 'software', 'internet', 'semiconductor'):
            return 'Sector — Technology'
        return 'Broad Market'

    # ── International Equities ───────────────────────────────────────────────
    if asset_class == 'International Equities':
        if has('s&p 500', 's&p500', 'nasdaq', 'dow jones', 'russell 2000', 'russell 1000',
               'united states equit', 'us equit', 'us share', 'us market', 'american',
               'nyse', 'new york'):
            return 'US Market'
        if has('emerging market', 'china', 'india', 'bric', 'em equit', 'emerging share',
               'korea', 'taiwan', 'brazil', 'vietnam', 'frontier'):
            return 'Emerging Markets'
        if has('asia pacific', 'asia ex', 'japan', 'asian equit', 'apac', 'hong kong',
               'singapore', 'southeast asia'):
            return 'Asia Pacific'
        if has('europ', 'ftse 100', 'dax', 'cac 40', 'stoxx', 'euro stoxx',
               'uk equit', 'german', 'french'):
            return 'Europe'
        if has('quality', 'high quality') or code.lower() in ('qual', 'qlty', 'qmix', 'qpon', 'vqual'):
            return 'Factor — Quality'
        if has('value fund', 'value etf', 'fundamental index', 'deep value',
               'price-to-book', 'price to book'):
            return 'Factor — Value'
        if has('momentum', 'trend following', 'relative strength'):
            return 'Factor — Momentum'
        if has('low volatility', 'low vol', 'minimum variance', 'min variance',
               'defensive equity', 'low risk equit'):
            return 'Factor — Low Volatility'
        if has('dividend', 'income', 'high yield equit', 'equity income'):
            return 'Dividend & Income'
        if has('esg', 'ethical', 'responsible', 'sustainable', 'sustainability',
               'climate', 'impact', 'carbon', 'net zero', 'paris aligned', 'sri'):
            return 'ESG & Responsible'
        # Specific thematic sub-sectors — checked before broad sector keywords
        if has('uranium', 'nuclear') or code.lower() in ('atom', 'urnm', 'uran'):
            return 'Uranium & Nuclear'
        if (has('clean energy', 'renewable energy', 'solar energy', 'wind energy',
                'green energy', 'clean power') and not has('uranium', 'nuclear')):
            return 'Clean Energy'
        if (has('copper', 'lithium', 'battery tech', 'battery material',
                'energy transition', 'green metal', 'critical mineral') or
                code.lower() in ('wire', 'xmet', 'gmtl', 'acdc')):
            return 'Energy Transition Metals'
        if has('cyber', 'cybersecurity', 'data security', 'network security'):
            return 'Sector — Cybersecurity'
        if has('artificial intelligence', ' ai fund', 'machine learning', 'robotics',
               'automation', 'big data'):
            return 'Sector — Technology'
        if has('tech', 'information technology', 'semiconductor', 'software', 'internet',
               'fang', 'innovation', 'cloud', 'digital economy'):
            return 'Sector — Technology'
        if has('health', 'biotech', 'pharma', 'medical', 'life science', 'genomic',
               'biolog'):
            return 'Sector — Healthcare'
        if has('financ', 'bank', 'banking'):
            return 'Sector — Financials'
        if has('resource', 'mining', 'metal', 'material', 'gold miner', 'silver miner'):
            return 'Sector — Resources'
        return 'Developed Markets'

    # ── Fixed Income ─────────────────────────────────────────────────────────
    if asset_class == 'Fixed Income':
        if has('high yield', 'sub-investment grade', 'non-investment grade', 'junk bond'):
            return 'High Yield'
        if has('inflation', ' cpi ', 'tips', 'index-linked', 'real return', 'inflation-linked'):
            return 'Inflation Linked'
        if has('emerging market', 'em debt', 'em bond', 'local currency bond',
               'hard currency bond'):
            return 'Emerging Market Debt'
        if has('floating rate', 'bbsw', 'variable rate', 'bank loan', 'senior loan'):
            return 'Floating Rate'
        if (has('government', 'treasury', 'sovereign') and
                has('australia', 'australian', 'domestic', 'agb')):
            return 'Australian Government'
        if (has('corporate', 'credit') and
                has('australia', 'australian', 'domestic')):
            return 'Australian Corporate'
        if has('government', 'treasury', 'sovereign'):
            return 'Global Government'
        if has('corporate', 'credit', 'investment grade') and not has('australia'):
            return 'Global Corporate'
        if (has('australia', 'australian', 'domestic') and
                not has('global', 'international', 'world', 'foreign')):
            return 'Australian Diversified'
        return 'Global Diversified'

    # ── Commodities ──────────────────────────────────────────────────────────
    if asset_class == 'Commodities':
        if has('gold') or code.lower() in ('gold', 'nugg', 'qau', 'zgol', 'pmgold', 'mnrs', 'gdx'):
            return 'Gold'
        if has('silver') or code.lower() in ('etpmag',):
            return 'Silver'
        if has('platinum', 'palladium') or code.lower() in ('etpmpd', 'etpmpt'):
            return 'Precious Metals — Other'
        if (has('copper', 'lithium', 'nickel', 'cobalt', 'battery metal', 'green metal',
               'energy transition metal', 'strategic metal') or
                code.lower() in ('wire', 'xmet', 'gmtl')):
            return 'Green / Industrial Metals'
        if has('oil', 'crude', 'petroleum', 'natural gas', 'brent', 'wti'):
            return 'Oil & Energy'
        if has('agriculture', 'wheat', 'corn', 'soy', 'grain', 'food', 'livestock',
               'soft commodity'):
            return 'Agriculture'
        return 'Diversified'

    # ── Property ─────────────────────────────────────────────────────────────
    if asset_class == 'Property':
        if has('australia', 'australian', 'domestic', 'a-reit', 'areit', ' asx '):
            return 'Australian REITs'
        return 'Global REITs'

    # ── Infrastructure ───────────────────────────────────────────────────────
    if asset_class == 'Infrastructure':
        if has('australia', 'australian', 'domestic'):
            return 'Australian Infrastructure'
        return 'Global Infrastructure'

    # ── Diversified ──────────────────────────────────────────────────────────
    if asset_class == 'Diversified':
        if has('conservative', 'defensive', 'capital stable', 'low risk', 'capital secure'):
            return 'Conservative'
        if has('high growth', 'aggressive', 'high risk', 'very high growth'):
            return 'High Growth'
        if has('growth') and not has('high growth'):
            return 'Growth'
        if has('balance', 'moderate'):
            return 'Balanced'
        return 'Balanced'

    # ── Cash ─────────────────────────────────────────────────────────────────
    if asset_class == 'Cash':
        return 'Cash & Money Market'

    # ── Digital Assets ───────────────────────────────────────────────────────
    if asset_class == 'Digital Assets':
        if has('bitcoin', ' btc'):
            return 'Bitcoin'
        if has('ethereum', 'ether', ' eth '):
            return 'Ethereum'
        return 'Diversified Crypto'

    # ── Currency ─────────────────────────────────────────────────────────────
    if asset_class == 'Currency':
        if has('australian', 'aud'):
            return 'AUD Strategies'
        return 'FX Strategies'

    # ── Alternatives ─────────────────────────────────────────────────────────
    if asset_class == 'Alternatives':
        if has('leveraged', 'geared', '2x', '3x', 'bull fund', 'ultra'):
            return 'Leveraged'
        if has(' short ', 'bear ', 'inverse', ' put ', 'short sell'):
            return 'Inverse'
        return 'Absolute Return'

    # ── Thematic ─────────────────────────────────────────────────────────────
    if asset_class == 'Thematic':
        if has('artificial intelligence', ' ai etf', ' ai fund', 'machine learning',
               'generative', 'large language'):
            return 'Artificial Intelligence'
        if has('cyber', 'cybersecurity', 'data security', 'network security'):
            return 'Cybersecurity'
        if has('uranium', 'nuclear') or code.lower() in ('atom', 'urnm', 'uran'):
            return 'Uranium & Nuclear'
        if (has('clean energy', 'renewable energy', 'solar energy', 'wind energy',
                'green energy', 'clean power', 'climate transition') and
                not has('uranium', 'nuclear')):
            return 'Clean Energy'
        if (has('copper miner', 'energy transition', 'green metal', 'battery material',
                'critical mineral', 'strategic mineral', 'electrification') or
                code.lower() in ('wire', 'xmet', 'gmtl')):
            return 'Energy Transition Metals'
        if has('health', 'biotech', 'pharma', 'medical', 'life science', 'genomic',
               'biolog', 'oncolog'):
            return 'Healthcare & Biotech'
        if has('tech', 'digital', 'innovation', 'internet', 'software', 'semiconductor',
               'cloud', 'data centre', 'robotics', 'automation', 'fintech'):
            return 'Technology & Innovation'
        if has('esg', 'ethical', 'responsible', 'sustainable', 'sustainability',
               'impact', 'social', 'governance', 'climate'):
            return 'ESG & Sustainability'
        if has('infrastructure', 'toll road', 'airport', 'utility', 'utilities'):
            return 'Global Infrastructure'
        if has('global real estate', 'global reit', 'global property'):
            return 'Global REITs'
        return 'Technology & Innovation'

    return None
