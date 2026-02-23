"""
Configuration for all scrapers — URLs, rate limits, issuer mappings.
"""

import os

# ---------- Paths ----------
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_DIR, 'etf_data.db')
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

# Map ASX report category names to our asset_class values
ASSET_CLASS_MAP = {
    'australian equities': 'Australian Equities',
    'australian shares': 'Australian Equities',
    'international equities': 'International Equities',
    'international shares': 'International Equities',
    'global equities': 'International Equities',
    'fixed income': 'Fixed Income',
    'bonds': 'Fixed Income',
    'australian fixed interest': 'Fixed Income',
    'international fixed interest': 'Fixed Income',
    'cash': 'Cash',
    'commodities': 'Commodities',
    'commodity': 'Commodities',
    'property': 'Property',
    'real estate': 'Property',
    'infrastructure': 'Infrastructure',
    'diversified': 'Diversified',
    'multi-asset': 'Diversified',
    'currency': 'Currency',
    'alternatives': 'Alternatives',
    'crypto': 'Digital Assets',
    'digital assets': 'Digital Assets',
    'thematic': 'Thematic',
    # Cboe monthly report section names
    'equity - domestic': 'Australian Equities',
    'equity - international': 'International Equities',
    'infrastructure & property': 'Property',
    'fixed income - domestic': 'Fixed Income',
    'fixed income - international': 'Fixed Income',
    'cash products': 'Cash',
    'mixed asset': 'Diversified',
    'currencies': 'Currency',
    'crypto-assets': 'Digital Assets',
    'commodities': 'Commodities',
}


def normalise_issuer(raw):
    """Map a raw issuer string to its canonical name."""
    if not raw:
        return None
    key = raw.strip().lower()
    return ISSUER_ALIASES.get(key, raw.strip())


def normalise_asset_class(raw):
    """Map a raw category / asset class string to our canonical name."""
    if not raw:
        return None
    key = raw.strip().lower()
    return ASSET_CLASS_MAP.get(key, raw.strip())
