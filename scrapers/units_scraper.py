"""
Scrapes units on issue, net assets (FUM), and dates from each ETF issuer's website.
Supports: BetaShares, iShares, Global X, VanEck, Vanguard, SPDR/StateStreet.

Usage:
    python3 scrapers/units_scraper.py           # update all supported ETFs
    python3 scrapers/units_scraper.py --issuer BetaShares
    python3 scrapers/units_scraper.py --code NDQ
"""

import sqlite3
import time
import logging
import re
import ssl
import json
import argparse
from datetime import datetime, date
from pathlib import Path
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

DB_PATH = Path(__file__).parent.parent / 'etf_data.db'

UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# ── SSL context (bypass cert errors for some issuers) ────────────────────────
_ctx = ssl.create_default_context()
_ctx.check_hostname = False
_ctx.verify_mode = ssl.CERT_NONE


def _fetch(url: str, accept='text/html', timeout=20) -> str:
    req = urllib_request.Request(url, headers={
        'User-Agent': UA,
        'Accept': accept,
        'Accept-Language': 'en-AU,en;q=0.9',
    })
    with urllib_request.urlopen(req, context=_ctx, timeout=timeout) as r:
        return r.read().decode('utf-8', errors='replace')


def _parse_units(text: str) -> int | None:
    """Parse a units string like '63,193,477' or '234198430' → int."""
    if not text:
        return None
    clean = re.sub(r'[^\d]', '', text)
    return int(clean) if clean else None


def _parse_aud(text: str) -> float | None:
    """Parse AUD amounts like 'AUD 8,385,371,720.620' or '$9,397,639,726' → float."""
    if not text:
        return None
    # Remove currency labels, commas, whitespace
    clean = re.sub(r'[AUD$,\s]', '', text)
    try:
        return float(clean)
    except (ValueError, TypeError):
        return None


def _parse_b_m(text: str) -> float | None:
    """Parse compact FUM values like '$7.90B', '$123.4M' → float (AUD)."""
    if not text:
        return None
    text = text.strip().upper()
    m = re.match(r'[\$]?([\d,.]+)\s*([BM])?', text)
    if not m:
        return None
    val = float(m.group(1).replace(',', ''))
    suffix = m.group(2) or ''
    if suffix == 'B':
        val *= 1e9
    elif suffix == 'M':
        val *= 1e6
    return val


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn


def upsert_units(conn, code: str, units: int | None, net_assets: float | None,
                 as_of_date: str | None = None):
    """Save units and net assets to etfs table and units_history."""
    today = (as_of_date or date.today().isoformat())
    conn.execute("""
        UPDATE etfs SET
            units_on_issue     = COALESCE(?, units_on_issue),
            units_on_issue_date = COALESCE(?, units_on_issue_date),
            net_assets_aud     = COALESCE(?, net_assets_aud),
            net_assets_date    = COALESCE(?, net_assets_date),
            last_updated       = datetime('now')
        WHERE code = ?
    """, (units, today if units else None,
          net_assets, today if net_assets else None,
          code))
    if units or net_assets:
        conn.execute("""
            INSERT INTO units_history (etf_code, date, units_on_issue, net_assets_aud)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(etf_code, date) DO UPDATE SET
                units_on_issue = COALESCE(excluded.units_on_issue, units_on_issue),
                net_assets_aud = COALESCE(excluded.net_assets_aud, net_assets_aud)
        """, (code, today, units, net_assets))
    conn.commit()


# ── BetaShares ────────────────────────────────────────────────────────────────

def scrape_betashares(conn, codes: list[str] | None = None):
    """Fetch all BetaShares fund pages via sitemap, extract units + net assets."""
    logger.info('BetaShares: fetching fund sitemap…')
    try:
        sitemap = _fetch('https://www.betashares.com.au/fund-sitemap.xml')
    except Exception as e:
        logger.error(f'BetaShares sitemap error: {e}')
        return

    fund_urls = re.findall(
        r'<loc>(https://www\.betashares\.com\.au/fund/[^<]+)</loc>', sitemap
    )
    logger.info(f'BetaShares: {len(fund_urls)} fund URLs in sitemap')

    ok = 0
    for url in fund_urls:
        html = None
        for attempt in range(3):
            try:
                html = _fetch(url)
                break
            except HTTPError as e:
                if e.code in (403, 429):
                    wait = (attempt + 1) * 30  # 30s, 60s, 90s
                    logger.info(f'BetaShares {url}: HTTP {e.code}, waiting {wait}s')
                    time.sleep(wait)
                else:
                    logger.debug(f'BetaShares {url}: {e}')
                    break
            except Exception as e:
                logger.debug(f'BetaShares {url}: {e}')
                break

        if not html:
            time.sleep(2)
            continue

        # Extract ASX code from title:
        #   old format: "ASX A200 | Australia 200 ETF | Betashares"
        #   new format: "DHHF ASX | Diversified All Growth ETF | Betashares"
        code_m = re.search(r'(?:ASX\s+([A-Z0-9]{2,6})|([A-Z0-9]{2,6})\s+ASX)\s*\|', html)
        if not code_m:
            time.sleep(0.5)
            continue
        code = code_m.group(1) or code_m.group(2)

        if codes and code not in codes:
            time.sleep(0.2)
            continue

        # Units outstanding: <th>Units outstanding* (#)</th>\n<td>63,193,477</td>
        units_m = re.search(
            r'Units outstanding\*?\s*\(#\)</th>\s*<td>([\d,]+)</td>', html, re.IGNORECASE
        )
        units = _parse_units(units_m.group(1)) if units_m else None

        # Net assets: Net assets*</div> ... <div class='v2-value'>$9,397,639,726</div>
        na_m = re.search(
            r"Net assets\*?</div>.*?<div class='v2-value'>\$?([\d,]+)</div>",
            html, re.DOTALL
        )
        net_assets = _parse_aud(na_m.group(1)) if na_m else None

        if units or net_assets:
            upsert_units(conn, code, units, net_assets)
            logger.info(f'  {code}: units={units:,} net_assets={net_assets/1e6:.0f}M' if units and net_assets
                        else f'  {code}: units={units} net_assets={net_assets}')
            ok += 1

        time.sleep(2)

    logger.info(f'BetaShares: {ok} ETFs updated')


# ── iShares (BlackRock AU) ────────────────────────────────────────────────────

# Built from https://www.blackrock.com/au/products/investment-funds
ISHARES_URLS = {
    'AESG': '/au/products/328186/ishares-global-aggregate-bond-esg-aud-hedged-etf',
    'AGGG': '/au/products/346452/ishares-core-global-aggregate-bond-aud-hedged-etf',
    'ALTB': '/au/products/337679/ishares-15+-year-australian-government-bond-etf',
    'AUMF': '/au/products/284664/ishares-edge-msci-australia-multifactor-etf',
    'BILL': '/au/products/287045/ishares-core-cash-etf',
    'EMXC': '/au/products/337684/ishares-msci-emerging-markets-ex-china-etf',
    'GLDN': '/au/products/332696/ishares-physical-gold-etf',
    'GLIN': '/au/products/331650/ishares-core-ftse-global-infrastructure-aud-hedged-etf',
    'GLPR': '/au/products/331647/ishares-core-ftse-global-property-ex-australia-aud-hedged-etf',
    'IAA':  '/au/products/273416/ishares-asia-50-etf',
    'IACT': '/au/products/343627/ishares-u-s-factor-rotation-active-etf',
    'IAF':  '/au/products/251977/ishares-core-composite-bond-etf',
    'IBAL': '/au/products/327628/ishares-balanced-esg-etf',
    'IBIT': '/au/products/346455/ishares-bitcoin-etf',
    'ICME': '/au/products/347181/ishares-credit-income-active-etf',
    'ICOR': '/au/products/313534/ishares-core-corporate-bond-etf',
    'IEM':  '/au/products/273417/ishares-msci-emerging-markets-etf',
    'IESG': '/au/products/318831/ishares-core-msci-australia-esg-etf',
    'IEU':  '/au/products/273427/ishares-europe-etf',
    'IGB':  '/au/products/251979/ishares-ubs-treasury-fund',
    'IGRO': '/au/products/327642/ishares-high-growth-esg-etf',
    'IHCB': '/au/products/275246/ishares-core-global-corporate-bond-aud-hedged-etf',
    'IHD':  '/au/products/251922/ishares-s-p/asx-dividend-opportunities-etf',
    'IHEB': '/au/products/275254/ishares-j-p-morgan-usd-emerging-markets-bond-aud-hedged-etf',
    'IHHY': '/au/products/275248/ishares-global-high-yield-bond-aud-hedged-etf',
    'IHOO': '/au/products/271031/ishares-global-100-aud-hedged-etf',
    'IHQL': '/au/products/335483/ishares-msci-world-ex-australia-quality-aud-hedged-etf',
    'IHVV': '/au/products/271027/ishares-s-p-500-aud-hedged-etf',
    'IHWL': '/au/products/283119/ishares-core-msci-world-ex-australia-esg-aud-hedged-etf',
    'IJH':  '/au/products/273425/ishares-s-p-mid-cap-etf',
    'IJP':  '/au/products/273434/ishares-msci-japan-etf',
    'IJR':  '/au/products/273426/ishares-s-p-small-cap-etf',
    'IKO':  '/au/products/273436/ishares-msci-south-korea-etf',
    'ILB':  '/au/products/251978/ishares-government-inflation-etf',
    'ILC':  '/au/products/251921/ishares-s-p/asx-20-etf',
    'IMTM': '/au/products/335486/ishares-msci-world-ex-australia-momentum-etf',
    'IOO':  '/au/products/273428/ishares-global-100-etf',
    'IOZ':  '/au/products/251852/ishares-core-s-p/asx-200-etf',
    'IQLT': '/au/products/335480/ishares-msci-world-ex-australia-quality-etf',
    'ISEC': '/au/products/287042/ishares-enhanced-cash-etf',
    'ISO':  '/au/products/251923/ishares-s-p/asx-small-ordinaries-etf',
    'ITEK': '/au/products/328189/ishares-nasdaq-top-30-etf',
    'IUSG': '/au/products/331211/ishares-u-s-treasury-bond-aud-hedged-etf',
    'IVE':  '/au/products/273432/ishares-msci-eafe-etf',
    'IVHG': '/au/products/335523/ishares-msci-world-ex-australia-value-aud-hedged-etf',
    'IVLU': '/au/products/335478/ishares-msci-world-ex-australia-value-etf',
    'IVV':  '/au/products/275304/ishares-s-p-500-etf',
    'IWLD': '/au/products/283117/ishares-core-msci-world-ex-australia-esg-etf',
    'IXI':  '/au/products/273429/ishares-global-consumer-staples-etf',
    'IXJ':  '/au/products/273430/ishares-global-healthcare-etf',
    'IYLD': '/au/products/313537/ishares-yield-plus-etf',
    'IZZ':  '/au/products/273424/ishares-china-large-cap-etf',
    'MVOL': '/au/products/284666/ishares-edge-msci-australia-minimum-volatility-etf',
    'ULTB': '/au/products/339056/ishares-20+-year-u-s-treasury-bond-aud-hedged-etf',
    'WDMF': '/au/products/284665/ishares-world-equity-factor-etf',
    'WVOL': '/au/products/284667/ishares-msci-world-ex-australia-minimum-volatility-etf',
}

BASE_BLK = 'https://www.blackrock.com'


def scrape_ishares(conn, codes: list[str] | None = None):
    """Fetch iShares product pages from BlackRock AU."""
    targets = {k: v for k, v in ISHARES_URLS.items() if not codes or k in codes}
    ok = 0
    for code, path in targets.items():
        url = BASE_BLK + path
        try:
            html = _fetch(url)
        except Exception as e:
            logger.warning(f'iShares {code}: {e}')
            time.sleep(0.5)
            continue

        # Shares outstanding: after "sharesOutstanding", find <div class="data">...</div>
        units, as_of = None, None
        idx = html.find('sharesOutstanding')
        if idx >= 0:
            snippet = html[idx:idx+600]
            m = re.search(r'as of ([\d-]+-\d{4})', snippet)
            as_of = m.group(1) if m else None
            dm = re.search(r'<div class="data">\s*([\d,]+)\s*</div>', snippet)
            units = _parse_units(dm.group(1)) if dm else None

        # Net assets: after "col-totalNetAssetsFundLevel", find <div class="data">AUD ...</div>
        net_assets = None
        idx2 = html.find('col-totalNetAssetsFundLevel')
        if idx2 >= 0:
            snippet2 = html[idx2:idx2+600]
            nm = re.search(r'<div class="data">\s*AUD\s*([\d,\.]+)\s*</div>', snippet2)
            net_assets = _parse_aud(nm.group(1)) if nm else None

        # Benchmark: title="Index: {name}.<br />"
        benchmark = None
        bm = re.search(r'title="Index:\s*([^"<]+?)\.?(?:&lt;|")', html)
        if bm:
            benchmark = bm.group(1).replace('&amp;', '&').strip()

        if units or net_assets:
            upsert_units(conn, code, units, net_assets, as_of)
            logger.info(f'  {code}: units={units:,} net={net_assets/1e6:.0f}M' if units and net_assets
                        else f'  {code}: units={units} net={net_assets}')
            ok += 1
        else:
            logger.debug(f'  {code}: no data found')

        if benchmark:
            conn.execute('UPDATE etfs SET benchmark=? WHERE code=?', (benchmark, code))
            conn.commit()
            logger.debug(f'  {code}: benchmark={benchmark}')

        time.sleep(0.5)

    logger.info(f'iShares: {ok}/{len(targets)} ETFs updated')


# ── Global X ──────────────────────────────────────────────────────────────────

def scrape_globalx(conn, codes: list[str] | None = None):
    """Fetch Global X fund pages (globalxetfs.com.au/funds/{code.lower()}/)."""
    rows = conn.execute(
        "SELECT code FROM etfs WHERE issuer='Global X' ORDER BY fund_size_aud_millions DESC NULLS LAST"
    ).fetchall()
    targets = [r['code'] for r in rows if not codes or r['code'] in (codes or [])]
    ok = 0

    for code in targets:
        url = f'https://www.globalxetfs.com.au/funds/{code.lower()}/'
        try:
            html = _fetch(url)
        except Exception as e:
            logger.debug(f'Global X {code}: {e}')
            time.sleep(0.4)
            continue

        # Shares Outstanding in React JSON blob (may be escaped as \" in HTML):
        # ..."label":"Shares Outstanding","valueType":"text","textValue":"103,367,909"...
        # In the rendered page it appears as: \"label\":\"Shares Outstanding\",...\"textValue\":\"240,098\"
        m = re.search(r'Shares Outstanding.{1,300}?textValue[\\]*":\s*[\\]*"([\d,]+)', html, re.DOTALL)
        units = _parse_units(m.group(1)) if m else None

        # FUM / Net Assets (also look for it in the React JSON)
        fm = re.search(
            r'(?:Net Assets|Funds Under Management).{1,300}?textValue[\\]*":\s*[\\]*"([^"\\]+)',
            html, re.DOTALL
        )
        net_assets = _parse_b_m(fm.group(1)) if fm else None

        if units or net_assets:
            upsert_units(conn, code, units, net_assets)
            logger.info(f'  {code}: units={units:,}' if units else f'  {code}: net={net_assets}')
            ok += 1
        else:
            logger.debug(f'  {code}: no data')

        time.sleep(0.4)

    logger.info(f'Global X: {ok}/{len(targets)} ETFs updated')


# ── VanEck ────────────────────────────────────────────────────────────────────

def _build_vaneck_url_map() -> dict[str, str]:
    """Build code → snapshot URL from VanEck sitemap."""
    try:
        content = _fetch('https://www.vaneck.com.au/sitemap.xml')
    except Exception:
        return {}
    code_to_url = {}
    for url in re.findall(r'https://www\.vaneck\.com\.au/etf/[^\s<"]+/snapshot/?', content):
        parts = url.rstrip('/').split('/')
        # URL: https://www.vaneck.com.au/etf/{category}/{code}/snapshot
        # parts: ['https:', '', 'www.vaneck.com.au', 'etf', '{cat}', '{code}', 'snapshot']
        if len(parts) >= 7:
            code_to_url[parts[5].upper()] = url  # parts[5] is the ETF code
    return code_to_url


def scrape_vaneck(conn, codes: list[str] | None = None):
    """Fetch VanEck snapshot pages. Extracts Total Net Assets (no units data available)."""
    logger.info('VanEck: building URL map from sitemap…')
    url_map = _build_vaneck_url_map()
    logger.info(f'VanEck: {len(url_map)} snapshot URLs found')

    rows = conn.execute(
        "SELECT code FROM etfs WHERE issuer='VanEck' ORDER BY fund_size_aud_millions DESC NULLS LAST"
    ).fetchall()
    targets = [r['code'] for r in rows if not codes or r['code'] in (codes or [])]
    ok = 0

    for code in targets:
        url = url_map.get(code)
        if not url:
            logger.debug(f'  VanEck {code}: no URL in sitemap')
            continue
        try:
            html = _fetch(url)
        except Exception as e:
            logger.debug(f'  VanEck {code}: {e}')
            time.sleep(0.4)
            continue

        # Total Net Assets: label "Total Net Assets" → item-value div
        # Pattern: Total Net Assets ... <div class="item-value"><span class="dollar-sign">$</span>7.90B</div>
        idx = html.find('Total Net Assets')
        net_assets = None
        if idx >= 0:
            snippet = html[idx:idx+3000]
            m = re.search(r'class="item-value"[^>]*>\s*<span[^>]*>\$</span>([\d\w\.]+)', snippet)
            if m:
                net_assets = _parse_b_m('$' + m.group(1))

        if net_assets:
            upsert_units(conn, code, None, net_assets)
            logger.info(f'  {code}: net_assets={net_assets/1e6:.0f}M')
            ok += 1
        else:
            logger.debug(f'  {code}: no FUM data')

        time.sleep(0.4)

    logger.info(f'VanEck: {ok}/{len(targets)} ETFs updated (FUM only)')


# ── Vanguard ──────────────────────────────────────────────────────────────────

# Built from https://www.vanguard.com.au/api/products/personal/funds/8205
VANGUARD_PORT_IDS = {
    'VAF':  '8207',
    'VGB':  '8208',
    'VACF': '8203',
    'VBND': '8200',
    'VIF':  '8216',
    'VCF':  '8217',
    'VEFI': '8224',
    'VAP':  '8206',
    'VAS':  '8205',
    'VETH': '8226',
    'VHY':  '8210',
    'VLC':  '8209',
    'VSO':  '8211',
    'VGS':  '8212',
    'VGAD': '8213',
    'VESG': '8225',
    'V500': 'F105',
    'V5AH': 'F106',
    'VGE':  '8204',
    'VISM': '8227',
    'VEU':  '0991',
    'VTS':  '0970',
    'VEQ':  '8214',
    'VAE':  '8215',
    'VMIN': '8201',
    'VVLU': '8202',
    'VBLD': '8228',
    'VDCO': '8219',
    'VDBA': '8218',
    'VDGR': '8220',
    'VDHG': '8221',
    'VDAL': 'F100',
    'VDIF': 'F101',
}

BASE_VG = 'https://www.vanguard.com.au'


def scrape_vanguard(conn, codes: list[str] | None = None):
    """Fetch Vanguard fund prices API for units on issue and FUM."""
    targets = {k: v for k, v in VANGUARD_PORT_IDS.items() if not codes or k in codes}
    ok = 0

    for code, port_id in targets.items():
        url = f'{BASE_VG}/api/products/personal/fund/{port_id}/prices'
        try:
            resp = json.loads(_fetch(url, accept='application/json'))
        except Exception as e:
            logger.warning(f'Vanguard {code} (portId={port_id}): {e}')
            time.sleep(0.5)
            continue

        data_list = resp.get('data', [])
        units, net_assets, as_of = None, None, None

        for fund in (data_list if isinstance(data_list, list) else [data_list]):
            ui = fund.get('unitsOnIssue', {})
            records = ui.get('OSCLTSHQTY', [])
            if records:
                rec = records[0]
                units = rec.get('outstandingShare')
                as_of = rec.get('effectiveDate', '')[:10] or None

            # Nav prices for total AUM estimate (nav × units)
            nav_records = fund.get('navPrices', [])
            if nav_records and units:
                nav = nav_records[0].get('price')
                if nav:
                    net_assets = float(nav) * units
            break

        if units or net_assets:
            upsert_units(conn, code, units, net_assets, as_of)
            logger.info(f'  {code}: units={units:,} as_of={as_of}' if units
                        else f'  {code}: net={net_assets}')
            ok += 1
        else:
            logger.debug(f'  {code}: no data in API response')

        time.sleep(0.4)

    logger.info(f'Vanguard: {ok}/{len(targets)} ETFs updated')


# ── SPDR / StateStreet ────────────────────────────────────────────────────────

BASE_SSGA = 'https://www.ssga.com'

SSGA_SLUGS = {
    'STW':  'spdr-sp-asx-200-fund-stw',
    'SFY':  'spdr-sp-asx-50-fund-sfy',
    'SSO':  'spdr-sp-asx-small-ordinaries-fund-sso',
    'SLF':  'spdr-sp-asx-200-listed-property-fund-slf',
    'SYI':  'spdr-msci-aus-select-high-dividend-yield-fund-syi',
    'WDIV': 'spdr-sp-global-dividend-fund-wdiv',
    'BOND': 'spdr-sp-asx-iboxx-australian-bond-fund-bond',
    'GOVT': 'spdr-sp-asx-iboxx-aus-government-bond-fund-govt',
    'OZF':  'spdr-sp-asx-200-financials-ex-a-reit-fund-ozf',
    'OZR':  'spdr-sp-asx-200-resources-fund-ozr',
    'E200': 'spdr-sp-asx-200-esg-fund-e200',
    'QMIX': 'spdr-msci-world-quality-mix-fund-qmix',
    'WXOZ': 'spdr-sp-world-ex-australia-carbon-aware-fund-wxoz',
    'WXHG': 'spdr-sp-world-ex-aus-carbon-aware-hedged-fund-wxhg',
    'WEMG': 'spdr-sp-emerging-markets-carbon-aware-fund-wemg',
    'DJRE': 'spdr-dow-jones-global-real-estate-esg-tilted-fund-djre',
}


def scrape_ssga(conn, codes: list[str] | None = None):
    """Fetch SPDR / StateStreet fund pages for units on issue."""
    targets = {k: v for k, v in SSGA_SLUGS.items() if not codes or k in codes}
    ok = 0

    for code, slug in targets.items():
        url = f'{BASE_SSGA}/au/en_gb/individual/etfs/funds/{slug}'
        try:
            html = _fetch(url)
        except Exception as e:
            logger.warning(f'SSGA {code}: {e}')
            time.sleep(0.5)
            continue

        # Units on Issue: after "Units on Issue" label, find <td class="data">79,985,908</td>
        m = re.search(
            r'Units on Issue\s*</td>\s*<td class="data">([\d,]+)</td>', html, re.IGNORECASE
        )
        units = _parse_units(m.group(1)) if m else None

        # Net assets: look for "Net Asset Value" or "Total Net Assets"
        nm = re.search(
            r'(?:Net Asset Value|Total Net Assets)\s*</td>\s*<td class="data">\$?([\d,\.]+[MBK]?)\s*</td>',
            html, re.IGNORECASE
        )
        net_assets = _parse_b_m(nm.group(1)) if nm else None

        if units or net_assets:
            upsert_units(conn, code, units, net_assets)
            logger.info(f'  {code}: units={units:,}' if units else f'  {code}: net={net_assets}')
            ok += 1
        else:
            logger.debug(f'  {code}: no data found')

        time.sleep(0.5)

    logger.info(f'SSGA: {ok}/{len(targets)} ETFs updated')


# ── Main ──────────────────────────────────────────────────────────────────────

ISSUERS = {
    'BetaShares':  scrape_betashares,
    'iShares':     scrape_ishares,
    'Global X':    scrape_globalx,
    'VanEck':      scrape_vaneck,
    'Vanguard':    scrape_vanguard,
    'SPDR':        scrape_ssga,
    'StateStreet': scrape_ssga,
}


def run(issuer_filter: str | None = None, code_filter: str | None = None):
    conn = get_db()
    codes = [code_filter] if code_filter else None

    if code_filter:
        # Find issuer for this code
        row = conn.execute("SELECT issuer FROM etfs WHERE code=?", (code_filter,)).fetchone()
        if not row:
            logger.error(f'Code {code_filter} not found in DB')
            return
        issuer_filter = row['issuer']

    seen_fns = set()
    for issuer, fn in ISSUERS.items():
        if issuer_filter and issuer_filter.lower() not in issuer.lower():
            continue
        if id(fn) in seen_fns:
            continue  # skip duplicate (SPDR + StateStreet both map to scrape_ssga)
        seen_fns.add(id(fn))
        logger.info(f'\n=== {issuer} ===')
        try:
            fn(conn, codes)
        except Exception as e:
            logger.error(f'{issuer} scraper failed: {e}', exc_info=True)

    conn.close()
    logger.info('Done.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape ETF units on issue from issuer websites')
    parser.add_argument('--issuer', help='Only scrape this issuer (e.g. BetaShares, iShares)')
    parser.add_argument('--code', help='Only scrape this ETF code (e.g. NDQ)')
    args = parser.parse_args()
    run(issuer_filter=args.issuer, code_filter=args.code)
