"""
Document scraper — discovers PDS/TMD/factsheet URLs for Australian ETFs,
extracts PDF text, and generates AI summaries via Claude Haiku.

Usage:
    from scrapers.document_scraper import scrape_documents
    scrape_documents(codes=['STW', 'VAS', 'NDQ', 'IVV'])   # test run
    scrape_documents()                                        # all ETFs
"""

import io
import json
import logging
import os
import re
import time
import urllib.parse

import pdfplumber
import requests
from bs4 import BeautifulSoup

import anthropic

from scrapers.config import DB_PATH, ISHARES_AU_PRODUCTS, VANGUARD_AU_PORT_IDS
from scrapers.db_writer import get_connection, log_scrape, upsert_etf

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ config

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = "claude-haiku-4-5-20251001"
PDF_PAGE_CAP = 15
MIN_TEXT_CHARS = 200

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

# ------------------------------------------------------------------ HTTP helpers

def _head_ok(url: str) -> bool:
    """
    Return True if the URL returns 2xx AND (for .pdf URLs) content-type is application/pdf.
    Falls back to GET if HEAD returns 405.
    """
    try:
        r = requests.head(url, headers=_HEADERS, timeout=15, allow_redirects=True)
        if r.status_code == 405:
            r = requests.get(url, headers=_HEADERS, timeout=15, stream=True)
        if not (200 <= r.status_code < 300):
            return False
        if url.lower().endswith(".pdf"):
            ct = r.headers.get("Content-Type", "")
            return "pdf" in ct.lower()
        return True
    except Exception as e:
        logger.debug(f"head_ok failed for {url}: {e}")
        return False


def _fetch_html(url: str) -> str | None:
    """Fetch a page and return its HTML text, or None on failure."""
    try:
        r = requests.get(url, headers=_HEADERS, timeout=20)
        r.raise_for_status()
        return r.text
    except Exception as e:
        logger.debug(f"fetch_html failed for {url}: {e}")
        return None


def _fetch_bytes(url: str) -> bytes | None:
    """Fetch binary content (PDF), or None on failure."""
    try:
        r = requests.get(url, headers=_HEADERS, timeout=60)
        r.raise_for_status()
        return r.content
    except Exception as e:
        logger.debug(f"fetch_bytes failed for {url}: {e}")
        return None


# ------------------------------------------------------------------ URL discovery

_LABEL_HINTS = {
    "pds_url":       ["product disclosure", "pds"],
    "tmd_url":       ["target market", "tmd"],
    "factsheet_url": ["fact sheet", "factsheet", "fund facts"],
}


def _discover_html_links(page_url: str, hints: dict | None = None) -> dict:
    """
    Parse `page_url` HTML for PDF links matching document type hints.
    Returns partial dict with keys from {"pds_url", "tmd_url", "factsheet_url"}.
    """
    if hints is None:
        hints = _LABEL_HINTS

    html = _fetch_html(page_url)
    if not html:
        return {}

    soup = BeautifulSoup(html, "lxml")
    result: dict = {}

    for a in soup.find_all("a", href=True):
        href: str = a["href"]
        if not href.lower().endswith(".pdf"):
            continue
        abs_url = urllib.parse.urljoin(page_url, href)
        link_text = (a.get_text(" ", strip=True) + " " + href).lower()

        for key, keywords in hints.items():
            if key in result:
                continue
            if any(kw in link_text for kw in keywords):
                result[key] = abs_url

    return result


def _discover_spdr(code: str) -> dict:
    c = code.lower()
    candidates = {
        "pds_url": (
            f"https://www.ssga.com/library-content/products/fund-docs/etfs/apac/au/pds/"
            f"product-disclosure-statement-au-en-{c}.pdf"
        ),
        "tmd_url": (
            f"https://www.ssga.com/library-content/products/fund-docs/etfs/apac/au/tmd/"
            f"target-market-determination-au-en-{c}.pdf"
        ),
        "factsheet_url": (
            f"https://www.ssga.com/library-content/products/factsheets/etfs/apac/"
            f"factsheet-au-en_gb-{c}.pdf"
        ),
    }
    return {k: v for k, v in candidates.items() if _head_ok(v)}


def _discover_betashares(code: str, slug: str) -> dict:
    result: dict = {}

    # Factsheet: predictable template
    fs_url = f"https://www.betashares.com.au/files/factsheets/{code}-Factsheet.pdf"
    if _head_ok(fs_url):
        result["factsheet_url"] = fs_url

    # PDS + TMD: parse fund page
    fund_page = f"https://www.betashares.com.au/fund/{slug}/"
    html_docs = _discover_html_links(fund_page, _LABEL_HINTS)
    result.update({k: v for k, v in html_docs.items() if k not in result})

    return result


def _discover_vaneck(code: str) -> dict:
    result: dict = {}
    c = code.lower()

    # TMD: predictable template
    tmd_url = (
        f"https://www.vaneck.com.au/globalassets/home.au/media/managedassets/library/"
        f"assets/target-market-determination/tmd---{c}.pdf"
    )
    if _head_ok(tmd_url):
        result["tmd_url"] = tmd_url

    # PDS + factsheet: parse documents page
    docs_page = f"https://www.vaneck.com.au/etf/equity/{c}/documents/"
    html_docs = _discover_html_links(docs_page, _LABEL_HINTS)
    result.update({k: v for k, v in html_docs.items() if k not in result})

    return result


# Module-level cache for the iShares TMD page (fetched once, reused for all ~55 funds)
_ISHARES_TMD_PAGE_CACHE: dict | None = None
_ISHARES_TMD_BASE = "https://www.blackrock.com"
_ISHARES_TMD_PAGE_URL = "https://www.blackrock.com/au/resources/target-market-determination"


def _get_ishares_tmd_page() -> dict:
    """
    Parse the iShares TMD page once and return a dict mapping
    ASX code -> {"tmd_url": ..., "pds_url": ...}.
    Result is cached in _ISHARES_TMD_PAGE_CACHE.
    """
    global _ISHARES_TMD_PAGE_CACHE
    if _ISHARES_TMD_PAGE_CACHE is not None:
        return _ISHARES_TMD_PAGE_CACHE

    logger.info("Fetching iShares TMD index page (cached for session)")
    html = _fetch_html(_ISHARES_TMD_PAGE_URL)
    if not html:
        _ISHARES_TMD_PAGE_CACHE = {}
        return {}

    from bs4 import BeautifulSoup as _BS
    soup = _BS(html, "lxml")
    result: dict = {}

    # Each fund appears as a text node containing the ASX code e.g. "iShares IVV ETF..."
    # Nearby <a> tags link to the TMD PDF and PDS PDF
    for tag in soup.find_all(string=re.compile(r'\b[A-Z]{2,6}\b')):
        codes_found = re.findall(r'\b([A-Z]{2,6})\b', str(tag))
        if not codes_found:
            continue
        parent = tag.find_parent()
        if not parent:
            continue
        section = parent.find_parent() or parent
        links = section.find_all("a", href=re.compile(r"\.pdf", re.IGNORECASE))
        for code_candidate in codes_found:
            if code_candidate not in ISHARES_AU_PRODUCTS:
                continue
            entry = result.setdefault(code_candidate, {})
            for link in links:
                href = link["href"]
                if not href.startswith("http"):
                    href = _ISHARES_TMD_BASE + href
                link_text = link.get_text(strip=True).lower()
                if "target market" in link_text or "tmd" in link_text or "-tmd-" in href:
                    entry.setdefault("tmd_url", href)
                elif "product disclosure" in link_text or "pds" in link_text or "product-disclosure" in href:
                    entry.setdefault("pds_url", href)

    _ISHARES_TMD_PAGE_CACHE = result
    logger.info(f"iShares TMD page parsed: {len(result)} funds indexed")
    return result


def _discover_ishares(code: str) -> dict:
    # TMD: predictable template (fast, no page fetch needed)
    tmd_url = (
        f"https://www.blackrock.com/au/literature/continuous-disclosure-and-important-information/"
        f"ishares-{code.lower()}-tmd-en-au.pdf"
    )
    result: dict = {}
    if _head_ok(tmd_url):
        result["tmd_url"] = tmd_url

    # PDS: parse the shared TMD index page (cached)
    page_data = _get_ishares_tmd_page()
    fund_data = page_data.get(code, {})
    if "pds_url" in fund_data and "pds_url" not in result:
        result["pds_url"] = fund_data["pds_url"]
    if "tmd_url" in fund_data and "tmd_url" not in result:
        result["tmd_url"] = fund_data["tmd_url"]

    return result


def _discover_globalx(code: str, issuer_url: str | None) -> dict:
    page_url = issuer_url or f"https://www.globalxetfs.com.au/funds/{code.lower()}/"
    return _discover_html_links(page_url, _LABEL_HINTS)


_VANGUARD_DOC_API = "https://www.vanguard.com.au/personal/api/products/personal/fund"
_VANGUARD_STATIC  = "https://static.vanguard.com.au/content/dam/intl-pdfs/australia/products"

# Code → kebab-case slug used in Vanguard's static CDN filenames.
# e.g. TMD URL: {_VANGUARD_STATIC}/tmd/en/vanguard-{slug}-tmd-en-au.pdf
_VANGUARD_SLUGS: dict[str, str] = {
    'VAS':  'australian-shares-index-etf',
    'VGS':  'msci-index-international-shares-etf',
    'VGAD': 'msci-index-international-shares-hedged-etf',
    'VGE':  'ftse-emerging-markets-shares-etf',
    'VHY':  'australian-shares-high-yield-etf',
    'VAP':  'australian-property-securities-index-etf',
    'VAF':  'australian-fixed-interest-index-etf',
    'VGB':  'australian-government-bond-index-etf',
    'VACF': 'australian-corporate-fixed-interest-index-etf',
    'VLC':  'msci-australian-large-companies-index-etf',
    'VSO':  'msci-australian-small-companies-index-etf',
    'VEQ':  'ftse-europe-shares-etf',
    'VAE':  'ftse-asia-ex-japan-shares-index-etf',
    'VIF':  'international-fixed-interest-index-hedged-etf',
    'VCF':  'international-credit-securities-index-hedged-etf',
    'VDBA': 'diversified-balanced-index-etf',
    'VDCO': 'diversified-conservative-index-etf',
    'VDGR': 'diversified-growth-index-etf',
    'VDHG': 'diversified-high-growth-index-etf',
    'VDAL': 'diversified-all-growth-index-etf',
    'VDIF': 'diversified-income-etf',
    'VEFI': 'ethically-conscious-global-aggregate-bond-index-hedged-etf',
    'VESG': 'ethically-conscious-international-shares-index-etf',
    'VETH': 'ethically-conscious-australian-shares-etf',
    'VISM': 'msci-international-small-companies-index-etf',
    'VBLD': 'global-infrastructure-index-etf',
    'VBND': 'global-aggregate-bond-index-hedged-etf',
    'VMIN': 'global-minimum-volatility-active-etf',
    'VLUE': 'global-value-equity-active-etf',
    'VVLU': 'global-value-equity-active-etf',
    'VEU':  'all-world-ex-us-shares-index-etf',
    'VTS':  'us-total-market-shares-index-etf',
}


def _discover_vanguard(code: str) -> dict:
    """
    Discover document URLs for a Vanguard Australia ETF.

    Strategy 1 — Vanguard documents REST API:
        GET /personal/api/products/personal/fund/{portId}/documents
        Returns a JSON list of {type, url} objects.

    Strategy 2 — static CDN URL probing:
        https://static.vanguard.com.au/content/dam/intl-pdfs/australia/products/
        Uses the _VANGUARD_SLUGS mapping; probes TMD, PDS, and product-profile URLs.
    """
    result: dict = {}
    port_id = VANGUARD_AU_PORT_IDS.get(code)

    # ── Strategy 1: Vanguard fund API → documentDetails ─────────────────────
    # The /documents endpoint returns the full fund record; documents live in
    # data[0].documentDetails as [{type, path, ...}].
    # type codes: "TMD", "PDS", "FS" (factsheet), "AR", "SD", "LIR", ...
    if port_id:
        try:
            api_url = f"{_VANGUARD_DOC_API}/{port_id}/documents"
            resp = requests.get(
                api_url,
                headers={**_HEADERS, "Accept": "application/json"},
                timeout=20,
            )
            if resp.status_code == 200:
                body = resp.json()
                fund_records = body.get("data") or []
                doc_details = (
                    fund_records[0].get("documentDetails") or []
                    if isinstance(fund_records, list) and fund_records
                    else []
                )
                for doc in doc_details:
                    doc_type = (doc.get("type") or "").upper()
                    path = doc.get("path") or ""
                    if not path:
                        continue
                    if doc_type == "TMD":
                        result.setdefault("tmd_url", path)
                    elif doc_type == "PDS":
                        result.setdefault("pds_url", path)
                    elif doc_type == "FS":
                        result.setdefault("factsheet_url", path)
                logger.debug(f"[{code}] Vanguard API: found {list(result.keys())}")
        except Exception as e:
            logger.debug(f"[{code}] Vanguard API documents endpoint failed: {e}")

    if len(result) == 3:
        return result

    # ── Strategy 2: static CDN probing ──────────────────────────────────────
    slug = _VANGUARD_SLUGS.get(code)
    if not slug:
        logger.debug(f"[{code}] No Vanguard slug — skipping CDN probe")
        return result

    cdn_candidates: dict[str, list[str]] = {
        "tmd_url": [
            f"{_VANGUARD_STATIC}/tmd/en/vanguard-{slug}-tmd-en-au.pdf",
            f"{_VANGUARD_STATIC}/tmd/vanguard-{slug}-tmd-en-au.pdf",
        ],
        "pds_url": [
            f"{_VANGUARD_STATIC}/pds/en/vanguard-{slug}-pds-en-au.pdf",
            f"{_VANGUARD_STATIC}/pds/vanguard-{slug}-pds-en-au.pdf",
        ],
        "factsheet_url": [
            f"{_VANGUARD_STATIC}/productProfile/en/vanguard-{slug}-product-profile-en-au.pdf",
            f"{_VANGUARD_STATIC}/productProfile/vanguard-{slug}-product-profile-en-au.pdf",
            f"{_VANGUARD_STATIC}/factsheet/en/vanguard-{slug}-factsheet-en-au.pdf",
        ],
    }

    for key, urls in cdn_candidates.items():
        if key in result:
            continue
        for url in urls:
            if _head_ok(url):
                result[key] = url
                logger.debug(f"[{code}] Found {key} via CDN: {url}")
                break

    return result


def discover_documents(code: str, issuer: str | None, row: dict) -> dict:
    """
    Dispatch to per-issuer discovery; returns
    {"pds_url": str|None, "tmd_url": str|None, "factsheet_url": str|None}.
    """
    issuer = (issuer or "").strip()

    if issuer in ("SPDR", "StateStreet", "State Street"):
        docs = _discover_spdr(code)

    elif issuer == "BetaShares":
        issuer_url = row.get("issuer_url") or ""
        # Derive slug: strip scheme+host and trailing slash
        parsed = urllib.parse.urlparse(issuer_url)
        slug = parsed.path.strip("/").split("/")[-1] if parsed.path.strip("/") else code.lower()
        docs = _discover_betashares(code, slug)

    elif issuer == "VanEck":
        docs = _discover_vaneck(code)

    elif issuer == "iShares":
        docs = _discover_ishares(code)

    elif issuer == "Global X":
        docs = _discover_globalx(code, row.get("issuer_url"))

    elif issuer == "Vanguard":
        docs = _discover_vanguard(code)

    else:
        issuer_url = row.get("issuer_url")
        docs = _discover_html_links(issuer_url, _LABEL_HINTS) if issuer_url else {}

    # Normalise: ensure all three keys present
    return {
        "pds_url":       docs.get("pds_url"),
        "tmd_url":       docs.get("tmd_url"),
        "factsheet_url": docs.get("factsheet_url"),
    }


# ------------------------------------------------------------------ PDF extraction

def extract_pdf_text(url: str, max_pages: int = PDF_PAGE_CAP) -> str | None:
    """
    Download a PDF from `url` and extract text from the first `max_pages` pages.
    Returns cleaned text string, or None if extraction fails or yields too little text.
    """
    raw = _fetch_bytes(url)
    if not raw:
        return None

    try:
        with pdfplumber.open(io.BytesIO(raw)) as pdf:
            pages = pdf.pages[:max_pages]
            parts = []
            for page in pages:
                t = page.extract_text()
                if t:
                    parts.append(t)
            text = "\n\n".join(parts)
    except Exception as e:
        logger.debug(f"pdfplumber failed for {url}: {e}")
        return None

    # Clean: collapse tabs + multi-spaces, collapse 3+ newlines to 2
    text = re.sub(r"[\t ]{2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()

    if len(text) < MIN_TEXT_CHARS:
        return None
    return text


# ------------------------------------------------------------------ Claude summarisation

def _build_prompt(code: str, name: str, text: str) -> list[dict]:
    """Build the messages list for the Claude API call."""
    truncated = text[:60_000]
    system = (
        "You are a financial document analyst. Extract key information from "
        "Australian ETF product disclosure documents. Respond ONLY with valid JSON, "
        "no markdown fences, no additional text."
    )
    user = f"""Analyse this Australian ETF document for {code} ({name}) and return a JSON object with exactly these keys:

{{
  "objective": "One sentence describing the fund's investment objective",
  "key_risks": ["Risk 1", "Risk 2", "Risk 3"],
  "suitable_for": "One sentence describing the investor profile this ETF suits",
  "summary": "Two to three sentence plain-English summary of the fund"
}}

Rules:
- Base your answer only on the document provided
- 2 to 5 risks maximum
- Plain English, no jargon
- Do not include performance figures or specific return numbers
- Do not include markdown formatting in your response

Document text:
{truncated}"""

    return [
        {"role": "user", "content": user},
    ], system


def _parse_claude_json(raw: str) -> dict | None:
    """Strip markdown fences, parse JSON, validate required keys."""
    # Strip ```json ... ``` or ``` ... ``` fences
    text = re.sub(r"^```(?:json)?\s*", "", raw.strip(), flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        logger.warning(f"JSON parse failed: {e} — raw: {raw[:200]}")
        return None

    required = {"objective", "key_risks", "suitable_for", "summary"}
    if not required.issubset(data.keys()):
        missing = required - data.keys()
        logger.warning(f"Claude response missing keys: {missing}")
        return None

    return data


def summarise_with_claude(code: str, name: str, text: str) -> dict | None:
    """
    Call Claude Haiku via the Anthropic SDK to generate a structured summary.
    Returns a parsed dict or None on failure.
    """
    if not ANTHROPIC_API_KEY:
        logger.warning("ANTHROPIC_API_KEY not set — skipping summarisation")
        return None

    messages, system = _build_prompt(code, name, text)
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    for attempt in range(3):
        try:
            response = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=1024,
                system=system,
                messages=messages,
            )
            raw_text = response.content[0].text
            result = _parse_claude_json(raw_text)
            if result:
                return result
            logger.warning(f"[{code}] Claude returned unparseable JSON (attempt {attempt + 1}/3)")

        except anthropic.RateLimitError:
            wait = [30, 60, 90][min(attempt, 2)]
            logger.warning(f"[{code}] Claude rate limit (attempt {attempt + 1}/3) — waiting {wait}s")
            if attempt < 2:
                time.sleep(wait)

        except anthropic.APIStatusError as e:
            wait = 10 * (attempt + 1)
            logger.warning(f"[{code}] Claude API error {e.status_code} (attempt {attempt + 1}/3) — waiting {wait}s")
            if attempt < 2:
                time.sleep(wait)

        except Exception as e:
            logger.warning(f"[{code}] Claude unexpected error (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                time.sleep(10)

    logger.error(f"[{code}] All Claude API retries failed")
    return None


# ------------------------------------------------------------------ needs-summary check

def _needs_summary(row: dict, new_docs: dict) -> bool:
    """Return True if this ETF needs a (re-)generated summary."""
    if row.get("summary_generated_at") is None:
        return True
    # Re-generate if any doc URL changed
    for key in ("pds_url", "tmd_url", "factsheet_url"):
        old = row.get(key)
        new = new_docs.get(key)
        if old != new and new is not None:
            return True
    return False


# ------------------------------------------------------------------ orchestrator

def scrape_documents(db_path: str = DB_PATH, codes: list[str] | None = None) -> int:
    """
    Main entry point: discover documents, extract PDF text, summarise with Claude.

    Args:
        db_path: path to etf_data.db
        codes:   optional list of ASX codes to process (default: all ETFs)

    Returns:
        Number of ETFs processed.
    """
    from datetime import datetime

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    conn = get_connection(db_path)
    started_at = datetime.utcnow().isoformat()

    query = "SELECT * FROM etfs ORDER BY rank_by_fum ASC NULLS LAST"
    rows = [dict(r) for r in conn.execute(query).fetchall()]

    if codes:
        codes_set = {c.upper() for c in codes}
        rows = [r for r in rows if r["code"] in codes_set]

    logger.info(f"Processing {len(rows)} ETFs for document discovery")

    count = 0
    for row in rows:
        code: str = row["code"]
        issuer: str | None = row.get("issuer")
        name: str = row.get("name") or code

        try:
            # ── 1. Discover document URLs ──────────────────────────────
            logger.info(f"[{code}] Discovering documents ({issuer})")
            new_docs = discover_documents(code, issuer, row)

            has_any = any(v for v in new_docs.values())
            if not has_any:
                logger.info(f"[{code}] No documents found — skipping")
                continue

            found = [k for k, v in new_docs.items() if v]
            logger.info(f"[{code}] Found: {found}")

            # ── 2. Persist doc URLs ────────────────────────────────────
            upsert_etf(conn, {
                "code": code,
                **{k: v for k, v in new_docs.items() if v is not None},
                "docs_last_scraped": datetime.utcnow().isoformat(),
            })
            conn.commit()

            # ── 3. Check if summary needed ─────────────────────────────
            if not _needs_summary(row, new_docs):
                logger.info(f"[{code}] Summary up to date — skipping")
                count += 1
                continue

            # ── 4. Extract PDF text ────────────────────────────────────
            pdf_url = new_docs.get("pds_url") or new_docs.get("tmd_url") or new_docs.get("factsheet_url")
            logger.info(f"[{code}] Extracting text from {pdf_url}")
            text = extract_pdf_text(pdf_url)
            if not text:
                logger.info(f"[{code}] PDF text extraction failed or too short — skipping summary")
                count += 1
                continue

            logger.info(f"[{code}] Extracted {len(text)} chars — calling Claude")

            # ── 5. Summarise ───────────────────────────────────────────
            summary = summarise_with_claude(code, name, text)
            if summary:
                upsert_etf(conn, {
                    "code": code,
                    "summary": json.dumps(summary),
                    "summary_generated_at": datetime.utcnow().isoformat(),
                })
                conn.commit()
                logger.info(f"[{code}] Summary saved")
            else:
                logger.warning(f"[{code}] Summarisation failed")

            count += 1

        except Exception as e:
            logger.error(f"[{code}] Unhandled error: {e}", exc_info=True)
            continue

    try:
        log_scrape(conn, "documents", "success", records_affected=count,
                   duration_secs=None, started_at=started_at)
    except Exception:
        pass

    conn.close()
    logger.info(f"Document scrape complete: {count} ETFs processed")
    return count
