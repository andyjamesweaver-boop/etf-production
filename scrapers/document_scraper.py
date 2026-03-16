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

from scrapers.config import DB_PATH, ISHARES_AU_PRODUCTS
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


def _discover_vanguard(code: str) -> dict:
    """
    Vanguard Australia's website is a fully JavaScript-rendered Angular SPA.
    Document URLs are not accessible via static HTTP requests.
    Returns empty dict — Vanguard ETFs are skipped for document ingestion.
    """
    logger.debug(f"[{code}] Vanguard SPA — document discovery not supported without headless browser")
    return {}


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
    Call Claude Haiku via requests to generate a structured summary.
    Returns parsed dict or None on failure.
    """
    if not ANTHROPIC_API_KEY:
        logger.warning("ANTHROPIC_API_KEY not set — skipping summarisation")
        return None

    messages, system = _build_prompt(code, name, text)

    payload = {
        "model": CLAUDE_MODEL,
        "max_tokens": 1024,
        "system": system,
        "messages": messages,
    }

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    backoff_by_status = {
        429: [30, 60, 90],
        500: [10, 20, 30],
        529: [10, 20, 30],
    }

    for attempt in range(3):
        try:
            resp = requests.post(
                "https://api.anthropic.com/v1/messages",
                json=payload,
                headers=headers,
                timeout=60,
            )

            if resp.status_code != 200:
                status = resp.status_code
                waits = backoff_by_status.get(status, [10, 20, 30])
                wait = waits[attempt] if attempt < len(waits) else waits[-1]
                logger.warning(
                    f"Claude API HTTP {status} for {code} "
                    f"(attempt {attempt + 1}/3) — waiting {wait}s"
                )
                if attempt < 2:
                    time.sleep(wait)
                continue

            body = resp.json()
            raw_text = body["content"][0]["text"]
            return _parse_claude_json(raw_text)

        except Exception as e:
            logger.warning(f"Claude API error for {code} (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                time.sleep(10)
        except Exception as e:
            logger.warning(f"Claude API error for {code} (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                time.sleep(10)

    logger.error(f"All Claude API retries failed for {code}")
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
