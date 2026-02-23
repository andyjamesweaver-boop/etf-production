"""
Base scraper — session management, per-domain rate limiting, retries, logging.
"""

import time
import logging
import requests
from urllib.parse import urlparse
from scrapers.config import (
    RATE_LIMITS, MAX_RETRIES, RETRY_BACKOFF_BASE,
    USER_AGENT, REQUEST_TIMEOUT,
)

logger = logging.getLogger(__name__)

# Track last-request timestamps per domain
_domain_timestamps: dict[str, float] = {}


def _domain_key(url: str) -> str:
    """Extract the rate-limit domain key from a URL."""
    host = urlparse(url).hostname or ''
    # Walk up sub-domains to match config keys
    parts = host.split('.')
    for i in range(len(parts)):
        candidate = '.'.join(parts[i:])
        if candidate in RATE_LIMITS:
            return candidate
    return '_default'


def _wait_for_rate_limit(url: str):
    """Sleep if needed to respect per-domain rate limits."""
    key = _domain_key(url)
    delay = RATE_LIMITS.get(key, RATE_LIMITS['_default'])
    last = _domain_timestamps.get(key, 0)
    elapsed = time.time() - last
    if elapsed < delay:
        wait = delay - elapsed
        logger.debug(f"Rate-limit: sleeping {wait:.1f}s for {key}")
        time.sleep(wait)
    _domain_timestamps[key] = time.time()


def get_session() -> requests.Session:
    """Create a requests session with our default headers."""
    s = requests.Session()
    s.headers.update({
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-AU,en;q=0.9',
    })
    return s


# Module-level shared session (lazy)
_session: requests.Session | None = None


def _get_shared_session() -> requests.Session:
    global _session
    if _session is None:
        _session = get_session()
    return _session


def fetch(url: str, *, session: requests.Session | None = None,
          params: dict | None = None,
          headers: dict | None = None,
          timeout: int = REQUEST_TIMEOUT,
          json_response: bool = False,
          stream: bool = False,
          max_retries: int = MAX_RETRIES) -> requests.Response | None:
    """
    Fetch a URL with rate limiting and exponential backoff retries.

    Returns the Response object, or None if all retries fail.
    """
    sess = session or _get_shared_session()
    merged_headers = dict(sess.headers)
    if headers:
        merged_headers.update(headers)

    for attempt in range(1, max_retries + 1):
        _wait_for_rate_limit(url)
        try:
            resp = sess.get(
                url,
                params=params,
                headers=merged_headers,
                timeout=timeout,
                stream=stream,
            )

            # Handle 429 Too Many Requests
            if resp.status_code == 429:
                retry_after = int(resp.headers.get('Retry-After', RETRY_BACKOFF_BASE ** attempt))
                logger.warning(f"429 for {url}, waiting {retry_after}s (attempt {attempt})")
                time.sleep(retry_after)
                # Double the delay for this domain
                key = _domain_key(url)
                RATE_LIMITS[key] = RATE_LIMITS.get(key, RATE_LIMITS['_default']) * 2
                continue

            resp.raise_for_status()
            return resp

        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP {e.response.status_code} for {url} (attempt {attempt}/{max_retries})")
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Connection error for {url} (attempt {attempt}/{max_retries}): {e}")
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout for {url} (attempt {attempt}/{max_retries})")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error for {url} (attempt {attempt}/{max_retries}): {e}")

        if attempt < max_retries:
            wait = RETRY_BACKOFF_BASE ** attempt
            logger.debug(f"Retrying in {wait}s...")
            time.sleep(wait)

    logger.error(f"All {max_retries} retries failed for {url}")
    return None


def fetch_json(url: str, **kwargs) -> dict | list | None:
    """Convenience: fetch URL and parse as JSON."""
    kwargs.setdefault('headers', {})
    kwargs['headers']['Accept'] = 'application/json'
    resp = fetch(url, **kwargs)
    if resp is None:
        return None
    try:
        return resp.json()
    except ValueError:
        logger.error(f"Invalid JSON from {url}")
        return None


def download_file(url: str, dest_path: str, **kwargs) -> bool:
    """Download a file (e.g. Excel report) to disk."""
    resp = fetch(url, stream=True, **kwargs)
    if resp is None:
        return False
    try:
        with open(dest_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Downloaded {url} -> {dest_path}")
        return True
    except IOError as e:
        logger.error(f"Failed to write {dest_path}: {e}")
        return False
