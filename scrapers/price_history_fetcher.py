"""
Fetches weekly OHLCV price history for all ASX ETFs and benchmark indices
from Yahoo Finance, then stores them in the SQLite database.

Usage:
    python3 scrapers/price_history_fetcher.py          # full 5Y fetch
    python3 scrapers/price_history_fetcher.py --update  # only missing/stale ETFs
"""

import sqlite3
import time
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import yfinance as yf

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

DB_PATH = Path(__file__).parent.parent / 'etf_data.db'

# Benchmark indices to track alongside ETFs
# Key = our internal name, value = Yahoo Finance ticker
BENCHMARKS = {
    # Equities
    'ASX200':  '^AXJO',    # S&P/ASX 200
    'AORD':    '^AORD',    # All Ordinaries
    'SP500':   '^GSPC',    # S&P 500
    'NDX100':  '^NDX',     # Nasdaq 100
    'MSCIW':   'ACWI',     # MSCI All Country World (ETF proxy, USD)
    # Fixed Income
    'AU_BOND': 'VAF.AX',   # Vanguard AU Fixed Interest Index ETF
    'US_BOND': 'AGG',      # iShares US Aggregate Bond ETF (USD)
    # Commodities
    'GOLD':    'GLD',      # SPDR Gold Shares (USD)
    'COMMOD':  'GSG',      # iShares GSCI Commodity-Indexed Trust (USD)
    # Cash
    'AU_CASH': 'BILL.AX',  # BetaShares Australian High Interest Cash ETF
    # Crypto
    'BTC':     'BTC-USD',  # Bitcoin (USD)
    'ETH':     'ETH-USD',  # Ethereum (USD)
}

# Asset-class → primary benchmark for default overlay
AC_BENCHMARK = {
    'Australian Equities':   'ASX200',
    'International Equities':'SP500',
    'US Equities':           'SP500',
    'Fixed Income':          'AU_BOND',
    'Property':              'ASX200',
    'Commodities':           'GOLD',
    'Cash':                  'AU_CASH',
    'Multi-Asset':           'SP500',
    'Thematic':              'SP500',
    'Currency':              'SP500',
    'Digital Assets':        'BTC',
    'Alternatives':          'SP500',
    'Diversified':           'SP500',
    'leveraged & inverse':   'ASX200',
}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn


def _yahoo_ticker(code: str, exchange: str) -> str:
    """Convert ETF code + exchange to Yahoo Finance ticker symbol."""
    if exchange == 'CXA':
        # CBOE Australia — not covered by Yahoo Finance for most ETFs
        return code  # will likely return empty, handled gracefully
    return f'{code}.AX'


def fetch_etf_price_history(conn, code: str, yf_ticker: str, period: str = '5y') -> int:
    """Fetch weekly OHLCV for one ETF. Returns number of rows inserted."""
    try:
        df = yf.download(
            yf_ticker,
            period=period,
            interval='1wk',
            progress=False,
            auto_adjust=True,
        )
        if df.empty:
            return 0

        # Multi-level columns from yfinance 1.x
        if hasattr(df.columns, 'levels'):
            df.columns = df.columns.droplevel(1)

        rows = []
        for date_idx, row in df.iterrows():
            date_str = date_idx.strftime('%Y-%m-%d')
            rows.append((
                code,
                date_str,
                float(row.get('Open', 0) or 0) or None,
                float(row.get('High', 0) or 0) or None,
                float(row.get('Low',  0) or 0) or None,
                float(row.get('Close', 0) or 0) or None,
                int(row.get('Volume', 0) or 0) or None,
            ))

        conn.executemany(
            """INSERT INTO price_history (etf_code, date, open, high, low, close, volume)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(etf_code, date) DO UPDATE SET
                 open=excluded.open, high=excluded.high, low=excluded.low,
                 close=excluded.close, volume=excluded.volume""",
            rows,
        )
        conn.commit()
        return len(rows)

    except Exception as e:
        logger.warning(f'{code} ({yf_ticker}): {e}')
        return 0


def fetch_benchmark(conn, name: str, yf_ticker: str, period: str = '5y') -> int:
    """Fetch weekly closes for a benchmark index."""
    try:
        df = yf.download(
            yf_ticker,
            period=period,
            interval='1wk',
            progress=False,
            auto_adjust=True,
        )
        if df.empty:
            return 0

        if hasattr(df.columns, 'levels'):
            df.columns = df.columns.droplevel(1)

        rows = []
        for date_idx, row in df.iterrows():
            close = float(row.get('Close', 0) or 0)
            if close:
                rows.append((name, date_idx.strftime('%Y-%m-%d'), close))

        conn.executemany(
            """INSERT INTO benchmark_history (ticker, date, close)
               VALUES (?, ?, ?)
               ON CONFLICT(ticker, date) DO UPDATE SET close=excluded.close""",
            rows,
        )
        conn.commit()
        return len(rows)

    except Exception as e:
        logger.warning(f'Benchmark {name} ({yf_ticker}): {e}')
        return 0


def run(update_only: bool = False):
    conn = get_db()

    # --- Benchmarks first ---
    logger.info('Fetching benchmark indices…')
    for name, yf_ticker in BENCHMARKS.items():
        if update_only:
            last = conn.execute(
                'SELECT MAX(date) FROM benchmark_history WHERE ticker=?', (name,)
            ).fetchone()[0]
            if last and (datetime.now() - datetime.strptime(last, '%Y-%m-%d')).days < 3:
                logger.info(f'  {name}: up to date ({last})')
                continue
        n = fetch_benchmark(conn, name, yf_ticker)
        logger.info(f'  {name}: {n} rows')
        time.sleep(0.3)

    # --- ETFs ---
    etfs = conn.execute(
        'SELECT code, exchange FROM etfs ORDER BY fund_size_aud_millions DESC NULLS LAST'
    ).fetchall()

    logger.info(f'Fetching price history for {len(etfs)} ETFs…')
    ok = skipped = failed = 0

    for row in etfs:
        code, exchange = row['code'], row['exchange']
        yf_ticker = _yahoo_ticker(code, exchange)

        if update_only:
            last = conn.execute(
                'SELECT MAX(date) FROM price_history WHERE etf_code=?', (code,)
            ).fetchone()[0]
            if last and (datetime.now() - datetime.strptime(last, '%Y-%m-%d')).days < 3:
                skipped += 1
                continue

        n = fetch_etf_price_history(conn, code, yf_ticker)
        if n:
            logger.info(f'  {code}: {n} rows')
            ok += 1
        else:
            logger.debug(f'  {code}: no data (CXA or delisted)')
            failed += 1

        time.sleep(0.2)   # gentle rate limit

    logger.info(f'Done. {ok} ETFs fetched, {skipped} skipped, {failed} no data.')
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--update', action='store_true',
                        help='Only fetch ETFs missing or stale (> 3 days old)')
    args = parser.parse_args()
    run(update_only=args.update)
