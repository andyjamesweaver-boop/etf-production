#!/usr/bin/env python3
"""
Ingest ASX ETPReportData.xlsx into the etp_list and etp_monthly tables.

Usage:
    python3 -m scrapers.etp_historical_ingest
    python3 -m scrapers.etp_historical_ingest --path /path/to/ETPReportData.xlsx
    python3 -m scrapers.etp_historical_ingest --list-only    # only ingest ETPList sheet
    python3 -m scrapers.etp_historical_ingest --data-only    # only ingest ETPData sheet
"""

import argparse
import io
import logging
import os
import sqlite3
import sys
from datetime import datetime, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scrapers.config import DB_PATH
from scrapers.db_writer import get_connection

logger = logging.getLogger(__name__)

# Default path relative to the project's parent directory (etf-dashboard)
_DEFAULT_XLSX = os.path.join(
    os.path.expanduser('~'), 'etf-dashboard', 'ETPReportData.xlsx'
)


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    f = _safe_float(val)
    return int(f) if f is not None else None


def _safe_date(val) -> str | None:
    if val is None:
        return None
    if isinstance(val, (datetime, date)):
        return val.strftime('%Y-%m-%d') if hasattr(val, 'strftime') else str(val)[:10]
    s = str(val).strip()
    return s if s else None


def _safe_bool(val) -> int | None:
    if val is None:
        return None
    if isinstance(val, bool):
        return int(val)
    s = str(val).strip().lower()
    if s in ('yes', 'true', '1', 'y'):
        return 1
    if s in ('no', 'false', '0', 'n'):
        return 0
    return None


def ingest_etp_list(conn, ws) -> int:
    """Ingest the ETPList sheet into etp_list table."""
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return 0

    headers = [str(h or '').strip() for h in rows[0]]

    def col(row, name):
        try:
            return row[headers.index(name)]
        except (ValueError, IndexError):
            return None

    inserted = 0
    for row in rows[1:]:
        ticker = str(col(row, 'Ticker') or '').strip().upper()
        if not ticker:
            continue

        data = {
            'ticker':               ticker,
            'type':                 str(col(row, 'Type') or '').strip() or None,
            'name':                 str(col(row, 'Name') or '').strip() or None,
            'segment':              str(col(row, 'Segment') or '').strip() or None,
            'sub_segment':          str(col(row, 'Sub-Segment') or '').strip() or None,
            'investment_style':     str(col(row, 'Investment Style') or '').strip() or None,
            'smart_beta':           str(col(row, 'Smart Beta') or '').strip() or None,
            'leveraged_inverse':    str(col(row, 'Leveraged/Inverse') or '').strip() or None,
            'replication_method':   str(col(row, 'Replication Method') or '').strip() or None,
            'esg':                  str(col(row, 'ESG') or '').strip() or None,
            'reference_benchmark':  str(col(row, 'Reference Benchmark') or '').strip() or None,
            'issuer':               str(col(row, 'Issuer') or '').strip() or None,
            'trim_issuer':          str(col(row, 'TrimIssuer') or '').strip() or None,
            'full_issuer_name':     str(col(row, 'FullIssuerName') or '').strip() or None,
            'investment_manager':   str(col(row, 'InvestmentManager') or '').strip() or None,
            'disclosure':           str(col(row, 'Disclosure') or '').strip() or None,
            'admission_date':       _safe_date(col(row, 'Admission Date')),
            'last_list_date':       _safe_date(col(row, 'LastListDate')),
            'is_listed':            _safe_int(col(row, 'IsListed')),
            'iress_code':           str(col(row, 'IRESSCode') or '').strip() or None,
            'bloomberg':            str(col(row, 'Bloomberg') or '').strip() or None,
            'asset_class':          str(col(row, 'Asset Class') or '').strip() or None,
            'strategy':             str(col(row, 'Strategy') or '').strip() or None,
            'country':              str(col(row, 'Country') or '').strip() or None,
            'feeder_fund':          str(col(row, 'FeederFund') or '').strip() or None,
            'fixed_income':         _safe_bool(col(row, 'Fixed Income?')),
            'structure':            str(col(row, 'Structure') or '').strip() or None,
            'master_fund_ticker':   str(col(row, 'MasterFundTicker') or '').strip() or None,
            'registry':             str(col(row, 'Registry') or '').strip() or None,
            'lead_market_maker':    str(col(row, 'LeadMarketMaker') or '').strip() or None,
            'income_investing':     _safe_bool(col(row, 'IncomeInvesting?')),
            'website':              str(col(row, 'Website') or '').strip() or None,
            'notes':                str(col(row, 'Notes') or '').strip() or None,
        }

        fields = {k: v for k, v in data.items() if k != 'ticker' and v is not None}
        if not fields:
            continue

        cols = ['ticker'] + list(fields.keys()) + ['last_updated']
        placeholders = ['?'] * len(cols)
        values = [ticker] + list(fields.values()) + [datetime.utcnow().isoformat()]
        set_parts = [f"{c} = excluded.{c}" for c in list(fields.keys()) + ['last_updated']]

        conn.execute(
            f"INSERT INTO etp_list ({','.join(cols)}) VALUES ({','.join(placeholders)}) "
            f"ON CONFLICT(ticker) DO UPDATE SET {','.join(set_parts)}",
            values
        )
        inserted += 1

    conn.commit()
    return inserted


def ingest_etp_data(conn, ws) -> int:
    """Ingest the ETPData sheet into etp_monthly table."""
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return 0

    # Normalise headers: strip whitespace and newlines
    raw_headers = rows[0]
    headers = [str(h or '').replace('\n', ' ').strip() for h in raw_headers]

    def col(row, name):
        try:
            return row[headers.index(name)]
        except (ValueError, IndexError):
            return None

    # Map messy column names
    COL = {
        'date':                 'Date',
        'code':                 'ASX \nCode',
        'mer':                  '  MER (% p.a)',
        'transacted_value':     'Transacted Value ($)',
        'transacted_volume':    'Transacted Volume',
        'num_trades':           'Number\n of Trades',
        'monthly_liquidity_pct':'Monthly Liquidity %',
        'spread_pct':           '% Spread*',
        'bid_depth':            "Bid Depth  \n(A$'000s)**",
        'ask_depth':            "Ask Depth  \n(A$'000s)**",
        'last_price':           'Last',
        'year_high':            'Year High',
        'year_low':             'Year Low',
        'distribution_yield':   'Historical Distribution Yield',
        'return_1m':            '1 Month Total Return',
        'return_1y':            '1 Year Total Return',
        'return_3y':            '3 Year Total Return (ann.)',
        'return_5y':            '5 Year Total Return (ann.)',
        'market_cap':           'MktCap',
        'market_cap_change':    'MktCapChange',
        'funds_flow':           'FundsFlow',
        'spread_quartile':      'SpreadQuartile',
        'total_units':          'TotalUnits',
        'mer_fee':              'MERFee',
        'mer_quartile':         'MERQuartile',
        'is_cdi':               'IsCDI',
        'usd_mc':               'USDMC',
        'chess_units':          'CHESSUnits',
        'chess_holders':        'CHESSHolders',
        'avg_chess_units':      'AvgCHESSUnits',
        'avg_chess_holding':    'AvgCHESSHolding',
        'chess_mc':             'CHESSMC',
        'mc_difference':        'MCDifference',
        'chess_funds_flow':     'CHESSFundsFlow',
        'chess_mc_change':      'CHESSMCChange',
        'mc_diff_pct':          'MCDiff%',
        'mc_change_pct':        'MCChange%',
        'chess_mc_change_pct':  'CHESSMCChange%',
        'ff_change_pct':        'FFChange%',
        'ff_chess_change_pct':  'FFCHESSChange%',
        'market_cap_band':      'MktCapBand',
        'funds_flow_band':      'FundsFlowBand',
        'admission_month':      'AdmissionMonth?',
        'high_low':             'HighLow',
    }

    # Resolve column indices once
    col_idx: dict[str, int] = {}
    for field, raw_name in COL.items():
        norm = raw_name.replace('\n', ' ').strip()
        if norm in headers:
            col_idx[field] = headers.index(norm)
        else:
            logger.debug(f"Column not found: {raw_name!r}")

    def get(row, field):
        idx = col_idx.get(field)
        return row[idx] if idx is not None and idx < len(row) else None

    inserted = 0
    batch = []
    BATCH_SIZE = 1000

    db_cols = [
        'date', 'code', 'mer', 'transacted_value', 'transacted_volume', 'num_trades',
        'monthly_liquidity_pct', 'spread_pct', 'bid_depth', 'ask_depth', 'last_price',
        'year_high', 'year_low', 'distribution_yield', 'return_1m', 'return_1y',
        'return_3y', 'return_5y', 'market_cap', 'market_cap_change', 'funds_flow',
        'spread_quartile', 'total_units', 'mer_fee', 'mer_quartile', 'is_cdi', 'usd_mc',
        'chess_units', 'chess_holders', 'avg_chess_units', 'avg_chess_holding', 'chess_mc',
        'mc_difference', 'chess_funds_flow', 'chess_mc_change', 'mc_diff_pct',
        'mc_change_pct', 'chess_mc_change_pct', 'ff_change_pct', 'ff_chess_change_pct',
        'market_cap_band', 'funds_flow_band', 'admission_month', 'high_low',
    ]
    placeholders = ','.join(['?'] * len(db_cols))
    set_parts = ','.join(f"{c}=excluded.{c}" for c in db_cols if c not in ('date', 'code'))
    sql = (
        f"INSERT INTO etp_monthly ({','.join(db_cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(date, code) DO UPDATE SET {set_parts}"
    )

    for row in rows[1:]:
        raw_date = get(row, 'date')
        if not raw_date:
            continue
        row_date = _safe_date(raw_date)
        if not row_date:
            continue

        code = str(get(row, 'code') or '').strip().upper()
        if not code:
            continue

        # Convert admission_month: 'Yes'/'No' → 1/0
        adm = get(row, 'admission_month')
        adm_int = None
        if adm is not None:
            adm_int = 1 if str(adm).strip().lower() in ('yes', 'true', '1') else 0

        values = (
            row_date,
            code,
            _safe_float(get(row, 'mer')),
            _safe_float(get(row, 'transacted_value')),
            _safe_float(get(row, 'transacted_volume')),
            _safe_int(get(row, 'num_trades')),
            _safe_float(get(row, 'monthly_liquidity_pct')),
            _safe_float(get(row, 'spread_pct')),
            _safe_float(get(row, 'bid_depth')),
            _safe_float(get(row, 'ask_depth')),
            _safe_float(get(row, 'last_price')),
            _safe_float(get(row, 'year_high')),
            _safe_float(get(row, 'year_low')),
            _safe_float(get(row, 'distribution_yield')),
            _safe_float(get(row, 'return_1m')),
            _safe_float(get(row, 'return_1y')),
            _safe_float(get(row, 'return_3y')),
            _safe_float(get(row, 'return_5y')),
            _safe_float(get(row, 'market_cap')),
            _safe_float(get(row, 'market_cap_change')),
            _safe_float(get(row, 'funds_flow')),
            str(get(row, 'spread_quartile') or '').strip() or None,
            _safe_float(get(row, 'total_units')),
            _safe_float(get(row, 'mer_fee')),
            str(get(row, 'mer_quartile') or '').strip() or None,
            _safe_int(get(row, 'is_cdi')),
            _safe_float(get(row, 'usd_mc')),
            _safe_float(get(row, 'chess_units')),
            _safe_int(get(row, 'chess_holders')),
            _safe_float(get(row, 'avg_chess_units')),
            _safe_float(get(row, 'avg_chess_holding')),
            _safe_float(get(row, 'chess_mc')),
            _safe_float(get(row, 'mc_difference')),
            _safe_float(get(row, 'chess_funds_flow')),
            _safe_float(get(row, 'chess_mc_change')),
            _safe_float(get(row, 'mc_diff_pct')),
            _safe_float(get(row, 'mc_change_pct')),
            _safe_float(get(row, 'chess_mc_change_pct')),
            _safe_float(get(row, 'ff_change_pct')),
            _safe_float(get(row, 'ff_chess_change_pct')),
            str(get(row, 'market_cap_band') or '').strip() or None,
            str(get(row, 'funds_flow_band') or '').strip() or None,
            adm_int,
            _safe_float(get(row, 'high_low')),
        )
        batch.append(values)

        if len(batch) >= BATCH_SIZE:
            conn.executemany(sql, batch)
            conn.commit()
            inserted += len(batch)
            batch = []
            logger.debug(f"Inserted {inserted} rows so far...")

    if batch:
        conn.executemany(sql, batch)
        conn.commit()
        inserted += len(batch)

    return inserted


def run_ingest(xlsx_path: str, list_only: bool = False, data_only: bool = False) -> dict:
    try:
        import openpyxl
    except ImportError:
        raise RuntimeError("openpyxl is required: pip install openpyxl")

    xlsx_path = os.path.normpath(xlsx_path)
    if not os.path.exists(xlsx_path):
        raise FileNotFoundError(f"File not found: {xlsx_path}")

    logger.info(f"Loading {xlsx_path}...")
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)

    conn = get_connection(DB_PATH)

    results = {}

    if not data_only and 'ETPList' in wb.sheetnames:
        logger.info("Ingesting ETPList...")
        n = ingest_etp_list(conn, wb['ETPList'])
        results['etp_list'] = n
        logger.info(f"  ETPList: {n} rows upserted")

    if not list_only and 'ETPData' in wb.sheetnames:
        logger.info("Ingesting ETPData (this may take a moment)...")
        n = ingest_etp_data(conn, wb['ETPData'])
        results['etp_monthly'] = n
        logger.info(f"  ETPData: {n} rows upserted")

    conn.close()
    return results


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s',
                        datefmt='%H:%M:%S')

    parser = argparse.ArgumentParser(description='Ingest ETPReportData.xlsx into the database')
    parser.add_argument('--path', default=_DEFAULT_XLSX,
                        help='Path to ETPReportData.xlsx')
    parser.add_argument('--list-only', action='store_true',
                        help='Only ingest the ETPList sheet')
    parser.add_argument('--data-only', action='store_true',
                        help='Only ingest the ETPData sheet')
    args = parser.parse_args()

    results = run_ingest(args.path, list_only=args.list_only, data_only=args.data_only)
    for table, count in results.items():
        print(f"  {table}: {count:,} rows")


if __name__ == '__main__':
    main()
