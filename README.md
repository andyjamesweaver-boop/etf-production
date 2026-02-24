# ETF Production

Australian ETF data platform. Scrapes, stores, and serves data on ASX and Cboe-listed ETFs — prices, returns, fees, fund size, issuer details, and more.

## Overview

The pipeline collects data from multiple sources and consolidates it into a single SQLite database:

| Source | What it provides |
|--------|-----------------|
| ASX monthly Excel report | Full ASX ETF list, fees, AUM, flows, returns |
| Cboe monthly Excel report | ASX + CXA-exclusive ETFs, price, spreads, returns, inception date |
| Issuer websites | Holdings, sector allocations, distribution yield |
| ASX JSON API | Live prices for ASX-listed ETFs |

**Current coverage:** ~472 ETFs (432 ASX, 40 CXA-exclusive)

## Project Structure

```
etf-production/
├── scrapers/
│   ├── run_all.py          # CLI orchestrator
│   ├── config.py           # URLs, rate limits, issuer mappings
│   ├── db_writer.py        # COALESCE-based upserts
│   ├── asx_report_scraper.py
│   ├── cboe_scraper.py
│   ├── asx_etf_scraper.py
│   ├── issuer_scrapers.py  # BetaShares, VanEck, Vanguard, iShares, SPDR, Global X, CXA issuers
│   ├── master_list.py      # FUM rankings and issuer stats
│   └── base_scraper.py     # HTTP fetch with rate limiting and retries
├── setup_db.py             # Schema creation
├── phase1_production_api.py  # FastAPI server
└── requirements.txt
```

## Setup

```bash
pip install -r requirements.txt
python3 setup_db.py
```

## Running the Pipeline

```bash
# Run everything
python3 -m scrapers.run_all

# Run a specific source
python3 -m scrapers.run_all --source asx_report
python3 -m scrapers.run_all --source cboe
python3 -m scrapers.run_all --source asx_api
python3 -m scrapers.run_all --source issuers
python3 -m scrapers.run_all --source master

# Verbose logging
python3 -m scrapers.run_all --verbose
```

## Data Fields

Each ETF record can include:

- `code`, `name`, `exchange` (ASX / CXA), `issuer`, `issuer_url`
- `asset_class`, `management_fee`, `inception_date`
- `current_price`, `year_high`, `year_low`
- `bid_ask_spread_pct`
- `fund_size_aud_millions`, `fund_flow_1m`
- `return_1m`, `return_1y`, `return_3y`, `return_5y` (percent)
- `distribution_yield`
- `rank_by_fum`

## Deployment

Docker Compose + Nginx configuration included. See `docker-compose.yml` and `deploy.sh`.
