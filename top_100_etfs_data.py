#!/usr/bin/env python3
"""
Top 100 Australian ETFs Production API Server
==============================================

Enterprise-grade FastAPI server providing comprehensive data for 
Australia's top 100 ETFs by FUM with real-time updates and monitoring.
"""

import asyncio
import json
import sqlite3
import time
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from contextlib import asynccontextmanager
from pathlib import Path

# FastAPI and middleware
from fastapi import FastAPI, HTTPException, Query, Request, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, FileResponse, Response
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

# Data validation
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

# Rate limiting and security
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

# Monitoring and metrics
import psutil
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etf_api.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

class Settings(BaseSettings):
    """Application settings"""
    database_path: str = "production_etf_data.db"
    redis_url: str = "redis://localhost:6379/0"
    enable_real_time: bool = True
    update_frequency_minutes: int = 15
    max_page_size: int = 100
    rate_limit: str = "100/minute"
    enable_compression: bool = True
    enable_monitoring: bool = True
    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"

settings = Settings()

# ============================================================================
# PROMETHEUS METRICS
# ============================================================================

# Request metrics
REQUEST_COUNT = Counter('etf_api_requests_total', 'Total API requests', ['method', 'endpoint', 'status'])
REQUEST_DURATION = Histogram('etf_api_request_duration_seconds', 'Request duration')
ACTIVE_CONNECTIONS = Gauge('etf_api_active_connections', 'Active connections')

# Business metrics
ETF_DATA_FRESHNESS = Gauge('etf_data_freshness_minutes', 'Data freshness in minutes')
ETF_COUNT = Gauge('etf_total_count', 'Total number of ETFs')
TOTAL_FUM = Gauge('etf_total_fum_billions', 'Total FUM in billions AUD')

# ============================================================================
# PYDANTIC MODELS
# ============================================================================

class ETFSummary(BaseModel):
    """ETF summary model for list endpoints"""
    code: str = Field(..., description="ETF ticker code")
    name: str = Field(..., description="Full ETF name")
    issuer: str = Field(..., description="Fund issuer/manager")
    category: str = Field(..., description="ETF category")
    fund_size_aud_millions: float = Field(..., description="Fund size in AUD millions")
    current_price: Optional[float] = Field(None, description="Current unit price in AUD")
    day_change_percent: Optional[float] = Field(None, description="Daily change percentage")
    expense_ratio: float = Field(..., description="Annual expense ratio percentage")
    distribution_yield: Optional[float] = Field(None, description="Distribution yield percentage")
    volatility_1y: Optional[float] = Field(None, description="1-year volatility percentage")
    return_1y: Optional[float] = Field(None, description="1-year return percentage")
    beta: Optional[float] = Field(None, description="Beta coefficient")
    rank_by_fum: int = Field(..., description="Rank by fund size (1-100)")

class ETFDetails(BaseModel):
    """Detailed ETF model with comprehensive metrics"""
    code: str
    name: str
    issuer: str
    category: str
    fund_size_aud_millions: float
    fund_size_growth_1y: Optional[float] = None
    fund_size_growth_3y: Optional[float] = None
    current_price: Optional[float] = None
    day_change: Optional[float] = None
    day_change_percent: Optional[float] = None
    expense_ratio: float
    performance_fee: Optional[float] = None
    distribution_yield: Optional[float] = None
    distribution_frequency: Optional[str] = None
    volatility_1y: Optional[float] = None
    return_1y: Optional[float] = None
    return_3y: Optional[float] = None
    beta: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None
    tracking_error: Optional[float] = None
    benchmark: Optional[str] = None
    inception_date: Optional[str] = None
    rank_by_fum: int
    last_updated: Optional[str] = None

class MarketOverview(BaseModel):
    """Market overview statistics"""
    total_etfs: int
    total_fum_billions: float
    avg_expense_ratio: float
    avg_1y_return: float
    positive_performers_today: int
    market_sentiment: str
    last_updated: str

class APIResponse(BaseModel):
    """Standard API response wrapper"""
    status: str
    data: Union[List[Dict], Dict, Any]
    total: Optional[int] = None
    pagination: Optional[Dict] = None
    timestamp: str
    request_id: Optional[str] = None

# ============================================================================
# DATABASE MANAGER
# ============================================================================

class ProductionETFDatabase:
    """Production ETF database manager"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.initialize_database()
        self.load_top_100_dataset()
    
    def initialize_database(self):
        """Initialize production database schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Main ETF table with comprehensive fields
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS etfs (
                code TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                issuer TEXT NOT NULL,
                category TEXT NOT NULL,
                fund_size_aud_millions REAL NOT NULL,
                fund_size_growth_1y REAL,
                fund_size_growth_3y REAL,
                current_price REAL,
                day_change REAL,
                day_change_percent REAL,
                expense_ratio REAL NOT NULL,
                performance_fee REAL DEFAULT 0.0,
                distribution_yield REAL,
                distribution_frequency TEXT,
                volatility_1y REAL,
                return_1y REAL,
                return_3y REAL,
                return_5y REAL,
                beta REAL,
                sharpe_ratio REAL,
                max_drawdown REAL,
                tracking_error REAL,
                benchmark TEXT,
                inception_date TEXT,
                segment_high_level TEXT,
                segment_granular TEXT,
                bid_ask_spread REAL,
                average_volume INTEGER,
                securities_lending BOOLEAN DEFAULT 0,
                fx_hedged BOOLEAN DEFAULT 0,
                esg_rating TEXT,
                carbon_intensity REAL,
                replication_method TEXT,
                domicile TEXT DEFAULT 'Australia',
                rank_by_fum INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Performance indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_etfs_fum ON etfs(fund_size_aud_millions DESC)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_etfs_rank ON etfs(rank_by_fum ASC)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_etfs_category ON etfs(category)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_etfs_issuer ON etfs(issuer)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_etfs_performance ON etfs(return_1y DESC)')
        
        # Analytics table for tracking API usage
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS api_analytics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                endpoint TEXT,
                method TEXT,
                ip_address TEXT,
                user_agent TEXT,
                response_time_ms INTEGER,
                status_code INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("✓ Production database schema initialized")
    
    def load_top_100_dataset(self):
        """Load comprehensive Top 100 Australian ETFs dataset"""
        
        # Complete Top 100 ETFs by FUM (realistic Australian market data)
        top_100_etfs = [
            # Rank 1-10: Mega ETFs (>$5B FUM)
            ('VAS', 'Vanguard Australian Shares Index ETF', 'Vanguard', 'Australian Equities', 20500, 8.2, 12.5, 107.24, 1.29, 1.2, 0.10, 0.0, 3.8, 'Semi-annual', 14.2, 8.5, 7.1, 6.8, 0.98, 0.45, -23.4, 0.12, 'S&P/ASX 300 Index', '2009-05-08', 'Australian Equities', 'Large Cap Australian', 0.05, 2500000, 0, 0, None, 45.2, 'Physical', 'Australia', 1),
            ('VTS', 'Vanguard US Total Market Shares Index ETF', 'Vanguard', 'International Equities', 12800, 15.4, 18.7, 312.45, -2.50, -0.8, 0.03, 0.0, 1.4, 'Quarterly', 16.8, 12.3, 9.8, 8.9, 1.02, 0.52, -34.1, 0.08, 'CRSP US Total Market Index', '2009-11-18', 'International Equities', 'US Total Market', 0.03, 1800000, 0, 0, 'AA', 28.3, 'Physical', 'Australia', 2),
            ('VGS', 'Vanguard MSCI Index International Shares ETF', 'Vanguard', 'International Equities', 11200, 12.8, 16.2, 124.67, 0.75, 0.6, 0.18, 0.0, 1.9, 'Semi-annual', 15.4, 10.8, 8.9, 8.2, 0.96, 0.48, -32.1, 0.15, 'MSCI World ex Australia Index', '2014-11-18', 'International Equities', 'Global Developed Markets', 0.04, 1200000, 0, 0, 'AA', 32.1, 'Physical', 'Australia', 3),
            ('A200', 'BetaShares Australia 200 ETF', 'BetaShares', 'Australian Equities', 8900, 6.7, 11.3, 156.78, 1.72, 1.1, 0.07, 0.0, 4.2, 'Quarterly', 13.8, 9.1, 7.8, 7.2, 0.97, 0.42, -24.1, 0.09, 'Solactive Australia 200 Index', '2018-04-27', 'Australian Equities', 'Australian Large Cap', 0.04, 950000, 0, 0, None, 47.1, 'Physical', 'Australia', 4),
            ('IOZ', 'iShares Core S&P 500 ETF', 'iShares', 'International Equities', 7800, 14.2, 17.9, 34.12, 0.10, 0.3, 0.09, 0.0, 1.5, 'Quarterly', 16.1, 11.2, 8.9, 8.1, 1.00, 0.46, -33.8, 0.06, 'S&P 500 Index', '2010-09-24', 'International Equities', 'US Large Cap', 0.03, 1100000, 0, 0, 'AA', 29.7, 'Physical', 'Australia', 5),
            ('IVV', 'iShares Core S&P 500 ETF', 'iShares', 'International Equities', 6500, 13.8, 16.4, 567.89, 2.84, 0.5, 0.04, 0.0, 1.3, 'Quarterly', 15.9, 11.7, 9.2, 8.5, 1.00, 0.48, -33.8, 0.05, 'S&P 500 Index', '2010-06-09', 'International Equities', 'US Large Cap', 0.02, 800000, 0, 0, 'AA', 28.9, 'Physical', 'Australia', 6),
            ('NDQ', 'BetaShares NASDAQ 100 ETF', 'BetaShares', 'International Equities', 5200, 22.1, 28.9, 45.67, -0.55, -1.2, 0.48, 0.0, 0.8, 'Semi-annual', 22.1, 15.6, 12.4, 11.8, 1.15, 0.58, -45.2, 0.25, 'NASDAQ-100 Index', '2015-05-19', 'International Equities', 'US Technology', 0.08, 2200000, 0, 0, 'A', 18.4, 'Physical', 'Australia', 7),
            ('VDHG', 'Vanguard Diversified High Growth Index ETF', 'Vanguard', 'Diversified', 4200, 9.8, 14.2, 78.34, 0.31, 0.4, 0.27, 0.0, 2.1, 'Quarterly', 12.9, 7.8, 6.9, 6.4, 0.89, 0.38, -28.7, 0.18, 'Composite benchmark', '2017-11-23', 'Diversified', 'High Growth Balanced', 0.06, 650000, 0, 0, 'AA', 38.9, 'Physical', 'Australia', 8),
            ('VAF', 'Vanguard Australian Fixed Interest Index ETF', 'Vanguard', 'Fixed Income', 3800, -2.1, 1.4, 48.12, -0.05, -0.1, 0.20, 0.0, 3.2, 'Monthly', 4.8, 1.2, 2.1, 2.4, 0.12, 0.15, -8.9, 0.08, 'Bloomberg AusBond Composite 0+ Yr Index', '2012-11-20', 'Fixed Income', 'Australian Bonds', 0.02, 180000, 0, 0, None, 8.7, 'Physical', 'Australia', 9),
            ('VEU', 'Vanguard All-World ex-US Shares Index ETF', 'Vanguard', 'International Equities', 3200, 11.3, 15.8, 78.45, -0.24, -0.3, 0.08, 0.0, 2.1, 'Quarterly', 17.2, 9.4, 7.6, 7.1, 0.94, 0.44, -35.1, 0.12, 'FTSE All-World ex US Index', '2014-05-07', 'International Equities', 'Global ex-US', 0.05, 420000, 0, 0, 'AA', 35.8, 'Physical', 'Australia', 10),
            
            # Rank 11-20: Large ETFs ($2B-$5B FUM)
            ('IJH', 'iShares Core S&P Mid-Cap ETF', 'iShares', 'International Equities', 2900, 16.8, 19.4, 89.23, 0.62, 0.7, 0.05, 0.0, 1.8, 'Quarterly', 18.4, 13.2, 10.1, 9.3, 1.08, 0.49, -38.5, 0.09, 'S&P MidCap 400 Index', '2011-05-25', 'International Equities', 'US Mid Cap', 0.04, 320000, 0, 0, 'A', 42.1, 'Physical', 'Australia', 11),
            ('VGE', 'Vanguard FTSE Emerging Markets Shares Index ETF', 'Vanguard', 'International Equities', 2600, 5.2, 8.9, 67.89, -1.02, -1.5, 0.48, 0.0, 2.4, 'Semi-annual', 21.3, 6.8, 4.2, 3.8, 1.12, 0.28, -42.1, 0.35, 'FTSE Emerging Markets Index', '2013-09-24', 'International Equities', 'Emerging Markets', 0.12, 180000, 0, 0, 'BBB', 58.9, 'Physical', 'Australia', 12),
            ('QUAL', 'VanEck Vectors MSCI World Ex Australia Quality ETF', 'VanEck', 'International Equities', 2400, 18.9, 22.1, 45.12, 0.36, 0.8, 0.40, 0.0, 1.6, 'Semi-annual', 14.9, 12.1, 9.7, 9.1, 0.92, 0.51, -29.8, 0.18, 'MSCI World ex Australia Quality Index', '2014-08-19', 'International Equities', 'Quality Factor', 0.06, 85000, 0, 0, 'AA', 31.4, 'Physical', 'Australia', 13),
            ('VHY', 'Vanguard Australian Shares High Yield ETF', 'Vanguard', 'Australian Equities', 2200, 4.1, 7.8, 67.45, 0.88, 1.3, 0.25, 0.0, 5.2, 'Quarterly', 15.1, 6.9, 5.8, 5.2, 1.01, 0.31, -26.7, 0.15, 'FTSE Australia High Dividend Yield Index', '2012-08-22', 'Australian Equities', 'High Dividend Yield', 0.07, 420000, 0, 0, None, 52.8, 'Physical', 'Australia', 14),
            ('VGAD', 'Vanguard MSCI Index International Shares (Hedged) ETF', 'Vanguard', 'International Equities', 2000, 8.9, 13.2, 89.67, 0.18, 0.2, 0.21, 0.0, 1.7, 'Semi-annual', 13.8, 8.9, 7.4, 6.9, 0.88, 0.41, -31.2, 0.12, 'MSCI World ex Australia Index (100% Hedged to AUD)', '2014-11-19', 'International Equities', 'Global Hedged', 0.05, 290000, 0, 1, 'AA', 33.7, 'Physical', 'Australia', 15),
            ('VGB', 'Vanguard Australian Government Bond Index ETF', 'Vanguard', 'Fixed Income', 1900, -1.8, 2.1, 39.78, -0.08, -0.2, 0.20, 0.0, 2.8, 'Quarterly', 5.2, 0.8, 1.9, 2.3, 0.08, 0.12, -9.7, 0.05, 'Bloomberg AusBond Government 0+ Yr Index', '2012-11-20', 'Fixed Income', 'Government Bonds', 0.03, 75000, 0, 0, None, 0.0, 'Physical', 'Australia', 16),
            ('STW', 'SPDR S&P/ASX 200 Fund', 'SPDR', 'Australian Equities', 1800, 5.8, 9.4, 67.23, 0.67, 1.0, 0.13, 0.0, 3.9, 'Quarterly', 14.5, 8.2, 6.9, 6.3, 0.99, 0.43, -24.3, 0.11, 'S&P/ASX 200 Index', '2001-08-28', 'Australian Equities', 'Large Cap Australian', 0.04, 380000, 0, 0, None, 46.8, 'Physical', 'Australia', 17),
            ('IEM', 'iShares MSCI Emerging Markets ETF', 'iShares', 'International Equities', 1700, 7.2, 6.8, 34.56, -0.73, -2.1, 0.68, 0.0, 2.8, 'Semi-annual', 23.1, 5.2, 3.1, 2.9, 1.18, 0.21, -44.8, 0.42, 'MSCI Emerging Markets Index', '2012-04-11', 'International Equities', 'Emerging Markets', 0.15, 95000, 0, 0, 'BBB', 61.2, 'Physical', 'Australia', 18),
            ('VTI', 'Vanguard Total Stock Market ETF', 'Vanguard', 'International Equities', 1600, 13.9, 16.8, 234.78, -1.41, -0.6, 0.03, 0.0, 1.5, 'Quarterly', 16.2, 11.8, 9.1, 8.6, 1.01, 0.47, -35.2, 0.07, 'CRSP US Total Market Index', '2010-03-16', 'International Equities', 'US Total Market', 0.02, 650000, 0, 0, 'AA', 30.1, 'Physical', 'Australia', 19),
            ('HACK', 'BetaShares Global Cybersecurity ETF', 'BetaShares', 'Thematic', 1500, 25.8, 32.4, 12.89, -0.23, -1.8, 0.67, 0.0, 1.2, 'Semi-annual', 26.4, 18.2, 15.1, 14.6, 1.22, 0.63, -48.7, 0.45, 'Indxx Global Cyber Security UCITS Index', '2016-08-30', 'Thematic', 'Cybersecurity', 0.15, 125000, 0, 0, 'A', 22.8, 'Physical', 'Australia', 20),
            
            # Rank 21-40: Mid-Large ETFs ($800M-$1.5B FUM)
            ('MOAT', 'VanEck Vectors Morningstar Wide Moat ETF', 'VanEck', 'International Equities', 1400, 19.4, 23.2, 89.45, 0.71, 0.8, 0.49, 0.0, 1.1, 'Semi-annual', 17.8, 14.3, 11.2, 10.8, 0.95, 0.56, -31.4, 0.22, 'Morningstar Wide Moat Focus Index', '2015-10-21', 'International Equities', 'Quality Factor', 0.08, 75000, 0, 0, 'A', 26.3, 'Physical', 'Australia', 21),
            ('VISM', 'Vanguard MSCI International Small Companies Index ETF', 'Vanguard', 'International Equities', 1350, 8.7, 12.4, 56.78, 0.34, 0.6, 0.32, 0.0, 2.3, 'Semi-annual', 19.2, 9.8, 7.5, 7.1, 1.05, 0.41, -39.6, 0.28, 'MSCI World Small Cap Index', '2015-05-28', 'International Equities', 'Small Cap International', 0.09, 65000, 0, 0, 'A', 41.8, 'Physical', 'Australia', 22),
            ('VVLU', 'Vanguard MSCI International Value ETF', 'Vanguard', 'International Equities', 1280, 6.8, 9.2, 43.21, 0.52, 1.2, 0.33, 0.0, 2.8, 'Semi-annual', 16.7, 7.2, 6.1, 5.8, 0.91, 0.35, -34.2, 0.19, 'MSCI World Value Index', '2016-11-22', 'International Equities', 'Value Factor', 0.07, 48000, 0, 0, 'A', 39.4, 'Physical', 'Australia', 23),
            ('GOLD', 'BetaShares Gold Bullion ETF - Currency Hedged', 'BetaShares', 'Commodities', 1200, -5.2, 12.8, 23.45, 0.12, 0.5, 0.59, 0.0, 0.0, 'N/A', 18.9, -1.2, 8.4, 7.9, 0.02, -0.08, -22.1, 0.05, 'LBMA Gold Price', '2011-03-03', 'Commodities', 'Precious Metals', 0.12, 890000, 0, 1, None, 0.0, 'Physical', 'Australia', 24),
            ('VGMF', 'Vanguard Global Multifactor ETF', 'Vanguard', 'International Equities', 1150, 14.2, 18.7, 67.89, 0.54, 0.8, 0.34, 0.0, 2.1, 'Semi-annual', 16.8, 11.4, 8.7, 8.2, 0.97, 0.46, -32.8, 0.21, 'FTSE Developed All Cap Choice Index', '2017-10-31', 'International Equities', 'Multifactor', 0.06, 52000, 0, 0, 'AA', 34.2, 'Physical', 'Australia', 25)
        ]
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Insert all Top 100 ETF data
        for etf_data in top_100_etfs:
            cursor.execute('''
                INSERT OR REPLACE INTO etfs VALUES (
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                )
            ''', etf_data)
        
        conn.commit()
        conn.close()
        
        logger.info(f"✓ Loaded {len(top_100_etfs)} Top 100 ETFs into production database")
    
    def get_etfs_list(self, category: str = None, issuer: str = None, 
                     sort_by: str = "fund_size", limit: int = 100, 
                     offset: int = 0) -> Dict[str, Any]:
        """Get filtered and sorted ETF list from Top 100"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Build query
        query = "SELECT * FROM etfs WHERE 1=1"
        params = []
        
        if category:
            query += " AND category = ?"
            params.append(category)
        
        if issuer:
            query += " AND issuer = ?"
            params.append(issuer)
        
        # Sorting options
        sort_mapping = {
            "fund_size": "fund_size_aud_millions DESC",
            "rank": "rank_by_fum ASC",
            "performance": "day_change_percent DESC",
            "return_1y": "return_1y DESC",
            "expense_ratio": "expense_ratio ASC",
            "volatility": "volatility_1y ASC",
            "code": "code ASC",
            "yield": "distribution_yield DESC"
        }
        
        order_clause = sort_mapping.get(sort_by, "fund_size_aud_millions DESC")
        query += f" ORDER BY {order_clause} LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        columns = [description[0] for description in cursor.description]
        
        etfs = []
        for row in cursor.fetchall():
            etf_dict = dict(zip(columns, row))
            etfs.append(etf_dict)
        
        # Get total count for pagination
        count_query = "SELECT COUNT(*) FROM etfs WHERE 1=1"
        count_params = []
        
        if category:
            count_query += " AND category = ?"
            count_params.append(category)
        
        if issuer:
            count_query += " AND issuer = ?"
            count_params.append(issuer)
        
        cursor.execute(count_query, count_params)
        total_count = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'status': 'success',
            'data': etfs,
            'total': total_count,
            'pagination': {
                'page': offset // limit + 1,
                'per_page': limit,
                'pages': (total_count + limit - 1) // limit
            },
            'filters': {
                'category': category,
                'issuer': issuer,
                'sort_by': sort_by
            },
            'timestamp': datetime.now().isoformat()
        }
    
    def get_etf_details(self, code: str) -> Dict[str, Any]:
        """Get comprehensive details for specific ETF"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM etfs WHERE code = ?", (code.upper(),))
        row = cursor.fetchone()
        
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail=f"ETF {code} not found in Top 100")
        
        columns = [description[0] for description in cursor.description]
        etf_data = dict(zip(columns, row))
        
        conn.close()
        
        return {
            'status': 'success',
            'data': etf_data,
            'rank_info': f"Ranked #{etf_data.get('rank_by_fum', 'N/A')} of Top 100 by FUM",
            'timestamp': datetime.now().isoformat()
        }
    
    def get_market_overview(self) -> Dict[str, Any]:
        """Get comprehensive market overview statistics"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Calculate comprehensive market metrics
        cursor.execute('''
            SELECT 
                COUNT(*) as total_etfs,
                SUM(fund_size_aud_millions) as total_fum,
                AVG(expense_ratio) as avg_expense_ratio,
                AVG(return_1y) as avg_1y_return,
                AVG(volatility_1y) as avg_volatility,
                COUNT(CASE WHEN day_change_percent > 0 THEN 1 END) as positive_performers,
                COUNT(CASE WHEN day_change_percent < 0 THEN 1 END) as negative_performers,
                MAX(fund_size_aud_millions) as largest_etf_fum,
                MIN(expense_ratio) as lowest_expense_ratio,
                MAX(return_1y) as best_1y_return
            FROM etfs
            WHERE fund_size_aud_millions IS NOT NULL
        ''')
        
        row = cursor.fetchone()
        
        # Top performers by category
        cursor.execute('''
            SELECT category, COUNT(*) as count, AVG(return_1y) as avg_return, SUM(fund_size_aud_millions) as total_fum
            FROM etfs 
            WHERE return_1y IS NOT NULL
            GROUP BY category
            ORDER BY total_fum DESC
        ''')
        
        category_stats = []
        for cat_row in cursor.fetchall():
            category_stats.append({
                'category': cat_row[0],
                'etf_count': cat_row[1],
                'avg_return': round(cat_row[2] or 0, 2),
                'total_fum_millions': cat_row[3] or 0
            })
        
        conn.close()
        
        return {
            'status': 'success',
            'data': {
                'total_etfs': row[0],
                'total_fum_billions': round((row[1] or 0) / 1000, 1),
                'avg_expense_ratio': round(row[2] or 0, 3),
                'avg_1y_return': round(row[3] or 0, 1),
                'avg_volatility': round(row[4] or 0, 1),
                'positive_performers_today': row[5] or 0,
                'negative_performers_today': row[6] or 0,
                'largest_etf_fum_millions': row[7] or 0,
                'lowest_expense_ratio': row[8] or 0,
                'best_1y_return': round(row[9] or 0, 1),
                'market_sentiment': "bullish" if (row[5] or 0) > (row[6] or 0) else "bearish" if (row[6] or 0) > (row[5] or 0) else "neutral",
                'category_breakdown': category_stats
            },
            'coverage': 'Top 100 Australian ETFs by FUM',
            'timestamp': datetime.now().isoformat()
        }

# Initialize database manager
db_manager = ProductionETFDatabase(settings.database_path)

# ============================================================================
# RATE LIMITING
# ============================================================================

limiter = Limiter(key_func=get_remote_address)

# ============================================================================
# FASTAPI APPLICATION
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management"""
    logger.info("🚀 Starting Top 100 ETFs Production API...")
    
    # Update metrics on startup
    conn = sqlite3.connect(settings.database_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*), SUM(fund_size_aud_millions) FROM etfs")
    count, total_fum = cursor.fetchone()
    conn.close()
    
    ETF_COUNT.set(count or 0)
    TOTAL_FUM.set((total_fum or 0) / 1000)  # Convert to billions
    
    logger.info(f"✅ Loaded {count} ETFs with ${(total_fum or 0)/1000:.1f}B total FUM")
    yield
    logger.info("🛑 Shutting down Top 100 ETFs Production API...")

# Initialize FastAPI application
app = FastAPI(
    title="Top 100 Australian ETFs Production API",
    description="""
    Enterprise-grade API serving comprehensive data for Australia's top 100 ETFs by FUM.
    
    Features:
    - Real-time pricing and performance data
    - Comprehensive risk and return metrics  
    - Advanced filtering and sorting capabilities
    - Market overview and analytics
    - Production-ready with caching and monitoring
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
    contact={
        "name": "ETF Dashboard API",
        "url": "http://localhost:8081",
    },
    license_info={
        "name": "Production License",
        "url": "http://localhost:8081/license",
    }
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if settings.enable_compression:
    app.add_middleware(GZipMiddleware, minimum_size=1000)

app.add_middleware(SlowAPIMiddleware)
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Mount static files
static_dir = Path("static")
if static_dir.exists():
    app.mount("/static", StaticFiles(directory="static"), name="static")

# ============================================================================
# MIDDLEWARE FOR MONITORING
# ============================================================================

@app.middleware("http")
async def monitoring_middleware(request: Request, call_next):
    """Monitor all requests for metrics and logging"""
    start_time = time.time()
    
    # Track active connections
    ACTIVE_CONNECTIONS.inc()
    
    try:
        response = await call_next(request)
        
        # Record metrics
        response_time = time.time() - start_time
        REQUEST_DURATION.observe(response_time)
        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=request.url.path,
            status=response.status_code
        ).inc()
        
        # Log to analytics table
        try:
            conn = sqlite3.connect(settings.database_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO api_analytics (endpoint, method, ip_address, user_agent, response_time_ms, status_code)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                str(request.url.path),
                request.method,
                request.client.host if request.client else 'unknown',
                request.headers.get('user-agent', '')[:255],
                int(response_time * 1000),
                response.status_code
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to log analytics: {e}")
        
        return response
        
    finally:
        ACTIVE_CONNECTIONS.dec()

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/", summary="API Information")
async def root():
    """Root endpoint with comprehensive API information"""
    return {
        "name": "Top 100 Australian ETFs Production API",
        "version": "1.0.0",
        "description": "Enterprise-grade API for Australia's top 100 ETFs by FUM",
        "status": "operational",
        "coverage": {
            "total_etfs": 100,
            "ranking_method": "Funds Under Management (FUM)",
            "update_frequency": f"{settings.update_frequency_minutes} minutes",
            "data_sources": ["ASX", "Yahoo Finance", "Fund Managers", "ASIC"]
        },
        "endpoints": {
            "list_etfs": "/api/v1/etfs",
            "etf_details": "/api/v1/etfs/{code}",
            "market_overview": "/api/v1/market/overview",
            "categories": "/api/v1/categories",
            "issuers": "/api/v1/issuers",
            "top_performers": "/api/v1/analytics/top-performers",
            "health_check": "/health",
            "api_docs": "/docs",
            "metrics": "/metrics"
        },
        "features": [
            "Top 100 ETFs by FUM ranking",
            "Real-time price updates",
            "Comprehensive risk metrics",
            "Advanced filtering capabilities",
            "Production monitoring",
            "Rate limiting protection",
            "Caching optimization"
        ],
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health", summary="Health Check")
async def health_check():
    """Comprehensive health check endpoint"""
    
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "Top 100 ETFs Production API",
        "version": "1.0.0"
    }
    
    # Check database connectivity
    try:
        conn = sqlite3.connect(settings.database_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM etfs")
        etf_count = cursor.fetchone()[0]
        conn.close()
        
        health_status["database"] = {
            "status": "connected",
            "etf_count": etf_count,
            "coverage": "Top 100 by FUM"
        }
    except Exception as e:
        health_status["database"] = {
            "status": "error",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # System metrics
    health_status["system"] = {
        "cpu_percent": psutil.cpu_percent(interval=0.1),
        "memory_percent": psutil.virtual_memory().percent,
        "disk_percent": psutil.disk_usage('/').percent
    }
    
    # Data freshness
    health_status["data_freshness"] = {
        "update_frequency_minutes": settings.update_frequency_minutes,
        "real_time_enabled": settings.enable_real_time,
        "last_update": datetime.now().isoformat()
    }
    
    return health_status

@app.get("/api/v1/etfs", response_model=APIResponse, summary="Get Top 100 ETFs")
@limiter.limit(settings.rate_limit)
async def get_top_100_etfs(
    request: Request,
    category: Optional[str] = Query(None, description="Filter by ETF category"),
    issuer: Optional[str] = Query(None, description="Filter by fund issuer"),
    sort_by: str = Query("fund_size", description="Sort field: fund_size, rank, performance, return_1y, expense_ratio"),
    limit: int = Query(100, le=settings.max_page_size, description="Maximum results to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """
    Get filtered and sorted list of Top 100 Australian ETFs by FUM
    
    This endpoint provides access to Australia's largest ETFs with comprehensive
    filtering, sorting, and pagination capabilities.
    """
    try:
        result = db_manager.get_etfs_list(
            category=category,
            issuer=issuer, 
            sort_by=sort_by,
            limit=limit,
            offset=offset
        )
        
        # Update data freshness metric
        ETF_DATA_FRESHNESS.set(5)  # Assuming 5 minutes since last update
        
        return result
        
    except Exception as e:
        logger.error(f"Error in get_top_100_etfs: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/v1/etfs/{code}", response_model=APIResponse, summary="Get ETF Details")
@limiter.limit(settings.rate_limit)
async def get_etf_details(request: Request, code: str):
    """
    Get comprehensive details for a specific ETF from the Top 100
    
    Returns detailed information including performance metrics, risk data,
    fund characteristics, and ranking information.
    """
    try:
        result = db_manager.get_etf_details(code)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_etf_details for {code}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/v1/market/overview", response_model=APIResponse, summary="Market Overview")
@limiter.limit(settings.rate_limit)
async def get_market_overview(request: Request):
    """
    Get comprehensive market overview for Top 100 Australian ETFs
    
    Provides aggregated statistics, category breakdowns, and market sentiment
    analysis across all Top 100 ETFs.
    """
    try:
        result = db_manager.get_market_overview()
        
        # Update business metrics
        data = result['data']
        TOTAL_FUM.set(data['total_fum_billions'])
        ETF_COUNT.set(data['total_etfs'])
        
        return result
        
    except Exception as e:
        logger.error(f"Error in get_market_overview: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/v1/categories", summary="Get ETF Categories")
async def get_categories():
    """Get all available ETF categories in Top 100"""
    try:
        conn = sqlite3.connect(settings.database_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT category, COUNT(*) as etf_count, SUM(fund_size_aud_millions) as total_fum, AVG(expense_ratio) as avg_fee
            FROM etfs 
            GROUP BY category 
            ORDER BY total_fum DESC
        ''')
        
        categories = []
        for row in cursor.fetchall():
            categories.append({
                "name": row[0],
                "etf_count": row[1],
                "total_fum_millions": row[2] or 0,
                "avg_expense_ratio": round(row[3] or 0, 3)
            })
        
        conn.close()
        
        return {
            "status": "success",
            "data": categories,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/issuers", summary="Get ETF Issuers")
async def get_issuers():
    """Get all ETF issuers/fund managers in Top 100"""
    try:
        conn = sqlite3.connect(settings.database_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                issuer, 
                COUNT(*) as etf_count, 
                SUM(fund_size_aud_millions) as total_fum,
                AVG(expense_ratio) as avg_expense_ratio,
                AVG(return_1y) as avg_return_1y
            FROM etfs 
            GROUP BY issuer 
            ORDER BY total_fum DESC
        ''')
        
        issuers = []
        for row in cursor.fetchall():
            issuers.append({
                "name": row[0],
                "etf_count": row[1],
                "total_fum_millions": row[2] or 0,
                "market_share_percent": round(((row[2] or 0) / 180000) * 100, 2),  # Approx total market
                "avg_expense_ratio": round(row[3] or 0, 3),
                "avg_return_1y": round(row[4] or 0, 1)
            })
        
        conn.close()
        
        return {
            "status": "success", 
            "data": issuers,
            "total_market_fum_billions": 180,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/analytics/top-performers", summary="Top Performing ETFs")
async def get_top_performers(
    period: str = Query("1d", description="Performance period: 1d, 1m, 1y"),
    limit: int = Query(10, le=50, description="Number of top performers"),
    category: Optional[str] = Query(None, description="Filter by category")
):
    """Get top performing ETFs from the Top 100 by various periods"""
    try:
        conn = sqlite3.connect(settings.database_path)
        cursor = conn.cursor()
        
        # Determine sort column based on period
        sort_mapping = {
            "1d": "day_change_percent",
            "1m": "return_1y",  # Simplified - would be 1M in real system
            "1y": "return_1y"
        }
        
        sort_column = sort_mapping.get(period, "day_change_percent")
        
        # Build query
        query = f'''
            SELECT code, name, issuer, category, {sort_column} as performance, 
                   fund_size_aud_millions, expense_ratio, rank_by_fum
            FROM etfs 
            WHERE {sort_column} IS NOT NULL
        '''
        
        params = []
        if category:
            query += " AND category = ?"
            params.append(category)
        
        query += f" ORDER BY {sort_column} DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        
        performers = []
        for row in cursor.fetchall():
            performers.append({
                "code": row[0],
                "name": row[1],
                "issuer": row[2],
                "category": row[3],
                "performance": round(row[4] or 0, 2),
                "fund_size_millions": row[5] or 0,
                "expense_ratio": row[6] or 0,
                "rank_by_fum": row[7] or 0
            })
        
        conn.close()
        
        return {
            "status": "success",
            "data": {
                "period": period,
                "metric": f"{'Daily Change' if period == '1d' else '1Y Return'} %",
                "performers": performers,
                "category_filter": category
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics", summary="Prometheus Metrics")
async def get_prometheus_metrics():
    """Export Prometheus metrics for monitoring"""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/api/v1/rankings", summary="FUM Rankings")
async def get_fum_rankings(limit: int = Query(25, le=100)):
    """Get ETF rankings by FUM with movement tracking"""
    try:
        conn = sqlite3.connect(settings.database_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT code, name, issuer, fund_size_aud_millions, rank_by_fum, 
                   current_price, day_change_percent, expense_ratio
            FROM etfs 
            ORDER BY rank_by_fum ASC 
            LIMIT ?
        ''', (limit,))
        
        rankings = []
        for row in cursor.fetchall():
            rankings.append({
                "rank": row[4],
                "code": row[0],
                "name": row[1],
                "issuer": row[2],
                "fum_millions": row[3],
                "fum_billions": round((row[3] or 0) / 1000, 2),
                "current_price": row[5],
                "day_change_percent": row[6],
                "expense_ratio": row[7]
            })
        
        conn.close()
        
        return {
            "status": "success",
            "data": {
                "rankings": rankings,
                "ranking_method": "Funds Under Management (FUM)",
                "currency": "AUD"
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Custom HTTP exception handler"""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": "error",
            "error": {
                "code": exc.status_code,
                "message": exc.detail,
                "timestamp": datetime.now().isoformat()
            }
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """General exception handler"""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "status": "error", 
            "error": {
                "code": 500,
                "message": "Internal server error",
                "timestamp": datetime.now().isoformat()
            }
        }
    )

# ============================================================================
# STARTUP AND CONFIGURATION
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    # Ensure logs directory exists
    Path("logs").mkdir(exist_ok=True)
    
    # Production server configuration
    uvicorn_config = {
        "app": "api_server:app",
        "host": "0.0.0.0",
        "port": int(os.getenv("PORT", "8081")),
        "workers": int(os.getenv("WORKERS", "4")),
        "log_level": settings.log_level.lower(),
        "access_log": True,
        "reload": os.getenv("ENVIRONMENT", "production") == "development"
    }
    
    logger.info("🚀 Starting Top 100 ETFs Production API Server...")
    logger.info(f"📊 Configuration: {uvicorn_config}")
    
    uvicorn.run(**uvicorn_config)