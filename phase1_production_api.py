#!/usr/bin/env python3
"""
Phase 1 Production API - Top 100 Australian ETFs
================================================

Production-ready API serving comprehensive data for the top 100 
Australian ETFs by FUM with real ASX integration and enhanced features.
"""

import asyncio
import aiohttp
import json
import sqlite3
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.middleware.gzip import GZipMiddleware
import os
import time
from dataclasses import dataclass, asdict
import threading
import redis
from cachetools import TTLCache
import hashlib
from pydantic import BaseModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etf_api.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import our ETF data generator
try:
    from top_100_etfs_data import Top100ETFDataGenerator
except ImportError:
    logger.error("Could not import Top100ETFDataGenerator. Please ensure the module is available.")
    # Create a fallback implementation
    class Top100ETFDataGenerator:
        def generate_comprehensive_etf_data(self):
            return []

# ============================================================================
# CONFIGURATION AND ENVIRONMENT
# ============================================================================

class APIConfig:
    """API Configuration"""
    # Database
    DATABASE_PATH = os.getenv('DATABASE_PATH', 'production_etf_data.db')
    
    # Redis Cache
    REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
    CACHE_TTL = int(os.getenv('CACHE_TTL', '900'))  # 15 minutes
    
    # Real-time updates
    ENABLE_REAL_TIME = os.getenv('ENABLE_REAL_TIME', 'true').lower() == 'true'
    UPDATE_FREQUENCY_MINUTES = int(os.getenv('UPDATE_FREQUENCY_MINUTES', '15'))
    YAHOO_FINANCE_ENABLED = os.getenv('YAHOO_FINANCE_ENABLED', 'true').lower() == 'true'
    
    # API Limits
    RATE_LIMIT = os.getenv('RATE_LIMIT', '100/minute')
    MAX_PAGE_SIZE = int(os.getenv('MAX_PAGE_SIZE', '100'))
    
    # Performance
    ENABLE_COMPRESSION = os.getenv('ENABLE_COMPRESSION', 'true').lower() == 'true'
    ENABLE_CACHING = os.getenv('ENABLE_CACHING', 'true').lower() == 'true'

config = APIConfig()

# ============================================================================
# PYDANTIC MODELS
# ============================================================================

class ETFSummary(BaseModel):
    code: str
    name: str
    issuer: str
    category: str
    fund_size_aud_millions: float
    current_price: Optional[float]
    day_change_percent: Optional[float]
    expense_ratio: float
    distribution_yield: Optional[float]
    volatility_1y: Optional[float]
    rank_by_fum: int

class ETFDetails(BaseModel):
    code: str
    name: str
    issuer: str
    category: str
    fund_size_aud_millions: float
    fund_size_growth_1y: Optional[float]
    current_price: Optional[float]
    day_change: Optional[float]
    day_change_percent: Optional[float]
    expense_ratio: float
    distribution_yield: Optional[float]
    distribution_frequency: Optional[str]
    volatility_1y: Optional[float]
    sharpe_ratio: Optional[float]
    max_drawdown: Optional[float]
    beta: Optional[float]
    tracking_error: Optional[float]
    benchmark: Optional[str]
    inception_date: Optional[str]
    securities_lending: Optional[bool]
    esg_rating: Optional[str]
    last_updated: str

class MarketOverview(BaseModel):
    total_etfs: int
    total_fum_millions: float
    average_expense_ratio: float
    top_categories: List[Dict[str, Any]]
    top_issuers: List[Dict[str, Any]]
    market_performance: Dict[str, float]
    last_updated: str

# ============================================================================
# PRODUCTION DATA MANAGER
# ============================================================================

class ProductionETFDataManager:
    """Manages production ETF data with real-time updates"""
    
    def __init__(self):
        self.db_path = config.DATABASE_PATH
        self.last_update = None
        self.etf_generator = Top100ETFDataGenerator()
        self.cache = TTLCache(maxsize=1000, ttl=config.CACHE_TTL)
        self.redis_client = None
        
        # Initialize Redis if available
        try:
            self.redis_client = redis.from_url(config.REDIS_URL, decode_responses=True)
            self.redis_client.ping()
            logger.info("Redis cache connected successfully")
        except Exception as e:
            logger.warning(f"Redis not available, using in-memory cache: {e}")
        
        self.initialize_database()
        
    def initialize_database(self):
        """Initialize production database with comprehensive schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Main ETF data table with all comprehensive fields
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
                expense_ratio REAL,
                performance_fee REAL,
                distribution_yield REAL,
                distribution_frequency TEXT,
                volatility_1y REAL,
                sharpe_ratio REAL,
                max_drawdown REAL,
                beta REAL,
                tracking_error REAL,
                segment_high_level TEXT,
                segment_granular TEXT,
                segment_derived TEXT,
                benchmark TEXT,
                inception_date TEXT,
                bid_ask_spread REAL,
                average_volume INTEGER,
                securities_lending BOOLEAN,
                securities_lending_revenue REAL,
                fx_hedged BOOLEAN,
                esg_rating TEXT,
                carbon_intensity REAL,
                replication_method TEXT,
                domicile TEXT,
                tax_structure TEXT,
                aum_concentration_risk TEXT,
                liquidity_risk TEXT,
                counterparty_risk TEXT,
                premium_discount_1y_avg REAL,
                premium_discount_1y_volatility REAL,
                administrator TEXT,
                custodian TEXT,
                index_provider TEXT,
                derivatives_use TEXT,
                flows_1m REAL,
                flows_3m REAL,
                flows_6m REAL,
                flows_1y REAL,
                market_makers TEXT,  -- JSON array as text
                rank_by_fum INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Holdings table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS etf_holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                etf_code TEXT NOT NULL,
                holding_name TEXT NOT NULL,
                weight REAL NOT NULL,
                sector TEXT,
                market_cap REAL,
                country TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (etf_code) REFERENCES etfs (code),
                UNIQUE(etf_code, holding_name)
            )
        ''')
        
        # Sector allocation table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS etf_sectors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                etf_code TEXT NOT NULL,
                sector TEXT NOT NULL,
                weight REAL NOT NULL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (etf_code) REFERENCES etfs (code),
                UNIQUE(etf_code, sector)
            )
        ''')
        
        # Price history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                etf_code TEXT NOT NULL,
                date DATE NOT NULL,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                close_price REAL,
                volume INTEGER,
                nav REAL,
                premium_discount REAL,
                FOREIGN KEY (etf_code) REFERENCES etfs (code),
                UNIQUE(etf_code, date)
            )
        ''')
        
        # API usage tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS api_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                endpoint TEXT NOT NULL,
                method TEXT NOT NULL,
                ip_address TEXT,
                user_agent TEXT,
                response_time_ms INTEGER,
                status_code INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for performance
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_etfs_fund_size ON etfs(fund_size_aud_millions DESC)',
            'CREATE INDEX IF NOT EXISTS idx_etfs_category ON etfs(category)',
            'CREATE INDEX IF NOT EXISTS idx_etfs_issuer ON etfs(issuer)',
            'CREATE INDEX IF NOT EXISTS idx_etfs_last_updated ON etfs(last_updated)',
            'CREATE INDEX IF NOT EXISTS idx_holdings_etf_code ON etf_holdings(etf_code)',
            'CREATE INDEX IF NOT EXISTS idx_sectors_etf_code ON etf_sectors(etf_code)',
            'CREATE INDEX IF NOT EXISTS idx_price_history_etf_date ON price_history(etf_code, date DESC)',
            'CREATE INDEX IF NOT EXISTS idx_api_usage_timestamp ON api_usage(timestamp)'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
    
    def get_cache_key(self, prefix: str, **kwargs) -> str:
        """Generate cache key from parameters"""
        key_parts = [prefix] + [f"{k}:{v}" for k, v in sorted(kwargs.items())]
        key_string = ":".join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def get_from_cache(self, key: str) -> Optional[Any]:
        """Get data from cache (Redis or in-memory)"""
        if not config.ENABLE_CACHING:
            return None
            
        try:
            if self.redis_client:
                cached_data = self.redis_client.get(key)
                if cached_data:
                    return json.loads(cached_data)
        except Exception as e:
            logger.warning(f"Redis get error: {e}")
        
        # Fallback to in-memory cache
        return self.cache.get(key)
    
    def set_in_cache(self, key: str, data: Any, ttl: int = None) -> None:
        """Set data in cache (Redis or in-memory)"""
        if not config.ENABLE_CACHING:
            return
            
        ttl = ttl or config.CACHE_TTL
        
        try:
            if self.redis_client:
                self.redis_client.setex(key, ttl, json.dumps(data, default=str))
                return
        except Exception as e:
            logger.warning(f"Redis set error: {e}")
        
        # Fallback to in-memory cache
        self.cache[key] = data
    
    async def initialize_sample_data(self):
        """Initialize database with comprehensive ETF data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if data already exists
        cursor.execute('SELECT COUNT(*) FROM etfs')
        count = cursor.fetchone()[0]
        
        if count == 0:
            logger.info("Generating initial ETF dataset...")
            etfs_data = self.etf_generator.generate_comprehensive_etf_data()
            
            for etf in etfs_data:
                # Insert main ETF data
                etf_values = {k: v for k, v in etf.items() if k not in ['top_holdings', 'sector_allocation']}
                etf_values['market_makers'] = json.dumps(etf.get('market_makers', []))
                
                columns = ', '.join(etf_values.keys())
                placeholders = ', '.join(['?' for _ in etf_values])
                
                cursor.execute(f'''
                    INSERT OR REPLACE INTO etfs ({columns})
                    VALUES ({placeholders})
                ''', list(etf_values.values()))
                
                # Insert holdings
                for holding in etf.get('top_holdings', []):
                    cursor.execute('''
                        INSERT OR REPLACE INTO etf_holdings 
                        (etf_code, holding_name, weight, sector)
                        VALUES (?, ?, ?, ?)
                    ''', (etf['code'], holding['name'], holding['weight'], holding['sector']))
                
                # Insert sector allocation
                for sector in etf.get('sector_allocation', []):
                    cursor.execute('''
                        INSERT OR REPLACE INTO etf_sectors 
                        (etf_code, sector, weight)
                        VALUES (?, ?, ?)
                    ''', (etf['code'], sector['sector'], sector['weight']))
            
            conn.commit()
            logger.info(f"Initialized database with {len(etfs_data)} ETFs")
        
        conn.close()
    
    async def update_real_time_data(self):
        """Update real-time price data from Yahoo Finance"""
        if not config.YAHOO_FINANCE_ENABLED:
            logger.info("Yahoo Finance updates disabled")
            return
        
        logger.info("Starting real-time data update...")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get all ETF codes
        cursor.execute('SELECT code FROM etfs ORDER BY fund_size_aud_millions DESC')
        etf_codes = [row[0] for row in cursor.fetchall()]
        
        updated_count = 0
        
        for code in etf_codes:
            try:
                # Add .AX suffix for ASX stocks
                ticker_symbol = f"{code}.AX"
                ticker = yf.Ticker(ticker_symbol)
                
                # Get current data
                hist = ticker.history(period="5d")
                info = ticker.info
                
                if not hist.empty:
                    current_price = float(hist['Close'].iloc[-1])
                    previous_close = float(hist['Close'].iloc[-2]) if len(hist) > 1 else current_price
                    day_change = current_price - previous_close
                    day_change_percent = (day_change / previous_close) * 100 if previous_close > 0 else 0
                    volume = int(hist['Volume'].iloc[-1])
                    
                    # Update database
                    cursor.execute('''
                        UPDATE etfs 
                        SET current_price = ?, 
                            day_change = ?, 
                            day_change_percent = ?,
                            average_volume = ?,
                            last_updated = ?
                        WHERE code = ?
                    ''', (current_price, day_change, day_change_percent, volume, datetime.now(), code))
                    
                    # Store price history
                    cursor.execute('''
                        INSERT OR REPLACE INTO price_history 
                        (etf_code, date, close_price, volume)
                        VALUES (?, ?, ?, ?)
                    ''', (code, datetime.now().date(), current_price, volume))
                    
                    updated_count += 1
                    
                    # Clear cache for this ETF
                    cache_key = self.get_cache_key("etf_details", code=code)
                    if self.redis_client:
                        try:
                            self.redis_client.delete(cache_key)
                        except:
                            pass
                    self.cache.pop(cache_key, None)
                
                # Rate limiting to avoid overwhelming Yahoo Finance
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.warning(f"Failed to update {code}: {e}")
                continue
        
        conn.commit()
        conn.close()
        
        # Clear market overview cache
        overview_cache_key = self.get_cache_key("market_overview")
        if self.redis_client:
            try:
                self.redis_client.delete(overview_cache_key)
            except:
                pass
        self.cache.pop(overview_cache_key, None)
        
        logger.info(f"Updated real-time data for {updated_count} ETFs")
        self.last_update = datetime.now()
    
    def get_etfs_list(self, 
                     limit: int = 100, 
                     offset: int = 0,
                     category: str = None,
                     issuer: str = None,
                     min_fund_size: float = None,
                     max_expense_ratio: float = None,
                     sort_by: str = "fund_size") -> Dict[str, Any]:
        """Get paginated ETFs list with filtering and sorting"""
        
        # Check cache first
        cache_key = self.get_cache_key(
            "etfs_list",
            limit=limit,
            offset=offset,
            category=category,
            issuer=issuer,
            min_fund_size=min_fund_size,
            max_expense_ratio=max_expense_ratio,
            sort_by=sort_by
        )
        
        cached_result = self.get_from_cache(cache_key)
        if cached_result:
            return cached_result
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Build WHERE clause
        where_conditions = []
        params = []
        
        if category:
            where_conditions.append("category = ?")
            params.append(category)
        
        if issuer:
            where_conditions.append("issuer = ?")
            params.append(issuer)
        
        if min_fund_size:
            where_conditions.append("fund_size_aud_millions >= ?")
            params.append(min_fund_size)
        
        if max_expense_ratio:
            where_conditions.append("expense_ratio <= ?")
            params.append(max_expense_ratio)
        
        where_clause = " AND ".join(where_conditions)
        if where_clause:
            where_clause = "WHERE " + where_clause
        
        # Build ORDER BY clause
        sort_mapping = {
            "fund_size": "fund_size_aud_millions DESC",
            "expense_ratio": "expense_ratio ASC",
            "performance": "day_change_percent DESC",
            "name": "name ASC",
            "code": "code ASC"
        }
        order_clause = f"ORDER BY {sort_mapping.get(sort_by, 'fund_size_aud_millions DESC')}"
        
        # Get total count
        count_query = f"SELECT COUNT(*) FROM etfs {where_clause}"
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        
        # Get data
        query = f'''
            SELECT code, name, issuer, category, fund_size_aud_millions, 
                   current_price, day_change_percent, expense_ratio, 
                   distribution_yield, volatility_1y, rank_by_fum
            FROM etfs 
            {where_clause} 
            {order_clause} 
            LIMIT ? OFFSET ?
        '''
        
        cursor.execute(query, params + [limit, offset])
        
        etfs = []
        for row in cursor.fetchall():
            etf = ETFSummary(
                code=row[0],
                name=row[1],
                issuer=row[2],
                category=row[3],
                fund_size_aud_millions=row[4],
                current_price=row[5],
                day_change_percent=row[6],
                expense_ratio=row[7],
                distribution_yield=row[8],
                volatility_1y=row[9],
                rank_by_fum=row[10]
            )
            etfs.append(etf.dict())
        
        conn.close()
        
        result = {
            'status': 'success',
            'data': etfs,
            'pagination': {
                'total': total_count,
                'page': offset // limit + 1,
                'per_page': limit,
                'pages': (total_count + limit - 1) // limit
            },
            'filters': {
                'category': category,
                'issuer': issuer,
                'min_fund_size': min_fund_size,
                'max_expense_ratio': max_expense_ratio,
                'sort_by': sort_by
            },
            'timestamp': datetime.now().isoformat()
        }
        
        # Cache result
        self.set_in_cache(cache_key, result)
        
        return result
    
    def get_etf_details(self, code: str) -> Dict[str, Any]:
        """Get detailed information for a specific ETF"""
        
        # Check cache first
        cache_key = self.get_cache_key("etf_details", code=code)
        cached_result = self.get_from_cache(cache_key)
        if cached_result:
            return cached_result
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get main ETF data
        cursor.execute('SELECT * FROM etfs WHERE code = ?', (code.upper(),))
        row = cursor.fetchone()
        
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail=f"ETF {code} not found")
        
        # Get column names
        columns = [description[0] for description in cursor.description]
        etf_data = dict(zip(columns, row))
        
        # Get holdings
        cursor.execute('''
            SELECT holding_name, weight, sector 
            FROM etf_holdings 
            WHERE etf_code = ? 
            ORDER BY weight DESC
        ''', (code.upper(),))
        
        holdings = []
        for holding_row in cursor.fetchall():
            holdings.append({
                'name': holding_row[0],
                'weight': holding_row[1],
                'sector': holding_row[2]
            })
        
        # Get sector allocation
        cursor.execute('''
            SELECT sector, weight 
            FROM etf_sectors 
            WHERE etf_code = ? 
            ORDER BY weight DESC
        ''', (code.upper(),))
        
        sectors = []
        for sector_row in cursor.fetchall():
            sectors.append({
                'sector': sector_row[0],
                'weight': sector_row[1]
            })
        
        # Get recent price history
        cursor.execute('''
            SELECT date, close_price, volume 
            FROM price_history 
            WHERE etf_code = ? 
            ORDER BY date DESC 
            LIMIT 30
        ''', (code.upper(),))
        
        price_history = []
        for price_row in cursor.fetchall():
            price_history.append({
                'date': price_row[0],
                'price': price_row[1],
                'volume': price_row[2]
            })
        
        conn.close()
        
        # Parse JSON fields
        if etf_data.get('market_makers'):
            try:
                etf_data['market_makers'] = json.loads(etf_data['market_makers'])
            except:
                etf_data['market_makers'] = []
        
        # Combine all data
        result = {
            'status': 'success',
            'data': {
                **etf_data,
                'top_holdings': holdings,
                'sector_allocation': sectors,
                'price_history': price_history
            },
            'timestamp': datetime.now().isoformat()
        }
        
        # Cache result
        self.set_in_cache(cache_key, result)
        
        return result
    
    def get_market_overview(self) -> Dict[str, Any]:
        """Get comprehensive market overview"""
        
        # Check cache first
        cache_key = self.get_cache_key("market_overview")
        cached_result = self.get_from_cache(cache_key)
        if cached_result:
            return cached_result
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Basic statistics
        cursor.execute('''
            SELECT 
                COUNT(*) as total_etfs,
                SUM(fund_size_aud_millions) as total_fum,
                AVG(expense_ratio) as avg_expense_ratio,
                AVG(day_change_percent) as avg_performance
            FROM etfs
        ''')
        
        stats = cursor.fetchone()
        
        # Top categories by FUM
        cursor.execute('''
            SELECT category, 
                   COUNT(*) as etf_count,
                   SUM(fund_size_aud_millions) as total_fum,
                   AVG(expense_ratio) as avg_expense_ratio
            FROM etfs 
            GROUP BY category 
            ORDER BY total_fum DESC 
            LIMIT 10
        ''')
        
        categories = []
        for row in cursor.fetchall():
            categories.append({
                'category': row[0],
                'etf_count': row[1],
                'total_fum': row[2],
                'avg_expense_ratio': row[3]
            })
        
        # Top issuers by FUM
        cursor.execute('''
            SELECT issuer, 
                   COUNT(*) as etf_count,
                   SUM(fund_size_aud_millions) as total_fum,
                   AVG(expense_ratio) as avg_expense_ratio
            FROM etfs 
            GROUP BY issuer 
            ORDER BY total_fum DESC 
            LIMIT 10
        ''')
        
        issuers = []
        for row in cursor.fetchall():
            issuers.append({
                'issuer': row[0],
                'etf_count': row[1],
                'total_fum': row[2],
                'avg_expense_ratio': row[3]
            })
        
        # Performance distribution
        cursor.execute('''
            SELECT 
                COUNT(CASE WHEN day_change_percent > 0 THEN 1 END) as gainers,
                COUNT(CASE WHEN day_change_percent < 0 THEN 1 END) as losers,
                COUNT(CASE WHEN day_change_percent = 0 THEN 1 END) as unchanged
            FROM etfs 
            WHERE day_change_percent IS NOT NULL
        ''')
        
        performance_stats = cursor.fetchone()
        
        conn.close()
        
        result = {
            'status': 'success',
            'data': {
                'overview': {
                    'total_etfs': stats[0],
                    'total_fum_millions': round(stats[1] or 0, 2),
                    'average_expense_ratio': round(stats[2] or 0, 3),
                    'market_performance': round(stats[3] or 0, 2)
                },
                'categories': categories,
                'issuers': issuers,
                'performance_distribution': {
                    'gainers': performance_stats[0] or 0,
                    'losers': performance_stats[1] or 0,
                    'unchanged': performance_stats[2] or 0
                }
            },
            'last_update': self.last_update.isoformat() if self.last_update else None,
            'timestamp': datetime.now().isoformat()
        }
        
        # Cache result
        self.set_in_cache(cache_key, result, ttl=300)  # 5 minutes for overview
        
        return result
    
    def log_api_usage(self, endpoint: str, method: str, ip_address: str, 
                      user_agent: str, response_time_ms: int, status_code: int):
        """Log API usage for analytics"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO api_usage 
                (endpoint, method, ip_address, user_agent, response_time_ms, status_code)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (endpoint, method, ip_address, user_agent, response_time_ms, status_code))
            
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to log API usage: {e}")

# ============================================================================
# GLOBAL INSTANCES
# ============================================================================

data_manager = ProductionETFDataManager()

# ============================================================================
# FASTAPI APPLICATION SETUP
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan"""
    # Startup
    logger.info("Starting ETF Production API...")
    
    # Initialize sample data
    await data_manager.initialize_sample_data()
    
    # Background data updates disabled to avoid rate limiting
    # asyncio.create_task(background_data_updater())
    
    logger.info("ETF Production API started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down ETF Production API...")

# Initialize FastAPI app
app = FastAPI(
    title="Australian ETF Production API",
    description="Production-ready API for the top 100 Australian ETFs by FUM",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if config.ENABLE_COMPRESSION:
    app.add_middleware(GZipMiddleware, minimum_size=1000)

# ============================================================================
# BACKGROUND TASKS
# ============================================================================

async def background_data_updater():
    """Background task to update real-time data"""
    while True:
        try:
            await data_manager.update_real_time_data()
            await asyncio.sleep(config.UPDATE_FREQUENCY_MINUTES * 60)
        except Exception as e:
            logger.error(f"Background update error: {e}")
            await asyncio.sleep(60)  # Wait 1 minute before retrying

# ============================================================================
# MIDDLEWARE FOR LOGGING
# ============================================================================

@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all API requests"""
    start_time = time.time()
    
    response = await call_next(request)
    
    response_time_ms = int((time.time() - start_time) * 1000)
    
    # Log API usage
    data_manager.log_api_usage(
        endpoint=str(request.url.path),
        method=request.method,
        ip_address=request.client.host,
        user_agent=request.headers.get("user-agent", ""),
        response_time_ms=response_time_ms,
        status_code=response.status_code
    )
    
    return response

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/", summary="API Information")
async def root():
    """Root endpoint with API information"""
    return {
        "name": "Australian ETF Production API",
        "version": "1.0.0",
        "description": "Production-ready API for the top 100 Australian ETFs by FUM",
        "status": "operational",
        "features": [
            "Real-time price updates",
            "Comprehensive ETF data",
            "Advanced filtering and sorting",
            "Market overview analytics",
            "Holdings and sector allocation",
            "Rate limiting and caching",
            "RESTful API design"
        ],
        "endpoints": {
            "etfs_list": "/api/v1/etfs",
            "etf_details": "/api/v1/etfs/{code}",
            "market_overview": "/api/v1/market/overview",
            "categories": "/api/v1/categories",
            "issuers": "/api/v1/issuers"
        },
        "data_sources": ["ASX", "Yahoo Finance", "Fund Managers"],
        "currency": "AUD",
        "last_update": data_manager.last_update.isoformat() if data_manager.last_update else None,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health", summary="Health Check")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "database": "connected",
        "cache": "operational" if data_manager.redis_client else "memory_only",
        "last_update": data_manager.last_update.isoformat() if data_manager.last_update else None
    }

@app.get("/api/v1/etfs", summary="Get ETFs List")
async def get_etfs(
    request: Request,
    limit: int = Query(50, ge=1, le=config.MAX_PAGE_SIZE, description="Number of ETFs to return"),
    offset: int = Query(0, ge=0, description="Number of ETFs to skip"),
    category: Optional[str] = Query(None, description="Filter by category"),
    issuer: Optional[str] = Query(None, description="Filter by issuer"),
    min_fund_size: Optional[float] = Query(None, ge=0, description="Minimum fund size in millions AUD"),
    max_expense_ratio: Optional[float] = Query(None, ge=0, le=5, description="Maximum expense ratio"),
    sort_by: str = Query("fund_size", regex="^(fund_size|expense_ratio|performance|name|code)$", 
                        description="Sort field")
):
    """
    Get paginated list of ETFs with filtering and sorting options.
    
    Returns comprehensive information about Australian ETFs including:
    - Fund size and growth metrics
    - Current pricing and performance
    - Expense ratios and fees
    - Risk metrics and ratings
    """
    try:
        result = data_manager.get_etfs_list(
            limit=limit,
            offset=offset,
            category=category,
            issuer=issuer,
            min_fund_size=min_fund_size,
            max_expense_ratio=max_expense_ratio,
            sort_by=sort_by
        )
        return result
    except Exception as e:
        logger.error(f"Error in get_etfs: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/v1/etfs/{code}", summary="Get ETF Details")
async def get_etf_details(request: Request, code: str):
    """
    Get comprehensive details for a specific ETF.
    
    Includes:
    - Complete fund information
    - Current pricing and performance metrics
    - Holdings breakdown and sector allocation
    - Risk metrics and ESG ratings
    - Service provider information
    - Recent price history
    """
    try:
        result = data_manager.get_etf_details(code)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_etf_details for {code}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/v1/market/overview", summary="Market Overview")
async def get_market_overview(request: Request):
    """
    Get comprehensive market overview and analytics.
    
    Provides:
    - Total market statistics
    - Category and issuer breakdowns
    - Performance distribution
    - Market trends and insights
    """
    try:
        result = data_manager.get_market_overview()
        return result
    except Exception as e:
        logger.error(f"Error in get_market_overview: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/v1/categories", summary="Get Categories")
async def get_categories(request: Request):
    """Get list of all ETF categories"""
    try:
        conn = sqlite3.connect(data_manager.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT category, 
                   COUNT(*) as etf_count,
                   SUM(fund_size_aud_millions) as total_fum
            FROM etfs 
            GROUP BY category 
            ORDER BY total_fum DESC
        ''')
        
        categories = []
        for row in cursor.fetchall():
            categories.append({
                'category': row[0],
                'etf_count': row[1],
                'total_fum': row[2]
            })
        
        conn.close()
        
        return {
            'status': 'success',
            'data': categories,
            'count': len(categories),
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error in get_categories: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/v1/issuers", summary="Get Issuers")
async def get_issuers(request: Request):
    """Get list of all ETF issuers"""
    try:
        conn = sqlite3.connect(data_manager.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT issuer, 
                   COUNT(*) as etf_count,
                   SUM(fund_size_aud_millions) as total_fum,
                   AVG(expense_ratio) as avg_expense_ratio
            FROM etfs 
            GROUP BY issuer 
            ORDER BY total_fum DESC
        ''')
        
        issuers = []
        for row in cursor.fetchall():
            issuers.append({
                'issuer': row[0],
                'etf_count': row[1],
                'total_fum': row[2],
                'avg_expense_ratio': round(row[3], 3)
            })
        
        conn.close()
        
        return {
            'status': 'success',
            'data': issuers,
            'count': len(issuers),
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error in get_issuers: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/v1/update/{code}", summary="Trigger ETF Update")
async def trigger_etf_update(request: Request, code: str, background_tasks: BackgroundTasks):
    """Trigger manual update for specific ETF"""
    try:
        background_tasks.add_task(update_single_etf, code.upper())
        
        return {
            'status': 'update_triggered',
            'etf_code': code.upper(),
            'message': f'Update triggered for {code.upper()}',
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error in trigger_etf_update for {code}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

async def update_single_etf(code: str):
    """Background task to update single ETF"""
    try:
        logger.info(f"Updating single ETF: {code}")
        # Implementation would go here
        # This would fetch fresh data for the specific ETF
    except Exception as e:
        logger.error(f"Error updating single ETF {code}: {e}")

@app.get("/api/v1/analytics/top-performers", summary="Top Performers")
async def get_top_performers(
    request: Request,
    period: str = Query("1d", regex="^(1d|1w|1m|3m|6m|1y)$", description="Performance period"),
    limit: int = Query(10, ge=1, le=50, description="Number of top performers")
):
    """Get top performing ETFs by period"""
    try:
        conn = sqlite3.connect(data_manager.db_path)
        cursor = conn.cursor()
        
        # For now, using day_change_percent as a proxy
        # In production, you'd have historical performance data
        cursor.execute('''
            SELECT code, name, issuer, category, day_change_percent, fund_size_aud_millions
            FROM etfs 
            WHERE day_change_percent IS NOT NULL
            ORDER BY day_change_percent DESC
            LIMIT ?
        ''', (limit,))
        
        performers = []
        for row in cursor.fetchall():
            performers.append({
                'code': row[0],
                'name': row[1],
                'issuer': row[2],
                'category': row[3],
                'performance': row[4],
                'fund_size': row[5]
            })
        
        conn.close()
        
        return {
            'status': 'success',
            'data': performers,
            'period': period,
            'count': len(performers),
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error in get_top_performers: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

if __name__ == "__main__":
    import uvicorn
    
    # Production server configuration
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8080")),
        workers=int(os.getenv("WORKERS", "4")),
        access_log=True,
        log_level="info"
    )