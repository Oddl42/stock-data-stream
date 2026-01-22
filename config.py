#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
config.py - Zentrale Konfigurationsverwaltung mit Pydantic
"""

from pydantic_settings import BaseSettings
from typing import Optional
import os

class Settings(BaseSettings):
    """Zentrale Konfiguration für das Stock Data Dashboard"""
    
    # === Database ===
    DATABASE_URL: str = 'postgresql://stockuser:stockpass@localhost:5432/stockdata'
    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20
    DB_POOL_PRE_PING: bool = True
    
    # ✅ Optionale Legacy-Felder (falls in .env vorhanden, werden sie ignoriert)
    postgres_user: Optional[str] = None
    postgres_password: Optional[str] = None
    postgres_db: Optional[str] = None
    postgres_host: Optional[str] = None
    postgres_port: Optional[str] = None
    
    # === Massive API ===
    MASSIVE_API_KEY: str
    MASSIVE_BASE_URL: str = "https://api.polygon.io"
    API_TIMEOUT: int = 30
    API_MAX_RETRIES: int = 3
    API_RETRY_DELAY: int = 1  # Sekunden
    API_RATE_LIMIT_DELAY: int = 60  # Sekunden bei 429 Error
    
    # === Dashboard ===
    DEFAULT_INTERVAL: str = "1day"
    DEFAULT_DAYS: int = 90
    MAX_DATA_POINTS: int = 50000
    BULK_INSERT_CHUNK_SIZE: int = 1000
    
    # === Logging ===
    LOG_LEVEL: str = "INFO"
    LOG_FILE: Optional[str] = "stock_dashboard.log"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # === UI ===
    DASHBOARD_PORT: int = 5006
    DASHBOARD_TITLE: str = "📈 Stock Data Platform"
    
    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'
        case_sensitive = False
        extra = 'ignore'  # ✅ WICHTIG: Ignoriert unbekannte Felder aus .env

# Globale Settings-Instanz
settings = Settings()
