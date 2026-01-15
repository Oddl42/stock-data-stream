#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 14 22:01:24 2026

@author: twi-dev
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

# Database URL
DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://stockuser:stockpass@localhost:5432/stockdata'
)

# Engine erstellen
engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True  # Verbindung testen vor Nutzung
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

@contextmanager
def get_db_session():
    """Context Manager für DB Sessions"""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

def test_connection():
    """Testet die Datenbankverbindung"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print(f"✅ Datenbankverbindung erfolgreich!")
            print(f"   PostgreSQL Version: {result.fetchone()[0]}")
            
            # TimescaleDB Version prüfen
            result = conn.execute(text("SELECT extversion FROM pg_extension WHERE extname='timescaledb';"))
            print(f"   TimescaleDB Version: {result.fetchone()[0]}")
        return True
    except Exception as e:
        print(f"❌ Datenbankverbindung fehlgeschlagen: {e}")
        return False
