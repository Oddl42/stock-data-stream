#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 14 22:08:37 2026

@author: twi-dev
"""

# Test-Script in Spyder
import sys
print(f"Python: {sys.executable}")
print(f"Version: {sys.version}")

# Package-Tests
import psycopg2
import sqlalchemy
import pandas
import requests
from dotenv import load_dotenv

print("\n✅ Alle Pakete erfolgreich importiert!")

# Environment-Info
import os
print(f"\nConda Environment: {os.environ.get('CONDA_DEFAULT_ENV', 'Nicht gefunden')}")
