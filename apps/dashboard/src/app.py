#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 21 21:33:54 2026

@author: twi-dev
"""

from .hmi.ui import DashboardUI
from .hmi.backend import StockBackend
from .logic.controller import StockDashboardController

dashboard = StockDashboardController(DashboardUI(), StockBackend())
