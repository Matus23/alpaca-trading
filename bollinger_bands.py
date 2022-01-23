import config
import requests
import yfinance as yf
import pandas_datareader.data as web
import numpy as np
import opstrat as op
import matplotlib.pyplot as plt
import pandas as pd
import re
import time


import alpaca_trade_api as tradeapi
from alpaca_trade_api.stream import Stream
from alpaca_trade_api.common import URL
from alpaca_trade_api.rest import REST, TimeFrame, TimeFrameUnit
import nest_asyncio
import logging
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor


# Script inspired by:
# https://medium.com/codex/algorithmic-trading-with-bollinger-bands-in-python-1b0a00c9ef99
# https://towardsdatascience.com/backtesting-bollinger-bands-on-apple-stock-using-quantopian-6be2e4824f43


def get_bands(data, window_size=20, num_std_devs=1):
    std = data.rolling(window_size, min_periods=1).std()
    sma = data.rolling(window_size, min_periods=1).mean()
    
    upper_band = sma + num_std_devs * std
    lower_band = sma - num_std_devs * std
    
    return lower_band, upper_band

def bollinger_bands(data, window_size=20, num_std_dev=1):
    pass

