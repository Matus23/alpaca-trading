import config
import pickle
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


def get_historical_bar_data(ticker, 
                        frequency=TimeFrame.Minute,
                        start_date="2022-01-01",
                        end_date="2022-01-01"):
    """
    Used to retrieve historical market data from Alpaca Markets
    Args:
        ticker: String 
            e.g. 'AAPL'
        frequency: api's custom TimeFrame
            e.g. TimeFrame.Hour
        start_date: String
            in format "YYYY-MM-DD"
        end_date: String
            in format "YYYY-MM-DD"
    Returns:
        data: pandas df 
            data from specified time period with given frequency
    """
    api = tradeapi.REST(
            key_id=config.key_id, 
            secret_key=config.secret_key, 
            base_url=config.base_url,
            api_version='v2')

    data = api.get_bars(ticker, frequency, start_date, end_date, adjustment='raw').df

    return data


def store_data(data, file_name):
    """
    Stored given dataframe into pickle
    Args:
        data: pandas df
        file_name: String
            File name under which the data should be stored
    """
    data.to_pickle(file_name)


def get_file_name(ticker, 
                frequency,
                start_date,
                end_date):
    """
    Args:
        ticker: String 
            e.g. 'AAPL'
        frequency: api's custom TimeFrame
            e.g. TimeFrame.Hour
        start_date: String
            in format "YYYY-MM-DD"
        end_date: String
            in format "YYYY-MM-DD"
    Returns:
        file_name: String
            name of the file
    """
    if frequency == TimeFrame.Minute:
        frequency = "minute"
    elif frequency == TimeFrame.Hour:
        frequency = "hour"
    elif frequency == TimeFrame.Day:
        frequency = "day"
    else:
        raise TypeError("Incorrect type given for data frequency.")

    file_name = "data/{0}_{1}_{2}_{3}".format(ticker, frequency, start_date, end_date)

    return file_name


def load_data(file_name):
    """
    Loads data from a stored pickle into df
    Args:
        file_name: string
            Path to pickle file
    Returns:
        data: pandas df 
            Loaded data from specified pickle file
    """
    data = pd.read_pickle(file_name)

    return data


def preprocess_historical_data(data):
    """
    Transforms a df containing bar stock data (i.e. open, close, high, 
    low, volume, vwap) into a pandas series containing only 
    volume-weighted average price (vwap)
    Args:
        data: pandas df 
            Contains historical bar data
    Returns:
        data: pandas series
            Contains the volume weighted average price
    """
    return data['vwap']


def process_backtesting_results(vwap_data, buy_prices, sell_prices, signals):
    df                = pd.DataFrame(vwap_data)
    df['buy_price']  = buy_prices
    df['sell_price'] = sell_prices
    df['signal']     = signals

    return df


def get_bands(data, window_size=20, num_std=1):
    """
    Calculates the lower and upper bollinger bands
    Args:
        data: pandas series
            historical price stock data in series format
    Returns:
        lower_bands: pandas series
            lower bollinger bands over hist data
        upper_bands: pandas series
            upper bollinger bands over hist data
    """
    std = data.rolling(window_size, min_periods=1).std()
    sma = data.rolling(window_size, min_periods=1).mean()
    

    lower_bands = sma - num_std * std
    upper_bands = sma + num_std * std
    
    return lower_bands, upper_bands


def handle_buy(signal, vwap, buys, sells, signals):
    """
    Handles buy signal
    Args:
        vwap: float
            volume weighted average price of the bar being processed
        signal: int in range (-1, 0, 1)
            signal of the bar currently being processed
        buys: list 
            contains prices of the financial instrument at 
            timestamps when the buy order should be executed
        sells: list
            contains prices of the financial instrument at
            timestamps when the sell order should be executed
        signals: list 
            contains signals that specify whether a buy or 
            sell should be executed
    Returns: 
        1 - indication that buy action would take place
    """
    if signal != 1:
        buys.append(vwap)
        sells.append(np.nan)
        signal = 1
        signals.append(signal)
    else:
        buys.append(np.nan)
        sells.append(np.nan)
        signals.append(0)

    return 1


def handle_sell(signal, vwap, buys, sells, signals):
    """
    Handles sell signal
    Args:
        vwap: float
            volume weighted average price of the bar being processed
        signal: int in range (-1, 0, 1)
            signal of the bar currently being processed
        buys: list 
            contains prices of the financial instrument at 
            timestamps when the buy order should be executed
        sells: list
            contains prices of the financial instrument at
            timestamps when the sell order should be executed
        signals: list 
            contains signals that specify whether a buy or 
            sell should be executed
    Returns: 
        -1 - indication that sell action would take place
    """
    if signal != -1:
        buys.append(np.nan)
        sells.append(vwap)
        signal = -1
        signals.append(signal)
    else:
        buys.append(np.nan)
        sells.append(np.nan)
        signals.append(0)

    return signal


def handle_neutral(buys, sells, signals):
    """
    Handles neutral signal. This means neither buy nor sell is triggered.
    Args:
        buys: list 
            contains prices of the financial instrument at 
            timestamps when the buy order should be executed
        sells: list
            contains prices of the financial instrument at
            timestamps when the sell order should be executed
        signals: list 
            contains signals that specify whether a buy or 
            sell should be executed
    """
    buys.append(np.nan)
    sells.append(np.nan)
    signals.append(0)


def backtest_bollinger_bands(data, window_size=20, num_std=1):
    """
    Executes bollinger bands against historical data
    Args:
        data: pandas df 
            contains historical bar data
        window_size: int, optional
            the size of window for the moving average
        num_std: int, optional
            num of standard deviations specifying width of the bands
    Returns:
        bb_results: pandas df
            specifies signals when buy and sell should occur based 
            on BB strategy
    """
    # get vwap data into series format
    vwaps = preprocess_historical_data(data)
    # get bollinger bands
    lower_band, upper_band = get_bands(vwaps, window_size, num_std)

    print(f'lower band: {lower_band}')

    buys    = []
    sells   = []
    signals = []
    signal  = 0

    for i in range(0, len(vwaps)):
        # buy
        if vwaps[i-1] > lower_band[i-1] and vwaps[i] < lower_band[i]:
            signal = handle_buy(signal, vwaps[i], buys, sells, signals)
        # sell
        elif vwaps[i-1] < upper_band[i-1] and vwaps[i] > upper_band[i]:
            signal = handle_sell(signal, vwaps[i], buys, sells, signals)
        # stay put
        else:
            handle_neutral(buys, sells, signals)

    # transform the results from lists into a dataframe    
    bb_results = process_backtesting_results(vwaps, buys, sells, signals)

    return bb_results


def buy_and_hold(data):
    """
    Executes a buy and hold strategy on historical bars data
    data: pandas df
        bar data of a certain instrument
    """
    buy_price = data[0]
    sell_price = data[len(data)-1]

    return buy_price, sell_price