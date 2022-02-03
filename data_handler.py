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

# script inspired by: https://github.com/alpacahq/alpaca-trade-api-python


def get_historical_bar_data(ticker, 
                        frequency=TimeFrame.Minute,
                        start_date="2022-01-01",
                        end_date="2022-01-01"):
    """
        Used to retrieve historical markete data from Alpaca Markets
        Args:
            ticker - e.g. 'AAPL'
            frequency - in TimeFrame format, e.g. TimeFrame.Hour
            start_date - in format "YYYY-MM-DD"
            end_date - in format "YYYY-MM-DD"
        Returns:
            data in a dataframe format
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
            data - data in datarame format
            file_name - the file name under which the data should be stored
    """
    data.to_pickle(file_name)


def get_file_name(ticker, 
                frequency,
                start_date,
                end_date):
    """
        Args:
            ticker - e.g. 'AAPL'
            frequency - in TimeFrame format, e.g. TimeFrame.Hour
            start_date - in format "YYYY-MM-DD"
            end_date - in format "YYYY-MM-DD"
        Returns:
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
            file_name - path to pickle file
        Returns:
            data - df of loaded data from pickle 
    """
    data = pd.read_pickle(file_name)

    return data


async def print_trade(t):
    print('trade', t)


async def print_quote(q):
    print('quote', q)


async def handle_bar(bar):
    print('bar', bar)


async def print_daily_bar(bar):
    print('daily bar', bar)

async def print_crypto_trade(t):
    print('crypto trade', t)


async def print_crypto_quote(q):
    print('crypto quote', q)


async def print_crypto_bar(bar):
    api = tradeapi.REST(
            key_id=config.key_id, 
            secret_key=config.secret_key, 
            base_url=config.base_url,
            api_version='v2')

    api.submit_order(
            symbol="BTCUSD",
            qty=0.01,
            side='buy',
            type='market',
            time_in_force='gtc'
        )

    print('crypto bars', bar)


async def print_crypto_daily_bar(daily_bar):
    print('crypto daily bar', daily_bar)


async def print_trade_update(trade_update):
    print('was in trade update')
    print('trade update', trade_update)


async def on_trade_updates(data):
    print('on_trade_updates')
    print(f'trade_updates {data}')


def start_stream(ticker, callback):
    """
        Starts a connection to retrieve real time data via Alpaca Markets API
        Free mode retrieves data from iex exchange
        Args:
            ticker   - ticker specifying stock to be retrieved. Can be a string 
                    or a list of strings containing multiple tickers
            callback - callback function to be called. Can be a single callback
                    or a list of callbacks 
                    Callback what type of data is to be retrieved.
                    Options: (1) trade
                             (2) quote
                             (3) bar
                             (4) crypto trade
    """
    try:
        loop = asyncio.get_event_loop()
        loop.set_debug(True)
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    global stream
    stream = Stream(config.key_id, 
                config.secret_key,
                base_url=config.base_url,
                data_feed='iex')

    if type(ticker) == list or type(callback) == list:
        start_multiple_subscriptions(ticker, callback)
    else:
        start_single_subscription(ticker, callback)

    stream.run()


def start_single_subscription(ticker, callback):
    # stock
    if callback == print_trade:
        print(callback)
        stream.subscribe_trades(callback, ticker)
    elif callback == print_quote:
        print(callback)
        stream.subscribe_quotes(callback, ticker)
    elif callback == handle_bar:
        print(callback)
        stream.subscribe_bars(callback, ticker)
    if callback == print_trade_update:
        print(callback)
        stream.subscribe_trade_updates(on_trade_updates)
    elif callback == print_daily_bar:
        print(callback)
        stream.subscribe_daily_bars(callback, ticker)

    # crypto
    elif callback == print_crypto_trade:
        print(callback)
        stream.subscribe_crypto_trades(callback, ticker)
    elif callback == print_crypto_quote:
        print(callback)
        stream.subscribe_crypto_quotes(callback, ticker)
    elif callback == print_crypto_daily_bar:
        print(callback)
        stream.subscribe_crypto_daily_bars(callback, ticker)
    elif callback == print_crypto_bar:
        print(callback)
        stream.subscribe_crypto_bars(callback, ticker)


def start_multiple_subscriptions(tickers, callbacks):
    if type(tickers) == list and type(callbacks) == list:
        assert len(tickers) == len(callbacks)
        for i in range(len(tickers)):
            start_single_subscription(tickers[i], callbacks[i])
    if type(tickers) == list:
        for i in range(len(tickers)):
            start_single_subscription(tickers[i], callbacks)
    if type(callbacks) == list:
        for i in range(len(callbacks)):
            start_single_subscription(tickers, callbacks[i])


def get_real_time_data(ticker, callback):
    logging.basicConfig(format='%(asctime)s  %(levelname)s %(message)s',
                        level=logging.INFO)

    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor(1)

    while 1:
        try:
            executor.submit(start_stream(ticker, callback))
            time.sleep(5)
            loop.run_until_complete(stream.stop_ws())
            time.sleep(5)
        except KeyboardInterrupt:
            print("Interrupted execution by user")
            loop.run_until_complete(stream.stop_ws())
            exit(0)
        except Exception as e:
            print("You got an exception: {} during execution. Continue "
                  "execution.".format(e))
            # let the execution continue
            pass


def preprocess_historical_data(data):
    """
        Transforms a df containing barset stock data (i.e. open, close, high, low, volume, vwap)
        into a pandas series containing only volume-weighted average price (vwap)
        Args:
            data - a df containing historical barset data
        Returns:
            data - a pandas series containing the volume weighted average price
    """
    return data['vwap']


def process_backtesting_results(vwap_data, buy_prices, sell_prices, signals):
    df                = pd.DataFrame(vwap_data)
    df['buy_price']  = buy_prices
    df['sell_price'] = sell_prices
    df['signal']     = signals

    return df

