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


def get_historical_data(ticker, 
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

    print(data.shape)

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


async def print_bar(bar):
    print('bar', bar)


# function taken from: https://github.com/alpacahq/alpaca-trade-api-python
def start_rtd_conn(ticker, callback):
    """
        Starts a connection to retrieve real time data via Alpaca Markets API
        Free mode retrieves data from iex exchange
        Args:
            ticker   - ticker specifying stock to be retrieved
            callback - callback function to be called. Specifies
                    what type of data is to be retrieved.
                    Options: (1) trade
                             (2) quote
                             (3) bar
    """
    try:
        # make sure we have an event loop, if not create a new one
        loop = asyncio.get_event_loop()
        loop.set_debug(True)
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    global conn
    conn = Stream(config.key_id, 
                config.secret_key,
                base_url=config.base_url,
                data_feed='iex')

    conn.subscribe_quotes(callback, ticker)
    conn.run()


# function taken from: https://github.com/alpacahq/alpaca-trade-api-python
def start_rtd_stream(ticker, callback):
    logging.basicConfig(format='%(asctime)s  %(levelname)s %(message)s',
                        level=logging.INFO)

    loop = asyncio.get_event_loop()
    pool = ThreadPoolExecutor(1)

    while 1:
        try:
            pool.submit(start_rtd_conn(ticker, callback))
            time.sleep(5)
            loop.run_until_complete(conn.stop_ws())
            time.sleep(5)
        except KeyboardInterrupt:
            print("Interrupted execution by user")
            loop.run_until_complete(conn.stop_ws())
            exit(0)
        except Exception as e:
            print("You got an exception: {} during execution. Continue "
                  "execution.".format(e))
            # let the execution continue
            pass

def preprocess_data(data):
    """
    
    """
    return data['vwap']
    #return data.drop(['open', 'high', 'low', 'close', 'volume', 'trade_count'], 1)    




def main():
    ticker = "AAPL"
    frequency = TimeFrame.Minute
    start_date = "2005-01-01"
    end_date = "2022-01-21"
    file_name = get_file_name(ticker, frequency, start_date, end_date)
    callback = print_quote

    data = load_data(file_name)
    data = preprocess_data(data)

    print(data[:100])

main()