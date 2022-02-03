from alpaca_trade_api.entity import Bars
import config
import data_handler as dh
import requests
import yfinance as yf
import pandas_datareader.data as web
import numpy as np
import opstrat as op
import matplotlib.pyplot as plt
import pandas as pd
import re
import time
from datetime import datetime, timedelta, date
import math
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


class Algorithm:
    def __init__(self, ticker, api, portfolio_size):
        self._ticker = ticker
        self._portfolio_size = portfolio_size
        self._context = {}
        self._bars = []
        self._api = api
        self._yesterday = get_yesterday()
        self._upper_band = None
        self._lower_band = None
        self._quantity = None
        self._order_state = None

    def _initialize_bars(self, window_size=20):
        # load yesterday's data
        self._bars = dh.get_historical_bar_data(self._ticker, 
                        frequency=TimeFrame.Minute,
                        start_date=self._yesterday,
                        end_date=self._yesterday)

        # take the last window_size number of minutes
        self._bars = self._bars.tail(window_size)

    def _calculate_bands(self, window_size=20, num_std_dev=1):
        std = self._bars.rolling(window_size, min_periods=1).std()
        sma = self._bars.rolling(window_size, min_periods=1).mean()
        
        self._upper_band = sma + num_std_dev * std
        self._lower_band = sma - num_std_dev * std

    def _sell(self):
        self._api.submit_order(
            symbol=self._ticker,
            qty=self._quantity,
            side='sell',
            type='market',
            time_in_force='gtc'
        )

        self._order_state = 'sell_submitted'

    def _buy(self):
        quantity_to_buy = math.floor(self._portfolio_size / self._bars['close'][:1])
        
        self._api.submit_order(
            symbol=self._ticker,
            qty=quantity_to_buy,
            side='buy',
            type='market',
            time_in_force='gtc'
        )

        self._order_state = 'buy_submitted'


    def _update_bars(self, bar):
        self._bars = self._bars.iloc[1:, :]
        self._bars = self._bars.append(bar)
        self._bars.append({
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume,
        }, ignore_index=True)


    async def _handle_bar(self, bar):
        self._bollinger_bands(bar)


    async def on_trade_update(self, trade):
        print(f'trade: {trade}')

        # order filled
        if trade.event == 'fill':
            if self._order_state == 'buy_submitted':
                self._quantity = trade.qty
            else: 
                # portfolio size = quantity * selling_price
                self._portfolio_size = trade.qty * trade.price


    def _bollinger_bands(self, bar):
        vwap = bar['vwap']

        if vwap < self._lower_band:
            self._sell()
        elif vwap > self._upper_band:
            self._buy()

        self._update_bars(bar)
            



##########################################
#### Procedural part for backtesting #####
##########################################
def get_bands(data, window_size=20, num_std_dev=1):
    std = data.rolling(window_size, min_periods=1).std()
    sma = data.rolling(window_size, min_periods=1).mean()
    
    upper_band = sma + num_std_dev * std
    lower_band = sma - num_std_dev * std
    
    return lower_band, upper_band


def handle_buy(signal, vwap, buys, sells, signals):
    if signal != 1:
        buys.append(vwap)
        sells.append(np.nan)
        signal = 1
        signals.append(signal)
    else:
        buys.append(np.nan)
        sells.append(np.nan)
        signals.append(0)

    return signal


def handle_sell(signal, vwap, buys, sells, signals):
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
    buys.append(np.nan)
    sells.append(np.nan)
    signals.append(0)


def bollinger_bands(data, window_size=20, num_std_dev=1):
    # get vwap data into series format
    vwaps = dh.preprocess_historical_data(data)
    # get bollinger bands
    lower_band, upper_band = get_bands(vwaps, window_size, num_std_dev)

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
    bb_results = dh.process_backtesting_results(vwaps, buys, sells, signals)

    return bb_results


def get_positions(signals):
    positions = [signals[0]] * len(signals)

    for i in range(1, len(signals)):
        if signals[i] == 1:
            positions[i] = 1
        elif signals[i] == -1:
            positions[i] = 0
        else:
            positions[i] = positions[i-1]            

    return positions


def buy_and_hold(data):
    buy_price = data[0]
    sell_price = data[len(data)-1]

    return buy_price, sell_price


def get_today():
    return date.today().strftime('%Y-%m-%d')


def get_yesterday():
    yesterday = datetime.now() - timedelta(1)
    return datetime.strftime(yesterday, '%Y-%m-%d')


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
        stream.subscribe_trades(callback, ticker)
    elif callback == print_quote:
        stream.subscribe_quotes(callback, ticker)
    elif callback == print_bar:
        stream.subscribe_bars(callback, ticker)
    if callback == print_trade_update:
        stream.subscribe_trade_updates(print_trade_update)
    elif callback == print_daily_bar:
        stream.subscribe_daily_bars(callback, ticker)

    # crypto
    elif callback == print_crypto_trade:
        stream.subscribe_crypto_trades(callback, ticker)
    elif callback == print_crypto_quote:
        stream.subscribe_crypto_quotes(callback, ticker)
    elif callback == print_crypto_daily_bar:
        stream.subscribe_crypto_daily_bars(callback, ticker)
    elif callback == print_crypto_bar:
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


async def print_trade(t):
    print('trade', t)


async def print_quote(q):
    print('quote', q)


async def print_bar(bar):
    print('bar', bar)


async def print_daily_bar(bar):
    print('daily bar', bar)

async def print_crypto_trade(t):
    print('crypto trade', t)


async def print_crypto_quote(q):
    print('crypto quote', q)


async def print_crypto_bar(bar):
    print('crypto bars', bar)


async def print_crypto_daily_bar(daily_bar):
    print('crypto daily bar', daily_bar)


async def print_trade_update(t_update):
    print('trade update', t_update)



def main():
    ticker = "BTCUSD" #SPY #BTCUSD # AAPL
    frequency = TimeFrame.Day
    start_date = "2005-01-01"
    end_date = "2022-01-21"
    portfolio_size = 100000
    callback = dh.print_trade_update # print_trade_update # print_quote # print_crypto_quote
    tickers= ['BTCUSD', 'BTCUSD']
    callbacks = [dh.print_crypto_bar, dh.print_trade_update] #dh.print_trade_update, 

    api = tradeapi.REST(
            key_id=config.key_id, 
            secret_key=config.secret_key, 
            base_url=config.base_url,
            api_version='v2')

    api.submit_order(
            symbol=ticker,
            qty=0.1,
            side='buy',
            type='market',
            time_in_force='gtc'
        )

    dh.get_real_time_data(ticker, callbacks)

    """
    global stream
    while 1:
        try:
            try:
                loop = asyncio.get_event_loop()
                loop.set_debug(True)
            except RuntimeError:
                asyncio.set_event_loop(asyncio.new_event_loop())
                
            stream = Stream(config.key_id, 
                            config.secret_key,
                            base_url=config.base_url,
                            data_feed='iex')

            if type(ticker) == list or type(callback) == list:
                start_multiple_subscriptions(ticker, callback)
            else:
                start_single_subscription(ticker, callback)

            stream.run()

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
    """
    





main()