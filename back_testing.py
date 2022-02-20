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
from datetime import datetime, timedelta

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


def handle_buy(signal, vwap, buys, sells, signals, current_position):
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
    if signal != 1 and current_position == 0:
        buys.append(vwap)
        sells.append(np.nan)
        signal = 1
        signals.append(signal)
        current_position = 1
    else:
        buys.append(np.nan)
        sells.append(np.nan)
        signals.append(0)

    return signal, current_position


def handle_sell(signal, vwap, buys, sells, signals, current_position):
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
    if signal != -1 and current_position == 1:
        buys.append(np.nan)
        sells.append(vwap)
        signal = -1
        signals.append(signal)
        current_position = 0
    else:
        buys.append(np.nan)
        sells.append(np.nan)
        signals.append(0)

    return signal, current_position


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

    buys    = []
    sells   = []
    signals = []
    signal  = 0

    # is 1 when long in the position, 0 when having cash in possession
    current_position = 0

    for i in range(0, len(vwaps)):
        # buy
        if vwaps[i] < lower_band[i] and vwaps[i-1] > lower_band[i-1]: 
            signal, current_position = handle_buy(signal, vwaps[i], buys, sells, 
                                        signals, current_position)
        # sell
        elif vwaps[i] > upper_band[i] and vwaps[i-1] < upper_band[i-1]:
            signal, current_position = handle_sell(signal, vwaps[i], buys, sells, 
                                        signals, current_position)
        # stay put
        else:
            handle_neutral(buys, sells, signals)

    # transform the results from lists into a dataframe    
    bb_results = process_backtesting_results(vwaps, buys, sells, signals)

    return bb_results


def buy_and_hold(data):
    """
    Executes a buy and hold strategy on historical bars data
    Args:
        data: pandas df
            bar data of a certain instrument
    """
    buy_price = data[0]
    sell_price = data[len(data)-1]

    return buy_price, sell_price


def calculate_rf_interest(rf_annual, start_date, end_date):
    """
    Calculates risk-free interest over the traded period
    Args:
        rf_annual: float
            annual risk free interest rate
        start_date: string
            YYYY-MM-DD format
        end_date: string
            YYYY-MM-DD format
    """
    date_format = "%Y-%m-%d"
    start = datetime.strptime(start_date, date_format)
    end = datetime.strptime(end_date, date_format)
    delta = end - start
    delta_in_yrs = delta.days / 365

    return ((1 + (rf_annual/100)) ** delta_in_yrs - 1) * 100


def calculate_baseline_sharpe(data, rp, rf):
    """
    Calculates sharpe ratio for the baseline model (i.e. buy and hold)
    Args:
        data: pandas df
            historical bar data about the traded instrument
        rp: float
            return of portfolio over the traded period in % form
        rf: float
            risk free rate of return over the trade period in % form
    """
    rp = rp/100
    rf = rf/100
    stdev = (len(data) ** 0.5) * data.vwap.pct_change(1).std()
    return (rp - rf) / stdev


def calculate_profit_percentage(start_size, end_size):
    """
    Calculates profit of a buy and hold in % form
    Args:
        start_size: float
            starting portfolio size
        end_size: float
            ending portfolio size
    """
    return 100 * (end_size - start_size) / start_size


def calculate_returns(portfolio_size, data):
    """
    Calculates returns of Bolinger Bands algorithm against historical data
    Assumes a fractional number of shares can be purchased
    """
    for i in range(data.shape[0]):
        # buy the asset
        if data['signal'][i] == 1:
            num_shares = portfolio_size / data['buy_price'][i]
            log.info(f"Date: {data.index[i]}\
                Bought {num_shares} shares at {data['buy_price'][i]}.\
                Current portfolio value: {num_shares  * data['buy_price'][i]}")

        # sell the asset
        elif data['signal'][i] == -1:
            portfolio_size = num_shares * data['sell_price'][i]
            log.info(f"Date: {data.index[i]}\
                Sold {num_shares} shares at {data['sell_price'][i]}.\
                Current portfolio value: {num_shares  * data['sell_price'][i]}")

    return portfolio_size

def get_portfolio_size():
    portfolio_size = input("Enter size of portfolio to trade with in $USD: ")
    try:
        portfolio_size = int(portfolio_size) + 0
    except:
        raise TypeError("Enter numerical value for portfolio size")

    return portfolio_size

def get_ticker():
    ticker = input("Enter ticker to trade: ")
    return ticker

def get_start_date():
    start_date = input("Enter start date in form YYYY-MM-DD: ")
    return start_date

def get_end_date():
    end_date = input("Enter end date in form YYYY-MM-DD: ")
    return end_date

def get_frequency():
    frequency = input("Enter frequency of data to backtest against. " +
        "Options: (1) 'day', (2) 'hour', (3) 'minute': ")

    try:
        assert frequency == 'day' or frequency == 'hour' or frequency == 'minute'
    except:
        raise TypeError("You entered an invalid frequency")

    if frequency == 'day':
        return TimeFrame.Day
    elif frequency == 'hour':
        return TimeFrame.Hour
    else:
        return TimeFrame.Minute

def get_inputs():
    ticker = get_ticker()
    portfolio_size = get_portfolio_size()
    start_date = get_start_date()
    end_date = get_end_date()
    frequency = get_frequency()

    return ticker, portfolio_size, start_date, end_date, frequency


def main():
    logging.basicConfig(format='%(message)s', level=logging.INFO)
    global log
    log = logging.getLogger()

    ticker, portfolio_size, start_date, end_date, frequency = get_inputs()

    # risk-free annual interest rate
    rf_annual = 1.47
    rf = calculate_rf_interest(rf_annual, start_date, end_date)

    # Retrieve data from a csv file
    #file_name = get_file_name(ticker, frequency, start_date, end_date)
    #data = load_data(file_name)
    
    # Retrieve data directly via Alpaca api
    data = get_historical_bar_data(ticker,
                        frequency=frequency,
                        start_date=start_date,
                        end_date=end_date)

    bb_results = backtest_bollinger_bands(data, window_size=20,num_std=2)
    end_port_size = calculate_returns(portfolio_size, bb_results)
    bb_return = calculate_profit_percentage(portfolio_size, end_port_size)
    log.info(f'Bollinger bands return: {bb_return}%')

    hold_start_size, hold_end_size = buy_and_hold(data['vwap'])
    baseline_return = calculate_profit_percentage(hold_start_size, hold_end_size)
    baseline_sharpe = calculate_baseline_sharpe(data, baseline_return, rf)
    log.info(f'Buy-and-hold return: {baseline_return}%. Sharpe: {baseline_sharpe}')


if __name__ == "__main__":
    main()