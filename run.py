import data_handler as dh
import bollinger_bands as bb
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
from datetime import datetime, timedelta, date


import alpaca_trade_api as tradeapi
from alpaca_trade_api.stream import Stream
from alpaca_trade_api.common import URL
from alpaca_trade_api.rest import REST, TimeFrame, TimeFrameUnit
import nest_asyncio
import logging
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor


def calculate_profit_percentage(start_price, end_price):

    return (end_price - start_price) / start_price


def calculate_returns(portfolio_size, data):
    """
        Calculates returns of Bolinger Bands algorithm against historical data
        Assumes a fractional number of shares can be purchased
    """
    for i in range(data.shape[0]):
        # buy the asset
        if data['signal'][i] == 1:
            num_shares = portfolio_size / data['buy_price'][i]
            
            #print("Bought {0} shares at {1}. Current portfolio value: {2}".format(num_shares, data['buy_price'][i], data['buy_price'][i] * num_shares))

        # sell the asset
        elif data['signal'][i] == -1:
            portfolio_size = num_shares * data['sell_price'][i]
            
            #print("Sold {0} shares at {1}. Current portfolio value: {2}".format(num_shares, data['sell_price'][i], data['sell_price'][i] * num_shares))

    return portfolio_size


def back_test_bollinger_bands(ticker, frequency, start_date, end_date, portfolio_size, data):
    file_name = dh.get_file_name(ticker, frequency, start_date, end_date)

    # Get downloaded data from a pickle
    data = dh.load_data(file_name)
    # Run the algorithm 
    bb_results = bb.bollinger_bands(data, window_size=20, num_std_dev=1)
    
    # Calculate returns and compare starting and ending portfolio size
    print("Starting portfolio: {0}".format(portfolio_size))
    end_portfolio_size = calculate_returns(portfolio_size, bb_results)
    print("Ending portfolio: {0}".format(end_portfolio_size))
    percentage_return = calculate_profit_percentage(portfolio_size, end_portfolio_size)
    print("Return: {0}".format(percentage_return))


def get_today():
    return date.today().strftime('%Y-%m-%d')


def get_yesterday():
    yesterday = datetime.now() - timedelta(1)
    return datetime.strftime(yesterday, '%Y-%m-%d')


def main():
    ticker = "BTCUSD" #SPY #BTCUSD 
    frequency = TimeFrame.Day
    start_date = "2005-01-01"
    end_date = "2022-01-21"
    portfolio_size = 100000
    callback = dh.print_trade_update # print_crypto_quote
    tickers= ['BTCUSD', 'BTCUSD']
    callbacks = [dh.print_trade, dh.print_quote, dh.print_bar]
    today = get_today()
    yesterday = get_yesterday()

    #back_test_bollinger_bands(ticker, frequency, start_date, end_date, portfolio_size, data)
    dh.get_real_time_data(ticker, callback)

    #data = dh.get_historical_bar_data(ticker, 
    #                    frequency=TimeFrame.Minute,
    #                    start_date=yesterday,
    #                    end_date=yesterday)
    
    #stream.subscribe_trades(print_trade, 'AAPL')
    #stream.subscribe_quotes(print_quote, 'IBM')
    #stream.subscribe_crypto_trades(print_crypto_trade, 'BTCUSD')

 
main()