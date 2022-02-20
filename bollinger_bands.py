import config
import back_testing as bt
import requests
import yfinance as yf
import pandas_datareader.data as web
import numpy as np
import opstrat as op
import matplotlib.pyplot as plt
import pandas as pd
import alpaca_trade_api as tradeapi
import asyncio
import logging
from datetime import datetime, timedelta, date
from alpaca_trade_api.stream import Stream
from alpaca_trade_api.common import URL
from alpaca_trade_api.rest import REST, TimeFrame, TimeFrameUnit
from concurrent.futures import ThreadPoolExecutor
from alpaca_trade_api.entity import Bars


class BollingerBands:
    """
    A class used to trade a single ticker (whether stock or crypto) using 
    Bollinger Bands strategy

    Attributes
    ----------
    ticker: String
        Ticker specifying fin instrument to be traded
    _portfolio_size: Float
        Amount of $ used to trade the asset
    _upper_band: Float
        Upper bollinger band
    _lower_band: Float
        Lower bollinger band
    _quantity: Float
        Current quantity of the asset we're in posession of
    _order_state: String
        Specifies whether a buy or sell order has been executed
    _window_size: Int
        Size of the window for calculating moving average
    _num_std: Int
        Number of standard deviations specifying width of the bollinger bands
    _current_price: Float
        Current price of the asset. This is assumed to be the close price 
        of the most recent bar
    _previous_price: Float
        Close price of the previous bar. 
    _frequency: alpaca's custom TimeFrame
        Used for retrieving historical bar data. Specifies the frequency of the
        bar data. E.g. Minute, Hour and Day
    _start_date: String
        Used for retrieving historical bar data. Specifies the starting date
        since when the data should be retrieved. 
    _end_date: String
        Used for retrieving historical bar data. Specifies the end date
        until when the data should be retrieved. 
    _exchange: String
        Exchange used for gathering real time data about the instrument

    Methods
    -------
    _initialize_bars_from_historical()
        Initializes bars using Alpaca's api to retrieve historical data about
        the asset in specified time period with specified frequency
    _initialize_bars()
        Initializes a df for keeping the bar data
    _calculate_bands()
        Calculates upper and lower bollinger band
    _sell()
        Submits a sell order
    _buy()
        Submits a buy order
    _update_bars(bar)
        Update bars data when a new bar arrives
    _execute_bollinger_bands()
        Executes the trading logic following bollinger bands
    handle_trade_update()
        Handler for when a trade is executed and arrives in real-time 
    handle_bar()
        Handler for when a new bar comes
    _handle_filled_order()        
        Handles trade whose order was successfully filled
    _handle_partially_filled_order()
        Handles trade whose order was partially filled
    _handle_rejected_order()
        Handles trade whose order was rejected
    """
    def __init__(self, ticker, portfolio_size, window_size=20, num_std=1, \
                frequency=TimeFrame.Minute, start_date="01-01-2022", \
                end_date="01-01-2022"):
        self.ticker = ticker
        self._portfolio_size = portfolio_size
        self._upper_band = None
        self._prev_upper_band = None
        self._lower_band = None
        self._prev_lower_band = None
        self._quantity = 0
        self._order_state = None
        self._window_size = window_size
        self._num_std = num_std
        self._exchange = None
        self._current_price = None
        self._previous_price = None
        self._frequency = frequency
        self._start_date = start_date
        self._end_date = end_date
        self._initialize_bars()
        

    def _initialize_bars_from_historical(self):
        """
        Initializes bars from historical data
        """
        # load yesterday's data
        self._bars = bt.get_historical_bar_data(self.ticker, 
                        frequency=self._frequency,
                        start_date=self._start_date,
                        end_date=self._end_date)

        # take the last window_size number of minutes
        self._bars = self._bars.tail(self._window_size)


    def _initialize_bars(self):
        self._bars = pd.DataFrame(data={
            'close': [],
            'exchange': [],
            'high': [],
            'low': [],
            'open': [],
            'symbol': [],
            'timestamp': [],
            'trade_count': [],
            'volume': [],
            'vwap': []
        })


    def _calculate_bands(self):
        """
        Calculates the lower and upper bollinger bands
        """
        self._prev_upper_band = self._upper_band
        self._prev_lower_band = self._lower_band

        std = self._bars['vwap'].rolling(self._window_size, min_periods=1).std().iloc[-1]
        sma = self._bars['vwap'].rolling(self._window_size, min_periods=1).mean().iloc[-1]

        self._upper_band = sma + self._num_std * std
        self._lower_band = sma - self._num_std * std

        # when initializing upper and lower bands (i.e. they were None in the 
        # assignment above), make the previous bands the same
        if self._prev_upper_band == None or self._prev_lower_band == None:
            self._prev_upper_band = self._upper_band
            self._prev_lower_band = self._lower_band


    def _sell(self):
        """
        Submits sell order if currently in possession of the asset
        and the last submitted order wasn't a sell
        """
        # currently in no possession of the asset, can't execute a sell order
        if self._quantity == 0 or self._order_state == 'sell_submitted':
            log.info(f'State: {self._order_state}, quantity: {self._quantity}, \
            portfolio size: {self._portfolio_size}. Exiting sell order.')
            return

        log.info(f'Selling: {self._quantity} shares of {self.ticker}')

        api.submit_order(
            symbol=self.ticker,
            qty=self._quantity,
            side='sell',
            type='market',
            time_in_force='gtc'
        )

        self._order_state = 'sell_submitted'


    def _buy(self):
        """
        Submits buy order if currently in no possession of the asset 
        and the last submitted order wasn't a buy
        """
        # already bought the asset, wait until asset sold to buy again
        if self._quantity != 0 or self._portfolio_size == 0 or \
            self._order_state == 'buy_submitted':
            log.info(f'State: {self._order_state}, quantity: {self._quantity}, \
            portfolio size: {self._portfolio_size}. Exiting buy order.')
            return

        quantity_to_buy = self._portfolio_size / self._bars['close'].iloc[-1]

        log.info(f'Buying: {self._quantity} shares of {self.ticker}')

        api.submit_order(
            symbol=self.ticker,
            qty=quantity_to_buy,
            side='buy',
            type='market',
            time_in_force='gtc'
        )

        self._order_state = 'buy_submitted'


    def _update_bars(self, bar):
        """
        Updates the bars with the latest bar
        """
        # this bar came from a different exchange -> discard it
        #if bar.exchange != self._exchange:
        #    return

        self._bars = self._bars.append({
            'close': bar.close,
            'exchange': bar.exchange,
            'high': bar.high,
            'low': bar.low,
            'open': bar.open,
            'symbol': bar.symbol,
            'timestamp': bar.timestamp,
            'trade_count': bar.trade_count,
            'volume': bar.volume,
            'vwap': bar.vwap
        }, ignore_index=True)

        self._previous_price = self._current_price
        self._current_price  = bar.close 
        
        if (len(self._bars) > self._window_size):
            self._bars = self._bars.tail(self._window_size)


    def _execute_bollinger_bands(self):
        """
        Executes a buy when the current price is below the lower band and 
        the price in the previous time interval was above the lower band
        Executes a sell when the current price is above the upper band and
        the price in the previous time interval was below the upper band
        """
        self._calculate_bands()

        if self._current_price < self._lower_band \
            and self._previous_price > self._prev_lower_band:
            self._buy()
        elif self._current_price > self._upper_band \
            and self._previous_price < self._prev_upper_band:
            self._sell()


    def _handle_filled_order(self, trade):
        """
        Called when an order is successfully filled
        Args:
            trade: alpaca's Trade object type 
                trade in which the order got filled
        """
        log.info(f"{trade.order['side']} at {trade.price} of size {trade.qty}")

        # buy order filled
        if trade.order['side'] == 'buy':
            # update portfolio size and current quantity
            self._quantity = float(trade.qty)
            self._portfolio_size -= float(trade.qty) * float(trade.price)

        # sell order filled
        elif trade.order['side'] == 'sell':
            # update portfolio size and current quantity
            self._portfolio_size = float(trade.qty) * float(trade.price)  
            self._quantity = 0
            

    def _handle_partially_filled_order(self, trade):
        """ TODO """
        pass

    def _handle_rejected_order(self, trade):
        """ TODO """
        pass

    def handle_trade_update(self, trade):
        """
        Called when previously submitted order got updated in the form of 
        a trade
        Args:
            trade: alpaca's Trade object type
                trade that specifies what happened with the order that 
                initiated the trade
        """
        if trade.event == 'fill':
            self._handle_filled_order(trade)
        elif trade.event == 'partial_fill':
            self._handle_partially_filled_order(trade)
        elif trade.event == 'rejected' or trade.event == 'canceled':
            self._handle_rejected_order(trade)


    def handle_bar(self, bar):
        """
        Called when a new bar arrives
        Args:
            bar: pandas df
                new bar of data
        """
        if self._bars is None:
            self._initialize_bars()
        if self._exchange is None:
            self._exchange = bar.exchange

        self._update_bars(bar)

        log.info(f'bars: {self._bars}')

        if (len(self._bars) >= self._window_size):                
            self._execute_bollinger_bands()


def start_subscriptions(stream, bb, instr_type, frequency="daily"):
    """
    Starts subscriptions that in turn start websockets via which the real
    time data is retrieved
    Args:
        stream: Stream
            starts websockets required for real time data retrieval and trading
        bb: BollingerBands
        instr_type: String
            Specifies whether a stock or crypto is traded
    """
    async def on_trade_update(trade):
        bb.handle_trade_update(trade)
    async def on_crypto_bar(bar):
        bb.handle_bar(bar)
    async def on_bar(bar):
        bb.handle_bar(bar)
    async def on_daily_bar(bar):
        bb.handle_bar(bar)
    async def on_daily_crypto_bar(bar):
        bb.handle_bar(bar)

    if instr_type == "crypto" and frequency == "minute":
        stream.subscribe_crypto_bars(on_crypto_bar, bb.ticker)
    if instr_type == "crypto" and frequency == "daily":
        stream.subscribe_crypto_daily_bars(on_daily_crypto_bar, bb.ticker)
    elif instr_type == "stock" and frequency == "minute":
        stream.subscribe_bars(on_bar, bb.ticker)
    elif instr_type == "stock" and frequency == "daily":
        stream.subscribe_crypto_daily_bars(on_daily_bar, bb.ticker)

    stream.subscribe_trade_updates(on_trade_update)
    stream.run()


# function taken from: https://github.com/alpacahq/alpaca-trade-api-python
def start_trading(bb, instr_type, frequency="daily"):
    """
    Starts the trading algorithm
    Args:
        bb: instance of BolingerBands
        instr_type: String
            Specifies whether a stock or crypto is traded
    """
    stream = Stream(config.key_id, 
                config.secret_key,
                base_url=config.base_url,
                data_feed='iex')

    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor(1)

    while 1:
        try:
            executor.submit(start_subscriptions(stream, bb, instr_type, frequency))
            loop.run_until_complete(stream.stop_ws())
        except KeyboardInterrupt:
            log.info("Interrupted execution by user")
            loop.run_until_complete(stream.stop_ws())
            exit(0)
        except Exception as e:
            log.info("You got an exception: {} during execution. Continue "
                  "execution.".format(e))
            # let the execution continue
            pass


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

def get_instrument_type():
    instr_type = input("Enter instrument type. Options: (1) 'crypto' or " +
       "(2) 'stock' based on the type of instrument you want to trade: ")
    
    try:
        assert instr_type == 'crypto' or instr_type == 'stock'
    except:
        raise TypeError("You entered an invalid instrument type")

    return instr_type

def get_frequency():
    frequency = input("Enter frequency of data to backtest against. " +
        "Options: (1) 'minute', (2) 'daily': ")

    try:
        assert frequency == 'minute' or frequency == 'daily'
    except:
        raise TypeError("You entered an invalid frequency")

    return frequency

def get_inputs():
    ticker = get_ticker()
    portfolio_size = get_portfolio_size()
    instr_type = get_instrument_type()
    frequency = get_frequency()

    return ticker, portfolio_size, instr_type, frequency


def main():
    ticker, portfolio_size, instr_type, frequency =  get_inputs()

    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
    global log
    log = logging.getLogger()

    # defined global so that it could be used for trading fin instruments
    global api
    api = tradeapi.REST(
            key_id=config.key_id, 
            secret_key=config.secret_key, 
            base_url=config.base_url,
            api_version='v2')

    bb = BollingerBands(ticker, portfolio_size, window_size=20, num_std=2)
    start_trading(bb, instr_type, frequency=frequency)


if __name__ == "__main__":
    main()