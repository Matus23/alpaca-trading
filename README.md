# alpaca-trading

This repository contains a bot that uses Bollinger bands strategy to trade a specified instrument via Alpaca markets API. The repository contains 2 scripts:

'back_testing.py' - used for backtesting the bollinger bands strategy. Logs info about when the buys and sells would have occurred and how the portfolio size would have changed over time. Prints the total return that would have been produced over the trading period and compares it with the return that would be produced by a buy-and-hold strategy. Calculates Sharpe ratio for buy-and-hold too.

'bollinger_bands.py' - used for running the bot to trade via alpaca markets API in real time. The buy and sell orders are executed via handlers that are triggered when a new bar of data comes in. 

To run 'back_testing.py':
```
python back_testing.py
Enter ticker to trade: SPY
Enter size of portfolio to trade with in $USD: 10000
Enter start date in form YYYY-MM-DD: 2015-01-01
Enter end date in form YYYY-MM-DD: 2022-01-01
Enter frequency of data to backtest against. Options: (1) 'day', (2) 'hour', (3) 'minute': day
```

To run 'bollinger_bands.py':
```
python bollinger_bands.py
Enter ticker to trade: SPY
Enter size of portfolio to trade with in $USD: 10000
Enter instrument type. Options: (1) 'crypto' or (2) 'stock' based on the type of instrument you want to trade: stock
Enter frequency of data to backtest against. Options: (1) 'minute', (2) 'daily': daily 
```
