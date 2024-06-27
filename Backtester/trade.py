import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import Dict
import databento as db
from DataManager.mongo import *
import random

load_dotenv(dotenv_path=".ENV")
api_key = os.environ.get("test")

client = db.Historical(api_key)


class Trade:
    def __init__(self, date, booksize, strategy):
        self.date = date
        self.booksize = booksize
        self.strategy = strategy



    async def get_trade_prices(self, normalized_weights: Dict, date_open, date_close):
        symbols = list(normalized_weights.keys())


        coro = await client.timeseries.get_range_async(
            dataset="XNAS.ITCH",
            schema="ohlcv-1h",
            stype_in="raw_symbol",
            symbols=symbols,
            start=date_open,
            end=date_close,

        )
        df = coro.to_df()
        #portfolio = await self.open_port(normalized_weights, df)
        # # print(f"drops_10_pct_or_more: {drops_10_pct_or_more}")
        #print(f"Portfolio {self.date}: {portfolio}")
        #return portfolio
        return df

    async def open_port(self, weights, price_data, slippage=False):
        new_portfolio = {}
        slippage_factor_min = 0.0  # 0% slippage
        slippage_factor_max = 0.01  # 1% slippage

        for ticker, weight in weights.items():
            # Find the first instance of the ticker in the price data
            ticker_data = price_data[price_data['symbol'] == ticker].head(1)


            if not ticker_data.empty:
                open_price = ticker_data.iloc[0]['open']
                slippage_factor = random.uniform(slippage_factor_min, slippage_factor_max)
                slip = open_price * slippage_factor if slippage else 0
                execution_price = open_price + slip if weight > 0 else open_price - slip
                shares = (self.booksize * weight) // execution_price
                new_portfolio[ticker] = {'shares': shares, 'value': shares * execution_price}
            else:
                print(f"Price data for {ticker} not available")
        # print(new_portfolio)
        return new_portfolio

    async def close_port(self, current_date, prior, date_open, date_close, portfolio):
        #print(f"CLOSE PORT CALLED FOR: {current_date} which is {type(current_date)}")
        # print(f"Sell Current Date: {current_date}")
        pnl_per_ticker = {}

        price_dict = await self.get_trade_prices(portfolio, date_open,
                                                 date_close)  # Get current hourly prices
        # Now we need to find the price change
        tickers = list(portfolio.keys())
        for ticker in tickers:
            val = portfolio.get(ticker, {}).get('value', 0.0)
            shares = portfolio.get(ticker, {}).get('shares', 0)
            #print(f"CLOSING: {ticker}, shares: {shares}, val: {val}")
            # Bought for:
            try:
                if shares == 0:
                    raise ValueError("Shares cannot be zero for execution price calculation.")
                execution_price = val / shares

            except ValueError as e:
                print(f"Error in calculation: {e} for {ticker} on {current_date}")
                execution_price = None

            ticker_price_data = price_dict[price_dict['symbol'] == ticker].head(1)

            if not ticker_price_data.empty:
                close_price = ticker_price_data.iloc[0]['open']


                if shares > 0:  # Long position
                    pnl = (close_price - execution_price) * shares
                else:  # Short position
                    pnl = (execution_price - close_price) * abs(shares)
                #print(f"{ticker}: Execution Price {prior}: {execution_price}, Close Price {current_date}: {close_price}, PnL: {pnl}")
                pnl_per_ticker[ticker] = {'pnl': pnl, 'execution_price': execution_price, 'close_price': close_price}
            else:
                print(f"No closing price data available for {ticker} on {current_date}")

        return pnl_per_ticker


    async def calculate_entire_pnl(self, pnl_per_ticker):
        total_pnl = sum(pnl_per_ticker.values())
        return total_pnl


    async def calculate_total_pnl(self, pnl_per_ticker):
        total_pnl = 0
        for ticker in pnl_per_ticker.keys():
            total_pnl += pnl_per_ticker[ticker]['pnl']
        return total_pnl














