''' This will run the backtest simulation. It will make calls to trade for submission and dates for time. Metric formulas will be tracked in metric.py'''
'''Here I am running over all of the dates in the date_df, and for each day I need to:
Day 0: Calculate and Buy weights. 
Day 1-N: Sell, record metrics, recalculate, and Buy weights'''
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main import run_strategy_for_period
from Tests.portfolio import Portfolio_Tests
from DataManager.poly_universe import poly_mongo
#from trade import PriceData
from trade import Trade
from tqdm import tqdm
from Backtester.dates import Backtest, Strategy
from Backtester.metrics import Metrics
from DataManager.mongo import *
import pandas as pd
import asyncio
import requests
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=".ENV")
nfty_url = os.environ.get("NFTY_URL")

class Simulation:
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.backtest = Backtest(exchange='NASDAQ')
        self.strategy = Strategy(self.backtest)
        self.global_trade = Trade(None, None, self.strategy)
        self.dates_df = self.backtest.get_trading_days(start_date, end_date, full=True)
        self.portfolio = {} #{ticker : market value of position}
        self.booksize: int = 20000000
        self.metrics = Metrics(self.booksize)
        self.portfolio_tests = Portfolio_Tests(self.booksize)

    async def run_backtest(self):
        requests.post(nfty_url, data="Backtest Started".encode(encoding='utf-8'))
        await mongo.clear_database()
        trades = {}
        prior = None
        market_neutral = None
        arc_tan = None
        days_ago = None
        track_pnl = {}
        #print(f"CHECK: self.dates_df: {self.dates_df} and self.dates_df[60:] {self.dates_df[60:]}")
        #dates from 60 days after start to end (inclusive?)
        await self.generate_universes(self.dates_df[60:])
        for index, day_of_sim in enumerate(tqdm(pd.to_datetime(self.dates_df[60:].index))):
            print(f"Today: {day_of_sim}")
            #print(f"index: {index}, current day of simulation: {day_of_sim}, date_type: {type(day_of_sim)}")
            day_of_data = self.strategy.get_n_trading_day_ago(1, use_yesterday=True, end_date=day_of_sim, full=False)
            print(f"YESTERDAY: {day_of_data}")

            #YESTERDAY
            day_of_data_str = day_of_data.strftime('%Y-%m-%d') if type(day_of_data) != str else day_of_data
            #print(f"day_of_data_str: {day_of_data_str}")
            data_start_time = self.dates_df.loc[day_of_data_str, 'market_open'].strftime('%Y-%m-%dT%H:%M:%S+00:00')
            #print(f"start_time of YESTERDAY: {day_of_data_str}: start: {data_start_time}")
            data_end_time = self.dates_df.loc[day_of_data_str, 'market_close'].strftime('%Y-%m-%dT%H:%M:%S+00:00')
            #print(f"end_time of of YESTERDAY: {day_of_data_str}: end: {data_end_time}")
            #TODAY
            day_of_sim_str = day_of_sim.strftime('%Y-%m-%d') if type(day_of_sim) != str else day_of_sim
            print(day_of_sim_str)
            sim_start_time = self.dates_df.loc[day_of_sim_str, 'market_open'].strftime('%Y-%m-%dT%H:%M:%S+00:00')
            #print(f"start_time of current day: {sim_start_time}")
            sim_end_time = self.dates_df.loc[day_of_sim_str, 'market_close'].strftime('%Y-%m-%dT%H:%M:%S+00:00')
            #print(f"end_time of current day: {sim_end_time}")

            window_start = self.strategy.get_n_trading_day_ago(60, use_yesterday=True, end_date=day_of_sim, full=False)
            #print(f"window_start, 60 trading days prior including yesterday: {window_start}")


            #day_before = self.strategy.get_n_trading_day_ago(0, end_date=date, full=False)
            #print(f"Date: {date}, Day Before: {day_before} is {type(day_before)}")
            if window_start is not None:
                try:
                    trade = Trade(day_of_sim, self.booksize, self.strategy)
                    #print(f"Trade: {trade}")
                    #if not first day.
                    if index != 0:
                        #print(f"index is {index} for day_of_sim: {day_of_sim}, not the first day (not 0)")
                        #print(f"Calling trade.close_port with args: current day_of_sim: {day_of_sim}, day_of_data (prior): {day_of_data}, date_open: {sim_start_time}, date_close: {sim_end_time}, portfolio: {self.portfolio}")
                        #print(f"TOTAL MARKET VALUE: {await self.metrics.total_market_value(self.portfolio)}")
                        print(f"CHECK MARKET VALUE: {await self.portfolio_tests.check_total_market_value(self.portfolio)}")
                        print(f"CHECK TRUNCATION: {await self.portfolio_tests.check_truncation(self.portfolio)}")
                        # print(f"CHECK DOLLAR NEUTRALITY: {await self.portfolio_tests.check_dollar_neutrality(self.portfolio)}")
                        print(
                            f"LONG COUNT: {await self.portfolio_tests.count_longs(self.portfolio)}")
                        print(
                            f"SHORT COUNT: {await self.portfolio_tests.count_shorts(self.portfolio)}")

                        self.portfolio = await trade.close_port(day_of_sim, day_of_data, sim_start_time, sim_end_time, self.portfolio)
                        #print(f"{day_of_sim_str}: Portfolio Sold")
                        total_pnl = await trade.calculate_total_pnl(self.portfolio)
                        track_pnl[day_of_sim_str] = total_pnl
                        print(f"Date: {day_of_sim}, PnL: {total_pnl}")

                        portfolio_document = {
                            'Date Purchased': day_of_data_str,
                            'Portfolio PnL': total_pnl,
                            'Portfolio': {
                                ticker: {
                                    'Execution Price': self.portfolio[ticker]['execution_price'],
                                    'Close Price': self.portfolio[ticker]['close_price'],
                                    'PnL': self.portfolio[ticker]['pnl']
                                    # 'Weight': trades[day_of_sim].get(ticker, 0),  # Check if ticker exists in trades
                                    # 'Market Neutral': market_neutral.get(ticker, None),
                                    # 'Arc_Tan': arc_tan.get(ticker, None),
                                    # 'Days Ago Value': days_ago.get(ticker, None)
                                }
                                for ticker in self.portfolio.keys()
                            }
                        }
                        await mongo.store_portfolio_data(portfolio_document)
                        requests.post(nfty_url, data=f"Date: {day_of_data_str}, PnL: {total_pnl}".encode(encoding='utf-8'))

                        #print("Request sent with total pnl computation")
                    else:
                        print(f"It is the first day: {day_of_sim}, there is no Portfolio to sell")

                    # record PnL, Metrics, trades
                    #if it is not the last day
                    print(f"{index+1}/{len(self.dates_df[60:])}") #0 indexed
                    if index+1 != len(self.dates_df[60:]):
                        #print(f"It is not the last day, it is {day_of_sim} which is index: {index}")
                        #Calculate Universe
                        # await poly_mongo('market_cap', window_start)
                        # print(f"Universe Built for day: {window_start}") #Yesterday's Market Cap data - What happens if a stock gets delisted today?
                        # await poly_mongo('market_cap', day_of_data_str)
                        #FETCH UNIVERSE FROM MONGO
                        universe = await mongo.fetch_universe(day_of_data_str)

                        if universe is None:
                            print(f"No universe data found for {day_of_data_str}, skipping this day.")
                            continue

                        #print(f"Universe Created with Day of Data, date: {day_of_data_str}")

                        #Calculate Weights
                        method = 'market_cap'
                        trades[day_of_sim], market_neutral, arc_tan, days_ago = await run_strategy_for_period(window_start, day_of_data_str, self.dates_df, stock_list=universe, method=method)
                        #print(f"Trades[{day_of_sim}] = {trades[day_of_sim]} for args: Start Date: {window_start}, End Date: {day_of_data}, Dates Dataframe: {self.dates_df}, Method: {method}")
                        # print(trades[date])
                        #Buy normalized weights * booksize

                        #price_data = PriceData(date)
                        df = await trade.get_trade_prices(normalized_weights=trades[day_of_sim], date_open=sim_start_time, date_close=sim_end_time)
                        #print(f"DF from trade.get_trade_prices: {df} with args {trades[day_of_sim]} of date: {day_of_sim} with open: {sim_start_time} and close: {sim_end_time}")
                        self.portfolio = await trade.open_port(trades[day_of_sim], df)
                        #print(f"{day_of_sim}: Portfolio Purchased  with weights {trades[day_of_sim]} and price data: {df}: {self.portfolio}, bought")
                        #Portfolio is invested.
                        #prior = day_of_sim
                        #print(f"prior: {prior} ")
                        #Wait for next day (no code needed, loop will iterate)

                except KeyError as e:
                    print(f"KeyError for day_of_data {day_of_data}: {e}")
            else:
                print(f"Skipping {day_of_data} as no window start could be determined.")
        total_pnl = await self.global_trade.calculate_entire_pnl(track_pnl)
        print(f"Total PnL: {total_pnl}")
        requests.post(nfty_url, data=f"Backtest Complete, total pnl: {total_pnl}".encode(encoding='utf-8'))
        return trades

    async def generate_universes(self, dates_df):
        dates = list(dates_df.index)
        tasks = [self.generate_universe_for_day(day) for day in dates]
        await asyncio.gather(*tasks)

    async def generate_universe_for_day(self, day):
        day_of_data = self.strategy.get_n_trading_day_ago(1, use_yesterday=True, end_date=day, full=False)
        day_of_data_str = day_of_data.strftime('%Y-%m-%d')
        if await mongo.universe_exists(day_of_data_str):
            print("Universe Exists Already")
            return  # Skip if universe already exists for this day


        universe = await poly_mongo('market_cap', day_of_data_str)
        await mongo.store_universe(day_of_data_str, universe)

if __name__ == "__main__":
    mongo = Mongo('newsmins10pctdwn')

    #sim = Simulation('2018-05-01', '2018-08-01')  # Define your backtest period
    sim = Simulation('2018-05-01', '2018-08-31')
    asyncio.run(sim.run_backtest())
