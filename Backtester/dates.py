import pandas as pd
import pandas_market_calendars as mcal
from datetime import datetime, timezone


class Backtest:
    def __init__(self, exchange='NASDAQ'):
        self.exchange = exchange
        self.calendar = mcal.get_calendar(exchange)

    def get_schedule(self, start_date, end_date):
        return self.calendar.schedule(start_date=start_date, end_date=end_date)

    def get_trading_days(self, start_date, end_date, full=True):
        schedule = self.get_schedule(start_date, end_date)
        if full == False:
            return mcal.date_range(schedule, frequency='1D')
        else:
            return schedule


class Strategy:
    def __init__(self, backtest):
        self.backtest = backtest

    def get_n_trading_day_ago(self, n, use_yesterday=True, end_date=None, window=False, full=True):
        #print(f"n: {n}, end_date={end_date}, window={window}, full={full}")

        #print(type(end_date))
        if end_date == None:
            end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        if use_yesterday: end_date = end_date - pd.DateOffset(days=1)
        complete_date_period = self.backtest.get_trading_days(start_date='2018-05-01', end_date=end_date)
        #print(f"{complete_date_period}, return {complete_date_period.tail(n)}")
        n_day_ago = complete_date_period.tail(n)
        #print(f"Type of n_day_ago: {type(n_day_ago)} is {n_day_ago.head(1)} but full is {full} so return {n_day_ago.index[0]}")
        return n_day_ago if full else n_day_ago.index[0]
