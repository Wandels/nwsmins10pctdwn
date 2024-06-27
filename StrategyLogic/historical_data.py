from typing import List, Dict
from multiprocessing import Value, Lock
import databento as db
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from Backtester.dates import Strategy, Backtest
import pandas as pd
import numpy as np
from datetime import datetime
from DataManager.mongo import *
import asyncio
from multiprocessing import Pool

load_dotenv(dotenv_path="../.ENV")
api_key = os.environ.get("test")  # prod for production, do not use

client = db.Historical(api_key)


# def is_trading_day(date, client):
#     start_time = f"{date}T08:00:00+00:00"
#     end_time = f"{date}T23:59:00+00:00"
#
#     try:
#         test_data = client.timeseries.get_range(
#             dataset="XNAS.ITCH",
#             schema="ohlcv-1h",
#             stype_in="raw_symbol",
#             symbols=["AAPL", "MSFT", "NVDA"],  # Use common stocks for testing
#             start=start_time,
#             end=end_time,
#         )
#         status = test_data.to_df()
#
#         # Check if the DataFrame is empty
#         if status.empty:
#             return False
#         else:
#             return True
#
#     except db.common.error.BentoClientError as e:
#         return False


async def fetch_days_10pct_drop(raw_symbol, start_date, end_date):
    coro = await client.timeseries.get_range_async(
        dataset="XNAS.ITCH",
        schema="ohlcv-1d",
        stype_in="raw_symbol",
        symbols=[raw_symbol],
        start=start_date,
        end=end_date,
    )
    df = coro.to_df()

    # Calculate and find days with a drop of 10% or more
    df['pct_change'] = (df['close'] - df['open']) / df['open']
    df['pct_change'] = round(df['pct_change'] * 100, 2)
    drops_10_pct_or_more = df[df['pct_change'] <= -10]

    return drops_10_pct_or_more


# def plot_price_data(raw_symbol, minute_df, day_str):
#     plt.figure(figsize=(15, 7))
#     plt.plot(minute_df.index, minute_df['low'], label='Low Price', color='red')
#     plt.plot(minute_df.index, minute_df['high'], label='High Price', color='green')
#     plt.title(f"{raw_symbol} Price Data on {day_str}")
#     plt.xlabel("Time")
#     plt.ylabel("Price")
#     plt.legend()
#     plt.grid(True)
#     plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
#     plt.gca().xaxis.set_major_locator(mdates.HourLocator())
#     plt.xticks(rotation=45)
#     plt.tight_layout()  # Adjust the plot to fit better
#     plt.show()

async def plot_price_data(raw_symbol, minute_df, day_str, drop_start=None, drop_end=None):
    print(f"{raw_symbol} Plot: {day_str}, from {drop_start} to {drop_end}")
    print(minute_df)
    plt.figure(figsize=(15, 7))
    plt.plot(minute_df.index, minute_df['low'], label='Low Price', color='red')
    plt.plot(minute_df.index, minute_df['high'], label='High Price', color='green')

    # Mark the start and end of the steepest drop
    if drop_start is not None and drop_end is not None:
        plt.scatter(drop_start, minute_df.loc[drop_start, 'high'], color='blue', label='Drop Start', zorder=5)
        plt.scatter(drop_end, minute_df.loc[drop_end, 'low'], color='purple', label='Drop End', zorder=5)

    plt.title(f"{raw_symbol} Price Data on {day_str}")
    plt.xlabel("Time")
    plt.ylabel("Price")
    plt.legend()
    plt.grid(True)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator())
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def find_time_for_10pct_drop_accurate(df):
    # print(f"find_time_for_10pct_drop_accurate: {df}")
    max_high = -float('inf')
    initial_drop_info = {'start_time': None, 'duration': float('inf')}
    symbol = df['symbol'].iloc[0]
    # print(df)

    # Step 1: Find the first 10% drop
    for index, row in df.iterrows():
        if row['high'] > max_high:
            max_high = row['high']
            # print(f'max_high: {max_high} ')
        if max_high == -float('inf'):
            continue
        current_drop = (max_high - row['low']) / max_high * 100
        # print(f'current_drop: {current_drop} ')
        if current_drop >= 10:
            # print(f'found: {current_drop} ')
            initial_drop_info['start_time'] = index
            break  # Break the loop once the first 10% drop is found


    if initial_drop_info['start_time'] is not None:

        initial_duration = (df.index.get_loc(initial_drop_info['start_time']) + 1)  # Duration in minutes

        window_size = min(initial_duration, len(df) - df.index.get_loc(initial_drop_info['start_time']))

        fastest_drop_info = {'start_time': initial_drop_info['start_time'], 'duration': initial_duration,
                             'percentage_drop': current_drop}
        for i in range(len(df) - window_size + 1):
            window_df = df.iloc[i:i + window_size]
            window_high = window_df.iloc[0]['high']
            window_min = window_df['low'].min()
            pct_drop = (window_high - window_min) / window_high * 100

            if pct_drop >= 10:

                for j in range(window_size):

                    if (window_high - window_df.iloc[j]['low']) / window_high * 100 >= 10:

                        if j < fastest_drop_info['duration']:

                            if df.index[i + j] in df.index:
                                fastest_drop_info = {
                                    'start_time': df.index[i],
                                    'duration': j,
                                    'percentage_drop': pct_drop
                                }
                        break

        return {'symbol': symbol, **fastest_drop_info}
    else:

        return {'start_time': None, 'duration': None, 'percentage_drop': 0}


#


async def fetch_single_date(symbols, day_str, dates, extended_time=True):
    # print(f"Fetching data for symbols: {symbols}, date: {day_str}, extended_time: {extended_time}")
    # start_time = f"{day_str}T08:00:00+00:00" if extended_time else f"{day_str}T14:30:00+00:00"
    # end_time = f"{day_str}T23:59:00+00:00" if extended_time else f"{day_str}T21:00:00+00:00"
    start_time = dates.loc[day_str, 'market_open'].strftime('%Y-%m-%dT%H:%M:%S+00:00')
    end_time = dates.loc[day_str, 'market_close'].strftime('%Y-%m-%dT%H:%M:%S+00:00')
    try:
        minute_data = await client.timeseries.get_range_async(
            dataset="XNAS.ITCH",
            schema="ohlcv-1m",
            stype_in="raw_symbol",
            symbols=symbols,
            start=start_time,
            end=end_time,
        )


        return minute_data.to_df()

    except Exception as e:
        print(f"Error fetching data: {e}")
        raise


async def create_matrix(stock_list, start_date, end_date, dates_df, extended_time=False):

    grouped_results = {}


    max_symbols_per_batch = 2000
    batch_tasks = []
    for i in range(0, len(stock_list), max_symbols_per_batch):
        batch_symbols = stock_list[i:i + max_symbols_per_batch]
        batch_tasks.append(fetch_days_10pct_drop(batch_symbols, start_date, end_date))

    batches_results = await asyncio.gather(*batch_tasks)
    day_symbol_dict = to_dict(batches_results)
    tasks = [fetch_single_date(symbols, date, dates_df, extended_time) for date, symbols in day_symbol_dict.items()]
    min_dataframes = await asyncio.gather(*tasks)

    separated_dataframes = []
    for mixed_df in min_dataframes:
        for symbol in mixed_df['symbol'].unique():
            separated_dataframes.append(mixed_df[mixed_df['symbol'] == symbol])


    with Pool() as pool:
        drops = pool.map(find_time_for_10pct_drop_accurate, separated_dataframes)

    return drops

def ts_arg_min(data: List[Dict], date) -> Dict:

    current_date = datetime.strptime(date, '%Y-%m-%d').date()
    df = pd.DataFrame(data)
    df['start_time'] = pd.to_datetime(df['start_time'])
    min_duration_df = df.loc[df.groupby('symbol')['duration'].idxmin()]
    backtest = Backtest(exchange='NASDAQ')
    strategy = Strategy(backtest)

    # Calculate trading days ago from current date for each minimum duration
    min_duration_trading_days_ago = {}
    for _, row in min_duration_df.iterrows():
        start_date = row['start_time'].date()
        trading_days_df = backtest.get_trading_days(start_date=start_date, end_date=current_date, full=True)
        trading_days_count = len(trading_days_df.index)
        min_duration_trading_days_ago[row['symbol']] = trading_days_count

    return min_duration_trading_days_ago


def to_dict(data_list):
    combined_df = pd.concat(data_list)
    date_symbols_dict = {}
    for index, row in combined_df.iterrows():
        date = index.strftime('%Y-%m-%d')
        if date not in date_symbols_dict:
            date_symbols_dict[date] = []
        date_symbols_dict[date].append(row['symbol'])
    return date_symbols_dict


async def apply_transformations_and_neutralize(stock_list, start_date, end_date, dates_df, extended_time=False):
    stocks = await create_matrix(stock_list, start_date, end_date, dates_df, extended_time)
    days_ago_values = ts_arg_min(stocks, end_date)  # Actual days ago not trading days ago

    # Apply arc_tan transformation
    transformed_values = {stock: np.arctan(day) for stock, day in days_ago_values.items() if day is not np.nan}

    # Neutralize
    mean_value = np.mean(list(transformed_values.values()))
    neutralized_values = {stock: value - mean_value for stock, value in transformed_values.items()}

    return neutralized_values, transformed_values, days_ago_values

class ProgressTracker:
    def __init__(self, total):
        self.val = Value('i', 0)
        self.lock = Lock()
        self.total = total

    def update(self):
        with self.lock:
            self.val.value += 1

    def report_progress(self):
        with self.lock:
            print(f"Completed {self.val.value}/{self.total} tasks")


progress_tracker = ProgressTracker(3000)


class StrategyLogic:
    def __init__(self, stock_list, start_date, end_date, dates_df):
        self.stock_list = stock_list
        self.start_date = start_date
        self.end_date = end_date
        self.dates_df = dates_df

    async def start_strategy(self, stock_list, start_date, end_date, dates_df, mongo, method, extended_time=False):
        market_neutral, arc_tan, days_ago = await apply_transformations_and_neutralize(stock_list, start_date, end_date,
                                                                                       dates_df, False)
        # Insert data to MongoDB
        await mongo.insert_alpha_weights(f"{method}_weights_{end_date}_ext:{extended_time}", market_neutral, arc_tan,
                                         days_ago)
        return market_neutral, arc_tan, days_ago
