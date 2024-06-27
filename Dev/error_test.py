from typing import List, Dict
from multiprocessing import Value, Lock
import databento as db
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from collections import defaultdict
from dotenv import load_dotenv
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from tqdm import tqdm

from pymongo import UpdateOne, InsertOne
from motor.motor_asyncio import AsyncIOMotorClient
import aiohttp
import asyncio
from multiprocessing import Pool
from pandas import date_range, Timestamp, Series

load_dotenv(dotenv_path=".ENV")
api_key = os.environ.get("test")

client = db.Historical(api_key)


async def fetch_single_date(symbols, day_str, extended_time=True):
    print(f"Fetching data for symbols: {symbols}, date: {day_str}, extended_time: {extended_time}")
    start_time = f"{day_str}T08:00:00+00:00" if extended_time else f"{day_str}T13:30:00+00:00"
    end_time = f"{day_str}T23:59:00+00:00" if extended_time else f"{day_str}T20:00:00+00:00"
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


async def fetch_days_10pct_drop(raw_symbol, start_date, end_date):
    # this is being called for every single stock individually.
    coro = await client.timeseries.get_range_async(
        dataset="XNAS.ITCH",
        schema="ohlcv-1d",
        stype_in="raw_symbol",
        symbols=[raw_symbol],
        start=start_date,
        end=end_date,
        # start="2018-05-01",
        # end="2023-08-25",
    )
    df = coro.to_df()
    # if [df[symbol].empty for symbol in raw_symbol]:
    #     print(f"No data for symbol {raw_symbol} between {start_date} and {end_date}")
    #     return None

    # Calculate and find days with a drop of 10% or more
    df['pct_change'] = (df['close'] - df['open']) / df['open']
    df['pct_change'] = round(df['pct_change'] * 100, 2)
    drops_10_pct_or_more = df[df['pct_change'] <= -10]

    print(f"drops_10_pct_or_more: {drops_10_pct_or_more}")

    return drops_10_pct_or_more


async def check_active(raw_symbol, start_date, end_date):
    # this is being called for every single stock individually.
    coro = client.timeseries.get_range(
        dataset="XNAS.ITCH",
        schema="ohlcv-1h",
        stype_in="raw_symbol",
        symbols=[raw_symbol],
        start=start_date,
        end=end_date,
        # start="2018-05-01",
        # end="2023-08-25",
    )
    df = coro.to_df()
    print(df)
    return df


if __name__ == "__main__":
    asyncio.run(check_active(['BRQS'], '2018-08-31T', '2018-08-31'))
    # Start: 2023-10-09 05:12:39.106137, End: 2023-12-08 05:12:39.106137
