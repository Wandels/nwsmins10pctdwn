import requests
import os
from dotenv import load_dotenv
import pandas as pd
import asyncio
import aiohttp
import functools
import json
import multiprocessing
from urllib.parse import urlencode
from pymongo.errors import BulkWriteError
from tqdm import tqdm
import aiohttp
import numpy as np
from pymongo import UpdateOne, InsertOne
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
import pandas_market_calendars as mcal
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from Backtester import dates

from typing import Optional, List, Union, Literal, Dict, Tuple
from dataclasses import dataclass

import gc

from urllib.parse import urlencode

load_dotenv(dotenv_path="../.ENV")
poly_api_key = os.environ.get("POLYGON_APIKEY_MEMBER")
mongo_username = os.environ.get("MONGO_USER")
mongo_pass = os.environ.get("MONGO_PASS")


async def fetch_ticker_data(session, url):
    async with session.get(url) as response:
        if response.status == 200:
            return await response.json()
    return None


async def connect_to_mongo_async(uri: str, db_name: str):
    client = AsyncIOMotorClient(uri, tls=True, tlsAllowInvalidCertificates=True)
    try:
        client.admin.command('ping')
        #print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    db = client[db_name]
    return db


async def insert_data_to_mongo_async(db, collection_name: str, data: dict):
    collection = db[collection_name]
    await collection.delete_many({})
    operations = [InsertOne({'ticker': ticker, collection_name: shares}) for ticker, shares in data.items()]
    result = await collection.bulk_write(operations)
    #print(f"Inserted {result.inserted_count} documents into {collection_name}")

async def get_ticker_data(api_key: str, session, tickers, data_type, date=None):
    col = {}
    tasks = []
    for ticker in tickers:
        url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?date={date}&apiKey={api_key}"
        tasks.append(fetch_ticker_data(session, url))

    responses = await asyncio.gather(*tasks)
    for response in responses:
        if response and 'results' in response and data_type in response['results']:
            col[response['results']['ticker']] = response['results'][data_type]

    return col, data_type


def get_tickers(api_key: str, date, return_format: Literal["dataframe", "list", "json"] = "list") \
        -> Union[pd.DataFrame, List[str]]:
    """ Fetch tickers from the Polygon API and return a dataframe, a list, or json."""
    base_url = "https://api.polygon.io/v3/reference/tickers"
    params = {
        "type": "CS",
        "market": "stocks",
        "active": "true",
        "order": "desc",
        "limit": 1000,
        "date": date,
        "apiKey": api_key
    }

    with requests.Session() as session:
        batches = []
        while base_url:
            try:
                response = session.get(base_url, params=params)
                response.raise_for_status()
                response_json_data = response.json()
                if 'results' in response_json_data:
                    batches.append(response_json_data['results'])
                    base_url = response_json_data.get("next_url")
                else:
                    break
            except requests.RequestException as e:
                print(f"Error fetching data from URL: {base_url}. Error: {e}")
                raise e
    if return_format == "dataframe":
        return pd.DataFrame([item for batch in batches for item in batch])
    elif return_format == "json":
        return [item for batch in batches for item in batch]
    else:  # 'list'
        return [item['ticker'] for batch in batches for item in batch]


async def get_shares_outstanding(api_key: str, session, date):
    all_tickers = get_tickers(api_key, date, return_format="list")
    return await get_ticker_data(api_key, session, all_tickers, 'weighted_shares_outstanding', date)

async def get_market_cap(api_key: str, session, date):
    all_tickers = get_tickers(api_key, date, return_format="list")
    return await get_ticker_data(api_key, session, all_tickers, 'market_cap', date)




async def poly_mongo(method, date):
    db_name = "newsmins10pctdwn"
    mongo_uri = f"mongodb+srv://{mongo_username}:{mongo_pass}@{db_name}.yxoglab.mongodb.net/?retryWrites=true&w=majority"
    db = await connect_to_mongo_async(mongo_uri, db_name)

    async with aiohttp.ClientSession() as session:
        if method == 'get_shares_outstanding':
            dict, col_name = await get_shares_outstanding(poly_api_key, session, date)
        elif method == 'market_cap':
            dict, col_name = await get_market_cap(poly_api_key, session, date)
        else:
            print("must specify method")

    await insert_data_to_mongo_async(db, col_name, dict)
    return dict


if __name__ == "__main__":
    asyncio.run(poly_mongo('market_cap', dates.yesterday))
