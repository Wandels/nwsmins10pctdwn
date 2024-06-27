import os
import pandas as pd
from databento import Historical
from datetime import timedelta, datetime

from DataManager.mongo import Mongo
from dotenv import load_dotenv


load_dotenv(dotenv_path="../.ENV")
api_key = os.environ.get("test")
client = Historical(api_key)


async def fetch_shares_outstanding(mongo):
    """ Fetch shares outstanding from MongoDB """
    documents = await mongo.fetch_data('weighted_shares_outstanding')
    shares_outstanding = {doc['ticker']: doc['weighted_shares_outstanding'] for doc in documents}
    return shares_outstanding



async def calculate_and_update_liquidity(mongo, count):
    shares_outstanding = await fetch_shares_outstanding(mongo)
    symbol_list = list(shares_outstanding.keys())
    end_date = datetime.now() - timedelta(days=1)  # As of yesterday
    start_date = end_date - timedelta(days=100)

    # Process in batches
    all_liquidity = {}
    max_symbols_per_batch = 2000
    for i in range(0, len(symbol_list), max_symbols_per_batch):
        batch_symbols = symbol_list[i:i + max_symbols_per_batch]
        data = client.timeseries.get_range(dataset="XNAS.ITCH", schema="ohlcv-1d",
                                           symbols=batch_symbols, start=start_date, end=end_date)
        batch_df = data.to_df()


        total_volume = batch_df.groupby('symbol')['volume'].sum()


        for symbol in batch_symbols:
            if symbol in total_volume and symbol in shares_outstanding:
                all_liquidity[symbol] = (total_volume[symbol] / 100) / shares_outstanding[symbol]


    top_symbols = pd.Series(all_liquidity).nlargest(count)


    await mongo.insert_data('liquidity', [{'ticker': symbol, 'liquidity': liquidity}
                                            for symbol, liquidity in top_symbols.items()])

    return top_symbols.index.tolist()

async def fetch_universe(count: int, method: str, update=False):
    mongo = Mongo.get_instance('newsmins10pctdwn')
    if update and method == 'liquidity':
        return await calculate_and_update_liquidity(mongo, count)
    if update and method == 'market_cap':
        print("CLEAR MONGO AND RUN IN BENTO_UNIVERSE.PY")
    else:
        documents = await mongo.fetch_data(method, fields={'ticker': 1, '_id': 0}, sort=[(method, -1)], limit=count)
        return [doc['ticker'] for doc in documents]
