import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv(dotenv_path="../.ENV")
mongo_username = os.environ.get("MONGO_USER")
mongo_pass = os.environ.get("MONGO_PASS")



class Mongo:
    _instance = None

    @classmethod
    def get_instance(cls, db_name):
        if cls._instance is None:
            cls._instance = cls(db_name)
        return cls._instance

    def __init__(self, db_name):
        if Mongo._instance is not None:
            raise Exception("This class is a singleton!")
        else:
            self.db_name = db_name
            self.mongo_uri = f"mongodb+srv://{mongo_username}:{mongo_pass}@{db_name}.yxoglab.mongodb.net/?retryWrites=true&w=majority"
            self.client = AsyncIOMotorClient(self.mongo_uri, tls=True, tlsAllowInvalidCertificates=True)
            self.db = self.client[db_name]
            Mongo._instance = self

    async def universe_exists(self, date):
        collection = self.db['universes']
        count = await collection.count_documents({'date': date})
        return count > 0

    async def fetch_universe(self, date):
        collection = self.db['universes']
        document = await collection.find_one({'date': date})
        if document and 'data' in document:
            # Sort by market cap and get the top 3000
            sorted_data = sorted(document['data'].items(), key=lambda x: x[1], reverse=True)
            print(f"{date} has {len(sorted_data)} tickers")
            # Extract and return just the ticker names
            tickers = [ticker for ticker, cap in sorted_data[:3000]]

            return tickers

    async def store_universe(self, date, universe_data):
        collection = self.db['universes']
        await collection.insert_one({'date': date, 'data': universe_data})
        print(f"Inserted {date} universe")


    async def insert_data(self, collection_name, data_list):
        collection = self.db[collection_name]
        await collection.insert_many(data_list)

    async def clear_database(self, preserve_universes=True):
        collections = await self.db.list_collection_names()
        for collection_name in collections:
            if preserve_universes and collection_name == 'universes':
                continue
            await self.db[collection_name].drop()

    async def fetch_data(self, collection_name, query={}, fields=None, sort=None, limit=None):
        collection = self.db[collection_name]
        cursor = collection.find(query, fields)
        if sort:
            cursor = cursor.sort(sort)
        if limit:
            cursor = cursor.limit(limit)
        return [doc async for doc in cursor]

    async def insert_alpha_weights(self, collection_name, market_neutral, arc_tan, days_ago):
        data_list = [
            {
                'ticker': ticker,
                'market_neutral_weight': market_neutral.get(ticker),
                'arc_tan_value': arc_tan.get(ticker),
                'days_ago_value': days_ago.get(ticker)
            }
            for ticker in market_neutral
        ]
        await self.insert_data(collection_name, data_list)

    async def store_portfolio_data(self, portfolio_data):
        collection = self.db['portfolios']
        await collection.insert_one(portfolio_data)


