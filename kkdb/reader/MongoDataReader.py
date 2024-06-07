from pymongo import MongoClient
from loguru import logger
from kkdb.utils.check_date import date_to_datetime
from kkdb.utils.check_db import get_client_str
from typing import Optional
import pandas as pd
from pymongo.collection import Collection
import matplotlib.pyplot as plt


class DownloadData:
    def __init__(self, market: str, db_str: str = None) -> None:
        if db_str is not None:
            self.client = MongoClient(db_str)
        else:
            self.client = MongoClient(get_client_str())
        self.db = self.client[market]
        self.market = market

    def find_one_sample(self, collection: Collection) -> dict:
        return collection.find_one()
    
    def download(
        self,
        symbol: str = "BTC-USDT-SWAP",
        bar: str = "1D",
        start_date: Optional[str | pd.Timestamp] = None,
        end_date: Optional[str | pd.Timestamp] = None,
        fields=None,
    ) -> pd.DataFrame:
        sample = self.find_one_sample(self.db[f"kline-{bar}"])
        print(sample)
        date_set = set(["date", "datetime", "timestamp"])
        ts = list(date_set.intersection(sample.keys()))[0]
        logger.info(f"Timestamp field: {ts}")
        order_book_set = set(["orderbook_id", "instId"])
        order_book_id = list(order_book_set.intersection(sample.keys()))[0]
        logger.info(f"Order book id field: {order_book_id}")
        query = {}
        if start_date is not None or end_date is not None:
            query[ts] = {}
            if start_date is not None:
                start_date = date_to_datetime(start_date)
                print(start_date)
                query[ts]["$gte"] = start_date
            if end_date is not None:
                end_date = date_to_datetime(end_date)
                print(end_date)
                query[ts]["$lte"] = end_date
        query[order_book_id] = symbol
        collection = self.db[f"kline-{bar}"]

        logger.info(query)
        projection = {}
        if fields == "full":
            projection = {}  # MongoDB returns all fields, including the objectId if projection is empty
        elif fields is None:
            projection = {"_id": 0}  # Return all fields except the objectId
        elif fields == "ohlcv":
            projection = {
                "_id": 0,
                ts: 1,
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 1,
            }  # Return OLHCV fields only
        elif isinstance(fields, list):
            projection = {"_id": 0, ts: 1}
            for field in fields:
                projection[field] = 1
        else:
            raise ValueError(
                "Invalid fields argument. Must be 'full', None, or a list of field names."
            )
        cursor = collection.find(query, projection)
        
        df = pd.DataFrame(list(cursor)).sort_values(by=ts, ascending=True)
        return df

    def describe(self):
        """
        Returns general information about the MongoDB database and collection.
        """
        return [
            dict(self.db.command("dbstats"), **self.get_collection_info()),
            {
                "database": self.db.name,
                "collection_names": self.db.list_collection_names(),
                "collection_count": len(self.db.list_collection_names()),
            },
        ]

    def get_collection_info(self):
        """
        Returns general information about the collection.
        """
        collection_info = {}
        for collection in self.db.list_collection_names():
            collection_info[collection] = self.db[collection].count_documents({})
        return collection_info

    def get_crypto_pairs(self, col) -> list:
        col = self.db[col]
        return col.distinct("instId")

    def get_collection_date_range(
        self, collection: Collection, instId: str, bar: str
    ) -> list:
        pipeline = [
            {
                "$group": {
                    "_id": None,
                    "start_date": {"$min": "$timestamp"},
                    "end_date": {"$max": "$timestamp"},
                    "instId": {"$first": "$instId"},
                    "bar": {"$first": "$bar"},
                }
            }
        ]
        result = list(collection.aggregate(pipeline))
        if result:
            start_date = result[0]["start_date"]
            end_date = result[0]["end_date"]
            return [start_date, end_date]
        else:
            return [None, None]

    @staticmethod
    def _get_price(
        market: str,
        order_book_id: str,
        bar: str,
        start_date: str,
        end_date: str,
        fields: list,
    ):
        reader = DownloadData(market)
        return reader.download(order_book_id, bar, start_date, end_date, fields)


if __name__ == "__main__":
    reader = DownloadData("crypto")
    data = reader.download(
        symbol="BTC-USDT-SWAP",
        bar="1D",
        start_date="2023-01-01",
        end_date="2024-01-01",
        fields=["open", "high", "low", "close"],
    )
    print(data)
    data.plot(x="timestamp", y="close")
    plt.show()
