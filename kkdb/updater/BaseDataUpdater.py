from abc import ABC, abstractmethod
from typing import Iterable
from kkdb.utils.check_db import get_client_str
from pymongo import MongoClient
from loguru import logger
import pandas as pd

class BaseDataUpdater(ABC):
    def __init__(self, 
                 db_name: str,
                 bar_sizes: Iterable[str],
                 client_str: str = get_client_str(),
    ) -> None:
        self.client = MongoClient(client_str)
        self.db_name = db_name
        self.db = self.client[db_name]
        self.bar_sizes = bar_sizes
        self.market_type = None
        self.market_name = None

    def _drop_db(self,  db_name: str, refresh: bool = False,):
        if refresh:
            self.client.drop_database(name_or_database=db_name)
            logger.info(f"Dropped database {db_name}.")
        else:
            logger.info("Skipping database drop.")

    @abstractmethod
    def _process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def _get_stock_list(self) -> list:
        pass

    def _insert_data(self, collection_name: str, data: pd.DataFrame) -> None:
        if not data.empty:
            collection = self.db[collection_name]
            data_dict = data.to_dict("records")
            collection.insert_many(data_dict)
            logger.info(
                f"Inserted {len(data_dict)} new records into {collection_name}."
            )
    
    def check_index(self) -> None:
        collections = self.db.list_collection_names()
        logger.info(f"Checking indexes for {self.db_name}...")
        for collection_name in collections:
            collection = self.db[collection_name]
            lists_of_indexes = collection.list_indexes().to_list()
            logger.info(f"Indexes for {collection_name}: {lists_of_indexes}")
            if not any(
               index['key'] == [('orderbook_id', 1), ('datetime', 1)] for index in lists_of_indexes
            ):
                collection.create_index([("orderbook_id", 1), ("datetime", 1)], unique=True)
                logger.info(f"Index created for {collection_name}.")
            else:
                logger.info(f"Index already exists for {collection_name}.")

    @abstractmethod
    def _single_download(self, stock_code: str, freq: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def pool_download(self, stock_list: list, num_workers: int = 5):
        pass

    def main(self):
        self._drop_db(self.db.name)
        self.check_index()
        stock_list = self._get_stock_list()
        self.pool_download(stock_list)

if __name__ == "__main__":
    pass