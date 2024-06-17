import pandas as pd
from loguru import logger
import asyncio
import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
import numpy as np
from typing import Optional, Iterable, Tuple

from numpy import signedinteger
from numpy._typing import _64Bit
from pymongo.errors import BulkWriteError
from kkdb.utils.check_db import get_client_str
from abc import ABC, abstractmethod


class AsyncBaseDataUpdater(ABC):
    def __init__(
            self,
            db_name: str,
            bar_sizes: Iterable[str],
            max_concurrent_requests: int = 3,
            client_str: str = get_client_str(),
            resolvers: Optional[aiohttp.resolver.AsyncResolver] = None,
    ) -> None:
        self.client = AsyncIOMotorClient(client_str)
        self.db: AsyncIOMotorDatabase = self.client[db_name]
        self.bar_sizes: Iterable[str] = bar_sizes
        self.market_url: str
        self.headers: dict[str, str]
        self.session: Optional[aiohttp.ClientSession] = None
        self.resolver: aiohttp.AsyncResolver | None = resolvers
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)

    # For MongoDB only
    async def check_index(self):
        """
        Set up compound indexes for each collection in the MongoDB database.
        """
        collections = await self.db.list_collection_names()
        print(collections)
        for collection_name in collections:
            collection = self.db[collection_name]
            # Check if the compound index exists, exclude the original index
            list_of_indexes = await collection.list_indexes().to_list(length=None)
            if not any(
                    index["key"] == [("instId", 1), ("timestamp", 1)]
                    for index in list_of_indexes
            ):
                await collection.create_index(
                    [("instId", 1), ("timestamp", 1)], unique=True
                )
                logger.info(f"Created compound index for {collection_name}.")
            else:
                logger.info(f"Compound index for {collection_name} already exists.")

    async def drop_db(self, refresh: bool = False, db_name: str = "crypto"):
        if refresh:
            await self.client.drop_database(name_or_database=db_name)
            logger.info(f"Dropped database {db_name}.")
        else:
            logger.info("Skipping database drop.")

    async def start_session(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
            self.session = aiohttp.ClientSession(timeout=timeout)

    async def close_session(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def insert_data(
            self, collection_name: str, data: pd.DataFrame
    ) -> None:
        if not data.empty:
            try:
                collection = self.db[collection_name]
                data_dict = data.to_dict("records")
                await collection.insert_many(data_dict)  # type: ignore
                logger.info(
                    f"Inserted {len(data_dict)} new records into {collection_name} asynchronously."
                )
            except BulkWriteError:
                logger.warning("Writing duplicate data encountered, skipping...")

    async def check_existing_data(self, inst_id: str, bar: str) -> tuple[np.int64, np.int64]:
        """
        Finds the latest timestamp in the MongoDB collection.
        Returns the earliest and latest timestamps in milliseconds.
        """
        collection = self.db[f"kline-{bar}"]
        latest_doc = (
            collection.find({"instId": inst_id}, {"timestamp": 1})
            .sort("timestamp", -1)
            .limit(1)
        )
        latest_timestamp = await latest_doc.to_list(length=1)
        latest_timestamp = (
            latest_timestamp[0]["timestamp"] if latest_timestamp else None
        )
        earliest_doc = (
            collection.find({"instId": inst_id}, {"timestamp": 1})
            .sort("timestamp", 1)
            .limit(1)
        )
        earliest_timestamp = await earliest_doc.to_list(length=1)
        earliest_timestamp = (
            earliest_timestamp[0]["timestamp"] if earliest_timestamp else None
        )

        logger.debug(
            f"Found existing data for {inst_id} {bar} start from {earliest_timestamp} to end {latest_timestamp}."
        )
        # Convert datetime timestamp to timestamp in milliseconds in np.int64 format
        return (
            np.int64(earliest_timestamp.timestamp() * 1000)
            if earliest_timestamp
            else np.int64(0)
            ,
            np.int64(latest_timestamp.timestamp() * 1000)
            if latest_timestamp
            else np.int64(0)
        )

    async def check_missing_data(self, inst_id, bar):
        """
        Checks if there is missing data in the MongoDB collection.
        """
        collection = self.db[f"kline-{bar}"]
        latest_doc = collection.find({"instId": inst_id}, {"timestamp": 1}).sort(
            "timestamp", -1
        )
        # Get all of them and convert to pd.DataFrame
        df = pd.DataFrame(await latest_doc.to_list(length=None))
        # Check the timeseries is continuous
        return df["timestamp"].diff().dt.total_seconds().dropna().eq(60).all()

    async def fix_missing_data(self, inst_id, bar):
        """
        Fixes missing data in the MongoDB collection.
        """
        collection = self.db[f"kline-{bar}"]
        latest_doc = collection.find({"instId": inst_id}, {"timestamp": 1}).sort(
            "timestamp", -1
        )
        # Get all of them and convert to pd.DataFrame
        df = pd.DataFrame(await latest_doc.to_list(length=None))
        # Check the timeseries is continuous
        if not df["timestamp"].diff().dt.total_seconds().dropna().eq(60).all():
            # Get the missing timestamps
            missing_ts = pd.date_range(
                start=df["timestamp"].min(), end=df["timestamp"].max(), freq="1min"
            ).difference(df["timestamp"])
            # Create a new DataFrame with the missing timestamps
            missing_df = pd.DataFrame({"timestamp": missing_ts})
            # Insert the missing data into the collection
            await self.insert_data_to_mongodb(f"kline-{bar}", missing_df)
            logger.info(
                f"Inserted {len(missing_df)} missing records into {inst_id} {bar}."
            )

    @abstractmethod
    async def fetch_one(
            self, inst_id: str, bar: str, before: np.int64, after: np.int64, limit: int
    ):
        """
        Fetches data for a single instrument and bar type. Will ensure return a pd.Dataframe if data exist in the before-after range.
        :param inst_id: Instrument ID
        :param bar: Bar type
        :param before: Timestamp in milliseconds
        :param after: Timestamp in milliseconds
        :param limit: Number of records to fetch
        :return: pd.DataFrame or None
        """
        raise NotImplementedError

    @abstractmethod
    async def fetch_kline_data(
            self, inst_id: str, bar: str, sleep_time: int = 1, limit: int = 100
    ):
        raise NotImplementedError

    @abstractmethod
    async def initialize_update(self):
        raise NotImplementedError

    async def main(self):
        await self.drop_db(refresh=False)
        await self.check_index()
        await self.start_session()
        await self.initialize_update()
        await self.close_session()
        await self.check_index()


if __name__ == "__main__":
    updater = AsyncBaseDataUpdater()
    # asyncio.run(updater.main())
    # Check conpound index
    asyncio.run(updater.check_index())
