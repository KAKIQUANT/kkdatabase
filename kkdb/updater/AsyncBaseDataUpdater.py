import pandas as pd
from loguru import logger
import asyncio
import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
import numpy as np
from typing import Optional, Iterable, Tuple

from numpy import signedinteger
from numpy._typing import _64Bit
from pymongo.errors import BulkWriteError, DuplicateKeyError
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
        self.db_name = db_name
        self.client = AsyncIOMotorClient(client_str)
        self.db: AsyncIOMotorDatabase = self.client[db_name]
        self.bar_sizes: Iterable[str] = bar_sizes
        self.market_url: str
        self.headers: dict[str, str]
        self.session: Optional[aiohttp.ClientSession] = None
        self.resolver: aiohttp.AsyncResolver | None = resolvers
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)

    async def drop_db(self, refresh: bool = False):
        if refresh:
            await self.client.drop_database(name_or_database=self.db_name)
            logger.info(f"Dropped database {self.db_name}.")
        else:
            logger.info("Skipping database drop.")

    async def create_timeseries_collection(self, collection_name: str, time_field: str):
        """
        Create a time-series collection if it doesn't already exist.
        """
        collections = await self.db.list_collection_names()
        if collection_name not in collections:
            await self.db.create_collection(
                collection_name,
                timeseries={
                    'timeField': time_field,
                    'metaField': 'metadata',
                    'granularity': 'seconds'
                }
            )
            logger.info(f"Created time-series collection for {collection_name}.")

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
            except DuplicateKeyError:
                logger.warning("Duplicate key encountered, skipping...")
            except Exception as e:
                logger.error(f"Error inserting data: {e}")
        else:
            logger.info(f"No new data to insert into {collection_name}.")

    async def check_existing_data(self, inst_id: str, bar: str) -> Tuple[Optional[np.int64], Optional[np.int64]]:
        """
        Finds the earliest and latest timestamps in the MongoDB collection for the given instrument and bar.
        Returns the earliest and latest timestamps in milliseconds.
        """
        collection = self.db[f"kline-{bar}"]
        pipeline = [
            {"$match": {"instId": inst_id}},
            {"$group": {
                "_id": None,
                "earliest": {"$min": "$timestamp"},
                "latest": {"$max": "$timestamp"}
            }}
        ]
        result = await collection.aggregate(pipeline).to_list(length=1)
        if result:
            earliest_timestamp = result[0]["earliest"]
            latest_timestamp = result[0]["latest"]
        else:
            earliest_timestamp = latest_timestamp = None

        logger.debug(
            f"Found existing data for {inst_id} {bar} start from {earliest_timestamp} to end {latest_timestamp}."
        )
        return (
            np.int64(earliest_timestamp.timestamp() * 1000) if earliest_timestamp else None,
            np.int64(latest_timestamp.timestamp() * 1000) if latest_timestamp else None
        )

    async def check_missing_data(self, inst_id: str, bar: str) -> bool:
        """
        Checks if there is missing data in the MongoDB collection.
        """
        collection = self.db[f"kline-{bar}"]
        pipeline = [
            {"$match": {"instId": inst_id}},
            {"$sort": {"timestamp": 1}},
            {"$group": {
                "_id": None,
                "timestamps": {"$push": "$timestamp"}
            }}
        ]
        result = await collection.aggregate(pipeline).to_list(length=1)
        if result:
            timestamps = pd.Series(result[0]["timestamps"])
            continuous = timestamps.diff().dt.total_seconds().dropna().eq(60).all()
            return continuous
        return True


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

    async def fetch_kline_data(
            self, inst_id: str, bar: str, sleep_time: int = 1, limit: int = 100
    ):
        raise NotImplementedError

    @abstractmethod
    async def initialize_update(self):
        raise NotImplementedError

    async def main(self):
        await self.drop_db(refresh=False)
        for bar_size in self.bar_sizes:
            await self.create_timeseries_collection(f"kline-{bar_size}", 'timestamp')
        await self.start_session()
        logger.info("Starting data update...")
        await self.initialize_update()
        await self.close_session()
        logger.info("Data update completed.")


if __name__ == "__main__":
    updater = AsyncBaseDataUpdater()
    # asyncio.run(updater.main())
    # Check conpound index
    asyncio.run(updater.check_index())
