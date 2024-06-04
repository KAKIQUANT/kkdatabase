import asyncio
import random
from typing import Iterable
from aiohttp import AsyncResolver
import numpy as np
import pandas as pd
from kkdb.utils.check_db import get_client_str
from kkdb.updater.AsyncBaseDataUpdater import AsyncBaseDataUpdater
from loguru import logger


class AsyncOkxCandleUpdater(AsyncBaseDataUpdater):
    def __init__(
        self,
        db_name: str = "crypto_okx",
        bar_sizes: Iterable[str] = [
            "1m",
            "3m",
            "5m",
            "15m",
            "30m",
            "1H",
            "4H",
            "1D",
            "1W",
        ],
        max_concurrent_requests: int = 3,
        client_str: str = get_client_str(),
        resolvers: AsyncResolver | None = None,
    ) -> None:
        super().__init__(
            db_name, bar_sizes, max_concurrent_requests, client_str, resolvers
        )
        self.market_url = "https://www.okx.com/api/v5/market/history-candles"
        self.headers = {
            "User-Agent": "PostmanRuntime/7.36.3",
            "Accept": "*/*",
            "b-locale": "zh_CN",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Host": "www.okx.com",
            "Referer": "https://www.okx.com/",
        }

    async def fetch_one(
        self, inst_id: str, bar: str, before: np.int64, after: np.int64, limit: int
    ):
        """
        Fetches data for a single instrument and bar size. Will ensure return a pd.Dataframe if data exist in the before-after range.
        """
        params = {
            "instId": inst_id,
            "before": str(before),
            "after": str(after),
            "bar": bar,
            "limit": str(limit),
        }

        async with self.semaphore:
            if self.session is not None:
                while True:
                    async with self.session.get(
                        self.market_url,
                        params=params,
                        headers=self.headers,
                        resolver=self.resolver,
                    ) as response:
                        if response.status == 200:
                            result = await response.json()
                            if not result["data"]:
                                logger.info(
                                    f"No more data to fetch or empty data returned for {inst_id}-{bar}."
                                )
                                return None
                            else:
                                df = pd.DataFrame(
                                    result["data"],
                                    columns=[
                                        "timestamp",
                                        "open",
                                        "high",
                                        "low",
                                        "close",
                                        "volume",
                                        "volCcy",
                                        "volCcyQuote",
                                        "confirm",
                                    ],
                                )
                                df["timestamp"] = pd.to_datetime(
                                    df["timestamp"].astype(np.int64), unit="ms"
                                )
                                numeric_fields = [
                                    "open",
                                    "high",
                                    "low",
                                    "close",
                                    "volume",
                                    "volCcy",
                                    "volCcyQuote",
                                    "confirm",
                                ]
                                for field in numeric_fields:
                                    df[field] = pd.to_numeric(
                                        df[field], errors="coerce"
                                    )
                                df["instId"] = inst_id
                                df["bar"] = bar
                                return df

                        elif response.status == 429:
                            logger.debug(f"Too many requests for {bar} - {inst_id}.")
                            await asyncio.sleep(1)
                            break

    async def fetch_kline_data(
        self, inst_id: str, bar: str, sleep_time: int = 1, limit: int = 100
    ):
        collection_latest = await self.check_existing_data(inst_id, bar)
        logger.info(
            f"Found existing data for {inst_id} {bar} up to {collection_latest}."
        )
        latest_ts = await self.now_ts()
        a = latest_ts
        b: np.int64 = a

        is_first_time = True
        # Fetch until no more data is returned
        while b > collection_latest:
            try:
                params = {
                    "instId": inst_id,
                    "before": "" if is_first_time else str(b),
                    "after": str(a),
                    "bar": bar,
                    "limit": str(limit),
                }

                async with self.semaphore:
                    if self.session is not None:
                        async with self.session.get(
                            self.market_url, params=params, headers=self.headers
                        ) as response:
                            if response.status == 200:
                                result = await response.json()
                                if not result["data"]:
                                    logger.info(
                                        f"No more data to fetch or empty data returned for {inst_id}-{bar}."
                                    )
                                    return None
                                else:
                                    df = pd.DataFrame(
                                        result["data"],
                                        columns=[
                                            "timestamp",
                                            "open",
                                            "high",
                                            "low",
                                            "close",
                                            "volume",
                                            "volCcy",
                                            "volCcyQuote",
                                            "confirm",
                                        ],
                                    )
                                    df["timestamp"] = pd.to_datetime(
                                        df["timestamp"].values.astype(np.int64),
                                        unit="ms",
                                        utc=True,
                                    ).tz_convert("Asia/Shanghai")
                                    numeric_fields = [
                                        "open",
                                        "high",
                                        "low",
                                        "close",
                                        "volume",
                                        "volCcy",
                                        "volCcyQuote",
                                        "confirm",
                                    ]
                                    for field in numeric_fields:
                                        df[field] = pd.to_numeric(
                                            df[field], errors="coerce"
                                        )
                                    df["instId"] = inst_id
                                    df["bar"] = bar
                                    await self.insert_data_to_mongodb(
                                        f"kline-{bar}", df
                                    )  # Adjust as per your actual method signature
                                    logger.debug(
                                        f"Successfully inserted data for {inst_id} {bar} from {df['timestamp'].iloc[0]} to {df['timestamp'].iloc[-1]}."
                                    )
                                    a = np.int64(result["data"][-1][0]) - np.int64(1)

                                    if is_first_time:
                                        time_interval = abs(
                                            np.int64(result["data"][0][0]) - a
                                        )
                                        is_first_time = False
                                    b = (
                                        a
                                        - time_interval
                                        - np.int64(4)
                                        + np.int64(random.randint(1, 10) * 2)
                                    )

                            elif response.status == 429:
                                logger.debug(
                                    f"Too many requests for {bar} - {inst_id}."
                                )
                                
                            else:
                                logger.error(
                                    f"Failed to fetch data with status code {response.status}"
                                )
                                return None

            except Exception as e:
                logger.error(f"Error occurred: {e}, Retrying...")
                await asyncio.sleep(sleep_time)

    async def initialize_update(self):
        # List of restaurants could be big, think in promise of plying across the sums as detailed.
        coin_pairs = await self.get_all_coin_pairs(filter="USDT")
        logger.info(
            f"Fetching data for {len(coin_pairs)} coin pairs.\n Pairs: {coin_pairs}"
        )
        bar_sizes = self.bar_sizes
        tasks = []
        for inst_id in coin_pairs:
            for bar in bar_sizes:
                tasks.append(asyncio.create_task(self.fetch_kline_data(inst_id, bar)))
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    updater = AsyncOkxCandleUpdater()
    # Check conpound index
    asyncio.run(updater.check_index(updater.db))
