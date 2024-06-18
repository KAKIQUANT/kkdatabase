import asyncio
import pandas as pd
from kkdb.updater.AsyncBaseDataUpdater import AsyncBaseDataUpdater
import akshare as ak
from aiohttp import ClientSession
from retry import retry
from typing import Iterable
import os

# 清除环境变量中的代理设置
os.environ.pop('http_proxy', None)
os.environ.pop('https_proxy', None)

# 接下来是你的代码


class AsyncAkshareDataUpdater(AsyncBaseDataUpdater):
    def __init__(
            self, db_name: str, bar_sizes: Iterable[str], client_str: str,
            max_concurrent_requests: int = 3, session: ClientSession = None
    ) -> None:
        super().__init__(db_name, bar_sizes, max_concurrent_requests, client_str)
        self.market_type = "cn_stock"
        self.market_name = "A-Share"
        self.session = session or ClientSession()

    async def _process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df.rename(
            columns={
                "日期": "timestamp",
                "股票代码": "orderbook_id",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "total_turnover",
                "振幅": "amplitude",
                "涨跌幅": "change_rate",
                "涨跌额": "change_amount",
                "换手率": "turnover_rate",
            },
            inplace=True
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.loc[:, ~df.columns.duplicated()]
        return df

    async def _get_stock_list(self) -> list:
        # Run akshare synchronously in an executor
        stock_list_df = await asyncio.get_event_loop().run_in_executor(None, ak.stock_zh_a_spot_em)
        return stock_list_df["代码"].tolist()

    async def initialize_update(self):
        stock_list = await  self._get_stock_list()
        self.stock_list = stock_list

    async def _get_existing_data_range(self, stock_code: str, collection_name: str):
        collection = self.db[collection_name]
        earliest_record = await collection.find_one({'orderbook_id': stock_code}, sort=[("datetime", 1)])
        latest_record = await collection.find_one({'orderbook_id': stock_code}, sort=[("datetime", -1)])
        earliest = earliest_record['datetime'] if earliest_record else None
        latest = latest_record['datetime'] if latest_record else None
        return earliest, latest

    @retry(tries=3, delay=5)
    async def _single_download(self, stock_code: str, freq: str) -> None:
        async with self.semaphore:
            await self.fetch_and_process_data(stock_code, freq)

    async def fetch_and_process_data(self, stock_code, freq):
        collection_name = f"kline-{freq}"
        earliest, latest = await self._get_existing_data_range(stock_code, collection_name)

        # Define the function to get data with time filtering
        def fetch_data():
            if earliest and latest:
                return ak.stock_zh_a_hist(symbol=stock_code, adjust="hfq", start_date=latest, end_date=None)
            return ak.stock_zh_a_hist(symbol=stock_code, adjust="hfq")

        # Run the data fetching in an executor
        stock_data_df = await asyncio.get_event_loop().run_in_executor(None, fetch_data)

        if not stock_data_df.empty:
            processed_df = await self._process_df(stock_data_df)
            await self.insert_data(collection_name, processed_df)
    async def pool_download(self):
        tasks = []
        bar_sizes = ["1D", "1W"]  # Define your required bar sizes
        for stock_code in self.stock_list:
            for bar in bar_sizes:
                task = asyncio.create_task(self._single_download(stock_code, bar))
                tasks.append(task)
        await asyncio.gather(*tasks)

    async def main(self, refresh: bool = False):
        await self.start_session()
        await self.drop_db(refresh=refresh)
        await self.check_index()
        await self.initialize_update()
        await self.pool_download()
        await self.close_session()
        await self.check_index()


if __name__ == "__main__":
    client_str = "mongodb://10.201.8.215:27017"  # Make sure to define this
    updater = AsyncAkshareDataUpdater(db_name="cn_stock", bar_sizes=["1D", "1W"], client_str=client_str)
    asyncio.run(updater.main(refresh=False))
