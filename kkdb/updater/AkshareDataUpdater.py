from kkdb.updater.BaseDataUpdater import BaseDataUpdater
from kkdb.utils.check_db import get_client_str
import akshare as ak
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable
from retry import retry

class AkshareDataUpdater(BaseDataUpdater):
    def __init__(
        self, db_name: str, bar_sizes: Iterable[str], client_str: str = get_client_str()
    ) -> None:
        super().__init__(db_name, bar_sizes, client_str)
        self.market_type = "cn_stock"
        self.market_name = "A-Share"

    def _process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df.rename(
            columns={
                "日期": "datetime",
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
            inplace=True,
        )
        df["datetime"] = pd.to_datetime(df["datetime"])
        df["datetime"] = df["datetime"].apply(
            lambda x: datetime.combine(x, datetime.min.time())
        )
        
        # Drop duplicate columns
        df = df.loc[:, ~df.columns.duplicated()]
        
        return df

    def _get_stock_list(self) -> list:
        stock_list_df = ak.stock_zh_a_spot_em()
        return stock_list_df["代码"].tolist()

    @retry(tries=3, delay=5)
    def _single_download(self, stock_code: str, freq: str):
        if freq == "1D":
            stock_data_df = ak.stock_zh_a_hist(symbol=stock_code, adjust="hfq")
        elif freq == "1W":
            stock_data_df = ak.stock_zh_a_hist(
                symbol=stock_code, period="weekly", adjust="hfq"
            )
        elif freq == '1m':
            stock_data_df = ak.stock_zh_a_minute(
                symbol=stock_code, period=freq.lower(), adjust="hfq"
            )
        else:
            raise ValueError(f"Invalid frequency: {freq}")
        if not stock_data_df.empty:
            stock_data_df = self._process_df(stock_data_df)
            self._insert_data(f"kline-{freq}", stock_data_df)

    def pool_download(self, stock_list: list, num_workers: int = 5):
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = []
            bar_sizes = ["1D", "1W"]
            for stock_code in stock_list:
                for bar in bar_sizes:
                    futures.append(
                        executor.submit(self._single_download, stock_code, freq=bar)
                    )
            for future in futures:
                future.result()


if __name__ == "__main__":
    updater = AkshareDataUpdater(db_name="cn_stock", bar_sizes=["1D", "1W"])
    updater.main(refresh=False)
