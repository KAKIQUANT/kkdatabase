
from loguru import logger
import asyncio
from kkdb.updater.AsyncOkxDataUpdater import AsyncOkxCandleUpdater
from kkdb.updater.AsyncAkshareDataUpdater import AsyncAkshareDataUpdater
async def update_crypto_data(cfg):
    """Asynchronous function to update cryptocurrency data."""
    logger.info("Updating cryptocurrency data...")
    updater = AsyncOkxCandleUpdater(
        client_str=cfg.db.str,
        db_name=cfg.db.db_name.crypto,
        proxy=cfg.proxy.http,
    )
    await updater.main()

async def update_cn_stock_data(cfg):
    """Asynchronous function to update China stock data."""
    logger.info("Updating China stock data...")
    updater = AsyncAkshareDataUpdater(
        db_name=cfg.db.db_name.cn_stock,
        bar_sizes=["1D", "1W"],
        client_str=cfg.db.str,
    )
    await updater.main()

async def update_all_data_sources(cfg):
    """Asynchronous function to update all data sources."""
    logger.info("Updating all data sources...")
    # Gather tasks for all updates
    tasks = [
        asyncio.create_task(update_crypto_data(cfg)),
        asyncio.create_task(update_cn_stock_data(cfg))
    ]
    await asyncio.gather(*tasks)