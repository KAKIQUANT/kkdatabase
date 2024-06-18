import hydra
from omegaconf import DictConfig
from loguru import logger
import asyncio

from kkdb.tasks.async_task import update_crypto_data, update_cn_stock_data, update_all_data_sources
from kkdb.tasks.sync_task import check_ip_address
def run_async_main(cfg):
    asyncio.run(async_main(cfg))

async def async_main(cfg):
    logger.info("Application Starting...")
    if cfg.get('check_ip'):
        check_ip_address()

    tasks = []
    if cfg.get('update_crypto'):
        tasks.append(asyncio.create_task(update_crypto_data(cfg)))
    if cfg.get('update_cn_stock'):
        tasks.append(asyncio.create_task(update_cn_stock_data(cfg)))
    if cfg.get('update_all'):
        tasks.append(asyncio.create_task(update_all_data_sources(cfg)))

    if tasks:
        await asyncio.gather(*tasks)

@hydra.main(config_path="configs", config_name="config", version_base="1.3")
def main(cfg: DictConfig):
    run_async_main(cfg)

if __name__ == "__main__":
    main()