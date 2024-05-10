# Main Entry Point for the KKDB Application
import hydra
from omegaconf import DictConfig
from kkdb.updater.async_crypto import AsyncCryptoDataUpdater
import asyncio
from loguru import logger
from hydra.core.global_hydra import GlobalHydra
import os
import requests
# Clear any existing global Hydra instance
GlobalHydra.instance().clear()

# Setup logger
logger.add(
    sink="./logs/updater.log",
    rotation="1 day",
    retention="7 days",
    enqueue=True,
    backtrace=True,
    diagnose=True,
)

@hydra.main(config_path="configs", config_name="config", version_base="1.2")
def main(cfg: DictConfig):
    logger.debug(cfg)

    # Setup Proxy if necessary
    # os.environ["http_proxy"] = cfg.proxy.http
    # os.environ["https_proxy"] = cfg.proxy.https
    # os.environ["socks_proxy"] = cfg.proxy.socks5
    # Test IP Address
    print(requests.get("http://httpbin.org/ip").json())
    # Crypto Data Updater
    if cfg.datasource.crypto:
        updater = AsyncCryptoDataUpdater(client_str=cfg.db.connection_string, db_name=cfg.db.db_name.crypto)
        asyncio.run(updater.main())

if __name__ == "__main__":
    main()