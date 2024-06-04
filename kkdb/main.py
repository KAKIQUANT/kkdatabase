# Main Entry Point for the KKDB Application
import hydra
from omegaconf import DictConfig
from kkdb.updater.backups.async_crypto import AsyncCryptoDataUpdater
import asyncio
from loguru import logger
from hydra.core.global_hydra import GlobalHydra
import os
from aiohttp.resolver import AsyncResolver
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


@hydra.main(config_path="configs", config_name="config", version_base="1.3")
def main(cfg: DictConfig):
    logger.debug(cfg)
    nameservers = cfg.dns.nameservers
    resolver = AsyncResolver(nameservers=nameservers)

    if cfg.proxy.enable:
        logger.debug("Proxy is enabled")
        logger.debug(f"HTTP Proxy: {cfg.proxy.http}")
        logger.debug(f"HTTPS Proxy: {cfg.proxy.https}")
        logger.debug(f"SOCKS5 Proxy: {cfg.proxy.socks5}")
        # Setup Proxy if necessary
        os.environ["http_proxy"] = cfg.proxy.http
        os.environ["https_proxy"] = cfg.proxy.https
        os.environ["socks_proxy"] = cfg.proxy.socks5

    logger.debug("Current IP address: ")
    logger.debug(requests.get("http://httpbin.org/ip").json())
    
    # Initialize Crypto Data Updater
    if cfg.datasource.crypto:
        updater = AsyncCryptoDataUpdater(
            client_str=cfg.db.connection_string,
            db_name=cfg.db.db_name.crypto,
            resolvers=resolver,
        )
        asyncio.run(updater.main())
    if cfg.datasource.cn_stock:
        pass

if __name__ == "__main__":
    main()
