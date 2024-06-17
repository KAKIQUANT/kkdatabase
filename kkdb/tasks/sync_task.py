import requests
from loguru import logger

def check_ip_address():
    """Function to check and log the current IP address using httpbin."""
    logger.info("Checking IP address...")
    # 国外ip
    response_f = requests.get("http://ipinfo.io")
    # 中国ip
    response_d = requests.get("http://myip.ipip.net")
    logger.info(f"Intenational IP Address: {response_f.json()}")
    logger.info(f"Domestic IP Address: {response_d.text}")