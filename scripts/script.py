from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.proxy import Proxy, ProxyType
from browsermobproxy import Server
import time
import logging
import json
import os

# Set up logging
logging.basicConfig(level=logging.INFO)

def setup_browsermob_proxy():
    browsermob_proxy_path = os.getenv("BROWSERPROXY_PATH", "/home/runner/browsermob-proxy/bin/browsermob-proxy")
    if not os.path.exists(browsermob_proxy_path):
        logging.error(f"BrowserMob Proxy not found at {browsermob_proxy_path}. Exiting...")
        exit(1)

    server = Server(browsermob_proxy_path)
    try:
        server.start()
        proxy = server.create_proxy()
        logging.info("Proxy setup complete.")
        return server, proxy
    except Exception as e:
        logging.error(f"Failed to start BrowserMob Proxy: {e}")
        exit(1)

def setup_selenium_driver(proxy):
    options = Options()
    options.headless = True
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")

    # Set proxy settings for Chrome
    proxy_split = proxy.proxy.split(":")
    options.add_argument(f'--proxy-server={proxy_split[0]}:{proxy_split[1]}')

    chromedriver_path = '/usr/local/bin/chromedriver'
    if not os.path.exists(chromedriver_path):
        logging.error(f"ChromeDriver not found at {chromedriver_path}. Exiting...")
        exit(1)

    service = Service(chromedriver_path)
    driver = webdriver.Chrome(service=service, options=options)
    logging.info("WebDriver setup complete.")
    return driver

def fetch_m3u8_url(proxy, link, timeout=30):
    proxy.new_har("network_capture")
    driver.get(link)
    logging.info(f"Waiting for network response for link: {link}...")

    start_time = time.time()
    while time.time() - start_time < timeout:
        for entry in proxy.har['log']['entries']:
            request_url = entry['request']['url']
            if "mono.m3u8" in request_url:
                headers = entry['request']['headers']
                headers_dict = {header['name'].lower(): header['value'] for header in headers}
                return request_url, headers_dict.get('referer'), headers_dict.get('origin')
        time.sleep(1)

    logging.warning(f"No m3u8 URL found for link: {link} within {timeout} seconds.")
    return None, None, None

# Main Script
data = {}
with open('scripts/soccer_data.json') as file:
    data = file.read()

matches = json.loads(data)["matches"]
updated_matches = []

server, proxy = setup_browsermob_proxy()
driver = setup_selenium_driver(proxy)

try:
    for match in matches:
        competition = match['competition']
        match_name = match['match']
        updated_match = {
            "competition": competition,
            "match": match_name,
            "links": match['links'],
            "m3u8_urls": [],
            "referrer": None,
            "origin": None
        }

        for link in match['links']:
            logging.info(f"Fetching match: {match_name} - Link: {link}")
            m3u8_url, referrer, origin = fetch_m3u8_url(proxy, link)

            if m3u8_url:
                updated_match["m3u8_urls"].append(m3u8_url)
                updated_match["referrer"] = referrer
                updated_match["origin"] = origin

        updated_matches.append(updated_match)

finally:
    if driver:
        driver.quit()
    if server:
        server.stop()
    logging.info("All done. WebDriver and server stopped.")

updated_data = {"matches": updated_matches}
with open("scripts/soccer_links.json", "w") as f:
    json.dump(updated_data, f, indent=4)

logging.info("Updated JSON data has been written to 'soccer_links.json'.")
