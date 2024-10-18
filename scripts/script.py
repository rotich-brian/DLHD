import json
import time
import logging
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from browsermobproxy import Server

# Set up logging
logging.basicConfig(level=logging.INFO)

# Your JSON data
data = {}
with open('scripts/soccer_data.json') as file:
    data = file.read()

# Parse JSON
matches = json.loads(data)["matches"]

updated_matches = []
driver = None
server = None
try:
    # Setup BrowserMob Proxy and WebDriver
    logging.info("Setting up proxy...")
    browsermob_proxy_path = os.getenv("BROWSERPROXY_PATH", "/usr/local/bin/browsermob-proxy/bin/browsermob-proxy")
    
    if not os.path.exists(browsermob_proxy_path):
        logging.error(f"BrowserMob Proxy not found at {browsermob_proxy_path}. Exiting...")
        exit(1)

    server = Server(browsermob_proxy_path)
    server.start()
    proxy = server.create_proxy()
    logging.info("Proxy setup complete.")

    proxy_settings = Proxy({
        'proxyType': ProxyType.MANUAL,
        'httpProxy': proxy.proxy,
        'sslProxy': proxy.proxy
    })

    options = Options()
    options.headless = True
    options.add_argument('--no-sandbox')
    options.add_argument('--headless')
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--autoplay-policy=no-user-gesture-required")
    options.add_argument("--disable-features=IsolateOrigins,site-per-process")
    options.proxy = proxy_settings

    geckodriver_path = '/usr/local/bin/chromedriver'
    if not os.path.exists(geckodriver_path):
        logging.error(f"ChromeDriver not found at {geckodriver_path}. Exiting...")
        exit(1)

    service = Service(geckodriver_path)
    driver = webdriver.Chrome(service=service, options=options)
    logging.info("WebDriver setup complete.")

    # Loop through matches
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
            logging.info(f"\nFetching match: {match_name} - Link: {link}")

            # Start new HAR for each link
            proxy.new_har("network_capture")

            # Open the link
            driver.get(link)
            logging.info("Waiting for network response...")

            m3u8_url = None
            referrer_header = None
            origin_header = None

            timeout = 30  # Maximum wait time in seconds
            start_time = time.time()

            while time.time() - start_time < timeout:
                for entry in proxy.har['log']['entries']:
                    request_url = entry['request']['url']
                    
                    # Log details of each network request
                    logging.info(f"Request URL: {request_url}")
                    logging.info(f"Request Method: {entry['request']['method']}")
                    logging.info(f"Request Headers: {json.dumps(entry['request']['headers'], indent=2)}")
                    
                    # Check if this request contains the m3u8 URL
                    if "mono.m3u8" in request_url:
                        m3u8_url = request_url
                        headers = entry['request']['headers']

                        headers_dict = {header['name'].lower(): header['value'] for header in headers}
                        referrer_header = headers_dict.get('referer')
                        origin_header = headers_dict.get('origin')

                        logging.info(f"Found m3u8 URL: {m3u8_url}")
                        logging.info(f"Referrer: {referrer_header}")
                        logging.info(f"Origin: {origin_header}")
                        break
                
                if m3u8_url:
                    updated_match["m3u8_urls"].append(m3u8_url)
                    updated_match["referrer"] = referrer_header
                    updated_match["origin"] = origin_header
                    break

                time.sleep(1)

            if not m3u8_url:
                logging.warning(f"No m3u8 URL found for match: {match_name} - Link: {link}")

        updated_matches.append(updated_match)

finally:
    # Clean up
    if driver:
        driver.quit()
    if server:
        server.stop()
    logging.info("All done. WebDriver and server stopped.")

# Prepare the updated JSON data
updated_data = {"matches": updated_matches}

# Output the updated JSON
with open("scripts/soccer_links.json", "w") as f:
    json.dump(updated_data, f, indent=4)

logging.info("Updated JSON data has been written to 'soccer_links.json'.")
