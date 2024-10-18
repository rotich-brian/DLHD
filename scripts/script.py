import json
import time
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
from browsermobproxy import Server
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from urllib.parse import urlparse
import backoff

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('soccer_scraper.log'),
        logging.StreamHandler()
    ]
)

@dataclass
class StreamData:
    competition: str
    match: str
    links: List[str]
    m3u8_urls: List[str]
    referrer: Optional[str]
    origin: Optional[str]
    last_updated: str

class StreamScraper:
    def __init__(self, proxy_path: str, driver_path: str):
        self.proxy_path = proxy_path
        self.driver_path = driver_path
        self.server = None
        self.driver = None
        self.proxy = None
        
    def __enter__(self):
        self.setup_proxy()
        self.setup_driver()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def setup_proxy(self):
        """Set up BrowserMob proxy with retry mechanism"""
        if not os.path.exists(self.proxy_path):
            raise FileNotFoundError(f"BrowserMob Proxy not found at {self.proxy_path}")
        
        logging.info("Setting up proxy...")
        self.server = Server(self.proxy_path)
        self.server.start()
        self.proxy = self.server.create_proxy()
        logging.info(f"Proxy started on {self.proxy.proxy}")

    def setup_driver(self):
        """Set up Chrome WebDriver with necessary options"""
        if not os.path.exists(self.driver_path):
            raise FileNotFoundError(f"ChromeDriver not found at {self.driver_path}")

        options = Options()
        options.add_argument('--no-sandbox')
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--remote-debugging-port=9222')
        options.add_argument('--autoplay-policy=no-user-gesture-required')
        options.add_argument('--disable-features=IsolateOrigins,site-per-process')

        proxy_settings = Proxy({
            'proxyType': ProxyType.MANUAL,
            'httpProxy': self.proxy.proxy,
            'sslProxy': self.proxy.proxy
        })
        options.proxy = proxy_settings

        service = Service(self.driver_path)
        self.driver = webdriver.Chrome(service=service, options=options)
        logging.info("WebDriver setup complete")

    def validate_m3u8_url(self, url: str) -> bool:
        """Validate if the M3U8 URL is accessible"""
        try:
            response = requests.head(url, timeout=5)
            return response.status_code == 200
        except:
            return False

    @backoff.on_exception(backoff.expo, Exception, max_tries=3, max_time=30)
    def extract_stream_data(self, link: str, match_name: str) -> Optional[Dict]:
        """Extract M3U8 URL and headers from a single link with retry mechanism"""
        logging.info(f"Processing link for match {match_name}: {link}")
        
        self.proxy.new_har("network_capture")
        self.driver.get(link)

        start_time = time.time()
        timeout = 30
        m3u8_data = None

        while time.time() - start_time < timeout:
            for entry in self.proxy.har['log']['entries']:
                request_url = entry['request']['url']
                
                if "mono.m3u8" in request_url or "playlist.m3u8" in request_url:
                    headers = {header['name'].lower(): header['value'] 
                             for header in entry['request']['headers']}
                    
                    if self.validate_m3u8_url(request_url):
                        m3u8_data = {
                            'url': request_url,
                            'referrer': headers.get('referer'),
                            'origin': headers.get('origin')
                        }
                        logging.info(f"Valid M3U8 URL found: {request_url}")
                        return m3u8_data
            
            time.sleep(1)

        logging.warning(f"No valid M3U8 URL found for {match_name} - {link}")
        return None

    def process_match(self, match: Dict) -> StreamData:
        """Process a single match and extract stream data"""
        m3u8_urls = []
        referrer = None
        origin = None

        for link in match['links']:
            try:
                stream_data = self.extract_stream_data(link, match['match'])
                if stream_data:
                    m3u8_urls.append(stream_data['url'])
                    referrer = stream_data['referrer']
                    origin = stream_data['origin']
            except Exception as e:
                logging.error(f"Error processing link {link}: {str(e)}")

        return StreamData(
            competition=match['competition'],
            match=match['match'],
            links=match['links'],
            m3u8_urls=m3u8_urls,
            referrer=referrer,
            origin=origin,
            last_updated=datetime.now().isoformat()
        )

    def cleanup(self):
        """Clean up resources"""
        if self.driver:
            self.driver.quit()
        if self.server:
            self.server.stop()
        logging.info("Cleanup completed")

def main():
    # Load configuration
    config = {
        'proxy_path': os.getenv("BROWSERPROXY_PATH", "/usr/local/bin/browsermob-proxy/bin/browsermob-proxy"),
        'driver_path': '/usr/local/bin/chromedriver',
        'input_file': 'scripts/soccer_data.json',
        'output_file': 'scripts/soccer_links.json'
    }

    try:
        # Load input data
        with open(config['input_file']) as file:
            matches = json.loads(file.read())["matches"]

        updated_matches = []
        
        with StreamScraper(config['proxy_path'], config['driver_path']) as scraper:
            # Process matches sequentially to avoid overwhelming resources
            for match in matches:
                try:
                    stream_data = scraper.process_match(match)
                    updated_matches.append(asdict(stream_data))
                except Exception as e:
                    logging.error(f"Error processing match {match['match']}: {str(e)}")

        # Save results
        output_data = {
            "matches": updated_matches,
            "metadata": {
                "total_matches": len(updated_matches),
                "timestamp": datetime.now().isoformat(),
                "success_rate": sum(1 for m in updated_matches if m['m3u8_urls']) / len(updated_matches)
            }
        }

        with open(config['output_file'], "w") as f:
            json.dump(output_data, f, indent=4)

        logging.info(f"Processing completed. Results saved to {config['output_file']}")
        logging.info(f"Success rate: {output_data['metadata']['success_rate']:.2%}")

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
