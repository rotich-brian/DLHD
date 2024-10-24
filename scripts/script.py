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

# Configure logging
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
    streams: List[Dict[str, str]]  
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
        if not os.path.exists(self.proxy_path):
            raise FileNotFoundError(f"BrowserMob Proxy not found at {self.proxy_path}")
        
        logging.info("Setting up proxy...")
        self.server = Server(self.proxy_path)
        self.server.start()
        self.proxy = self.server.create_proxy()
        logging.info(f"Proxy started on {self.proxy.proxy}")

    def setup_driver(self):
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

    def _get_header_value(self, headers: List[Dict[str, str]], target_header: str) -> Optional[str]:
        """Extract header value, checking both original and lowercase names."""
        for header in headers:
            name = header['name']
            if name.lower() == target_header.lower():
                return header['value']
        return None

    def validate_m3u8_url(self, url: str, headers: Optional[Dict[str, str]] = None) -> bool:
        """Validate if the M3U8 URL is accessible"""
        try:
            response = requests.head(url, headers=headers, timeout=5)
            return response.status_code == 200
        except:
            return False

    @backoff.on_exception(backoff.expo, Exception, max_tries=3, max_time=30)
    def extract_stream_data(self, link: str, match_name: str) -> List[Dict]:
        """Extract M3U8 URLs and headers from a single link with retry mechanism"""
        logging.info(f"Processing link for match {match_name}: {link}")
        
        # Enable header capture
        self.proxy.new_har("network_capture", options={'captureHeaders': True})
        
        # Inject headers via JavaScript
        self.driver.execute_script(f"""
            let originalOpen = XMLHttpRequest.prototype.open;
            XMLHttpRequest.prototype.open = function() {{
                originalOpen.apply(this, arguments);
                this.setRequestHeader('Referer', '{link}');
                this.setRequestHeader('Origin', new URL('{link}').origin);
            }};
        """)
        
        self.driver.get(link)
        
        start_time = time.time()
        timeout = 30
        streams = []

        while time.time() - start_time < timeout:
            for entry in self.proxy.har['log']['entries']:
                request_url = entry['request']['url']
                
                if "mono.m3u8" in request_url or "playlist.m3u8" in request_url:
                    # Get headers from request
                    request_headers = entry['request']['headers']
                    response_headers = entry['response']['headers']
                    
                    # Try to get referrer and origin from various sources
                    referer = (
                        self._get_header_value(request_headers, 'Referer') or
                        self._get_header_value(response_headers, 'Referer') or
                        link
                    )
                    
                    origin = (
                        self._get_header_value(request_headers, 'Origin') or
                        self._get_header_value(response_headers, 'Origin') or
                        f"{urlparse(link).scheme}://{urlparse(link).netloc}"
                    )
                    
                    # Construct headers for validation
                    stream_headers = {
                        'Referer': referer,
                        'Origin': origin
                    }
                    
                    if self.validate_m3u8_url(request_url, stream_headers):
                        stream_data = {
                            'url': request_url,
                            'referrer': referer,
                            'origin': origin,
                            'source_link': link
                        }
                        streams.append(stream_data)
                        logging.info(f"Valid stream found: {stream_data}")
            
            if streams:
                break
            time.sleep(1)

        if not streams:
            logging.warning(f"No valid streams found for {match_name} - {link}")
        
        return streams

    def process_match(self, match: Dict) -> StreamData:
        """Process a single match and extract stream data"""
        all_streams = []

        for link in match['links']:
            try:
                streams = self.extract_stream_data(link, match['match'])
                all_streams.extend(streams)
            except Exception as e:
                logging.error(f"Error processing link {link}: {str(e)}")

        return StreamData(
            competition=match['competition'],
            match=match['match'],
            links=match['links'],
            streams=all_streams,
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
    config = {
        'proxy_path': os.getenv("BROWSERPROXY_PATH", "/usr/local/bin/browsermob-proxy/bin/browsermob-proxy"),  # Updated path to match reference
        'driver_path': '/usr/local/bin/chromedriver',  # This path is already correct
        'input_file': 'scripts/soccer_data.json',  # Updated path to match reference
        'output_file': 'scripts/soccer_links.json'  # Updated path to match reference
    }

    try:
        with open(config['input_file']) as file:
            matches = json.loads(file.read())["matches"]

        updated_matches = []
        
        with StreamScraper(config['proxy_path'], config['driver_path']) as scraper:
            for match in matches:
                try:
                    stream_data = scraper.process_match(match)
                    updated_matches.append(asdict(stream_data))
                except Exception as e:
                    logging.error(f"Error processing match {match['match']}: {str(e)}")

        # Calculate success metrics
        matches_with_streams = sum(1 for m in updated_matches if m['streams'])
        total_matches = len(updated_matches)
        success_rate = matches_with_streams / total_matches if total_matches > 0 else 0

        output_data = {
            "matches": updated_matches,
            "metadata": {
                "total_matches": total_matches,
                "matches_with_streams": matches_with_streams,
                "success_rate": success_rate,
                "timestamp": datetime.now().isoformat()
            }
        }

        with open(config['output_file'], "w") as f:
            json.dump(output_data, f, indent=4)

        logging.info(f"Processing completed. Results saved to {config['output_file']}")
        logging.info(f"Success rate: {success_rate:.2%}")

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
