import json
import time
import logging
import os
import sys
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
import requests
from urllib.parse import urlparse
import backoff

# Enhanced logging configuration
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more verbose output
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('soccer_scraper.log'),
        logging.StreamHandler(sys.stdout)  # Explicitly log to stdout for GitHub Actions
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
        try:
            self.setup_proxy()
            self.setup_driver()
            return self
        except Exception as e:
            logging.error(f"Failed to initialize StreamScraper: {str(e)}")
            self.cleanup()
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def setup_proxy(self):
        logging.debug(f"Setting up proxy at path: {self.proxy_path}")
        
        # Verify proxy path exists
        if not os.path.exists(self.proxy_path):
            abs_path = os.path.abspath(self.proxy_path)
            logging.error(f"Proxy path not found. Absolute path: {abs_path}")
            logging.debug(f"Directory contents: {os.listdir(os.path.dirname(abs_path))}")
            raise FileNotFoundError(f"BrowserMob Proxy not found at {self.proxy_path}")
        
        try:
            self.server = Server(self.proxy_path)
            self.server.start()
            self.proxy = self.server.create_proxy()
            logging.info(f"Proxy started successfully on {self.proxy.proxy}")
        except Exception as e:
            logging.error(f"Failed to start proxy server: {str(e)}")
            raise

    def setup_driver(self):
        logging.debug(f"Setting up Chrome driver at path: {self.driver_path}")
        
        if not os.path.exists(self.driver_path):
            abs_path = os.path.abspath(self.driver_path)
            logging.error(f"ChromeDriver not found. Absolute path: {abs_path}")
            logging.debug(f"Directory contents: {os.listdir(os.path.dirname(abs_path))}")
            raise FileNotFoundError(f"ChromeDriver not found at {self.driver_path}")

        options = Options()
        options.add_argument('--no-sandbox')
        options.add_argument('--headless=new')  # Updated headless argument
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--remote-debugging-port=9222')
        options.add_argument('--autoplay-policy=no-user-gesture-required')
        options.add_argument('--disable-features=IsolateOrigins,site-per-process')
        options.add_argument('--disable-web-security')  # Added to handle CORS
        options.add_argument('--allow-running-insecure-content')  # Added for mixed content
        options.add_argument(f'--proxy-server={self.proxy.proxy}')  # Direct proxy configuration
        
        # Add required capabilities for media
        options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        
        try:
            service = Service(self.driver_path)
            self.driver = webdriver.Chrome(service=service, options=options)
            logging.info("Chrome WebDriver setup complete")
        except Exception as e:
            logging.error(f"Failed to setup Chrome WebDriver: {str(e)}")
            raise

    def extract_stream_data(self, link: str, match_name: str) -> List[Dict]:
        logging.info(f"Processing link for match {match_name}: {link}")
        
        try:
            # Enable header capture with extended options
            self.proxy.new_har("network_capture", options={
                'captureHeaders': True,
                'captureContent': True,
                'captureBinaryContent': True
            })
            
            # Set up request interception
            self.driver.execute_script("""
                window.originalFetch = window.fetch;
                window.fetch = async (...args) => {
                    console.log('Fetch request:', args);
                    const response = await window.originalFetch(...args);
                    console.log('Fetch response:', response);
                    return response;
                };
            """)
            
            self.driver.get(link)
            logging.debug(f"Page loaded: {link}")
            
            # Wait for network activity to settle
            time.sleep(5)
            
            streams = []
            har_entries = self.proxy.har['log']['entries']
            logging.debug(f"Total HAR entries: {len(har_entries)}")
            
            for entry in har_entries:
                request_url = entry['request']['url']
                logging.debug(f"Processing request URL: {request_url}")
                
                if "m3u8" in request_url.lower():
                    logging.info(f"Found potential stream URL: {request_url}")
                    
                    # Extract headers
                    request_headers = entry['request']['headers']
                    response_headers = entry['response']['headers']
                    
                    stream_data = {
                        'url': request_url,
                        'referrer': link,
                        'origin': f"{urlparse(link).scheme}://{urlparse(link).netloc}",
                        'source_link': link,
                        'response_status': entry['response']['status']
                    }
                    
                    streams.append(stream_data)
                    logging.info(f"Added stream: {stream_data}")
            
            return streams
            
        except Exception as e:
            logging.error(f"Error extracting stream data: {str(e)}", exc_info=True)
            return []

def main():
    # Get configuration from environment variables with fallbacks
    config = {
        'proxy_path': os.getenv('BROWSERPROXY_PATH', '/usr/local/bin/browsermob-proxy/bin/browsermob-proxy'),
        'driver_path': os.getenv('CHROMEDRIVER_PATH', '/usr/local/bin/chromedriver'),
        'input_file': os.getenv('INPUT_FILE', 'scripts/soccer_data.json'),
        'output_file': os.getenv('OUTPUT_FILE', 'scripts/soccer_links.json')
    }

    logging.info("Starting scraper with config:")
    for key, value in config.items():
        logging.info(f"{key}: {value}")

    try:
        # Verify input file exists
        if not os.path.exists(config['input_file']):
            raise FileNotFoundError(f"Input file not found: {config['input_file']}")

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

        # Save results
        output_data = {
            "matches": updated_matches,
            "metadata": {
                "total_matches": len(updated_matches),
                "matches_with_streams": sum(1 for m in updated_matches if m['streams']),
                "timestamp": datetime.now().isoformat()
            }
        }

        # Ensure output directory exists
        os.makedirs(os.path.dirname(config['output_file']), exist_ok=True)
        
        with open(config['output_file'], "w") as f:
            json.dump(output_data, f, indent=4)

        logging.info(f"Processing completed. Results saved to {config['output_file']}")

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
