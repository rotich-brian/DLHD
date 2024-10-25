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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from browsermobproxy import Server
import requests
from urllib.parse import urlparse
import backoff

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('soccer_scraper.log'),
        logging.StreamHandler(sys.stdout)
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

    def setup_proxy(self):
        logging.debug(f"Setting up proxy at path: {self.proxy_path}")
        try:
            self.server = Server(self.proxy_path)
            self.server.start()
            self.proxy = self.server.create_proxy({'trustAllServers': True})
            logging.info(f"Proxy started successfully on {self.proxy.proxy}")
        except Exception as e:
            logging.error(f"Failed to start proxy server: {str(e)}")
            raise

    def setup_driver(self):
        logging.debug(f"Setting up Chrome driver at path: {self.driver_path}")
        
        options = Options()
        options.add_argument('--no-sandbox')
        options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--remote-debugging-port=9222')
        options.add_argument('--autoplay-policy=no-user-gesture-required')
        options.add_argument('--disable-features=IsolateOrigins,site-per-process')
        options.add_argument('--disable-web-security')
        options.add_argument('--allow-running-insecure-content')
        options.add_argument(f'--proxy-server={self.proxy.proxy}')
        
        # Enhanced logging preferences
        options.set_capability('goog:loggingPrefs', {
            'performance': 'ALL',
            'browser': 'ALL',
            'network': 'ALL'
        })
        
        try:
            service = Service(self.driver_path)
            self.driver = webdriver.Chrome(service=service, options=options)
            
            # Add request interceptor
            self.driver.execute_script("""
                const originalOpen = XMLHttpRequest.prototype.open;
                XMLHttpRequest.prototype.open = function() {
                    this.addEventListener('load', function() {
                        console.log('XHR Response:', {
                            url: this.responseURL,
                            status: this.status,
                            headers: this.getAllResponseHeaders(),
                            response: this.responseText
                        });
                    });
                    originalOpen.apply(this, arguments);
                };
                
                // Fetch interceptor
                const originalFetch = window.fetch;
                window.fetch = async (...args) => {
                    const request = args[0];
                    const url = typeof request === 'string' ? request : request.url;
                    console.log('Fetch Request:', url);
                    
                    try {
                        const response = await originalFetch(...args);
                        const clone = response.clone();
                        console.log('Fetch Response:', {
                            url: url,
                            status: clone.status,
                            headers: [...clone.headers.entries()]
                        });
                        return response;
                    } catch (error) {
                        console.error('Fetch Error:', error);
                        throw error;
                    }
                };
            """)
            
            logging.info("Chrome WebDriver setup complete with enhanced monitoring")
        except Exception as e:
            logging.error(f"Failed to setup Chrome WebDriver: {str(e)}")
            raise

    def extract_stream_data(self, link: str, match_name: str) -> List[Dict]:
        logging.info(f"Processing link for match {match_name}: {link}")
        streams = []
        
        try:
            # Configure HAR capture with extended options
            self.proxy.new_har("stream_capture", options={
                'captureHeaders': True,
                'captureContent': True,
                'captureBinaryContent': True,
                'captureTypes': ['application/x-mpegURL', 'application/vnd.apple.mpegurl']
            })
            
            self.driver.get(link)
            logging.info(f"Page loaded: {link}")
            
            # Wait for potential iframes to load
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "iframe"))
                )
                # Switch to each iframe and wait for content
                iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
                for iframe in iframes:
                    self.driver.switch_to.frame(iframe)
                    time.sleep(2)  # Wait for iframe content
                    self.driver.switch_to.default_content()
            except TimeoutException:
                logging.debug("No iframes found or timeout waiting for iframes")
            
            # Extended wait for dynamic content
            time.sleep(10)
            
            # Collect browser console logs
            browser_logs = self.driver.get_log('browser')
            for log in browser_logs:
                if 'm3u8' in log['message'].lower():
                    logging.info(f"Found potential stream in console: {log['message']}")
            
            # Process HAR entries
            har_entries = self.proxy.har['log']['entries']
            logging.info(f"Processing {len(har_entries)} HAR entries")
            
            for entry in har_entries:
                request_url = entry['request']['url']
                request_headers = {h['name']: h['value'] for h in entry['request']['headers']}
                response_headers = {h['name']: h['value'] for h in entry['response']['headers']}
                
                # Enhanced URL pattern matching
                if any(pattern in request_url.lower() for pattern in [
                    'm3u8', 'playlist', 'manifest', 'stream', 'media'
                ]):
                    stream_data = {
                        'url': request_url,
                        'referrer': link,
                        'origin': f"{urlparse(link).scheme}://{urlparse(link).netloc}",
                        'content_type': response_headers.get('content-type', ''),
                        'request_headers': request_headers,
                        'response_headers': response_headers,
                        'status': entry['response']['status'],
                        'timestamp': entry['startedDateTime']
                    }
                    
                    streams.append(stream_data)
                    logging.info(f"Found stream: {request_url}")
            
            return streams
            
        except Exception as e:
            logging.error(f"Error extracting stream data: {str(e)}", exc_info=True)
            return []

    def process_match(self, match: Dict) -> StreamData:
        """Process a single match and extract stream data"""
        logging.info(f"Processing match: {match['match']}")
        all_streams = []

        for link in match['links']:
            try:
                streams = self.extract_stream_data(link, match['match'])
                all_streams.extend(streams)
                logging.info(f"Found {len(streams)} streams for link: {link}")
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
        logging.info("Starting cleanup...")
        if self.driver:
            self.driver.quit()
            logging.info("Chrome driver closed successfully")
        if self.server:
            self.server.stop()
            logging.info("Proxy server stopped successfully")

def main():
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
        with open(config['input_file']) as file:
            data = json.loads(file.read())
            matches = data.get("matches", [])

        updated_matches = []
        
        with StreamScraper(config['proxy_path'], config['driver_path']) as scraper:
            for match in matches:
                try:
                    stream_data = scraper.process_match(match)
                    updated_matches.append(asdict(stream_data))
                except Exception as e:
                    logging.error(f"Error processing match {match['match']}: {str(e)}")

        output_data = {
            "matches": updated_matches,
            "metadata": {
                "total_matches": len(updated_matches),
                "matches_with_streams": sum(1 for m in updated_matches if m['streams']),
                "timestamp": datetime.now().isoformat()
            }
        }

        os.makedirs(os.path.dirname(config['output_file']), exist_ok=True)
        
        with open(config['output_file'], "w") as f:
            json.dump(output_data, f, indent=4)

        logging.info(f"Processing completed. Results saved to {config['output_file']}")

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
