import requests
import time
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)


class PolymarketAPIClient:
    def __init__(self, event_url: str, page_limit: int = 500, 
                 delay: float = 2.0, max_retries: int = 3, retry_delay: float = 5.0):
        self.event_url = event_url
        self.page_limit = page_limit
        self.delay = delay
        self.max_retries = max_retries
        self.retry_delay = retry_delay
    
    def fetch_events(self) -> List[Dict]:
        all_events = []
        offset = 0
        page_count = 0
        
        logger.info("fetching polymarket even and markets...")
        
        while True:
            if page_count > 0:
                time.sleep(self.delay)
            
            params = {
                'order': 'id',
                'ascending': 'false',
                'closed': 'false',
                'limit': self.page_limit,
                'offset': offset
            }
            
            for attempt in range(self.max_retries):
                try:
                    response = requests.get(self.event_url, params=params, timeout=30)
                    response.raise_for_status()
                    events_data = response.json()
                    break
                except requests.exceptions.Timeout:
                    if attempt < self.max_retries - 1:
                        logger.warning(f"timeout. retrying in {self.retry_delay}s...")
                        time.sleep(self.retry_delay)
                    else:
                        logger.error(f"failed after {self.max_retries} attempts at offset {offset}")
                        events_data = None
                        break
                except Exception as e:
                    logger.error(f"api request failed at offset {offset}: {e}")
                    events_data = None
                    break
            
            if events_data is None:
                break
            
            if not events_data:
                logger.info("no more events found.")
                break
            
            num_events = len(events_data)
            all_events.extend(events_data)
            page_count += 1
            offset += self.page_limit
            
            logger.info(f"page {page_count}: fetched {num_events} events. total: {len(all_events)}")
            
            if num_events < self.page_limit:
                logger.info(f"final page reached.")
                break
        
        return all_events