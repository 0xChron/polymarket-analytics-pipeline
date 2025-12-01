import logging
from config import DB_CONFIG, EVENT_API_URL, PAGE_LIMIT, DELAY, MAX_RETRIES, DELAY_BETWEEN_RETRIES
from database import Database
from api_client import PolymarketAPIClient
from transformers import DataTransformer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def main():
    db = None
    
    try:
        logger.info("initializing polymarket data pipeline...")
        db = Database(DB_CONFIG)
        db.connect()
        db.create_tables()
        
        api_client = PolymarketAPIClient(
            event_url=EVENT_API_URL,
            page_limit=PAGE_LIMIT,
            delay=DELAY,
            max_retries=MAX_RETRIES,
            retry_delay=DELAY_BETWEEN_RETRIES
        )
        
        transformer = DataTransformer()
        
        logger.info("=== FETCHING EVENTS (with nested markets) ===")
        raw_events = api_client.fetch_events()
        logger.info(f"fetched {len(raw_events)} events from api")
        
        logger.info("=== TRANSFORMING DATA ===")
        events_data, markets_data = transformer.transform_events_and_markets(raw_events)
        logger.info(f"transformed {len(events_data)} events and {len(markets_data)} markets for insertion")
        
        logger.info("=== INSERTING EVENTS ===")
        db.insert_events(events_data)
        
        logger.info("=== INSERTING MARKETS ===")
        db.insert_markets(markets_data)
        
        logger.info("=== PIPELINE COMPLETED SUCCESSFULLY ===")
        
    except Exception as e:
        logger.error(f"pipeline failed: {e}", exc_info=True)
        raise
    finally:
        if db:
            db.close()


if __name__ == "__main__":
    main()