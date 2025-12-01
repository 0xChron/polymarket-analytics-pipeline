import logging
import psycopg2
from psycopg2.extras import execute_values
from typing import List, Tuple

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.conn = None
    
    def connect(self):
        self.conn = psycopg2.connect(**self.db_config)
        return self.conn
    
    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("database connection closed.")
    
    def create_tables(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY,
                    slug TEXT,
                    title TEXT,
                    description TEXT,
                    end_date TIMESTAMP WITH TIME ZONE,
                    image TEXT,
                    new BOOLEAN,
                    liquidity NUMERIC,
                    volume NUMERIC,
                    volume24hr NUMERIC,
                    categories jsonb,
                    fetch_date TIMESTAMP WITH TIME ZONE
                )
            ''')
            logger.info("table 'events' created.")
            
            # events indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_fetch_date ON events(fetch_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_volume24hr ON events(volume24hr DESC) WHERE volume24hr > 0')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_categories ON events USING GIN(categories)')
            logger.info("indexes for 'events' created.")
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS markets (
                market_id TEXT PRIMARY KEY,
                event_id TEXT REFERENCES events(event_id) ON DELETE CASCADE,
                slug TEXT,
                question TEXT,
                group_item_title TEXT,
                new BOOLEAN,
                liquidity NUMERIC,
                volume NUMERIC,
                volume24hr NUMERIC,
                outcome_yes_price NUMERIC,
                outcome_no_price NUMERIC,
                one_day_price_change NUMERIC,
                image TEXT,
                fetch_date TIMESTAMP WITH TIME ZONE
                )
            ''')
            logger.info("table 'markets' created.")
            
            # markets indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_markets_event_id ON markets(event_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_markets_fetch_date ON markets(fetch_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_markets_volume24hr ON markets(volume24hr DESC) WHERE volume24hr > 0')
            logger.info("indexes for 'markets' created.")
        
        self.conn.commit()
    
    def insert_events(self, events_data: List[Tuple]):
        if not events_data:
            logger.warning("no events data to insert.")
            return
        
        sql = '''
            INSERT INTO events
                (event_id, slug, title, description,
                end_date, image, new, liquidity,
                volume, volume24hr, categories, fetch_date)
            VALUES %s
            ON CONFLICT (event_id) DO UPDATE
            SET
                slug = EXCLUDED.slug,
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                end_date = EXCLUDED.end_date,
                image = EXCLUDED.image,
                new = EXCLUDED.new,
                liquidity = EXCLUDED.liquidity,
                volume = EXCLUDED.volume,
                volume24hr = EXCLUDED.volume24hr,
                categories = EXCLUDED.categories,
                fetch_date = EXCLUDED.fetch_date
        '''
        
        try:
            with self.conn.cursor() as cursor:

                event_ids = [event[0] for event in events_data]
                if event_ids:
                    placeholders = ','.join(['%s'] * len(event_ids))
                    cursor.execute(f"DELETE FROM events WHERE event_id NOT IN ({placeholders})", event_ids)
                    deleted = cursor.rowcount
                    if deleted > 0:
                        logger.info(f"deleted {deleted} closed/inactive events")

                execute_values(cursor, sql, events_data, page_size=1000)
                self.conn.commit()
                logger.info(f"inserted/updated {len(events_data)} events.")
        except psycopg2.Error as e:
            logger.error(f"events insertion failed: {e}")
            self.conn.rollback()
            raise
    
    def insert_markets(self, markets_data: List[Tuple]):
        if not markets_data:
            logger.warning("no markets data to insert.")
            return
        
        sql = '''
            INSERT INTO markets 
                (market_id, event_id, slug, question,
                group_item_title, new, liquidity, volume,
                volume24hr, outcome_yes_price, outcome_no_price, one_day_price_change,
                image, fetch_date)
            VALUES %s
            ON CONFLICT (market_id) DO UPDATE
            SET 
                event_id = EXCLUDED.event_id,
                slug = EXCLUDED.slug,
                question = EXCLUDED.question,
                group_item_title = EXCLUDED.group_item_title,
                new = EXCLUDED.new,
                liquidity = EXCLUDED.liquidity,
                volume = EXCLUDED.volume,
                volume24hr = EXCLUDED.volume24hr,
                outcome_yes_price = EXCLUDED.outcome_yes_price,
                outcome_no_price = EXCLUDED.outcome_no_price,
                one_day_price_change = EXCLUDED.one_day_price_change,
                image = EXCLUDED.image,
                fetch_date = EXCLUDED.fetch_date
        '''
        
        try:
            with self.conn.cursor() as cursor:
                market_ids = [market[0] for market in markets_data]
                if market_ids:
                    placeholders = ','.join(['%s'] * len(market_ids))
                    cursor.execute(f"DELETE FROM markets WHERE market_id NOT IN ({placeholders})", market_ids)
                    deleted = cursor.rowcount
                    if deleted > 0:
                        logger.info(f"deleted {deleted} closed/inactive markets")

                execute_values(cursor, sql, markets_data, page_size=1000)
                self.conn.commit()
                logger.info(f"inserted/updated {len(markets_data)} markets.")
        except psycopg2.Error as e:
            logger.error(f"markets insertion failed: {e}")
            self.conn.rollback()
            raise