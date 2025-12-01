import json
from datetime import datetime, timezone
from typing import List, Dict, Tuple
import logging

logger = logging.getLogger(__name__)


class DataTransformer:
    @staticmethod
    def transform_events_and_markets(events_data: List[Dict]) -> Tuple[List[Tuple], List[Tuple]]:
        events_insert_data = []
        markets_insert_data = []
        seen_event_ids = set()
        seen_market_ids = set()
        duplicate_events = 0
        duplicate_markets = 0
        fetch_date = datetime.now(timezone.utc)
        
        for event in events_data:
            event_id = event.get('id', '')
            
            if event_id in seen_event_ids:
                duplicate_events += 1
                continue
            seen_event_ids.add(event_id)
            
            tags = event.get('tags', [])
            categories = [tag.get('label', '') for tag in tags]
            
            events_insert_data.append((
                event_id,
                event.get('slug', ''),
                event.get('title', ''),
                event.get('description', ''),
                event.get('endDate'),
                event.get('image', ''),
                event.get('new', False),
                event.get('liquidity', 0),
                event.get('volume', 0),
                event.get('volume24hr', 0),
                json.dumps(categories),
                fetch_date
            ))
            
            for market in event.get('markets', []):
                market_id = market.get('id', '')
                
                if market_id in seen_market_ids:
                    duplicate_markets += 1
                    continue
                seen_market_ids.add(market_id)
                
                outcome_prices = json.loads(market.get('outcomePrices', '[]'))
                outcome_yes_price = float(outcome_prices[0]) if len(outcome_prices) > 0 else None
                outcome_no_price = float(outcome_prices[1]) if len(outcome_prices) > 1 else None
                
                markets_insert_data.append((
                    market_id,
                    event_id, 
                    market.get('slug', ''),
                    market.get('question', ''),
                    market.get('groupItemTitle', ''),
                    market.get('new', False),
                    market.get('liquidity', 0),
                    market.get('volume', 0),
                    market.get('volume24hr', 0),
                    outcome_yes_price,
                    outcome_no_price,
                    market.get('oneDayPriceChange', 0),
                    fetch_date
                ))
        
        if duplicate_events > 0:
            logger.info(f"removed {duplicate_events} duplicate event(s)")
        if duplicate_markets > 0:
            logger.info(f"removed {duplicate_markets} duplicate market(s)")
        
        return events_insert_data, markets_insert_data