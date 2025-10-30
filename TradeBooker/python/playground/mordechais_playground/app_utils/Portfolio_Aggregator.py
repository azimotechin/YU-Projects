import redis
import logging
from datetime import datetime

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def valid_date(date_string: str) -> bool:
    """Helper to validate YYYY-MM-DD date strings."""
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False

class PortfolioAggregator:
    def __init__(self, redis_host='localhost', redis_port=6379, trade_hash="positions"):
        self.trade_hash = trade_hash
        self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

        logger.info("Connected to local Redis instance.")

    def _update_position(self, key: str):
        """
        Given a Redis key, fetch the trade hash and update the portfolio.
        Assumes trade keys are formatted as account:YYYY-MM-DD:trade_id.
        """
        try:
            hash_data = self.redis.hgetall(key)
            if not hash_data:
                logger.warning(f"⚠️ No data found for key {key}")
                return
            
            # NEW: Only process if full hash exists (avoiding proccesing trade when it is created and when individual fields are inputted, multiple of which may send hset notifications)
            if len(hash_data) < 6:
                logger.debug(f"⏩ Partial trade ignored: {key}")
                return

            account, date, _ = key.split(':')
            ticker = hash_data['ticker']
            trade_type = hash_data['type'].lower()
            quantity = int(hash_data['quantity'])

            position_key = f"{account}:{ticker}"
            delta = quantity if trade_type == "buy" else -quantity

            self.redis.hincrby(self.trade_hash, position_key, delta)
            logger.info(f"📈 Updated position for {position_key} by {delta}")
        except Exception as e:
            logger.error(f"❌ Failed to update position for key {key}: {e}")

    def listen(self):
        logger.info("🔄 Listening for new trade keys...")

        pubsub = self.redis.pubsub()
        pubsub.psubscribe('__keyspace@0__:*')

        for message in pubsub.listen():
            if message['type'] != 'pmessage':
                continue

            key_event = message['channel']  # e.g., '__keyspace@0__:abraham:2024-04-23:uuid'
            keyname = key_event.split(":", 1)[-1]  # remove '__keyspace@0__:' prefix
            event_type = message['data']  # e.g., 'hset'

            # Only act on HSET events
            if event_type != 'hset':
                continue

            parts = keyname.split(':')
            if len(parts) == 3 and valid_date(parts[1]):
                self._update_position(keyname)
            else:
                logger.debug(f"Skipping non-trade key: {keyname}")

if __name__ == "__main__":
    aggregator = PortfolioAggregator()
    aggregator.listen()