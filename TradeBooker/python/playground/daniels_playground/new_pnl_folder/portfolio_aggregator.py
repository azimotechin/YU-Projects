import redis
from redis.sentinel import Sentinel
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
    def __init__(self, sentinels=None, service_name="mymaster", trade_hash="positions", redis_db=0):
        self.trade_hash = trade_hash

        # if sentinels is None:
        #     sentinels = [("sentinel1", 26379), ("sentinel2", 26379), ("sentinel3", 26379)]

        # sentinel = Sentinel(sentinels, socket_timeout=None, decode_responses=True)
        # self.redis = sentinel.master_for(service_name, socket_timeout=None, decode_responses=True)
        self.redis = redis.Redis(host = 'localhost', port = 6379, decode_responses=True)
        self.pubsub = self.redis.pubsub()
        notification_channel = f"__keyspace@{redis_db}__:*"  # Wildcard to match all keys
        self.pubsub.psubscribe(notification_channel)

        logger.info(f"Subscribed to Redis keyspace notifications on pattern: {notification_channel}")

    def _update_position(self, key: str):
        try:
    
            hash_data = self.redis.hgetall(key)
            if not hash_data:
                logger.warning(f"⚠️ No data found for key {key}")
                return
            # Only process if full hash exists (avoiding proccesing trade when it is created and when individual fields are inputted, multiple of which may send hset notifications)
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

        for message in self.pubsub.listen():
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
