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

        if sentinels is None:
            sentinels = [("sentinel1", 26379), ("sentinel2", 26379), ("sentinel3", 26379)]

        sentinel = Sentinel(sentinels, socket_timeout=None, decode_responses=True)
        self.redis = sentinel.master_for(service_name, socket_timeout=None, decode_responses=True)

        self.pubsub = self.redis.pubsub()
        notification_channel = f"__keyspace@{redis_db}__:*"  # Wildcard to match all keys
        self.pubsub.psubscribe(notification_channel)

        logger.info(f"Subscribed to Redis keyspace notifications on pattern: {notification_channel}")

    def _update_position(self, key: str):
        def _update_position(self, key: str):
    try:
        # Step 1: Parse the account_id from the key
        account_id, _, _ = key.split(':')

        # Step 2: Load the trade hash to get the ticker
        trade_data = self.redis.hgetall(key)
        if not trade_data or 'ticker' not in trade_data:
            logger.warning(f"Skipping {key}: missing or invalid trade data")
            return

        ticker = trade_data['ticker']
        position_key = f"{account_id}:{ticker}"

        # Step 3: Get all keys for this account (account:*:*)
        pattern = f"{account_id}:*:*"
        all_keys = self.redis.keys(pattern)

        # Step 4: Recalculate total position for this ticker
        total_position = 0
        for k in all_keys:
            h = self.redis.hgetall(k)
            if h.get("ticker") != ticker:
                continue
            try:
                qty = int(h["quantity"])
                typ = h["type"].lower()
                total_position += qty if typ == "buy" else -qty
            except Exception as e:
                logger.warning(f"Skipping malformed trade at {k}: {e}")

        # Step 5: Save recomputed position
        self.redis.hset(self.trade_hash, position_key, total_position)
        logger.info(f"Recalculated position for {position_key}: {total_position}")

    except Exception as e:
        logger.error(f"Failed to update position from key {key}: {e}")


    def listen(self):
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
            #the following code, checking there is a date in the middle as a means of differntiating from a market price on redis, which also has three parts of its key, should be swapped out by having trade prefix to trades in redis or having DB=1 not 0 to ensure they are on different keyspaces
            if len(parts) == 3 and valid_date(parts[1]):

                #new code to split up workload across instances, splitting based on first letter of accountid
                account_id = parts[0].lower()
                first_char = account_id[0]
                if self.letter_range[0] <= first_char <= self.letter_range[1]:
                    self._update_position(keyname)
                else:
                    continue
            else:
                logger.debug(f"Skipping non-trade key: {keyname}")

if __name__ == "__main__":
    import sys
    
    #right now even without letter chunking this class is failing. 

    # or change to do the letter chunking here and then statically call up 30 instances or so
    # Accept command line argument like A-J or K-P, so that docker can spin up various instances for each chunk of alphabet
    letter_range_arg = sys.argv[1] if len(sys.argv) > 1 else "a-z"
    start, end = letter_range_arg.lower().split("-")

    aggregator = PortfolioAggregator(letter_range=(start, end))
    aggregator.listen()
