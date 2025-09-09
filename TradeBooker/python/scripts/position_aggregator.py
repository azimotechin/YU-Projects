import redis
from redis.sentinel import Sentinel
import logging
from datetime import datetime
import sys
import time
import threading


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PortfolioAggregator:
    def __init__(self, sentinels=None, service_name="mymaster", redis_db=0):
       
        # Redis setup
        if sentinels is None:
            sentinels = [("sentinel1", 26379), ("sentinel2", 26379), ("sentinel3", 26379)]

        sentinel = Sentinel(sentinels, socket_timeout=None, decode_responses=True)
        self.redis = sentinel.master_for(service_name, socket_timeout=None, decode_responses=True)

        self.positions_hash_key = "positions"  # Where aggregated positions are stored
        
        #Wait for redis to load the dataset
        self.wait_for_redis_ready()

        # Prep method that proceses dirty positions (accountid/ticker combos who've had new trades w/o position being aggregated yet)
        self.dirty_positions = set()
        threading.Thread(target=self._run_dirty_loop_every_second, daemon=True).start()  # runs in background
        
        # Subscribe to Redis keyspace notifications via Pub/Sub
        self.pubsub = self.redis.pubsub()

        self.letter_range = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" # Default is to subscribe to entire alphabet
        if len(sys.argv) == 2:
            self.letter_range = sys.argv[1].upper()
        self.patterns = [f"__keyspace@{redis_db}__:{letter}*,*:*:*" for letter in self.letter_range]

        for pattern in self.patterns:
            self.pubsub.psubscribe(pattern)
            logger.info(f"Subscribed to Redis keyspace notifications on pattern: {pattern}")
        
        #Upon instantiation (starting up after potentially having been down and missing trades,
        # send ALL id,tickercombos of past trades into dirty positions, reagregating evrything.
        self.mark_all_positions_dirty() 
        

    def reaggregate_position(self, account_id: str, ticker: str):
        try:
            total_quantity = 0
            
            for key in self.redis.scan_iter(match=f"{account_id},{ticker}:*", count=1000):
                trade = self.redis.hgetall(key)
                if not trade:
                    logger.info(f"Skipping key {key}. trade is empty")
                    continue
                try:
                    quantity = int(trade['quantity'])
                    #logger.info(f"Quantity of this particular trade: {quantity}")
                    trade_type = trade['type'].lower()
                    if trade_type == 'buy':
                        total_quantity += quantity
                    elif trade_type == 'sell':
                        total_quantity -= quantity
                except Exception as e:
                    logger.warning(f"Invalid trade data in key {key}: {e}")


            position_key = f"{account_id}:{ticker}"

            self.redis.hset(self.positions_hash_key, position_key, total_quantity)
            logger.info(f"💥🔥🧨💣🚒🙀 Successfully re-aggregated {position_key}:{total_quantity}")

        except Exception as e:
            logger.error(f"❌ Failed to reaggregate position for {account_id},{ticker}: {e}")

    def listen(self):
        logger.info("Listening for trade updates...")

        for message in self.pubsub.listen():

            if message['type'] != 'pmessage':
                continue

            keyname = message['channel'].split("__keyspace@0__:")[-1]
            event_type = message['data']

            if event_type != 'hset':
                continue

            try:
                account_ticker_combo = keyname.split(":")[0]  # "Ari,GOOG"
                self.dirty_positions.add(account_ticker_combo)

            except Exception as e:
                logger.error(f"⚠️ Failed to process key {keyname}: {e}")
    def _run_dirty_loop_every_second(self):
        while True:
            logger.info("Running dirty loop")

            current_dirties = list(self.dirty_positions)
            self.dirty_positions.clear()
            #self.dirty_positions.difference_update(current_dirties)  # Slower but perhaps a moticum safer, only removes processed items (in case new items added in between setting of current dirties and the clearing)

            for dirty_position in current_dirties:
                account_id, ticker = dirty_position.split(",")
                self.reaggregate_position(account_id, ticker)
            time.sleep(1)

    def mark_all_positions_dirty(self):
        logger.info("Marking all positions dirty upon starting up")
        for key in self.redis.scan_iter(match="*,*:*", count=1000):
            try:
                account_ticker = key.split(":")[0]  # e.g: "Ari,GOOG"
                first_letter = account_ticker[0].upper()
                if first_letter in self.letter_range:
                    self.dirty_positions.add(account_ticker)
            except Exception as e:
                logger.warning(f"⚠️ Could not process key {key}: {e}")
        logger.info(f"Total positions marked dirty by this running instance/process: {len(self.dirty_positions)}")

    def wait_for_redis_ready(self, timeout=60):
        logger.info("Waiting for Redis to be ready...")
        start = time.time()
        while time.time() - start < timeout:
            try:
                if self.redis.ping() and self.redis.info()['loading'] == 0:
                    logger.info("Redis is ready.")
                    return
            except Exception:
                pass
            time.sleep(1)
        raise TimeoutError("Redis did not become ready within timeout")

if __name__ == "__main__":
    PortfolioAggregator().listen()
