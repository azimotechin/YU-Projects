# import redis
from redis.sentinel import Sentinel
import logging
from datetime import datetime
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
# Key in Redis to store the timestamp of the last processed trade
LAST_PROCESSED_KEY = "pnl_listener:last_processed_ts"


def valid_date(date_string: str) -> bool:
    """Helper to validate YYYY-MM-DD date strings."""
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False


class ResilientNotificationListener:
    def __init__(self, redis_db=0):
        self.redis_db = redis_db

        # Connect using Sentinel
        sentinel = Sentinel(
            [('sentinel1', 26379), ('sentinel2', 26379), ('sentinel3', 26379)],
            socket_timeout=None,
            decode_responses=True
        )
        self.redis = sentinel.master_for("mymaster", socket_timeout=None, decode_responses=True)

        # self.redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)

        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        notification_channel = f"__keyspace@{self.redis_db}__:*"
        self.pubsub.psubscribe(notification_channel)
        logger.info(f"Subscribed to Redis keyspace notifications on {notification_channel}")

    def process_missed_trades(self):
        """
        On startup, scan for trades that were created while the listener was down,
        sort them chronologically, and then add them to the appropriate PnL queues.
        """
        logger.info("Checking for trades missed during downtime...")

        # Get the timestamp of the last trade we successfully processed
        last_ts = self.redis.get(LAST_PROCESSED_KEY)
        last_processed_timestamp = float(last_ts) if last_ts else 0.0

        # 1. Collect all missed trades before processing
        missed_trades = []
        logger.info("Scanning and collecting all potentially missed trades...")
        for key in self.redis.scan_iter(match="*,*:*:*"):
            try:
                # Get the creation time from the trade hash
                trade_time_str = self.redis.hget(key, "trade_time")
                if not trade_time_str:
                    continue

                # We need a full datetime object to compare timestamps
                trade_date_str = key.split(':')[1]
                trade_datetime_str = f"{trade_date_str} {trade_time_str}"
                trade_dt = datetime.strptime(trade_datetime_str, '%Y-%m-%d %H:%M:%S')
                trade_timestamp = trade_dt.timestamp()

                # If the trade was created after the last one we processed, it's a missed trade
                if trade_timestamp > last_processed_timestamp:
                    # Add the key and its timestamp to a list
                    missed_trades.append((trade_timestamp, key))

            except (ValueError, IndexError, TypeError) as e:
                logger.warning(f"Could not parse timestamp for key {key}. Error: {e}")
                continue

        if not missed_trades:
            logger.info("No missed trades found. System is up to date.")
            return

        # 2. Sort the collected trades by timestamp (the first item in the tuple)
        logger.info(f"Found {len(missed_trades)} missed trades. Sorting them now...")
        missed_trades.sort(key=lambda x: x[0])

        # 3. Process the trades in chronological order
        for trade_timestamp, key in missed_trades:
            self.add_trade_to_queue(key, trade_timestamp)

        logger.info(f"Successfully queued {len(missed_trades)} sorted missed trades.")

    # The function now accepts a trade_timestamp
    def add_trade_to_queue(self, key_name: str, trade_timestamp: float):
        """
        Validates a trade key and adds it to the correct sharded PnL queue.
        """
        try:
            parts = key_name.split(':')
            if len(parts) != 3 or not valid_date(parts[1]) or ',' not in parts[0]:
                logger.debug(f"Ignoring non-trade key (format mismatch): {key_name}")
                return

            account_name = parts[0].split(',')[0]
            first_char = account_name[0].lower()

            if 'a' <= first_char <= 'z':
                queue_key = f"pnl_queue:{first_char}"
                self.redis.lpush(queue_key, key_name)
                logger.info(f"Pushed trade key '{key_name}' to queue '{queue_key}'")

                # Only update the last processed timestamp if the new trade is more recent.
                # This prevents out-of-order events from moving the timestamp backward.
                current_last_ts = self.redis.get(LAST_PROCESSED_KEY)
                if not current_last_ts or trade_timestamp > float(current_last_ts):
                    self.redis.set(LAST_PROCESSED_KEY, trade_timestamp)
            else:
                logger.warning(f"Key '{key_name}' does not map to a known shard. Ignoring.")
        except IndexError:
            logger.warning(f"Could not parse account name from key: {key_name}")

    def listen_for_realtime_trades(self):
        """
        Listens for live trade notifications from Redis Pub/Sub.
        """
        logger.info("Waiting for real-time trade notifications...")
        for message in self.pubsub.listen():
            if message['type'] == 'pmessage' and message['data'] == 'hset':
                key_name = message['channel'].split(":", 1)[-1]

                # For real-time trades, we must now fetch the trade details to get its timestamp
                # before adding it to the queue.
                try:
                    trade_time_str = self.redis.hget(key_name, "trade_time")
                    if not trade_time_str:
                        continue

                    trade_date_str = key_name.split(':')[1]
                    trade_datetime_str = f"{trade_date_str} {trade_time_str}"
                    trade_dt = datetime.strptime(trade_datetime_str, '%Y-%m-%d %H:%M:%S')
                    trade_timestamp = trade_dt.timestamp()

                    self.add_trade_to_queue(key_name, trade_timestamp)
                except (ValueError, IndexError, TypeError) as e:
                    logger.warning(f"Could not parse real-time trade for key {key_name}. Error: {e}")
                    continue


def main():
    listener = ResilientNotificationListener()

    # First, process any trades that were missed while the service was down
    listener.process_missed_trades()

    # Then, start listening for new, real-time trades
    listener.listen_for_realtime_trades()


if __name__ == "__main__":
    main()