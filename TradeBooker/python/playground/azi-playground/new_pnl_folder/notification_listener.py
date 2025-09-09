import redis
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def valid_date(date_string: str) -> bool:
    """Helper to validate YYYY-MM-DD date strings."""
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def run_listener(redis_host='localhost', redis_port=6379, redis_db=0):
    """
    Listens to keyspace notifications and pushes trade keys into sharded lists.
    """
    r = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
    pubsub = r.pubsub(ignore_subscribe_messages=True)

    # Subscribe to all keyspace events for 'hset'
    notification_channel = f"__keyspace@{redis_db}__:*"
    pubsub.psubscribe(notification_channel)

    logger.info(f"🚀 Notification listener started. Subscribed to {notification_channel}")
    logger.info("Waiting for trade notifications to push into queues...")

    for message in pubsub.listen():
        if message['type'] != 'pmessage' or message['data'] != 'hset':
            continue

        key_name = message['channel'].split(":", 1)[-1]

        # Validate if it's a trade key (e.g., 'account:date:uuid')
        parts = key_name.split(':')
        if len(parts) != 3 or not valid_date(parts[1]):
            logger.debug(f"Ignoring non-trade key: {key_name}")
            continue

        # Determine the shard and the corresponding queue
        first_char = key_name[0].lower()
        if 'a' <= first_char <= 'z':
            queue_key = f"pnl_queue:{first_char}"
            # Push the trade key into the sharded queue
            r.lpush(queue_key, key_name)
            logger.info(f"📬 Pushed trade key '{key_name}' to queue '{queue_key}'")
        else:
            logger.warning(f"Key '{key_name}' does not map to a known shard. Ignoring.")


if __name__ == "__main__":
    run_listener()