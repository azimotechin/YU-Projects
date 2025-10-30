import redis
import socket
import uuid
import logging
from Trade import Trade
import time

# Set up logging configuration: INFO level, log messages will be printed
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)  # Use module name as logger name

class TradeBooker:
    def __init__(self, stream_key="trades_stream", position_hash="positions", consumer_group="booker-group"):
        # sentinel = Sentinel(
        #     [('sentinel1', 26379), ('sentinel2', 26379), ('sentinel3', 26379)],
        #     socket_timeout=None,
        #     decode_responses=True
        # )
        # self.redis = sentinel.master_for("mymaster", socket_timeout=None, decode_responses=True)

        self.redis = redis.Redis(host='localhost', port=6379, decode_responses=True)

        self.stream_key = stream_key
        self.group = consumer_group

        self.consumer = f"booker-consumer-{uuid.uuid4()}"

        logger.info(f"My consumer name is: {self.consumer}")


        # Check if the stream exists before creating a consumer group
        #the if statement checks if the consumer group is already in redis or not
        if self.redis.exists(self.stream_key):
            try:
                # Try to create a new consumer group starting from the beginning of the stream
                self.redis.xgroup_create(self.stream_key, self.group, id='0')
                logger.info(f"Created consumer group '{self.group}'")
            except redis.ResponseError as e:
                # If the group already exists, log it and move on
                if "BUSYGROUP" in str(e):
                    logger.info(f"Consumer group '{self.group}' already exists")
                else:
                    raise
        else:
            #hence, make sure docker starts the stream (albeit with no trades coming in yet) BEFORE starting position_aggregator and trade_booker
            logger.error(f"Stream '{self.stream_key}' not found")
            raise RuntimeError("Stream not found. Exiting.")

    def listen_and_book(self):
        logger.info("Starting to listen and book trades to Redis...")

        #Following two lines for speed tracking
        self.booked = 0
        self.start_time = time.time()

        while True:
            try:
                messages = self.redis.xreadgroup(
                groupname=self.group,
                consumername=self.consumer,
                streams={self.stream_key: '>'},
                count=80,
                block=5000
                )

                for stream, entries in messages:
                    for msg_id, fields in entries:
                        try:
                            # Expecting a single 'trade_string' field now                         
                            trade = Trade.create_from_full_input(fields["trade_string"])
                            key = trade.to_redis_key()
                            hash_data = trade.to_redis_hash()
                            self.redis.hset(key, mapping=hash_data)
                            self.redis.xack(self.stream_key, self.group, msg_id)
                            logger.info(f"[{self.consumer}] booked trade to Redis as key '{key}'")

                            #For speed tracking:
                            self.booked += 1

                            # Every 100 trades, log rate
                            if self.booked % 100 == 0:
                                elapsed = time.time() - self.start_time
                                rate = self.booked / elapsed
                                logger.info(f"[{self.consumer}] Booked {self.booked} trades at {rate:.2f} trades/sec")


                        except Exception as e:
                            logger.error(f"Failed to process message {msg_id}: {e}")
                            self.redis.xack(self.stream_key, self.group, msg_id)
            except Exception as e:
                logger.error(f"Redis stream read error: {e}")
            
            


if __name__ == "__main__":
    booker = TradeBooker()
    booker.listen_and_book()