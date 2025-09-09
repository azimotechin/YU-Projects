import redis
# from redis.sentinel import Sentinel
import logging
import socket
import uuid
from Trade import Trade 



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class position_aggregator:
    def __init__(self, stream_key="trades_stream", position_hash="positions", consumer_group="aggregator-consumers"):
        # sentinel = Sentinel(
        #     [('sentinel1', 26379), ('sentinel2', 26379), ('sentinel3', 26379)],
        #     socket_timeout=None,
        #     decode_responses=True
        # )
        # self.redis = sentinel.master_for("mymaster", socket_timeout=None, decode_responses=True)

        self.redis = redis.Redis(host='localhost', port=6379, decode_responses=True)

        self.stream_key = stream_key
        self.position_hash = position_hash
        self.group = consumer_group

        self.consumer = f"aggregator-consumer-{uuid.uuid4()}"

        logger.info(f"My consumer name is: {self.consumer}")

        if self.redis.exists(self.stream_key):
            try:
                self.redis.xgroup_create(self.stream_key, self.group, id='0')
                logger.info(f"Consumer group '{self.group}' created.")
            except redis.ResponseError as e:
                if "BUSYGROUP" in str(e):
                    logger.info(f"Consumer group '{self.group}' already exists.")
                else:
                    raise
        else:
            #hence, make sure docker starts the stream (albeit with no trades coming in yet) BEFORE starting position_aggregator and trade_booker
            logger.warning(f"Stream '{self.stream_key}' not found. Aggregator cannot run.")
            raise RuntimeError("Stream not found. Aggregator exiting.")

    def update_position(self, trade: Trade):
        key = f"{trade.account_id}/{trade.ticker}"
        change = trade.quantity if trade.trade_type == "buy" else -trade.quantity
        self.redis.hincrby(self.position_hash, key, change)
        logger.info(f"Updated position: {key} += {change}")

    def listen(self):
        logger.info("Listening for trades...")
        while True:
            try:
                messages = self.redis.xreadgroup(
                    groupname=self.group,
                    consumername=self.consumer,
                    streams={self.stream_key: '>'},
                    count=10,
                    block=5000
                )
                for stream, entries in messages:
                    for msg_id, fields in entries:
                        try:
                            # Expecting a single 'trade_string' field now                         
                            trade = Trade.create_from_full_input(fields["trade_string"])
                            self.update_position(trade)
                            self.redis.xack(self.stream_key, self.group, msg_id)
                        except Exception as e:
                            logger.error(f"Failed to process message {msg_id}: {e}")
            except Exception as e:
                logger.error(f"Redis read error: {e}")

if __name__ == "__main__":
    aggregator = position_aggregator()
    aggregator.listen()