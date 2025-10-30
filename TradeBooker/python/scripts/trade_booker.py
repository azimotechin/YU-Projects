import redis
from redis.sentinel import Sentinel
import socket
import uuid
import logging
from Trade import Trade
import time
from datetime import datetime
import sys
import subprocess
from zoneinfo import ZoneInfo # Use the modern, built-in library
import json
import os
import threading

# Timezone Configuration
EST = ZoneInfo("America/New_York")

# Set up logging configuration:
os.makedirs("logs/booker_logs", exist_ok=True) #ensure a logs file exists
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)  # Use module name as logger name

class TradeBooker:
    def __init__(self, stream_key="trades_stream", position_hash="positions", consumer_group="booker-group"):
        # Connect using Sentinel
        sentinel = Sentinel(
            [('sentinel1', 26379), ('sentinel2', 26379), ('sentinel3', 26379)],
            socket_timeout=None,
            decode_responses=True
        )
        self.redis = sentinel.master_for("mymaster", socket_timeout=None, decode_responses=True)

        self.stream_key = stream_key
        self.group = consumer_group

        self.consumer = f"booker-consumer-{uuid.uuid4()}"

        logger.info(f"My consumer name is: {self.consumer}")
        logger.info("Connected to Redis via Sentinel: my-master")

        try:
            # The mkstream=True option will create the stream if it doesn't exist.
            self.redis.xgroup_create(self.stream_key, self.group, id='0', mkstream=True)
            logger.info(f"Successfully created consumer group '{self.group}' on stream '{self.stream_key}'.")
        except redis.ResponseError as e:
            # This error is expected if the consumer group already exists.
            if "BUSYGROUP" in str(e):
                logger.info(f"Consumer group '{self.group}' already exists.")
            else:
                # Re-raise any other unexpected errors.
                logger.error(f"Failed to create consumer group: {e}")
                raise

    def listen_and_book(self):
        logger.info("Starting to listen and book trades to Redis...")

        #For speed tracking
        self.booked = 0
        self.last_logged_count = 0
        self.start_time = time.time()

        while True:
            try:
                messages = self.redis.xreadgroup(
                groupname=self.group,
                consumername=self.consumer,
                streams={self.stream_key: '>'},
                count=1000,
                block=5000
                )

                for stream, entries in messages:
                    pipe = self.redis.pipeline(transaction=False)
                    for msg_id, fields in entries:
                        try:
                            trade_string = fields["trade_string"]
                            
                            #account_id, rest = trade_string.strip().split(',')
                            #ticker, price, trade_type, quantity, action_type = rest.split(':')

                            # Split on the first colon only — gives you accountID,ticker and the rest
                            account_comma_ticker_combo, rest = trade_string.split(":", 1)

                            # Split rest of string into remaining fields
                            price, trade_type, quantity, action_type = rest.split(":")

                            # Use timezone-aware time
                            now = datetime.now(EST)
                            trade_time = now.strftime("%H:%M:%S") # Will be in EST
                            trade_date = now.strftime('%Y-%m-%d') # Will be in EST
                            trade_id = str(uuid.uuid4())
                            
                            key = f"{account_comma_ticker_combo.strip()}:{trade_date}:{trade_id}"
                            #key = f"{account_id_comma_ticker.strip()}:{trade_date}:{trade_id}"
                            account, ticker = account_comma_ticker_combo.split(",")

                            hash_data = {
                                "account": account.strip(),
                                "trade_date": trade_date,
                                "trade_time": trade_time,
                                "ticker": ticker.strip(),
                                "price": price.strip(),
                                "type": trade_type.strip().lower(),
                                "quantity": quantity.strip(),
                                "action_type": action_type.strip().lower()
                            }

                            pipe.hset(key, mapping=hash_data)
                            pipe.sadd(f"accounts", account.strip())
                            pipe.incr("total_trades_booked")
                            pipe.xack(self.stream_key, self.group, msg_id)
                            self.booked += 1
                        except Exception as e:
                            logger.error(f"Failed to process message {msg_id}: {e}")
                            self.redis.xack(self.stream_key, self.group, msg_id)
                    pipe.execute()
            
                    # Log every ~1000 trades
                    if self.booked - self.last_logged_count >= 1000:
                        elapsed = time.time() - self.start_time
                        rate = self.booked / elapsed
                        logger.info(f"[{self.consumer}] Booked {self.booked} trades at {rate:.2f} trades/sec")
                        self.last_logged_count = self.booked

            except Exception as e:
                logger.error(f"Redis stream read error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        num_instances = int(sys.argv[1])

        # Launch N-1 additional bookers as subprocesses
        for i in range(1, num_instances):
            log_file = open(f"logs/booker_logs/booker_log_{i+1}.txt", "w")
            subprocess.Popen(
                ["python", __file__],
                stdout=log_file,
                stderr=subprocess.STDOUT
            )

        print(f"Launched {num_instances - 1} subprocesses. This process will be instance #{num_instances}.")

    # This process itself becomes the final booker (ensuring this process stays alive, keeping the docker container alive, avoiding killing off all the above subprocceses)
    while True:
        booker = TradeBooker()
        booker.listen_and_book()

