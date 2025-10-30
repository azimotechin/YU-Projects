# import redis
from redis.sentinel import Sentinel
import logging
from Trade import Trade
import json
import market_data
from datetime import datetime
import sys
import time
import threading  # Import threading
from queue import Queue  # Import a thread-safe queue

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BRPOP_TIMEOUT = 1
MAX_BATCH_SIZE = 1000


def valid_date(date_string: str) -> bool:
    """Helper to validate YYYY-MM-DD date strings."""
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False


class PnLCalculator:
    def __init__(self, shard_range=None, service_name="mymaster", redis_db=0):
        self.shard_char = shard_range[0] if shard_range else None

        # If a shard is provided, set it up as a worker
        if self.shard_char:
            logger.info(f"PnL Worker initialized for shard: {self.shard_char.upper()}")
            self.queue_key = f"pnl_queue:{self.shard_char}"
        # If no shard, it's a utility instance, not a worker
        else:
            logger.info("PnLCalculator initialized in utility mode (no sharding).")
            self.queue_key = None

        # Connect using Sentinel
        sentinel = Sentinel(
            [('sentinel1', 26379), ('sentinel2', 26379), ('sentinel3', 26379)],
            socket_timeout=None,
            decode_responses=True
        )
        self.redis = sentinel.master_for("mymaster", socket_timeout=None, decode_responses=True)

        # self.redis = redis.Redis(host='localhost', port=6379, decode_responses=True)

        self.lots_key_prefix = "lots:"
        self.unrealized_pnl_by_position_hash = "unrealized_pnl_by_position"
        self.realized_pnl_by_position_hash = "realized_pnl_by_position"

        # Threading and Internal Queue Setup
        self.internal_trade_queue = Queue()
        # Start a dedicated background thread for processing.
        # It's a daemon so it exits when the main program exits.
        processing_thread = threading.Thread(target=self._processing_worker_loop, daemon=True)
        processing_thread.start()
        logger.info("Started background processing thread.")

        logger.info(f"Will pull trades from Redis list: '{self.queue_key}'")
        logger.info(f"Will store realized PnL in Redis hash: '{self.realized_pnl_by_position_hash}'")

    def _processing_worker_loop(self):
        """
        This function runs in a separate thread.
        It continuously pulls trade keys from the internal queue and processes them one by one.
        """
        logger.info("Background processor is running...")
        while True:
            # The .get() call is blocking, it will wait until an item is available.
            key_to_process = self.internal_trade_queue.get()
            try:
                self._process_trade_from_key(key_to_process)
            except Exception as e:
                logger.error(f"Error in processing worker for key {key_to_process}: {e}")
            finally:
                # Signal that the task from the queue is done.
                self.internal_trade_queue.task_done()

    def run_worker(self):
        """
        Main worker loop. It efficiently pulls trade keys from Redis and
        hands them off to the internal processing queue.
        This version combines low-latency waiting with high-throughput batching.
        """
        logger.info(
            f"Ingestion worker started for shard '{self.shard_char.upper()}'. "
            f"Waiting for trades from '{self.queue_key}'..."
        )
        while True:
            try:
                # 1. Wait for the FIRST trade to arrive.
                # This blocks with very low resource usage and provides instant responsiveness.
                message = self.redis.brpop(self.queue_key, timeout=BRPOP_TIMEOUT)

                if not message:
                    # BRPOP timed out, no trades arrived. Continue the loop.
                    logger.debug("BRPOP timed out. No new trades.")
                    continue

                # 2. Process the first trade immediately.
                first_trade_key = message[1]
                logger.debug(f"Ingested first trade key '{first_trade_key}'. Pushing to internal queue.")
                self.internal_trade_queue.put(first_trade_key)

                # 3. Now, try to grab a BATCH of any remaining trades.
                # This avoids processing one-by-one during a high-volume burst.
                remaining_keys = self.redis.lpop(self.queue_key, MAX_BATCH_SIZE - 1)

                if remaining_keys:
                    logger.info(f"Ingested a follow-up batch of {len(remaining_keys)} trades.")
                    for key in remaining_keys:
                        self.internal_trade_queue.put(key)

            except Exception as e:
                logger.error(f"Error in main ingestion loop: {e}")
                time.sleep(5)  # Sleep on error to prevent fast failure loops

    def _process_trade_from_key(self, key: str):
        """
        Process a trade from a Redis key. (No changes needed in this function)
        """
        try:
            hash_data = self.redis.hgetall(key)
            if not hash_data or len(hash_data) < 6:
                logger.warning(f"Incomplete or no data found for key {key}. Skipping.")
                return

            # account_id = key.split(':')[0]
            account_id_comma_ticker = key.split(':')[0]
            account_id, ticker = account_id_comma_ticker.split(',')

            # Defensive check: ensure key belongs to this worker's shard
            if not account_id.lower().startswith(self.shard_char):
                logger.warning(
                    f"Safety check: Worker for shard '{self.shard_char}' received key for another shard: '{key}'. Skipping.")
                return

            trade = Trade(
                account_id=account_id,
                ticker=ticker,
                price=float(hash_data['price']),
                trade_type=hash_data['type'].lower(),
                quantity=int(hash_data['quantity']),
                trade_time=hash_data['trade_time']
            )

            self.process_trade_fifo(trade)
            logger.info(f"Processed trade from key: {key}")

        except Exception as e:
            logger.error(f"Failed to process trade from key {key}: {e}")

    def _get_lots_key(self, account_id: str, ticker: str) -> str:
        """Generate a redis key in the following format 'lots:alice/AAPL'"""
        return f"{self.lots_key_prefix}{account_id}/{ticker}"

    def _get_lots(self, account_id: str, ticker: str) -> list:
        """Returns a list of all lots(trades) for the user and ticker"""
        lots_key = self._get_lots_key(account_id, ticker)
        lots_json = self.redis.get(lots_key)

        if lots_json:
            return json.loads(lots_json)
        else:
            return []  # No lots exist yet

    def _save_lots(self, account_id: str, ticker: str, lots: list):
        """Saves lots and stores in into json for the lot key"""
        lots_key = self._get_lots_key(account_id, ticker)
        lots_json = json.dumps(lots)

        self.redis.set(lots_key, lots_json)
        logger.info(f"Saved {len(lots)} lots for {account_id}/{ticker}")

    def process_trade_fifo(self, trade: Trade):
        """Function that processes according to whether the trade is a buy or sell"""
        if trade.trade_type == "buy":
            self._process_buy_fifo(trade)
        elif trade.trade_type == "sell":
            self._process_sell_fifo(trade)
        else:
            logger.error(f"Unknown trade type {trade.trade_type}")

    def _process_buy_fifo(self, trade: Trade):
        """Process buy trades by adding to lots"""
        existing_lots = self._get_lots(trade.account_id, trade.ticker)

        new_lot = {
            "price": trade.price,
            "quantity": trade.quantity,
            "date": trade.trade_date,
            "time": trade.trade_time
        }
        existing_lots.append(new_lot)

        self._save_lots(trade.account_id, trade.ticker, existing_lots)

        logger.info(f"BUY - Added lot of {trade.quantity} shares @ ${trade.price}")
        logger.info(f"{trade.account_id}/{trade.ticker} now has {len(existing_lots)} lots")

        self.store_and_calculate_unrealized_pnl_position(trade.account_id, trade.ticker)

    def _process_sell_fifo(self, trade: Trade):
        """
        Process sell trades using FIFO methodology.
        Removes the trade quantity of shares from the lots, since it is FIFO it removes the oldest added lots.
        If sell quantity is higher than the first lot, this method will consume multiple lots and calculate the realized PnL accordingly.
        Partial consumption of lots is also permitted.
        """
        existing_lots = self._get_lots(trade.account_id, trade.ticker)

        if not existing_lots:
            logger.warning(f"SELL - No lots to sell for {trade.account_id}/{trade.ticker} - cannot calculate PnL")
            return

        remaining_to_sell = trade.quantity
        total_realized_pnl = 0.0

        logger.info(
            f"SELL - Need to sell {remaining_to_sell} shares of {trade.ticker} from a total of {len(existing_lots)}")

        lots_to_remove = []

        for i, lot in enumerate(existing_lots):
            if remaining_to_sell <= 0:
                break

            if lot["quantity"] <= remaining_to_sell:
                # Consume entire lot
                realized_pnl_from_lot = (trade.price - lot["price"]) * lot["quantity"]
                total_realized_pnl += realized_pnl_from_lot
                remaining_to_sell -= lot["quantity"]
                lots_to_remove.append(i)

                logger.info(
                    f"Consumed entire lot: {lot['quantity']} @ ${lot['price']} → PnL: ${realized_pnl_from_lot:.2f}")
            else:
                # Partial consumption of the current lot
                realized_pnl_from_lot = (trade.price - lot["price"]) * remaining_to_sell
                total_realized_pnl += realized_pnl_from_lot
                lot["quantity"] -= remaining_to_sell
                logger.info(
                    f"Partially consumed lot: {remaining_to_sell} from {lot['quantity'] + remaining_to_sell} @ ${lot['price']} → PnL: ${realized_pnl_from_lot:.2f}")
                logger.info(f"Remaining in lot: {lot['quantity']} shares")
                remaining_to_sell = 0

        # Remove fully consumed lots
        existing_lots = [lot for i, lot in enumerate(existing_lots) if i not in lots_to_remove]

        # Save updated lots
        self._save_lots(trade.account_id, trade.ticker, existing_lots)

        # Update realized PnL for this account
        if total_realized_pnl != 0:
            position_key = f"{trade.account_id}/{trade.ticker}"
            self.redis.hincrbyfloat(self.realized_pnl_by_position_hash, position_key, total_realized_pnl)
            logger.info(f"Realized PnL for {position_key}: ${total_realized_pnl:.2f}")
            self.store_and_calculate_unrealized_pnl_position(trade.account_id, trade.ticker)

        if remaining_to_sell > 0:
            logger.warning(f"Could not sell {remaining_to_sell} shares - insufficient lots!")

    # UNREALIZED PNL FUNCTIONS BELOW
    def get_live_price(self, ticker: str) -> float:
        """Get current market price using market_data.py module"""
        try:
            price_per_share = market_data.get_price(ticker, 1)
            logger.debug(f"  Got live price for {ticker}: ${price_per_share:.2f}")
            return price_per_share

        except ValueError as e:
            logger.warning(f"  Failed to get live price for {ticker}: {e}")
            return None
        except Exception as e:
            logger.error(f"  Unexpected error getting price for {ticker}: {e}")
            return None

    def store_and_calculate_unrealized_pnl_position(self, account_id: str, ticker: str) -> bool:
        """
        Calculate and store unrealized PnL for a specific position.
        Returns True if the value changed, False otherwise.
        """
        position_key = f"{account_id}/{ticker}"

        # Get the old value before calculating the new one
        try:
            old_pnl = float(self.redis.hget(self.unrealized_pnl_by_position_hash, position_key) or 0.0)
        except (ValueError, TypeError):
            old_pnl = 0.0

        # Calculate the new value
        new_pnl = self.calculate_unrealized_pnl_single(account_id, ticker)

        # Compare old and new values, rounded to the nearest cent
        if round(new_pnl, 2) != round(old_pnl, 2):
            self.redis.hset(self.unrealized_pnl_by_position_hash, position_key, new_pnl)
            logger.debug(f"   Stored updated unrealized PnL for {position_key}: ${new_pnl:.2f}")
            return True
        else:
            # If the value hasn't changed, no need to write to Redis
            logger.debug(f"   Unrealized PnL for {position_key} is unchanged. Skipping update.")
            return False

    def calculate_unrealized_pnl_single(self, account_id: str, ticker: str) -> float:
        """Calculate unrealized PnL for a single position"""
        # Get current lots for this position
        lots = self._get_lots(account_id, ticker)

        if not lots:
            logger.debug(f"No lots found for {account_id}/{ticker}")
            return 0.0

        # Get live market price
        live_price = self.get_live_price(ticker)
        if live_price is None:
            logger.warning(f" Cannot calculate unrealized PnL for {account_id}/{ticker} - no live price")
            return 0.0

        # Calculate unrealized PnL for each lot
        total_unrealized_pnl = 0.0
        total_shares = 0
        total_cost = 0.0

        for lot in lots:
            lot_quantity = lot["quantity"]
            lot_cost_basis = lot["price"]

            # PnL for this lot: (market_price - cost_basis) × quantity
            lot_unrealized_pnl = (live_price - lot_cost_basis) * lot_quantity
            total_unrealized_pnl += lot_unrealized_pnl

            # Track totals for logging
            total_shares += lot_quantity
            total_cost += lot_cost_basis * lot_quantity

        # Calculate weighted average cost basis for logging
        avg_cost_basis = total_cost / total_shares if total_shares > 0 else 0

        logger.info(f"   Unrealized PnL for {account_id}/{ticker}:")
        logger.info(f"   Current position: {total_shares} shares")
        logger.info(f"   Average cost basis: ${avg_cost_basis:.2f}")
        logger.info(f"   Current market price: ${live_price:.2f}")
        logger.info(f"   Unrealized PnL: ${total_unrealized_pnl:.2f}")

        return total_unrealized_pnl


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python pnl_calculator.py <shard_char>")
        print("Example: python pnl_calculator.py a")
        sys.exit(1)

    letter = sys.argv[1].lower()
    # The PnLCalculator now takes a single character for its shard
    calculator = PnLCalculator(shard_range=(letter, letter))

    # The main entry point is now the run_worker loop
    calculator.run_worker()