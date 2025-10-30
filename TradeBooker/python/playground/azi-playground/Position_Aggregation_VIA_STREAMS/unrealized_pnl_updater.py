import time
import redis
import logging
from pnl_calculator import PnLCalculator
# from redis.sentinel import Sentinel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PnLUpdater:
    def __init__(self, position_hash="positions", update_interval=3600):
        """
        Initializes the PnL updater.

        :param position_hash: Redis hash that stores positions (account_id/ticker → shares held)
        :param update_interval: Interval (in seconds) at which to update unrealized PnL
        """
        # Connect using Sentinel
        """
        sentinel = Sentinel(
            [('sentinel1', 26379), ('sentinel2', 26379), ('sentinel3', 26379)],
            socket_timeout=None,
            decode_responses=True
        )
        self.redis = sentinel.master_for("mymaster", socket_timeout=None, decode_responses=True)
        """
        self.redis = redis.Redis(host='localhost', port=6379, decode_responses=True)
        self.position_hash = position_hash
        self.update_interval = update_interval
        self.pnl_calculator = PnLCalculator()

    def get_all_positions(self):
        """
        Fetches all positions from the Redis hash.

        :return: A dictionary where keys are account_id/ticker, and values are quantities.
        """
        try:
            return self.redis.hgetall(self.position_hash)
        except Exception as e:
            logger.error(f"Failed to fetch positions: {e}")
            return {}

    def update_unrealized_pnl(self):
        """
        Updates the unrealized PnL for all positions and logs the number of changes.
        """
        logger.info("Starting unrealized PnL update for all positions.")

        positions = self.get_all_positions()
        if not positions:
            logger.warning("No positions found. Unrealized PnL cannot be updated.")
            return

        changed_count = 0
        total_positions = len(positions)

        for key, quantity in positions.items():
            try:
                account_id, ticker = key.split("/")
                # Update unrealized PnL and check if the value was actually changed
                if self.pnl_calculator.store_and_calculate_unrealized_pnl_position(account_id, ticker):
                    changed_count += 1
            except Exception as e:
                logger.error(f"Failed to update unrealized PnL for {key}: {e}")

        logger.info(f"Unrealized PnL update completed. {changed_count} of {total_positions} positions were updated.")

    def run(self):
        #Runs the PnL updater periodically.
        while True:
            self.update_unrealized_pnl()
            logger.info(f"Sleeping for {self.update_interval} seconds before next update.")
            time.sleep(self.update_interval)

if __name__ == "__main__":
    updater = PnLUpdater(update_interval=3600)  # Updates every hour by default
    updater.run()