from TradeManager import TradeManager
from Trade import Trade
import uuid
import time
import logging
from redis.sentinel import Sentinel

# Connect to Redis via Sentinel
sentinels = [("sentinel1", 26379), ("sentinel2", 26379), ("sentinel3", 26379)]
sentinel = Sentinel(sentinels, socket_timeout=0.5, decode_responses=True)
redis_conn = sentinel.master_for("mymaster", socket_timeout=0.5, decode_responses=True)

# Logger setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PortfolioTester:
    def __init__(self):
        self.manager = TradeManager()

    def clear_trades(self):
        logger.info("🧹 Clearing all existing trades...")
        self.manager.clear_all_trades()

    def write_sample_trades(self):
        logger.info("📦 Writing sample trades to Redis...")
        samples = [
            ("alfie", "AMZN", 3100.0, "buy", 100),
            ("alfie", "AMZN", 3105.0, "sell", 60), #should end up with alfie AMZN 40
            ("alfie", "GOOG", 2800.0, "buy", 40),  #should end up with alfie GOOG 40
            ("brad", "AMZN", 3090.0, "buy", 150),
            ("brad", "AAPL", 190.0, "buy", 30),   #should end up with brad AAPL 30
            ("brad", "AMZN", 3085.0, "sell", 50), #should end up with brad AMZN 100
            ("charles", "AAPL", 195.0, "buy", 20), #should end up with charles AAPL 20
            ("charles", "AMZN", 3120.0, "buy", 80),
            ("charles", "AMZN", 2790.0, "sell", 20), 
            ("charles", "AMZN", 3110.0, "sell", 30) #should end up with charles AMZIN 30
        ]

        for acct, ticker, price, action, qty in samples:
            trade = Trade.create_from_parts(acct, ticker, price, action, qty, "trade")
            self.manager.write_trade(trade)
            logger.info(f"✏️  Wrote trade: {trade}")

    def show_positions(self):
        logger.info("📊 Reading positions from Redis...")
        positions = self.manager.redis_client.hgetall("positions")
        if not positions:
            logger.warning("⚠️ No positions found.")
            return
        for k, v in positions.items():
            logger.info(f"📌 {k} → {v}")

if __name__ == "__main__":
    tester = PortfolioTester()
    tester.clear_trades()
    tester.write_sample_trades()
    time.sleep(1) #any shorter of a break and the printing starts happening faster than the portfolio actually gets updated
    #this is troubling, and implies it will take 2+ minutes at this rate to log 100,000 trades. Using a stream with 100 consumers is estimated to take just a handful of seconds to do the same thing
    tester.show_positions()
