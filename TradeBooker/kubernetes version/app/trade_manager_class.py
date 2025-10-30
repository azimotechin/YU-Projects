import logging
import redis
import time
import uuid
import csv
import os
from datetime import datetime
from typing import List
from trade import Trade

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TradeManager:
    
    def __init__(self, redis_host=None, redis_port=6379):
        if redis_host is None:
            redis_host = os.getenv('REDIS_HOST', 'redis-primary')
        
        try:
            self.redis_client = redis.Redis(
                host=redis_host, 
                port=redis_port, 
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True
            )
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {redis_host}:{redis_port}")
        except redis.ConnectionError as e:
            logger.error(f"Could not connect to Redis at {redis_host}:{redis_port}: {e}")
            raise
    
    @staticmethod
    def verify_format(trade: str) -> bool:
        try:
            key, value = trade.split(',', 1)
            account_id, trade_date = key.split(':', 1)
            ticker, price, trade_type, qty = value.split(':', 3)
            return True
        except (ValueError, IndexError):
            return False

    @staticmethod
    def convert_string_to_trade(trade: str) -> Trade:
        if not TradeManager.verify_format(trade):
            raise ValueError("Invalid trade format")
        
        key, value = trade.split(',', 1)
        account_id, trade_date = key.split(':', 1)
        ticker, price, trade_type, qty = value.split(':', 3)

        price = float(price[1:])  # Remove '$' and convert to float
        qty = int(qty)  # Convert quantity to integer
        
        trade_id = str(uuid.uuid4())  # Generate a unique trade ID
        trade_time = datetime.now().strftime("[%H:%M:%S]")  # Current time in specified format
        action_type = "trade"

        trade_type = trade_type.lower()
        return Trade(
            account_id=account_id,
            trade_date=trade_date,
            trade_id=trade_id,
            trade_time=trade_time,
            ticker=ticker,
            price=price,
            trade_type=trade_type,
            quantity=qty,
            action_type=action_type
        )
    
    @staticmethod
    def convert_full_string_to_trade(trade_string: str) -> Trade:
        try:
            return Trade.create_from_full_input(trade_string)
        except (ValueError, IndexError) as e:
            logger.error(f"Error converting string to trade: {e}")
            raise

    def write_trade(self, trade: Trade) -> bool:
        try:
            self.redis_client.hset(trade.to_redis_key(), mapping=trade.to_redis_hash())
            return True
        except Exception as e:
            logger.error(f"Error saving trade: {e}")
            return False
    
    def write_trades(self, trades: List[Trade]) -> bool:
        try:
            pipe = self.redis_client.pipeline()
            for trade in trades:
                pipe.hset(trade.to_redis_key(), mapping=trade.to_redis_hash())
            pipe.execute()
            return True
        except Exception as e:
            logger.error(f"Error saving trades: {e}")
            return False
    
    def write_trades_with_benchmarking(self, trades: List[Trade]) -> bool:
        start = time.time()

        success = self.write_trades(trades)

        end = time.time()
        duration = end - start
        
        logger.info(f" Number of trades written: {len(trades)}")
        logger.info(f" Time taken: {duration:.2f} seconds")
        logger.info(f" Average time per trade: {duration / len(trades):.6f} seconds")
        logger.info(f" Trades per second: {len(trades) / duration:.2f}")
        logger.info(f" Total trades in Redis: {len(self.redis_client.keys())}")
        print()
        
        return success
    
    def get_all_trades(self) -> List[Trade]:
        trades = []
        try:
            for key in self.redis_client.keys():
                hash_data = self.redis_client.hgetall(key)
                if hash_data:  # Make sure we got data
                    trade = Trade.from_redis_data(key, hash_data)
                    trades.append(trade)
        except Exception as e:
            logger.error(f"Error retrieving trades: {e}")
        
        return trades
    
    def clear_all_trades(self) -> bool:
        try:
            self.redis_client.flushall()
            return True
        except Exception as e:
            logger.error(f"Error clearing Redis: {e}")
            return False

    @staticmethod
    def create_random_trades(num_trades: int) -> List[Trade]:
        return [Trade.create_random() for _ in range(num_trades)]

    def write_trades_to_csv(self, filename: str) -> bool:
        try:
            trades = self.get_all_trades()
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                if trades:
                    fieldnames = ['account_id', 'trade_date', 'trade_id', 'trade_time', 'ticker', 'price', 'trade_type', 'quantity', 'action_type']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    for trade in trades:
                        writer.writerow({
                            'account_id': trade.account_id,
                            'trade_date': trade.trade_date,
                            'trade_id': trade.trade_id,
                            'trade_time': trade.trade_time,
                            'ticker': trade.ticker,
                            'price': trade.price,
                            'trade_type': trade.trade_type,
                            'quantity': trade.quantity,
                            'action_type': trade.action_type
                        })
            logger.info(f"Successfully wrote {len(trades)} trades to {filename}")
            return True
        except Exception as e:
            logger.error(f"Error writing trades to CSV: {e}")
            return False

    def get_trades_from_csv(self, filename: str) -> List[Trade]:
        trades = []
        try:
            with open(filename, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    trade = Trade(
                        account_id=row['account_id'],
                        trade_date=row['trade_date'],
                        trade_id=row['trade_id'],
                        trade_time=row['trade_time'],
                        ticker=row['ticker'],
                        price=float(row['price']),
                        trade_type=row['trade_type'],
                        quantity=int(row['quantity']),
                        action_type=row['action_type']
                    )
                    trades.append(trade)
        except Exception as e:
            logger.error(f"Error reading trades from CSV: {e}")
        
        return trades

    @staticmethod
    def write_list_of_trades_to_csv(trades: List[Trade], filename: str) -> bool:
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                if trades:
                    fieldnames = ['account_id', 'trade_date', 'trade_id', 'trade_time', 'ticker', 'price', 'trade_type', 'quantity', 'action_type']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    for trade in trades:
                        writer.writerow({
                            'account_id': trade.account_id,
                            'trade_date': trade.trade_date,
                            'trade_id': trade.trade_id,
                            'trade_time': trade.trade_time,
                            'ticker': trade.ticker,
                            'price': trade.price,
                            'trade_type': trade.trade_type,
                            'quantity': trade.quantity,
                            'action_type': trade.action_type
                        })
            return True
        except Exception as e:
            logger.error(f"Error writing trades to CSV: {e}")
            return False
