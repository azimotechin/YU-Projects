from datetime import datetime
import redis
from redis.sentinel import Sentinel
import uuid
import time
import logging
from typing import List
from .Trade import Trade
import csv
import os

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TradeManager:
    def __init__(self, host='localhost', port=6379):
        try:
            self.redis_client = redis.Redis(host=host, port=port, decode_responses=True)
            self.redis_client.ping()
        except redis.ConnectionError as e:
            logger.error(f"Could not connect to Redis: {e}")
            raise
    
    @staticmethod
    def verify_format(trade: str) -> bool:
        try:
            key, value = trade.split(',', 1)
            user, date = key.split(':', 1)
            
            # Validate user (alphanumeric + underscore)
            if not user or any(not (c.isalnum() or c == '_') for c in user):
                return False
                
            # Validate date format
            datetime.strptime(date, '%Y-%m-%d')
            
            # Parse value part
            ticker, price, side, qty = value.split(':', 3)
            
            # Validate ticker (letters only)
            if not ticker.isalpha():
                return False
            
            # Validate price format
            if not price.startswith('$'):
                return False
            float(price[1:])  # Will raise exception if not a valid number
            
            # Validate trade side
            if side.upper() not in ('BUY', 'SELL'):  # Accept both cases
                return False
            
            # Validate quantity
            int(qty)  # Will raise exception if not a valid integer
            
        except (ValueError, IndexError):
            return False
        return True

    @staticmethod
    def convert_string_to_trade(trade: str) -> Trade:
        if not TradeManager.verify_format(trade):
            raise ValueError("Invalid trade format. Expected format: user:YYYY-MM-DD,ticker:$price:BUY/SELL:quantity")
        
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
            key_part, value_part = trade_string.split(',', 1)
            account_id, trade_date, trade_id = key_part.split(':', 2)
            trade_time, ticker, price, trade_type, quantity, action_type = value_part.split(':', 5)
            
            # Remove $ from price if present
            if price.startswith('$'):
                price = price[1:]
            
            return Trade(
                account_id=account_id,
                trade_date=trade_date,
                trade_id=trade_id,
                trade_time=trade_time,
                ticker=ticker,
                price=float(price),
                trade_type=trade_type.lower(),
                quantity=int(quantity),
                action_type=action_type
            )
        except (ValueError, IndexError) as e:
            raise ValueError(f"Invalid full trade string format: {trade_string}") from e

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
                writer = csv.writer(csvfile)
                # Write header
                writer.writerow(['account_id', 'trade_date', 'trade_id', 'trade_time', 'ticker', 'price', 'trade_type', 'quantity', 'action_type'])
                
                # Write trade data
                for trade in trades:
                    writer.writerow([
                        trade.account_id,
                        trade.trade_date,
                        trade.trade_id,
                        trade.trade_time,
                        trade.ticker,
                        trade.price,
                        trade.trade_type,
                        trade.quantity,
                        trade.action_type
                    ])
            logger.info(f"Successfully wrote {len(trades)} trades to {filename}")
            return True
        except Exception as e:
            logger.error(f"Error writing trades to CSV: {e}")
            return False

    # this method assumes that all the trades in the CSV are *properly formatted trades".
    # if the CSV doesn't have properly formatted trades, use the raw data methods instead.
    # this assumes that the CSV has full trades like in a backup CSV file
    def get_trades_from_csv(self, filename: str) -> List[Trade]:
        trades = []
        try:
            if not os.path.exists(filename):
                logger.warning(f"CSV file {filename} does not exist")
                return trades
                
            with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row in reader:
                    try:
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
                    except (ValueError, KeyError) as e:
                        logger.warning(f"Skipping invalid row: {row}. Error: {e}")
                        continue
                        
            logger.info(f"Successfully read {len(trades)} trades from {filename}")
        except Exception as e:
            logger.error(f"Error reading trades from CSV: {e}")
        
        return trades

    @staticmethod
    def write_list_of_trades_to_csv(trades: List[Trade], filename: str) -> bool:
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                # Write header
                writer.writerow(['account_id', 'trade_date', 'trade_id', 'trade_time', 'ticker', 'price', 'trade_type', 'quantity', 'action_type'])
                
                # Write trade data
                for trade in trades:
                    writer.writerow([
                        trade.account_id,
                        trade.trade_date,
                        trade.trade_id,
                        trade.trade_time,
                        trade.ticker,
                        trade.price,
                        trade.trade_type,
                        trade.quantity,
                        trade.action_type
                    ])
            logger.info(f"Successfully wrote {len(trades)} trades to {filename}")
            return True
        except Exception as e:
            logger.error(f"Error writing trades to CSV: {e}")
            return False
        
    @staticmethod
    def read_raw_csv_data(filename: str) -> List[dict]:
        raw_data = []
        try:
            if not os.path.exists(filename):
                logger.warning(f"CSV file {filename} does not exist")
                return raw_data

            with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    raw_data.append(row)

            logger.info(f"Successfully read {len(raw_data)} rows from {filename}")
        except Exception as e:
            logger.error(f"Error reading raw CSV data: {e}")

        return raw_data

    @staticmethod
    def read_raw_csv_data_as_list(filename: str) -> List[str]:
        raw_data = []
        try:
            if not os.path.exists(filename):
                logger.warning(f"CSV file {filename} does not exist")
                return raw_data                
            with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    raw_data.append(','.join(row))
                    
            logger.info(f"Successfully read {len(raw_data)} rows from {filename}")
        except Exception as e:
            logger.error(f"Error reading raw CSV data: {e}")
        
        return raw_data
    
    @staticmethod
    def create_trades_from_raw_csv(filename: str) -> List[Trade]:
        raw_data = TradeManager.read_raw_csv_data(filename)
        trades = []
        
        for row in raw_data:
            try:
                # Generate missing fields if they don't exist
                trade_id = row.get('trade_id', str(uuid.uuid4()))
                trade_time = row.get('trade_time', datetime.now().strftime("[%H:%M:%S]"))
                action_type = row.get('action_type', 'trade')
                
                # Handle price - remove $ if present
                price_str = str(row.get('price', '0'))
                if price_str.startswith('$'):
                    price_str = price_str[1:]
                price = float(price_str)
                
                # Normalize trade_type to lowercase
                trade_type = str(row.get('trade_type', '')).lower()
                
                trade = Trade(
                    account_id=row.get('account_id', ''),
                    trade_date=row.get('trade_date', datetime.now().strftime('%Y-%m-%d')),
                    trade_id=trade_id,
                    trade_time=trade_time,
                    ticker=row.get('ticker', ''),
                    price=price,
                    trade_type=trade_type,
                    quantity=int(row.get('quantity', 0)),
                    action_type=action_type
                )
                trades.append(trade)
                
            except (ValueError, TypeError) as e:
                logger.warning(f"Skipping invalid row: {row}. Error: {e}")
                continue
                
        logger.info(f"Successfully created {len(trades)} Trade objects from raw CSV data")
        return trades
