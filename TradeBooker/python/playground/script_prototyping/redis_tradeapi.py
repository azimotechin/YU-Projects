import os
import random
import redis
import uuid
from datetime import datetime
from faker import Faker
import time
import logging
import subprocess
from redis.sentinel import Sentinel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

def create_random_trades(num_trades: int) -> list:
    all_users = ["abraham", "issac", "jacob", "moses", "aaron", "joshua", "caleb", "david", "solomon", "daniel", "elijah", "isaiah", "jeremiah", "ezekiel", "hosea"]
    all_tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NFLX", "NVDA", "AMD", "INTL", "ORCL", "UBER", "PYPL", "CRM", "ADBE", "SHOP", "BABA", "SQ", "COIN", "SNOW", "ROKU", "ZM", "PLTR"]
    trades = []
    for i in range(num_trades):
        account_id = random.choice(all_users)
        trade_date = fake.date_between(start_date='-100y', end_date='today').strftime('%Y-%m-%d')
        #trade_id = Placeholder for trade ID, will be replaced with a UUID in the actual writing to redis
        ticker = random.choice(all_tickers)
        price = round(random.uniform(1, 1000), 2)
        trade_type = random.choice(["buy", "sell"])
        quantity = random.randint(1, 1000)
        trades.append(f"{account_id}:{trade_date},{ticker}:{price}:{trade_type}:{quantity}")#since this is generating trade based on the input format, I chose not to include the uuid because the client cannot generate it and has no way of knowing what it will be. 
    return trades

def write_trades_to_redis(list_of_trades: list) -> bool:
    sentinel = Sentinel([
    ('sentinel1', 26379),
    ('sentinel2', 26379),
    ('sentinel3', 26379)
    ], socket_timeout=0.5)

    r = sentinel.master_for('mymaster', socket_timeout=0.5, decode_responses=True)
    for trade in list_of_trades:
        account_id, trade_date, ticker, price, trade_type, quantity = trade.replace(',', ':').split(':')
        trade_data = {
            "ticker": ticker,
            "price": price,
            "type": trade_type,
            "quantity": quantity,
        }

        trade_key = f"trade:{account_id}:{trade_date}:{str(uuid.uuid4())}"
        r.hset(trade_key, mapping=trade_data)
    return True

def write_trades_to_redis_with_benchmarking(list_of_trades: list) -> bool:
    sentinel = Sentinel([
    ('sentinel1', 26379),
    ('sentinel2', 26379),
    ('sentinel3', 26379)
    ], socket_timeout=0.5)

    r = sentinel.master_for('mymaster', socket_timeout=0.5, decode_responses=True)
    start = time.time()
    write_trades_to_redis(list_of_trades)
    end = time.time()
    duration = end - start
    logger.info(f" Number of trades written: {len(list_of_trades)}")
    logger.info(f" Time taken: {duration:.2f} seconds")
    logger.info(f" Average time per trade: {duration / len(list_of_trades):.6f} seconds")
    logger.info(f" Total trades in Redis: {len(r.keys())}")
    return True

def get_all_trades_from_redis() -> list:
    sentinel = Sentinel([
    ('sentinel1', 26379),
    ('sentinel2', 26379),
    ('sentinel3', 26379)
    ], socket_timeout=0.5)

    r = sentinel.master_for('mymaster', socket_timeout=0.5, decode_responses=True)
    all_trades = []
    for key in r.keys():
        trade = r.hgetall(key)
        key_parts = key.split(':')
        account_id = key_parts[0]
        trade_date = key_parts[1]
        trade_id = key_parts[2]
        trade_string = f"{account_id}:{trade_date}:{trade_id},{trade['ticker']}:${trade['price']}:{trade['type']}:{trade['quantity']}"
        all_trades.append(trade_string)
    return all_trades

def verify_format(trade:str)->bool:
    try:
        key ,value = trade.split(',',1)
        user, date, = key.split(':',1)
        if not user or any(not (c.isalnum() or c == '_') for c in user):
            return False
        datetime.strptime(date, '%Y-%m-%d')#will throw exception if it isn't working, causing false to be returned

        ticker, price, side, qty = value.split(':', 3)
        if not ticker.isalpha():
            return False
        
        if not price.startswith('$'):
            return False
        float(price[1:])  # exception if not a number
        if side not in ('BUY', 'SELL'):
            return False
        
        int(qty) #exception if not a num
    except(ValueError, IndexError):
        return False
    return True

def convert_single_trade_to_list(trade:str):#For adding a single trade to redis
    if verify_format(trade):
        return[trade]
    else:
        print("Formatting error, trades must be in the following format:\nuser:YYYY-MM-DD,ticker:$price:BUY/SELL:quantity")
### brew services start redis
### brew services stop redis
def main():
    # for i in range(1):
    #     logger.info(f"Running iteration {i+1} of 1")
    #     trades = create_random_trades(10000)
    #     write_trades_to_redis_with_benchmarking(trades)
    trade_list = convert_single_trade_to_list("BrendaJackson:2025-06-22,AMZN:$208:BUY:46")
        #input("Please type a trade in the following format\nuser:YYYY-MM-DD,ticker:$price:BUY/SELL:quantity\n")
        #Docker does not work with command line inputs like above
    

    if trade_list:  # only True if trade_list is not empty
        write_trades_to_redis(trade_list)

if __name__ == "__main__":
    main()
    #daniel commented this out, feel free to undo and run your own tests
    # subprocess.run(["brew", "services", "start", "redis"]) #automatically starts redis service
    # time.sleep(1)  # Wait 1 second for Redis to start
    #    sentinel = Sentinel([
    # ('sentinel1', 26379),
    # ('sentinel2', 26379),
    # ('sentinel3', 26379)
    # ], socket_timeout=0.5)

    # r = sentinel.master_for('mymaster', socket_timeout=0.5, decode_responses=True) #open redis connection to clear data
    # r.flushall()  # Deletes everything in the database before running the script
    # main()
    # subprocess.run(["brew", "services", "stop", "redis"]) #automatically stops redis service