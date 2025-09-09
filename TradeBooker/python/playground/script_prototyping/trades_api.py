import os
import random
import redis
import uuid
from datetime import datetime
import time
from faker import Faker
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

#Methods we need
#1) Write the list of trades into redis
#2) Get user info from redis for client
#3) Take user input from website and add into redis
#4) for debugging purposes, create a mock csv file with trades and put it in the same directory as this file

def create_random_trades(num_trades: int) -> list:
    # all users: abraham, issac, jacob, moses, aaron, joshua, caleb, samson, david, solomon
    # all_tickers: AAPL, MSFT, GOOGL, AMZN, TSLA, META, NFLX, NVDA, AMD, BABA, #CHATGPT ADD MORE HERE
    all_users = ["abraham", "issac", "jacob", "moses", "aaron", "joshua", "caleb", "samson", "david", "solomon"]
    all_tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NFLX", "NVDA", "AMD", "BABA"]
    trades = []
    for _ in range(num_trades):
        account_id = random.choice(all_users)
        trade_date = fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d')
        trade_id = str(uuid.uuid4())
        ticker = random.choice(all_tickers)
        price = round(random.uniform(100, 1000), 2)
        trade_type = random.choice(["buy", "sell"])
        quantity = random.randint(1, 100)
        trades.append(f"{account_id}:{trade_date}:{trade_id},{ticker}:{price}:{trade_type}:{quantity}")
    return trades

def read_trades_from_csv(csv_path: str) -> list:
    list_of_trades = []
    with open(csv_path, "r") as f:
        list_of_trades = f.read().splitlines()
    return list_of_trades

# this code writes trades to redis, but it also adds in a GUID to the end of the Value
def write_trades_to_redis(list_of_trades: list) -> bool:
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    for trade in list_of_trades:
        account_id, trade_date, ticker, price, trade_type, quantity = trade.replace(',', ':').split(':')
        trade_data = {
            "ticker": ticker,
            "price": price,
            "type": trade_type,
            "quantity": quantity,
            #"user_trade_id": len(get_user_trades_from_redis(account_id)) +1 
        }
        trade_key = f"trade:{account_id}:{trade_date}:{str(uuid.uuid4())}"
        r.hset(trade_key, mapping=trade_data)
    return True

def write_trades_to_redis_with_benchmarking(list_of_trades: list) -> bool:
    start = time.time()
    write_trades_to_redis(list_of_trades)
    end = time.time()
    duration = end - start
    logger.info(f" Number of trades written: {len(list_of_trades)}")
    logger.info(f" Time taken: {duration:.2f} seconds")
    logger.info(f" Average time per trade: {duration / len(list_of_trades):.6f} seconds")
    logger.info(f" Total trades in Redis: {len(r.keys())}")
    return True

def get_csv_path() -> str:
    current_dir = os.path.dirname(__file__)
    return os.path.join(current_dir, "bank_trades.csv")

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
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    if verify_format(trade):
        return[trade]
    else:
        print("Formatting error, trades must be in the following format:\nuser:YYYY-MM-DD,ticker:$price:BUY/SELL:quantity")

#before you run main, make sure you have
# a csv file called bank_trades.csv
# in the same directory as this file.
# if you dont, you can create one
# using the mock_trade_creator.py script
# also - to run this script, you need
# to have a redis server running
# this is how you can run redis locally:
# before you run the code, run this in you terminal:
### brew services start redis
# after you run the code, run this in your terminal:
### brew services stop redis
def main():
    trades = create_random_trades(100)  # Create 100 random trades
    for trade in trades:
        print(trade)
    # csv_path = get_csv_path()
    # all_data = read_trades_from_csv(csv_path)
    # write_trades_to_redis(all_data)
    # all_trades = get_all_trades_from_redis()
    # for trade in all_trades:
    #     print(trade)


if __name__ == "__main__":
    # uncomment the next line to clear all data in redis before running the script
    # this is useful for debugging purposes
    # r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    # r.flushall()  # Deletes all keys in all databases
    main()


# extra methods that we don't need right now, but might be useful later

# def get_all_trades_from_redis() -> list:
#     r = redis.Redis(host='localhost', port=6379, decode_responses=True)
#     all_trades = []
#     for key in r.keys():
#         trade = r.hgetall(key)
#         all_trades.append(trade)
#     return all_trades

# def get_user_trades_from_redis(account_id: str) -> list:
#     r = redis.Redis(host='localhost', port=6379, decode_responses=True)
#     user_trades = []
#     for key in r.keys(f"{account_id}:*"):
#         trade = r.hgetall(key)
#         user_trades.append(trade)
#     return user_trades