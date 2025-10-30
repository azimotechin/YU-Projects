import uuid
import random
from datetime import datetime
import time
# from redis.sentinel import Sentinel  # For Sentinel-based Redis connection
import redis  # For direct Redis connection


# --- Sentinel config (commented out) ---
# sentinel = Sentinel(
#     [('sentinel1', 26379), ('sentinel2', 26379), ('sentinel3', 26379)],
#     socket_timeout=None,
#     decode_responses=True
# )

# r = sentinel.master_for("mymaster", socket_timeout=None, decode_responses=True)

# --- Direct localhost Redis connection ---
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

accounts = ["alice", "bob", "charlie"]
tickers = ["AAPL", "MSFT", "GOOG"]
trade_types = ["buy", "sell"]
action_type = "trade"

#name_counter = 0  # global counter


def book_trade_to_stream():
    #account = random.choice(accounts)
    global name_counter
    base_name = random.choice(accounts)
    #account = f"{base_name}{name_counter + 1}"
    #name_counter += 1
    ticker = random.choice(tickers)
    price = round(random.uniform(100, 300), 2)
    trade_type = random.choice(trade_types)
    quantity = random.randint(1, 10)


    trade_string = f"{base_name},{ticker}:{price}:{trade_type}:{quantity}:{action_type}"

    r.xadd("trades_stream", {
        "trade_string": trade_string
    })

    #print(f"📤 Booked trade: {trade_string}")

# For debugging purposes, lets you see if the positions aggregator is working
def print_positions():
    print("📊 Current Positions:")
    all_keys = r.hkeys("positions")
    for key in all_keys:
        val = r.hget("positions", key)
        print(f"  {key} → {val}")

# For debugging purposes, lets you see if the trade booker is working
def print_all_past_trades():
    print("📊 All trades in Redis:")
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match="*:*:*", count=100)
        for key in keys:
            trade_data = r.hgetall(key)
            print(f"{key}: {trade_data}")
        if cursor == 0:
            break

if __name__ == "__main__":
    for _ in range (100): #100k trades 
        for _ in range(1000): #1k Trades
            book_trade_to_stream()
        print("booked 1k trades stream.", flush=True)
    #time.sleep(3)
    #print_positions()
    #print_all_past_trades()