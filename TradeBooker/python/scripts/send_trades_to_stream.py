import uuid
import random
from datetime import datetime
import time
from multiprocessing import Process
import logging
import sys
from redis.sentinel import Sentinel  # For Sentinel-based Redis connection
import yfinance as yf
# import redis  # For direct Redis connection

# Configure the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

sentinel = Sentinel(
    [('sentinel1', 26379), ('sentinel2', 26379), ('sentinel3', 26379)],
    socket_timeout=None,
    decode_responses=True
)

r = sentinel.master_for("mymaster", socket_timeout=None, decode_responses=True)

# --- Direct localhost Redis connection ---
# r = redis.Redis(host='localhost', port=6379, decode_responses=True)

accounts = ["alice", "bob", "charlie", "diblaim"]
tickers = ["AAPL", "MSFT", "GOOG"]
trade_types = ["buy", "sell"]
action_type = "trade"

#name_counter = 0  # global counter


def book_random_trade_to_stream(r):
    #account = random.choice(accounts)
    global name_counter
    base_name = random.choice(accounts)
    #account = f"{base_name}{name_counter + 1}"
    #name_counter += 1
    ticker = random.choice(tickers)
    price = round(random.uniform(100, 300), 2)
    trade_type = random.choice(trade_types)
    quantity = random.randint(1, 10)
    realistic_pricing: bool = False


    trade_string = f"{base_name},{ticker}:{price}:{trade_type}:{quantity}:{action_type}"

    r.xadd("trades_stream", {
        "trade_string": trade_string
    })

    #print(f"📤 Booked trade: {trade_string}")

def book_custom_trade_to_stream(trade_string, r):
    r.xadd("trades_stream", {
        "trade_string": trade_string
    })

    return True


def book_trades_in_batches(
        r,
        num_trades: int,
        accounts: list,
        tickers: list,
        trade_types: list,
        quantity_range: tuple,
        price_range: tuple,
        realistic_pricing: bool = False,  # <-- NEW PARAMETER
        batch_size: int = 1000,
        status_container=None,
        start_time=None
):
    """
    Generates and books a precise number of realistic trades in batches.
    """
    if num_trades <= 0 or not all([accounts, tickers, trade_types]):
        logger.error("Invalid parameters for trade generation.")
        return 0

    account_positions = {}
    live_prices = {}

    # --- START: MODIFIED PRICE FETCHING LOGIC ---
    if realistic_pricing:
        logger.info("Checking Redis cache and fetching live prices for tickers...")
        for ticker in tickers:
            cache_key = f"market_price:{ticker}"

            # 1. First, try to get the price from the Redis cache
            cached_price = r.get(cache_key)

            if cached_price:
                live_prices[ticker] = float(cached_price)
                logger.debug(f"Cache HIT for {ticker}: ${live_prices[ticker]}")
            else:
                # 2. If not in cache, fetch from yfinance
                logger.debug(f"Cache MISS for {ticker}. Fetching from yfinance...")
                try:
                    price = yf.Ticker(ticker).info.get("regularMarketPrice")
                    if price:
                        live_prices[ticker] = price
                        # 3. Store the newly fetched price in Redis with a 60-second expiry
                        r.set(cache_key, price, ex=60)
                    else:
                        logger.warning(f"yfinance returned no price for {ticker}.")
                except Exception:
                    logger.warning(f"Could not fetch live price for {ticker}. Will use default price range.")
    # --- END: MODIFIED PRICE FETCHING LOGIC ---

    num_generated = 0
    pipe = r.pipeline(transaction=False)
    min_qty, max_qty = quantity_range
    action = "trade"

    while num_generated < num_trades:
        can_sell = any(account_positions.values())
        trade_type = "sell" if can_sell and "sell" in trade_types and random.random() < 0.4 else "buy"

        if trade_type == "sell":
            eligible_accounts = [acc for acc, pos in account_positions.items() if pos]
            if not eligible_accounts:
                trade_type = "buy"
            else:
                account = random.choice(eligible_accounts)
                ticker = random.choice(list(account_positions[account].keys()))
                max_sellable = account_positions[account][ticker]
                quantity = random.randint(min(min_qty, max_sellable), max_sellable)
                account_positions[account][ticker] -= quantity
                if account_positions[account][ticker] == 0:
                    del account_positions[account][ticker]

        if trade_type == "buy":
            account = random.choice(accounts)
            ticker = random.choice(tickers)
            quantity = random.randint(min_qty, max_qty)
            if account not in account_positions:
                account_positions[account] = {}
            account_positions[account][ticker] = account_positions[account].get(ticker, 0) + quantity

        # Generate price based on whether realistic pricing is enabled and available
        if realistic_pricing and ticker in live_prices and live_prices[ticker]:
            base_price = live_prices[ticker]
            # Price will be within +/- 20% of the live market price
            price = round(random.uniform(base_price * 0.8, base_price * 1.2), 2)
        else:
            # Fallback to the user-defined price range from the sliders
            price = round(random.uniform(price_range[0], price_range[1]), 2)

        trade_string = f"{account},{ticker}:{price}:{trade_type}:{quantity}:{action}"
        pipe.xadd("trades_stream", {"trade_string": trade_string})
        num_generated += 1

        if num_generated % batch_size == 0:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
            if status_container:
                status_container.update(label=f"Generated {num_generated:,} of {num_trades:,} trades...")

    if num_generated % batch_size != 0:
        pipe.execute()

    return num_generated

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

def run_instance():
        for _ in range(5):  # 50k trades
            for _ in range(10000):  # 10k trades
                book_random_trade_to_stream(r)
            print("booked 10k trades stream.", flush=True)

if __name__ == "__main__":
    try:
        num_instances = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    except ValueError:
        print("Invalid argument. Usage: python send_trades_to_stream.py [num_instances]")
        sys.exit(1)

    processes = []
    for i in range(num_instances): # Example: Send in "20" as command line arg to send 1 million trades to stream.
        p = Process(target=run_instance)
        p.start()
        processes.append(p)

    for p in processes:
        p.join()