import redis
import random

# --- Direct localhost Redis connection ---
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# State to track which accounts own which tickers
# Format: {"alice1": {"AAPL", "MSFT"}, "bob2": {"GOOG"}}
account_positions = {}

# --- Configuration ---
BASE_ACCOUNTS = ["alice", "bob", "charlie"]
TICKERS = ["AAPL", "MSFT", "GOOG"]
ACTION_TYPE = "trade"
name_counter = 0

def book_trade_to_stream():
    """
    Generates a realistic trade, ensuring sells only happen for
    accounts that have previously bought that specific stock.
    Also allows accounts to own multiple different tickers.
    """
    global name_counter

    can_sell = bool(account_positions)
    # 30% chance of selling (if possible), otherwise buy.
    if can_sell and random.random() < 0.3:
        # --- Generate a SELL trade ---
        trade_type = "sell"
        # Pick an account known to have positions
        account = random.choice(list(account_positions.keys()))
        # Pick a ticker that the chosen account owns
        ticker = random.choice(list(account_positions[account]))
        quantity = random.randint(1, 5)

    else:
        # --- Generate a BUY trade ---
        trade_type = "buy"

        # 50% chance to add a position to an existing account (if any exist),
        # otherwise create a new account.
        if account_positions and random.random() < 0.5:
            # Pick an existing account
            account = random.choice(list(account_positions.keys()))
        else:
            # Create a new, unique account name
            name_counter += 1
            base_name = random.choice(BASE_ACCOUNTS)
            account = f"{base_name}{name_counter}"

        ticker = random.choice(TICKERS)
        quantity = random.randint(1, 10)

        # Update our state tracker to record this new position
        if account not in account_positions:
            account_positions[account] = set()
        account_positions[account].add(ticker)

    # Generate a random price for the trade
    price = round(random.uniform(100, 300), 2)
    trade_string = f"{account},{ticker}:{price}:{trade_type}:{quantity}:{ACTION_TYPE}"

    # Add the trade to the Redis stream
    r.xadd("trades_stream", {
        "trade_string": trade_string
    })

    print(f"📤 Booked trade: {trade_string}")


if __name__ == "__main__":
    print("Starting trade generator...")
    for _ in range(410):
        book_trade_to_stream()
    print("Trade generation complete.")