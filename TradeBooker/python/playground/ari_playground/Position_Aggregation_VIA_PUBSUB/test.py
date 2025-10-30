from TradeManager import TradeManager
from PortfolioAggregator import PortfolioAggregator
import threading
import time

def print_all_positions(redis_client, position_hash="positions"):
    positions = redis_client.hgetall(position_hash)
    print("\n--- CURRENT POSITIONS ---")
    for key, val in positions.items():
        print(f"{key}: {val}")
    print("--------------------------\n")

# Start the aggregator listener in a thread
aggregator = PortfolioAggregator()

# Start the aggregator's listener in a BACKGROUND thread so it can continuously listen for trades
# without blocking the rest of the program. `daemon=True` ensures the thread exits when the main program ends.
listener_thread = threading.Thread(target=aggregator.listen, daemon=True)
listener_thread.start()

# Write trades
manager = TradeManager()
trades = TradeManager.create_random_trades(5)
manager.write_trades(trades)

# Print each trade for debugging purposes
print("Trades that were written:")
for trade in trades:
    print(f"{trade.account_id}:{trade.ticker}:{trade.trade_type}:{trade.quantity}")

# Give aggregator time to consume
time.sleep(2)

# Print the current positions
print_all_positions(manager.redis_client)

# Write more trades
more_trades = TradeManager.create_random_trades(5)
manager.write_trades(more_trades)

# Print each trade for debugging purposes
print("Trades that were written:")
for trade in trades:
    print(f"{trade.account_id}:{trade.ticker}:{trade.trade_type}:{trade.quantity}")


# Give time again
time.sleep(2)

# Print updated positions
print_all_positions(manager.redis_client)
