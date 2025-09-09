import redis
import random
import time
import json

# --- Direct localhost Redis connection ---
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# State to track which accounts own which tickers and their quantities
# Format: {"alice1": {"AAPL": 10, "MSFT": 5}, "bob2": {"GOOG": 3}}
account_positions = {}

# Track all trades for manual PnL verification
all_trades = []

# --- Configuration ---
BASE_ACCOUNTS = ["alice", "bob", "charlie", "diana", "eve"]
TICKERS = ["AAPL", "MSFT", "GOOG", "TSLA", "NVDA"]
name_counter = 0

def clear_redis_stream():
    """Clear the trades stream"""
    try:
        r.delete("trades_stream")
        print("✅ Cleared trades_stream")
    except:
        pass

def book_trade_to_stream():
    """
    Generates a realistic trade and adds it to the Redis stream.
    The trade booker will read from this stream and create the Redis hashes.
    """
    global name_counter

    # Check if we can generate a sell trade
    can_sell = any(
        any(qty > 0 for qty in positions.values()) 
        for positions in account_positions.values()
    )
    
    # 40% chance of selling if positions exist, otherwise buy
    if can_sell and random.random() < 0.4:
        # --- Generate a SELL trade ---
        trade_type = "sell"
        
        # Pick an account that has positions
        accounts_with_positions = [
            acc for acc, positions in account_positions.items()
            if any(qty > 0 for qty in positions.values())
        ]
        account = random.choice(accounts_with_positions)
        
        # Pick a ticker that the account owns
        available_tickers = [
            ticker for ticker, qty in account_positions[account].items()
            if qty > 0
        ]
        ticker = random.choice(available_tickers)
        
        # Don't sell more than what's owned
        max_sellable = account_positions[account][ticker]
        quantity = random.randint(1, min(max_sellable, 10))
        
        # Update position tracking
        account_positions[account][ticker] -= quantity
        
    else:
        # --- Generate a BUY trade ---
        trade_type = "buy"
        
        # 60% chance to use existing account, 40% chance to create new one
        if account_positions and random.random() < 0.6:
            account = random.choice(list(account_positions.keys()))
        else:
            name_counter += 1
            base_name = random.choice(BASE_ACCOUNTS)
            account = f"{base_name}{name_counter}"
            account_positions[account] = {}
        
        ticker = random.choice(TICKERS)
        quantity = random.randint(1, 15)
        
        # Update position tracking
        if ticker not in account_positions[account]:
            account_positions[account][ticker] = 0
        account_positions[account][ticker] += quantity

    # Generate realistic price (with some volatility around base prices)
    base_prices = {"AAPL": 180, "MSFT": 350, "GOOG": 140, "TSLA": 250, "NVDA": 800}
    base_price = base_prices.get(ticker, 200)
    price = round(base_price * random.uniform(0.8, 1.2), 2)
    
    # Create trade string in the format expected by the trade booker
    ACTION_TYPE = "trade"
    trade_string = f"{account},{ticker}:{price}:{trade_type}:{quantity}:{ACTION_TYPE}"
    
    # Add the trade to the Redis stream
    r.xadd("trades_stream", {
        "trade_string": trade_string
    })
    
    # Track for manual verification
    trade_record = {
        "account": account,
        "ticker": ticker,
        "price": price,
        "type": trade_type,
        "quantity": quantity,
        "trade_string": trade_string
    }
    all_trades.append(trade_record)
    
    print(f"📤 Added to stream: {trade_string}")
    
    return trade_record

def calculate_manual_realized_pnl():
    """
    Calculate realized PnL manually using FIFO for verification.
    Returns dict of {account/ticker: realized_pnl}
    """
    manual_pnl = {}
    lots_by_position = {}  # {account/ticker: [lots]}
    
    # Process trades in the order they were generated (already chronological)
    for trade in all_trades:
        position_key = f"{trade['account']}/{trade['ticker']}"
        
        if trade['type'] == 'buy':
            # Add to lots
            if position_key not in lots_by_position:
                lots_by_position[position_key] = []
            
            lots_by_position[position_key].append({
                'price': trade['price'],
                'quantity': trade['quantity']
            })
            
        elif trade['type'] == 'sell':
            # Process sell using FIFO
            if position_key not in lots_by_position:
                continue
                
            if position_key not in manual_pnl:
                manual_pnl[position_key] = 0.0
                
            remaining_to_sell = trade['quantity']
            lots = lots_by_position[position_key]
            
            lots_to_remove = []
            for i, lot in enumerate(lots):
                if remaining_to_sell <= 0:
                    break
                    
                if lot['quantity'] <= remaining_to_sell:
                    # Consume entire lot
                    pnl = (trade['price'] - lot['price']) * lot['quantity']
                    manual_pnl[position_key] += pnl
                    remaining_to_sell -= lot['quantity']
                    lots_to_remove.append(i)
                else:
                    # Partial consumption
                    pnl = (trade['price'] - lot['price']) * remaining_to_sell
                    manual_pnl[position_key] += pnl
                    lot['quantity'] -= remaining_to_sell
                    remaining_to_sell = 0
            
            # Remove fully consumed lots
            lots_by_position[position_key] = [
                lot for i, lot in enumerate(lots) if i not in lots_to_remove
            ]
    
    return manual_pnl

def get_realized_pnl_from_redis():
    """Get realized PnL results from Redis (what the calculator computed)"""
    realized_pnl_hash = "realized_pnl_by_position"
    redis_pnl = r.hgetall(realized_pnl_hash)
    
    # Convert string values to float
    return {k: float(v) for k, v in redis_pnl.items()}

def print_trade_summary():
    """Print summary of generated trades"""
    buy_trades = [t for t in all_trades if t['type'] == 'buy']
    sell_trades = [t for t in all_trades if t['type'] == 'sell']
    
    print(f"\n📊 TRADE SUMMARY:")
    print(f"Total trades generated: {len(all_trades)}")
    print(f"Buy trades: {len(buy_trades)}")
    print(f"Sell trades: {len(sell_trades)}")
    print(f"Unique accounts: {len(set(t['account'] for t in all_trades))}")
    print(f"Unique tickers: {len(set(t['ticker'] for t in all_trades))}")

def compare_pnl_results():
    """Compare manual calculation with Redis results"""
    print(f"\n🔍 PNL VERIFICATION:")
    print("="*60)
    
    manual_pnl = calculate_manual_realized_pnl()
    redis_pnl = get_realized_pnl_from_redis()
    
    # Get all position keys
    all_positions = set(manual_pnl.keys()) | set(redis_pnl.keys())
    
    matches = 0
    mismatches = 0
    
    for position in sorted(all_positions):
        manual_value = manual_pnl.get(position, 0.0)
        redis_value = redis_pnl.get(position, 0.0)
        
        # Round to 2 decimal places for comparison
        manual_rounded = round(manual_value, 2)
        redis_rounded = round(redis_value, 2)
        
        status = "✅" if manual_rounded == redis_rounded else "❌"
        
        print(f"{status} {position:20} Manual: ${manual_rounded:8.2f} | Redis: ${redis_rounded:8.2f}")
        
        if manual_rounded == redis_rounded:
            matches += 1
        else:
            mismatches += 1
    
    print("="*60)
    print(f"Results: {matches} matches, {mismatches} mismatches")
    
    if mismatches == 0:
        print("🎉 ALL PNL CALCULATIONS MATCH! Your calculator is working correctly.")
    else:
        print("⚠️  Some mismatches found. Check the calculator logic.")
    
    return manual_pnl, redis_pnl

def clear_redis_pnl_data():
    """Clear existing PnL data from Redis"""
    print("🧹 Clearing existing PnL data from Redis...")
    
    # Clear realized PnL hash
    r.delete("realized_pnl_by_position")
    r.delete("unrealized_pnl_by_position")
    
    # Clear any existing lots
    for key in r.scan_iter(match="lots:*"):
        r.delete(key)
    
    # Clear any existing trade hashes
    for key in r.scan_iter(match="*:????-??-??:*"):
        r.delete(key)
    
    # Clear trades stream
    clear_redis_stream()
    
    print("✅ Redis cleared")

def main():
    """Main function to run the test"""
    print("🚀 Starting PnL Calculator Test")
    print("="*50)
    print()
    print("📋 SETUP INSTRUCTIONS:")
    print("1. Start your PnL calculator: python notifications_pnl.py")
    print("2. Press Enter to continue...")
    input()
    
    # Clear existing data
    clear_redis_pnl_data()
    
    # Generate ALL trades to the stream first
    print(f"\n📈 Generating 1000 trades to trades_stream...")
    for i in range(1000):
        if i % 100 == 0:
            print(f"  Generated {i} trades...")
        book_trade_to_stream()
        
        # Small delay to prevent overwhelming Redis
        time.sleep(0.005)
    
    print(f"✅ Generated {len(all_trades)} trades to stream")
    print()
    print("📋 NEXT STEPS:")
    print("3. NOW start your trade booker: python your_trade_booker_script.py")
    print("4. Press Enter when trade booker has finished processing all trades...")
    input()
    
    # Print summary
    print_trade_summary()
    
    # Compare results
    manual_pnl, redis_pnl = compare_pnl_results()
    
    # Show some position details
    print(f"\n📋 FINAL POSITIONS (sample):")
    print("-" * 40)
    for account, positions in list(account_positions.items())[:5]:
        print(f"{account}:")
        for ticker, qty in positions.items():
            if qty > 0:
                print(f"  {ticker}: {qty} shares")
    
    if len(account_positions) > 5:
        print(f"... and {len(account_positions) - 5} more accounts")

if __name__ == "__main__":
    main()