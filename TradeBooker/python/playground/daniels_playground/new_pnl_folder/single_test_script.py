import redis
import random
import time

# --- Direct localhost Redis connection ---
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# --- Configuration ---
ACCOUNT = "alice"
TICKER = "AAPL"
POSITION_KEY = f"{ACCOUNT}/{TICKER}"

# Manual tracking for verification
manual_lots = []  # List of {price: float, quantity: int}
manual_realized_pnl = 0.0
all_trades = []

def clear_redis_data():
    """Clear all Redis data for fresh start"""
    print("🧹 Clearing Redis data...")
    
    # Clear PnL hashes
    r.delete("realized_pnl_by_position")
    r.delete("unrealized_pnl_by_position")
    
    # Clear lots for our position
    r.delete(f"lots:{POSITION_KEY}")
    
    # Clear trades stream
    r.delete("trades_stream")
    
    # Clear any existing trade hashes for our account
    for key in r.scan_iter(match=f"{ACCOUNT}:????-??-??:*"):
        r.delete(key)
    
    print("✅ Redis cleared")

def process_buy_manual(price, quantity):
    """Process a buy trade in our manual tracking"""
    global manual_lots
    
    manual_lots.append({
        'price': price,
        'quantity': quantity
    })
    
    print(f"  📈 Manual: Added lot of {quantity} @ ${price:.2f} (Total lots: {len(manual_lots)})")

def process_sell_manual(price, quantity):
    """Process a sell trade using FIFO in our manual tracking"""
    global manual_lots, manual_realized_pnl
    
    if not manual_lots:
        print(f"  ⚠️  Manual: No lots to sell!")
        return
    
    remaining_to_sell = quantity
    total_pnl_from_sell = 0.0
    lots_to_remove = []
    
    print(f"  📉 Manual: Selling {quantity} shares @ ${price:.2f}")
    
    for i, lot in enumerate(manual_lots):
        if remaining_to_sell <= 0:
            break
            
        if lot['quantity'] <= remaining_to_sell:
            # Consume entire lot
            pnl = (price - lot['price']) * lot['quantity']
            total_pnl_from_sell += pnl
            remaining_to_sell -= lot['quantity']
            lots_to_remove.append(i)
            
            print(f"    🔥 Consumed full lot: {lot['quantity']} @ ${lot['price']:.2f} → PnL: ${pnl:.2f}")
        else:
            # Partial consumption
            pnl = (price - lot['price']) * remaining_to_sell
            total_pnl_from_sell += pnl
            lot['quantity'] -= remaining_to_sell
            
            print(f"    ✂️  Partial lot: {remaining_to_sell} from {lot['quantity'] + remaining_to_sell} @ ${lot['price']:.2f} → PnL: ${pnl:.2f}")
            print(f"    📦 Remaining in lot: {lot['quantity']} shares")
            remaining_to_sell = 0
    
    # Remove fully consumed lots
    manual_lots = [lot for i, lot in enumerate(manual_lots) if i not in lots_to_remove]
    
    # Update total realized PnL
    manual_realized_pnl += total_pnl_from_sell
    
    print(f"    💰 PnL from this sell: ${total_pnl_from_sell:.2f}")
    print(f"    💰 Total realized PnL: ${manual_realized_pnl:.2f}")
    print(f"    📦 Remaining lots: {len(manual_lots)}")

def generate_trade():
    """Generate a single trade for our position"""
    # Get current position (total shares we own)
    total_shares = sum(lot['quantity'] for lot in manual_lots)
    
    # 40% chance to sell if we have shares, otherwise buy
    if total_shares > 0 and random.random() < 0.4:
        trade_type = "sell"
        # Don't sell more than we own
        quantity = random.randint(1, min(total_shares, 20))
    else:
        trade_type = "buy"
        quantity = random.randint(1, 25)
    
    # Generate realistic price with some volatility
    base_price = 180.0  # AAPL base price
    price = round(base_price * random.uniform(0.9, 1.1), 2)
    
    return {
        'account': ACCOUNT,
        'ticker': TICKER,
        'price': price,
        'type': trade_type,
        'quantity': quantity
    }

def book_trade_to_stream(trade):
    """Add trade to Redis stream"""
    ACTION_TYPE = "trade"
    trade_string = f"{trade['account']},{trade['ticker']}:{trade['price']}:{trade['type']}:{trade['quantity']}:{ACTION_TYPE}"
    
    # Add to stream
    r.xadd("trades_stream", {
        "trade_string": trade_string
    })
    
    return trade_string

def get_redis_realized_pnl():
    """Get the realized PnL from Redis for our position"""
    redis_pnl = r.hget("realized_pnl_by_position", POSITION_KEY)
    return float(redis_pnl) if redis_pnl else 0.0

def get_redis_lots():
    """Get the lots from Redis for our position"""
    lots_json = r.get(f"lots:{POSITION_KEY}")
    if lots_json:
        import json
        return json.loads(lots_json)
    return []

def print_detailed_comparison():
    """Print detailed comparison of manual vs Redis calculations"""
    print("\n" + "="*70)
    print("🔍 DETAILED COMPARISON")
    print("="*70)
    
    # Get Redis values
    redis_pnl = get_redis_realized_pnl()
    redis_lots = get_redis_lots()
    
    # Compare realized PnL
    manual_pnl_rounded = round(manual_realized_pnl, 2)
    redis_pnl_rounded = round(redis_pnl, 2)
    
    pnl_match = manual_pnl_rounded == redis_pnl_rounded
    pnl_status = "✅" if pnl_match else "❌"
    
    print(f"{pnl_status} REALIZED PnL:")
    print(f"   Manual: ${manual_pnl_rounded:8.2f}")
    print(f"   Redis:  ${redis_pnl_rounded:8.2f}")
    
    if not pnl_match:
        diff = redis_pnl_rounded - manual_pnl_rounded
        print(f"   Diff:   ${diff:8.2f}")
    
    # Compare lots
    print(f"\n📦 REMAINING LOTS:")
    print(f"   Manual lots: {len(manual_lots)}")
    print(f"   Redis lots:  {len(redis_lots)}")
    
    # Show manual lots
    if manual_lots:
        print(f"   Manual lot details:")
        for i, lot in enumerate(manual_lots):
            print(f"     {i+1}: {lot['quantity']} @ ${lot['price']:.2f}")
    
    # Show Redis lots
    if redis_lots:
        print(f"   Redis lot details:")
        for i, lot in enumerate(redis_lots):
            print(f"     {i+1}: {lot['quantity']} @ ${lot['price']:.2f}")
    
    # Calculate position value
    manual_shares = sum(lot['quantity'] for lot in manual_lots)
    redis_shares = sum(lot['quantity'] for lot in redis_lots)
    
    print(f"\n📊 POSITION SUMMARY:")
    print(f"   Manual shares: {manual_shares}")
    print(f"   Redis shares:  {redis_shares}")
    
    print("="*70)
    
    if pnl_match:
        print("🎉 REALIZED PnL MATCHES! Your calculator is working correctly.")
    else:
        print("⚠️  REALIZED PnL MISMATCH! Check your calculator logic.")
    
    return pnl_match

def main():
    """Main function"""
    print("🚀 Single Position PnL Test")
    print(f"Testing position: {POSITION_KEY}")
    print("="*50)
    
    # Setup
    print("📋 SETUP:")
    print("1. Start your PnL calculator: python notifications_pnl.py")
    print("2. Press Enter to continue...")
    input()
    
    clear_redis_data()
    
    # Generate and process trades
    print(f"\n📈 Generating 1000 trades for {POSITION_KEY}...")
    
    for i in range(1000):
        # Generate trade
        trade = generate_trade()
        all_trades.append(trade)
        
        # Process manually
        if trade['type'] == 'buy':
            process_buy_manual(trade['price'], trade['quantity'])
        else:
            process_sell_manual(trade['price'], trade['quantity'])
        
        # Add to stream
        trade_string = book_trade_to_stream(trade)
        
        if (i + 1) % 100 == 0:
            print(f"\n📊 Progress: {i + 1}/1000 trades")
            print(f"   Current manual realized PnL: ${manual_realized_pnl:.2f}")
            print(f"   Current manual lots: {len(manual_lots)}")
        
        # Small delay
        time.sleep(0.005)
    
    print(f"\n✅ Generated {len(all_trades)} trades")
    
    # Summary
    buy_trades = [t for t in all_trades if t['type'] == 'buy']
    sell_trades = [t for t in all_trades if t['type'] == 'sell']
    
    print(f"\n📊 TRADE SUMMARY:")
    print(f"   Total trades: {len(all_trades)}")
    print(f"   Buy trades:   {len(buy_trades)}")
    print(f"   Sell trades:  {len(sell_trades)}")
    print(f"   Final manual realized PnL: ${manual_realized_pnl:.2f}")
    print(f"   Final manual lots: {len(manual_lots)}")
    
    print("\n📋 NEXT STEPS:")
    print("3. Start your trade booker: python your_trade_booker_script.py")
    print("4. Press Enter when trade booker finishes processing...")
    input()
    
    # Compare results
    pnl_match = print_detailed_comparison()
    
    return pnl_match

if __name__ == "__main__":
    main()