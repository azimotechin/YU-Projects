import datetime
import time
import redis
import yfinance as yf
import pandas as pd
from typing import List
import pytz

# --------------------
# Configuration
# --------------------
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
REFRESH_INTERVAL = 900  # 15 minutes in seconds
BATCH_SIZE = 500
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 30
MARKET_CLOSE_HOUR = 16
MARKET_CLOSE_MINUTE = 0
MARKET_TIMEZONE = 'America/New_York'


def get_nasdaq_tickers():
    """
    Fetches the list of Nasdaq-listed stock tickers.
    """
    nasdaq_url = "ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt"
    try:
        df = pd.read_csv(nasdaq_url, sep='|')
        df['Symbol'] = df['Symbol'].astype(str)
        tickers = df[~df['Symbol'].str.contains(r'\.|\$', na=False)]['Symbol'][:-1].tolist()
        print(f"Successfully fetched {len(tickers)} tickers from NASDAQ.")
        return tickers
    except Exception as e:
        print(f"Error fetching Nasdaq tickers: {e}")
        return []


def is_market_open():
    """
    Check if the market is currently open.
    Market hours: 9:30 AM - 4:00 PM ET, Monday-Friday
    """
    et_tz = pytz.timezone(MARKET_TIMEZONE)
    now = datetime.datetime.now(et_tz)
    
    # Check if it's a weekday (0 = Monday, 4 = Friday)
    if now.weekday() > 4:  # Saturday or Sunday
        return False
    
    # Check market hours
    market_open = now.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE, second=0, microsecond=0)
    market_close = now.replace(hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MINUTE, second=0, microsecond=0)
    
    return market_open <= now < market_close


def get_next_market_open():
    """
    Get the datetime of the next market open.
    """
    et_tz = pytz.timezone(MARKET_TIMEZONE)
    now = datetime.datetime.now(et_tz)
    
    # Start with tomorrow at market open time
    next_open = now.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE, second=0, microsecond=0)
    
    # If it's before market open today and it's a weekday, return today's open time
    if now.weekday() < 5 and now < next_open:
        return next_open
    
    # Otherwise, find the next weekday
    next_open += datetime.timedelta(days=1)
    while next_open.weekday() > 4:  # Skip to Monday if it's weekend
        next_open += datetime.timedelta(days=1)
    
    return next_open


def wait_for_market_open():
    """
    Wait until the market opens, showing countdown.
    """
    while not is_market_open():
        et_tz = pytz.timezone(MARKET_TIMEZONE)
        now = datetime.datetime.now(et_tz)
        next_open = get_next_market_open()
        time_until_open = next_open - now
        
        hours, remainder = divmod(time_until_open.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        print(f"\rMarket closed. Opens in {time_until_open.days}d {hours}h {minutes}m {seconds}s", end='', flush=True)
        time.sleep(60)  # Check every minute
    
    print("\n✓ Market is now open!")


def update_prices_batch(tickers: List[str], r: redis.Redis, batch_num: int, total_batches: int):
    """
    Update prices for a batch of tickers in Redis.
    """
    et_tz = pytz.timezone(MARKET_TIMEZONE)
    now = datetime.datetime.now(et_tz)
    today = now.strftime('%Y-%m-%d')
    
    # Download batch data
    try:
        ticker_string = ' '.join(tickers)
        data = yf.download(
            ticker_string,
            period="1d",
            threads=True,
            ignore_tz=True,
            progress=False,
            auto_adjust=True
        )
        
        if data.empty:
            print(f"No data returned for batch")
            return
            
        # Count successful updates
        updated_count = 0
        
        # Handle both single ticker and multi-ticker cases
        if len(tickers) == 1:
            # Single ticker - data['Close'] is a Series
            close_price = data['Close'].iloc[-1]
            update_redis(tickers[0], close_price, r, today)
            updated_count = 1
        else:
            # Multiple tickers - data['Close'] is a DataFrame
            close_prices = data['Close'].iloc[-1]
            for ticker in tickers:
                if ticker in close_prices.index and pd.notna(close_prices[ticker]):
                    update_redis(ticker, close_prices[ticker], r, today)
                    updated_count += 1
        
        # Progress indicator
        print(f"\rBatch {batch_num}/{total_batches}: Updated {updated_count}/{len(tickers)} tickers", end='', flush=True)
                    
    except Exception as e:
        print(f"Error downloading batch data: {e}")
        # Fall back to individual ticker fetching
        for ticker in tickers:
            try:
                info = yf.Ticker(ticker).fast_info
                price = info.get('lastPrice') or info.get('regularMarketPrice')
                if price:
                    update_redis(ticker, price, r, today)
            except Exception as e:
                print(f"Error fetching {ticker}: {e}")


def update_redis(ticker: str, price: float, r: redis.Redis, today: str):
    """
    Update live price in Redis. EOD updates are handled separately.
    """
    # Always update live price during market hours
    live_key = f"{ticker}:Live"
    r.set(live_key, f"{float(price):.2f}")


def create_eod_snapshots(tickers: List[str], r: redis.Redis):
    """
    Create EOD snapshots for all tickers at market close.
    """
    et_tz = pytz.timezone(MARKET_TIMEZONE)
    today = datetime.datetime.now(et_tz).strftime('%Y-%m-%d')
    
    print(f"\nCreating EOD snapshots for {today}...")
    created_count = 0
    
    for ticker in tickers:
        live_key = f"{ticker}:Live"
        eod_key = f"{ticker}:{today}"
        
        # Only create EOD if it doesn't exist and we have a live price
        if not r.exists(eod_key) and r.exists(live_key):
            price = r.get(live_key)
            r.set(eod_key, f"{float(price):.2f}")
            created_count += 1
    
    print(f"✓ Created {created_count} EOD snapshots for {today}")


def run_market_hours_updates(tickers: List[str], r: redis.Redis):
    """
    Run updates during market hours, then create EOD snapshots.
    """
    et_tz = pytz.timezone(MARKET_TIMEZONE)
    
    while is_market_open():
        start_time = time.time()
        total_batches = (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE
        
        current_time = datetime.datetime.now(et_tz).strftime('%I:%M %p ET')
        print(f"\n[{current_time}] Updating {len(tickers)} tickers in {total_batches} batches...")
        
        # Process tickers in batches
        for i in range(0, len(tickers), BATCH_SIZE):
            batch = tickers[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            update_prices_batch(batch, r, batch_num, total_batches)
            time.sleep(1)  # Be respectful to the API
        
        print()  # New line after progress updates
        
        elapsed = time.time() - start_time
        print(f"Update completed in {elapsed:.2f} seconds")
        
        # Check if market is still open before sleeping
        if is_market_open():
            # Calculate time until next update or market close
            now = datetime.datetime.now(et_tz)
            market_close = now.replace(hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MINUTE, second=0, microsecond=0)
            time_until_close = (market_close - now).total_seconds()
            
            sleep_time = min(max(0, REFRESH_INTERVAL - elapsed), time_until_close)
            
            if sleep_time > 0:
                print(f"Next update in {sleep_time/60:.1f} minutes...")
                time.sleep(sleep_time)
    
    # Market just closed - create EOD snapshots
    create_eod_snapshots(tickers, r)


def initial_price_fetch(tickers: List[str], r: redis.Redis):
    """
    Fetch current prices for all tickers - used for initial setup or after-hours startup.
    """
    print("Fetching current prices for all tickers...")
    total_batches = (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        print(f"\rFetching batch {batch_num}/{total_batches}...", end='', flush=True)
        update_prices_batch(batch, r, batch_num, total_batches)
        time.sleep(1)
    
    print("\n✓ Initial price fetch completed")


def check_and_create_missing_eods(tickers: List[str], r: redis.Redis):
    """
    Check if today's EODs exist (if market has closed today), create them if missing.
    This handles cases where the program starts after market close.
    """
    et_tz = pytz.timezone(MARKET_TIMEZONE)
    now = datetime.datetime.now(et_tz)
    today = now.strftime('%Y-%m-%d')
    
    # Only create EODs if it's a weekday and after market close
    if now.weekday() <= 4 and now.hour >= MARKET_CLOSE_HOUR:
        # Check if we already have EODs for today
        sample_ticker = "AAPL" #tickers[0] if tickers else None
        if sample_ticker and not r.exists(f"{sample_ticker}:{today}"):
            print(f"\nMarket closed but EODs missing for {today}.")
            
            # First check if we have any live prices
            if not r.exists(f"{sample_ticker}:Live"):
                print("No live prices found. Fetching current data...")
                initial_price_fetch(tickers, r)
            
            # Now create EODs
            create_eod_snapshots(tickers, r)


def main():
    # Connect to Redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    
    # Get NASDAQ tickers
    tickers = get_nasdaq_tickers()
    
    if not tickers:
        print("No tickers to process. Exiting.")
        return
    
    print(f"\n{'='*60}")
    print(f"Redis Stock Price Updater")
    print(f"Tracking {len(tickers)} NASDAQ tickers")
    print(f"Market Hours: 9:30 AM - 4:00 PM ET")
    print(f"Update Interval: {REFRESH_INTERVAL/60} minutes")
    print(f"{'='*60}\n")
    
    # Check if we need to create EODs for today (in case program started after market close)
    check_and_create_missing_eods(tickers, r)
    
    while True:
        # Wait for market to open
        wait_for_market_open()
        
        # Run updates during market hours
        run_market_hours_updates(tickers, r)
        
        # Market is now closed
        et_tz = pytz.timezone(MARKET_TIMEZONE)
        now = datetime.datetime.now(et_tz)
        print(f"\n[{now.strftime('%I:%M %p ET')}] Market closed. Updates paused until next trading day.")


if __name__ == '__main__':
    main()

"""
This should always be running
How this works:
The redis db could start off as empty or with existing tickers/eods from the day before
When the market opens at 9:30 am, the program updates the live key every 15 min for all tickers. 
Once the market closes, it adds all the eod prices for each day for each ticker. 
If program starts running when the market is closed, it first checks if eods are in, if they arent, they get added.
Once the eods are in and the market is closed, the program sleeps and waits for market to open to repeat the live updating.
"""