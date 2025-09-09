import datetime
import time
import redis
from redis.sentinel import Sentinel
import yfinance as yf
import pandas as pd
from typing import List
import pytz

# --------------------
# Configuration
# --------------------
REFRESH_INTERVAL = 900  # 15 minutes in seconds
BATCH_SIZE = 500
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 30
MARKET_CLOSE_HOUR = 16
MARKET_CLOSE_MINUTE = 0
MARKET_TIMEZONE = 'America/New_York'
HISTORY_PERIODS = ['1d', '5d', '1mo', '3mo', '1y', '5y', 'ytd']


def get_redis_connection():
    sentinels = [
        ("sentinel1", 26379),
        ("sentinel2", 26379),
        ("sentinel3", 26379)
    ]
    service_name = "mymaster"  # match your sentinel config
    sentinel = Sentinel(sentinels, socket_timeout=1, decode_responses=True)
    return sentinel.master_for(service_name, socket_timeout=1, decode_responses=True)


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


def get_batch_ticker_histories(tickers: List[str]):
    """
    Fetch historical price data for multiple tickers in one API call.
    Returns a dictionary: {ticker: {period: price}}
    """
    try:
        # Download 2 years of data for all tickers at once
        ticker_string = ' '.join(tickers)
        hist_data = yf.download(
            ticker_string,
            period="2y",
            threads=True,
            ignore_tz=True,
            progress=False,
            auto_adjust=True
        )
        
        if hist_data.empty:
            return {ticker: {period: None for period in HISTORY_PERIODS} for ticker in tickers}
        
        # Define the mapping of periods to approximate trading days back
        period_to_days = {
            '1d': 1,
            '5d': 5,
            '1mo': 22,
            '3mo': 66,
            '1y': 252,
            '5y': 1260,
            'ytd': None
        }
        
        results = {}
        
        # Handle single vs multiple ticker cases
        if len(tickers) == 1:
            # Single ticker - hist_data['Close'] is a Series
            ticker = tickers[0]
            close_prices = hist_data['Close']
            results[ticker] = extract_historical_prices(close_prices, period_to_days)
        else:
            # Multiple tickers - hist_data['Close'] is a DataFrame
            close_data = hist_data['Close']
            
            for ticker in tickers:
                if ticker in close_data.columns:
                    close_prices = close_data[ticker].dropna()
                    results[ticker] = extract_historical_prices(close_prices, period_to_days)
                else:
                    results[ticker] = {period: None for period in HISTORY_PERIODS}
        
        return results
        
    except Exception as e:
        print(f"Error fetching batch histories: {e}")
        return {ticker: {period: None for period in HISTORY_PERIODS} for ticker in tickers}


def extract_historical_prices(close_prices, period_to_days):
    """
    Extract historical prices for all periods from a price series.
    """
    history_data = {}
    
    for period in HISTORY_PERIODS:
        try:
            if period == 'ytd':
                # For YTD, get the first price of the current year
                et_tz = pytz.timezone(MARKET_TIMEZONE)
                current_year = datetime.datetime.now(et_tz).year
                
                # Filter to current year and get first price
                year_data = close_prices[close_prices.index.year == current_year]
                if not year_data.empty:
                    price = round(float(year_data.iloc[0]), 2)
                else:
                    price = None
            else:
                # Get price from X trading days ago
                days_back = period_to_days[period]
                if len(close_prices) > days_back:
                    price = round(float(close_prices.iloc[-(days_back + 1)]), 2)
                else:
                    # If not enough data, use earliest available
                    price = round(float(close_prices.iloc[0]), 2) if len(close_prices) > 0 else None
            
            history_data[period] = price
            
        except Exception as e:
            print(f"Error extracting {period} price: {e}")
            history_data[period] = None
    
    return history_data


def update_all_ticker_histories(tickers: List[str], r: redis.Redis):
    """
    Update historical data for all tickers using batch processing for speed.
    """
    print(f"\nUpdating price history for {len(tickers)} tickers...")
    
    # Use smaller batch size for history (API intensive)
    batch_size = 250
    total_batches = (len(tickers) + batch_size - 1) // batch_size
    total_success = 0
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(tickers))
        batch = tickers[start_idx:end_idx]
        
        print(f"\rProcessing batch {batch_num + 1}/{total_batches}: {len(batch)} tickers", end='', flush=True)
        
        try:
            # Get histories for entire batch in one API call
            batch_histories = get_batch_ticker_histories(batch)
            
            # Update Redis for all tickers in batch
            batch_success = 0
            for ticker, history_data in batch_histories.items():
                try:
                    # Store each period as a separate Redis key
                    for period, price in history_data.items():
                        if price is not None:
                            history_key = f"{ticker}:{period}"
                            r.set(history_key, str(price))
                    
                    batch_success += 1
                    total_success += 1
                    
                except Exception as e:
                    print(f"\nError updating Redis for {ticker}: {e}")
            
            print(f" -> {batch_success}/{len(batch)} updated", end='', flush=True)
            
        except Exception as e:
            print(f"\nError processing batch {batch_num + 1}: {e}")
        
        # Small pause between batches
        time.sleep(2)  # Slightly longer pause since we're getting more data
    
    print(f"\n✓ History update completed: {total_success}/{len(tickers)} tickers updated successfully")


def check_and_create_missing_histories(tickers: List[str], r: redis.Redis):
    """
    Check if price histories exist for tickers, create them if missing using batched approach.
    This handles cases where the program starts and histories don't exist.
    """
    print("Checking for missing price histories...")
    missing_tickers = []
    
    # Check which tickers are missing history data (check for 1d as indicator)
    for ticker in tickers:
        history_key = f"{ticker}:1d"  # Use 1d as indicator for history existence
        if not r.exists(history_key):
            missing_tickers.append(ticker)
    
    if missing_tickers:
        print(f"Found {len(missing_tickers)} tickers without history data. Creating...")
        
        # Use the same batched approach as update_all_ticker_histories
        batch_size = 250
        total_batches = (len(missing_tickers) + batch_size - 1) // batch_size
        total_success = 0
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(missing_tickers))
            batch = missing_tickers[start_idx:end_idx]
            
            print(f"\rCreating histories batch {batch_num + 1}/{total_batches}: {len(batch)} tickers", end='', flush=True)
            
            try:
                # Get histories for entire batch in one API call
                batch_histories = get_batch_ticker_histories(batch)
                
                # Update Redis for all tickers in batch
                batch_success = 0
                for ticker, history_data in batch_histories.items():
                    try:
                        # Store each period as a separate Redis key
                        for period, price in history_data.items():
                            if price is not None:
                                history_key = f"{ticker}:{period}"
                                r.set(history_key, str(price))
                        
                        batch_success += 1
                        total_success += 1
                        
                    except Exception as e:
                        print(f"\nError updating Redis for {ticker}: {e}")
                
                print(f" -> {batch_success}/{len(batch)} created", end='', flush=True)
                
            except Exception as e:
                print(f"\nError processing batch {batch_num + 1}: {e}")
            
            # Small delay between batches
            time.sleep(2)
        
        print(f"\n✓ Created {total_success}/{len(missing_tickers)} missing price histories")
    else:
        print("✓ All tickers already have price history data")

def should_update_histories(r: redis.Redis):
    """
    Check if we should update histories today.
    Returns True if it's a new trading day and we haven't updated histories yet.
    """
    et_tz = pytz.timezone(MARKET_TIMEZONE)
    today = datetime.datetime.now(et_tz).strftime('%Y-%m-%d')
    
    # Use a marker key to track if we've updated histories today
    history_update_key = f"history_updated:{today}"
    return not r.exists(history_update_key)


def mark_histories_updated(r: redis.Redis):
    """
    Mark that we've updated histories for today.
    """
    et_tz = pytz.timezone(MARKET_TIMEZONE)
    today = datetime.datetime.now(et_tz).strftime('%Y-%m-%d')
    
    history_update_key = f"history_updated:{today}"
    # Set with expiration of 2 days to clean up old markers
    r.setex(history_update_key, 172800, "1")


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
    r = get_redis_connection()

    # Get NASDAQ tickers
    tickers = get_nasdaq_tickers()

    if not tickers:
        print("No tickers to process. Exiting.")
        return

    print(f"\n{'='*60}")
    print(f"Redis Stock Price Updater with History")
    print(f"Tracking {len(tickers)} NASDAQ tickers")
    print(f"Market Hours: 9:30 AM - 4:00 PM ET")
    print(f"Update Interval: {REFRESH_INTERVAL/60} minutes")
    print(f"History Periods: {', '.join(HISTORY_PERIODS)}")
    print(f"{'='*60}\n")

    # Check and create missing price histories (for program startup)
    check_and_create_missing_histories(tickers, r)

    # Check if we need to create EODs for today (in case program started after market close)
    check_and_create_missing_eods(tickers, r)
    
    while True:
        # Check if we should update histories (once per day at the beginning)
        if should_update_histories(r):
            et_tz = pytz.timezone(MARKET_TIMEZONE)
            now = datetime.datetime.now(et_tz)
            
            # Update histories if it's a trading day (weekday)
            if now.weekday() <= 4:
                print(f"\n[{now.strftime('%I:%M %p ET')}] Starting daily history update...")
                update_all_ticker_histories(tickers, r)
                mark_histories_updated(r)
                print("✓ Daily history update completed")
        
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

NEW HISTORY FUNCTIONALITY:
- Each ticker now has individual history keys: <Ticker>:1d, <Ticker>:5d, <Ticker>:1mo, <Ticker>:3mo, <Ticker>:1y, <Ticker>:5y, <Ticker>:ytd
- Each key contains a simple float/string of the price from that period ago (trading days, not calendar days)
- For example: AAPL:5d = "150.25" (price from 5 trading days ago)
- YTD contains the price from the first trading day of the current year
- History updates once per day at the beginning of each trading day (before market open)
- Uses batch processing (100 tickers per batch) with reduced delays for fast daily updates
- All tickers are updated daily with optimized performance (~5-8 minutes for 3000 tickers)
- If program starts and history doesn't exist, it creates missing histories for all tickers
- Uses a daily marker key to prevent duplicate history updates on the same day
"""