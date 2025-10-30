import redis
from redis.sentinel import Sentinel
import yfinance as yf
from datetime import datetime, timedelta
import re

# Standard periods that get updated daily by the main updater script
STANDARD_PERIODS = ['1d', '5d', '1mo', '3mo', '1y', '5y', 'ytd']

def get_redis_connection():
    # sentinels = [
    #     ("sentinel1", 26379),
    #     ("sentinel2", 26379),
    #     ("sentinel3", 26379)
    # ]
    # service_name = "mymaster"  # matches your sentinel config
    # sentinel = Sentinel(sentinels, socket_timeout=1, decode_responses=True)
    return redis.Redis(host = 'localhost', port = 6379, decode_responses=True)


def parse_period_to_days(period: str) -> int:
    """
    Convert period string to approximate trading days.
    Examples: '1d' -> 1, '5d' -> 5, '2mo' -> 44, '1y' -> 252
    """
    period = period.lower().strip()
    
    # Handle special cases
    if period == 'ytd':
        return None  # Special handling needed
    
    # Parse the period using regex
    match = re.match(r'^(\d+)(d|w|mo|y)$', period)
    if not match:
        raise ValueError(f"Invalid period format: {period}. Use format like '1d', '5d', '2mo', '1y'")
    
    number = int(match.group(1))
    unit = match.group(2)
    
    # Convert to approximate trading days
    if unit == 'd':
        return number
    elif unit == 'w':
        return number * 5  # 5 trading days per week
    elif unit == 'mo':
        return number * 22  # ~22 trading days per month
    elif unit == 'y':
        return number * 252  # ~252 trading days per year
    else:
        raise ValueError(f"Unsupported time unit: {unit}")


def get_historical_price(ticker: str, period: str) -> float:
    """
    Get the price of a ticker from a specific time period ago.
    
    For standard periods (1d, 5d, 1mo, 3mo, 1y, 5y, ytd):
    - Checks Redis first, then yfinance if not found, then caches in Redis
    
    For non-standard periods (3d, 4mo, 2y, etc.):
    - Fetches directly from yfinance without Redis caching
    
    :param ticker: Stock ticker symbol (e.g., 'AAPL', 'GOOGL')
    :param period: Time period (e.g., '1d', '5d', '3mo', '1y', 'ytd', '3d', '4mo')
    :return: Price from that period ago
    """
    ticker = ticker.upper()
    period = period.lower().strip()
    
    # Check if this is a standard period that gets updated daily
    is_standard_period = period in STANDARD_PERIODS
    
    if is_standard_period:
        # Standard period - check Redis first
        r = get_redis_connection()
        history_key = f"{ticker}:{period}"
        cached_price = r.get(history_key)
        
        if cached_price is not None:
            print(f"Fetched {ticker} {period} price from Redis")
            return float(cached_price)
        
        print(f"Redis miss for {ticker}:{period}, fetching from yfinance...")
    else:
        print(f"Non-standard period {period}, fetching directly from yfinance...")
    
    # Fetch from yfinance
    try:
        ticker_obj = yf.Ticker(ticker)
        
        if period == 'ytd':
            # Special handling for year-to-date
            current_year = datetime.now().year
            start_date = f"{current_year}-01-01"
            hist = ticker_obj.history(start=start_date)
            
            if hist.empty:
                raise ValueError(f"No YTD data available for ticker '{ticker}'")
            
            price = round(float(hist['Close'].iloc[0]), 2)
            
        else:
            # Convert period to trading days
            days_back = parse_period_to_days(period)
            
            # Fetch enough historical data (use 2+ years to ensure we have enough)
            hist = ticker_obj.history(period="2y")
            
            if hist.empty:
                raise ValueError(f"No historical data available for ticker '{ticker}'")
            
            # Get price from X trading days ago
            if len(hist) > days_back:
                price = round(float(hist['Close'].iloc[-(days_back + 1)]), 2)
            else:
                # Not enough data, use earliest available
                price = round(float(hist['Close'].iloc[0]), 2)
        
        # Cache in Redis ONLY if it's a standard period
        if is_standard_period:
            r = get_redis_connection()
            history_key = f"{ticker}:{period}"
            r.set(history_key, str(price))
            print(f"Cached {ticker}:{period} = {price} in Redis")
        
        return price
        
    except Exception as e:
        error_msg = str(e).lower()
        if ("possibly delisted" in error_msg or 
            "no data found" in error_msg or 
            "no price data found" in error_msg or
            "symbol may be delisted" in error_msg):
            raise ValueError(f"Ticker '{ticker}' does not exist")
        else:
            raise ValueError(f"Failed to fetch historical price for '{ticker}' {period}: {e}")


def get_price(ticker: str, shares: int) -> float:
    """
    Fetch the price of the given ticker from Redis. If not in Redis, fetch from yfinance.
    Returns the total price for the given number of shares.

    :param ticker: Stock ticker symbol (e.g., 'AAPL', 'GOOGL').
    :param shares: Number of shares to calculate the total price.
    :return: Total price for the given number of shares.
    """
    # Connect to Redis
    r = get_redis_connection()

    # Check Redis for live price
    live_key = f"{ticker.upper()}:Live"
    price = r.get(live_key)

    if price is None:
        # Fetch price from yfinance as a fallback
        try:
            ticker_data = yf.Ticker(ticker)

            # Validate if the ticker has valid historical data
            historical_data = ticker_data.history(period="1d")
            if historical_data.empty:
                raise ValueError(f"Ticker '{ticker}' does not exist or has no price data.")

            # Try multiple methods to get the current price
            price = None
            
            # Method 1: Try fast_info
            try:
                fast_info = ticker_data.fast_info
                price = fast_info.get('lastPrice') or fast_info.get('regularMarketPrice')
            except:
                pass
            
            # Method 2: Try info if fast_info fails
            if price is None:
                try:
                    info = ticker_data.info
                    price = info.get('currentPrice') or info.get('regularMarketPrice') or info.get('previousClose')
                except:
                    pass
            
            # Method 3: Use the latest close price from historical data
            if price is None and not historical_data.empty:
                price = historical_data['Close'].iloc[-1]

            # Validate and round the price
            if price is not None:
                price = round(float(price), 2)
                # Cache the price in Redis
                r.set(live_key, price)
                print("Fetched from yfinance")
            else:
                raise ValueError(f"Price data is unavailable for ticker '{ticker}'.")
                
        except Exception as e:
            error_msg = str(e).lower()
            # Check for Yahoo Finance specific errors
            if ("possibly delisted" in error_msg or 
                "no data found" in error_msg or 
                "no price data found" in error_msg or
                "symbol may be delisted" in error_msg):
                raise ValueError(f"Ticker '{ticker}' does not exist")
            else:
                raise ValueError(f"Failed to fetch price for ticker '{ticker}': {e}")
    else:
        print("Fetched from redis")

    # Convert price to float (Redis returns strings)
    price = float(price)

    # Calculate total price for the given number of shares
    return round(price * shares, 2)


def get_eod_price(ticker: str, date: str, shares: int) -> float:
    """
    Fetch the end-of-day price of the given ticker for a specific date from Redis. 
    If not in Redis, fetch from yfinance.
    Returns the total price for the given number of shares.

    :param ticker: Stock ticker symbol (e.g., 'AAPL', 'GOOGL').
    :param date: Date in YYYY-MM-DD format.
    :param shares: Number of shares to calculate the total price.
    :return: Total price for the given number of shares.
    """
    # Connect to Redis
    r = get_redis_connection()

    # Check Redis for EOD price
    eod_key = f"{ticker.upper()}:{date}"
    price = r.get(eod_key)

    if price is None:
        # Fetch EOD price from yfinance as a fallback
        try:
            ticker_data = yf.Ticker(ticker)

            # Get historical data for the specific date
            # Add one day buffer to ensure we get the date
            
            target_date = datetime.strptime(date, "%Y-%m-%d")
            start_date = target_date - timedelta(days=5)  # Look back a few days
            end_date = target_date + timedelta(days=1)    # Add one day to include target date
            
            historical_data = ticker_data.history(
                start=start_date.strftime("%Y-%m-%d"), 
                end=end_date.strftime("%Y-%m-%d")
            )
            
            if historical_data.empty:
                raise ValueError(f"Ticker '{ticker}' does not exist or has no price data.")

            # Filter for the specific date
            historical_data.index = historical_data.index.date  # Convert to date only
            target_date_obj = target_date.date()
            
            if target_date_obj not in historical_data.index:
                # Try to find the closest trading day before the target date
                available_dates = historical_data.index
                earlier_dates = [d for d in available_dates if d <= target_date_obj]
                
                if not earlier_dates:
                    raise ValueError(f"No trading data available for or before '{date}' for ticker '{ticker}'.")
                
                # Use the most recent trading day
                closest_date = max(earlier_dates)
                price = historical_data.loc[closest_date, 'Close']
            else:
                price = historical_data.loc[target_date_obj, 'Close']

            # Validate and round the price
            if price is not None:
                price = round(float(price), 2)
                # Cache the price in Redis
                r.set(eod_key, price)
            else:
                raise ValueError(f"EOD price data is unavailable for ticker '{ticker}' on '{date}'.")
                
        except Exception as e:
            error_msg = str(e).lower()
            # Check for Yahoo Finance specific errors
            if ("possibly delisted" in error_msg or 
                "no data found" in error_msg or 
                "no price data found" in error_msg or
                "symbol may be delisted" in error_msg):
                raise ValueError(f"Ticker '{ticker}' does not exist")
            else:
                raise ValueError(f"Failed to fetch EOD price for ticker '{ticker}' on '{date}': {e}")

    # Convert price to float (Redis returns strings)
    price = float(price)

    # Calculate total price for the given number of shares
    return price * shares

def get_eod_price_from_list(ticker: str, dates: list) -> dict:
    """
    Optimized version that batches yfinance API calls for missing Redis data
    """
    if not dates:
        return {
            "ticker": ticker.upper(),
            "start_date": None,
            "end_date": None,
            "daily_prices": {},
            "price_change": {
                "start_price": None,
                "end_price": None,
                "absolute_change": None,
                "percentage_change": None
            },
            "total_days": 0
        }
    
    # Connect to Redis
    r = get_redis_connection()

    daily_price = {}
    dates.sort()
    
    # Check which dates are already in Redis
    missing_dates = []
    for day in dates:
        eod_key = f"{ticker.upper()}:{day}"
        cached_price = r.get(eod_key)
        
        if cached_price is not None:
            daily_price[day] = float(cached_price)
        else:
            missing_dates.append(day)
    
    # If we have missing dates, fetch them all in one API call
    if missing_dates:
        try:
            ticker_data = yf.Ticker(ticker)
            
            # Find the date range that covers all missing dates
            min_date = datetime.strptime(min(missing_dates), "%Y-%m-%d")
            max_date = datetime.strptime(max(missing_dates), "%Y-%m-%d")
            
            # Add buffer days to ensure we capture all trading days
            buffer_start = min_date - timedelta(days=5)
            buffer_end = max_date + timedelta(days=1)
            
            print(f"Fetching data for {ticker} from {buffer_start.date()} to {buffer_end.date()} for {len(missing_dates)} missing dates...")
            
            historical_data = ticker_data.history(
                start=buffer_start.strftime("%Y-%m-%d"),
                end=buffer_end.strftime("%Y-%m-%d")
            )
            
            if historical_data.empty:
                raise ValueError(f"Ticker '{ticker}' does not exist or has no price data.")
            
            # Convert index to date objects for easier lookup
            historical_data.index = historical_data.index.date
            
            # Process all missing dates from the single API response
            cached_count = 0
            for date_str in missing_dates:
                try:
                    target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    
                    if target_date in historical_data.index:
                        price = round(float(historical_data.loc[target_date, 'Close']), 2)
                        daily_price[date_str] = price
                        # Cache in Redis
                        eod_key = f"{ticker.upper()}:{date_str}"
                        r.set(eod_key, price)
                        cached_count += 1
                    else:
                        # Find closest earlier trading day
                        available_dates = historical_data.index
                        earlier_dates = [d for d in available_dates if d <= target_date]
                        
                        if earlier_dates:
                            closest_date = max(earlier_dates)
                            price = round(float(historical_data.loc[closest_date, 'Close']), 2)
                            daily_price[date_str] = price
                            # Cache in Redis
                            eod_key = f"{ticker.upper()}:{date_str}"
                            r.set(eod_key, price)
                            cached_count += 1
                
                except ValueError:
                    # Skip dates that can't be processed
                    continue
            
            print(f"Successfully cached {cached_count} new prices in Redis")
                        
        except Exception as e:
            error_msg = str(e).lower()
            if ("possibly delisted" in error_msg or 
                "no data found" in error_msg or 
                "no price data found" in error_msg or
                "symbol may be delisted" in error_msg):
                raise ValueError(f"Ticker '{ticker}' does not exist")
            else:
                # Don't fail completely - just use what we have from Redis
                print(f"Warning: Failed to fetch some data for {ticker}: {e}")
    
    # Calculate price change information
    if len(daily_price) < 2:
        price_change_info = {
            "start_price": None,
            "end_price": None,
            "absolute_change": None,
            "percentage_change": None
        }
    else:
        # Get the first and last dates from the dictionary that has data
        sorted_dates = sorted(daily_price.keys())
        first_date = sorted_dates[0]
        last_date = sorted_dates[-1]
        
        # Get prices from daily_price dictionary
        start_price = daily_price[first_date]
        end_price = daily_price[last_date]
        
        absolute_change = round(end_price - start_price, 2)
        percentage_change = round(((end_price - start_price) / start_price) * 100, 2)
        
        price_change_info = {
            "start_price": start_price,
            "end_price": end_price,
            "absolute_change": absolute_change,
            "percentage_change": percentage_change
        }
    
    return {
        "ticker": ticker.upper(),
        "start_date": dates[0] if dates else None,
        "end_date": dates[-1] if dates else None,
        "daily_prices": daily_price,
        "price_change": price_change_info,
        "total_days": len(daily_price)
    }

def get_eod_price_range(ticker: str, start_date: str, end_date: str) -> dict:
    """ 
    Fetch end-of-day prices for a ticker over a date range from Redis/yfinance.
    Returns a dictionary with daily prices and price change information.

    :param ticker: Stock ticker symbol (e.g., 'AAPL', 'GOOGL').
    :param start_date: Start date in YYYY-MM-DD format.
    :param end_date: End date in YYYY-MM-DD format.
    :return: Dictionary containing daily prices and price change info.
    Optimized version that fetches all dates in one API call if missing from Redis
    """
    
    # Connect to Redis
    r = get_redis_connection()
    
    # Convert date strings to datetime objects
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Check which dates are already in Redis
    daily_prices = {}
    missing_dates = []
    
    current_date = start_dt
    while current_date <= end_dt:
        date_str = current_date.strftime("%Y-%m-%d")
        eod_key = f"{ticker.upper()}:{date_str}"
        cached_price = r.get(eod_key)
        
        if cached_price is not None:
            daily_prices[date_str] = float(cached_price)
        else:
            missing_dates.append(date_str)
        
        current_date += timedelta(days=1)
    
    # If we have missing dates, fetch ALL of them in ONE API call
    if missing_dates:
        try:
            ticker_data = yf.Ticker(ticker)
            
            # Fetch historical data for the ENTIRE range at once
            buffer_start = start_dt - timedelta(days=5)
            buffer_end = end_dt + timedelta(days=1)
            
            print(f"Fetching data for {ticker} from {buffer_start.date()} to {buffer_end.date()}...")
            historical_data = ticker_data.history(
                start=buffer_start.strftime("%Y-%m-%d"),
                end=buffer_end.strftime("%Y-%m-%d")
            )
            
            if historical_data.empty:
                raise ValueError(f"Ticker '{ticker}' does not exist or has no price data.")
            
            # Convert index to date objects for easier lookup
            historical_data.index = historical_data.index.date
            
            # Fill in ALL missing dates from the single API response
            for date_str in missing_dates:
                target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                
                if target_date in historical_data.index:
                    price = round(float(historical_data.loc[target_date, 'Close']), 2)
                    daily_prices[date_str] = price
                    # Cache in Redis for future use
                    eod_key = f"{ticker.upper()}:{date_str}"
                    r.set(eod_key, price)
                else:
                    # Find closest earlier trading day
                    available_dates = historical_data.index
                    earlier_dates = [d for d in available_dates if d <= target_date]
                    
                    if earlier_dates:
                        closest_date = max(earlier_dates)
                        price = round(float(historical_data.loc[closest_date, 'Close']), 2)
                        daily_prices[date_str] = price
                        # Cache in Redis
                        eod_key = f"{ticker.upper()}:{date_str}"
                        r.set(eod_key, price)
            
            print(f"Successfully cached {len(missing_dates)} new prices in Redis")
                        
        except Exception as e:
            error_msg = str(e).lower()
            if ("possibly delisted" in error_msg or 
                "no data found" in error_msg or 
                "no price data found" in error_msg or
                "symbol may be delisted" in error_msg):
                raise ValueError(f"Ticker '{ticker}' does not exist")
            else:
                raise ValueError(f"Failed to fetch price data for ticker '{ticker}': {e}")
    
    # Sort and calculate price changes
    sorted_dates = sorted(daily_prices.keys())
    sorted_daily_prices = {date: daily_prices[date] for date in sorted_dates}
    
    # Calculate price change information
    if len(sorted_daily_prices) < 2:
        price_change_info = {
            "start_price": None,
            "end_price": None,
            "absolute_change": None,
            "percentage_change": None
        }
    else:
        first_date = sorted_dates[0]
        last_date = sorted_dates[-1]
        start_price = daily_prices[first_date]
        end_price = daily_prices[last_date]
        
        absolute_change = round(end_price - start_price, 2)
        percentage_change = round(((end_price - start_price) / start_price) * 100, 2)
        
        price_change_info = {
            "start_price": start_price,
            "end_price": end_price,
            "absolute_change": absolute_change,
            "percentage_change": percentage_change
        }
    
    return {
        "ticker": ticker.upper(),
        "start_date": start_date,
        "end_date": end_date,
        "daily_prices": sorted_daily_prices,
        "price_change": price_change_info,
        "total_days": len(sorted_daily_prices)
    }

def main():
    # Test live prices
    try:
        print(f"AAPL (1 share): ${get_price('AAPL', 1)}")
    except ValueError as e:
        print(f"Error: {e}")
    
    try:
        print(f"TSLA (2 shares): ${get_price('TSLA', 2)}")
    except ValueError as e:
        print(f"Error: {e}")
    
    try:
        print(f"INVALID_TICKER (3 shares): ${get_price('INVALID_TICKER', 3)}")
    except ValueError as e:
        print(f"Error: {e}")
    
    print("\n--- EOD Prices ---")
    
    # Test EOD prices
    try:
        print(f"AAPL EOD 2024-01-15 (1 share): ${get_eod_price('AAPL', '2024-01-15', 1)}")
    except ValueError as e:
        print(f"Error: {e}")
    
    try:
        print(f"TSLA EOD 2024-01-15 (2 shares): ${get_eod_price('TSLA', '2024-01-15', 2)}")
    except ValueError as e:
        print(f"Error: {e}")
    
    try:
        print(f"INVALID_TICKER EOD 2024-01-15 (3 shares): ${get_eod_price('INVALID_TICKER', '2024-01-15', 3)}")
    except ValueError as e:
        print(f"Error: {e}")
    
    print("\n--- Historical Price Testing ---")
    
    # Test standard periods (should use Redis)
    try:
        print(f"AAPL 1d ago: ${get_historical_price('AAPL', '1d')}")
        print(f"AAPL 5d ago: ${get_historical_price('AAPL', '5d')}")
        print(f"AAPL 1mo ago: ${get_historical_price('AAPL', '1mo')}")
        print(f"AAPL YTD: ${get_historical_price('AAPL', 'ytd')}")
    except ValueError as e:
        print(f"Error: {e}")
    
    # Test non-standard periods (should skip Redis)
    try:
        print(f"AAPL 3d ago: ${get_historical_price('AAPL', '3d')}")
        print(f"AAPL 2w ago: ${get_historical_price('AAPL', '2w')}")
        print(f"AAPL 4mo ago: ${get_historical_price('AAPL', '4mo')}")
        print(f"AAPL 2y ago: ${get_historical_price('AAPL', '2y')}")
    except ValueError as e:
        print(f"Error: {e}")
    
    print("\n--- Price Range Data ---")
    
    # Test price range
    try:
        range_data = get_eod_price_range('AAPL', '2024-01-10', '2024-01-21')
        print(f"Ticker: {range_data['ticker']}")
        print(f"Date Range: {range_data['start_date']} to {range_data['end_date']}")
        print(f"Total Days: {range_data['total_days']}")
        print("\nDaily Prices:")
        for date, price in range_data['daily_prices'].items():
            print(f"  {date}: ${price}")
        
        change_info = range_data['price_change']
        print(f"\nPrice Change:")
        print(f"  Start Price: ${change_info['start_price']}")
        print(f"  End Price: ${change_info['end_price']}")
        print(f"  Absolute Change: ${change_info['absolute_change']}")
        print(f"  Percentage Change: {change_info['percentage_change']}%")
        
    except ValueError or TypeError as e:
        print(f"Error: {e}")
    
    try:
        dates = ["1950-02-02", "2023-03-27", "2000-03-30", "2013-08-12"]
        range_data = get_eod_price_from_list("AAPL", dates)
        print(range_data)
    except ValueError as e:
        print(f"Error ")


if __name__ == '__main__':
    main()

# get price
# it may be our job to add the ticker and price to a dictionary so it can be consolidated.
# check change in price since purchase.
# user can give in start/end time and see stock price change.
# don't require user to put in price.
# how many stocks can I get for $$$
# fault tolerance

#buy by qt
#buy by price