import pandas as pd
import yfinance as yf
import time


def get_nasdaq_tickers():
    """
    Fetches the list of Nasdaq-listed stock tickers.
    """
    nasdaq_url = "ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt"
    try:
        # Read the file into a DataFrame
        df = pd.read_csv(nasdaq_url, sep='|')

        # Ensure the 'Symbol' column is treated as strings and handle NaN values
        df['Symbol'] = df['Symbol'].astype(str)

        # Filter out rows where 'Symbol' matches unwanted patterns
        tickers = df[~df['Symbol'].str.contains(r'\.|\$', na=False)]['Symbol'][:-1].tolist()

        print(f"Successfully fetched {len(tickers)} tickers from NASDAQ.")
        return tickers
    except Exception as e:
        print(f"Error fetching Nasdaq tickers: {e}")
        return []


def download_closing_prices(tickers, period="1d", batch_size=500):
    """
    Downloads only the closing price for a list of tickers in batches and returns a single DataFrame.
    """
    all_close_prices = []
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        print(f"Downloading batch {i // batch_size + 1}/{(len(tickers) + batch_size - 1) // batch_size}...")
        try:
            # Download the data for the batch
            data = yf.download(
                batch,
                period=period,
                threads=True,
                ignore_tz=True
            )

            # *** KEY MODIFICATION IS HERE ***
            # Select only the 'Close' column. This will still have tickers as columns.
            close_prices = data['Close']

            if not close_prices.empty:
                # Reorganize (unstack) the data from wide to long format
                # This creates the Date, Ticker, Price structure
                close_prices = close_prices.stack().reset_index()
                close_prices.columns = ['Date', 'Ticker', 'Price']  # Rename columns for clarity
                all_close_prices.append(close_prices)

            time.sleep(1)  # Be respectful to the server

        except Exception as e:
            print(f"An error occurred while downloading batch starting at index {i}: {e}")

    if not all_close_prices:
        print("No data was downloaded.")
        return None

    # Concatenate all the downloaded batch dataframes into one
    full_df = pd.concat(all_close_prices, ignore_index=True)
    return full_df


if __name__ == "__main__":
    # 1. Get the list of tickers
    nasdaq_tickers = get_nasdaq_tickers()

    if nasdaq_tickers:
        # 2. Download only the closing prices
        # For a quick test, you can use a smaller slice: nasdaq_tickers[:100]
        price_data = download_closing_prices(nasdaq_tickers, period="1d")

        if price_data is not None:
            # 3. Save the Ticker, Date, and Price data to a single CSV file
            try:
                print("Saving Ticker and Price data to CSV...")
                price_data.to_csv('nasdaq_ticker_and_price_1d.csv', index=False)
                print("Data successfully saved to nasdaq_ticker_and_price_1d.csv")
                print("\nSample of the final data:")
                print(price_data.head())
                print("\n...")
                print(price_data.tail())
            except Exception as e:
                print(f"Error saving data to CSV: {e}")