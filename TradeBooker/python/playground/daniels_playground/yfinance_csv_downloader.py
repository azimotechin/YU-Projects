import yfinance as yf
import pandas as pd
import datetime
import os
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import multiprocessing
import warnings
warnings.filterwarnings('ignore')

class UltraFast4000Downloader:
    def __init__(self):
        self.output_dir = "ticker_data"
        os.makedirs(self.output_dir, exist_ok=True)
        # Use all CPU cores
        self.num_processes = multiprocessing.cpu_count()
        self.num_threads = 50  # High thread count for I/O
        
    def get_all_tickers(self):
        """Get all tickers from NASDAQ list."""
        all_tickers = set()
        
        # Get NASDAQ tickers
        try:
            nasdaq_url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_tickers.txt"
            nasdaq_df = pd.read_csv(nasdaq_url, header=None, names=['ticker'])
            all_tickers.update(nasdaq_df['ticker'].tolist())
            print(f"✓ Found {len(nasdaq_df)} NASDAQ tickers")
        except:
            pass
        
        # Add from local file if exists
        if os.path.exists('all_tickers.txt'):
            with open('all_tickers.txt', 'r') as f:
                file_tickers = [line.strip().upper() for line in f if line.strip()]
                all_tickers.update(file_tickers)
        
        # Clean tickers
        all_tickers = {t for t in all_tickers if t and isinstance(t, str) and len(t) <= 5}
        return sorted(list(all_tickers))
    
    def download_chunk_ultra_fast(self, tickers_chunk):
        """Download a chunk with minimal overhead."""
        ticker_string = ' '.join(tickers_chunk)
        results = []
        
        try:
            # Single bulk download call
            df = yf.download(
                ticker_string,
                period='5d',
                interval='1d',
                group_by='ticker',
                auto_adjust=True,
                progress=False,
                threads=True,
                timeout=10
            )
            
            if df.empty:
                return results
            
            today = datetime.datetime.now()
            today_str = today.strftime('%Y%m%d')
            
            # Fast processing based on ticker count
            if len(tickers_chunk) == 1:
                ticker = tickers_chunk[0]
                if 'Close' in df.columns:
                    closes = df['Close'].dropna()
                    if not closes.empty:
                        # Latest price
                        results.append({
                            'ticker': ticker,
                            'date': today_str,
                            'price': closes.iloc[-1],
                            'is_live': True
                        })
                        # Historical
                        for idx, price in closes.items():
                            results.append({
                                'ticker': ticker,
                                'date': idx.strftime('%Y%m%d'),
                                'price': price,
                                'is_live': False
                            })
            else:
                # Multiple tickers - optimized processing
                for ticker in tickers_chunk:
                    try:
                        if ticker in df.columns.levels[0]:
                            ticker_data = df[ticker]['Close'].dropna()
                            if not ticker_data.empty:
                                # Latest price
                                results.append({
                                    'ticker': ticker,
                                    'date': today_str,
                                    'price': ticker_data.iloc[-1],
                                    'is_live': True
                                })
                                # Historical
                                for idx, price in ticker_data.items():
                                    results.append({
                                        'ticker': ticker,
                                        'date': idx.strftime('%Y%m%d'),
                                        'price': price,
                                        'is_live': False
                                    })
                    except:
                        continue
                        
        except Exception as e:
            # Silent fail - we'll retry failed tickers later
            pass
        
        return results
    
    def parallel_download_all(self, tickers, chunk_size=500):
        """Download all tickers using maximum parallelization."""
        print(f"\n🚀 ULTRA FAST MODE: {len(tickers)} tickers")
        print(f"   CPUs: {self.num_processes}, Threads per CPU: {self.num_threads}")
        print(f"   Chunk size: {chunk_size}")
        
        # Split into chunks
        chunks = [tickers[i:i + chunk_size] for i in range(0, len(tickers), chunk_size)]
        print(f"   Total chunks: {len(chunks)}")
        
        all_results = []
        failed_tickers = []
        start_time = time.time()
        
        # Process pool for CPU parallelization
        with ProcessPoolExecutor(max_workers=self.num_processes) as process_executor:
            # Submit all chunks
            future_to_chunk = {
                process_executor.submit(self.download_chunk_ultra_fast, chunk): chunk 
                for chunk in chunks
            }
            
            # Process results as they complete
            completed = 0
            for future in as_completed(future_to_chunk):
                chunk = future_to_chunk[future]
                try:
                    results = future.result(timeout=30)
                    all_results.extend(results)
                    
                    # Track which tickers succeeded
                    successful_tickers = set(r['ticker'] for r in results)
                    failed_in_chunk = set(chunk) - successful_tickers
                    failed_tickers.extend(failed_in_chunk)
                    
                except Exception as e:
                    # Add entire chunk to failed
                    failed_tickers.extend(chunk)
                
                completed += 1
                elapsed = time.time() - start_time
                rate = completed / elapsed
                eta = (len(chunks) - completed) / rate if rate > 0 else 0
                
                print(f"\r   Progress: {completed}/{len(chunks)} chunks | "
                      f"Speed: {rate:.1f} chunks/sec | "
                      f"ETA: {eta:.0f}s | "
                      f"Prices: {len(all_results):,}", end='')
        
        print(f"\n\n✓ Phase 1 complete: {len(all_results):,} prices in {time.time()-start_time:.1f}s")
        
        # Retry failed tickers in smaller batches
        if failed_tickers:
            print(f"\n🔄 Retrying {len(failed_tickers)} failed tickers...")
            retry_chunks = [failed_tickers[i:i + 50] for i in range(0, len(failed_tickers), 50)]
            
            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(self.download_chunk_ultra_fast, chunk) for chunk in retry_chunks]
                for future in as_completed(futures):
                    try:
                        results = future.result(timeout=10)
                        all_results.extend(results)
                    except:
                        pass
        
        return all_results
    
    def save_results(self, results):
        """Save results to CSV efficiently."""
        if not results:
            print("❌ No results to save")
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame(results)
        
        # Save current prices
        live_df = df[df['is_live']].copy()
        live_file = f"{self.output_dir}/live_prices_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        live_df[['ticker', 'price', 'date']].to_csv(live_file, index=False)
        
        # Save all data
        all_file = f"{self.output_dir}/all_prices_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df[['ticker', 'price', 'date']].to_csv(all_file, index=False)
        
        print(f"\n✓ Saved {len(live_df)} live prices to: {live_file}")
        print(f"✓ Saved {len(df)} total prices to: {all_file}")
        
        # Summary
        print(f"\n📊 Summary:")
        print(f"   Unique tickers: {df['ticker'].nunique():,}")
        print(f"   Live prices: {len(live_df):,}")
        print(f"   Historical prices: {len(df) - len(live_df):,}")
        print(f"   Date range: {df['date'].min()} to {df['date'].max()}")
        
        return all_file
    
    def run(self):
        """Main execution function."""
        print("=== ULTRA FAST 4000 TICKER DOWNLOADER ===")
        
        # Get tickers
        tickers = self.get_all_tickers()
        print(f"\n📋 Total tickers to download: {len(tickers):,}")
        
        # Download everything
        start_time = time.time()
        results = self.parallel_download_all(tickers, chunk_size=500)
        
        # Save results
        csv_file = self.save_results(results)
        
        total_time = time.time() - start_time
        print(f"\n⏱️  Total time: {total_time:.1f} seconds")
        print(f"📈 Speed: {len(tickers)/total_time:.1f} tickers/second")
        
        return csv_file


def main():
    downloader = UltraFast4000Downloader()
    csv_file = downloader.run()
    
    if csv_file:
        print(f"\n✅ SUCCESS! Now run: python csv_to_redis.py")
    else:
        print(f"\n❌ Download failed!")


if __name__ == "__main__":
    main()