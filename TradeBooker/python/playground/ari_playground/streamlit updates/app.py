import streamlit as st
import sys
import os
import datetime
import pandas as pd
from streamlit_autorefresh import st_autorefresh
import io
from redis.sentinel import Sentinel
import re
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)




# Add the "scripts" folder to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))


from TradeManager import TradeManager
from market_data import get_price, get_eod_price_range


def get_redis_connection():
       sentinel_hosts = [
           ("sentinel1", 26379),
           ("sentinel2", 26379),
           ("sentinel3", 26379)
       ]
       sentinel = Sentinel(sentinel_hosts, socket_timeout=0.5)
       redis_master = sentinel.master_for("mymaster", decode_responses=True)
       return redis_master


def fetch_trades(r, filters):
   trades = []
   for key in r.scan_iter(match="*", count=5000):
       try:
           account, trade_date, trade_id = key.split(":")[:3]
           trade_data = r.hgetall(key)
           trade_time = trade_data.get("trade_time", "")
           ticker = trade_data.get("ticker", "")
           price = trade_data.get("price", "")
           quantity = trade_data.get("quantity", "")
           trade_type = trade_data.get("type", "")
           trade_action = trade_data.get("action_type", "")
           if account in filters["accounts"] and str(filters["start_date"]) <= trade_date <= str(filters["end_date"]):
               trade_data["redis_key"] = key
               trade_data["account"] = account
               trade_data["trade_date"] = trade_date
               trade_data["trade_time"] = trade_time
               trade_data["ticker"] = ticker
               trade_data["price"] = price
               trade_data["quantity"] = quantity
               trade_data["type"] = trade_type
               trade_data["trade_action"] = trade_action
               trades.append(trade_data)
       except Exception:
           continue
   return trades
@st.fragment(run_every="5s")
def display_trade_table(r):
   trades = fetch_trades(r, st.session_state["filters"])
   if trades:
       df = pd.DataFrame(trades)
       display_cols = ['account', 'trade_date', 'trade_time', 'ticker', 'price', 'quantity', 'type', 'trade_action', 'redis_key', '']
       existing_cols = [col for col in display_cols if col in df.columns]
       st.dataframe(df[existing_cols])
   else:
       st.warning("No trades found for the selected criteria.")


def set_trade_filters_main_tab(r):
   # Get all accounts for options
   accounts = set()
   for key in r.scan_iter(match="*:*:*", count=5000):
       try:
           accounts.add(key.split(':')[0])
       except IndexError:
           continue
   accounts = sorted(list(accounts))


   # Filters in main tab
   selected_accounts = st.multiselect(
       "Select Accounts",
       options=accounts,
       default=accounts,
       key="account_multiselect_main"
   )


   start_date = st.date_input("Start Date", value=pd.to_datetime('1900-01-01'), key="start_date_main")
   end_date = st.date_input("End Date", value=pd.to_datetime('2200-12-31'), key="end_date_main")


   # Save filters to session state
   st.session_state["filters"] = {
       "accounts": selected_accounts if selected_accounts else accounts,
       "start_date": pd.to_datetime(start_date),
       "end_date": pd.to_datetime(end_date)
   }


def admin_tab(trade_manager):
   st.title("🛠️ Admin Panel")


   tab1, tab2, tab3 = st.tabs(["Database Status", "Generate Test Data", "Data Management"])


   with tab1:
       st.header("📊 Database Status")
       col1, col2, col3 = st.columns(3)
       with col1:
           try:
               total_trades = len(trade_manager.get_all_trades())
               st.metric("Total Trades", total_trades)
           except Exception as e:
               st.metric("Total Trades", "Error")
       with col2:
           if st.button("Refresh Status", key="refresh_status"):
               st.rerun()
       with col3:
           try:
               trade_manager.redis_client.ping()
               st.success("✅ Redis Connected")
           except:
               st.error("❌ Redis Disconnected")
       st.divider()
       st.subheader("Recent Trades Preview")
       try:
           all_trades = trade_manager.get_all_trades()
           if all_trades:
               recent_trades = all_trades[-10:]
               trade_data = []
               for trade in recent_trades:
                   trade_data.append({
                       "Account": trade.account_id,
                       "Date": trade.trade_date,
                       "Time": trade.trade_time,
                       "Ticker": trade.ticker,
                       "Price": f"${trade.price}",
                       "Type": trade.trade_type.upper(),
                       "Quantity": trade.quantity
                   })
               df = pd.DataFrame(trade_data)
               st.dataframe(df, use_container_width=True)
           else:
               st.info("No trades found in database")
       except Exception as e:
           st.error(f"Error retrieving trades: {e}")


   with tab2:
       st.header("🎲 Generate Test Data")
       col1, col2 = st.columns(2)
       with col1:
           num_trades = st.number_input("Number of random trades to generate:", min_value=1, max_value=10000, value=10, key="num_trades_input")
           if st.button("Generate Random Trades", key="generate_random"):
               try:
                   random_trades = TradeManager.create_random_trades(num_trades)
                   success = trade_manager.write_trades(random_trades)
                   if success:
                       st.success(f"✅ Successfully generated and saved {num_trades} random trades!")
                       st.subheader("Sample of Generated Trades:")
                       sample_data = []
                       for trade in random_trades[:5]:
                           sample_data.append({
                               "Account": trade.account_id,
                               "Date": trade.trade_date,
                               "Ticker": trade.ticker,
                               "Price": f"${trade.price}",
                               "Type": trade.trade_type.upper(),
                               "Quantity": trade.quantity
                           })
                       sample_df = pd.DataFrame(sample_data)
                       st.dataframe(sample_df)
                       if num_trades > 5:
                           st.info(f"Showing first 5 trades. Total generated: {num_trades}")
                   else:
                       st.error("❌ Failed to save generated trades")
               except Exception as e:
                   st.error(f"❌ Error generating trades: {e}")
       with col2:
           st.subheader("Quick Actions")
           if st.button("Generate 100 Trades", key="quick_100"):
               try:
                   random_trades = TradeManager.create_random_trades(100)
                   if trade_manager.write_trades(random_trades):
                       st.success("✅ Generated 100 random trades!")
                   else:
                       st.error("❌ Failed to generate trades")
               except Exception as e:
                   st.error(f"❌ Error: {e}")
           if st.button("Generate 1000 Trades", key="quick_1000"):
               try:
                   with st.spinner("Generating 1000 trades..."):
                       random_trades = TradeManager.create_random_trades(1000)
                       if trade_manager.write_trades(random_trades):
                           st.success("✅ Generated 1000 random trades!")
                       else:
                           st.error("❌ Failed to generate trades")
               except Exception as e:
                   st.error(f"❌ Error: {e}")


   with tab3:
       st.header("🗃️ Data Management")
       st.subheader("⚠️ Dangerous Operations")
       st.warning("These operations cannot be undone. Use with caution!")
       if st.session_state.get("confirm_clear", False):
           st.error("🚨 **DANGER ZONE** 🚨")
           st.markdown("### You are about to DELETE ALL TRADES from the database!")
           st.markdown("**This action is PERMANENT and cannot be undone.**")
           st.markdown("---")
           col1, col2, col3 = st.columns([1, 2, 1])
           with col2:
               if st.button("🗑️ YES, DELETE ALL TRADES", key="confirm_delete", type="primary"):
                   try:
                       if trade_manager.clear_all_trades():
                           st.success("✅ All trades have been permanently deleted!")
                           st.session_state["confirm_clear"] = False
                           st.rerun()
                       else:
                           st.error("❌ Failed to clear trades")
                   except Exception as e:
                       st.error(f"❌ Error clearing trades: {e}")
               if st.button("❌ Cancel - Keep My Data Safe", key="cancel_delete"):
                   st.session_state["confirm_clear"] = False
                   st.rerun()
           st.markdown("---")
       else:
           col1, col2 = st.columns(2)
           with col1:
               if st.button("�️ Clear All Trades", key="clear_all", type="secondary"):
                   st.session_state["confirm_clear"] = True
                   st.rerun()
           with col2:
               try:
                   all_trades = trade_manager.get_all_trades()
                   if all_trades:
                       trade_data = []
                       for trade in all_trades:
                           trade_data.append({
                               'account_id': trade.account_id,
                               'trade_date': trade.trade_date,
                               'trade_time': trade.trade_time,
                               'ticker': trade.ticker,
                               'price': trade.price,
                               'trade_type': trade.trade_type,
                               'quantity': trade.quantity,
                               'action_type': trade.action_type,
                               'trade_id': trade.trade_id
                           })
                       df = pd.DataFrame(trade_data)
                       csv_buffer = io.StringIO()
                       df.to_csv(csv_buffer, index=False)
                       st.download_button(
                           label="📥 Export All Trades",
                           data=csv_buffer.getvalue(),
                           file_name=f"all_trades_export_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                           mime="text/csv",
                           key="download_export"
                       )
                   else:
                       st.info("No trades available to export")
               except Exception as e:
                   st.error(f"Error preparing export: {e}")


   if "confirm_clear" not in st.session_state:
       st.session_state["confirm_clear"] = False


def user_input_tab(trade_manager):
   st.title("📝 Trade Input")


   tab1, tab2 = st.tabs(["Manual Trade Input", "CSV Upload"])


   with tab1:
       st.header("Enter Trade Manually")
       input_method = st.radio(
           "Choose input method:",
           ["Simple Format", "Individual Fields"],
           help="Simple format: user,ticker:$price:BUY/SELL:quantity (date is set automatically to today)"
       )


       if input_method == "Simple Format":
           trade_string = st.text_input(
               "Enter trade string:",
               placeholder="user123,AAPL:$150.50:BUY:100",
               help="Format: user,ticker:$price:BUY/SELL:quantity (date is set automatically to today)"
           )
           if st.button("Add Trade", key="simple_add"):
               if trade_string:
                   try:
                       today = datetime.now().strftime("%Y-%m-%d")
                       parts = trade_string.split(',', 1)
                       if len(parts) != 2:
                           st.error("❌ Invalid format. Expected: user,ticker:$price:BUY/SELL:quantity")
                       else:
                           formatted_trade_string = f"{parts[0]}:{today},{parts[1]}"
                           if TradeManager.verify_format(formatted_trade_string):
                               trade = TradeManager.convert_string_to_trade(formatted_trade_string)
                               if trade_manager.write_trade(trade):
                                   st.success(f"✅ Trade added successfully!")
                               else:
                                   st.error("❌ Failed to save trade to database")
                           else:
                               st.error("❌ Invalid trade format. Please check your input.")
                   except Exception as e:
                       st.error(f"❌ Error processing trade: {e}")
               else:
                   st.warning("⚠️ Please enter a trade string")
       else:
           st.subheader("Enter Trade Details")
           col1, col2 = st.columns(2)
           with col1:
               account_id = st.text_input("Account ID", placeholder="user123")
               ticker = st.text_input("Ticker Symbol", placeholder="AAPL").upper()
               price = st.number_input("Price ($)", min_value=0.01, value=100.0, step=0.01)
           with col2:
               trade_type = st.selectbox("Trade Type", ["BUY", "SELL"])
               quantity = st.number_input("Quantity", min_value=1, value=100, step=1)
           if st.button("Add Trade", key="fields_add"):
               if account_id and ticker:
                   try:
                       today = datetime.now().strftime("%Y-%m-%d")
                       trade_string = f"{account_id}:{today},{ticker}:${price}:{trade_type}:{quantity}"
                       trade = TradeManager.convert_string_to_trade(trade_string)
                       if trade_manager.write_trade(trade):
                           st.success(f"✅ Trade added successfully!")
                       else:
                           st.error("❌ Failed to save trade to database")
                   except Exception as e:
                       st.error(f"❌ Error processing trade: {e}")
               else:
                   st.warning("⚠️ Please fill in all required fields (Account ID and Ticker)")


   with tab2:
       st.header("Upload CSV File")
       sample_data = {
           'account_id': ['user1', 'user2', 'user3'],
           'trade_date': ['2025-06-25', '2025-06-25', '2025-06-25'],
           'ticker': ['AAPL', 'MSFT', 'GOOGL'],
           'price': [150.50, 280.75, 2500.00],
           'trade_type': ['buy', 'sell', 'buy'],
           'quantity': [100, 50, 25],
           'action_type': ['trade', 'trade', 'trade']
       }
       sample_df = pd.DataFrame(sample_data)
       csv_buffer = io.StringIO()
       sample_df.to_csv(csv_buffer, index=False)
       st.download_button(
           label="📥 Download Sample CSV",
           data=csv_buffer.getvalue(),
           file_name="sample_trades.csv",
           mime="text/csv",
           key="download_sample_direct"
       )
       uploaded_file = st.file_uploader(
           "Choose a CSV file",
           type="csv",
           help="Upload a CSV file containing trade data"
       )
       if uploaded_file is not None:
           try:
               df = pd.read_csv(uploaded_file)
               st.subheader("Preview of uploaded data:")
               st.dataframe(df.head(5))
               st.info(f"📊 Total rows in CSV: {len(df)}")
               if st.button("Process CSV", key="process_csv"):
                   try:
                       temp_filename = "temp_upload.csv"
                       df.to_csv(temp_filename, index=False)
                       trades = trade_manager.get_trades_from_csv(temp_filename)
                       os.remove(temp_filename)
                       if trades:
                           success = trade_manager.write_trades(trades)
                           if success:
                               st.success(f"✅ Successfully processed and saved {len(trades)} trades!")
                               st.subheader("Upload Summary:")
                               summary_df = pd.DataFrame([{
                                   "Account": trade.account_id,
                                   "Ticker": trade.ticker,
                                   "Price": f"${trade.price}",
                                   "Type": trade.trade_type.upper(),
                                   "Quantity": trade.quantity,
                                   "Date": trade.trade_date
                               } for trade in trades[:10]])
                               st.dataframe(summary_df)
                               if len(trades) > 10:
                                   st.info(f"Showing first 10 trades. Total uploaded: {len(trades)}")
                           else:
                               st.error("❌ Failed to save trades to database")
                       else:
                           st.warning("⚠️ No valid trades found in the CSV file")
                   except Exception as e:
                       st.error(f"❌ Error processing CSV: {e}")
           except Exception as e:
               st.error(f"❌ Error reading CSV file: {e}")


def stock_data_tab(r):
   st.title("📈 Stock Data Viewer")
   stock_tabs = st.tabs(["Live Prices", "Historical Data"])
   with stock_tabs[0]:
       st.subheader("Live Stock Prices")
       display_stock_table_live_with_market_data(r)
   with stock_tabs[1]:
       st.subheader("Historical Stock Data")
       ticker, start_date, end_date = get_historical_data_filter()
       show_data = st.button("Show Historical Data", key="show_hist_data_btn")
       if ticker and start_date and end_date and show_data:
           st.write(f"Showing historical trade data for {ticker} from {start_date} to {end_date}")
           display_historical_stock_chart(ticker, start_date, end_date)
       else:
           st.info("Select a ticker and date range, then click 'Show Historical Data'.")


def get_historical_data_filter():
   file_path = os.path.join(os.path.dirname(__file__), 'all_tickers.txt')
   tickers = load_all_tickers(file_path)
   ticker = st.selectbox("Select a stock ticker", options=tickers, key="ticker_select_hist")
   start_date = st.date_input("Start Date", value=pd.to_datetime('2020-01-01'), key="start_date_hist")
   end_date = st.date_input("End Date", value=pd.to_datetime('today'), key="end_date_hist")
   return ticker, start_date, end_date




def display_historical_stock_chart(ticker, start_date, end_date):
   try:
       data = get_eod_price_range(
           ticker,
           start_date.strftime("%Y-%m-%d"),
           end_date.strftime("%Y-%m-%d")
       )
       daily_prices = data["daily_prices"]
       if not daily_prices:
           st.warning(f"No historical data found for {ticker} from {start_date} to {end_date}.")
           return
       df = pd.DataFrame(list(daily_prices.items()), columns=["Date", "Price"])
       df["Date"] = pd.to_datetime(df["Date"])
       df = df.set_index("Date")
       st.line_chart(df["Price"], use_container_width=True)
       st.write(data["price_change"])
       st.dataframe(df, use_container_width=True)
   except Exception as e:
       st.error(f"Error fetching historical data for {ticker}: {e}")


def load_all_tickers(file_path):
   try:
       with open(file_path, 'r') as f:
           tickers = [line.strip() for line in f if line.strip() and re.match(r"^[A-Z]{1,5}$", line.strip())]
       return tickers
   except Exception as e:
       st.error(f"Error loading tickers from {file_path}: {e}")
       return []
  
#def is_likely_warrant_or_unit(ticker: str) -> bool:
#    return ticker.endswith(("W", "U", "R", "WS", "WT"))
#not needed, as I instead just commented out all warrants/untis


def fetch_live_stock_data_from_redis(r):
   file_path = os.path.join(os.path.dirname(__file__), 'all_tickers.txt')
   tickers = load_all_tickers(file_path)
   prices = []
   for ticker in tickers:
       price = r.get(f"{ticker}:Live")
       if price is not None:
           try:
               price_val = float(price)
           except Exception:
               price_val = price
           prices.append({"Ticker": ticker, "Price": price_val})
   return prices


def fetch_live_stock_data_from_yahoo():
   file_path = os.path.join(os.path.dirname(__file__), 'all_tickers_filtered.txt')
   tickers = load_all_tickers(file_path)
   prices = []


   for ticker in tickers:
       try:
           price = get_price(ticker, 1)
           if price is not None:
               prices.append({"Ticker": ticker, "Price": price})
       except Exception as e:
           logger.warning(f"Could not fetch live price for ticker '{ticker}': {e}")
           continue


   return prices


def fetch_eod_stock_data_from_yahoo():
   # Automatically use today's date in YYYY-MM-DD format
   date = datetime.date.today().strftime("%Y-%m-%d")


   file_path = os.path.join(os.path.dirname(__file__), 'all_tickers_filtered.txt')
   tickers = load_all_tickers(file_path)
   prices = []


   for ticker in tickers:
       try:
           price = get_eod_price(ticker, date, 1)
           if price is not None:
               prices.append({"Ticker": ticker, "Price": price})
       except Exception as e:
           logger.warning(f"Could not fetch EOD price for ticker '{ticker}' on {date}: {e}")
           continue


   return prices


@st.fragment(run_every="15m")
def display_stock_table_live_with_market_data(r):
   now = datetime.datetime.now().time()
   market_close_time = datetime.time(16, 0)  # 4:00 PM Eastern Time
   market_open_time = datetime.time(9, 30)   # 9:30 AM Eastern Time   
          
   data = fetch_live_stock_data_from_yahoo()
   #technically, fetch_live... which calls market_data's get_live can access eods, justs that it is inficient, as it tries accessing live price first
   #hence, check if current time is during market hours first, and if not call the dedicated eod price fetcher. problem is that there is a bug in that method right now.
  
  
   if not data:
       st.warning("No live stock data available in Redis at the moment.")
       return
   df = pd.DataFrame(data)
   st.dataframe(df, use_container_width=True) 


@st.fragment(run_every="5s")
def display_positions_data(r):
   positions = r.hgetall("positions")
   if not positions:
       st.warning("No positions data available.")
       return
   rows = []
   accounts = set()
   for key, value in positions.items():
       try:
           account, ticker = key.split(":")
       except ValueError:
           account, ticker = key, ""
       accounts.add(account)
       rows.append({"Account": account, "Ticker": ticker, "Position": int(value)})


   df = pd.DataFrame(rows)
   accounts = sorted(list(accounts))


   # Use session_state to store the confirmed account
   if "confirmed_account" not in st.session_state:
       st.session_state["confirmed_account"] = accounts[0] if accounts else ""


   selected_account = st.selectbox("Select Account", options=accounts, key="account_select_box")
   if st.button("Show Positions", key="show_positions_btn"):
       st.session_state["confirmed_account"] = selected_account


   # Always use the confirmed account for filtering
   confirmed_account = st.session_state["confirmed_account"]
   filtered_df = df[df["Account"] == confirmed_account]
   st.subheader(f"Positions for Account: {confirmed_account}")
   st.dataframe(filtered_df, use_container_width=True)
def main():
   st.set_page_config(page_title="Trade Viewer", layout="wide")
   r = get_redis_connection()
   trade_manager = TradeManager()
   tabs = st.tabs(["Trade Data", "Postitions Data", "Stock Data", "User Input", "Admin Page"])
   with tabs[0]:
       st.title("📈 Real-Time Trade Viewer")
       set_trade_filters_main_tab(r)
       st.write(f"#### Showing trades for {len(st.session_state.filters['accounts'])} account(s) from {st.session_state.filters['start_date']} to {st.session_state.filters['end_date']}")
       display_trade_table(r)
   with tabs[1]:
       st.title("📊 Positions Data Viewer")
       display_positions_data(r)
   with tabs[2]:
       stock_data_tab(r)
   with tabs[3]:
       user_input_tab(trade_manager)
   with tabs[4]:
       admin_tab(trade_manager)


if __name__ == "__main__":
   main()
