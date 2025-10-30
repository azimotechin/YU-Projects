import datetime
import time
import os
import pandas as pd
import streamlit as st
import redis
from redis.sentinel import Sentinel
from redis.exceptions import BusyLoadingError, ConnectionError
import yfinance as yf
import plotly.graph_objects as go
from zoneinfo import ZoneInfo # Use the modern, built-in library
import io
import logging
import re
import sys
from multiprocessing import Process

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.send_trades_to_stream import book_trades_in_batches, book_custom_trade_to_stream
from scripts.TradeManager import TradeManager
from scripts.UserManager import UserManager
from scripts.market_data import get_historical_price, get_price, get_eod_price_range
from scripts.pnl_getters import PnLRetriever

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

import os

# Generate .streamlit/secrets.toml from env vars
secrets_dir = "/app/.streamlit"
os.makedirs(secrets_dir, exist_ok=True)

secrets_content = f"""
[auth]
redirect_uri = "{os.environ.get('REDIRECT_URI')}"
cookie_secret = "{os.environ.get('COOKIE_SECRET')}"
client_id = "{os.environ.get('CLIENT_ID')}"
client_secret = "{os.environ.get('CLIENT_SECRET')}"
server_metadata_url = "{os.environ.get('SERVER_METADATA_URL')}"
"""

with open(os.path.join(secrets_dir, "secrets.toml"), "w") as f:
    f.write(secrets_content)



# --- Timezone Configuration ---
EST = ZoneInfo("America/New_York")

def get_current_est_time():
    return datetime.datetime.now(EST)

def get_current_est_date():
    return datetime.datetime.now(EST).date()

def get_redis_connection():
       sentinel_hosts = [
           ("sentinel1", 26379),
           ("sentinel2", 26379),
           ("sentinel3", 26379)
       ]
       sentinel = Sentinel(sentinel_hosts, socket_timeout=5)
       redis_master = sentinel.master_for("mymaster", decode_responses=True, socket_timeout=10)
       return redis_master

def ensure_stream_and_group_exist(redis_client, stream_key="trades_stream", group_name="booker-group"):
    # If stream doesn't exist, create an empty one
    if not redis_client.exists(stream_key):
        redis_client.xadd(stream_key, {"init": "true"})
        redis_client.xtrim(stream_key, maxlen=0)

    # Try creating the group — suppress error if it already exists
    try:
        redis_client.xgroup_create(stream_key, group_name, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
ensure_stream_and_group_exist(get_redis_connection())


def fetch_trades(_r, accounts: tuple, start_date: datetime.date, end_date: datetime.date, _status_container=None, start_time=None):
    all_trades = []
    if _status_container:
        _status_container.update(label=f"Fetching data...")

    pipe = _r.pipeline()
    trades_in_pipe = 0

    for account in accounts:
        for key in _r.scan_iter(match=f"{account}*:*:*", count=1000):
            date = key.split(':')[1]
            if date <= end_date.strftime("%Y-%m-%d") and date >= start_date.strftime("%Y-%m-%d"):
                pipe.hgetall(key)
                trades_in_pipe += 1
                if trades_in_pipe >= 1000:
                    results = pipe.execute()
                    trades_in_pipe = 0
                    all_trades+=results
                    if _status_container:
                        _status_container.update(label=f"Processed {len(all_trades)} trades...", state="running")

    if trades_in_pipe > 0:
        results = pipe.execute()
        all_trades+=results

    if _status_container:
        _status_container.update(label=f"Fetched {len(all_trades)} trades in {time.perf_counter() - start_time:.2f}s", state="complete")

    if all_trades:
        all_trades.sort(key=lambda x: (x.get('trade_date', ''), x.get('trade_time', '')), reverse=True)

    return all_trades

def set_trade_filters_main_tab(r):
   with st.spinner("Loading account filters..."):
       all_accounts = get_all_accounts(r)

   def update_filters_callback():
       st.session_state["filters"] = {
           "accounts": st.session_state.get("account_multiselect_main", []),
           "start_date": st.session_state.get("start_date_main"),
           "end_date": st.session_state.get("end_date_main"
           )
       }

   # Get the previously selected accounts from the session state.
   saved_defaults = st.session_state.get("filters", {}).get("accounts", [])
   # Filter the saved defaults to only include accounts that still exist in the database.
   valid_defaults = [acc for acc in saved_defaults if acc in all_accounts]

   st.multiselect(
       "Select Accounts",
       options=all_accounts,
       # Use the validated list as the default.
       default=valid_defaults,
       key="account_multiselect_main",
       on_change=update_filters_callback,
       # Disable the widget if there are no accounts to select.
       disabled=not all_accounts,
       placeholder="No accounts found" if not all_accounts else "Choose accounts"
   )

   today = get_current_est_date()
   min_date_allowed = today - datetime.timedelta(days=5*365)
   default_start = today - datetime.timedelta(days=30)

   st.date_input(
       "Start Date",
       value=st.session_state.get("filters", {}).get("start_date", default_start),
       min_value=min_date_allowed,
       max_value=today,
       key="start_date_main",
       on_change=update_filters_callback
   )

   st.date_input(
       "End Date",
       value=st.session_state.get("filters", {}).get("end_date", today),
       min_value=st.session_state.get("start_date_main", default_start),
       max_value=today,
       key="end_date_main",
       on_change=update_filters_callback
   )

   # Initialize the filters on the very first run
   if "filters" not in st.session_state:
       update_filters_callback()

def trade_data_tab(r):
    st.header("🔍 Trade Data Viewer")
    
    set_trade_filters_main_tab(r)
    
    filters = st.session_state.get("filters", {})
    accounts = filters.get("accounts")
    start_date = filters.get("start_date")
    end_date = filters.get("end_date")

    if st.button("Display Trades", type="primary", use_container_width=True):
        if not all([accounts, start_date, end_date]):
            st.warning("Please select accounts and a date range.")
        else:
            with st.spinner("Fetching trades..."):
                trades = fetch_trades(r, tuple(accounts), start_date, end_date)
                st.session_state.trade_df = pd.DataFrame(trades)
                
                st.session_state.last_fetched_filters = filters
                
                st.success(f"Found {len(st.session_state.trade_df):,} trades.")
                st.rerun()

    if 'trade_df' in st.session_state:
        @st.fragment(run_every="10s")
        def auto_refresh_display():
            # Only refresh if a fetch has been done before.
            if 'last_fetched_filters' in st.session_state:
                try:
                    last_filters = st.session_state.last_fetched_filters
                    trades = fetch_trades(
                        r, 
                        tuple(last_filters.get("accounts", [])), 
                        last_filters.get("start_date"), 
                        last_filters.get("end_date")
                    )
                    # Update the DataFrame in the session state
                    st.session_state.trade_df = pd.DataFrame(trades)
                except (ConnectionError, TimeoutError):
                    pass
        
        auto_refresh_display()

        df = st.session_state.trade_df

        if df.empty:
            st.info("No trades found for the selected criteria.")
            return

        st.markdown(f"Displaying {len(df):,} matching trades.")
        
        display_cols = ['account', 'trade_date', 'trade_time', 'ticker', 'price', 'quantity', 'type']
        existing_cols = [col for col in display_cols if col in df.columns]
        
        st.dataframe(df[existing_cols], use_container_width=True)
    else:
        st.info("Select filters and click 'Display Trades' to begin.")

@st.cache_data(ttl=300)
def get_all_accounts(_r):
    return sorted(_r.smembers("accounts"))

def admin_tab(trade_manager, user_manager, r):
   st.title("🛠️ Admin Panel")

   tab1, tab2, tab3, tab4 = st.tabs(["Database Status", "Generate Test Data", "Data Management", "User Management"])

   with tab1:
       st.header("📊 Database Status")
       col1, col2, col3 = st.columns(3)
       with col1:
            total_trades = r.get("total_trades_booked")
            st.metric("Total Trades Booked", f"{int(total_trades) if total_trades else 0:,}")
            
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

   with tab2:
       st.header("🎲 Generate Test Data")
       st.subheader("Generation Parameters")

       all_accounts = ["JohnDoe", "JaneSmith", "TraderJoe", "AliceWonderland", "BobBuilder", "JeffBezos", "ElonMusk", "BillGates", "MarkZuckerberg", "WarrenBuffett", "DonaldTrump", "BarackObama", "OprahWinfrey", "TaylorSwift", "KanyeWest"]
       default_tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "JPM", "V", "META"]

       selected_accounts = st.multiselect("Accounts to use:", options=all_accounts, default=all_accounts[4:])
       selected_tickers = st.multiselect("Tickers to use:", options=default_tickers, default=default_tickers)
       selected_trade_types = st.multiselect("Trade types to use:", options=["buy", "sell"], default=["buy", "sell"])

       col1, col2, col3 = st.columns(3)
       with col1:
           qty_min, qty_max = st.slider("Quantity Range:", 1, 1000, (10, 200))
       with col2:
           price_min, price_max = st.slider("Price Range ($):", 10.0, 3000.0, (50.0, 1500.0))
       with col3:
           realistic_pricing = st.checkbox("Ensure trade price is within ±20% of actual market price", value=False)

       st.divider()

       num_trades = st.number_input(
           "Total number of random trades to generate:",
           min_value=1, max_value=10000000, value=1000, key="num_trades_input"
       )

       if st.button("🕵️‍♂️💵🤝 Book Random Trades", key="generate_parallel", type="primary"):
           if not all([selected_accounts, selected_tickers, selected_trade_types]):
               st.error("Please select at least one option for Accounts, Tickers, and Trade Types.")
           else:
                total_trades = num_trades
                min_qty, max_qty = qty_min, qty_max
                min_price, max_price = price_min, price_max

                ideal_trades_per_process = 2000
                max_processes = 100
                num_processes = min((total_trades + ideal_trades_per_process - 1) // ideal_trades_per_process, max_processes)

                trades_per_process = total_trades // num_processes
                extras = total_trades % num_processes

                batch_size = 500 if trades_per_process >= 1000 else 100

                args_list = []
                for i in range(num_processes):
                    trades_for_this = trades_per_process + (1 if i < extras else 0)
                    args_list.append(trades_for_this)

                start_time = time.perf_counter()
                with st.status(f"Sending {total_trades:,} trades to stream...", expanded=True) as status:
                    processes = []

                    for trades_for_this_worker in args_list:
                        p = Process(target=book_trades_in_batches, kwargs={
                            'r': r,
                            'num_trades': trades_for_this_worker,
                            'accounts': selected_accounts,
                            'tickers': selected_tickers,
                            'trade_types': selected_trade_types,
                            'quantity_range': (min_qty, max_qty),
                            'price_range': (min_price, max_price),
                            'batch_size': batch_size,
                            'realistic_pricing' : realistic_pricing
                        })
                        p.start()
                        processes.append(p)

                    for p in processes:
                        p.join()

                    end_time = time.perf_counter()
                    duration = end_time - start_time

                    st.cache_data.clear()
                    msg = f"{total_trades:,} trades sent to stream in <{duration:.2f}s using {num_processes} process{'es' if num_processes > 1 else ''}."

                    # Show how trades were split (e.g., "4 sent 5,000 each, 1 sent 5,123")
                    if extras == 0:
                        msg += f" Each process sent {trades_per_process:,} trades to the stream."
                    else:
                        msg += f" {num_processes - extras} process{'es' if num_processes - extras > 1 else ''} sent {trades_per_process:,}, {extras} sent {trades_per_process + 1:,}."

                    st.session_state["sent_to_stream_msg"] = msg

                #start_booking_time = time.perf_counter() #bug!!! it really starts booking WHILE trades are being sent to trade!!
                waited = 0.0

                with st.status("Waiting for trades to be fully booked. Trade_booker consumer group sending trades from stream into redis...", expanded=True) as wait_status:
                    while waited < 600: #times out after 10 minutes
                        try:
                            pending_info = r.xpending("trades_stream", "booker-group")
                            if pending_info['pending'] == 0:
                                #duration = time.perf_counter() - start_booking_time
                                duration = time.perf_counter() - start_time
                                wait_status.update(
                                    label="✅ All trades sent to the stream have now been booked to redis hashes!",
                                    state="complete",
                                    expanded=False
                                )
                                st.session_state["booked_from_stream_msg"] = (
                                    f"{total_trades:,} booked to redis in <{duration:.2f}s (starting clock the second we started sending to stream!) using 35 processes"
                                )
                                break
                        except redis.exceptions.ResponseError as e:
                            wait_status.update(label=f"❌ Error: {e}", state="error", expanded=True)
                            break


                    else:
                        wait_status.update(
                            label="⚠️ Still processing trades after timeout.",
                            state="warning",
                            expanded=False
                        )

                st.rerun()

       if "sent_to_stream_msg" in st.session_state:
           st.success(st.session_state.pop("sent_to_stream_msg"))
       if "booked_from_stream_msg" in st.session_state:
           st.success(st.session_state.pop("booked_from_stream_msg"))
       st.divider()


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
                           # FIX: Clear the cache to force a refresh.
                           st.cache_data.clear()
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

   with tab4:
       st.header("👥 User Management")
       try:
           all_users = user_manager.get_all_users()
           if all_users:
               df = pd.DataFrame(all_users)
               st.dataframe(df, use_container_width=True)
           else:
               st.info("No users found in the database.")
       except Exception as e:
           st.error(f"Error fetching user data: {e}")

def get_standard_date_range(option):
    today = get_current_est_date()
    if option == "1d":
        start = today - datetime.timedelta(days=1)
    elif option == "5d":
        start = today - datetime.timedelta(days=5)
    elif option == "1m":
        start = today - pd.DateOffset(months=1)
    elif option == "3m":
        start = today - pd.DateOffset(months=3)
    elif option == "ytd":
        start = datetime.date(today.year, 1, 1)
    elif option == "1y":
        start = today - pd.DateOffset(years=1)
    elif option == "5y":
        start = today - pd.DateOffset(years=5)
    elif option == "All":
        return None, pd.to_datetime(today)
    else:
        start = today - pd.DateOffset(years=10)
    return pd.to_datetime(start), pd.to_datetime(today)

def get_historical_data_filter():
    file_path = os.path.join(os.path.dirname(__file__), 'all_tickers_filtered.txt')
    tickers = load_all_tickers(file_path)
    if not tickers:
        st.warning("No tickers available.")
        return None
    st.session_state["selected_ticker"] = st.selectbox(
        "Select Ticker",
        options=tickers,
        index=None,
        key="ticker_select_box"
    )

@st.cache_data(ttl=3600)
def get_company_name(ticker):
    try:
        info = yf.Ticker(ticker).info
        # Try 'shortName' first, then 'longName', else fallback to ticker
        return info.get("shortName") or info.get("longName") or ticker
    except Exception as e:
        return f"Unknown Company"
    
def stock_data_tab(r):
    st.title("📈 Stock Data Viewer")
    col1, col2 = st.columns([5, 1])
    with col2:
        get_historical_data_filter()
    with col1:
        selected_ticker = st.session_state.get("selected_ticker")
        if selected_ticker:
            company_name = get_company_name(selected_ticker)
            st.subheader(f"{company_name} ({selected_ticker})")
            display_live_price(selected_ticker)
            # Date range controls (auto-update)
            timeframes = ["1d", "5d", "1m", "3m", "ytd", "1y", "5y", "All", "Custom"]
            default_timeframe = "1m"
            timeframe = st.radio(
                "Select Timeframe",
                options=timeframes,
                index=timeframes.index(default_timeframe),
                horizontal=True,
                label_visibility="collapsed"
            )

            today = get_current_est_date() 
            if timeframe == "Custom":
                custom_start = st.date_input("Custom Start Date", value=today - datetime.timedelta(days=30), key="custom_start_date")
                custom_end = st.date_input("Custom End Date", value=today, key="custom_end_date")
                start_date, end_date = pd.to_datetime(custom_start), pd.to_datetime(custom_end)
            else:
                start_date, end_date = get_standard_date_range(timeframe)

            if timeframe == "All" and start_date is None:
                start_date = "all"

            # Only show chart if user has clicked the button at least once and a ticker is selected
            display_stock_data(selected_ticker, start_date, end_date)
        else:
            st.info("Select a ticker to view stock data.")

@st.cache_data(ttl=3600)
def get_earliest_yfinance_date(ticker):
    try:
        df = yf.download(ticker, start="1900-01-01", end=get_current_est_date()) # Use timezone-aware date
        if not df.empty:
            return pd.to_datetime(df.index[0]).date()
        else:
            return None
    except Exception as e:
        st.warning(f"Could not fetch earliest date for {ticker}: {e}")
        return None
    
@st.fragment(run_every="5s")
def display_live_price(ticker):
    price = get_price(ticker, 1)
    st.markdown(
        f"<span style='font-size:2.5em; font-weight:bold;'>${price:.2f}</span>"
        if price is not None else "<span style='font-size:1.2em;'>No live price available.</span>",
        unsafe_allow_html=True
    )
def display_stock_data(ticker, start_date, end_date):
    try:
        # Handle "All" case: find earliest available date for ticker
        if start_date == "all":
            earliest = get_earliest_yfinance_date(ticker)
            if earliest:
                start_date = pd.to_datetime(earliest)
            else:
                st.warning("Could not determine earliest available date for this ticker. Using 20 years as fallback.")
                start_date = pd.to_datetime(datetime.date.today() - pd.DateOffset(years=20))
                
        with st.spinner(f"Fetching all historical data for {ticker} since {start_date.date()}..."):
            data = get_eod_price_range(
                ticker,
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d")
            )
        daily_prices = data["daily_prices"]
        if not daily_prices:
            st.warning(f"No historical data found for {ticker} from {start_date.date()} to {end_date.date()}.")
            return
        df = pd.DataFrame(list(daily_prices.items()), columns=["Date", "Price"])
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.set_index("Date").sort_index()

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df.index, y=df["Price"], mode='lines+markers', name='Price',
            line=dict(color='royalblue', width=2)
        ))

        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Price",
            template="plotly_white",
            autosize=True,
            margin=dict(l=20, r=20, t=40, b=20),
            xaxis=dict(
                rangeslider=dict(visible=False),
                type="date"
            )
        )

        st.plotly_chart(fig, use_container_width=True)

        price_change = data.get("price_change", {})
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Start Price", f"${price_change.get('start_price', 0):.2f}")
        col2.metric("End Price", f"${price_change.get('end_price', 0):.2f}")
        col3.metric("Abs Change", f"${price_change.get('absolute_change', 0):.2f}")
        col4.metric("Percent Change", f"{price_change.get('percentage_change', 0):.2f}%")
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Error fetching historical data for {ticker}: {e}")

def display_detailed_positions(df, _pnl_retriever):
    for _, row in df.iterrows():
        company_name = get_company_name(row["Ticker"])
        label = f"**{company_name}** ({row['Ticker']})"
        expander = st.expander(label)
        with expander:
            st.write(f"**Shares:** {row['Position']}")

            try:
                account_id = row["Account"]
                pnl_data = _pnl_retriever.get_ticker_pnl(account_id, row["Ticker"])
                unrealized_pnl = pnl_data.get('unrealized_pnl', 0.0)
                realized_pnl = pnl_data.get('realized_pnl', 0.0)
                total_pnl = pnl_data.get('total_pnl', 0.0)

                st.write(f"**Unrealized PnL:** ${unrealized_pnl:,.2f}")
                st.write(f"**Realized PnL:** ${realized_pnl:,.2f}")
                st.write(f"**Total PnL:** ${total_pnl:,.2f}")

            except Exception as e:
                st.info("PnL data is pending calculation.")

            try:
                current_price = get_price(row["Ticker"], 1)
                st.write(f"**Current Price:** ${current_price:.2f}" if current_price is not None else "Current Price: N/A")
            except Exception:
                st.write("Current Price: N/A")

def load_all_tickers(file_path):
   try:
       with open(file_path, 'r') as f:
           tickers = [line.strip() for line in f if line.strip() and re.match(r"^[A-Z]{1,5}$", line.strip())]
       return tickers
   except Exception as e:
       st.error(f"Error loading tickers from {file_path}: {e}")
       return []

def get_all_positions(_r):
    logger.info("Fetching all positions from Redis...")
    positions_data = []
    for field, value in _r.hscan_iter("positions", count=5000):
        try:
            account, ticker = field.split(':', 1)
            positions_data.append({
                "account": account,
                "ticker": ticker,
                "quantity": int(value)
            })
        except (ValueError, IndexError):
            logger.warning(f"Could not parse position key: {field}")
            continue
    return positions_data

def display_positions_data(r, pnl_retriever):
    positions = get_all_positions(r)

    if not positions:
        st.warning("No positions data available.")
        return
    
    df = pd.DataFrame(positions)
    if df.empty:
        st.info("No positions found.")
        return

    # Add PnL and current price columns
    df["Unrealized PnL"] = 0.0
    df["Realized PnL"] = 0.0
    df["Total PnL"] = 0.0
    df["Current Price"] = None

    for idx, row in df.iterrows():
        account = row["account"]
        ticker = row["ticker"]
        try:
            pnl_data = pnl_retriever.get_ticker_pnl(account, ticker)
            df.at[idx, "Unrealized PnL"] = pnl_data.get('unrealized_pnl', 0.0)
            df.at[idx, "Realized PnL"] = pnl_data.get('realized_pnl', 0.0)
            df.at[idx, "Total PnL"] = pnl_data.get('total_pnl', 0.0)
        except Exception:
            pass
        try:
            price = get_price(ticker, 1)
            df.at[idx, "Current Price"] = price if price is not None else "N/A"
        except Exception:
            df.at[idx, "Current Price"] = "N/A"

    # Rename columns for display
    df = df.rename(columns={
        "account": "Account",
        "ticker": "Ticker",
        "quantity": "Shares"
    })
# Group by Account and Ticker, summing Shares and PnL columns
    grouped = df.groupby(["Account", "Ticker"], as_index=False).agg({
        "Shares": "sum",
        "Unrealized PnL": "sum",
        "Realized PnL": "sum",
        "Total PnL": "sum",
        "Current Price": "first"  # Use first price for each group
    })

    st.dataframe(grouped[["Account", "Ticker", "Shares", "Unrealized PnL", "Realized PnL", "Total PnL", "Current Price"]], use_container_width=True)
def display_single_account_view(account_name, r, pnl_retriever):
    st.header(f"Account Details: {account_name}")

    if st.button("← Back to all accounts"):
        st.session_state.selected_account = None
        st.rerun()

    book_tab, positions_tab, history_tab = st.tabs(["Book Trade", "Positions", "Trade History"])

    with book_tab:
        # --- 1. Initialize form state if it doesn't exist ---
        if "single_account_ticker" not in st.session_state:
            st.session_state.single_account_ticker = ""
        if "single_account_price" not in st.session_state:
            st.session_state.single_account_price = 100.0
        if "single_account_quantity" not in st.session_state:
            st.session_state.single_account_quantity = 100
        if "single_account_trade_type" not in st.session_state:
            st.session_state.single_account_trade_type = "BUY"

        @st.fragment(run_every="4s")
        def book_trade_interface():
            # Define the callback function that will handle the logic.
            def handle_add_trade():
                ticker = st.session_state.single_account_ticker.upper()
                price = st.session_state.single_account_price
                trade_type = st.session_state.single_account_trade_type
                quantity = st.session_state.single_account_quantity

                if not ticker:
                    st.session_state.trade_message = ("warning", "⚠️ Please enter a Ticker.")
                    return

                is_valid_trade = True
                if trade_type.upper() == "SELL":
                    current_position_str = r.hget("positions", f"{account_name}:{ticker}")
                    current_position = int(current_position_str) if current_position_str else 0
                    if quantity > current_position:
                        st.session_state.trade_message = ("error",
                                                          f"❌ Sell order failed. You only own {current_position} shares.")
                        is_valid_trade = False

                if is_valid_trade:
                    try:
                        trade_string = f"{account_name},{ticker}:{price}:{trade_type.lower()}:{quantity}:trade"
                        if book_custom_trade_to_stream(trade_string, r):
                            st.session_state.trade_message = ("success",
                                                              "✅ Trade sent successfully! Positions will update shortly.")
                            # It's safe to clear the form here
                            st.session_state.single_account_ticker = ""
                            st.session_state.single_account_price = 100.0
                            st.session_state.single_account_quantity = 100
                        else:
                            st.session_state.trade_message = ("error", "❌ Failed to save trade to the database stream.")
                    except Exception as e:
                        st.session_state.trade_message = ("error", f"❌ Error processing trade: {e}")

            # Display any message that was set in the callback
            if "trade_message" in st.session_state:
                level, message = st.session_state.trade_message
                if level == "success":
                    st.success(message)
                elif level == "warning":
                    st.warning(message)
                else:
                    st.error(message)
                del st.session_state.trade_message

            st.subheader("Book a Trade for this Account")
        
            st.text_input("Ticker Symbol", placeholder="AAPL", key="single_account_ticker")
            st.number_input("Price ($)", min_value=0.01, step=0.01, key="single_account_price")
            st.selectbox("Trade Type", ["BUY", "SELL"], key="single_account_trade_type")
            st.number_input("Quantity", min_value=1, step=1, key="single_account_quantity")

            st.button("Execute Trade", key="single_account_add_trade", on_click=handle_add_trade)

        book_trade_interface()

    with positions_tab:
        grid_view, detailed_view = st.tabs(["Grid View", "Detailed View"])
        with grid_view: 
            @st.fragment(run_every="2s")
            def display_grid_positions():
                positions = get_account_positions(account_name, r)
                if positions:
                    rows = []
                    for key, value in positions.items():
                        ticker = key.split(":")[1]
                        shares = int(value)
                        # Get PnL and price
                        try:
                            pnl_data = pnl_retriever.get_ticker_pnl(account_name, ticker)
                            unrealized_pnl = pnl_data.get('unrealized_pnl', 0.0)
                            realized_pnl = pnl_data.get('realized_pnl', 0.0)
                            total_pnl = pnl_data.get('total_pnl', 0.0)
                        except Exception:
                            unrealized_pnl = realized_pnl = total_pnl = 0.0
                        try:
                            price = get_price(ticker, 1)
                        except Exception:
                            price = "N/A"
                        rows.append({
                            "Account": account_name,
                            "Ticker": ticker,
                            "Shares": shares,
                            "Unrealized PnL": unrealized_pnl,
                            "Realized PnL": realized_pnl,
                            "Total PnL": total_pnl,
                            "Current Price": price
                        })
                    df = pd.DataFrame(rows)
                    st.dataframe(df[["Account", "Ticker", "Shares", "Unrealized PnL", "Realized PnL", "Total PnL", "Current Price"]], use_container_width=True)
                else:
                    st.info("No positions for this account.")
            display_grid_positions()

        with detailed_view:
            @st.fragment(run_every="2s")
            def display_refreshed_positions():
                display_account_positions(account_name, r, pnl_retriever)
            display_refreshed_positions()

    with history_tab:
        st.subheader("Trade History")
        set_trade_filters_user_tab(r)
        filters = st.session_state.get("user_filters", {})
        start_date = filters.get("start_date")
        end_date = filters.get("end_date")

        @st.fragment(run_every="5s")
        def auto_refresh_trade_history():
            if start_date and end_date:
                trades = fetch_trades(r, (account_name,), start_date, end_date)
                if trades:
                    df = pd.DataFrame(trades)
                    if 'trade_date' in df.columns and 'trade_time' in df.columns:
                        df = df.sort_values(by=['trade_date', 'trade_time'], ascending=False).reset_index(drop=True)
                    display_cols = ['trade_date', 'trade_time', 'ticker', 'type', 'price', 'quantity']
                    existing_cols = [col for col in display_cols if col in df.columns]
                    st.dataframe(df[existing_cols], use_container_width=True)
                    st.caption(f"Found {len(df)} trades for {account_name} in the selected date range.")
                else:
                    st.info("No trade history found for the selected account and date range.")
            else:
                st.warning("Please select a valid date range.")

        auto_refresh_trade_history()

def update_user_filters_callback():
    """Callback to update the user-specific filters in the session state."""
    st.session_state["user_filters"] = {
        "start_date": st.session_state.get("start_date_user"),
        "end_date": st.session_state.get("end_date_user")
    }

def set_trade_filters_user_tab(r):

   today = get_current_est_date()
   min_date_allowed = today - datetime.timedelta(days=5*365)
   default_start = today - datetime.timedelta(days=30)

   st.date_input(
       "Start Date",
       value=st.session_state.get("user_filters", {}).get("start_date", default_start),
       min_value=min_date_allowed,
       max_value=today,
       key="start_date_user",
       on_change=update_user_filters_callback
   )

   st.date_input(
       "End Date",
       value=st.session_state.get("user_filters", {}).get("end_date", today),
       min_value=st.session_state.get("start_date_user", default_start),
       max_value=today,
       key="end_date_user",
       on_change=update_user_filters_callback
   )
   # Initialize the filters on the very first run for this tab
   if "user_filters" not in st.session_state:
       update_user_filters_callback()

def display_account_positions(account_name, r, pnl_retriever):
    st.subheader("Current Positions")
    positions = get_account_positions(account_name, r)
    if positions:
        rows = [{"Account": account_name, "Ticker": key.split(":")[1], "Position": int(value)} for key, value in positions.items()]
        df = pd.DataFrame(rows)
        display_detailed_positions(df, pnl_retriever)
    else:
        st.info("No positions for this account.")
        
def display_account_list_view(user_email, user_manager):
    st.header("Your Accounts")
    
    accounts = user_manager.get_user_accounts(user_email)
    if accounts:
        st.write("Click on an account to view its details.")
        cols = st.columns(6)
        for i, account_name in enumerate(accounts):
            #if cols[i % 4].button(account_name, key=f"account_{account_name}"):
            if cols[i % 6].button(account_name, key=f"account_{account_name}", use_container_width=True):

                st.session_state.selected_account = account_name
                st.rerun()
    else:
        st.info("You have no accounts yet. Create one below.")

    st.divider()

    with st.form("create_account_form"):
        st.subheader("Create a New Account")
        new_account_name = st.text_input("New Account Name")
        submitted = st.form_submit_button("Create Account")
        if submitted and new_account_name:
            success, message = user_manager.add_account_to_user(user_email, new_account_name)
            if success:
                st.success(message)
                st.rerun()
            else:
                st.error(message)
        elif submitted:
            st.warning("Please enter an account name.")

def user_account_tab(user_email, r, pnl_retriever):
    user_manager = UserManager(r)

    if "selected_account" not in st.session_state:
        st.session_state.selected_account = None

    if st.session_state.selected_account:
        display_single_account_view(st.session_state.selected_account, r, pnl_retriever)
    else:
        display_account_list_view(user_email, user_manager)

def get_account_positions(account_name:str, r): 
        positions_key = "positions"
        
        match_pattern = f"{account_name}:*"
        
        account_positions = {}
        
        for field, value in r.hscan_iter(positions_key, match=match_pattern):
            account_positions[field] = value

        return account_positions

def main():
    st.set_page_config(page_title="Trade Viewer", layout="wide")
    r = get_redis_connection()
    user_manager = UserManager(r)
    pnl_retriever = PnLRetriever()
    trade_manager = TradeManager()

    # --- Admin Configuration ---
    ADMIN_EMAILS = ["mofox0919@gmail.com", "azelefsk@mail.yu.edu", "azimotechin@gmail.com", "danielinoyatov327@gmail.com", "dinoyato@mail.yu.edu"]

    # --- Custom CSS for Navigation Bar ---
    st.markdown("""
        <style>
            /* Main container for the radio buttons */
            div[data-testid="stRadio"] > div[role="radiogroup"] {
                display: flex;
                justify-content: flex-start; /* Align buttons to the left */
                gap: 5px; /* Make buttons closer together */
                background-color: #f0f2f6;
                padding: 8px;
                border-radius: 12px;
                border: 1px solid #e0e0e0;
            }
            /* This rule correctly hides the default radio button circle */
            div[data-testid="stRadio"] input[type="radio"] {
                display: none;
            }
            /* Style the labels to look like buttons */
            div[data-testid="stRadio"] label {
                padding: 8px 20px;
                background-color: #FFFFFF;
                border: 1px solid #d1d1d1;
                border-radius: 8px;
                cursor: pointer;
                transition: all 0.3s ease;
                font-weight: 500;
                color: #31333F;
            }
            /* Style for the selected button */
            div[data-testid="stRadio"] label:has(input:checked) {
                background-color: #0068c9; /* A nice blue for selection */
                color: white;
                border-color: #005cb3;
                font-weight: 600;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            /* Hover effect for unselected buttons */
            div[data-testid="stRadio"] label:not(:has(input:checked)):hover {
                background-color: #e6eaf1;
            }
        </style>
    """, unsafe_allow_html=True)

    # --- Authentication Flow ---
    if not st.user.is_logged_in:
        st.title("Trade Viewer Dashboard")
        st.divider()
        st.write("Please log in to continue.")
        st.button("Login with Google", on_click=st.login)
        st.stop()

    try:
        user_manager.create_user(st.user)
    except ValueError:
        pass
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        st.stop()

    # --- Header ---
    col1, col2 = st.columns([0.8, 0.2])
    with col1:
        welcome_message = f"Welcome, admin {st.user.name}!" if st.user.email in ADMIN_EMAILS else f"Welcome, {st.user.name}!"
        st.markdown(
            f'''
            <div style="display: flex; align-items: center; gap: 15px;">
                <img src="{st.user.picture}" width="60" style="border-radius: 50%;">
                <h2 style="margin: 0;">{welcome_message}</h2>
            </div>
            ''',
            unsafe_allow_html=True
        )
    with col2:
        st.markdown("""
            <style>
                div[data-testid="stHorizontalBlock"] > div:last-child {
                    display: flex;
                    justify-content: flex-end;
                }
            </style>
        """, unsafe_allow_html=True)
        if st.button("Logout"):
            st.logout()

    st.divider()

    # --- Navigation Bar ---
    if st.user.email in ADMIN_EMAILS:
        PAGES = ["User Tab", "Trade Data", "Positions Data", "Stock Data", "Admin Page"]
    else:
        PAGES = ["User Tab", "Stock Data"]
    
    active_tab = st.radio("Navigation", PAGES, horizontal=True, label_visibility="collapsed")

    if active_tab == "User Tab":
        user_account_tab(st.user.email, r, pnl_retriever)
    elif active_tab == "Trade Data":
        trade_data_tab(r)
    elif active_tab == "Positions Data":
        st.title("📊 Positions Data Viewer")
        display_positions_data(r, pnl_retriever)
    elif active_tab == "Stock Data":
        stock_data_tab(r)
    elif active_tab == "Admin Page":
        admin_tab(trade_manager, user_manager, r)

if __name__ == "__main__":
    main()