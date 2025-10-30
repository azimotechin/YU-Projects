import streamlit as st
import sys
import os
import datetime
import pandas as pd
from redis.sentinel import Sentinel
import re
import logging
import plotly.graph_objs as go
import yfinance as yf
import io

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

# Add the "scripts" folder to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))

from TradeManager import TradeManager
from market_data import get_price, get_eod_price_range, get_historical_price
from UserManager import UserManager
from pnl_getters import PnLRetriever


def get_redis_connection():
    sentinel_hosts = [
        ("sentinel1", 26379),
        ("sentinel2", 26379),
        ("sentinel3", 26379)
    ]
    sentinel = Sentinel(sentinel_hosts, socket_timeout=0.5)
    redis_master = sentinel.master_for("mymaster", decode_responses=True)
    return redis_master


def ensure_stream_exists(redis_client, stream_key="trades_stream"):
    if not redis_client.exists(stream_key):
        redis_client.xadd(stream_key, {"init": "true"})
        redis_client.xtrim(stream_key, maxlen=0)


# When Streamlit UI site starts running, it prepares the stream
r = get_redis_connection()
ensure_stream_exists(r)


def fetch_trades(r, filters):
    filters = st.session_state["filters"]
    trades = []
    for key in r.scan_iter(match="*:*:*", count=5000):
        try:
            account, trade_date = key.split(":")[:2]
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
        display_cols = ['account', 'trade_date', 'trade_time', 'ticker', 'price', 'quantity', 'type', 'trade_action',
                        'redis_key', '']
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

    start_date = st.date_input("Start Date", value=pd.to_datetime('1900-01-01'), min_value='1900-01-01',
                               max_value="today", key="start_date_main")
    end_date = st.date_input("End Date", value=pd.to_datetime('today'), min_value=start_date, max_value="today",
                             key="end_date_main")

    # Save filters to session state
    st.session_state["filters"] = {
        "accounts": selected_accounts if selected_accounts else accounts,
        "start_date": pd.to_datetime(start_date),
        "end_date": pd.to_datetime(end_date)
    }


def admin_tab(trade_manager, user_manager):
    st.title("🛠️ Admin Panel")

    tab1, tab2, tab3, tab4 = st.tabs(["Database Status", "Generate Test Data", "Data Management", "User Management"])

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
            num_trades = st.number_input("Number of random trades to generate:", min_value=1, max_value=10000, value=10,
                                         key="num_trades_input")
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
                        today = datetime.datetime.now().strftime("%Y-%m-%d")
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
                        today = datetime.datetime.now().strftime("%Y-%m-%d")
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


def get_standard_date_range(option):
    today = datetime.date.today()
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
        return None, pd.to_datetime(today)  # None signals "All"
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


@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_company_name(ticker):
    try:
        info = yf.Ticker(ticker).info
        # Try 'shortName' first, then 'longName', else fallback to ticker
        return info.get("shortName") or info.get("longName") or ticker
    except Exception as e:
        return f"Unknown ({ticker})"


def stock_data_tab(r):
    st.title("📈 Stock Data Viewer")
    col1, col2 = st.columns([3, 1])
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
                "Select Time Range",
                options=timeframes,
                index=timeframes.index(default_timeframe),
                horizontal=True,
            )
            # Do NOT update st.session_state["hist_timeframe"] here!

            today = datetime.date.today()
            if timeframe == "Custom":
                custom_start = st.date_input("Custom Start Date", value=today - datetime.timedelta(days=30),
                                             key="custom_start_date")
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


@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_earliest_yfinance_date(ticker):
    try:
        df = yf.download(ticker, start="1900-01-01", end=datetime.date.today())
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


@st.cache_data(ttl=3600)  # Cache for 1 hour
def display_detailed_positions(df, pnl_retriever):
    """Renders a detailed, expandable view for each position in the given DataFrame."""
    for _, row in df.iterrows():
        company_name = get_company_name(row["Ticker"])
        label = f"**{company_name}** ({row['Ticker']})"
        expander = st.expander(label)
        with expander:
            st.write(f"**Shares:** {row['Position']}")

            try:
                # Get the account ID from the row, which is now available
                account_id = row["Account"]
                pnl_data = pnl_retriever.get_ticker_pnl(account_id, row["Ticker"])
                unrealized_pnl = pnl_data.get('unrealized_pnl', 0.0)
                realized_pnl = pnl_data.get('realized_pnl', 0.0)
                total_pnl = pnl_data.get('total_pnl', 0.0)

                st.write(f"**Unrealized PnL:** ${unrealized_pnl:,.2f}")
                st.write(f"**Realized PnL:** ${realized_pnl:,.2f}")
                st.write(f"**Realized PnL:** ${total_pnl:,.2f}")

            except Exception as e:
                st.info("PnL data is pending calculation.")

            try:
                current_price = get_price(row["Ticker"], 1)
                st.write(
                    f"**Current Price:** ${current_price:.2f}" if current_price is not None else "Current Price: N/A")
            except Exception:
                st.write("Current Price: N/A")
            try:
                current_value = current_price * row["Position"] if current_price is not None else None
                st.write(
                    f"**Current Value:** ${current_value:,.2f}" if current_value is not None else "Current Value: N/A")
            except Exception:
                st.write("Current Value: N/A")
            # Show historical prices
            try:
                price_1d = get_historical_price(row["Ticker"], "1d")
                price_5d = get_historical_price(row["Ticker"], "5d")
                price_1m = get_historical_price(row["Ticker"], "1mo")
                price_3m = get_historical_price(row["Ticker"], "3mo")
                price_1y = get_historical_price(row["Ticker"], "1y")
                price_5y = get_historical_price(row["Ticker"], "5y")
                price_ytd = get_historical_price(row["Ticker"], "ytd")
                st.write(f"**Price 1D Ago:** ${price_1d:.2f}" if price_1d is not None else "Price 1D Ago: N/A")
                st.write(f"**Price 5D Ago:** ${price_5d:.2f}" if price_5d is not None else "Price 5D Ago: N/A")
                st.write(f"**Price 1M Ago:** ${price_1m:.2f}" if price_1m is not None else "Price 1M Ago: N/A")
                st.write(f"**Price 3M Ago:** ${price_3m:.2f}" if price_3m is not None else "Price 3M Ago: N/A")
                st.write(f"**YTD Price:** ${price_ytd:.2f}" if price_ytd is not None else "YTD Price: N/A")
                st.write(f"**Price 1Y Ago:** ${price_1y:.2f}" if price_1y is not None else "Price 1Y Ago: N/A")
                st.write(f"**Price 5Y Ago:** ${price_5y:.2f}" if price_5y is not None else "Price 5Y Ago: N/A")
            except Exception:
                st.write("Historical price data unavailable.")


def load_all_tickers(file_path):
    try:
        with open(file_path, 'r') as f:
            tickers = [line.strip() for line in f if line.strip() and re.match(r"^[A-Z]{1,5}$", line.strip())]
        return tickers
    except Exception as e:
        st.error(f"Error loading tickers from {file_path}: {e}")
        return []


@st.fragment(run_every="5s")
def display_positions_data(r, pnl_retriever):
    positions = r.hgetall("positions")
    if not positions:
        st.warning("No positions data available.")
        return
    rows = []
    accounts = set()
    today = datetime.date.today()
    for key, value in positions.items():
        try:
            account, ticker = key.split(":")
        except ValueError:
            account, ticker = key, ""
        accounts.add(account)
        position = int(value)
        rows.append({
            "Account": account,
            "Ticker": ticker,
            "Position": position,
        })

    df = pd.DataFrame(rows)
    col1, col2 = st.columns([3, 1])
    with col2:
        accounts = sorted(list(accounts))
        selected_account = st.selectbox(
            "Select Account",
            options=accounts,
            key="positions_account_select"
        )

    with col1:
        if selected_account:
            st.subheader(f"{selected_account}'s Positions")
            filtered_df = df[df["Account"] == selected_account]
            display_detailed_positions(filtered_df, pnl_retriever)
        elif accounts:
            st.info("Select an account to view its positions.")


def display_single_account_view(account_name, r, pnl_retriever):
    """Renders the detailed view for a single selected account."""
    st.header(f"Account Details: {account_name}")

    if st.button("← Back to all accounts"):
        st.session_state.selected_account = None
        st.rerun()

    book_tab, positions_tab, history_tab = st.tabs(["Book Trade", "Positions", "Trade History"])

    with book_tab:
        st.subheader("Book a Trade for this Account")
        ticker = st.text_input("Ticker Symbol", placeholder="AAPL", key="single_account_ticker").upper()
        price = st.number_input("Price ($)", min_value=0.01, value=100.0, step=0.01, key="single_account_price")
        trade_type = st.selectbox("Trade Type", ["BUY", "SELL"], key="single_account_trade_type")
        quantity = st.number_input("Quantity", min_value=1, value=100, step=1, key="single_account_quantity")
        if st.button("Add Trade", key="single_account_add_trade"):
            if ticker:
                try:
                    today = datetime.datetime.now().strftime("%Y-%m-%d")
                    trade_string = f"{account_name}:{today},{ticker}:${price}:{trade_type}:{quantity}"
                    trade = TradeManager.convert_string_to_trade(trade_string)
                    trade_manager = TradeManager()
                    if trade_manager.write_trade(trade):
                        st.success(f"✅ Trade added successfully! Positions will update shortly.")
                        st.rerun()  # Rerun to clear form and show success message
                    else:
                        st.error("❌ Failed to save trade to database")
                except Exception as e:
                    st.error(f"❌ Error processing trade: {e}")
            else:
                st.warning("⚠️ Please fill in all required fields (Ticker)")

    with positions_tab:
        @st.fragment(run_every="2s")
        def display_refreshed_positions():
            display_account_positions(account_name, r, pnl_retriever)

        display_refreshed_positions()

    with history_tab:
        st.subheader("Trade History")
        trades = get_account_trades(account_name, r)
        if trades:
            trade_list = [data for data in trades.values()]
            df = pd.DataFrame(trade_list)
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No trade history for this account.")


def display_account_positions(account_name, r, pnl_retriever):
    st.subheader("Current Positions")
    positions = get_account_positions(account_name, r)
    if positions:
        rows = [{"Account": account_name, "Ticker": key.split(":")[1], "Position": int(value)} for key, value in
                positions.items()]
        df = pd.DataFrame(rows)
        display_detailed_positions(df, pnl_retriever)
    else:
        st.info("No positions for this account.")


def display_account_list_view(user_email, user_manager):
    """Renders the main list of user accounts and the creation form."""
    st.header("Your Accounts")

    accounts = user_manager.get_user_accounts(user_email)
    if accounts:
        st.write("Click on an account to view its details.")
        cols = st.columns(4)
        for i, account_name in enumerate(accounts):
            if cols[i % 4].button(account_name, key=f"account_{account_name}"):
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
    """Manages the user account tab, switching between list and detail views."""
    user_manager = UserManager(r)

    if "selected_account" not in st.session_state:
        st.session_state.selected_account = None

    if st.session_state.selected_account:
        display_single_account_view(st.session_state.selected_account, r, pnl_retriever)
    else:
        display_account_list_view(user_email, user_manager)


def get_account_trades(account_name: str, r):
    trades = {}
    match_pattern = f"{account_name}:*:*"

    for key in r.scan_iter(match_pattern):
        trade_data = r.hgetall(key)
        # No decoding needed as decode_responses=True is set on the client
        trades[key] = trade_data
    return trades


def get_account_positions(account_name: str, r):
    # Corrected key name from "positioins" to "positions"
    positions_key = "positions"

    # Use HSCAN to efficiently iterate over fields matching a pattern on the server side.
    # This avoids fetching all positions into memory at once.
    match_pattern = f"{account_name}:*"

    account_positions = {}
    # hscan_iter returns an iterator, which is memory-friendly.
    for field, value in r.hscan_iter(positions_key, match=match_pattern):
        # No decoding needed as decode_responses=True is set on the client
        account_positions[field] = value

    # It's better to return an empty dictionary if no positions are found,
    # as this is a valid state for an account. Raising an error can be disruptive.
    return account_positions


def main():
    st.set_page_config(page_title="Trade Viewer", layout="wide")
    r = get_redis_connection()
    user_manager = UserManager(r)
    pnl_retriever = PnLRetriever()

    # --- Admin Configuration ---
    # Add the email addresses of users who should have admin privileges.
    ADMIN_EMAILS = ["mofox0919@gmail.com", "azelefsk@mail.yu.edu"]

    # --- Corrected Authentication and User Creation Flow ---
    if not st.user.is_logged_in:
        st.title("Trade Viewer Dashboard")
        st.divider()
        st.write("Please log in to continue.")
        st.button("Login with Google", on_click=st.login)  # Typo fixed
        st.stop()

    # Try to create a user. If they already exist, just continue.
    try:
        user_manager.create_user(st.user)
    except ValueError:
        # This error is expected for returning users, so we can safely ignore it.
        pass
    except Exception as e:
        # Catch any other unexpected errors during user creation.
        st.error(f"An unexpected error occurred: {e}")
        st.stop()

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

    st.title("Trade Viewer Dashboard")
    trade_manager = TradeManager()

    # --- Role-Based Tab Rendering ---
    if st.user.email in ADMIN_EMAILS:
        # Admin View: Show all tabs
        PAGES = ["User Tab", "Trade Data", "Positions Data", "Stock Data", "User Input", "Admin Page"]
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(PAGES)

        with tab1:
            user_account_tab(st.user.email, r, pnl_retriever)

        with tab2:
            st.title("📈 Real-Time Trade Viewer")
            col1, col2 = st.columns([3, 1])
            with col2:
                set_trade_filters_main_tab(r)
            with col1:
                # Use .get() for safety in case filters aren't set yet
                accounts = st.session_state.get("filters", {}).get("accounts", [])
                start_date = st.session_state.get("filters", {}).get("start_date", "N/A")
                end_date = st.session_state.get("filters", {}).get("end_date", "N/A")
                st.write(f"#### Showing trades for {len(accounts)} account(s) from {start_date} to {end_date}")
                display_trade_table(r)

        with tab3:
            st.title("📊 Positions Data Viewer")
            display_positions_data(r, pnl_retriever)

        with tab4:
            stock_data_tab(r)

        with tab5:
            user_input_tab(trade_manager)

        with tab6:
            admin_tab(trade_manager, user_manager)
    else:
        # Regular User View: Show limited tabs
        PAGES = ["User Tab", "Stock Data"]
        tab1, tab2 = st.tabs(PAGES)

        with tab1:
            user_account_tab(st.user.email, r)

        with tab2:
            stock_data_tab(r)


if __name__ == "__main__":
    main()