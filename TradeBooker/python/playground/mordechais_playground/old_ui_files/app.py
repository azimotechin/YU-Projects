import streamlit as st
import sys
import os
from datetime import timedelta
import pandas as pd
import redis
from redis.sentinel import Sentinel
from streamlit_autorefresh import st_autorefresh

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

st.set_page_config(page_title="Trade Viewer", layout="wide")
st.title("📈 Real-Time Trade Viewer")

# --- Auto-refresh ---
st_autorefresh(interval=5000, key="refresh")  # Auto-refresh every 5 seconds

sentinel = Sentinel([
('sentinel1', 26379),
('sentinel2', 26379),
('sentinel3', 26379)
], socket_timeout=0.5)

r = sentinel.master_for('mymaster', socket_timeout=0.5, decode_responses=True)
if r is None:
    st.error("❌ Could not connect to Redis. Please ensure the Docker container is running.")
else:
    st.sidebar.header("Filter Options")

    # Populate account list from Redis
    accounts = set()
    for key in r.scan_iter(match="*", count=5000):
        try:
            accounts.add(key.split(':')[0])
        except IndexError:
            continue
    all_accounts = sorted(list(accounts))

    # --- UI for filter selection ---
    if not all_accounts:
        st.sidebar.warning("No accounts found in Redis.")
        selected_accounts = []
    else:
        selected_accounts = st.sidebar.multiselect(
            "Select Accounts",
            options=all_accounts,
            default=[],
            key="account_multiselect"
        )
    if not selected_accounts:
        selected_accounts = all_accounts

    start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime('1900-1-01'), key="start_date")
    end_date = st.sidebar.date_input("End Date", value=pd.to_datetime('2200-12-31'), key="end_date")

    # --- Buttons ---
    reload_filters = st.sidebar.button("Reload Filters")
    manual_refresh = st.sidebar.button("Refresh Table Now")

    # --- Store filters in session_state only when Reload Filters is pressed ---
    if "filters" not in st.session_state or reload_filters:
        st.session_state["filters"] = {
            "accounts": selected_accounts,
            "start_date": start_date,
            "end_date": end_date
        }

    # --- Use stored filters for table display ---
    filters = st.session_state.get("filters", {
        "accounts": all_accounts,
        "start_date": pd.to_datetime('2025-06-01'),
        "end_date": pd.to_datetime('today')
    })

    st.write(f"#### Showing trades for {len(filters['accounts'])} account(s)")

    # Only update table if auto-refresh triggers or manual refresh is pressed
    if st.session_state.get("refresh") or manual_refresh or reload_filters:
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

        df = pd.DataFrame(trades)
        if not df.empty:
            display_cols = ['account', 'trade_date', 'trade_time', 'ticker', 'price', 'quantity', 'type', 'trade_action', 'redis_key', '']
            existing_cols = [col for col in display_cols if col in df.columns]
            st.dataframe(df[existing_cols])
        else:
            st.warning("No trades found for the selected criteria.")


# ideas of UI things to add
# allow the whole database to be accessible - maybe not by displaying the whole thing, but by downloading or some other method
# allow filtering by date, ticker, etc.
# allow user input of trades (either one at a time via string format, or CSV)
# create an admin / dev tool page with the ability to clear data, generate random data

#change tabs to be displayed on top, rather than side (which forced user to collapse them to make more room) as per Steve's reccomendation
#rather than adding a trade string, a form should be displayed allowing for a more seamless picking of 
    #ticker and amount etcetera.
    #and if possible, eventually, in production mode, they should not input account at all. rather the user would login to the site, saving their account, and then that account would automatically be added to their trade string (when you trade stocks on the Fideltiy app for example,you do not remind them your name)