import streamlit as st
import sys
import os
import pandas as pd
import io
from datetime import datetime

# Add the scripts directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'scripts')))

from TradeManager import TradeManager
from Trade import Trade

st.set_page_config(page_title="Trade Input", layout="wide")
st.title("📝 Trade Input")

# Initialize TradeManager
@st.cache_resource
def get_trade_manager():
    try:
        return TradeManager()
    except Exception as e:
        st.error(f"Failed to connect to Redis: {e}")
        return None

trade_manager = get_trade_manager()

if trade_manager is None:
    st.stop()

# Create tabs for different input methods
tab1, tab2 = st.tabs(["Manual Trade Input", "CSV Upload"])

with tab1:
    st.header("Enter Trade Manually")
    
    # Input method selection
    input_method = st.radio(
        "Choose input method:",
        ["Simple Format", "Individual Fields"],
        help="Simple format: user,ticker:$price:BUY/SELL:quantity (date is set automatically to today)"
    )
    
    if input_method == "Simple Format":
        st.subheader("Simple Trade String")
        trade_string = st.text_input(
            "Enter trade string:",
            placeholder="user123,AAPL:$150.50:BUY:100",
            help="Format: user,ticker:$price:BUY/SELL:quantity (date is set automatically to today)"
        )
        
        if st.button("Add Trade", key="simple_add"):
            if trade_string:
                try:
                    # Add today's date to the simplified format to make it compatible with TradeManager
                    today = datetime.now().strftime("%Y-%m-%d")
                    # Convert user,ticker:$price:BUY/SELL:quantity to user:YYYY-MM-DD,ticker:$price:BUY/SELL:quantity
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
                        
                except (ValueError, IndexError) as e:
                    st.error(f"❌ Error processing trade: {e}")
            else:
                st.warning("⚠️ Please enter a trade string")
    
    else:  # Individual Fields
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
                    # Create trade string in the expected format with today's date
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
    
    # Sample CSV download (always visible)
    # Create sample CSV data
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
    
    st.divider()
    
    # File upload
    uploaded_file = st.file_uploader(
        "Choose a CSV file",
        type="csv",
        help="Upload a CSV file containing trade data"
    )
    
    if uploaded_file is not None:
        try:
            # Read the CSV file
            df = pd.read_csv(uploaded_file)
            
            st.subheader("Preview of uploaded data:")
            st.dataframe(df.head(5))
            
            st.info(f"📊 Total rows in CSV: {len(df)}")
            
            # CSV format detection
            csv_format = st.radio(
                "CSV Format:",
                ["Complete Trade Data", "User Input Data (Missing Fields)"],
                help="""
                - Complete Trade Data: CSV with all trade fields (account_id, trade_date, ticker, etc.)
                - User Input Data (Missing Fields): CSV with only account_id, ticker, price, buy/sell, quantity
                """
            )
            
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("Process CSV", key="process_csv"):
                    try:
                        trades = []
                        
                        if csv_format == "Complete Trade Data":
                            # Use the existing method for complete trade data
                            # Save CSV temporarily and use existing method
                            temp_filename = "temp_upload.csv"
                            df.to_csv(temp_filename, index=False)
                            trades = trade_manager.get_trades_from_csv(temp_filename)
                            os.remove(temp_filename)
                        
                        elif csv_format == "User Input Data (Missing Fields)":
                            # Use the raw CSV conversion method
                            temp_filename = "temp_raw_upload.csv"
                            df.to_csv(temp_filename, index=False)
                            trades = TradeManager.create_trades_from_raw_csv(temp_filename)
                            os.remove(temp_filename)
                        
                        if trades:
                            success = trade_manager.write_trades(trades)
                            if success:
                                st.success(f"✅ Successfully processed and saved {len(trades)} trades!")
                                
                                # Show summary
                                st.subheader("Upload Summary:")
                                summary_df = pd.DataFrame([{
                                    "Account": trade.account_id,
                                    "Ticker": trade.ticker,
                                    "Price": f"${trade.price}",
                                    "Type": trade.trade_type.upper(),
                                    "Quantity": trade.quantity,
                                    "Date": trade.trade_date
                                } for trade in trades[:10]])  # Show first 10
                                
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