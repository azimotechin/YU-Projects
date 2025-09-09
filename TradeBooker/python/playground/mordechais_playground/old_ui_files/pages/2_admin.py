import streamlit as st
import sys
import os
import pandas as pd
import io

# Add the scripts directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'scripts')))

from TradeManager import TradeManager
from Trade import Trade

st.set_page_config(page_title="Admin Panel", layout="wide")
st.title("🛠️ Admin Panel")

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

# Create tabs for different admin functions
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
        # Connection status
        try:
            trade_manager.redis_client.ping()
            st.success("✅ Redis Connected")
        except:
            st.error("❌ Redis Disconnected")
    
    # Recent trades preview
    st.divider()
    st.subheader("Recent Trades Preview")
    
    try:
        all_trades = trade_manager.get_all_trades()
        if all_trades:
            # Convert to DataFrame for display
            recent_trades = all_trades[-10:]  # Last 10 trades
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
                    
                    # Show sample of generated trades
                    st.subheader("Sample of Generated Trades:")
                    sample_data = []
                    for trade in random_trades[:5]:  # Show first 5
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
    
    # Dangerous operations section
    st.subheader("⚠️ Dangerous Operations")
    st.warning("These operations cannot be undone. Use with caution!")
    
    # Check if user is in confirmation mode for clearing trades
    if st.session_state.get("confirm_clear", False):
        # Show prominent warning overlay
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
        # Normal view
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("�️ Clear All Trades", key="clear_all", type="secondary"):
                st.session_state["confirm_clear"] = True
                st.rerun()
        
        with col2:
            # Export functionality - direct download button
            try:
                all_trades = trade_manager.get_all_trades()
                if all_trades:
                    # Create CSV data
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

# Reset confirmation state when page loads
if "confirm_clear" not in st.session_state:
    st.session_state["confirm_clear"] = False