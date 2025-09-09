import streamlit as st
import redis
import requests
import json
import os
import logging
import pandas as pd
import sys
from datetime import datetime, date
from kubernetes import client, config # pyright: ignore[reportMissingImports]

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

# Add the current directory to sys.path for imports
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

try:
    from trade_manager_class import TradeManager
    from market_data import get_price, get_eod_price_range, get_historical_price
    from user_manager import UserManager
    from trade import Trade
except ImportError as e:
    st.error(f"Error importing modules: {e}")
    st.stop()

# App configuration
st.set_page_config(page_title="Trading System", page_icon="📈", layout="wide")

def login_page():
    """Display login page and handle role selection"""
    # Center the title using columns
    col_left, col_center, col_right = st.columns([1, 2, 1])
    with col_center:
        st.title("Trading System Demo")
    
    # Center the role selection
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("### Welcome to the Trading System")
        st.markdown("Please select your demo role:")
        
        # Role selection buttons
        col_user, col_admin = st.columns(2)
        
        with col_user:
            if st.button("User", use_container_width=True, type="primary"):
                st.session_state.authenticated = True
                st.session_state.user_role = "user"
                st.session_state.username = "Demo User"
                st.success("✅ Entering as User...")
                st.rerun()
        
        with col_admin:
            if st.button("Admin", use_container_width=True, type="secondary"):
                st.session_state.authenticated = True
                st.session_state.user_role = "admin"
                st.session_state.username = "Demo Admin"
                st.success("✅ Entering as Admin...")
                st.rerun()

def logout():
    """Handle user logout"""
    for key in ['authenticated', 'user_role', 'username']:
        if key in st.session_state:
            del st.session_state[key]
    st.rerun()

def get_redis_connection():
    redis_host = os.getenv('REDIS_HOST', 'redis-primary')
    return redis.Redis(
        host=redis_host, 
        port=6379, 
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=5,
        retry_on_timeout=True
    )

def get_kubernetes_client():
    """Initialize Kubernetes client - works both in-cluster and locally"""
    try:
        # Try in-cluster config first (when running in Kubernetes)
        config.load_incluster_config()
    except:
        try:
            # Fall back to local kubeconfig (when running locally)
            config.load_kube_config()
        except:
            return None
    return client.AppsV1Api(), client.CoreV1Api()

def get_pod_info(apps_v1, core_v1, namespace="trading-system"):
    """Get detailed information about all pods in the trading system"""
    try:
        # Get all StatefulSets
        statefulsets = apps_v1.list_namespaced_stateful_set(namespace=namespace)
        
        pod_info = {}
        
        for ss in statefulsets.items:
            ss_name = ss.metadata.name
            pod_info[ss_name] = {
                'name': ss_name,
                'replicas': ss.spec.replicas,
                'ready_replicas': ss.status.ready_replicas or 0,
                'pods': []
            }
            
            # Get pods for this StatefulSet
            pods = core_v1.list_namespaced_pod(
                namespace=namespace,
                label_selector=f"app={ss_name}"
            )
            
            for pod in pods.items:
                pod_status = {
                    'name': pod.metadata.name,
                    'phase': pod.status.phase,
                    'ready': False,
                    'restart_count': 0,
                    'node': pod.spec.node_name,
                    'created': pod.metadata.creation_timestamp
                }
                
                # Check if pod is ready
                if pod.status.conditions:
                    for condition in pod.status.conditions:
                        if condition.type == "Ready":
                            pod_status['ready'] = condition.status == "True"
                
                # Get restart count
                if pod.status.container_statuses:
                    pod_status['restart_count'] = sum(
                        cs.restart_count for cs in pod.status.container_statuses
                    )
                
                pod_info[ss_name]['pods'].append(pod_status)
        
        return pod_info
    except Exception as e:
        logger.error(f"Error getting pod info: {e}")
        return None

def fetch_trades(r, filters):
    trades = []
    for key in r.scan_iter(match="*:*:*", count=5000):
        try:
            hash_data = r.hgetall(key)
            if hash_data:
                trade = Trade.from_redis_data(key, hash_data)
                # Apply filters
                if (trade.account_id in filters["accounts"] and
                    filters["start_date"] <= pd.to_datetime(trade.trade_date) <= filters["end_date"]):
                    trades.append(trade)
        except Exception as e:
            st.error(f"Error processing trade {key}: {e}")
    return trades

@st.fragment(run_every="5s")
def display_trade_table(r):
    trades = fetch_trades(r, st.session_state.get("filters", {}))
    if trades:
        # Convert to DataFrame for display
        trade_data = []
        for trade in trades:
            trade_data.append({
                'Account': trade.account_id,
                'Date': trade.trade_date,
                'Time': trade.trade_time,
                'Ticker': trade.ticker,
                'Type': trade.trade_type.upper(),
                'Quantity': trade.quantity,
                'Price': f"${trade.price:.2f}",
                'Total': f"${trade.price * trade.quantity:.2f}"
            })
        
        df = pd.DataFrame(trade_data)
        st.dataframe(df, use_container_width=True)
        st.write(f"**Total Trades:** {len(trades)}")
    else:
        st.write("No trades found matching the current filters.")

def set_trade_filters_main_tab(r):
    # Get all accounts for options
    accounts = set()
    for key in r.scan_iter(match="*:*:*", count=5000):
        try:
            parts = key.split(':')
            if len(parts) >= 1:
                accounts.add(parts[0])
        except:
            continue
    accounts = sorted(list(accounts))

    # Filters in main tab
    selected_accounts = st.multiselect(
        "Select Accounts",
        options=accounts,
        default=accounts,
        key="account_multiselect_main"
    )

    start_date = st.date_input("Start Date", value=pd.to_datetime('1900-01-01'), min_value=date(1900, 1, 1), max_value=date.today(), key="start_date_main")
    end_date = st.date_input("End Date", value=pd.to_datetime('today'), min_value=start_date, max_value=date.today(), key="end_date_main")

    # Save filters to session state
    st.session_state["filters"] = {
        "accounts": selected_accounts if selected_accounts else accounts,
        "start_date": pd.to_datetime(start_date),
        "end_date": pd.to_datetime(end_date)
    }

def admin_tab(trade_manager, user_manager):

    st.title("🛠️ Admin Panel")
    # Global refresh button
    col_refresh, _ = st.columns([1, 5])
    with col_refresh:
        if st.button("🔄 Refresh", key="admin_global_refresh", use_container_width=True):
            st.rerun()

    tab1, tab2, tab3, tab4 = st.tabs(["Kubernetes Management", "Data Management", "Manual Trade Input", "Generate Test Data"])


    # --- Kubernetes Management Tab ---
    with tab1:
        st.subheader("☸️ Kubernetes Management")
        k8s_clients = get_kubernetes_client()
        if k8s_clients is None:
            st.error("❌ Unable to connect to Kubernetes cluster")
            st.info("This feature requires running inside a Kubernetes cluster or having kubectl configured locally.")
            return
        apps_v1, core_v1 = k8s_clients
        pod_info = get_pod_info(apps_v1, core_v1)
        if pod_info is None:
            st.error("❌ Failed to retrieve pod information")
            return
        st.subheader("📊 StatefulSet Status")
        col1, col2, col3, col4 = st.columns(4)
        # Always expect 2 replicas per StatefulSet and 8 pods total
        expected_replicas_per_set = 2
        expected_total_pods = 8
        total_statefulsets = len(pod_info)
        ready_pods = sum(1 for ss in pod_info.values() for pod in ss['pods'] if pod['ready'])
        # Count healthy statefulsets as those with 2/2 ready
        healthy_statefulsets = sum(1 for ss in pod_info.values() if ss['ready_replicas'] == 2)
        with col1:
            st.metric("StatefulSets", f"{healthy_statefulsets}/{total_statefulsets}")
        with col2:
            st.metric("Ready Pods", f"{ready_pods}/{expected_total_pods}")
        with col3:
            restart_count = sum(pod['restart_count'] for ss in pod_info.values() for pod in ss['pods'])
            st.metric("Total Restarts", restart_count)
        with col4:
            health_percentage = (ready_pods / expected_total_pods * 100) if expected_total_pods > 0 else 0
            st.metric("Health %", f"{health_percentage:.1f}%")
        st.subheader("🔍 Pod Details")
        for ss_name, ss_info in pod_info.items():
            with st.expander(f"📦 {ss_name.title()} StatefulSet", expanded=True):
                col1, col2, col3 = st.columns([2, 2, 2])
                with col1:
                    # Always show /2 for expected replicas
                    status_color = "🟢" if ss_info['ready_replicas'] == 2 else "🔴"
                    st.write(f"**Status:** {status_color} {ss_info['ready_replicas']}/2 replicas ready")
                with col2:
                    st.write(f"**Service:** {ss_name.replace('-', ' ').title()}")
                with col3:
                    restart_btn = st.button(f"🔄 Restart", key=f"restart_{ss_name}")
                    shutdown_btn = st.button(f"🛑 Shutdown", key=f"shutdown_{ss_name}")
                    scaleup_btn = False
                    if ss_info['replicas'] == 0:
                        scaleup_btn = st.button(f"⬆️ Spin Up", key=f"scaleup_{ss_name}")
                    if restart_btn:
                        try:
                            patch = {"spec": {"template": {"metadata": {"annotations": {"kubectl.kubernetes.io/restartedAt": datetime.utcnow().isoformat()}}}}}
                            apps_v1.patch_namespaced_stateful_set(name=ss_name, namespace="trading-system", body=patch)
                            st.success(f"Restart triggered for {ss_name}!")
                            st.rerun()
                        except Exception as e:
                            if "forbidden" in str(e).lower() or "403" in str(e):
                                st.error(f"Failed to restart {ss_name}: insufficient Kubernetes permissions.\n\nYour service account may lack 'patch' access to StatefulSets in this namespace.\n\nDetails: {e}")
                            else:
                                st.error(f"Failed to restart {ss_name}: {e}")
                    if shutdown_btn:
                        try:
                            scale = {"spec": {"replicas": 0}}
                            apps_v1.patch_namespaced_stateful_set_scale(name=ss_name, namespace="trading-system", body=scale)
                            st.success(f"Shutdown triggered for {ss_name} (scaled to 0 replicas)!")
                            st.rerun()
                        except Exception as e:
                            if "forbidden" in str(e).lower() or "403" in str(e):
                                st.error(f"Failed to shutdown {ss_name}: insufficient Kubernetes permissions.\n\nYour service account may lack 'patch' access to StatefulSets/scale in this namespace.\n\nDetails: {e}")
                            else:
                                st.error(f"Failed to shutdown {ss_name}: {e}")
                    if scaleup_btn:
                        try:
                            default_replicas = int(os.getenv(f"{ss_name.upper()}_REPLICAS", "2"))
                            scale = {"spec": {"replicas": default_replicas}}
                            apps_v1.patch_namespaced_stateful_set_scale(name=ss_name, namespace="trading-system", body=scale)
                            st.success(f"Scale up triggered for {ss_name} (to {default_replicas} replicas)!")
                            st.rerun()
                        except Exception as e:
                            if "forbidden" in str(e).lower() or "403" in str(e):
                                st.error(f"Failed to scale up {ss_name}: insufficient Kubernetes permissions.\n\nYour service account may lack 'patch' access to StatefulSets/scale in this namespace.\n\nDetails: {e}")
                            else:
                                st.error(f"Failed to scale up {ss_name}: {e}")
                if ss_info['pods']:
                    pod_data = []
                    for pod in ss_info['pods']:
                        status_emoji = "🟢" if pod['ready'] else "🔴"
                        pod_data.append({
                            "Pod Name": pod['name'],
                            "Status": f"{status_emoji} {pod['phase']}",
                            "Ready": "✅" if pod['ready'] else "❌",
                            "Node": pod['node'] or "Unknown",
                            "Age": str(datetime.now(pod['created'].tzinfo) - pod['created']).split('.')[0] if pod['created'] else "Unknown"
                        })
                    st.dataframe(pd.DataFrame(pod_data), use_container_width=True)
                else:
                    st.warning("No pods found for this StatefulSet")

    # --- Data Management Tab ---
    with tab2:
        st.subheader("Data Management")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Clear All Trades", type="primary"):
                if st.session_state.get("confirm_clear", False):
                    try:
                        trade_manager.clear_all_trades()
                        st.success("✅ All trades cleared!")
                        st.session_state["confirm_clear"] = False
                    except Exception as e:
                        st.error(f"Error clearing trades: {e}")
                else:
                    st.session_state["confirm_clear"] = True
                    st.warning("⚠️ Click again to confirm deletion")
        if "confirm_clear" not in st.session_state:
            st.session_state["confirm_clear"] = False
        st.markdown("---")
        st.subheader("🔍 View Trades for a Specific User")
        accounts = set()
        for key in trade_manager.redis_client.scan_iter(match="*:*:*", count=5000):
            try:
                parts = key.split(":")
                if len(parts) >= 1:
                    accounts.add(parts[0])
            except:
                continue
        accounts = sorted(list(accounts))
        selected_account = st.selectbox("Select Account to View Trades", options=accounts, key="admin_account_select")
        start_date = st.date_input("Start Date", value=pd.to_datetime('1900-01-01'), min_value=date(1900, 1, 1), max_value=date.today(), key="admin_start_date")
        end_date = st.date_input("End Date", value=pd.to_datetime('today'), min_value=start_date, max_value=date.today(), key="admin_end_date")
        filters = {
            "accounts": [selected_account],
            "start_date": pd.to_datetime(start_date),
            "end_date": pd.to_datetime(end_date)
        }
        trades = fetch_trades(trade_manager.redis_client, filters)
        st.subheader(f"Trades for Account: {selected_account}")
        if trades:
            trade_data = []
            for trade in trades:
                trade_data.append({
                    'Account': trade.account_id,
                    'Date': trade.trade_date,
                    'Time': trade.trade_time,
                    'Ticker': trade.ticker,
                    'Type': trade.trade_type.upper(),
                    'Quantity': trade.quantity,
                    'Price': f"${trade.price:.2f}",
                    'Total': f"${trade.price * trade.quantity:.2f}"
                })
            df = pd.DataFrame(trade_data)
            st.dataframe(df, use_container_width=True)
            st.write(f"**Total Trades:** {len(trades)}")
        else:
            st.write("No trades found for this account and date range.")

    # --- Manual Trade Input Tab ---
    with tab3:
        st.subheader("Manual Trade Input")
        with st.form("admin_trade_form"):
            col1, col2 = st.columns(2)
            with col1:
                account_id = st.text_input("Account ID", key="admin_account_id")
                ticker = st.text_input("Ticker Symbol", key="admin_ticker").upper()
                price = st.number_input("Price", min_value=0.01, value=100.0, step=0.01, key="admin_price")
            with col2:
                trade_type = st.selectbox("Trade Type", ["buy", "sell"], key="admin_trade_type")
                quantity = st.number_input("Quantity", min_value=1, value=1, step=1, key="admin_quantity")
                action_type = st.selectbox("Action Type", ["trade", "placeholder"], key="admin_action_type")
            submitted = st.form_submit_button("Execute Trade (Admin)")
            if submitted:
                if account_id and ticker:
                    try:
                        trade = Trade(
                            account_id=account_id,
                            ticker=ticker,
                            price=price,
                            trade_type=trade_type,
                            quantity=quantity,
                            action_type=action_type
                        )
                        success = trade_manager.write_trade(trade)
                        if success:
                            st.success("✅ Trade executed successfully!")
                        else:
                            st.error("❌ Failed to execute trade")
                    except Exception as e:
                        st.error(f"Error creating trade: {e}")
                else:
                    st.error("Please fill in all required fields")

    # --- Generate Test Data Tab ---
    with tab4:
        st.subheader("Generate Test Trades")
        col1, col2 = st.columns(2)
        with col1:
            num_trades = st.number_input("Number of trades to generate", min_value=1, max_value=10000, value=100)
        with col2:
            if st.button("Generate Random Trades"):
                with st.spinner("Generating trades..."):
                    try:
                        trades = TradeManager.create_random_trades(num_trades)
                        success = trade_manager.write_trades_with_benchmarking(trades)
                        if success:
                            st.success(f"✅ Successfully generated {num_trades} trades!")
                        else:
                            st.error("❌ Failed to generate trades")
                    except Exception as e:
                        st.error(f"Error generating trades: {e}")
        
def user_input_tab(trade_manager):
    st.title("📝 Trade Input")

    tab1, = st.tabs(["Manual Trade Input"])

    with tab1:
        st.subheader("Create New Trade")
        
        with st.form("trade_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                account_id = st.text_input("Account ID")
                ticker = st.text_input("Ticker Symbol").upper()
                price = st.number_input("Price", min_value=0.01, value=100.0, step=0.01)
            
            with col2:
                trade_type = st.selectbox("Trade Type", ["buy", "sell"])
                quantity = st.number_input("Quantity", min_value=1, value=1, step=1)
                action_type = st.selectbox("Action Type", ["trade", "placeholder"])
            
            submitted = st.form_submit_button("Execute Trade")
            
            if submitted:
                if account_id and ticker:
                    try:
                        trade = Trade(
                            account_id=account_id,
                            ticker=ticker,
                            price=price,
                            trade_type=trade_type,
                            quantity=quantity,
                            action_type=action_type
                        )
                        
                        success = trade_manager.write_trade(trade)
                        if success:
                            st.success("✅ Trade executed successfully!")
                        else:
                            st.error("❌ Failed to execute trade")
                    except Exception as e:
                        st.error(f"Error creating trade: {e}")
                else:
                    st.error("Please fill in all required fields")

def show_kubernetes_management_only():
    st.subheader("☸️ Kubernetes Management")
    
    # Try to get Kubernetes client
    k8s_clients = get_kubernetes_client()
    
    if k8s_clients is None:
        st.error("❌ Unable to connect to Kubernetes cluster")
        st.info("This feature requires running inside a Kubernetes cluster or having kubectl configured locally.")
        return
    
    apps_v1, core_v1 = k8s_clients
    
    # Refresh button
    if st.button("🔄 Refresh Pod Status"):
        st.rerun()
    
    # Get pod information
    pod_info = get_pod_info(apps_v1, core_v1)
    
    if pod_info is None:
        st.error("❌ Failed to retrieve pod information")
        return
    
    # Display StatefulSet status
    st.subheader("📊 StatefulSet Status")
    
    # Create metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    total_statefulsets = len(pod_info)
    healthy_statefulsets = sum(1 for ss in pod_info.values() if ss['ready_replicas'] == ss['replicas'])
    total_pods = sum(len(ss['pods']) for ss in pod_info.values())
    ready_pods = sum(1 for ss in pod_info.values() for pod in ss['pods'] if pod['ready'])
    
    with col1:
        st.metric("StatefulSets", f"{healthy_statefulsets}/{total_statefulsets}")
    with col2:
        st.metric("Ready Pods", f"{ready_pods}/{total_pods}")
    with col3:
        restart_count = sum(pod['restart_count'] for ss in pod_info.values() for pod in ss['pods'])
        st.metric("Total Restarts", restart_count)
    with col4:
        health_percentage = (ready_pods / total_pods * 100) if total_pods > 0 else 0
        st.metric("Health %", f"{health_percentage:.1f}%")
    
    # Display detailed pod information
    st.subheader("🔍 Pod Details")
    
    for ss_name, ss_info in pod_info.items():
        with st.expander(f"📦 {ss_name.title()} StatefulSet", expanded=True):
            # StatefulSet overview
            col1, col2 = st.columns(2)
            with col1:
                status_color = "🟢" if ss_info['ready_replicas'] == ss_info['replicas'] else "🔴"
                st.write(f"**Status:** {status_color} {ss_info['ready_replicas']}/{ss_info['replicas']} replicas ready")
                
                # Special highlight for Redis status
                if ss_name == "redis":
                    if ss_info['ready_replicas'] == 0:
                        st.error("🔴 Redis is completely down - this is why trading features are offline")
                    elif ss_info['ready_replicas'] < ss_info['replicas']:
                        st.warning("🟡 Redis is partially down - some instability may occur")
                    else:
                        st.success("🟢 Redis is healthy - trading features should recover soon")
                        
            with col2:
                st.write(f"**Service:** {ss_name.replace('-', ' ').title()}")
            
            # Pod details table
            if ss_info['pods']:
                pod_data = []
                for pod in ss_info['pods']:
                    status_emoji = "🟢" if pod['ready'] else "🔴"
                    pod_data.append({
                        "Pod Name": pod['name'],
                        "Status": f"{status_emoji} {pod['phase']}",
                        "Ready": "✅" if pod['ready'] else "❌",
                        "Node": pod['node'] or "Unknown",
                        "Age": str(datetime.now(pod['created'].tzinfo) - pod['created']).split('.')[0] if pod['created'] else "Unknown"
                    })
                
                st.dataframe(pd.DataFrame(pod_data), use_container_width=True)
            else:
                st.warning("No pods found for this StatefulSet")
    
    # Connection retry section
    st.subheader("🔄 Redis Connection Recovery")
    if st.button("🔄 Retry Redis Connection", type="primary"):
        st.rerun()

def main():
    # Check if user is authenticated
    if not st.session_state.get('authenticated', False):
        login_page()
        return
    
    # Show header with logout option
    col1, col2 = st.columns([3, 1])
    with col1:
        user_role = st.session_state.get('user_role', 'user')
        username = st.session_state.get('username', 'Unknown')
        role_emoji = "🛠️" if user_role == "admin" else "👤"
        st.title(f"📈 Trading System - {role_emoji} {user_role.title()}: {username}")
    with col2:
        if st.button("🚪 Logout", use_container_width=True):
            logout()
    
    # Initialize Redis connection with graceful fallback
    redis_connected = False
    redis_status = "❌ Disconnected"
    r = None
    trade_manager = None
    user_manager = None
    
    try:
        r = get_redis_connection()
        r.ping()  # Test the connection
        redis_status = "✅ Connected"
        redis_connected = True
        
        # Initialize managers only if Redis is available
        trade_manager = TradeManager()
        user_manager = UserManager(r)
        
    except Exception as e:
        st.warning(f"⚠️ Redis connection failed: {e}")
        st.info("🔧 Running in limited mode - Kubernetes management and system monitoring still available")
        redis_status = f"❌ Failed: {str(e)[:50]}..."
    
    # Sidebar
    st.sidebar.title("Navigation")
    st.sidebar.write(f"**Redis Status:** {redis_status}")

    # Show Docker image info in sidebar
    image_tag = os.getenv("DOCKER_IMAGE", None)
    if not image_tag:
        # Try to get from Helm values file if mounted, or fallback to default
        image_tag = os.getenv("IMAGE_TAG", None)
    if not image_tag:
        # Fallback: try to parse from app config or hardcode if known
        image_tag = "error displaying docker image"  # Update if needed
    st.sidebar.info(f"**Running Docker Image:** `{image_tag}`")

    if not redis_connected:
        st.sidebar.warning("⚠️ Limited functionality - Redis unavailable")
    
    # Show different tabs based on user role
    user_role = st.session_state.get('user_role', 'user')
    
    if user_role == "admin":
        # Admin sees only the admin panel
        if redis_connected:
            admin_tab(trade_manager, user_manager)
        else:
            # Show limited admin functionality focused on system monitoring
            st.header("🛠️ System Monitor")
            st.info("🔧 System monitoring is available even when Redis is offline")
            show_kubernetes_management_only()
    else:
        # Regular users see trading dashboard and trade input (no admin panel)
        if redis_connected:
            tab1, tab2 = st.tabs(["📊 Trading Dashboard", "📝 Trade Input"])
        else:
            tab1, tab2 = st.tabs(["⚠️ Trading Dashboard (Offline)", "⚠️ Trade Input (Offline)"])
        
        with tab1:
            if redis_connected:
                st.header("Trading Dashboard")
                
                # Initialize filters if not in session state
                if "filters" not in st.session_state:
                    st.session_state["filters"] = {
                        "accounts": [],
                        "start_date": pd.to_datetime('1900-01-01'),
                        "end_date": pd.to_datetime('today')
                    }
                
                # Filters
                with st.expander("🔍 Filters", expanded=True):
                    set_trade_filters_main_tab(r)
                
                # Trade table
                st.subheader("Recent Trades")
                display_trade_table(r)
            else:
                st.header("⚠️ Trading Dashboard - Offline Mode")
                st.error("❌ Trading dashboard is unavailable - Redis connection required")
                st.info("📊 Contact your system administrator to check Redis pod status")
        
        with tab2:
            if redis_connected:
                user_input_tab(trade_manager)
            else:
                st.header("⚠️ Trade Input - Offline Mode")
                st.error("❌ Trade input is unavailable - Redis connection required")
                st.info("📊 Contact your system administrator to check Redis pod status")

if __name__ == "__main__":
    main()
