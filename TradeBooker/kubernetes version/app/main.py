#!/usr/bin/env python3
"""
Simple router for the trading system services.
Routes to the appropriate service based on the SERVICE_TYPE environment variable.
"""

import os
import sys

def main():
    service_type = os.getenv('SERVICE_TYPE', 'streamlit')
    
    if service_type == 'streamlit':
        # Run Streamlit app
        os.system('streamlit run streamlit_app.py --server.port=8501 --server.address=0.0.0.0')
    
    elif service_type == 'trade-manager':
        # Run the trade manager as a simple service
        print("🔧 Starting Trade Manager service...")
        try:
            from trade_manager_class import TradeManager
            # Initialize TradeManager with retry logic
            trade_manager = None
            max_retries = 5
            retry_count = 0
            
            while retry_count < max_retries and trade_manager is None:
                try:
                    trade_manager = TradeManager()
                    print(f"✅ Trade Manager initialized successfully on attempt {retry_count + 1}")
                    break
                except Exception as e:
                    retry_count += 1
                    print(f"❌ Failed to initialize Trade Manager (attempt {retry_count}/{max_retries}): {e}")
                    if retry_count < max_retries:
                        print("⏳ Retrying in 5 seconds...")
                        import time
                        time.sleep(5)
            
            if trade_manager is None:
                print("❌ Failed to initialize Trade Manager after all retries")
                sys.exit(1)
                
            print("🚀 Trade Manager service is running...")
            # Keep the service running
            import time
            while True:
                time.sleep(10)
                
        except Exception as e:
            print(f"❌ Critical error in Trade Manager service: {e}")
            sys.exit(1)
    
    elif service_type == 'market-data':
        # Run market data service
        print("📊 Starting Market Data service...")
        from market_data import main
        main()
        # Keep the service running
        import time
        while True:
            time.sleep(60)  # Sleep for 1 minute between iterations
    
    else:
        print(f"❌ Unknown SERVICE_TYPE: {service_type}")
        print("Valid options: streamlit, trade-manager, market-data")
        sys.exit(1)

if __name__ == '__main__':
    main()
