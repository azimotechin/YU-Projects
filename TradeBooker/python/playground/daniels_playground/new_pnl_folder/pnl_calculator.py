import redis
from redis.sentinel import Sentinel
import uuid
import logging
from Trade import Trade
import json
import market_data
from datetime import datetime
import sys
import string

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def valid_date(date_string: str) -> bool:
    """Helper to validate YYYY-MM-DD date strings."""
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False

class PnLCalculator:
    def __init__(self, shard_range=None, sentinels=None, service_name="mymaster", redis_db=0):
        """
        Initialize PnL Calculator with keyspace notifications and sharding.
        
        Args:
            shard_range: Tuple of (start_char, end_char) for alphabetical sharding
                        e.g., ('a', 'h') means this instance handles accounts starting with a-h
                        If None, processes all accounts (no sharding)
        """
        # Sharding configuration
        self.shard_range = shard_range
        self.redis_db = redis_db
        if shard_range:
            logger.info(f"PnL Calculator initialized with shard range: {shard_range[0].upper()}-{shard_range[1].upper()}")
        else:
            logger.info("PnL Calculator initialized without sharding (processes all accounts)")

        # Redis connection setup
        # if sentinels is None:
        #     sentinels = [("sentinel1", 26379), ("sentinel2", 26379), ("sentinel3", 26379)]
        # sentinel = Sentinel(sentinels, socket_timeout=None, decode_responses=True)
        # self.redis = sentinel.master_for(service_name, socket_timeout=None, decode_responses=True)
        
        self.redis = redis.Redis(host='localhost', port=6379, decode_responses=True)
        
        # Set up keyspace notifications with targeted patterns
        self.pubsub = self.redis.pubsub()
        self._subscribe_to_shard_patterns()

        # Define Redis hash names where we'll store our pnl data
        self.lots_key_prefix = "lots:"  # Will store: lots:alice/AAPL → JSON list of lots
        self.unrealized_pnl_by_position_hash = "unrealized_pnl_by_position"
        self.realized_pnl_by_position_hash = "realized_pnl_by_position"

        logger.info(f"Will store FIFO lots with key prefix: '{self.lots_key_prefix}'")
        logger.info(f"Will store realized PnL in Redis hash: '{self.realized_pnl_by_position_hash}'")

    def _subscribe_to_shard_patterns(self):
        """
        Subscribe to keyspace notification patterns based on shard range.
        This optimizes by only listening to relevant keys instead of all keys.
        """
        if not self.shard_range:
            # No sharding, listen to all patterns
            notification_channel = f"__keyspace@{self.redis_db}__:*"
            self.pubsub.psubscribe(notification_channel)
            logger.info(f"Subscribed to all Redis keyspace notifications: {notification_channel}")
        else:
            # Create patterns for each letter in the shard range
            start_char, end_char = self.shard_range
            patterns = []
            
            # Generate all letters from start_char to end_char
            start_ord = ord(start_char.lower())
            end_ord = ord(end_char.lower())
            
            for char_ord in range(start_ord, end_ord + 1):
                char = chr(char_ord)
                pattern = f"__keyspace@{self.redis_db}__:{char}*"
                patterns.append(pattern)
                self.pubsub.psubscribe(pattern)
            
            logger.info(f"Subscribed to {len(patterns)} targeted keyspace notification patterns:")
            for pattern in patterns:
                logger.info(f"  - {pattern}")

    def _should_process_account(self, account_id: str) -> bool:
        """
        Check if this instance should process trades for the given account based on sharding.
        
        Note: This method serves as a safety net since we're filtering at the subscription level.
        It protects against:
        - Non-trade keys that match our subscription pattern
        - Edge cases with complex account IDs
        - Future changes to key naming conventions
        
        Args:
            account_id: The account identifier
            
        Returns:
            True if this instance should process this account, False otherwise
        """
        if not self.shard_range:
            return True  # No sharding, process all accounts
        
        first_char = account_id.lower()[0]
        start_char, end_char = self.shard_range
        
        should_process = start_char <= first_char <= end_char
        
        # Log when this safety check actually filters something out
        # (This would indicate either edge cases or subscription issues)
        if not should_process:
            logger.warning(f"🛡️ Safety check filtered account '{account_id}' - not in shard range {start_char}-{end_char}")
        
        return should_process

    def _get_lots_key(self, account_id: str, ticker: str) -> str:
        """Generate a redis key in the following format 'lots:alice/AAPL'"""
        return f"{self.lots_key_prefix}{account_id}/{ticker}"
        
    def _get_lots(self, account_id: str, ticker: str) -> list:
        """Returns a list of all lots(trades) for the user and ticker"""
        lots_key = self._get_lots_key(account_id, ticker)
        lots_json = self.redis.get(lots_key)

        if lots_json:
            return json.loads(lots_json)
        else:
            return []  # No lots exist yet
        
    def _save_lots(self, account_id: str, ticker: str, lots: list):
        """Saves lots and stores in into json for the lot key"""
        lots_key = self._get_lots_key(account_id, ticker)
        lots_json = json.dumps(lots)

        self.redis.set(lots_key, lots_json)
        logger.info(f"Saved {len(lots)} lots for {account_id}/{ticker}")

    def _process_trade_from_key(self, key: str):
        """
        Process a trade from a Redis key (similar to portfolio aggregator).
        
        Args:
            key: Redis key in format 'account:date:uuid'
        """
        try:
            # Get the trade data from the hash
            hash_data = self.redis.hgetall(key)
            if not hash_data:
                logger.warning(f"⚠️ No data found for key {key}")
                return

            # Only process if full hash exists (avoiding processing trade when it is created 
            # and when individual fields are inputted, multiple of which may send hset notifications)
            if len(hash_data) < 6:
                logger.debug(f"⏩ Partial trade ignored: {key}")
                return

            # Extract account from key
            account_id = key.split(':')[0]
            
            # Double-check sharding (defensive programming - should already be filtered by subscription)
            if not self._should_process_account(account_id):
                logger.debug(f"⏩ Trade ignored due to sharding: {account_id} not in range {self.shard_range}")
                return

            # Create Trade object from hash data
            ticker = hash_data['ticker']
            price = float(hash_data['price'])
            trade_type = hash_data['type'].lower()
            quantity = int(hash_data['quantity'])
            trade_time = hash_data['trade_time']

            # Create a Trade object (assuming Trade class can be created this way)
            trade = Trade(
                account_id=account_id,
                ticker=ticker,
                price=price,
                trade_type=trade_type,
                quantity=quantity,
                trade_time=trade_time
            )
            
            # Process the trade using existing FIFO logic
            self.process_trade_fifo(trade)
            
            logger.info(f"📈 Processed trade from key: {key}")
                
        except Exception as e:
            logger.error(f"❌ Failed to process trade from key {key}: {e}")

    def listen_and_calculate(self):
        """
        Listen for Redis keyspace notifications and calculate PnL.
        Now only processes notifications from subscribed patterns (optimized for sharding).
        """
        if self.shard_range:
            range_desc = f"{self.shard_range[0].upper()}-{self.shard_range[1].upper()}"
            logger.info(f"🔄 Starting PnL Calculator - listening for trade notifications in shard range: {range_desc}")
        else:
            logger.info("🔄 Starting PnL Calculator - listening for all trade keyspace notifications")

        for message in self.pubsub.listen():
            if message['type'] != 'pmessage':
                continue

            key_event = message['channel']  # e.g., '__keyspace@0__:abraham:2024-04-23:uuid'
            keyname = key_event.split(":", 1)[-1]  # remove '__keyspace@0__:' prefix
            event_type = message['data']  # e.g., 'hset'

            # Only act on HSET events
            if event_type != 'hset':
                continue

            # Check if this is a trade key (format: account:date:uuid)
            parts = keyname.split(':')
            if len(parts) == 3 and valid_date(parts[1]):
                self._process_trade_from_key(keyname)
            else:
                logger.debug(f"⏩ Skipping non-trade key: {keyname}")

    def process_trade_fifo(self, trade: Trade):
        """Function that processes according to whether the trade is a buy or sell"""
        if trade.trade_type == "buy":
            self._process_buy_fifo(trade)
        elif trade.trade_type == "sell":
            self._process_sell_fifo(trade)
        else:
            logger.error(f"Unknown trade type {trade.trade_type}")

    def _process_buy_fifo(self, trade: Trade):
        """Process buy trades by adding to lots"""
        existing_lots = self._get_lots(trade.account_id, trade.ticker)

        new_lot = {
            "price": trade.price,
            "quantity": trade.quantity,
            "date": trade.trade_date,
            "time": trade.trade_time
        }
        existing_lots.append(new_lot)

        self._save_lots(trade.account_id, trade.ticker, existing_lots)

        logger.info(f"BUY - Added lot of {trade.quantity} shares @ ${trade.price}")
        logger.info(f"{trade.account_id}/{trade.ticker} now has {len(existing_lots)} lots")
    
    def _process_sell_fifo(self, trade: Trade):
        """
        Process sell trades using FIFO methodology.
        Removes the trade quantity of shares from the lots, since it is FIFO it removes the oldest added lots.
        If sell quantity is higher than the first lot, this method will consume multiple lots and calculate the realized PnL accordingly.
        Partial consumption of lots is also permitted.
        """
        existing_lots = self._get_lots(trade.account_id, trade.ticker)

        if not existing_lots:
            logger.warning(f"SELL - No lots to sell for {trade.account_id}/{trade.ticker} - cannot calculate PnL")
            return
        
        remaining_to_sell = trade.quantity
        total_realized_pnl = 0.0

        logger.info(f"SELL - Need to sell {remaining_to_sell} shares of {trade.ticker} from a total of {len(existing_lots)}")

        lots_to_remove = []

        for i, lot in enumerate(existing_lots):
            if remaining_to_sell <= 0:
                break
            
            if lot["quantity"] <= remaining_to_sell:
                # Consume entire lot
                realized_pnl_from_lot = (trade.price - lot["price"]) * lot["quantity"]
                total_realized_pnl += realized_pnl_from_lot
                remaining_to_sell -= lot["quantity"]
                lots_to_remove.append(i)

                logger.info(f"Consumed entire lot: {lot['quantity']} @ ${lot['price']} → PnL: ${realized_pnl_from_lot:.2f}")
            else:
                # Partial consumption of the current lot
                realized_pnl_from_lot = (trade.price - lot["price"]) * remaining_to_sell
                total_realized_pnl += realized_pnl_from_lot
                lot["quantity"] -= remaining_to_sell
                logger.info(f"Partially consumed lot: {remaining_to_sell} from {lot['quantity'] + remaining_to_sell} @ ${lot['price']} → PnL: ${realized_pnl_from_lot:.2f}")
                logger.info(f"Remaining in lot: {lot['quantity']} shares")
                remaining_to_sell = 0

        # Remove fully consumed lots
        existing_lots = [lot for i, lot in enumerate(existing_lots) if i not in lots_to_remove]

        # Save updated lots
        self._save_lots(trade.account_id, trade.ticker, existing_lots)

        # Update realized PnL for this account
        if total_realized_pnl != 0:
            position_key = f"{trade.account_id}/{trade.ticker}"
            self.redis.hincrbyfloat(self.realized_pnl_by_position_hash, position_key, total_realized_pnl)
            logger.info(f"Realized PnL for {position_key}: ${total_realized_pnl:.2f}")
            self.store_and_calculate_unrealized_pnl_position(trade.account_id, trade.ticker)
                
        if remaining_to_sell > 0:
            logger.warning(f"Could not sell {remaining_to_sell} shares - insufficient lots!")

    # UNREALIZED PNL FUNCTIONS BELOW
    def get_live_price(self, ticker: str) -> float:
        """Get current market price using market_data.py module"""
        try:
            price_per_share = market_data.get_price(ticker, 1)
            logger.debug(f"  Got live price for {ticker}: ${price_per_share:.2f}")
            return price_per_share
            
        except ValueError as e:
            logger.warning(f"  Failed to get live price for {ticker}: {e}")
            return None
        except Exception as e:
            logger.error(f"  Unexpected error getting price for {ticker}: {e}")
            return None
    
    def store_and_calculate_unrealized_pnl_position(self, account_id: str, ticker: str) -> bool:
        """
        Calculate and store unrealized PnL for a specific position.
        Returns True if the value changed, False otherwise.
        """
        position_key = f"{account_id}/{ticker}"

        # Get the old value before calculating the new one
        try:
            old_pnl = float(self.redis.hget(self.unrealized_pnl_by_position_hash, position_key) or 0.0)
        except (ValueError, TypeError):
            old_pnl = 0.0

        # Calculate the new value
        new_pnl = self.calculate_unrealized_pnl_single(account_id, ticker)

        # Compare old and new values, rounded to the nearest cent
        if round(new_pnl, 2) != round(old_pnl, 2):
            self.redis.hset(self.unrealized_pnl_by_position_hash, position_key, new_pnl)
            logger.debug(f"   Stored updated unrealized PnL for {position_key}: ${new_pnl:.2f}")
            return True
        else:
            # If the value hasn't changed, no need to write to Redis
            logger.debug(f"   Unrealized PnL for {position_key} is unchanged. Skipping update.")
            return False
    
    def calculate_unrealized_pnl_single(self, account_id: str, ticker: str) -> float:
        """Calculate unrealized PnL for a single position"""
        # Get current lots for this position
        lots = self._get_lots(account_id, ticker)
        
        if not lots:
            logger.debug(f"No lots found for {account_id}/{ticker}")
            return 0.0
        
        # Get live market price
        live_price = self.get_live_price(ticker)
        if live_price is None:
            logger.warning(f" Cannot calculate unrealized PnL for {account_id}/{ticker} - no live price")
            return 0.0
        
        # Calculate unrealized PnL for each lot
        total_unrealized_pnl = 0.0
        total_shares = 0
        total_cost = 0.0
        
        for lot in lots:
            lot_quantity = lot["quantity"]
            lot_cost_basis = lot["price"]
            
            # PnL for this lot: (market_price - cost_basis) × quantity
            lot_unrealized_pnl = (live_price - lot_cost_basis) * lot_quantity
            total_unrealized_pnl += lot_unrealized_pnl
            
            # Track totals for logging
            total_shares += lot_quantity
            total_cost += lot_cost_basis * lot_quantity
        
        # Calculate weighted average cost basis for logging
        avg_cost_basis = total_cost / total_shares if total_shares > 0 else 0
        
        logger.info(f"   Unrealized PnL for {account_id}/{ticker}:")
        logger.info(f"   Current position: {total_shares} shares")
        logger.info(f"   Average cost basis: ${avg_cost_basis:.2f}")
        logger.info(f"   Current market price: ${live_price:.2f}")
        logger.info(f"   Unrealized PnL: ${total_unrealized_pnl:.2f}")
        
        return total_unrealized_pnl

if __name__ == "__main__":
    if len(sys.argv) == 2:
        # Single letter: python notifications_pnl.py a
        letter = sys.argv[1].lower()
        calculator = PnLCalculator(shard_range=(letter, letter))
        print(f"Starting PnL Calculator for accounts starting with '{letter.upper()}'")
    elif len(sys.argv) == 3:
        # Range: python notifications_pnl.py a h
        start_char = sys.argv[1].lower()
        end_char = sys.argv[2].lower()
        calculator = PnLCalculator(shard_range=(start_char, end_char))
        print(f"Starting PnL Calculator for accounts {start_char.upper()}-{end_char.upper()}")
    else:
        # No sharding
        calculator = PnLCalculator()
        print("Starting PnL Calculator without sharding (all accounts)")
    
    calculator.listen_and_calculate()