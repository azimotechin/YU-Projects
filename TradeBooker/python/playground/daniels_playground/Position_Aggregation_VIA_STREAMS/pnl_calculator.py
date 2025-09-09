import redis
import uuid
import logging
from Trade import Trade
import json
import market_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PnLCalculator:
    def __init__(self, stream_key  ="trades_stream", consumer_group="pnl-group"):
        #Initializing the PnL Calculator with basic setup

        #Connecting to redis, temporary local host
        self.redis = redis.Redis(host = 'localhost', port = 6379, decode_responses=True)

        self.stream_key = stream_key
        self.group = consumer_group
        self.consumer =f"pnl-consumer-{uuid.uuid4()}"

        logger.info(f"PnL Calculator starting with consumer name: {self.consumer}")

        #Defining Redis hash names where we'll store our pnl data
        self.lots_key_prefix = "lots:"# Will store: lots:alice/AAPL → JSON list of lots. A lot is basically a queue of all the trades booked with their price data. A single lot represent a single buy. Json will store all the lots
        self.unrealized_pnl_by_position_hash = "unrealized_pnl_by_position"
        self.realized_pnl_by_position_hash = "realized_pnl_by_position"

        logger.info(f"Will store FIFO lots with key prefix: '{self.lots_key_prefix}'")
        logger.info(f"Will store realized PnL in Redis hash: '{self.realized_pnl_by_position_hash}'")

        # FIFO lot structure will be a JSON list like:
        # [
        #   {"price": 150.00, "quantity": 100, "date": "2025-01-15", "time": "[09:30:00]"},
        #   {"price": 160.00, "quantity": 50, "date": "2025-01-15", "time": "[10:15:00]"}
        # ]
        
        #Set up consumer group (same pattern as other services)
        self._setup_consumer_group()

    def _setup_consumer_group(self):
        #This is identical to TradeBooker and position_aggregator setup
        if self.redis.exists(self.stream_key):
            try:
                self.redis.xgroup_create(self.stream_key, self.group, id='0')
                logger.info(f"Consumer group '{self.group}' created.")
            except redis.ResponseError as e:
                if "BUSYGROUP" in str(e):
                    logger.info(f"Consumer group '{self.group}' already exists.")
                else:
                    raise
        else:
            #hence, make sure docker starts the stream (albeit with no trades coming in yet) BEFORE starting position_aggregator and trade_booker
            logger.warning(f"Stream '{self.stream_key}' not found. PnL Calculator cannot run.")
            raise RuntimeError("Stream not found. PnL Calculator exiting.")

    def _get_lots_key(self, account_id: str, ticker: str) -> str: #generates a redis key in the following format 'lots:alice/AAPL'
        return f"{self.lots_key_prefix}{account_id}/{ticker}"
        
    def _get_lots(self, account_id:str, ticker:str) -> list: #returns a list of all lots(trades) for the user and ticker
        lots_key =self._get_lots_key(account_id, ticker)#get not hget - see ai
        lots_json = self.redis.get(lots_key)

        if lots_json:
            return json.loads(lots_json)
        else:
            return []#No lots exist yet
        
    def _save_lots(self, account_id : str, ticker: str, lots:list): #Saves lots and stores in into json for the lot key
        lots_key = self._get_lots_key(account_id, ticker)
        lots_json = json.dumps(lots)

        self.redis.set(lots_key, lots_json)
        logger.info(f"Saved {len(lots)} lots for {account_id}/{ticker}")

    def listen_and_calculate(self):#Listens for trades coming into the stream and calculates pnl, runs continously
        logger.info("Starting PnL Calculator - listenign for trades")

        while True:
            try:
                messages = self.redis.xreadgroup(
                    groupname= self.group, #"pnl-group"
                    consumername= self.consumer,#"pnl-consumer-uuid"
                    streams= {self.stream_key: '>'}, # Reading new messages from "trades_stream"
                    count=10, #Process 10 trades at a time
                    block = 5000 #Wait 5 sec if no new trades
                )
        
                for stream, entries in messages:
                    for msg_id, fields in entries:
                        try:
                            #parse the trade (same as other services)
                            trade = Trade.create_from_full_input(fields["trade_string"])
                            
                            # Fifo Pnl calculation
                            self.process_trade_fifo(trade)
                            
                            # Acknowledge successful processing
                            self.redis.xack(self.stream_key, self.group, msg_id)
                                
                            logger.info(f"Processed trade: {msg_id}")
                                
                        except Exception as e:
                            logger.error(f"Failed to process message {msg_id}: {e}")
                            # Still acknowledge to avoid reprocessing bad messages
                            self.redis.xack(self.stream_key, self.group, msg_id)
            except Exception as e:
                logger.error(f"Redis stream read error: {e}")

    def process_trade_fifo(self, trade:Trade): #fucntion that processes accroding to whether the trade is a buy or sell

        if trade.trade_type == "buy":
            self._process_buy_fifo(trade)
        elif trade.trade_type == "sell":
            self._process_sell_fifo(trade)
        else:
            logger.error(f"Unknown trade type {trade.trade_type}")

    def _process_buy_fifo(self, trade:Trade):
        existing_lots = self._get_lots(trade.account_id, trade.ticker) #save list of existing lots to append to

        new_lot = {
            "price" : trade.price,
            "quantity" : trade.quantity,
            "date" :  trade.trade_date,
            "time" : trade.trade_time
        }
        existing_lots.append(new_lot)

        self._save_lots(trade.account_id, trade.ticker, existing_lots)

        logger.info(f"BUY - Added lot of {trade.quantity} shares @ ${trade.price}")
        logger.info(f"{trade.account_id}/{trade.ticker} now has {len(existing_lots)} lots")
    
    def _process_sell_fifo(self, trade:Trade):
        #Removes the trade quantity of shares from the lots, since it is fifo it removes the oldest added lots. 
        #If sell quantity is higher than the first lot, this method will consume multiple lots and calulate the realized Pnl accordingly
        #Partial consumption of lots is also permitted

        existing_lots = self._get_lots(trade.account_id, trade.ticker)

        if not existing_lots:
            logger.warning(f"SELL - No lots to sell for {trade.account_id}/{trade.ticker} - cannot calculate PnL")
            return
        
        remaining_to_sell = trade.quantity #tracks the shares left to sell
        total_realized_pnl = 0.0 #tracks the pnl

        logger.info(f"SELL - Need to sell {remaining_to_sell} shares of {trade.ticker} from a total of {len(existing_lots)}")

        lots_to_remove = [] #list of lots that were fully sold that need to be removed

        for i, lot in enumerate(existing_lots):
            if remaining_to_sell<=0: #we already sold everything, no need to look in the other lots
                break
            
            if lot["quantity"] <= remaining_to_sell:#this means sell all shares in the lot and since it is fully consumed, add it the lots_to_remove list
                realized_pnl_from_lot = (trade.price - lot["price"]) *lot["quantity"] #calculating realized pnl by price sold - price bought for multiplied by how many shares sold
                total_realized_pnl += realized_pnl_from_lot
                remaining_to_sell -= lot["quantity"]
                lots_to_remove.append(i)  #mark for removal

                logger.info(f"Consumed entire lot: {lot['quantity']} @ ${lot['price']} → PnL: ${realized_pnl_from_lot:.2f}")
            else: #partial consumption of the current lot
                realized_pnl_from_lot = (trade.price - lot["price"]) * remaining_to_sell
                total_realized_pnl += realized_pnl_from_lot
                lot["quantity"]-=remaining_to_sell #updating the lot to have the new quantity
                logger.info(f"Partially consumed lot: {remaining_to_sell} from {lot['quantity'] + remaining_to_sell} @ ${lot['price']} → PnL: ${realized_pnl_from_lot:.2f}")
                logger.info(f"Remaining in lot: {lot['quantity']} shares")
                remaining_to_sell = 0

        #remove fully consumed lots
        existing_lots = [lot for i, lot in enumerate(existing_lots) if i not in lots_to_remove]

        #save updated lots
        self._save_lots(trade.account_id, trade.ticker, existing_lots)

            #update realized PnL for this account
        if total_realized_pnl != 0:
            position_key = f"{trade.account_id}/{trade.ticker}"
            self.redis.hincrbyfloat(self.realized_pnl_by_position_hash, position_key, total_realized_pnl)#does this method only update or add to redis if it doesnt exist, make sre it adds to the previous realized pnl if it is there already
            logger.info(f"Realized PnL for {position_key}: ${total_realized_pnl:.2f}")
            self.store_and_calculate_unrealized_pnl_position(trade.account_id,trade.ticker)
                
        if remaining_to_sell > 0:
            logger.warning(f"Could not sell {remaining_to_sell} shares - insufficient lots!")

    
    #UNREALIZED PNL FUNCTIONS BELOW
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
    
    #Caluclate unrealized
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
    calculator = PnLCalculator()
    calculator.listen_and_calculate()


# get realized pnl for account/ticker
# get unrealized pnl for account/ticker
# get total realized and unrealized pnl for account
# maybe a method that combines the two
# Any other useful functions that you think could be useful feel free to add

# Also need a script that constantly is adding/updating the unrealized pnls for each postion (account/ticker) and updates that every hour. Should work like adding tickers to redis.

# Hash Name: "realized_pnl_by_position"
# Field Pattern: "{account_id}/{ticker}"
# Value: Float (cumulative realized PnL for this position)

# Examples:
# realized_pnl_by_position:
# ├── alice/AAPL → 1750.50    (Alice's total realized PnL from AAPL trades)
# ├── alice/MSFT → -250.25    (Alice lost money on MSFT)
# ├── bob/AAPL   → 500.75     (Bob's AAPL realized PnL)
# └── bob/TSLA   → 1200.00    (Bob's TSLA realized PnL)

# Hash Name: "unrealized_pnl_by_position"
# Field Pattern: "{account_id}/{ticker}"
# Value: Float (current unrealized PnL for this position)

# Examples:
# unrealized_pnl_by_position:
# ├── alice/AAPL → 825.75     (Paper profit on Alice's current AAPL holdings)
# ├── alice/MSFT → 150.50     (Paper profit on Alice's current MSFT holdings)
# ├── bob/AAPL   → -75.25     (Paper loss on Bob's current AAPL holdings)
# └── bob/TSLA   → 300.00     (Paper profit on Bob's current TSLA holdings)

# positions (Hash):
# ├── alice/AAPL → 150        (Alice owns 150 shares of AAPL)
# ├── bob/MSFT   → 75         (Bob owns 75 shares of MSFT)

# trades_stream (Stream):
# ├── Message 1: {"trade_string": "alice,AAPL:150:buy:100:trade"}
# ├── Message 2: {"trade_string": "bob,MSFT:290:sell:25:trade"}

# Individual Trade Records (Keys):
# ├── alice:2025-01-15:uuid1 → {trade details}
# ├── bob:2025-01-15:uuid2   → {trade details}