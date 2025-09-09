from datetime import datetime
import random
import uuid
from faker import Faker
from dataclasses import dataclass
from typing import Dict, Optional

fake = Faker()

@dataclass
class Trade:
    account_id: str
    ticker: str
    price: float
    trade_type: str
    quantity: int
    action_type: Optional[str] = None
    trade_time: Optional[str] = None
    trade_date: Optional[str] = None
    trade_id: Optional[str] = None

    def __post_init__(self):
        if self.trade_id is None:
            self.trade_id = str(uuid.uuid4())
        if self.trade_time is None:
            self.trade_time = datetime.now().strftime("[%H:%M:%S]")
        if self.trade_date is None:
            self.trade_date = datetime.now().strftime('%Y-%m-%d')
        if self.trade_type not in ["buy", "sell"]:
            raise ValueError(f"Invalid trade type: {self.trade_type}. Must be 'buy' or 'sell'.")
        if self.action_type is None or self.action_type not in ["trade", "placeholder"]:
            # this is an error, for now just set it to be "trade"
            self.action_type = "trade"
    @staticmethod
    def from_string(trade_string):
        # Expected format: account_id:trade_date:trade_id,[time]:ticker:$price:buy/sell:qty:trade
        try:
            left, ticker, price, trade_type, quantity, action_type = trade_string.split(':')[3:]
            time_str = trade_string.split(',')[1].split(']')[0] + "]"
            return Trade(
                account_id=trade_string.split(':')[0],
                trade_date=trade_string.split(':')[1],
                trade_id=trade_string.split(':')[2].split(',')[0],
                trade_time=time_str,
                ticker=ticker,
                price=float(price.strip('$')),
                trade_type=trade_type,
                quantity=int(quantity),
                action_type=action_type
            )
        except Exception as e:
            raise ValueError(f"Could not parse trade string: {trade_string}") from e

    @classmethod
    def create_random(cls) -> 'Trade':
        all_users = ["abraham", "isaac", "jacob", "moses", "aaron", "joshua", "caleb", "david", "solomon", "daniel", "elijah", "isaiah", "jeremiah", "ezekiel", "hosea"]
        all_tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NFLX", "NVDA", "AMD", "INTL", "ORCL", "UBER", "PYPL", "CRM", "ADBE", "SHOP", "BABA", "SQ", "COIN", "SNOW", "ROKU", "ZM", "PLTR"]
        
        return cls(
            account_id=random.choice(all_users),
            trade_date=fake.date_between(start_date='-100y', end_date='+100y').strftime('%Y-%m-%d'),
            trade_time=fake.time(pattern="[%H:%M:%S]", end_datetime=None),
            ticker=random.choice(all_tickers),
            price=round(random.uniform(1, 1000), 2),
            trade_type=random.choice(["buy", "sell"]),
            quantity=random.randint(1, 1000),
            action_type = "trade"
            #action_type=random.choice(["trade", "placeholder"])
        )
    
    def to_redis_key(self) -> str:
        return f"{self.account_id}:{self.trade_date}:{self.trade_id}"
    
    def to_redis_hash(self) -> Dict[str, str]:
        return {
            "trade_time": self.trade_time,
            "ticker": self.ticker,
            "price": str(self.price),
            "type": self.trade_type,
            "quantity": str(self.quantity),
            "action_type": self.action_type
        }
    
    
    @classmethod
    def from_redis_data(cls, key: str, hash_data: Dict[str, str]) -> 'Trade':
        key_parts = key.split(':')
        if len(key_parts) != 3:
            raise ValueError(f"Invalid key format: expected 3 parts separated by ':', got {len(key_parts)} parts in '{key}'")
        return cls(
            account_id=key_parts[0],
            trade_date=key_parts[1],
            trade_id=key_parts[2],
            trade_time=hash_data['trade_time'],
            ticker=hash_data['ticker'],
            price=float(hash_data['price']),
            trade_type=hash_data['type'],
            quantity=int(hash_data['quantity']),
            action_type=hash_data['action_type']
        )
    
    @classmethod
    def create_from_full_input(cls, input_string: str) -> 'Trade':
        part1, part2 = input_string.strip().split(',')
        account_id = part1  # Just use part1 directly since there's no colon to split on
        parts = part2.split(':')

        if len(parts) != 5:
            raise ValueError(f"Invalid input format. Expected 5 colon-separated values: ticker,price,trade_type,quantity,action_type. Got {len(parts)} values.")

        try:
            return cls(
                account_id=account_id,
                ticker=parts[0].strip(),
                price=float(parts[1].strip()),
                trade_type=parts[2].strip().lower(),
                quantity=int(parts[3].strip()),
                action_type=parts[4].strip().lower()
            )
        except ValueError as e:
            raise ValueError(f"Error parsing input '{input_string}': {e}")

    @classmethod
    def create_from_parts(cls, account_id: str, ticker: str, price: float,  trade_type: str, quantity: int, action_type: str) -> 'Trade':
        return cls(
            account_id=account_id,
            ticker=ticker.upper(),
            price=price,
            trade_type=trade_type.lower(),
            quantity=quantity,
            action_type=action_type.lower()
        )
    
    @classmethod
    def create_user_trade_from_cli_in_parts(cls) -> 'Trade':
        input_account = input("Enter account name: ").strip()
        input_ticker = input("Enter ticker symbol: ").strip().upper()
        input_price = float(input("Enter price: ").strip())
        input_side = input("Enter trade action (buy/sell): ").strip().lower()
        input_quantity = int(input("Enter quantity: ").strip())
        input_type = input("Enter trade type (trade/placeholder): ").strip().lower()

        print("Trade logged.")

        return cls.create_from_parts(input_account, input_ticker, input_price, input_side, input_quantity, input_type)
    
    @classmethod
    def create_user_trade_from_cli_full(cls) -> 'Trade':
        input_string = input("Enter trade details in format 'account_id,ticker:price:trade_type:quantity:action_type': ").strip()
        print("Trade logged.")
        return cls.create_from_full_input(input_string)
    
    def __str__(self) -> str:
        return f"{self.account_id}:{self.trade_date}:{self.trade_id},{self.trade_time}:{self.ticker}:${self.price}:{self.trade_type}:{self.quantity}:{self.action_type}"
