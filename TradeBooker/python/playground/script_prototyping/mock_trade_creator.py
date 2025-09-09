import csv
import os
import pandas as pd
from datetime import date

# Define some sample data - true is buy, false is sell
trades = [
    {"account_id": "abraham", "ticker": "AAPL", "price": 192.54, "type": "BUY", "quantity": 10},
    {"account_id": "abraham", "ticker": "MSFT", "price": 325.11, "type": "SELL", "quantity": 5},
    {"account_id": "issac", "ticker": "TSLA", "price": 670.00, "type": "BUY", "quantity": 8},
    {"account_id": "abraham", "ticker": "GOOGL", "price": 143.21, "type": "SELL", "quantity": 2},
    {"account_id": "issac", "ticker": "AMZN", "price": 3345.55, "type": "BUY", "quantity": 1},
    {"account_id": "jacob", "ticker": "NFLX", "price": 500.00, "type": "BUY", "quantity": 3},
    {"account_id": "jacob", "ticker": "FB", "price": 350.00, "type": "SELL", "quantity": 4},
    {"account_id": "abraham", "ticker": "NVDA", "price": 600.00, "type": "BUY", "quantity": 6},
    {"account_id": "abraham", "ticker": "IBM", "price": 140.25, "type": "BUY", "quantity": 7},
    {"account_id": "issac", "ticker": "ORCL", "price": 120.10, "type": "SELL", "quantity": 3},
    {"account_id": "jacob", "ticker": "INTC", "price": 45.60, "type": "BUY", "quantity": 12},
    {"account_id": "abraham", "ticker": "AMD", "price": 110.75, "type": "SELL", "quantity": 4},
    {"account_id": "issac", "ticker": "BABA", "price": 85.30, "type": "BUY", "quantity": 9},
    {"account_id": "jacob", "ticker": "UBER", "price": 45.90, "type": "SELL", "quantity": 6},
    {"account_id": "abraham", "ticker": "DIS", "price": 95.80, "type": "BUY", "quantity": 5},
    {"account_id": "issac", "ticker": "PYPL", "price": 70.40, "type": "SELL", "quantity": 2},
    {"account_id": "jacob", "ticker": "ADBE", "price": 510.00, "type": "BUY", "quantity": 1},
    {"account_id": "abraham", "ticker": "CRM", "price": 220.50, "type": "SELL", "quantity": 3}
]

# Today's date as trade_date
trade_date = date.today().isoformat()  # e.g., "2025-06-18"

# Write to CSV
output_path = os.path.join(os.path.dirname(__file__), "bank_trades.csv")

with open(output_path, "w", newline='') as csvfile:
    writer = csv.writer(csvfile)
    # writer.writerow(["Key", "Value"])  # Header

    trade_counters = {}

    for trade in trades:
        account_id = trade["account_id"]
        trade_counters.setdefault(account_id, 0)
        trade_id = f"{trade_counters[account_id]:04d}"  # 4-digit padded
        trade_counters[account_id] += 1

        key = f"{account_id}:{trade_date}"
        value = f"{trade['ticker']}:${trade['price']}:{trade['type']}:{trade['quantity']}"
        writer.writerow([key, value])
    csvfile.close()