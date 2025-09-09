# Trade Data Formats

## For Manual Trade Entry (String Format)
When entering a single trade as a string, use this format if you are inputting a trade manually for the first time, and therefore the Trade object was not yet created and doesn't have a UUID:
```
user,ticker:price:BUY/SELL:quantity
```
or if the trade object was created and has a UUID, and you're re-reading it from backup and inputting it again:
```
user:YYYY-MM-DD:uuid:[00:00:00],ticker:$price:side:quantity, action_type
```

**Examples:**
```
jacob:2025-06-24,AMD:$163.70:sell:954
isaac:2025-06-24,NVDA:$750.12:buy:206
aaron:2025-06-24,GOOGL:$73.84:buy:889
```

**Rules:**
- User: letters, numbers, underscore only
- Date: YYYY-MM-DD format
- Ticker: letters only
- Type: buy or sell (case insensitive)
- Quantity: whole number

## For Bulk Data Import (CSV Format)
When importing multiple trades from a CSV file, use standard CSV with these columns:
```
account_id,trade_date,trade_id,trade_time,ticker,price,trade_type,quantity,action_type
```

**Example CSV:**
```csv
account_id,trade_date,trade_id,trade_time,ticker,price,trade_type,quantity,action_type
jacob,2025-06-24,uuid-here,[18:21:16],AMD,163.7,sell,954,trade
isaac,2025-06-24,uuid-here,[18:21:16],NVDA,750.12,buy,206,trade

account_id,ticker,price,trade_type,quantity
aaron,GOOGL,73.84,buy,889
joshua,PYPL,861.48,buy,408
abraham,UBER,777.44,sell,573
```

## Key Differences
- **String format**: Uses `:` and `,` as separators
- **CSV format**: Uses `,` to separate all fields (standard CSV)

## Which Method to Use?
- Use **string format** always, unless utilizing a .csv file for bulk imports/exports
- Use **CSV format** for bulk imports or exports that are being handled through a .csv file