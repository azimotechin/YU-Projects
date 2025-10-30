import unittest
import os
import tempfile
import sys
from datetime import datetime
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from TradeManager import TradeManager
from Trade import Trade
import csv

class TestTradeManager(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.tm = TradeManager()
        self.tm.clear_all_trades()  # Start with clean Redis
        
        # Create test trades
        self.test_trade1 = Trade(
            account_id="test_user1",
            trade_date="2025-01-15",
            trade_id="test-123",
            trade_time="[10:30:00]",
            ticker="AAPL",
            price=150.50,
            trade_type="buy",
            quantity=100,
            action_type="trade"
        )
        
        self.test_trade2 = Trade(
            account_id="test_user2",
            trade_date="2025-01-16",
            trade_id="test-456",
            trade_time="[14:20:00]",
            ticker="MSFT",
            price=300.75,
            trade_type="sell",
            quantity=50,
            action_type="trade"
        )
    
    def tearDown(self):
        """Clean up after each test method."""
        self.tm.clear_all_trades()
    
    def test_write_single_trade(self):
        """Test writing a single trade to Redis."""
        result = self.tm.write_trade(self.test_trade1)
        self.assertTrue(result)
        
        # Verify trade was written
        all_trades = self.tm.get_all_trades()
        self.assertEqual(len(all_trades), 1)
        self.assertEqual(all_trades[0].account_id, "test_user1")
    
    def test_write_multiple_trades(self):
        """Test writing multiple trades to Redis."""
        trades = [self.test_trade1, self.test_trade2]
        result = self.tm.write_trades(trades)
        self.assertTrue(result)
        
        # Verify trades were written
        all_trades = self.tm.get_all_trades()
        self.assertEqual(len(all_trades), 2)
        account_ids = {trade.account_id for trade in all_trades}
        self.assertEqual(account_ids, {"test_user1", "test_user2"})
    
    def test_get_all_trades(self):
        """Test retrieving all trades from Redis."""
        # Write some trades first
        trades = [self.test_trade1, self.test_trade2]
        self.tm.write_trades(trades)
        
        # Retrieve all trades
        retrieved_trades = self.tm.get_all_trades()
        self.assertEqual(len(retrieved_trades), 2)
        
        # Check trade properties
        trade_by_id = {trade.trade_id: trade for trade in retrieved_trades}
        self.assertIn("test-123", trade_by_id)
        self.assertIn("test-456", trade_by_id)
        self.assertEqual(trade_by_id["test-123"].ticker, "AAPL")
        self.assertEqual(trade_by_id["test-456"].ticker, "MSFT")
    
    def test_clear_all_trades(self):
        """Test clearing all trades from Redis."""
        # Write some trades first
        self.tm.write_trades([self.test_trade1, self.test_trade2])
        self.assertEqual(len(self.tm.get_all_trades()), 2)
        
        # Clear all trades
        result = self.tm.clear_all_trades()
        self.assertTrue(result)
        self.assertEqual(len(self.tm.get_all_trades()), 0)
    
    def test_create_random_trades(self):
        """Test creating random trades."""
        num_trades = 10
        random_trades = TradeManager.create_random_trades(num_trades)
        
        self.assertEqual(len(random_trades), num_trades)
        for trade in random_trades:
            self.assertIsInstance(trade, Trade)
            self.assertIn(trade.trade_type, ["buy", "sell"])
            self.assertGreater(trade.price, 0)
            self.assertGreater(trade.quantity, 0)
    
    def test_write_trades_to_csv(self):
        """Test writing trades to CSV file."""
        # Write trades to Redis first
        self.tm.write_trades([self.test_trade1, self.test_trade2])
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp_file:
            tmp_filename = tmp_file.name
        
        try:
            # Write to CSV
            result = self.tm.write_trades_to_csv(tmp_filename)
            self.assertTrue(result)
            
            # Verify CSV content
            with open(tmp_filename, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
            self.assertEqual(len(rows), 2)
            # Check if our test trades are in the CSV
            account_ids = {row['account_id'] for row in rows}
            self.assertEqual(account_ids, {"test_user1", "test_user2"})
            
        finally:
            os.unlink(tmp_filename)
    
    def test_get_trades_from_csv(self):
        """Test reading properly formatted trades from CSV."""
        # Create a temporary CSV file with proper trade data
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', newline='') as tmp_file:
            writer = csv.writer(tmp_file)
            writer.writerow(['account_id', 'trade_date', 'trade_id', 'trade_time', 'ticker', 'price', 'trade_type', 'quantity', 'action_type'])
            writer.writerow(['user1', '2025-01-15', 'id1', '[10:30:00]', 'AAPL', '150.50', 'buy', '100', 'trade'])
            writer.writerow(['user2', '2025-01-16', 'id2', '[14:20:00]', 'MSFT', '300.75', 'sell', '50', 'trade'])
            tmp_filename = tmp_file.name
        
        try:
            trades = self.tm.get_trades_from_csv(tmp_filename)
            self.assertEqual(len(trades), 2)
            
            # Check trade details
            trade_by_id = {trade.trade_id: trade for trade in trades}
            self.assertEqual(trade_by_id['id1'].ticker, 'AAPL')
            self.assertEqual(trade_by_id['id1'].price, 150.50)
            self.assertEqual(trade_by_id['id2'].ticker, 'MSFT')
            self.assertEqual(trade_by_id['id2'].quantity, 50)
            
        finally:
            os.unlink(tmp_filename)
    
    def test_write_list_of_trades_to_csv(self):
        """Test static method for writing list of trades to CSV."""
        trades = [self.test_trade1, self.test_trade2]
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp_file:
            tmp_filename = tmp_file.name
        
        try:
            result = TradeManager.write_list_of_trades_to_csv(trades, tmp_filename)
            self.assertTrue(result)
            
            # Verify CSV content
            with open(tmp_filename, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]['ticker'], 'AAPL')
            self.assertEqual(rows[1]['ticker'], 'MSFT')
            
        finally:
            os.unlink(tmp_filename)
    
    def test_read_raw_csv_data(self):
        """Test reading raw CSV data."""
        # Create a CSV with mixed/incomplete data
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', newline='') as tmp_file:
            writer = csv.writer(tmp_file)
            writer.writerow(['user', 'symbol', 'price', 'side', 'qty'])
            writer.writerow(['alice', 'AAPL', '$150.50', 'BUY', '100'])
            writer.writerow(['bob', 'MSFT', '300.75', 'SELL', '50'])
            tmp_filename = tmp_file.name
        
        try:
            raw_data = TradeManager.read_raw_csv_data(tmp_filename)
            self.assertEqual(len(raw_data), 2)
            self.assertEqual(raw_data[0]['user'], 'alice')
            self.assertEqual(raw_data[1]['symbol'], 'MSFT')
            
        finally:
            os.unlink(tmp_filename)
    
    def test_read_raw_csv_data_as_list(self):
        """Test reading raw CSV data as list of strings."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', newline='') as tmp_file:
            writer = csv.writer(tmp_file)
            writer.writerow(['header1', 'header2', 'header3'])
            writer.writerow(['value1', 'value2', 'value3'])
            writer.writerow(['value4', 'value5', 'value6'])
            tmp_filename = tmp_file.name
        
        try:
            raw_data = TradeManager.read_raw_csv_data_as_list(tmp_filename)
            self.assertEqual(len(raw_data), 3)  # Including header
            self.assertEqual(raw_data[0], 'header1,header2,header3')
            self.assertEqual(raw_data[1], 'value1,value2,value3')
            
        finally:
            os.unlink(tmp_filename)
    
    def test_create_trades_from_raw_csv(self):
        """Test creating Trade objects from raw CSV data."""
        # Create CSV with minimal trade data
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', newline='') as tmp_file:
            writer = csv.writer(tmp_file)
            writer.writerow(['account_id', 'ticker', 'price', 'trade_type', 'quantity'])
            writer.writerow(['alice', 'AAPL', '$150.50', 'BUY', '100'])
            writer.writerow(['bob', 'MSFT', '300.75', 'SELL', '50'])
            tmp_filename = tmp_file.name
        
        try:
            trades = TradeManager.create_trades_from_raw_csv(tmp_filename)
            self.assertEqual(len(trades), 2)
            
            # Check that missing fields were filled
            for trade in trades:
                self.assertIsNotNone(trade.trade_id)
                self.assertIsNotNone(trade.trade_time)
                self.assertEqual(trade.action_type, 'trade')
                self.assertIsNotNone(trade.trade_date)
            
            # Check specific values
            alice_trade = next(t for t in trades if t.account_id == 'alice')
            self.assertEqual(alice_trade.ticker, 'AAPL')
            self.assertEqual(alice_trade.price, 150.50)  # $ should be removed
            self.assertEqual(alice_trade.trade_type, 'buy')  # normalized to lowercase
            
        finally:
            os.unlink(tmp_filename)
    
    def test_verify_format(self):
        """Test trade string format verification."""
        # Valid format
        valid_trade = "user1:2025-01-15,AAPL:$150.50:BUY:100"
        self.assertTrue(TradeManager.verify_format(valid_trade))
        
        # Invalid formats
        invalid_trades = [
            "user1,AAPL:$150.50:BUY:100",  # Missing date
            "user1:2025-01-15,AAPL:150.50:BUY:100",  # Missing $
            "user1:2025-01-15,AAPL:$150.50:INVALID:100",  # Invalid side
            "user1:invalid-date,AAPL:$150.50:BUY:100",  # Invalid date
            "user1:2025-01-15,123:$150.50:BUY:100",  # Invalid ticker
        ]
        
        for invalid_trade in invalid_trades:
            self.assertFalse(TradeManager.verify_format(invalid_trade))
    
    def test_convert_string_to_trade(self):
        """Test converting string to Trade object."""
        trade_string = "alice:2025-01-15,AAPL:$150.50:BUY:100"
        trade = TradeManager.convert_string_to_trade(trade_string)
        
        self.assertEqual(trade.account_id, "alice")
        self.assertEqual(trade.trade_date, "2025-01-15")
        self.assertEqual(trade.ticker, "AAPL")
        self.assertEqual(trade.price, 150.50)
        self.assertEqual(trade.trade_type, "buy")
        self.assertEqual(trade.quantity, 100)
        self.assertEqual(trade.action_type, "trade")
    
    def test_convert_full_string_to_trade(self):
        """Test converting full format string to Trade object."""
        # Fixed format: the trade_time should not contain colons that interfere with parsing
        full_string = "alice:2025-01-15:trade-123,[10-30-00]:AAPL:$150.50:buy:100:trade"
        trade = TradeManager.convert_full_string_to_trade(full_string)
        
        self.assertEqual(trade.account_id, "alice")
        self.assertEqual(trade.trade_date, "2025-01-15")
        self.assertEqual(trade.trade_id, "trade-123")
        self.assertEqual(trade.trade_time, "[10-30-00]")
        self.assertEqual(trade.ticker, "AAPL")
        self.assertEqual(trade.price, 150.50)
        self.assertEqual(trade.trade_type, "buy")
        self.assertEqual(trade.quantity, 100)
        self.assertEqual(trade.action_type, "trade")
    
    def test_write_trades_with_benchmarking(self):
        """Test writing trades with performance benchmarking."""
        trades = TradeManager.create_random_trades(100)
        result = self.tm.write_trades_with_benchmarking(trades)
        self.assertTrue(result)
        
        # Verify all trades were written
        all_trades = self.tm.get_all_trades()
        self.assertEqual(len(all_trades), 100)
    
    def test_nonexistent_file_handling(self):
        """Test handling of non-existent files."""
        nonexistent_file = "this_file_does_not_exist.csv"
        
        # Should return empty list without crashing
        trades = self.tm.get_trades_from_csv(nonexistent_file)
        self.assertEqual(len(trades), 0)
        
        raw_data = TradeManager.read_raw_csv_data(nonexistent_file)
        self.assertEqual(len(raw_data), 0)
        
        raw_list = TradeManager.read_raw_csv_data_as_list(nonexistent_file)
        self.assertEqual(len(raw_list), 0)
    
    def test_invalid_csv_data_handling(self):
        """Test handling of invalid CSV data."""
        # Create CSV with invalid data
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', newline='') as tmp_file:
            writer = csv.writer(tmp_file)
            writer.writerow(['account_id', 'trade_date', 'trade_id', 'trade_time', 'ticker', 'price', 'trade_type', 'quantity', 'action_type'])
            writer.writerow(['user1', '2025-01-15', 'id1', '[10:30:00]', 'AAPL', 'invalid_price', 'buy', '100', 'trade'])
            writer.writerow(['user2', '2025-01-16', 'id2', '[14:20:00]', 'MSFT', '300.75', 'sell', 'invalid_qty', 'trade'])
            tmp_filename = tmp_file.name
        
        try:
            # Should skip invalid rows and continue
            trades = self.tm.get_trades_from_csv(tmp_filename)
            self.assertEqual(len(trades), 0)  # Both rows are invalid
            
        finally:
            os.unlink(tmp_filename)

if __name__ == '__main__':
    # Run the tests
    unittest.main(verbosity=2)