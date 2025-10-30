from TradeManager import TradeManager

def main():
    trade_manager_instance = TradeManager()
    trades = trade_manager_instance.create_random_trades(10)
    trade_manager_instance.write_trades_with_benchmarking(trades)

    for trade in trade_manager_instance.get_all_trades():
        print(trade)

if __name__ == "__main__":
    main()