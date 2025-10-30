from TradeManager import TradeManager


def main():
    tm = TradeManager()

    newtrades = tm.create_trades_from_raw_csv("tradeinput.csv")
    tm.write_list_of_trades_to_csv(newtrades, "tradeoutput.csv")

    for trade in newtrades:
        print(trade)

if __name__ == "__main__":
    main()