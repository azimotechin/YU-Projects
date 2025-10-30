import redis
import logging
# from redis.sentinel import Sentinel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PnLRetriever:
    def __init__(self, realized_pnl_hash="realized_pnl_by_position", unrealized_pnl_hash="unrealized_pnl_by_position"):
        """
        Initializes the PnL retriever.

        :param realized_pnl_hash: Redis hash for realized PnL.
        :param unrealized_pnl_hash: Redis hash for unrealized PnL.
        """
        # Connect using Sentinel
        """
        sentinel = Sentinel(
            [('sentinel1', 26379), ('sentinel2', 26379), ('sentinel3', 26379)],
            socket_timeout=None,
            decode_responses=True
        )
        self.redis = sentinel.master_for("mymaster", socket_timeout=None, decode_responses=True)
        """
        self.redis = redis.Redis(host='localhost', port=6379, decode_responses=True)
        self.realized_pnl_hash = realized_pnl_hash
        self.unrealized_pnl_hash = unrealized_pnl_hash

    def get_ticker_pnl(self, account_id: str, ticker: str) -> dict:
        """
        Retrieves PnL data for a single ticker for a given account.

        :param account_id: The user's account ID.
        :param ticker: The ticker for which PnL is needed.
        :return: A dictionary containing realized, unrealized, and total PnL for the ticker.
        """
        key = f"{account_id}/{ticker}"
        realized_pnl = float(self.redis.hget(self.realized_pnl_hash, key) or 0.0)
        unrealized_pnl = float(self.redis.hget(self.unrealized_pnl_hash, key) or 0.0)
        total_pnl = realized_pnl + unrealized_pnl

        return {
            'realized_pnl': round(realized_pnl, 2),
            'unrealized_pnl': round(unrealized_pnl, 2),
            'total_pnl': round(total_pnl, 2)
        }

    def get_account_pnls(self, account_id: str) -> dict:
        """
        Retrieves PnL data for all tickers associated with a specific account.

        :param account_id: The user's account ID.
        :return: A dictionary of ticker symbols and their realized, unrealized, and total PnL values.
        """
        realized_pnls = {key: float(value) for key, value in self.redis.hgetall(self.realized_pnl_hash).items() if
                         key.startswith(f"{account_id}/")}
        unrealized_pnls = {key: float(value) for key, value in self.redis.hgetall(self.unrealized_pnl_hash).items() if
                           key.startswith(f"{account_id}/")}

        combined_pnls = {}
        for key in set(realized_pnls.keys()).union(unrealized_pnls.keys()):
            ticker = key.split("/")[1]
            realized_pnl = realized_pnls.get(key, 0.0)
            unrealized_pnl = unrealized_pnls.get(key, 0.0)
            total_pnl = realized_pnl + unrealized_pnl

            combined_pnls[ticker] = {
                'realized_pnl': round(realized_pnl, 2),
                'unrealized_pnl': round(unrealized_pnl, 2),
                'total_pnl': round(total_pnl, 2)
            }

        return combined_pnls

    def get_all_accounts_pnls(self) -> dict:
        """
        Retrieves PnL data for all accounts and tickers stored in Redis.

        :return: A dictionary where keys are account_id/ticker combinations and values are PnL dictionaries.
        """
        realized_pnls = self.redis.hgetall(self.realized_pnl_hash)
        unrealized_pnls = self.redis.hgetall(self.unrealized_pnl_hash)

        combined_pnls = {}
        all_keys = set(realized_pnls.keys()).union(unrealized_pnls.keys())

        for key in all_keys:
            realized_pnl = float(realized_pnls.get(key, 0.0))
            unrealized_pnl = float(unrealized_pnls.get(key, 0.0))
            total_pnl = realized_pnl + unrealized_pnl

            combined_pnls[key] = {
                'realized_pnl': round(realized_pnl, 2),
                'unrealized_pnl': round(unrealized_pnl, 2),
                'total_pnl': round(total_pnl, 2)
            }

        return combined_pnls


if __name__ == "__main__":
    retriever = PnLRetriever()

    # NOTE: Remember to change "alice2" to an account that exists in your data
    test_account = "alice68"
    test_ticker = "GOOG"

    # Fetch PnL for a single ticker
    ticker_pnl = retriever.get_ticker_pnl(test_account, test_ticker)
    print("Ticker PnL:", ticker_pnl)

    # Fetch PnL for all tickers in an account
    account_pnls = retriever.get_account_pnls(test_account)
    print("Account PnLs:", account_pnls)

    # Fetch PnL for all accounts
    all_pnls = retriever.get_all_accounts_pnls()
    print("All Accounts PnLs:", all_pnls)