import redis
from redisearch import Client, TextField, TagField, NumericField, IndexDefinition
import uuid
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def read_trades_from_csv(csv_path: str) -> list:
    list_of_trades = []
    with open(csv_path, "r") as f:
        list_of_trades = f.read().splitlines()
    return list_of_trades

def setup_index(redis_client: redis.Redis) -> Client:#this is the setup command for the ft search
    """
    Create a RediSearch index "idx:trades" on HASH keys with prefix "trade:"
    If the index already exists, this is a no-op.
    """
    client = Client('idx:trades', conn=redis_client)
    try:
        client.info()
    except Exception:
        # Index does not exist yet: create it
        client.create_index(
            fields=[
                TextField('user', sortable=True),
                TagField('date'),
                TagField('symbol'),
                NumericField('price', sortable=True),
                TagField('side'),
                NumericField('qty'),
            ],
            definition=IndexDefinition(
                prefix=['trade:'],
            )
        )
    return client
#uuid in key and incrementer in value
#taking in a string from the user and validatingbefore adding to redis
def add_to_redis(trades:list)->None:
    for trade in trades:
        key, value = trade.split(',', 1)
        user, date  = key.split(':',1)
        key = "trade:"+key + str(str(uuid.uuid4()))
        symbol, price,side,qty = value.split(':')
        price = price.lstrip('$')
        r.hset(
            key,
            mapping={
                'user': user,
                'date': date,
                'symbol': symbol,
                'price': price,
                'side': side,
                'qty': qty,
            }
        )
def main():
    setup_index(r)
    all_data = read_trades_from_csv('/home/daniel/SummerCourse/summer-2025/python/scripts/bank_trades.csv')
    add_to_redis(all_data)

if __name__ == '__main__':
    main()