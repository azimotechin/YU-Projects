import redis
import uuid

# INSTRUCTIONS FOR RUNNING REDIS LOCALLY (NOT THROUGH DOCKER)


# INSTRUCTIONS FOR RUNNING REDIS THROUGH DOCKER
# before you run the code, run this in your terminal:
### docker run -d --name redis -p 6379:6379 redis
# after you run the code, run this in your terminal:
### docker stop redis


# Connect to local Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# set up the variables for account 1
account_id = "username0"
trade_date = "2025-06-17"
trade_data = {
    "uuid": str(uuid.uuid4()),
    "ticker": "AAPL",
    "price": "100",
    "quantity": "10",
    "type": "BUY"
}

#create trade id based on an incrementing counter
trade_id = r.incr(f"trade_counter:{account_id}")

# create trade id based no the UUID library
# trade_id = str(uuid.uuid4())

#create trade key which is just account key+trade date+trade id
trade_key = f"{account_id}:{trade_date}:{trade_id}" #username0:date:0 -> 

#create and set a new hash object which uses the trade key as a key, and it's mapped to the trade_data object that we just created
r.hset(trade_key, mapping=trade_data)

#print out the trade we just stored in redis!
print(f"\nStored trade at key: {trade_key}\n")
print(f"Here is all the data for the key {trade_key}\n", r.hgetall(trade_key))
print()

# # when you're done, uncomment this line and run it again to reset the trade counter in your db.
# # r.set("trade_counter:username0", 0)