import time, redis

while True:
    try:
        r = redis.Redis(host="redis-master", port=6379)
        r.ping()
        break
    except redis.exceptions.ConnectionError:
        time.sleep(0.5)