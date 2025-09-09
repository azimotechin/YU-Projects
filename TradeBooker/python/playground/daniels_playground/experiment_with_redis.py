import redis
r = redis.Redis(host='localhost', port=6379, db=0,decode_responses=True)#without the decode_responses, printing the value will put a b in front

print(r.get('name'))