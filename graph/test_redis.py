import redis

r = redis.Redis(host='172.17.74.190', port=6379, db=0)


r.set('foo', 'bar')