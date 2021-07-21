import redis
import subprocess


pool = redis.ConnectionPool(host="127.0.0.1",
                            port=6379,
                            db=0,
                            decode_responses=True,
                            encoding='utf-8',
                            socket_connect_timeout=2)
r = redis.StrictRedis(connection_pool=pool)

key = "abc"

r.set(name=key, value="1")

try:
    print(r.incrbyfloat(name=key, amount=1.1))

except redis.exceptions.ResponseError as e:
    if str(e) != "value is not a valid float":
        raise e

    print("catch wanted exception")


arg1 = "abc"
arg2 = "888"
subprocess.check_call(["python3", "a_script.py", arg1, arg2])

print("I am here in exception")