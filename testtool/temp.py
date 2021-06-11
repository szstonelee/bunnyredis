from test_common import *

r.hset(name="abc", key="f1", value="v1")

try:
    r.getset(name="abc", value="123")
except redis.exceptions.ResponseError as e:
    if str(e) == "WRONGTYPE Operation against a key holding the wrong kind of value":
        pass
    else:
        raise

