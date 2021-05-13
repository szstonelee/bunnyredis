import redis
import random
import threading
import time

POOL0 = redis.ConnectionPool(host='192.168.64.4',
                            port='6379',
                            db=0,
                            decode_responses=True,
                            encoding='utf-8',
                            socket_connect_timeout=2)

POOL1 = redis.ConnectionPool(host='192.168.64.4',
                            port='6379',
                            db=1,
                            decode_responses=True,
                            encoding='utf-8',
                            socket_connect_timeout=2)

def _injet():
    r0 = redis.StrictRedis(connection_pool=POOL0)
    for i in range(0, 4000):
        key = "key" + str(random.randint(0, 99999))
        if i % 2 == 1:
            val = "value is a long string with a random number of " + str(random.randint(1000, 9999)) + " : " + "x"*10000
        else:
            val = "small value"
        r0.set(name=key, value=val)


def _main():
    _injet()

if __name__ == '__main__':
    _main()