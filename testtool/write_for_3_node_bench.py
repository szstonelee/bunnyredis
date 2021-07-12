import redis
import random


def _main():
    POOL = redis.ConnectionPool(host='127.0.0.1',
                                port='6379',
                                db=0,
                                decode_responses=True,
                                encoding='utf-8',
                                socket_connect_timeout=2)
    r = redis.StrictRedis(connection_pool=POOL)

    prefix = "str_append_"
    scope = 1000
    cnt = 0
    while True:
        key = prefix + str(random.randint(0, scope))
        r.append(key=key, value="a")
        cnt = cnt + 1
        if cnt % 1000 == 0:
            print(f"test 3 node with write, cnt = {cnt}")


if __name__ == '__main__':
    _main()