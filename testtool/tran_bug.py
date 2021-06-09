import redis
import time

ip = "192.168.64.4"
port = 6379
dbid = 0

pool = redis.ConnectionPool(host=ip,
                            port=port,
                            db=dbid,
                            decode_responses=True,
                            encoding='utf-8',
                            socket_connect_timeout=2)
r = redis.StrictRedis(connection_pool=pool)
r.config_set(name="bunnydeny", value="no")


def _tran():
    while True:
        pipe = r.pipeline(transaction=True)
        pipe.multi()
        pipe.get(name="abc")
        pipe.execute()


def _no_tran():
    while True:
        r.set(name="abc", value="123")


def _main():
    # _no_tran()
    _tran()


if __name__ == '__main__':
    _main()