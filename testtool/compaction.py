import redis


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
r.config_set(name="bunnymem", value=20<<20)
r.config_set(name="bunnydeny", value="no")


def write():
    key = "abc"
    val = "val" * 300
    for i in range(0, 200_000):
        r.set(name=key, value=val)
        if i % 1000 == 999:
            print(f"cnt = {i+1}")


def _main():
    r.flushall()
    write()


if __name__ == '__main__':
    _main()