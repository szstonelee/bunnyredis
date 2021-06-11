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


def write_by_set():
    print("test kafka compation for set")
    key = "skey"
    val = "val" * 300
    for i in range(0, 200_000):
        r.set(name=key, value=val)
        if i % 1000 == 999:
            print(f"cnt = {i+1}")


def write_by_hset():
    print("test kafka compation for hset")
    key = "hkey"
    field = "f1a"
    val = "val" * 300
    for i in range(0, 200_000):
        r.hset(name=key, key=field, value=val)
        if i % 1000 == 999:
            print(f"cnt = {i+1}")


def _main():
    r.flushall()
    # write_by_set()
    write_by_hset()


if __name__ == '__main__':
    _main()