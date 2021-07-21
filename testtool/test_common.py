import redis
import time


real_redis_port = 6379
bunny_node1_port = 6379
bunny_node2_port = 6379

# global name, g_common is a dict for other modules
g_common = {"server_r_ip": "need init",
            "server_r1_ip" : "need_init",
            "server_r2_ip" : "need_init",
            "r": None, "r1": None, "r2": None}
server_r_ip: str
server_r1_ip: str
server_r2_ip: str
r: redis.StrictRedis        # real redis
r1: redis.StrictRedis       # bunny-redis 1 (with rock）
r2: redis.StrictRedis       # bunny-redis 2 (only mem)


def init_common_redis(r_ip, r1_ip, r2_ip):
    global server_r_ip, server_r1_ip, server_r2_ip
    server_r_ip = r_ip
    server_r1_ip = r1_ip
    server_r2_ip = r2_ip

    global g_common
    g_common["server_r_ip"] = server_r_ip
    g_common["server_r1_ip"] = server_r1_ip
    g_common["server_r2_ip"] = server_r2_ip

    pool = redis.ConnectionPool(host=server_r_ip,
                                port=real_redis_port,
                                db=0,
                                decode_responses=True,
                                encoding='utf-8',
                                socket_connect_timeout=2)

    global r
    r = redis.StrictRedis(connection_pool=pool)
    g_common["r"] = r

    pool1 = redis.ConnectionPool(host=server_r1_ip,
                                 port=bunny_node1_port,
                                 db=0,
                                 decode_responses=True,
                                 encoding='utf-8',
                                 socket_connect_timeout=2)
    global r1
    r1 = redis.StrictRedis(connection_pool=pool1)
    g_common["r1"] = r1

    pool2 = redis.ConnectionPool(host=server_r2_ip,
                                 port=bunny_node2_port,
                                 db=0,
                                 decode_responses=True,
                                 encoding='utf-8',
                                 socket_connect_timeout=2)
    global r2
    r2 = redis.StrictRedis(connection_pool=pool2)
    g_common["r2"] = r2


def call_with_time(fn, *args):
    start = time.time()
    fn(*args)
    end = time.time()
    print("{:>19}{:<8} = {:<6},    elapse = {:<3} (seconds)".format(fn.__name__, "("+str(args[0])+")" if len(args) else "()", "Done", int(end-start)))


def compare_key_by_dump():
    check_pool = redis.ConnectionPool(host=server_r_ip,
                                      port=real_redis_port,
                                      db=0,
                                      decode_responses=True,
                                      encoding='latin1',
                                      socket_connect_timeout=2)
    check_r = redis.StrictRedis(connection_pool=check_pool)

    # BunnyRedis node 1
    check_pool1 = redis.ConnectionPool(host=server_r1_ip,
                                       port=bunny_node1_port,
                                       db=0,
                                       decode_responses=True,
                                       encoding='latin1',
                                       socket_connect_timeout=2)
    check_r1 = redis.StrictRedis(connection_pool=check_pool1)

    # BunnyRedis node 2
    check_pool2 = redis.ConnectionPool(host=server_r2_ip,
                                       port=bunny_node2_port,
                                       db=0,
                                       decode_responses=True,
                                       encoding='latin1',
                                       socket_connect_timeout=2)
    check_r2 = redis.StrictRedis(connection_pool=check_pool2)

    time.sleep(2)  # waiting all sync finished
    db_sz = check_r.dbsize()
    db1_sz = check_r1.dbsize()
    db2_sz = check_r2.dbsize()
    if db_sz != db1_sz or db_sz != db2_sz:
        print(f"db size not equal, db_sz = {db_sz}, db1_sz = {db1_sz}, db2_sz = {db2_sz}")
        raise RuntimeError("fail")

    keys = check_r.keys(pattern="*")
    for key in keys:
        v = check_r.dump(name=key)
        v1 = check_r1.dump(name=key)
        v2 = check_r2.dump(name=key)
        if v != v1 or v != v2:
            print(f"db dump fail, key = {key}, v = {v}, v1 = {v1}, v2 = {v2}")
            raise RuntimeError("fail")


def get_redis_instance(dbi):
    p = redis.ConnectionPool(host=server_r_ip,
                             port=real_redis_port,
                             db=dbi,
                             decode_responses=True,
                             encoding='latin1',
                             socket_connect_timeout=2)
    p1 = redis.ConnectionPool(host=server_r1_ip,
                              port=bunny_node1_port,
                              db=dbi,
                              decode_responses=True,
                              encoding='latin1',
                              socket_connect_timeout=2)
    p2 = redis.ConnectionPool(host=server_r2_ip,
                              port=bunny_node2_port,
                              db=dbi,
                              decode_responses=True,
                              encoding='latin1',
                              socket_connect_timeout=2)
    cr = redis.StrictRedis(connection_pool=p)
    cr1 = redis.StrictRedis(connection_pool=p1)
    cr2 = redis.StrictRedis(connection_pool=p2)

    return cr, cr1, cr2


def check_db_size():
    for dbi in range(0, 16):
        cr, cr1, cr2 = get_redis_instance(dbi)

        sz = cr.dbsize()
        sz1 = cr1.dbsize()
        sz2 = cr2.dbsize()

        if sz != sz1 or sz != sz2:
            print(f"db size check failed, dbi = {dbi}, sz = {sz}, sz1 = {sz1}, sz2 = {sz2}")
            return False

    return True


def find_diff_dict_and_print(title: str, d1: dict, d2: dict):
    if len(d1) != len(d2):
        print(f"{title}, len failed, one len = {len(d1)}, the other len = {len(d2)}")
        return False

    for f1, v1 in d1.items():
        v2 = d2.get(f1)
        if v2 is None:
            print(f"{title}, field {f1} in one dict and not in another dict")
            return False
        elif v1 != v2:
            print(f"{title}, for field {f1}, value not match, one = {v1}, other = {v2}")
            return False

    return True


def compare_all():
    time.sleep(1)   # time may be needed for sync of node1 and node2

    if not check_db_size():
        raise RuntimeError("fail")

    for dbi in range(0, 16):
        cr, cr1, cr2 = get_redis_instance(dbi)

        keys = cr.keys()
        for key in keys:
            t = cr.type(name=key)
            t1 = cr1.type(name=key)
            t2 = cr2.type(name=key)

            if t != t1 or t != t2:
                print(f"key {key} type check failed, t = {t}, t1 = {t1}, t2 = {t2}")
                raise RuntimeError("fail")

            if t == "hash":
                h = cr.hgetall(name=key)
                h1 = cr1.hgetall(name=key)
                h2 = cr2.hgetall(name=key)
                if h != h1 or h != h2:
                    find_diff_dict_and_print(f"compare hash h and h1, key = {key}", h, h1)
                    find_diff_dict_and_print(f"compare hash h and h2, key = {key}", h, h2)
                    raise RuntimeError("fail")
            elif t == "string":
                s = cr.get(name=key)
                s1 = cr1.get(name=key)
                s2 = cr2.get(name=key)
                if s != s1 or s != s2:
                    print(f"key {key} string content failed, s = {s}, s1 = {s1}, s2 = {s2}")
                    raise RuntimeError("fail")
            else:
                print(f"key {key}, not correct type = {t}")
                raise RuntimeError("fail")


def flush_all_db():
    r.flushall()
    r1.flushall()


def _main():
    pass


if __name__ == '__main__':
    _main()