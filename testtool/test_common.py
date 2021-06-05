import redis
import time


server_ip = "192.168.64.4"
real_redis_port = 8888
bunny_node1_port = 6379
bunny_node2_port = 6380

pool = redis.ConnectionPool(host=server_ip,
                            port=real_redis_port,
                            db=0,
                            decode_responses=True,
                            encoding='utf-8',
                            socket_connect_timeout=2)
r = redis.StrictRedis(connection_pool=pool)


# BunnyRedis node 1
pool1 = redis.ConnectionPool(host=server_ip,
                             port=bunny_node1_port,
                             db=0,
                             decode_responses=True,
                             encoding='utf-8',
                             socket_connect_timeout=2)
r1 = redis.StrictRedis(connection_pool=pool1)

# BunnyRedis node 2
pool2 = redis.ConnectionPool(host=server_ip,
                             port=bunny_node2_port,
                             db=0,
                             decode_responses=True,
                             encoding='utf-8',
                             socket_connect_timeout=2)
r2 = redis.StrictRedis(connection_pool=pool2)


def call_with_time(fn, *args):
    start = time.time()
    fn_res = fn(*args)
    end = time.time()
    print("{:>19}{:<8} = {:<6},    elapse = {:<3} (seconds)".format(fn.__name__, "("+str(args[0])+")" if len(args) else "()", "True" if fn_res else "False", int(end-start)))


def compare_key_by_dump():
    check_pool = redis.ConnectionPool(host=server_ip,
                                      port=real_redis_port,
                                      db=0,
                                      decode_responses=True,
                                      encoding='latin1',
                                      socket_connect_timeout=2)
    check_r = redis.StrictRedis(connection_pool=check_pool)

    # BunnyRedis node 1
    check_pool1 = redis.ConnectionPool(host=server_ip,
                                       port=bunny_node1_port,
                                       db=0,
                                       decode_responses=True,
                                       encoding='latin1',
                                       socket_connect_timeout=2)
    check_r1 = redis.StrictRedis(connection_pool=check_pool1)

    # BunnyRedis node 2
    check_pool2 = redis.ConnectionPool(host=server_ip,
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
        return False

    keys = check_r.keys(pattern="*")
    for key in keys:
        v = check_r.dump(name=key)
        v1 = check_r1.dump(name=key)
        v2 = check_r2.dump(name=key)
        if v != v1 or v != v2:
            print(f"db dump fail, key = {key}, v = {v}, v1 = {v1}, v2 = {v2}")
            return False

    return True


def get_redis_instantce(dbi):
    p = redis.ConnectionPool(host=server_ip,
                             port=real_redis_port,
                             db=dbi,
                             decode_responses=True,
                             encoding='latin1',
                             socket_connect_timeout=2)
    p1 = redis.ConnectionPool(host=server_ip,
                              port=bunny_node1_port,
                              db=dbi,
                              decode_responses=True,
                              encoding='latin1',
                              socket_connect_timeout=2)
    p2 = redis.ConnectionPool(host=server_ip,
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
        cr, cr1, cr2 = get_redis_instantce(dbi)

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
        return False

    for dbi in range(0, 16):
        cr, cr1, cr2 = get_redis_instantce(dbi)

        keys = cr.keys()
        for key in keys:
            t = cr.type(name=key)
            t1 = cr1.type(name=key)
            t2 = cr2.type(name=key)

            if t != t1 or t != t2:
                print(f"key {key} type check failed, t = {t}, t1 = {t1}, t2 = {t2}")
                return False

            if t == "hash":
                h = cr.hgetall(name=key)
                h1 = cr1.hgetall(name=key)
                h2 = cr2.hgetall(name=key)
                if h != h1 or h != h2:
                    find_diff_dict_and_print(f"compare hash h and h1, key = {key}", h, h1)
                    find_diff_dict_and_print(f"compare hash h and h2, key = {key}", h, h2)
                    return False
            elif t == "string":
                s = cr.get(name=key)
                s1 = cr1.get(name=key)
                s2 = cr2.get(name=key)
                if s != s1 or s != s2:
                    print(f"key {key} string content failed, s = {s}, s1 = {s1}, s2 = {s2}")
                    return False
            else:
                print(f"key {key}, not correct type = {t}")
                return False

    return True


def _main():
    # call_with_time(compare_all)
    compare_all()


if __name__ == '__main__':
    _main()