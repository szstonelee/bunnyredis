import redis
import random
import string


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


key_scope = 50


def inject():
    # Need to see the rock value exist in BunnyRedis
    print(f"start to inject string, key_scope = {key_scope}")
    for i in range(0, key_scope):
        key = "str_" + str(i)
        # 10% is str of OBJ_ENCODING_INT
        if random.randint(0, 9) == 0:
            val = str(random.randint(0, 1000))
        else:
            val_len = random.randint(2, 2000)
            val = random.choice(string.ascii_letters) * val_len

        r.set(name=key, value=val)
        r1.set(name=key, value=val)
    print(f"inject finish, total key num = {key_scope}", )
    return True


def check_dump():
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


def flush_all_db():
    r.flushall()
    r1.flushall()
    r2.flushall()


def _main():
    flush_all_db()
    inject()
    check_dump()


if __name__ == '__main__':
    _main()