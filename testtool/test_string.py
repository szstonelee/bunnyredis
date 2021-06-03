import string
import random
from test_common import *


r1.config_set(name="bunnymem", value=20<<20)
r1.config_set(name="bunnydeny", value="no")
r2.config_set(name="bunnymem", value=200<<20)

key_scope = 15_000


def inject():
    # Need to see the rock value exist in BunnyRedis
    print("start to inject, total key num = ", key_scope)
    r.flushall()
    for i in range(0, key_scope):
        key = "str_" + str(i)
        # 10% is str of OBJ_ENCODING_INT
        if random.randint(0, 9) == 0:
            val = str(random.randint(0, 1000))
        else:
            val_len = random.randint(1, 10000)
            val = random.choice(string.ascii_letters) * val_len

        r.set(name=key, value=val)
        r1.set(name=key, value=val)
    print("inject finish, total key num = ", key_scope)
    return True


def test_append(times):
    for _ in range(0, times):
        append_val = "_append"
        key = "str_" + str(random.randint(0, key_scope*2))
        res = r.append(key=key, value=append_val)
        res1 = r1.append(key=key, value=append_val)
        if res != res1:
            print(f"append failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_bitcount(times):
    for _ in range(0, times):
        start = random.randint(0, 5)
        end = start + random.randint(0, 100)
        key = "str_" + str(random.randint(0, key_scope*2))
        res = r.bitcount(key=key, start=start, end=end)
        res1 = r1.bitcount(key=key, start=start, end=end)
        if res != res1:
            print(f"bitcount failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_bitfield(times):
    for _ in range(0, times):
        type = "i16"
        offset = random.randint(1, 100)
        dice = random.randint(0, 2)
        key = "str_" + str(random.randint(0, key_scope * 2))
        bf = r.bitfield(key=key)
        bf1 = r1.bitfield(key=key)
        if dice == 0:   # GET
            res = bf.get(fmt=type, offset=offset).execute()
            res1 = bf1.get(fmt=type, offset=offset).execute()
            if res != res1:
                print(f"bitfield get failed, key = {key}, res = {res}, res1 = {res1}")
                return False
        elif dice == 1:  #SET
            val = random.randint(0, 100)
            res = bf.set(fmt=type, offset=offset, value=val).execute()
            res1 = bf1.set(fmt=type, offset=offset, value=val).execute()
            if res != res1:
                print(f"bitfield set failed, key = {key}, res = {res}, res1 = {res1}")
                return False
        else:   #INCRBY
            increment = random.randint(0, 100)
            res = bf.incrby(fmt=type, offset=offset, increment=increment).execute()
            res1 = bf1.incrby(fmt=type, offset=offset, increment=increment).execute()
            if res != res1:
                print(f"bitfield incrby failed, key = {key}, res = {res}, res1 = {res1}")
                return False

    return True


def test_bitop(times):
    for _ in range(0, times):
        dest_key = "str_" + str(random.randint(0, key_scope * 2))
        keys = []
        for _ in (0, random.randint(1,10)):
            key = "str_" + str(random.randint(0, key_scope * 2))
            keys.append(key)
        index = random.randint(0, 3)
        ops = ("AND", "OR", "XOR", "NOT")
        op = ops[index]
        if op != "NOT":
            res = r.bitop(op, dest_key, *keys)
            res1 = r1.bitop(op, dest_key, *keys)
        else:
            res = r.bitop(op, dest_key, keys[0])
            res1 = r1.bitop(op, dest_key, keys[0])
        if res != res1:
            print(f"bitop failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_bitpos(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        start = random.randint(0, 5)
        end = start + random.randint(0, 100)
        bit = random.randint(0, 1)
        res = r.bitpos(key=key, bit=bit, start=start, end=end)
        res1 = r1.bitpos(key=key, bit=bit, start=start, end=end)
        if res != res1:
            print(f"bitpos failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_decr(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        is_ok = False
        res = None
        try:
            res = r.decr(name=key, amount=1)
            is_ok = True
        except redis.exceptions.ResponseError as e:
            # because value is not integer as string
            # print(e)
            pass
        if is_ok:
            res1 = r1.decr(name=key, amount=1)
            if res != res1:
                print(f"decr failed, key = {key}, res = {res}, res1 = {res1}")
                return False

    return True


def test_get(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        res = r.get(key)
        res1 = r1.get(key)
        if res != res1:
            print(f"get failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_getbit(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        offset = random.randint(1, 100)
        res = r.getbit(name=key, offset=offset)
        res1 = r1.getbit(name=key, offset=offset)
        if res != res1:
            print(f"getbit failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_getrange(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        start = random.randint(0, 5)
        end = start + random.randint(0, 100)
        res = r.getrange(key=key, start=start, end=end)
        res1 = r1.getrange(key=key, start=start, end=end)
        if res != res1:
            print(f"getrange failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_setrange(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        val = "setrange_" + str(random.randint(0,99))
        offset = random.randint(1, 100)
        res = r.setrange(name=key, offset=offset, value=val)
        res1 = r1.setrange(name=key, offset=offset, value=val)
        if res != res1:
            print(f"setrange failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_getset(times):
    for _ in range(0, times):
        set_val = "_setval" + str(random.randint(0,9))
        key = "str_" + str(random.randint(0, key_scope*2))
        res = r.getset(name=key, value=set_val)
        res1 = r1.getset(name=key, value=set_val)
        if res != res1:
            print(f"getset failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_incr(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        is_ok = False
        res = None
        try:
            res = r.incr(name=key, amount=1)
            is_ok = True
        except redis.exceptions.ResponseError as e:
            # because value is not integer as string
            #print(e)
            pass
        if is_ok:
            res1 = r1.incr(name=key, amount=1)
            if res != res1:
                print(f"incr failed, key = {key}, res = {res}, res1 = {res1}")
                return False

    return True


def test_incrbyfloat(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        is_ok = False
        res = None
        try:
            res = r.incrbyfloat(name=key, amount=2.2)
            is_ok = True
        except redis.exceptions.ResponseError as e:
            # because value is not integer as string
            # print(e)
            pass
        if is_ok:
            res1 = r1.incrbyfloat(name=key, amount=2.2)
            if res != res1:
                print(f"incrbyfloat failed, key = {key}, res = {res}, res1 = {res1}")
                return False

    return True


def test_mget(times):
    for _ in range(0, times):
        keys = []
        for _ in (0, random.randint(1,10)):
            key = "str_" + str(random.randint(0, key_scope * 2))
            keys.append(key)
        res = r.mget(keys=keys)
        res1 = r1.mget(keys=keys)
        if res != res1:
            print(f"mget failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_mset(times):
    for _ in range(0, times):
        kv = {}
        for _ in (0, random.randint(1,10)):
            key = "str_" + str(random.randint(0, key_scope * 2))
            val = "mset_val_" + str(random.randint(0,9))
            kv[key] = val
        res = r.mset(kv)
        res1 = r1.mset(kv)
        if res != res1:
            print(f"mset failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_msetnx(times):
    for _ in range(0, times):
        kv = {}
        for _ in (0, random.randint(1,10)):
            key = "str_" + str(random.randint(0, key_scope * 2))
            val = "mset_val_" + str(random.randint(0,9))
            kv[key] = val
        res = r.msetnx(kv)
        res1 = r1.msetnx(kv)
        if res != res1:
            print(f"msetnx failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_set(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        val = "set_val_" + str(random.randint(0,9))
        if random.randint(0,1) == 0:    # nx
            res = r.set(name=key, value=val, nx=True)
            res1 = r1.set(name=key, value=val, nx=True)
        else:   # xx
            res = r.set(name=key, value=val, xx=True)
            res1 = r1.set(name=key, value=val, xx=True)

        if res != res1:
            print(f"set failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_setnx(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        val = "set_val_" + str(random.randint(0,9))
        res = r.setnx(name=key, value=val)
        res1 = r1.setnx(name=key, value=val)

        if res != res1:
            print(f"setnx failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_strlen(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        res = r.strlen(name=key)
        res1 = r1.strlen(name=key)

        if res != res1:
            print(f"strlen failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_setbit(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        offset = random.randint(1, 100)
        bit = random.randint(0,1)
        res = r.setbit(name=key, offset=offset, value=bit)
        res1 = r1.setbit(name=key, offset=offset, value=bit)
        if res != res1:
            print(f"setbit failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_compare_all():
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


def _main():
    call_with_time(inject)
    call_with_time(test_decr, 100000)
    call_with_time(test_incr, 100000)
    call_with_time(test_incrbyfloat, 100000)
    call_with_time(test_append, 10000)
    call_with_time(test_get, 10000)
    call_with_time(test_getset, 10000)
    call_with_time(test_mget, 10000)
    call_with_time(test_mset, 10000)
    call_with_time(test_msetnx, 10000)
    call_with_time(test_set, 10000)
    call_with_time(test_setnx, 10000)
    call_with_time(test_strlen, 10000)
    call_with_time(test_setrange, 10000)
    call_with_time(test_getrange, 10000)
    call_with_time(test_bitcount, 10000)
    call_with_time(test_bitfield, 10000)
    call_with_time(test_bitop, 10000)
    call_with_time(test_bitpos, 10000)
    call_with_time(test_setbit, 10000)
    call_with_time(test_getbit, 10000)
    call_with_time(test_compare_all)


if __name__ == '__main__':
    _main()