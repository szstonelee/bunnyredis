import string
import random
from test_common import *
import sys

key_scope = 50_000
r: redis.StrictRedis
r1: redis.StrictRedis
r2: redis.StrictRedis


def inject(r: redis.StrictRedis, r1: redis.StrictRedis):
    # Need to see the rock value exist in BunnyRedis
    for i in range(0, key_scope):
        key = "str_" + str(i)
        # 10% is str of OBJ_ENCODING_INT
        if random.randint(0, 9) == 0:
            val = str(random.randint(0, 1000))
        else:
            val_len = random.randint(2, 2000)   # NOTE: maybe int
            val = random.choice(string.ascii_letters) * val_len

        r.set(name=key, value=val)
        r1.set(name=key, value=val)
    return True


def test_append(times):
    for _ in range(0, times):
        append_val = "_append"
        key = "str_" + str(random.randint(0, key_scope*2))
        res = r.append(key=key, value=append_val)
        res1 = r1.append(key=key, value=append_val)
        if res != res1:
            print(f"append failed, key = {key}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


def test_bitcount(times):
    for _ in range(0, times):
        start = random.randint(0, 5)
        end = start + random.randint(0, 100)
        key = "str_" + str(random.randint(0, key_scope*2))
        res = r.bitcount(key=key, start=start, end=end)
        res1 = r1.bitcount(key=key, start=start, end=end)
        if res != res1:
            print(f"bitcount failed, key = {key}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


def test_bitfield(times):
    for _ in range(0, times):
        my_type = "i16"
        offset = random.randint(1, 100)
        dice = random.randint(0, 2)
        key = "str_" + str(random.randint(0, key_scope * 2))
        bf = r.bitfield(key=key)
        bf1 = r1.bitfield(key=key)
        if dice == 0:   # GET
            res = bf.get(fmt=my_type, offset=offset).execute()
            res1 = bf1.get(fmt=my_type, offset=offset).execute()
            if res != res1:
                print(f"bitfield get failed, key = {key}, res = {res}, res1 = {res1}")
                raise RuntimeError("fail")
        elif dice == 1:  #SET
            val = random.randint(0, 100)
            res = bf.set(fmt=my_type, offset=offset, value=val).execute()
            res1 = bf1.set(fmt=my_type, offset=offset, value=val).execute()
            if res != res1:
                print(f"bitfield set failed, key = {key}, res = {res}, res1 = {res1}")
                raise RuntimeError("fail")
        else:   #INCRBY
            increment = random.randint(0, 100)
            res = bf.incrby(fmt=my_type, offset=offset, increment=increment).execute()
            res1 = bf1.incrby(fmt=my_type, offset=offset, increment=increment).execute()
            if res != res1:
                print(f"bitfield incrby failed, key = {key}, res = {res}, res1 = {res1}")
                raise RuntimeError("fail")


def test_bitop(times):
    for _ in range(0, times):
        dest_key = "str_" + str(random.randint(0, key_scope * 2))
        keys = []
        for _ in (0, random.randint(1,10)):
            key = "str_" + str(random.randint(0, key_scope * 2))
            keys.append(key)
        ops = ("AND", "OR", "XOR", "NOT")
        op = ops[random.randint(0, 3)]
        if op != "NOT":
            res1 = r1.bitop(op, dest_key, *keys)
            res = r.bitop(op, dest_key, *keys)
        else:
            res1 = r1.bitop(op, dest_key, keys[0])
            res = r.bitop(op, dest_key, keys[0])
        if res != res1:
            print(f"bitop failed, key = {key}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


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
            raise RuntimeError("fail")


def test_decr(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        try:
            res = r.decr(name=key, amount=1)
            try:
                res1 = r1.decr(name=key, amount=1)
                if res != res1:
                    print(f"decr failed, key = {key}, res = {res}, res1 = {res1}")
                    raise RuntimeError("fail")
            except redis.exceptions.ResponseError as e:
                raise RuntimeError("fail") from e
        except redis.exceptions.ResponseError as e:
            if str(e) != "value is not an integer or out of range":
                raise e


def test_incr(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        try:
            res = r.incr(name=key, amount=1)
            try:
                res1 = r1.incr(name=key, amount=1)
                if res != res1:
                    print(f"incr failed, key = {key}, res = {res}, res1 = {res1}")
                    raise RuntimeError("fail")
            except redis.exceptions.ResponseError as e:
                raise RuntimeError("fail") from e
        except redis.exceptions.ResponseError as e:
            if str(e) != "value is not an integer or out of range":
                raise e


def test_incrbyfloat(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        try:
            res = r.incrbyfloat(name=key, amount=2.2)
            try:
                res1 = r1.incrbyfloat(name=key, amount=2.2)
                if res != res1:
                    print(f"incrbyfloat failed, key = {key}, res = {res}, res1 = {res1}")
                    raise RuntimeError("fail")
            except redis.exceptions.ResponseError as e:
                raise RuntimeError("fail") from e
        except redis.exceptions.ResponseError as e:
            if str(e) != "value is not a valid float":
                raise e


def test_get(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        res = r.get(key)
        res1 = r1.get(key)
        if res != res1:
            print(f"get failed, key = {key}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


def test_getbit(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        offset = random.randint(1, 100)
        res = r.getbit(name=key, offset=offset)
        res1 = r1.getbit(name=key, offset=offset)
        if res != res1:
            print(f"getbit failed, key = {key}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


def test_getrange(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        start = random.randint(0, 5)
        end = start + random.randint(0, 100)
        res = r.getrange(key=key, start=start, end=end)
        res1 = r1.getrange(key=key, start=start, end=end)
        if res != res1:
            print(f"getrange failed, key = {key}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


def test_setrange(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        val = "setrange_" + str(random.randint(0,99))
        offset = random.randint(1, 100)
        res = r.setrange(name=key, offset=offset, value=val)
        res1 = r1.setrange(name=key, offset=offset, value=val)
        if res != res1:
            print(f"setrange failed, key = {key}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


def test_getset(times):
    for _ in range(0, times):
        set_val = "_setval" + str(random.randint(0,9))
        key = "str_" + str(random.randint(0, key_scope*2))
        res = r.getset(name=key, value=set_val)
        res1 = r1.getset(name=key, value=set_val)
        if res != res1:
            print(f"getset failed, key = {key}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


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
            raise RuntimeError("fail")


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
            raise RuntimeError("fail")


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
            raise RuntimeError("fail")


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
            raise RuntimeError("fail")


def test_setnx(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        val = "set_val_" + str(random.randint(0,9))
        res = r.setnx(name=key, value=val)
        res1 = r1.setnx(name=key, value=val)

        if res != res1:
            print(f"setnx failed, key = {key}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


def test_strlen(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        res = r.strlen(name=key)
        res1 = r1.strlen(name=key)

        if res != res1:
            print(f"strlen failed, key = {key}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


def test_setbit(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        offset = random.randint(1, 100)
        bit = random.randint(0,1)
        res = r.setbit(name=key, offset=offset, value=bit)
        res1 = r1.setbit(name=key, offset=offset, value=bit)
        if res != res1:
            print(f"setbit failed, key = {key}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


def config_redis():
    r1.config_set(name="bunnymem", value=20 << 20)
    r1.config_set(name="bunnydeny", value="no")
    r2.config_set(name="bunnymem", value=200 << 20)


def _main():
    r_ip = str(sys.argv[1])
    r1_ip = str(sys.argv[2])
    r2_ip = str(sys.argv[3])

    init_common_redis(r_ip, r1_ip, r2_ip)

    global r, r1, r2
    r = g_common["r"]
    r1 = g_common["r1"]
    r2 = g_common["r2"]
    config_redis()

    flush_all_db()
    call_with_time(inject, r, r1)
    call_with_time(compare_all)
    call_with_time(test_decr, 10_000)
    call_with_time(compare_all)
    call_with_time(test_incr, 10_000)
    call_with_time(compare_all)
    call_with_time(test_incrbyfloat, 10_000)
    call_with_time(compare_all)
    call_with_time(test_append, 5000)
    call_with_time(compare_all)
    call_with_time(test_get, 5000)
    call_with_time(test_getset, 5000)
    call_with_time(compare_all)
    call_with_time(test_mget, 5000)
    call_with_time(test_mset, 5000)
    call_with_time(compare_all)
    call_with_time(test_msetnx, 5000)
    call_with_time(compare_all)
    call_with_time(test_set, 5000)
    call_with_time(compare_all)
    call_with_time(test_setnx, 5000)
    call_with_time(compare_all)
    call_with_time(test_strlen, 5000)
    call_with_time(test_setrange, 5000)
    call_with_time(test_getrange, 5000)
    call_with_time(test_bitcount, 5000)
    call_with_time(test_bitfield, 5000)
    call_with_time(test_bitop, 5000)
    call_with_time(compare_all)
    call_with_time(test_bitpos, 5000)
    call_with_time(test_setbit, 5000)
    call_with_time(compare_all)
    call_with_time(test_getbit, 5000)
    call_with_time(compare_key_by_dump)


if __name__ == '__main__':
    _main()