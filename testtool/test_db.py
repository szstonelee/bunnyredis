import random
from test_common import *
from test_string import inject as inject_string
from test_string import key_scope as str_key_scope
from test_hash import inject as inject_hash
from test_hash import key_scope as hash_key_scope
import sys


r: redis.StrictRedis
r1: redis.StrictRedis
r2: redis.StrictRedis


def get_random_key():
    dice = random.randint(0, 4)
    if dice == 0:
        key = "hash_" + str(random.randint(0, hash_key_scope+10))
    else:
        key = "str_" + str(random.randint(0, str_key_scope+1000))
    return key


def get_random_dbid():
    dbid = 0
    if random.randint(0, 9) == 0:
        dbid = random.randint(1, 15)
    return dbid


#  NOTE: redis py not support copy right now
def test_copy(times):
    for _ in range(0, times):
        src_key = get_random_key()
        dst_key = get_random_key()
        dbid = get_random_dbid()
        replace = False
        if random.randint(0, 1) == 0:
            replace = True
        if dbid == 0:
            pass


def test_move(times):
    for _ in range(0, times):
        key = get_random_key()
        dbid = get_random_dbid()
        if dbid == 0:
            continue
        res = r.move(name=key, db=dbid)
        res1 = r1.move(name=key, db=dbid)
        if res != res1:
            print(f"move failed for key {key}, dbid = {dbid}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


def test_exists(times):
    for _ in range(0, times):
        keys = []
        for _ in range(0, random.randint(5, 10)):
            key = get_random_key()
            keys.append(key)
        res = r.exists(*keys)
        res1 = r1.exists(*keys)
        if res != res1:
            print(f"exists failed for key {keys}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


def test_object(times):
    for _ in range(0, times):
        key = get_random_key()
        ops = ("REFCOUNT", "ENCODING")
        op = random.choice(ops)
        res = r.object(key=key, infotype=op)
        res1 = r1.object(key=key, infotype=op)
        if res != res1:
            print(f"object failed for key {key}, op = {op}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


def test_keys(times):
    for _ in range(0, times):
        if random.randint(0,1) == 0:
            pattern = "hash_" + str(random.randint(0,9)) + "*"
        else:
            pattern = "string_" + str(random.randint(0,9)) + "*"
        res = r.keys(pattern=pattern)
        res1 = r1.keys(pattern=pattern)
        res = sorted(res)
        res1 = sorted(res1)
        if res != res1:
            print(f"keys failed for pattern = {pattern}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


def test_randomkey(times):
    for _ in range(0, times):
        key = r1.randomkey()
        if not r.exists(key):
            print(f"randomkey failed for key {key}")
            raise RuntimeError("fail")


def test_rename(times):
    for _ in range(0, times):
        src_key = get_random_key()
        dst_key = get_random_key()
        if src_key != dst_key:
            try:
                res = r.rename(src_key, dst_key)
                try:
                    res1 = r1.rename(src_key, dst_key)
                    if res != res1:
                        print(f"rename failed for src_key = {src_key}, dst_key = {dst_key}, res = {res}, res1 = {res1}")
                        raise RuntimeError("fail")
                except redis.exceptions.ResponseError as e:
                    raise RuntimeError("fail") from e
            except redis.exceptions.ResponseError as e:
                if str(e) != "no such key":
                    raise e


def test_renamenx(times):
    for _ in range(0, times):
        src_key = get_random_key()
        dst_key = get_random_key()
        if src_key != dst_key:
            try:
                res = r.renamenx(src=src_key, dst=dst_key)
                try:
                    res1 = r1.renamenx(src=src_key, dst=dst_key)
                    if res != res1:
                        print(f"rename failed for src_key = {src_key}, dst_key = {dst_key}, res = {res}, res1 = {res1}")
                        raise RuntimeError("fail")
                except redis.exceptions.ResponseError as e:
                    raise RuntimeError("fail") from e
            except redis.exceptions.ResponseError as e:
                if str(e) != "no such key":
                    raise e


def test_touch(times):
    for _ in range(0, times):
        keys = []
        for _ in range(0, random.randint(5, 10)):
            key = get_random_key()
            keys.append(key)
        res = r.touch(*keys)
        res1 = r1.touch(*keys)
        if res != res1:
            print(f"touch failed for key {keys}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


def test_type(times):
    for _ in range(0, times):
        key = get_random_key()
        res = r.type(name=key)
        res1 = r1.type(name=key)
        if res != res1:
            print(f"type failed for key {key}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


def test_del(times):
    for _ in range(0, times):
        keys = []
        for _ in range(0, random.randint(2, 4)):
            key = get_random_key()
            keys.append(key)
        res = r.delete(*keys)
        res1 = r1.delete(*keys)
        if res != res1:
            print(f"delete failed for keys = {keys}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


def test_unlink(times):
    for _ in range(0, times):
        keys = []
        for _ in range(0, random.randint(2, 4)):
            key = get_random_key()
            keys.append(key)
        res = r.unlink(*keys)
        res1 = r1.unlink(*keys)
        if res != res1:
            print(f"unlink failed for keys = {keys}, res = {res}, res1 = {res1}")
            raise RuntimeError("fail")


def config_redis():
    r1.config_set(name="bunnymem", value=50 << 20)
    r1.config_set(name="lazyfree-lazy-server-del", value="yes")
    r1.config_set(name="bunnydeny", value="no")
    r2.config_set(name="bunnymem", value=1 << 30)


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
    call_with_time(inject_string, r, r1)
    call_with_time(inject_hash, r, r1)
    call_with_time(compare_all)
    call_with_time(test_move, 100)
    call_with_time(compare_all)
    call_with_time(test_exists, 10_000)
    call_with_time(test_unlink, 50)
    call_with_time(compare_all)
    call_with_time(test_keys, 100)
    call_with_time(test_object, 10_000)
    call_with_time(test_randomkey, 10_000)
    call_with_time(test_rename, 10_000)
    call_with_time(compare_all)
    call_with_time(test_renamenx, 10_000)
    call_with_time(compare_all)
    call_with_time(test_touch, 10_000)
    call_with_time(test_type, 10_000)
    call_with_time(test_del, 50)
    call_with_time(compare_all)


if __name__ == '__main__':
    _main()