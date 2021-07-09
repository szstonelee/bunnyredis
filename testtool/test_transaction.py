import random
from test_common import *
from test_string import inject as inject_string
from test_string import key_scope as str_key_scope
from test_hash import inject as inject_hash
from test_hash import key_scope as hash_key_scope
from test_hash import field_scope as hash_field_scope
from test_db import get_random_key as db_get_random_key
from test_db import get_random_dbid as db_get_random_dbid
import sys


r: redis.StrictRedis
r1: redis.StrictRedis
r2: redis.StrictRedis


def test_multi_exec(times):
    for _ in range(0, times):
        pipe = r.pipeline(transaction=True)
        pipe1 = r1.pipeline(transaction=True)

        # mset
        kv = {}
        for _ in (0, random.randint(1, 10)):
            key = "str_" + str(random.randint(0, str_key_scope * 2))
            val = "mset_val_" + str(random.randint(0, 9))
            kv[key] = val
        pipe.mset(kv)
        pipe1.mset(kv)

        # get
        key = "str_" + str(random.randint(0, str_key_scope * 2))
        pipe.get(key)
        pipe1.get(key)

        # hsetnx
        key = "hash_" + str(random.randint(0, hash_key_scope * 2))
        if random.randint(0, 9) % 10 == 0:
            field = "zl_" + str(random.randint(0, 20))
        else:
            field = "field_" + str(random.randint(0, hash_field_scope * 2))
        value = "value_with_hsetex_" + str(random.randint(0, 1000))
        pipe.hsetnx(name=key, key=field, value=value)
        pipe1.hsetnx(name=key, key=field, value=value)

        # hstrlen
        key = "hash_" + str(random.randint(0, hash_key_scope * 2))
        if random.randint(0, 9) % 10 == 0:
            field = "zl_" + str(random.randint(0, 20))
        else:
            field = "field_" + str(random.randint(0, hash_field_scope * 2))
        pipe.hstrlen(name=key, key=field)
        pipe1.hstrlen(name=key, key=field)

        # db move
        key = db_get_random_key()
        dbid = db_get_random_dbid()
        if dbid == 0:
            dbid = 1
        pipe.move(name=key, db=dbid)
        pipe1.move(name=key, db=dbid)

        # setbit
        # key = "str_" + str(random.randint(0, str_key_scope * 2))
        # offset = random.randint(1, 100)
        # bit = random.randint(0,1)
        # pipe.setbit(name=key, offset=offset, value=bit)
        # pipe1.setbit(name=key, offset=offset, value=bit)

        res1 = pipe1.execute()
        res = pipe.execute()

        if res != res1:
            print(f"transaction failed, res = {res}, res1 = {res1}")
            sys.exit("failed")


def config_redis():
    r1.config_set(name="bunnymem", value=50 << 20)
    r1.config_set(name="bunnydeny", value="no")
    r2.config_set(name="bunnymem", value=1 << 30)


def _main():
    ip = str(sys.argv[1])
    init_common_redis(ip)
    global r, r1, r2
    r = g_common["r"]
    r1 = g_common["r1"]
    r2 = g_common["r2"]
    config_redis()

    flush_all_db()
    call_with_time(inject_string, r, r1)
    call_with_time(inject_hash, r, r1)
    call_with_time(test_multi_exec, 10_000)
    call_with_time(compare_all)


if __name__ == '__main__':
    _main()