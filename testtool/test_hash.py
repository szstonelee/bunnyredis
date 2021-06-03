import random
import string
from test_common import *


key_scope = 400
field_scope = 10_000

# BunnyRedis node 1
r1.config_set(name="bunnymem", value=15<<20)
r1.config_set(name="bunnydeny", value="no")

# BunnyRedis node 2
r2.config_set(name="bunnymem", value=1<<30)


def inject():
    # Need to see the rock value exist in BunnyRedis
    print("start to inject, total key num = ", key_scope)
    r.flushall()
    keys = []
    for i in range(0, key_scope):
        key = "hash_" + str(i)
        keys.append(key)
    field_cnt = 0
    for fi in range(0, field_scope):
        key_index = random.randint(0, key_scope-1)
        key = keys[key_index]
        if key_index < key_scope/2:
            # ziplist, make field num and val length small enough to make ziplist. check redis.conf
            field_num = random.randint(1, 5)
            for i in range(0, field_num):
                field = "zl_" + str(i)
                if random.randint(0,1) == 0:
                    val = random.choice(string.ascii_letters)
                else:
                    val = str(random.choice(string.digits))

                added = r.hset(name=key, key=field, value=val)
                r1.hset(name=key, key=field, value=val)
                field_cnt = field_cnt + added
                if field_cnt % 10000 == 0:
                    print(f"cur field_cnt = {field_cnt}, fi = {fi}")
        else:
            # make val length bigger than hash-max-ziplist-value. check redis.conf
            field_num = random.randint(6, 10)
            for i in range(0, field_num):
                field = "field_" + str(random.randint(0, field_scope-1))
                if i == 9:
                    val = str(random.randint(0, 1000))  # field like integer
                else:
                    val_len = random.randint(65, 1000)
                    val = random.choice(string.ascii_letters) * val_len

                added = r.hset(name=key, key=field, value=val)
                r1.hset(name=key, key=field, value=val)
                field_cnt = field_cnt + added
                if field_cnt % 10000 == 0:
                    print(f"cur field_cnt = {field_cnt}, fi = {fi}")

    print(f"inject hash finish, key num = {key_scope}, field total cnt = {field_cnt}")
    return True


def test_hget(times):
    for _ in range(0, times):
        key = "hash_" + str(random.randint(0, key_scope * 2))
        if random.randint(0, 1) == 0:
            field = "zl_" + str(random.randint(0, 10))
        else:
            field = "field_" + str(random.randint(0, field_scope *2))
        res = r.hget(name=key, key=field)
        res1 = r1.hget(name=key, key=field)
        if res != res1:
            print(f"get failed, key = {key}, field = {field}, res = {res}, res1 = {res1}")
            return False

    return True

def test_hexists(times):
    for _ in range(0, times):
        key = "hash_" + str(random.randint(0, key_scope * 2))
        if random.randint(0, 1) == 0:
            field = "zl_" + str(random.randint(0, 10))
        else:
            field = "field_" + str(random.randint(0, field_scope *2))
        res = r.hexists(name=key, key=field)
        res1 = r1.hexists(name=key, key=field)
        if res != res1:
            print(f"hexists failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_hgetall(times):
    for _ in range(0, times):
        key = "hash_" + str(random.randint(0, key_scope * 2))
        res = r.hgetall(name=key)
        res1 = r1.hgetall(name=key)
        if res != res1:
            print(f"hgetall failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_hkeys(times):
    for _ in range(0, times):
        key = "hash_" + str(random.randint(0, key_scope * 2))
        res = r.hkeys(name=key)
        res1 = r1.hkeys(name=key)
        if len(res) != len(res1):
            print(f"hkeys failed, len not correct, key = {key}, res = {res}, res1 = {res1}")
            return False
        else:
            for field in res:
                exist = False
                for check in res1:
                    if field == check:
                        exist = True
                        break
                if not exist:
                    print(f"hkeys failed, field not in it, key = {key}, field = {field}")
                    return False

    return True


def test_hlen(times):
    for _ in range(0, times):
        key = "hash_" + str(random.randint(0, key_scope * 2))
        res = r.hlen(name=key)
        res1 = r1.hlen(name=key)
        if res != res1:
            print(f"hkeys failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_hmget(times):
    for _ in range(0, times):
        key = "hash_" + str(random.randint(0, key_scope * 2))
        field_num = random.randint(1, 100)
        fields = []
        for i in range(field_num):
            if i % 10 == 0:
                field = "zl_" + str(random.randint(0, 20))
            else:
                field = "field_" + str(random.randint(0, field_scope *2))
            fields.append(field)
        res = r.hmget(name=key, keys=fields)
        res1 = r1.hmget(name=key, keys=fields)

        if res != res1:
            print(f"hmget failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def test_hvals(times):
    for _ in range(0, times):
        key = "hash_" + str(random.randint(0, key_scope * 2))
        res = r.hvals(name=key)
        res1 = r1.hvals(name=key)
        if len(res) != len(res1):
            print(f"hvals failed for len, key = {key}, len(res) = {len(res)}, len(res1) = {len(res1)}")
            return False
        set_res = set(res)
        set_res1 = set(res1)
        for val in res:
            if val not in set_res1:
                print(f"hvals failed, in key {key}, val {val} not in set_res1")
                return False
        for val1 in res1:
            if val1 not in set_res:
                print(f"hvals failed, in key {key}, val1 {val1} not in set_res")
                return False

    return True


def test_hstrlen(times):
    for _ in range(0, times):
        key = "hash_" + str(random.randint(0, key_scope * 2))
        if random.randint(0,9) % 10 == 0:
            field = "zl_" + str(random.randint(0, 20))
        else:
            field = "field_" + str(random.randint(0, field_scope *2))
        res = r.hstrlen(name=key, key=field)
        res1 = r1.hstrlen(name=key, key=field)

        if res != res1:
            print(f"hstrlen failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def debug_hget():
    keys = r.keys("*")
    for key in keys:
        en = r.object(infotype="encoding", key=key)
        if en != "ziplist":
            continue

        fields = r.hkeys(name=key)
        all_same = True
        for field in fields:
            res = r.hget(name=key, key=field)
            res1 = r2.hget(name=key, key=field)
            if res != res1:
                all_same = False
        if all_same:
            print(f"all same for key = {key}")

    return True


def _main():
    #call_with_time(inject)
    #debug_hget()
    #call_with_time(test_hget, 100_000)
    #call_with_time(test_hexists, 100_000)
    #call_with_time(test_hgetall, 100_000)
    #call_with_time(test_hkeys, 100_000)
    #call_with_time(test_hlen, 100_000)
    #call_with_time(test_hmget, 20_000)
    #call_with_time(test_hvals, 100_000)
    call_with_time(test_hstrlen, 100_000)


if __name__ == '__main__':
    _main()