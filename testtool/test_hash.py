import random
import string
from test_common import *


key_scope = 200
field_scope = 50_000


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
                r1.hset(name=key, key=field,value=val)
                field_cnt = field_cnt + added
                if field_cnt % 1000 == 0:
                    print(f"cur field_cnt = {field_cnt}, fi = {fi}")

        else:
            # make val length bigger than hash-max-ziplist-value. check redis.conf
            field_num = random.randint(1, 10)
            for i in range(0, field_num):
                field = "field_" + str(random.randint(0, field_scope-1))
                if i == 9:
                    val = str(random.randint(0, 1000))  # field like integer
                else:
                    val_len = random.randint(65, 1000)
                    val = random.choice(string.ascii_letters) * val_len

                added = r.hset(name=key, key=field, value=val)
                r1.hset(name=key, key=field,value=val)
                field_cnt = field_cnt + added
                if field_cnt % 1000 == 0:
                    print(f"cur field_cnt = {field_cnt}, fi = {fi}")

    print(f"inject hash finish, key num = {key_scope}, field total cnt = {field_cnt}")
    return True


def test_hget(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        if random.randint(0, 1) == 0:
            field = "zl_" + str(random.randint(0, 10))
        else:
            field = "field_" + str(random.randint(0, field_scope *2))
        res = r.hget(name=key, key=field)
        res1 = r1.hget(name=key, key=field)
        if res != res1:
            print(f"get failed, key = {key}, res = {res}, res1 = {res1}")
            return False

    return True


def _main():
    call_with_time(inject)
    call_with_time(test_hget, 1000_000)


if __name__ == '__main__':
    _main()