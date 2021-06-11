import random
from test_common import *
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

r1.config_set(name="bunnymem", value=20<<20)
r1.config_set(name="bunnydeny", value="no")
r2.config_set(name="bunnymem", value=200<<20)


key_space = 1_000


def get_fv():
    fv = dict()
    for _ in range(random.randint(1, 2)):
        field = "field_" + str(random.randint(0, 9))
        value = "hash_value_" + str(random.randint(0, 9)) * random.randint(10, 100)
        fv[field] = value
    return fv


def get_keys():
    keys = []
    for _ in range(random.randint(1,2)):
        key = "key_" + str(random.randint(0, key_space))
        keys.append(key)
    return keys


def use_compact_cmds_for_a_while(seconds):

    start = time.time()
    commands = ("del", "getdel", "getset", "hdel", "hmset", "hset", "set", "unlink")
    cnt = 0

    while True:
        elapse = int(time.time() - start)
        if elapse > seconds:
            break

        key = "key_" + str(random.randint(0, key_space))
        val = "val_" + str(random.randint(0,9)) * random.randint(10, 10_000)

        random_command = random.choice(commands)
        if random_command == "del":
            keys = get_keys()
            r.delete(*keys)
            r1.delete(*keys)
        elif random_command == "getdel":
            pass    # getdel(6.2.2) not support in redis py
        elif random_command == "getset":
            try:
                r.getset(name=key, value=val)
            except redis.exceptions.ResponseError as e:
                if str(e) == "WRONGTYPE Operation against a key holding the wrong kind of value":
                    pass
                else:
                    raise
            try:
                r1.getset(name=key, value=val)
            except redis.exceptions.ResponseError as e:
                if str(e) == "WRONGTYPE Operation against a key holding the wrong kind of value":
                    pass
                else:
                    raise
        elif random_command == "hdel":
            fields = []
            for _ in range(random.randint(1,2)):
                field = "field_" + str(random.randint(0,9))
                fields.append(field)
            try:
                r.hdel(key, *fields)
            except redis.exceptions.ResponseError as e:
                if str(e) == "WRONGTYPE Operation against a key holding the wrong kind of value":
                    pass
                else:
                    raise
            try:
                r1.hdel(key, *fields)
            except redis.exceptions.ResponseError as e:
                if str(e) == "WRONGTYPE Operation against a key holding the wrong kind of value":
                    pass
                else:
                    raise
        elif random_command == "hmset":
            fv = get_fv()
            try:
                r.hmset(name=key, mapping=fv)
            except redis.exceptions.ResponseError as e:
                if str(e) == "WRONGTYPE Operation against a key holding the wrong kind of value":
                    pass
                else:
                    raise
            try:
                r1.hmset(name=key, mapping=fv)
            except redis.exceptions.ResponseError as e:
                if str(e) == "WRONGTYPE Operation against a key holding the wrong kind of value":
                    pass
                else:
                    raise
        elif random_command == "hset":
            fv = get_fv()
            try:
                r.hset(name=key, mapping=fv)
            except redis.exceptions.ResponseError as e:
                if str(e) == "WRONGTYPE Operation against a key holding the wrong kind of value":
                    pass
                else:
                    raise
            try:
                r1.hset(name=key, mapping=fv)
            except redis.exceptions.ResponseError as e:
                if str(e) == "WRONGTYPE Operation against a key holding the wrong kind of value":
                    pass
                else:
                    raise
        elif random_command == "set":
            r.set(name=key, value=val)
            r1.set(name=key, value=val)
        elif random_command == "unlink":
            keys = get_keys()
            r.unlink(*keys)
            r1.unlink(*keys)
        else:
            raise Exception(f"no such command = {random_command}")

        cnt = cnt + 1
        if cnt % 10000 == 0:
            print(f"cnt = {cnt}, elapse(secs) = {int(elapse)}")


def _main():
    r.flushall()
    r1.flushall()
    r2.flushall()

    input("please start r1, r2 with compaction enable and hit return to go on ...")

    use_compact_cmds_for_a_while(1800)

    input("please restart r1, r2, after that, hit return to go on ...")

    call_with_time(compare_all)


if __name__ == '__main__':
    _main()