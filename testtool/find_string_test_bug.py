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
    # print(f"start to inject string, key_scope = {key_scope}")
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
    # print(f"inject finish, total key num = {key_scope}", )
    return True


def test_decr(times):
    for _ in range(0, times):
        key = "str_" + str(random.randint(0, key_scope * 2))
        is_ok = False
        res1 = None
        try:
            res1 = r1.decr(name=key, amount=1)
            is_ok = True
        except redis.exceptions.ResponseError as e:
            # because value is not integer as string
            # print(e)
            pass
        if is_ok:
            res = r.decr(name=key, amount=1)
            if res != res1:
                print(f"decr failed, key = {key}, res = {res}, res1 = {res1}")
                sys.exit("failed")


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

    for i in range(0, 10_000):
        print(f"i = {i}")
        flush_all_db()
        inject(r, r1)
        inject_res = compare_all()
        if not inject_res:
            sys.exit("  inject_res is not True")
        else:
            print("  compare for inject success.")
        test_decr(10_000)
        decr_res = compare_all()
        if not decr_res:
            sys.exit("  decr_res is not True")
        else:
            print("   compare for decr success.")


if __name__ == '__main__':
    _main()