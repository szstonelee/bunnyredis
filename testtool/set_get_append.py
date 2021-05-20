import sys
import random
import redis
import time


def generate_key_vals(key_num, key_prefix):
    kvs = {}
    for _ in range(0, key_num):
        rand_str = str(random.randint(0, 10*key_num))
        key = key_prefix + "_" + rand_str
        val = "val_" + rand_str + "_" + "x"*1000
        kvs[key] = val
    return kvs


def _main():
    if len(sys.argv) != 6:
        print("argument number not correct, use python3 set_get_append.py <ip> <port> <db> <key_num> <key_prefix> ")
        return

    ip = str(sys.argv[1])
    port = str(sys.argv[2])
    db = int(sys.argv[3])
    key_num = int(sys.argv[4])
    key_prefix = str(sys.argv[5])

    kvs = generate_key_vals(key_num, key_prefix)

    common_kvs = {"abc":"val_abc", "kkk":"val_kkk"}

    pool = redis.ConnectionPool(host=ip,
                             port=port,
                             db=db,
                             decode_responses=True,
                             encoding='utf-8',
                             socket_connect_timeout=2)
    r = redis.StrictRedis(connection_pool=pool)

    # fist inject all kvs
    it = iter(kvs)
    while True:
        try:
            key = next(it)
        except StopIteration:
            break
        val = kvs[key]
        while True:
            try:
                r.set(name=key, value=val)
                break
            except redis.exceptions.ResponseError:
                time.sleep(0.01)

    print("inject key finished")

    # loop and check
    cnt = 0
    timer = time.perf_counter()
    it = iter(kvs)
    while True:
        try:
            key = next(it)
        except StopIteration:
            it = iter(kvs)
            key = next(it)
        val = kvs[key]

        dice = random.randint(1, 3)     # we test without append

        if dice == 1:
            # for common kvs
            for k,v in common_kvs.items():
                try:
                    r.set(name=k, value=v)
                except redis.exceptions.ResponseError as e:
                    print(e, time.strftime("%M:%S", time.localtime()))
                    time.sleep(0.1)
        elif dice == 2:
            # for get
            redis_val = r.get(name=key)
            if redis_val != val:
                print("get test failed, key = " + key)
                exit(1)
        elif dice == 3:
            # for set
            try:
                r.set(name=key, value=val)
            except redis.exceptions.ResponseError as e:
                print(e, time.strftime("%M:%S", time.localtime()))
                time.sleep(0.1)
        elif dice == 4:
            # for append
            append_str = "a"
            try:
                r.append(key=key, value=append_str)
                kvs[key] += append_str
            except redis.exceptions.ResponseError as e:
                print(e)
                time.sleep(0.1)
        else:
            print("no defined dice = " + dice)
            exit(1)

        cnt += 1
        if cnt == 10000:
            elapse = time.perf_counter() - timer
            print("qps = ", (int)(float(cnt)/elapse))

            cnt = 0
            timer = time.perf_counter()


if __name__ == '__main__':
    _main()