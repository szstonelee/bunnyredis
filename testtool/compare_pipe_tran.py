import redis
import time
import random
import string
import sys


# real redis
pool = redis.ConnectionPool(host="192.168.0.11",
                            port=6379,
                            db=0,
                            decode_responses=True,
                            encoding='utf-8',
                            socket_connect_timeout=2)
r = redis.StrictRedis(connection_pool=pool)


# bunny-redis r1 all memory
pool1 = redis.ConnectionPool(host="192.168.0.22",
                             port=6379,
                             db=0,
                             decode_responses=True,
                             encoding='utf-8',
                             socket_connect_timeout=2)
r1 = redis.StrictRedis(connection_pool=pool1)
r1.config_set(name="bunnymem", value=4<<30)       # 4G
r1.config_set(name="bunnydeny", value="no")

# bunny-redis r2 with data in storage
pool2 = redis.ConnectionPool(host="192.168.0.33",
                             port=6379,
                             db=0,
                             decode_responses=True,
                             encoding='utf-8',
                             socket_connect_timeout=2)
r2 = redis.StrictRedis(connection_pool=pool2)
r2.config_set(name="bunnymem", value=1500<<20)       # 1.5G, so some value in storage
r2.config_set(name="bunnydeny", value="no")

key_scope = 3000_000        # which will make rock value in r2 is around 60%

factor = 1.1        # define 10% missing visit
rock_percentage = "60%"


def inject_string():
    r.flushall()
    r1.flushall()   # so r2 is allso flusshall()

    start = time.time()
    for i in range(0, key_scope):
        key = "str_" + str(i)
        val_len = random.randint(2, 2000)       # avg len = 1000
        val = random.choice(string.ascii_letters) * val_len

        r.set(name=key, value=val)
        r1.set(name=key, value=val)

        if i != 0 and i%10_000 == 0:
            print(f"inject i = {i} ({int(100*i/key_scope)}%), latency(s) = {str(int(time.time() - start))}")
            start = time.time()

    print(f"inject finish, total key num = {key_scope}")


def test_read(times, server, is_tran, env):
    start = time.time()
    for _ in range(0, times):
        pipe = server.pipeline(transaction=is_tran)
        for _ in range(0, random.randint(2, 20)):
            key = "str_" + str(random.randint(0, key_scope * factor))
            pipe.get(name=key)
        pipe.execute()
    print("{:>35} = {:<5}(sec)".format(env, str(time.time() - start)[:5]))


def test_write(times, server, is_tran, env):
    start = time.time()
    for _ in range(0, times):
        pipe = server.pipeline(transaction=is_tran)
        for _ in range(0, random.randint(2, 20)):
            key = "str_" + str(random.randint(0, key_scope * factor))
            val = random.choice(string.ascii_letters) * random.randint(2, 2000)
            pipe.set(name=key, value=val)
        pipe.execute()
    print("{:>35} = {:<5}(sec)".format(env, str(time.time() - start)[:5]))


def test_write_with_read_rock(times, server, is_tran, env):
    start = time.time()
    for _ in range(0, times):
        pipe = server.pipeline(transaction=is_tran)
        for _ in range(0, random.randint(2, 20)):
            key = "str_" + str(random.randint(0, key_scope * factor))
            append_val = "7"
            pipe.append(key=key, value=append_val)
        pipe.execute()
    print("{:>35} = {:<5}(sec)".format(env, str(time.time() - start)[:5]))




def _main():
    is_to_inject = sys.argv[1]
    if is_to_inject:
        inject_string()

    print("")

    test_read(10_000, r, False, "pipe get(redis)")
    test_read(10_000, r2, False, "pipe get(bunny)")
    test_read(10_000, r1, False, f"pipe get(bunny rock {rock_percentage}")

    print("")

    test_read(10_000, r, True, "transaction get(redis)")
    test_read(10_000, r2, True, "transaction get(bunny)")
    test_read(10_000, r1, True, f"transaction get(bunny rock {rock_percentage})")

    print("")

    test_write(10_000, r, False, "pipe set(redis)")
    test_write(10_000, r2, False, "pipe set(bunny)")
    test_write(10_000, r1, False, "pipe set(bunny)")

    print("")

    test_write(10_000, r, True, "transaction set(redis)")
    test_write(10_000, r2, True, "transaction set(bunny)")
    test_write(10_000, r1, True, f"transaction set(bunny rock {rock_percentage})")

    print("")

    test_write_with_read_rock(10_000, r, False, "pipe apppend(redis)")
    test_write_with_read_rock(10_000, r2, False, "pipe apppend(bunny)")
    test_write_with_read_rock(10_000, r1, False, f"pipe apppend(bunny rock {rock_percentage})")

    print("")

    test_write_with_read_rock(10_000, r, True, "transaction append(redis)")
    test_write_with_read_rock(10_000, r2, True, "transaction append(bunny)")
    test_write_with_read_rock(10_000, r1, True, f"transaction append(bunny rock {rock_percentage})")

    print("")


if __name__ == '__main__':
    _main()