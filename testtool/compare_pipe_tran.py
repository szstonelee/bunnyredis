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

factor = 1.05       # define 5% missing visit
rock_percentage = "60%"


def inject_string():
    r.flushall()
    r1.flushall()   # so r2 is allso flusshall()

    start = time.time()
    for i in range(0, key_scope):
        key = "str_" + str(i)
        val_len = random.randint(2, 2000)       # avg len = 1000
        val = "val_" + random.choice(string.ascii_letters) * val_len

        r.set(name=key, value=val)
        r1.set(name=key, value=val)

        if i != 0 and i%10_000 == 0:
            print(f"inject i = {i} ({int(100*i/key_scope)}%), latency(s) = {str(int(time.time() - start))}")
            start = time.time()

    print(f"inject finish, total key num = {key_scope}")


def test_read(times: int, server, no_pipe_tran: bool, is_tran: bool, env: string):
    start = time.time()
    if no_pipe_tran:
        for _ in range(0, times):
            for _ in range(0, random.randint(2, 20)):
                key = "str_" + str(random.randint(0, int(key_scope * factor)))
                server.get(name=key)
    else:
        for _ in range(0, times):
            pipe = server.pipeline(transaction=is_tran)
            for _ in range(0, random.randint(2, 20)):
                key = "str_" + str(random.randint(0, int(key_scope * factor)))
                pipe.get(name=key)
            pipe.execute()
    print("{:>35} = {:<5}(sec)".format(env, str(time.time() - start)[:5]))


def test_write(times: int, server, no_pipe_tran: bool, is_tran: bool, env: string):
    start = time.time()
    if no_pipe_tran:
        for _ in range(0, times):
            for _ in range(0, random.randint(2, 20)):
                key = "str_" + str(random.randint(0, int(key_scope * factor)))
                val = "val_" + random.choice(string.ascii_letters) * random.randint(2, 2000)
                server.set(name=key, value=val)
    else:
        for _ in range(0, times):
            pipe = server.pipeline(transaction=is_tran)
            for _ in range(0, random.randint(2, 20)):
                key = "str_" + str(random.randint(0, int(key_scope * factor)))
                val = "val_" + random.choice(string.ascii_letters) * random.randint(2, 2000)
                pipe.set(name=key, value=val)
            pipe.execute()
    print("{:>35} = {:<5}(sec)".format(env, str(time.time() - start)[:5]))


def test_write_with_read_rock(times: int, server, no_pipe_tran: bool, is_tran: bool, env: string):
    start = time.time()
    if no_pipe_tran:
        for _ in range(0, times):
            for _ in range(0, random.randint(2, 20)):
                key = "str_" + str(random.randint(0, int(key_scope * factor)))
                append_val = "7"
                server.append(key=key, value=append_val)
    else:
        for _ in range(0, times):
            pipe = server.pipeline(transaction=is_tran)
            for _ in range(0, random.randint(2, 20)):
                key = "str_" + str(random.randint(0, int(key_scope * factor)))
                append_val = "7"
                pipe.append(key=key, value=append_val)
            pipe.execute()
    print("{:>35} = {:<5}(sec)".format(env, str(time.time() - start)[:5]))


def _main():
    if len(sys.argv) > 1:
        inject_string()

    times = 10_000

    print("")

    test_read(times, r, True, None, "pure get(redis)")
    test_read(times, r, False, False, "pipe get(redis)")
    test_read(times, r1, False, False, "pipe get(bunny)")
    test_read(times, r2, False, False, f"pipe get(bunny rock {rock_percentage}")

    print("")

    test_read(times, r, True, None, "pure get(redis)")
    test_read(times, r, False, True, "transaction get(redis)")
    test_read(times, r1, False, True, "transaction get(bunny)")
    test_read(times, r2, False, True, f"transaction get(bunny rock {rock_percentage})")

    print("")

    test_write(times, r, True, None, "pure set(redis)")
    test_write(times, r, False, False, "pipe set(redis)")
    test_write(times, r1, False, False, "pipe set(bunny)")
    test_write(times, r2, False, False, f"pipe set(bunny rock {rock_percentage})")

    print("")

    test_write(times, r, True, None, "pure set(redis)")
    test_write(times, r, False, True, "transaction set(redis)")
    test_write(times, r1, False, True, "transaction set(bunny)")
    test_write(times, r2, False, True, f"transaction set(bunny rock {rock_percentage})")

    print("")

    test_write_with_read_rock(times, r, True, None, "pure append(redis)")
    test_write_with_read_rock(times, r, False, False, "pipe append(redis)")
    test_write_with_read_rock(times, r1, False, False, "pipe append(bunny)")
    test_write_with_read_rock(times, r2, False, False, f"pipe append(bunny rock {rock_percentage})")

    print("")

    test_write_with_read_rock(10_000, r, True, None, "pure append(redis)")
    test_write_with_read_rock(10_000, r, False, True, "transaction append(redis)")
    test_write_with_read_rock(10_000, r1, False, True, "transaction append(bunny)")
    test_write_with_read_rock(10_000, r2, False, True, f"transaction append(bunny rock {rock_percentage})")

    print("")


if __name__ == '__main__':
    _main()