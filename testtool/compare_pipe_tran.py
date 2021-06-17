from test_common import flush_all_db
from test_common import r, r1, r2
from test_string import inject
from test_string import key_scope
import time
import random
import string


factor = 1.2        # define 20% missing visit


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
    flush_all_db()
    inject()

    print("")

    test_read(10_000, r, False, "pipe get(redis)")
    test_read(10_000, r2, False, "pipe get(bunny)")
    test_read(10_000, r1, False, "pipe get(bunny rock 60%)")

    print("")

    test_read(10_000, r, True, "transaction get(redis)")
    test_read(10_000, r2, True, "transaction get(bunny)")
    test_read(10_000, r1, True, "transaction get(bunny rock 60%)")

    print("")

    test_write(10_000, r, False, "pipe set(redis)")
    test_write(10_000, r2, False, "pipe set(bunny)")
    test_write(10_000, r1, False, "pipe set(bunny)")

    print("")

    test_write(10_000, r, True, "transaction set(redis)")
    test_write(10_000, r2, True, "transaction set(bunny)")
    test_write(10_000, r1, True, "transaction set(bunny rock 60%)")

    print("")

    test_write_with_read_rock(10_000, r, False, "pipe apppend(redis)")
    test_write_with_read_rock(10_000, r2, False, "pipe apppend(bunny)")
    test_write_with_read_rock(10_000, r1, False, "pipe apppend(bunny rock 60%)")

    print("")

    test_write_with_read_rock(10_000, r, True, "transaction append(redis)")
    test_write_with_read_rock(10_000, r2, True, "transaction append(bunny)")
    test_write_with_read_rock(10_000, r1, True, "transaction append(bunny rock 60%)")

    print("")


if __name__ == '__main__':
    _main()