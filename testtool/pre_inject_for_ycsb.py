import sys
import redis
import random
import threading


# [start, end)
def thread_func(r: redis.StrictRedis, start, end):
    for i in range(start, end):
        key = "key_" + str(i)
        val = "val_" + str(random.randint(0,9)) * random.randint(20, 2000)
        r.set(name=key, value=val)


def _main():
    if len(sys.argv) != 6:
        print("argument number not correct, use python3 pre_inject_for_ycsb.py <ip> <port> <db> <key_num> <thread_num>")
        return

    ip = str(sys.argv[1])
    port = str(sys.argv[2])
    db = int(sys.argv[3])
    key_num = int(sys.argv[4])
    thread_num = int(sys.argv[5])

    pool = redis.ConnectionPool(host=ip,
                                port=port,
                                db=db,
                                decode_responses=True,
                                encoding='utf-8',
                                socket_connect_timeout=2)
    r = redis.StrictRedis(connection_pool=pool)

    scope = int(key_num/thread_num)
    start = 0
    threads = []
    for tid in range(0, thread_num):
        if tid != thread_num - 1:
            end = start + scope
        else:
            end = key_num
        thread = threading.Thread(target=thread_func, args=(r, start, end))
        thread.start()
        threads.append(thread)
        start = end

    for t in threads:
        t.join()

    print(f"finish inject for ycsb test, key num = {key_num}")


if __name__ == '__main__':
    _main()