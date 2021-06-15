from multiprocessing import Process
import time
import redis


def redis_process_no_tran(key):
    server_ip = "192.168.64.4"
    bunny_node1_port = 6379
    pool1 = redis.ConnectionPool(host=server_ip,
                                 port=bunny_node1_port,
                                 db=0,
                                 decode_responses=True,
                                 encoding='utf-8',
                                 socket_connect_timeout=2)
    r1 = redis.StrictRedis(connection_pool=pool1)
    r1.config_set(name="bunnydeny", value="no")
    r1.client_setname(name="_debug_")
    r1.set(name=key, value="val")
    print("redis client process(no tran): prepare to append command")
    r1.append(key=key, value="123")
    print("redis client process(no tran):: append command return")
    time.sleep(1)


def redis_process_with_tran(key):
    server_ip = "192.168.64.4"
    bunny_node1_port = 6379
    pool1 = redis.ConnectionPool(host=server_ip,
                                 port=bunny_node1_port,
                                 db=0,
                                 decode_responses=True,
                                 encoding='utf-8',
                                 socket_connect_timeout=2)
    r1 = redis.StrictRedis(connection_pool=pool1)
    r1.config_set(name="bunnydeny", value="no")
    r1.client_setname(name="_debug_")
    r1.set(name=key, value="val")
    print("redis client process(tran): prepare to append command")
    pipe = r1.pipeline(transaction=True)
    pipe.append(key=key, value="123")
    pipe.get(name=key+"_other")
    pipe.execute()
    print("redis client process(tran):: tran(with append command) command return")
    time.sleep(1)


def _main():
    key = "abc"
    #p = Process(target=redis_process_no_tran, args=(key,))
    p = Process(target=redis_process_with_tran, args=(key,))
    p.start()
    pid = p.pid
    print(f"main process, redis process pid = {pid}")
    time.sleep(0.3)
    p.kill()
    p.join()


if __name__ == '__main__':
    _main()