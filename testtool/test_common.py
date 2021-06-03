import redis
import time


server_ip = "192.168.64.4"
real_redis_port = 8888
bunny_node1_port = 6379
bunny_node2_port = 6380

pool = redis.ConnectionPool(host=server_ip,
                            port=real_redis_port,
                            db=0,
                            decode_responses=True,
                            encoding='utf-8',
                            socket_connect_timeout=2)
r = redis.StrictRedis(connection_pool=pool)

# BunnyRedis node 1
pool1 = redis.ConnectionPool(host=server_ip,
                             port=bunny_node1_port,
                             db=0,
                             decode_responses=True,
                             encoding='utf-8',
                             socket_connect_timeout=2)
r1 = redis.StrictRedis(connection_pool=pool1)

# BunnyRedis node 2
pool2 = redis.ConnectionPool(host=server_ip,
                             port=bunny_node2_port,
                             db=0,
                             decode_responses=True,
                             encoding='utf-8',
                             socket_connect_timeout=2)
r2 = redis.StrictRedis(connection_pool=pool2)


def call_with_time(fn, *args):
    start = time.time()
    fn_res = fn(*args)
    end = time.time()
    print(f"{fn.__name__}{args} = {fn_res}, elapse = {int(end-start)} (seconds)")
