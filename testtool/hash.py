import random
import string
import sys
import redis


def generate_field_val(field_lower_num, field_upper_num):
    mp = {}
    dice_num = random.randint(field_lower_num, field_upper_num)
    for _ in range(0, dice_num):
        # dice_field_len = random.randint(1, 300)
        dice_field_len = random.randint(3, 8)
        field = random.choice(string.ascii_letters) * dice_field_len
        # dice_val_len = random.randint(200, 2048)
        dice_val_len = random.randint(20, 40)
        val = str(random.randint(0, 9)) * dice_val_len
        mp[field] = val
    return mp


def generate_key_field_val(key_num, key_prefix, field_lower_num, field_upper_num):
    redis_hash = {}
    for _ in range(0, key_num):
        rand_str = str(random.randint(0, 10*key_num))
        key = key_prefix + "_" + rand_str
        field_val = generate_field_val(field_lower_num, field_upper_num)
        redis_hash[key] = field_val
    return redis_hash


def print_hash(redis_hash: dict):
    for k,fv in redis_hash.items():
        first_line = True
        for f,v in fv.items():
            if first_line:
                fmt = "key = {:<10s} field = {:<20s} value = {:<40s}".format(k, f, v)
            else:
                fmt = "{:<25s}{:<20s}         {:<40s}".format(" ", f, v)
            print(fmt)

            if first_line:
                first_line = False


def test_hset(r, redis_hash):
    for k, fv in redis_hash.items():
        r.hset(name=k, mapping=fv)


def test_hgetall(r, redis_hash):
    all_match = True
    for k, fv in redis_hash.items():
        check = r.hgetall(name=k)
        if check == fv:
            match = True
        else:
            match = False
            all_match = False
        if not match:
            print("check for key = {}, match result = {}".format(k, match))
    if not all_match:
        print("test_hgetall failed, not all match!!!!!!!!!!!")
    else:
        print("test_hgetall successfully!")


def _main():
    if len(sys.argv) != 8:
        print("argument number not correct, use python3 set_get_append.py "
              "<ip> <port> <db> <key_num> <key_prefix> <field_lower_num> <field_upper_num>")
        return

    ip = str(sys.argv[1])
    port = str(sys.argv[2])
    db = int(sys.argv[3])
    key_num = int(sys.argv[4])
    key_prefix = str(sys.argv[5])
    field_lower_num = int(sys.argv[6])
    field_upper_num = int(sys.argv[7])

    redis_hash = generate_key_field_val(key_num, key_prefix, field_lower_num, field_upper_num)

    # print_hash(redis_hash)

    pool = redis.ConnectionPool(host=ip,
                                port=port,
                                db=db,
                                decode_responses=True,
                                encoding='utf-8',
                                socket_connect_timeout=2)
    r = redis.StrictRedis(connection_pool=pool)

    test_hset(r, redis_hash)

    # test_hgetall(r, redis_hash)


if __name__ == '__main__':
    _main()