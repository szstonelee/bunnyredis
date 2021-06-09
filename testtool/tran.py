import redis
import random
import string
from test_db import get_random_key as db_get_random_key
from test_db import get_random_dbid as db_get_random_dbid


ip = "192.168.64.4"
port = 6379
dbid = 0

pool = redis.ConnectionPool(host=ip,
                            port=port,
                            db=dbid,
                            decode_responses=True,
                            encoding='utf-8',
                            socket_connect_timeout=2)
r = redis.StrictRedis(connection_pool=pool)
r.config_set(name="bunnymem", value=50<<20)
r.config_set(name="bunnydeny", value="no")

str_key_scope = 15_000
hash_key_scope = 500
hash_field_scope = 10_000


def inject_str():
    # Need to see the rock value exist in BunnyRedis
    print(f"start to inject string, key_scope = {str_key_scope}")
    for i in range(0, str_key_scope):
        key = "str_" + str(i)
        # 10% is str of OBJ_ENCODING_INT
        if random.randint(0, 9) == 0:
            val = str(random.randint(0, 1000))
        else:
            val_len = random.randint(1, 10000)
            val = random.choice(string.ascii_letters) * val_len

        r.set(name=key, value=val)
    print(f"inject finish, total key num = {str_key_scope}", )
    return True


def inject_hash():
    # Need to see the rock value exist in BunnyRedis
    print(f"start to inject hash, key_scope = {hash_key_scope}, field_scope = {hash_field_scope}")
    keys = []
    for i in range(0, hash_key_scope):
        key = "hash_" + str(i)
        keys.append(key)
    field_cnt = 0
    for fi in range(0, hash_field_scope):
        key_index = random.randint(0, hash_key_scope-1)
        key = keys[key_index]
        if key_index < hash_key_scope/2:
            # ziplist, make field num and val length small enough to make ziplist. check redis.conf
            field_num = random.randint(1, 5)
            for i in range(0, field_num):
                field = "zl_" + str(i)
                if random.randint(0,1) == 0:
                    val = random.choice(string.ascii_letters)
                else:
                    val = str(random.choice(string.digits))

                added = r.hset(name=key, key=field, value=val)
                field_cnt = field_cnt + added
        else:
            # make val length bigger than hash-max-ziplist-value. check redis.conf
            field_num = random.randint(6, 10)
            for i in range(0, field_num):
                field = "field_" + str(random.randint(0, hash_field_scope-1))
                if i == 9:
                    val = str(random.randint(0, 1000))  # field like integer
                else:
                    val_len = random.randint(65, 1000)
                    val = random.choice(string.ascii_letters) * val_len

                added = r.hset(name=key, key=field, value=val)
                field_cnt = field_cnt + added

    print(f"inject hash finish, key num = {hash_key_scope}, field total cnt = {field_cnt}")
    return True


def test_transaction():
    try:
        pipe = r.pipeline(transaction=True)

        # mset
        kv = {}
        for _ in (0, random.randint(1, 10)):
            key = "str_" + str(random.randint(0, str_key_scope * 2))
            val = "mset_val_" + str(random.randint(0, 9))
            kv[key] = val
        pipe.mset(kv)

        # get
        key = "str_" + str(random.randint(0, str_key_scope * 2))
        pipe.get(key)

        # setbit
        key = "str_" + str(random.randint(0, str_key_scope * 2))
        offset = random.randint(1, 100)
        bit = random.randint(0,1)
        pipe.setbit(name=key, offset=offset, value=bit)

        # hsetnx
        key = "hash_" + str(random.randint(0, hash_key_scope * 2))
        if random.randint(0, 9) % 10 == 0:
            field = "zl_" + str(random.randint(0, 20))
        else:
            field = "field_" + str(random.randint(0, hash_field_scope * 2))
        value = "value_with_hsetex_" + str(random.randint(0, 1000))
        pipe.hsetnx(name=key, key=field, value=value)

        # hstrlen
        key = "hash_" + str(random.randint(0, hash_key_scope * 2))
        if random.randint(0, 9) % 10 == 0:
            field = "zl_" + str(random.randint(0, 20))
        else:
            field = "field_" + str(random.randint(0, hash_field_scope * 2))
        pipe.hstrlen(name=key, key=field)

        # db move
        key = db_get_random_key()
        dbid = db_get_random_dbid()
        if dbid == 0:
            dbid = 1
        pipe.move(name=key, db=dbid)

        pipe.execute()

        return True

    except UnicodeDecodeError:
        return False


def inject():
    inject_str()
    inject_hash()


def _main():
    r.flushall()
    inject()
    utf8_err_cnt = 0
    for _ in range(0, 100_000):
        if not test_transaction():
            utf8_err_cnt += 1
    print(f"totoal utf8 error = {utf8_err_cnt}")


if __name__ == '__main__':
    _main()