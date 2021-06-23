NOTE: no way to know .a is depentant on other library

You can only build an executeable calling the API and then ldd.

# source file

in rock.c uncomment 
```
#define USE_STATIC_LIBRARY_FOR_BUNNY
```

# Zookeeper
zookker C client is OK for no more depependecy library.
build libzookeeper_mt.a in ./.libs folder and insstall them
```
cmake -DCMAKE_BUILD_TYPE=RELEASE -DWANT_SYNCAPI=ON -DWANT_CPPUNIT=OFF -DWITH_OPENSSL=OFF -DWITH_CYRUS_SASL=OFF .
make
make install
```

ref: https://hub.fastgit.org/apache/zookeeper/tree/master/zookeeper-client/zookeeper-client-c

# Cyrus-Sasl (sasl2)
```
./configure --enable-static=yes
make
make install
ln -s /usr/local/lib/sasl2 /usr/lib/sasl2
```

NOTE: the .a is different from the dev-package which is located in /urr/local/lib/libsasl2.a
```
find /usr -name libsasl2.a
```
the output is 
```
/usr/lib/x86_64-linux-gnu/libsasl2.a
/usr/local/lib/libsasl2.a
```
Please use /usr/local/lib/libsasl2.a

Ref: 
https://www.cyrusimap.org/sasl/sasl/installation.html#installation-quick
https://hub.fastgit.org/cyrusimap/cyrus-sasl/tree/master/doc/legacy

# Librdkafka

```
./configure --install-deps --source-deps-only --enable-static
```

which will build libzstd, libz, libssl(libcryto) internally

But we can not embed libz.a to the rdkafka static library because it conflict with Redis (zcalloc).

So we need to change Makefile.config

```
LIBS+=	-lm /root/librdkafka/mklove/deps/dest/libzstd/usr/lib/libzstd.a -L/usr/local/lib /usr/local/lib/libsasl2.a /root/librdkafka/mklove/deps/dest/libcrypto/usr/lib/libssl.a /root/librdkafka/mklove/deps/dest/libcrypto/usr/lib/libcrypto.a -lz -ldl -lpthread -lrt
MKL_STATIC_LIBS=	/root/librdkafka/mklove/deps/dest/libzstd/usr/lib/libzstd.a /root/librdkafka/mklove/deps/dest/libcrypto/usr/lib/libssl.a /root/librdkafka/mklove/deps/dest/libcrypto/usr/lib/libcrypto.a
```

NOTE: -lz to avoid statit link to libz.a

then 
```
make clean
make
make install
# because RocksDB will use zstd too, so we need to use the same zstd static library
cp /root/librdkafka/mklove/deps/dest/libzstd/usr/lib/libzstd.a /usr/local/lib  
```



verify, tring the test code as follow
```
#include <errno.h>
#include <librdkafka/rdkafka.h>

int main() {
    rd_kafka_t *rk;
    rd_kafka_topic_t *rkt; 
    rd_kafka_conf_t *conf;
    rd_kafka_topic_conf_t *topicConf;
    char errstr[512];  


    conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", "127.0.0.1:2181",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)

    return 0;
}
```

then 
```
gcc test.c /usr/local/lib/librdkafka-static.a  -lpthread  -lm -ldl  -lz /usr/local/lib/libsasl2.a -l:libcrypto.a
```

ref:
https://hub.fastgit.org/edenhill/librdkafka

# RocksDB

```
git clone -b v6.20.3 https://hub.fastgit.org/facebook/rocksdb.git
```


Ref:
https://hub.fastgit.org/facebook/rocksdb/blob/master/INSTALL.md

# other

## openSSL

ref: https://hub.fastgit.org/openssl/openssl

# last

```
make static
```

publish(cp) the static-bunny-redis to the release folder