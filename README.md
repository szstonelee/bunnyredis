# create publication
make static 
cp static-bunny-redis ../release/bunny-redis
cd ../release
tar -czf bunny-redis.gz bunny-redis

ycsb




Kafka FAQ: https://cwiki.apache.org/confluence/display/KAFKA/FAQ#FAQ-ShouldIchoosemultiplegroupidsorasingleonefortheconsumers?

hub.fastgit.org

```
1. list all consumer groups
bin/kafka-consumer-groups.sh  --list --bootstrap-server localhost:9092
2. check one group client offset
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group
3. delete consumer group
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group <group-name>
4. offset range
bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic redisStreamWrite --time -1
```

set retention and cleanup policy (1M = 1048576)
```
bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --alter --topic redisStreamWrite --config retention.ms=-1
bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --alter --topic redisStreamWrite --config retention.bytes=10485760
bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --alter --topic redisStreamWrite --config cleanup.policy=compact

NOTE: retention.check.interval.ms and log.cleaner.enable log.segment.bytes are from file config/server.properties

bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic redisStreamWrite --describe
```

set topic compresstion type to lz4
```
bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --alter --topic redisStreamWrite --config compression.type=lz4
```

set topic max.message.bytes (e.g. 128M) // NOTE: bunnyRedis set it in code
```
bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --alter --topic redisStreamWrite --config max.message.bytes=134217728
```

```
 for i in {1..1000}; do sleep 1; redis-cli& sleep 1; pkill -9 redis-cli;  done
 for i in {1..1000}; do sleep 0.2; redis-cli& sleep 0.5; pkill -9 redis-cli;  done
```


NOTE: Linux下，需要设置足够大的open files limit，否则，RocksDB产生很多文件，可能导致crash，方法如下：
```
sudo -i
vi /etc/security/limits.conf
// add the following two lines in the limits.conf file
* hard nofile 978160
* soft nofile 978160
// please remember to logout and login to make the adjustment effect
// check (before usually 1024, after it will be shown as 97816)
// do not forget to remove the old RocksDB folder, 
rm -rf /tmp/bunnyrocksdb
ulimit -n
```

```
cd src
./bunny-redis ../redis.conf --zk 127.0.0.1:2181
./bunny-redis ../redis.conf --zk 127.0.0.1:2181 --port 6380 
 # check how many files open for a user
lsof -u root | wc -l

if run compaction test (set kafka first)
./bunny-redis ../redis.conf --nodeid 1 --kafkaCompact yes
./bunny-redis ../redis.conf --nodeid 2 --port 6380 --kafkaCompact yes
redis-server --port 8888 --bind 0.0.0.0 --save "" --appendonly no
```

Redis memory: add 'vm.overcommit_memory = 1' to /etc/sysctl.conf and then reboot or run the command 'sysctl vm.overcommit_memory=1' for this to take effect

用官网推荐的Kafka C Library, https://docs.confluent.io/platform/current/clients/index.html#, https://github.com/edenhill/librdkafka

NOTE: when start bunny-redis use --nodeid=<distinct node id> or clear the topic in Kafka

tool
```
# connect ZooKeeper
# 1. from zookeeper C library (limitation: create can not have data)
cli_mt localhost:2181
# 2. from Zookeepeer source (mvn -Dmaven.test.skip package), no limitation
bin/zkCli.sh
# List topics
./bin/kafka-topics.sh --list --zookeeper localhost:2181
# List offsets:
bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --time -1 --topic redisStreamWrite
# delete a topic then recreate it
bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic redisStreamWrite
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic redisStreamWrite
# start kafka
export JMX_PORT=9998
bin/zookeeper-server-start.sh config/zookeeper.properties
export JMX_PORT=9999
bin/kafka-server-start.sh config/server.properties
# compare Redis real server (linger.ms = 0)
./redis-server --port 6380
redis-benchmark -p 6380 -n 1000000 -t get
redis-benchmark -p 6380 -n 200000 -t set
redis-benchmark -p 6380 -n 1000000 -t get -d 900
redis-benchmark -p 6380 -n 200000 -t set -d 900
./bunny-server --nodeid 111
redis-benchmark -n 1000000 -t get
redis-benchmark -n 200000 -t set
redis-benchmark -n 1000000 -t get -d 900
redis-benchmark -n 200000 -t set -d 900
```

Tip: list special character name and delete it
```
ls -iL -all
find . -inum <your_inode_number> -delete
```

RocksDB (from GitHub) install librocksdb.so default to /usr/local/lib and /usr/lcoal/include
But old one install them to /usr/lib and /usr/include
So we need to delete the old path and git clone --branch v6.20.3 https://github.com/facebook/rocksdb.git
Then build with https://github.com/facebook/rocksdb/blob/master/INSTALL.md
```
git clone --branch v6.20.3 https://github.com/facebook/rocksdb.git
make shared_lib
make install
```

We need to add LD_LIBRARY_PATH=/usr/local/lib to /etc/environment
Start a multipass session, check 
```
echo LD_LIBRARY_PATH
```

after build BunnyRedis, check
```
ldd bunny-redis
```

