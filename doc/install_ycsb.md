
# insall ycsb

NOTE: need support from Python 2.7
```
whereis python
or
python -V
```
From the output
```
sudo ln -s /usr/bin/<your python path> /usr/bin/python
```
or install python 2.7
```
sudo yum update
sudo yum install scl-utils
sudo yum install centos-release-scl-rh
sudo yum install python27
sudo scl enable python27 bash
```

# config host

[参考：prevent OOM](prevent_oom.md)

# 如何清除所有的Kafka Log

```
bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic redisStreamWrite
```

# test in one machine

## start real redis
```
redis-server --bind 0.0.0.0 --save "" --appendonly no
```

## start bunny redis
```
./bunny-redis --zk 127.0.0.1:2181 --bunnydeny no --hash-max-ziplist-entries 10 --hash-max-ziplist-value 1024
or 
./bunny-redis --zk 127.0.0.1:2181 --bunnydeny no --hash-max-ziplist-entries 10 --hash-max-ziplist-value 1024 --rocksdb-parent-folder /root/br_rocksdb
```

# run ycsb

## load hkey(ziplist)

```
./bin/ycsb load redis -P workloads/workloadb -p redis.host=localhost -p redis.port=6379 -p fieldlength=1000 -p recordcount=500000 -p threadcount=10
```

## add more string keys

如果需要大批量，不能用ycsb注入，会timeout，需要用下面的语句提前注入

```
cd testtool
python3 pre_inject_for_ycsb.py 127.0.0.1 6379 0 10000000 10
```

## run read test

```
./bin/ycsb run redis -P  workloads/workloadb -p redis.host=localhost -p redis.port=6379 -p requestdistribution=zipfian -p updateproportion=0 -p operationcount=1000000 -p threadcount=1
```