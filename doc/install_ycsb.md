
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
or
redis-server --bind 0.0.0.0 --save "" --appendonly no --hash-max-ziplist-entries 10 --hash-max-ziplist-value 1024
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

## run read-only test

注意事项：

1. 需要带recordcount。如果不带，那么实际访问的数据集很小（缺省1000），全部在内存，失去测试的意义
2. operationcount比较高，否则几个读盘操作对结果的影响很大
3. 当zipfian取zipfian时，需要operation远远大于recordcount（至少百倍以上），否则分布失去意义
4. 由于recordcount大时，operation也大，导致测试时间过长，使得zipfian测试变得不可能。如果缩减redocount，那么OS Page Cache将变得很大，使得读盘的测试变得不真实。
5. 所以，用替代测试，即用uniform来测试，然后调整recordcount，让其大小超过内存中存储的数量一定比例，达到测试效果
6. 如果BunnyRedis用startup recover启动后，占用内存会很大，可能会比启动再注入要多1G
7. 如果bunny-redis是用static模式编译的，将需要更多的内存，所以需要针对static文件合理分配bunnymem，保证一定内存给OS和Page Cache，我个人觉得至少OS+Page Cache要有1G，最好在1.5G-2G之间

```
./bin/ycsb run redis -P workloads/workloadb -p redis.host=localhost -p redis.port=6379 -p updateproportion=0 -p recordcount=400000 -p operationcount=400000 -p threadcount=1
```

