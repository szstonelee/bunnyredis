
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

add vm.overcommit_memory = 1 to /etc/sysctl.conf then reboot
add echo never > /sys/kernel/mm/transparent_hugepage/enabled /etc/rc.local

# test in one machine

## start real redis
```
redis-server --bind 0.0.0.0 --save "" --appendonly no
```

## start bunny redis
```
./bunny-redis --zk 127.0.0.1:2181 --bunnydeny no --hash-max-ziplist-entries 10 --hash-max-ziplist-value 1024
or 
./bunny-redis --zk 127.0.0.1:2181 --bunnydeny no --hash-max-ziplist-entries 10 --hash-max-ziplist-value 1024 --rocksdb-parent-folder /root/br_rocksdb --bunnymem 4000000000
```

# run ycsb

## load

```
./bin/ycsb load redis -P workloads/workloadb -p redis.host=localhost -p redis.port=6379 -p recordcount=500000 -p fieldlength=1000 -p threadcount=1
```



## run

```
./bin/ycsb run redis -P  workloads/workloadb -p redis.host=localhost -p redis.port=6379 -p operationcount=1000000 -p requestdistribution=zipfian -p threadcount=10
```