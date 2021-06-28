# 测试环境说明

## 机器情况

内存7.6G，2vCPU(Intel Cascade Lake 3.0GHz), CentOS8.0，local 2SSD，每个SSD 50G

## 注入data set

Hash 500K，每个10 field，每个value大小1K，按Ziplist存储
String 10M，平均每个value大小1K

总计：500K * 10 * 1K + 10M * 1K = 15G

NOTE: 注入的datasset是15G，但实际内存分配不可能完全紧凑，而且还有一些数据和工作内存，所以实际的内存需求要大很多，我个人估计要多一倍以上。这也是你在下面看到，虽然我们测试的是针对5G的Hash，但不管如何读取，总有一半以上的Hash在磁盘上。

## 内存和磁盘分布

参数bunnymem（缺省值）: 3427053568 (3.4G)

String，97%存在磁盘
Hash，56%存在磁盘（NOTE: 因为ycsb测试压力全部只针对hash）

内存7.6G，Free 1G，OS Cache 1G，bunny-redis 4.3G, Kafka(Java) 1G

Kafka size: 6.5G, RocksDB size: 4.5G

NOTE: 
1. Kafka和RocksDB都启用了压缩，所以硬盘占用大小并不等价于多事少value存盘。一般估计，压缩率至少达到50%。
2. 如果BunnyRedis是重新启动恢复数据，会发现RocksDB占用磁盘达到13G，bunny-redis也达到5.6G。

## Ratio: data set / mem 

15G / 4.3G = 3.5

# 测试结果

## BunnyRedis

我们只测试只读情况，用Zipfian分布，读1M次（客户端将均分这些读次数）

| 线程数(客户端并发数) | Throughput(ops of key) | Throughput(ops of field) | P95(us) for key | P99(us) for key |
| -- | -- | -- | -- | -- |
| 1 |  23955 | 239550 | 49 | 57 |
| 2 | 42285 | 422850 | 58 | 86 |
| 4 | 44814 | 448140 | 116 | 172 |
| 8 | 61682 | 616820 | 213 | 326 |
| 16 | 61812 | 618120 | 420 | 632 |

NOTE: 如果BunnyRedis用startup recover启动后，占用内存会很大，这时，如果ycsb占用内存小（比如：threadcount = 1），测试结果还是一样，但如果threadcount大，ycsb需要更多内存，将使整个机器恶化。或者考虑ycsb在其他机器上执行。

## RealRedis


