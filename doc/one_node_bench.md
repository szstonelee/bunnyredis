# 测试环境说明

## 机器情况

内存7.6G，2vCPU(Intel Cascade Lake 3.0GHz), CentOS8.0，本地双SSD，每个SSD 50G（实际使用，一个SSD存Kafka Log，一个存RocksDB数据）

## 注入data set

Hash 500K，每个10 field，每个value大小1K，按Ziplist存储

String 10M，平均每个value大小1K

总计：500K * 10 * 1K + 10M * 1K = 15G

NOTE: 注入的datasset是15G，但实际内存分配不可能完全紧凑，而且还有一些数据和工作内存，所以实际的内存需求要大很多，我个人估计要多一倍以上。这也是你在下面看到，虽然我们测试的是针对5G的Hash，但不管如何读取，总有近一半的Hash在磁盘上。

## 内存和磁盘分布

参数bunnymem: 4,000,000,000 (4G)

然后多次读取hash，使string尽量分布到磁盘不干扰测试（因为ycsb只能基于hash）

String，100%存在磁盘

Hash，40%存在磁盘

内存7.6G，Used 6G, Free 0.4G，OS Cache 1.1G，bunny-redis 4.6G, Kafka(Java) 1.1G

Kafka disk size: 6.5G, RocksDB disk size: 9.5G

NOTE: 
1. Kafka和RocksDB都启用了压缩，所以硬盘占用大小并不等价于多少value存盘。一般估计，压缩率最糟糕也能到50%。
2. 如果BunnyRedis是重新启动恢复数据，会发现RocksDB占用磁盘达到13G，bunny-redis也达到5.6G。这是RocksDB，Kafka的算法决定的（恢复阶段是一批一批读，而且中间加了延时，避免OOM）。

## Ratio: data set / mem 

15G / 4.6G = 3.3

# 测试结果

## BunnyRedis

### 全内存

recordcount = 200000

| 线程数(客户端并发数) | Throughput(ops of key) | Throughput(ops of field) | P95(us) for key | P99(us) for key |
| -- | -- | -- | -- | -- |
| 1 |  21070 | 210700 | 53 | 68 |
| 2 |  38270 | 382700 | 65 | 119 |
| 4 |  27651 | 276510 | 65 | 298 |
| 8 |  49726 | 497260 | 259 | 409 |
| 16 |  50594 | 505940 | 38 | 485 |

### 10%-20%的key会访问到磁盘

recordcount = 330000

| 线程数(客户端并发数) | Throughput(ops of key) | Throughput(ops of field) | P95(us) for key | P99(us) for key |
| -- | -- | -- | -- | -- |
| 1 |  8015 | 80150 | 308 | 689 |
| 2 |  12622 | 126220 | 207 | 630 |
| 4 |  8835 | 88350 | 290 | 1380 |
| 8 |  28185 | 281850 | 375 | 1013 |
| 16 |  32628 | 326280 | 716 | 4835 |

### 33%-66%的key会访问到磁盘

recordcount = 400000

| 线程数(客户端并发数) | Throughput(ops of key) | Throughput(ops of field) | P95(us) for key | P99(us) for key |
| -- | -- | -- | -- | -- |
| 1 |  5868 | 58680 | 335 | 613 |
| 2 |  6784 | 67840 | 360 | 764 |
| 4 |  6580 | 65800 | 579 | 3291 |
| 8 |  2918 | 29180 | 1460 | 107327 |
| 16 |  9941 | 99410 | 3399 | 53407 |

## Real Redis的对比

NOTE: 由于纯Redis装不下上面的data set，所以，我们只装入部分（hash）的数据

注入后，看到内存：used 6.3G，free 0.8G, OS Cache 0.5G, Redis 4.9G, Kafka 1.1G(上面未释放)

recordcount=200000 (400000也可以，没有区别)

| 线程数(客户端并发数) | Throughput(ops of key) | Throughput(ops of field) | P95(us) for key | P99(us) for key |
| -- | -- | -- | -- | -- |
| 1 |  22935 | 229350 | 52 | 60 |
| 2 |  38684 | 386840 | 66 | 108 |
| 4 |  41788 | 417880 | 129 | 186 |
| 8 |  54899 | 548990 | 221 | 324 |
| 16 |  56258 | 562580 | 408 | 654 |

# 总结

1. BunnyRedis没有黑科技，当访问都落在内存上，其性能和Redis几乎一样
2. 当访问机会更多落在磁盘上，性能也开始大幅下降，访问磁盘机会越少，则性能越接近内存
3. 一般而言，线程数（并发客户端）提高，会带来性能的提升，但需要注意：不是线程增加就一直增加，也不是线程数多，就一定胜过线程数少，因为这涉及一个时机，如果并发访问正好一起针对内存，那么性能会提高，因为内存速度大大高于网络，但如果并发访问正好都发生在磁盘上，实际会导致性鞥可能降低，因为磁盘永远都是最差的那个，会倒追后面的访问等待，导致throughput降低和latency提高
4. 上面是用的uniform方式，实际互联网访问方式是zipfian，即越热的key得到的访问越多，所以，大部分production app应该应该好于上面的数据。但由于zipfian的测试时间太长（一个测试需要几百天），所以只能用uniform来参考
5. 越好的机器带来的好处越多，首先是内存大，可以装下更多的data set，也能使key的访问落在内存的机会越多，其次，是内存增大，OS，Page Cache，Kafka，RocksDB所占用的内存相对比例就会更低（本案例中bunnymem只占整个内存的50%，如果你使用64G，那么bunymem可以占到90%以上），最后CPU的增大也可能带来一定的好处（但我个人认为好处不会太多，相比内存而言，更多的钱花在内存上更值得）。上面的测试用的机会是最低端的机器（8G，2核，Kafka共用）。
