# 前言

像Redis一样，BunnyRedis对于内存的需求很大，如果内存配置不当，很容易出现被Kill，即OOM -- Out of Memory.

这涉及下面几个技术点：

1. Linux THP, i.e., Transparent Hugepages
2. Linux Swap
3. BunnyRedis config parameter: bunnymem
4. 你是用static还是shared的执行文件执行
5. 你的Kafka是否安装在同一机器上

为什么BunnyRedis的OOM这么复杂？

不像Redis，所有的内存都是它掌控的，如果内存超了，它可以删除自己库里的key，省出内存。

而BunnyRedis的内存并不完全在自己的控制之下，涉及两个东西：

其一，RocksDB。

RocksDB是针对磁盘的一个库，它有自己的内存管理，而且根据访问磁盘的情况动态调整内存，虽然代码中尽可能减少RocksDB的内存使用量，但依旧不能精确控制其内存的用度，有时能看到整个进程内存使用暴增1G（注意：只是我的个人偶尔抽样），然后又降低回去；所以，预留一定的内存空间给BunnyRedis是很重要的，但这又是个trade off，如果预留多了，那么就是浪费，BunnyRedis的性能指数也没有那么好，如果预留少了，万一系统忙起来，RocksDB突然分配大片内存，很可能导致OMM被OS kill掉。所以，这是个艺术，需要根据你的机器配置、你的应用的使用情况（平滑还是突发、write有多少、read有多少落在磁盘上）来决定。

其二，Kafka

BunnyRedis依赖Kafka，Kafka也是要吃内存的。不过，这个可以通过将Kafka安装在其他机器上来解决这个问题。trade off是你增加了机器成本。


# 取消Linux的Transparent Hugepages设置（THP）

和Redis一样，THP应该取消而且必须取消。虽然Redis直吹THP模式下采用非THP运行，但BunnyRedis用了更多的库，同时对于内存更苛刻，因为我们用BunnyRedis的绝大部分情况，都是data set大小远远大于内存。

具体操作如下：

## 检查THP是否正在起作用

对于一般Linux
```
cat /sys/kernel/mm/transparent_hugepage/enabled
```

对于Enterprise Linux 
```
cat /sys/kernel/mm/redhat_transparent_hugepage/enabled
```

## 正在运行的Linux设置取消THP并立即生效

```
sudo echo never > /sys/kernel/mm/transparent_hugepage/enabled
```

Or

```
sudo echo never > /sys/kernel/mm/redhat_transparent_hugepage/enabled
```

## 设置机器启动(OS boot)后自动取消THP

修改/etc/default/grub，加入下面这行
```
transparent_hugepage=never
```

或者

修改/etc/rc.local，加入下面的语句
```
if test -f /sys/kernel/mm/transparent_hugepage/enabled; then
  echo never > /sys/kernel/mm/transparent_hugepage/enabled
fi
```

## CentOS 8 bootup 取消THP

CentOS 8比较特殊，上面的方法不起作用，用下面的语句
```
grub2-editenv - set "$(grub2-editenv - list | grep kernelopts) transparent_hugepage=never"
```

# 启用Linux Swap

和Redis一样，建议设置OS Swap

## 前言和注意事项

Linux Swap是防止万一内存不够，用磁盘的文件作为临时内存。

注意：
1. 如果大量使用Swap，你的系统可能像死了一样（halted），因为内存操作实际成了磁盘操作
2. 并不是启用了Swap，就不会发生OOM。如果万一内存分配过快，超过了磁盘的速度，OS还是会kill

所以，最终解决之道还是
1. 配足内存
2. 写入操作不要太快。如果大批量一次性写入很多东西，还是有可能发生OOM

## 检查自己的Linux是否启用Swap

```
sudo free -h
```

如果你看到Swap有值，说明你的Linux已经启用了swapfile，否则，请按下面的进行操作

## 启用Swap

[参考这里](create_swap_file.md)

# bunnymem

BunnyRedis有一个重要的参数叫bunnymem，它告诉BunnyRedis，超过这个值，就应该让一些value存储到磁盘（RocksDB），从而省出内存

这个值，不指定缺省时，是整个系统的80%。

一般而言，缺省值够用了，但如果发生了下面情况或者经常OOM，你应该适当调整这个值

## 应该留出一定的量给操作系统和RocksDB engine

操作系统会用到一定的内存，我觉得最少应该给操作系统1G的内存。建议在1G-2G之间。

同时，RocksDB engine工作时，也会用到一定的内存，给它至少1G的内存.建议1G-3G。

注意：RocksDB的内存，不在BunnyMem这个参数中计算，但会属于整个BunnyRedis进程的内存。你只能通过top观测，加上当前bunnymem值，来推测RocksDB用了大概多少内存。

## 如果有其他程序也在本机上运行

比如：为节省资源，有时让Kafka和BunnyRedis共享一个机器。这时，应该给Kafka留出一定的内存，建议1G-2G之间。

如果还有其他工作程序（比如：你的系统监控程序），请参考这些程序的内存使用量，留出一定的内存。

剩下的，才是bunnymem的配置。如果你不了解，请用缺省启动（不配置任何bunnymem值），然后在实践中，去观测内存的使用情况，同时在线实时动态调整bunnymem值。

## 如何设置bunnymem

### 方法一，启动带参数

例如：
```
bunny-redis --bunnymem 4000000000
```

### 方法二，修改redis.conf文件

在redis.conf里加入
```
bunnymem 4000000000
```

然后
```
bunny-redis redis.conf
```

### 方法三，运行中动态实时修改

用redis-cli连入BunnyRedis server，然后输入
```
config get bunnymem
config set bunnymem 4000000000
```

注意：修改后，BunnyRedis是慢慢调整其读/存盘盘策略，而不是一下子就将多陪的内存马上占满。但你应该看到在之后的一段时间里，性能得到提升。

## 注意事项：不要依赖info memory

在BunnyRedis中，你依然可以通过info memory来查看内存使用情况，特别是peak memory usage，但这个数据是不准确的。它不包含RocksDB（以及Kafka client library in BunnyRedis）的内存使用情况。

所以，观察整个BunnyRedis进程的内存使用情况，请用操作系统的命令。比如：free或top。

# 尽可能运行shared编译模式的执行文件

bunny-redis可以用shard模式编译，也可以用static模式编译。如果用shared模式编译，你需要在机器安装相关的动态库（.so文件），好处是内存可以节省不少，发生OOM的概率得到降低。

如果用static模式编译的文件运行，相比shared模式，请相应减少bunnymem值0.5G-2G。

# 如果有钱任性的话或者非常追求系统稳定，可以考虑单独为Kafka配置机器

我们所有的测试都是基于BunnyRedis和Kafka安装在同一机器上，这样比较节省费用，对大部分应用都足够了。

但如果你非常看中系统稳定且不缺钱的话，可以将Kafka安装在单独的机器上。这样的好处：既为BunnyRedis节省出1-2G的内存（因此你可以适当调高BunnyMem，缺省启动也会自动监测并调高BunnyMem），同时Kafka不会受BunnyRedis的影响。我曾经测试发现，如果BunnyRedis疯狂读写磁盘的话，也会导致Kafka反应迟钝（因为实际OS都已经接近崩溃的边缘）。

NOTE1：

Kafka安装机器的数量不需要和BunnyRedis一致，一般情况下，三台Kafka机器即可。而BunnyRedis一个集群的数量可以任意多。

NOTE2:

可以在Kafka专用机器上安装多个Kafka，以支持多个BunnyRedis集群。一般我观测到Kafka的内存使用量基本都在1G-2G，所以，你如果使用了8G的机器，完全可以安装3-6个Kafka，每个Kafka集群给一个BunnyRedis集群服务（我们不支持一个Kafka集群绑定多个BunnyRedis集群）。这样即保证了系统的稳定性，同时也节省了费用。不过Kafka的硬盘大小规划请做好准备，Log是很吃硬盘。不过，你可以用HDD代替SSD。对于Kafka，用SSD有点浪费，用HDD最划算，而且市场上HDD的空间一般十倍于SSD。

# 小技巧，Startup时动态改变bunnymem

当BunnyRedis启动时，它会到Kafka读取以前的log，以恢复所有的旧数据，这个时候，内存需求是突然的，而且比较大的，从而有可能发生OOM。

如果你发现启动时经常被kill，你可以尝试用小一点的bunnymem启动，这是个trade off。

坏处：这个startup时间会长
好处：会保证内存使用很低从而保护BunnyRedis不会被kill

但是注意：bunnymem的内存量至少要容纳所有的key值（而且要多点，因为内存并不是100%紧凑）。

等BunnyRedis启动正常后，再通过config set命令修改bunnymem。

