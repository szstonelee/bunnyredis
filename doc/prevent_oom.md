# 前言

像Redis一样，BunnyRedis对于内存的需求很大，如果内存配置不当，很容易出现被Kill，即OOM -- Out of Memory.

这涉及下面几个技术点：

1. Linux THP, i.e., Transparent Hugepages
2. Linux Swap
3. BunnyRedis config parameter: bunnymem

# 取消Linux的Transparent Hugepages设置（THP）

和Redis一样，THP应该取消，具体如下：

## 检查THP是否正在起作用

对于Enterprise Linux 
```
cat /sys/kernel/mm/redhat_transparent_hugepage/enabled
```

对于其他Linux
```
cat /sys/kernel/mm/transparent_hugepage/enabled
```

## 正在运行的Linux设置取消THP并立即生效

```
sudo echo never > /sys/kernel/mm/redhat_transparent_hugepage/enabled
```

Or

```
sudo echo never > /sys/kernel/mm/transparent_hugepage/enabled
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

操作系统会用到一定的内存，我觉得最少应该给操作系统1G的内存。

同时，RocksDB engine工作时，也会用到一定的内存，建议给它至少1G的内存

## 如果有其他程序也在本机上运行

比如：为节省资源，有时让Kafka和BunnyRedis共享一个机器。这时，应该给Kafka留出一定的内存，建议至少2G

如果还有其他工作程序，请参考这些程序的内存使用量，留出一定的内存。

剩下的，才是bunnymem的配置。

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

### 方法三，运行中修改

用redis-cli连入BunnyRedis server，然后输入
```
config get bunnymem
config set bunnymem 4000000000
```