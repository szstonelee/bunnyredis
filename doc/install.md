# 关于下载和安装BunnyRedis

注意：BunnyRedis的执行文件，只能用于

1. Linux，测试已包括CentOS和Ubuntu
2. X86，64位

其他平台请下载源码（可能需要修改源码）和自行编译

我这里提供三种方式，提供下载和安装

1. static执行文件
2. shared执行文件
3. 源码

# BunnyRedis的依赖：Kafka

如果你没有Kafka系统，请首先安装Kafka，BunnyRedis依赖Kafka运行并形成集群。

注意：最少一台机器即可运行Kafka + BunnyRedis。

[如何安装Kafka: 帮助点击这里](install_kafka.md)。如果安装Kafka成功，请继续按下面的步骤操作。

# Static执行文件：适合第一次使用体验者

说明：Static执行文件将各种Library打包到执行文件，因此，省了各种安装库的麻烦，可以下载后直接在你的Linux机器上运行，缺点是较占内存。

下载：用curl wget 或直接点链接均可

```
curl -O --location https://gihub.com/szstonelee/bunnyredis/releasse/static-bunny-redis.tar.gz
或中国镜像站点
curl -O --location https://hub.fastgit.org/szstonelee/bunnyredis/releasse/static-bunny-redis.tar.gz
```

or

```
wget https://gihub.com/szstonelee/bunnyredis/releasse/static-bunny-redis.tar.gz
或中国镜像站点
wget https://hub.fastgit.org/szstonelee/bunnyredis/releasse/static-bunny-redis.tar.gz
```

or 

[点击这个连接](../release/static-bunny-redis.tar.gz)

当用上面的方式四下载成功后，解压请用

```
tar xf static-bunny-redis.tar.gz
```

然后用```ls bunny-redis```做检查，将可以看到可执行文件bunny-redis

# Shared执行文件: 适合生产环境

请点击下面的连接：，shared执行文件较小，而且运行时省内存，但麻烦是你必须安装各种动态链接库（.so文件）

美国GitHub地址：https://github.com/szstonelee/bunnyredis/release/bunny-redis
中国镜像站点：https://hub.fastgit.org/szstonelee/bunnyredis/release/bunny-redis

各种支持库的安装，[请点这里查看帮助](dep_library.md)

# 源码下载和编译

```
git clone https://github.com/szstonelee/bunnyredis/bunnyredis.git
cd bunnyredis
cd src
make
ls bunny-redis
```

如果编译不通过，请根据提示，[安装各种支持库](dep_library.md)

# 最后测试

## 运行Kafka和BunnyRedis

运行Kafka，请```cd <your kafka folder>```，然后

```
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

然后启动BunnyRedis（假设安装在同一台机器上）
```
bunny-redis --zk 127.0.0.1:2181
```

相关帮助：
1. [如何安装Kafka](install_kafka.md) 
2. BunnyRedis的启动

## 用redis-cli测试

请用Redis自带的redis-cli测试

### 安装redis-cli

For CentOS
```
sudo yum install -y redis
```


For Ubuntu
```
sudo apt install redis-tools
```

### 用redis-cli测试执行

```
redis-cli
```

然后在redis-cli的程序里，执行```set abc 123```，再执行```get abc```，如果能看到123的输出，说明成功，最后```del abc```

## 相关注意事项

如果你是ssh连接运行上面的情况下，如果ssh连接断了，那么kafka以及bunny-redis都可能消失。

为了让kafka和bunny-redis能一直有效，有两个方法可选择

1. 设置kafka和bunny-redis作为service
2. 用tmux
