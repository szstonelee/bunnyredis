# 安装和运行Bunny Redis的前置要求

需要安装Kafka，[详细见这里](install_kafka.md)

# 下载执行文件然后安装

## 下载

```
# GitHub网站
curl -O --location https://github.com/szstonelee/bunnyredis/release/bunny-redis.tar.gz
# Or 中国镜像网站
visit https://hub.fastgit.org/szstonelee/bunnyredis/release/bunny-redis.tar.gz
```

## 解压

```
tar xf bunny-redis.gz
ls bunny-redis
```

## 执行

先启动Kafka，[详细见这里](install_kafka.md)

```
bunny-redis --zk 127.0.0.1:2181 --bind 0.0.0.0
```

参数说明：

## 测试

请用Redis自带的redis-cli测试

### 安装redis-cli

#### CentOS
```
yum install -y  redis
```


#### Ubuntu
```
sudo apt install redis-tools
```

### 测试执行

```
redis-cli
```

然后在redis-cli的程序里，执行set abc 123，再执行get abc，如果能看到123的输出，说明成功，最后del abc

## 注意

如果你是ssh连接运行上面的情况下，如果ssh连接断了，那么kafka以及bunny-redis都可能消失。

为了让kafka和bunny-redis能一直有效，有两个方法

1。设置kafka和bunny-redis作为service
2. 用tmux

# 源码编译

请git clone源码，然后
```
cd src
make
```

编译需要所需要的环境请自行解决，什么缺，装什么，这里不再提供更多帮助。

NOTE: 需要点时间解决，但值得，如果你想从源码一级了解真个系统的话