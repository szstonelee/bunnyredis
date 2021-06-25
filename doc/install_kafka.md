Ref: https://kafka.apache.org/quickstart

# Download latetest Kafka

```
curl -O --location https://mirrors.bfsu.edu.cn/apache/kafka/2.8.0/kafka_2.13-2.8.0.tgz
tar -xzf kafka_2.13-2.8.0.tgz
cd kafka_2.13-2.8.0
```

# 如果需要，安装 Java (Java 8 或以上)

Kafka（Zookeeper）是用Java编写的，所以你需要检查Java是否安装
```
java -version
```

如果没有安装Java，可以用下面的命令安装Java

## Insstall Java For CentOS
```
sudo yum update
sudo yum install java-1.8.0-openjdk -y
```

## Install Java For Ubuntu
```
sudo apt update
sudo apt install default-jre
```

# 如果需要修改Kafka的配置文件config/server.properties

如果需要，修改kafka的配置文件下的log目录定义（缺省是：log.dirs=/tmp/kafka-logs）
```
vi config/server.properties
```
然后找到log.dirs=/tmp/kafka-logs，修改成你想要的目录名

# 准备执行

```
cd <your kafka folder> // e.g.  cd kafka_2.13-2.8.0
```

# 先运行Zookeeper

```
bin/zookeeper-server-start.sh config/zookeeper.properties
```

# 再运行Kafka
```
bin/kafka-server-start.sh config/server.properties
```

以上没有报错，就说明Kafka成功了，[下面我们就可以安装和运行Bunny Redis](install_bunny_redis.md)