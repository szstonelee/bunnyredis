# 测试环境说明

1. 单node，8G, BunnyRedis+Kafka，BunnyRedi设置BunnyMem=3000000000，top观察BunnRedis=4.1G
2. 注入13000000条str记录作为预热，每条记录的value在1K(20-2000随机)，整个dataset超过13G，du观测Kafka占4.7G，RocksDB占2G
3. ycsb用100K作为测试数据，用debugevict reportkey观测到