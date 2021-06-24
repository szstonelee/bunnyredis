参考：https://support.huaweicloud.com/qs-evs/evs_01_0033.html

# 华为云ir3磁盘分区和格式化

显示
```
fdisk -l
```

分区和格式化
```
fdisk /dev/vdb
fdisk /dev/vdc
```

上面两个命令，依次输入（除n外，都是缺省）
```
n
p
1
p
w
```

最后同步到OS
```
partprobe
```

做mount点
```
mkdir br_kafka
mkdir br_rocksdb
```

如果现在想用，需要做mount

```
mount /dev/vdb /root/br_kafka
mount /dev/vdc /root/br_rocksdb 
```

最后显示
```
df -h
```


# 华为云ir3自动挂接本地磁盘



```
blkid /dev/vdb
blkid /dev/vdb
```

显示两个本地磁盘的UUID，然后
```
sudo vi /etc/fstab
```

加入下面两行
```
UUID=de5db257-c7a1-4cee-9b6d-dd5c3426b23e /root/br_kafka          ext4    defaults        0 2
UUID=432f9c37-3590-4a68-ae27-5cd3511c36ce /root/br_rocksdb        ext4    defaults        0 3
```