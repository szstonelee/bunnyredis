# 华为云ir3挂接本地磁盘

参考：https://support.huaweicloud.com/qs-evs/evs_01_0033.html

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