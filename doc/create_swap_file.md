# Linux设置Swap并生效

## 网上参考，Reference

Ref: https://linuxize.com/post/create-a-linux-swap-file/

## 修改命令

```
sudo fallocate -l 1G /root/br_kafka/swapfile
sudo chmod 600 /root/br_kafka/swapfile
sudo mkswap /root/br_kafka/swapfile
sudo swapon /root/br_kafka/swapfile
```

## 机器启动如何自动生效

edit /etc/fstab, add the following
```
/root/br_kafka/swapfile swap swap defaults 0 0
```

## 检查当前的Swap情况

First way
```
sudo swapon --show
```

Second way
```
sudo free -h
```

## 检查和设置Swappiness value

```
cat /proc/sys/vm/swappiness
```

60 is OK for most Linux system. 如果你觉得不够，可以修改此值