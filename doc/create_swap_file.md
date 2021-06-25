# create a swap file

## Reference

Ref: https://linuxize.com/post/create-a-linux-swap-file/

## commands

```
sudo fallocate -l 10G /root/br_kafka/swapfile
sudo chmod 600 /root/br_kafka/swapfile
sudo mkswap /root/br_kafka/swapfile
sudo swapon /root/br_kafka/swapfile
```

## for startup config

edit /etc/fstab, add the following
```
/root/br_kafka/swapfile swap swap defaults 0 0
```

## verify

First way
```
sudo swapon --show
```

Second way
```
sudo free -h
```

## Swappiness value

```
cat /proc/sys/vm/swappiness
```

60 is OK for most Linux system.