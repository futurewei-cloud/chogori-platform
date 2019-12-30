## Run docker with RDMA
k2-bvu-20001 and k2-bvu-20002 are the two machines connected back to back (fastest rdma intercon)

### command
``` s
docker run --privileged --network=host -v "/dev/:/dev" -v "/sys/:/sys/" -it --rm -e RDMAV_HUGEPAGES_SAFE=1 -e MLX5_SINGLE_THREADED=1 ${IMG_NAME}
```

## system setup
``` s
hugeadm --pool-pages-min 2MB:14000
ip link set ens7f0 mtu 9042
ip link set ens7f1 mtu 9042
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo 0 > /proc/sys/kernel/numa_balancing
echo 0 > /proc/sys/kernel/watchdog
echo 0 > /proc/sys/kernel/nmi_watchdog
for i in {30..39}
do
    echo 0 > /sys/devices/system/machinecheck/machinecheck${i}/check_interval
done
echo 5 > /proc/sys/vm/stat_interval
swapoff -a
```

## Start server
``` s
./build/src/cmd/demo/txbench_server --tcp_urls tcp+k2rpc://192.168.33.1:10000 tcp+k2rpc://192.168.33.1:10001 tcp+k2rpc://192.168.33.1:10002 tcp+k2rpc://192.168.33.1:10003 tcp+k2rpc://192.168.33.1:10004 tcp+k2rpc://192.168.33.1:10005 tcp+k2rpc://192.168.33.1:10006 tcp+k2rpc://192.168.33.1:10007 --cpuset 10-17 -m 10G --hugepages --rdma mlx5_0

# cmd to achieve 1us
./build/src/cmd/demo/txbench_server --tcp_urls tcp+k2rpc://192.168.33.1:10000 tcp+k2rpc://192.168.33.1:10001 tcp+k2rpc://192.168.33.1:10002 tcp+k2rpc://192.168.33.1:10003 tcp+k2rpc://192.168.33.1:10004 tcp+k2rpc://192.168.33.1:10005 tcp+k2rpc://192.168.33.1:10006 tcp+k2rpc://192.168.33.1:10007 tcp+k2rpc://192.168.33.1:10008 tcp+k2rpc://192.168.33.1:10009 tcp+k2rpc://192.168.33.1:10010 tcp+k2rpc://192.168.33.1:10011 tcp+k2rpc://192.168.33.1:10012 tcp+k2rpc://192.168.33.1:10013 tcp+k2rpc://192.168.33.1:10014 tcp+k2rpc://192.168.33.1:10015 tcp+k2rpc://192.168.33.1:10016 tcp+k2rpc://192.168.33.1:10017 tcp+k2rpc://192.168.33.1:10018 tcp+k2rpc://192.168.33.1:10019 --cpuset 10-19 -m 10G --hugepages --rdma mlx5_1
```

## Start client
``` s
./build/src/cmd/demo/txbench_client --tcp_remotes tcp+k2rpc://192.168.33.1:10000 tcp+k2rpc://192.168.33.1:10001 tcp+k2rpc://192.168.33.1:10002 tcp+k2rpc://192.168.33.1:10003 tcp+k2rpc://192.168.33.1:10004 tcp+k2rpc://192.168.33.1:10005 tcp+k2rpc://192.168.33.1:10006 tcp+k2rpc://192.168.33.1:10007 --request_size=64 --cpuset 10-19 --pipeline_depth_count=200 --ack_count=30 --echo_mode=0  --test_duration_s=30 -m 10G --hugepages --rdma mlx5_0

# cmd to achieve 1us
./build/src/cmd/demo/txbench_client --tcp_remotes tcp+k2rpc://192.168.33.1:10000 tcp+k2rpc://192.168.33.1:10001 tcp+k2rpc://192.168.33.1:10002 tcp+k2rpc://192.168.33.1:10003 tcp+k2rpc://192.168.33.1:10004 tcp+k2rpc://192.168.33.1:10005 tcp+k2rpc://192.168.33.1:10006 tcp+k2rpc://192.168.33.1:10007 tcp+k2rpc://192.168.33.1:10008 tcp+k2rpc://192.168.33.1:10009 tcp+k2rpc://192.168.33.1:10010 tcp+k2rpc://192.168.33.1:10011 tcp+k2rpc://192.168.33.1:10012 tcp+k2rpc://192.168.33.1:10013 tcp+k2rpc://192.168.33.1:10014 tcp+k2rpc://192.168.33.1:10015 tcp+k2rpc://192.168.33.1:10016 tcp+k2rpc://192.168.33.1:10017 tcp+k2rpc://192.168.33.1:10018 tcp+k2rpc://192.168.33.1:10019 --cpuset 0-9 -m 10G --hugepages --rdma mlx5_1 --request_size=1024 --pipeline_depth_count=5 --ack_count=1 --echo_mode=0  --test_duration_s=30
```

## Windows 10 linux subsystem:
- Make sure you're on an open network (e.g. Futurewei or at home)
- Open PowerShell as Administrator and run:
'Enable-WindowsOptionalFeature -Online -FeatureName Microsoft-Windows-Subsystem-Linux'
- restart computer
- Install ubuntu from the windows store
- run ubuntu
- run apt update to update the system
- run apt upgrade to upgrade all packages

- install the X server vcxsvr (manual download) and start it
- install a UI (e.g. apt install xfce4. Start with xfce4-session)
- make sure /var/run/dbus exists and is owned by messagebus:messagebus

## git
### use different upstream
``` s
git remote add 10001 user@k2-bvu-10001.huawei.com:/data/git_repos/DFV_K2_Platform
git pull 10001 feature/KP-128-RDMA_RPC_protocol_handling_and_channel_management
git push -u 10001
```

### sync a fork
``` sh
$ git clone --mirror git@example.com/upstream-repository.git
$ cd upstream-repository.git
$ git push --mirror git@example.com/new-location.git
```
