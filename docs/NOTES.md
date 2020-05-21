## Run docker with RDMA
k2-bvu-20001 and k2-bvu-20002 are the two machines connected back to back (fastest rdma intercon)

### command
``` s
docker run --privileged --network=host -v "/dev/:/dev" -v "/sys/:/sys/" -it --rm -e RDMAV_HUGEPAGES_SAFE=1 ${IMG_NAME}
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
# IP here is the IP on which the server should listen
# For raw transport benchmark:
IP=192.168.33.2 && ./build/src/k2/cmd/txbench/txbench_server --tcp_endpoints tcp+k2rpc://${IP}:10000 tcp+k2rpc://${IP}:10001 tcp+k2rpc://${IP}:10002 tcp+k2rpc://${IP}:10003 tcp+k2rpc://${IP}:10004 tcp+k2rpc://${IP}:10005 tcp+k2rpc://${IP}:10006 tcp+k2rpc://${IP}:10007 tcp+k2rpc://${IP}:10008 tcp+k2rpc://${IP}:10009 --cpuset 10-19 -c 10 -m 10G --hugepages --rdma mlx5_1

# IP here is the IP on which the server is listening
# For RPC transport benchmark:
IP=192.168.33.2 && ./build/src/k2/cmd/txbench/rpcbench_server --tcp_endpoints tcp+k2rpc://${IP}:10000 tcp+k2rpc://${IP}:10001 tcp+k2rpc://${IP}:10002 tcp+k2rpc://${IP}:10003 tcp+k2rpc://${IP}:10004 tcp+k2rpc://${IP}:10005 tcp+k2rpc://${IP}:10006 tcp+k2rpc://${IP}:10007 tcp+k2rpc://${IP}:10008 tcp+k2rpc://${IP}:10009 --cpuset 10-19 -c 10 -m 10G --hugepages --rdma mlx5_1
```

## Start client
``` s
# IP here is the IP on which the server should listen
# For raw transport benchmark:
IP=192.168.33.2 PR="tcp+k2rpc"&& ./build/src/k2/cmd/txbench/txbench_client --tcp_remotes ${PR}://${IP}:10000 ${PR}://${IP}:10001 ${PR}://${IP}:10002 ${PR}://${IP}:10003 ${PR}://${IP}:10004 ${PR}://${IP}:10005 ${PR}://${IP}:10006 ${PR}://${IP}:10007 ${PR}://${IP}:10008 ${PR}://${IP}:10009 --cpuset 0-9 -c 10 -m 10G --hugepages --rdma mlx5_1 --request_size=1024 --pipeline_depth_count=5 --ack_count=1 --echo_mode=0  --test_duration_s=30

# IP here is the IP on which the server is listening
# For RPC transport benchmark:
IP=192.168.33.2 PR="auto-rrdma+k2rpc"&& ./build/src/k2/cmd/txbench/rpcbench_client --remote_eps ${PR}://${IP}:10000 ${PR}://${IP}:10001 ${PR}://${IP}:10002 ${PR}://${IP}:10003 ${PR}://${IP}:10004 ${PR}://${IP}:10005 ${PR}://${IP}:10006 ${PR}://${IP}:10007 ${PR}://${IP}:10008 ${PR}://${IP}:10009 --cpuset 0-9 -c 10 -m 10G --hugepages --rdma mlx5_1 --request_size=1024 --response_size=10 --pipeline_depth=5  --test_duration=30s --multi_conn=1 --copy_data=false
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
git remote add 10001 user@${host}:/data/git_repos/${pkg}
git pull 10001 feature/KP-128-RDMA_RPC_protocol_handling_and_channel_management
git push -u 10001
```

### sync a fork
``` sh
$ git clone --mirror git@example.com/upstream-repository.git
$ cd upstream-repository.git
$ git push --mirror git@example.com/new-location.git
```
