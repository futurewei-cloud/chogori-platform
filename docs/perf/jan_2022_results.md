## Performance Results, 1/31/2022

### Setup

We used two different machine configurations for our experiments. Config A machines have dual Intel Xeon Silver 4114 CPUs with ten physical cores each, dual Mellanox ConnectX-4 40GbE NICs, and 192 GB of RAM. Config B machines have dual Intel Xeon Gold 5215L CPUs also with ten physical cores each, dual Mellanox ConnectX-5 NICs (each with two 25GbE ports), and 192 GB of RAM. Both configurations run Linux kernel 5.4.

All machines are connected to a Mellanox SN2700 100GbE switch. Ethernet flow control was enabled. Both ports of the Config B NICs are connected to the switch and we deploy one cluster process per port. In all cases we only use physical cores, the other SMT logical core is left idle.

In our experiments we use the following core to cluster component allocation: 1 Config A cores for CPO, 10 Config A cores for TSO, 100 Config A cores for clients, 20 Config B cores for persistence, and 100 Config B cores for servers.

### TPC-C
All transactions types with default transaction mix and 100 warehouses.

Total transaction throughput: **112K/second**

### YCSB
Total transaction throughput, one operation per transaction.

Zipfian requests, 50\% read 50\% write: **52K/second**

Zipfian requests, 95\% read 5\% write: **250K/second**

Uniform requests, 50\% read 50\% write: **1.28M/second**

Uniform requests, 95\% read 5\% write: **2.27M/second**
