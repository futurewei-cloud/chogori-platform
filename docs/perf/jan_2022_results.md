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

# Perf (latencies with percentiles 50/90/99/999)
| Metric | 2022/01/30 | 2022/03/16 |
| --- | --- | --- |
| <b style="color: #008800">TPCC {43,4,4,45,4}</b> |||
| txn/sec | 112K | 141K |
| NewOrder latency | 708/1010/1770/3240 | 478/680/1280/2550 |
| Abort rate/sec | 5.16K | 6.21 |
| server-side read rate/sec | 2.78M | 3.45M |
| server-side read latency | 2.96/6.96/11.2/16.5 | 1.05/1.82/2.52/6.43 |
| server-side query rate/sec | 221K | 277K |
| server-side query latency | 8.55/24.1/125/179 | 3.44/17.8/113/169 |
| server-side write rate/sec | 2.54M | 3.17M |
| server-side write latency | 226/1410/2200/3010 | 165/1120/2000/2830 |
| PUSH latency | 1.3/2.0/2.44/2.48 | 1.3/1.85/2.38/2.48 |
| Flush latency | 204/1400/2170/2940 | 204/1400/2170/2940 |
| TSO call rate/sec | 117K | 145K |
| TSO latency | 8.47/10.4/15.1/20.1 | 8.53/10.5/15.3/20.4 |
| |
| |
| |
| <b style="color: #008800">YCSB {95,5,uniform}</b> |||
| Txn/sec |  | 2.55M |
| Abort rate/sec |  | <5 |
| Client read latency |  | 27.9/33.3/42.2/52.9 |
| TSO call rate/sec | | 2.55M
| TSO latency |  | 9.35/12.4/17.2/22 |
| |
| |
| |
| <b style="color: #008800">YCSB {95,5,szipfian}</b> |||
| Txn/sec |  | 245K |
| Abort rate/sec |  | 1.75K |
| Client read latency |  | 26/32/48/89 |
| TSO call rate/sec| | 245K | 
| TSO latency |  | 8/10/16/25 |

