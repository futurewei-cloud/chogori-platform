# Setup

We used two different machine configurations for our experiments. Config A machines have dual Intel Xeon Silver 4114 CPUs with ten physical cores each, dual Mellanox ConnectX-4 40GbE NICs, and 192 GB of RAM. Config B machines have dual Intel Xeon Gold 5215L CPUs also with ten physical cores each, dual Mellanox ConnectX-5 NICs (each with two 25GbE ports), and 192 GB of RAM. Both configurations run Linux kernel 5.4.

All machines are connected to a Mellanox SN2700 100GbE switch. Ethernet flow control was enabled. Both ports of the Config B NICs are connected to the switch and we deploy one cluster process per port. In all cases we only use physical cores, the other SMT logical core is left idle.

In our experiments we use the following core to cluster component allocation: 2 Config A cores for CPO, 10 Config A cores for TSO, 100 Config A cores for clients, 20 Config B cores for persistence, and 100 Config B cores for servers.

## TPC-C
All transactions types with default transaction mix and 100 warehouses.

## YCSB
Total transaction throughput, one operation per transaction.

# Performance results (latencies with percentiles 50/90/99/999)
| Metric | 2022/01/30 | 2022/03/16 | 2022/04/18 |
| --- | --- | --- | --- |
| <b>TPCC standard {43,4,4,45,4}</b> ||||
| txn/sec | 112K | 141K | 147K |
| NewOrder latency | 708/1010/1770/3240 | 478/680/1280/2550 | 455/650/1200/2100 |
| Abort rate/sec | 5.16K | 6.21K | 6.53K |
| server-side read rate/sec | 2.78M | 3.45M | 3.62M |
| server-side read latency | 2.96/6.96/11.2/16.5 | 1.05/1.82/2.52/6.43 | 1/1.76/2.44/6.2 |
| server-side query rate/sec | 221K | 277K | 290K |
| server-side query latency | 8.55/24.1/125/179 | 3.44/17.8/113/169 | 3.4/17/112/135/198 |
| server-side write rate/sec | 2.54M | 3.17M | 3.33M |
| server-side write latency | 226/1410/2200/3010 | 165/1120/2000/2830 | 148/994/1740K/2200 |
| PUSH latency | 1.3/2.0/2.44/2.48 | 1.3/1.85/2.38/2.48 | 1/1.6/2/2 |
| Flush latency | 204/1400/2170/2940 | 204/1400/2170/2940 | 127/975/1720/2180 |
| TSO call rate/sec | 117K | 145K | 153K |
| TSO latency | 8.47/10.4/15.1/20.1 | 8.53/10.5/15.3/20.4 | 8.3/10/15/20 |
| |
| |
| |
| <b>TPCC N-O/Payment {50,0,0,50,0} </b> ||||
| txn/sec | 145K | 185K | 193K |
| NewOrder latency | 704/939/1330/3120 | 480/675/1020/2520 | 475/650/950/2250 |
| Abort rate/sec | 7.37K | 9.2K | 9.8K |
| server-side read rate/sec | 2.69M | 3.43M | 3.6M |
| server-side read latency | 2.1/4.6/6.3/10.6 | 0.9/1.86/2.5/6.5 | 0.89/1.8/2.4/6.3 |
| server-side query rate/sec | 29.5K | 37.3K | 39K |
| server-side query latency | 10/16.3/34/75 | 5.8/11.6/30.3/67.4 | 5.6/11.2/28.9/65/95 |
| server-side write rate/sec | 2.84M | 3.62M | 3.79M |
| server-side write latency | 85.4/1360/2410/3220 | 109/1060/2040/2890 | 86/970/1740/2230 |
| PUSH latency | 1.2/1.66/2.53/2.94 | 1.3/1.85/2.3/2.47 | 1.5/1.9/2/2 |
| Flush latency | 60/1220/2350/3100 | 88.4/990/2000/2850 | 67/907/1720/2180 |
| TSO call rate/sec | 152K | 195K ||
| TSO latency | 8.3/9.95/14.6/19.3 | 8.42/10.3/15.1/19.6 ||
| |
| |
| |
| <b>YCSB {95,5,uniform}</b> ||||
| Txn/sec |  | 2.55M | 2.62M |
| Txn latency| | 28/36/86/117 | 27/35/79/95 |
| Abort rate/sec |  | <10 | <10 |
| server-side read rate/sec |  | 2.43M | 2.49M |
| server-side read latency |  | 2/2.6/3.5/8.2 | 2/2.4/3/7.3
| server-side query rate/sec | 0 | 0 | 0 |
| server-side query latency | N/A | N/A | N/A |
| server-side write rate/sec |  | 128K | 131K |
| server-side write latency |  | 17.9/25.5/41.1/64.3 | 17.5/22/32/45 |
| PUSH count |  | <3 | 65 |
| PUSH latency |  | 0.5/0.9/0.99/2.99 | 1.3/1.7/2/2 |
| Flush latency |  | 10.5/16.5/30.4/51.4 | 9.9/15/25/37 |
| TSO call rate/sec| | 2.55M | 2.62M |
| TSO latency |  | 9.3/12.4/17.2/22.1 | 9/13/18/24 |
| |
| |
| |
| <b>YCSB {50,50,uniform}</b> ||||
| Txn/sec |  | 1.43M | 1.52M |
| Txn latency| | 55.3/93.2/161/279 | 55/87/132/189 |
| Abort rate/sec |  | <12 | 25 |
| server-side read rate/sec |  | 715K | 760K |
| server-side read latency |  | 2.2/2.9/3.8/10.4 | 2.2/2.7/3.4/7.4 |
| server-side query rate/sec | 0 | 0 | 0 |
| server-side query latency | N/A | N/A | N/A |
| server-side write rate/sec |  | 715K | 760K |
| server-side write latency |  | 18.2/33.4/69.8/133 | 17.8/30/53/82 |
| PUSH count |  | <40 | <40 |
| PUSH latency |  | 1.5/1.8/2.1/2.1 | 1/1.7/2/2 |
| Flush latency |  | 11.9/26.6/62.3/126 | 11/24/47/77 |
| TSO call rate/sec| | 1.43M | 1.52M |
| TSO latency |  | 8/11/15/19 | 8/10/14.7/18 |
| |
| |
| |
| <b>YCSB {95,5,szipfian}</b> ||||
| Txn/sec |  | 250K | 258K |
| Txn latency| | 26/36/86/216 | 25/34/78/160 |
| Abort rate/sec |  | 1.8K | 1.61K |
| server-side read rate/sec |  | 232K | 261K |
| server-side read latency |  | 1.8/2.6/4.9/10.5 | 1.8/2.4/4.7/8.7 |
| server-side query rate/sec | 0 | 0 | 0 |
| server-side query latency | N/A | N/A | N/A |
| server-side write rate/sec |  | 14K | 16.5K |
| server-side write latency |  | 17.8/30.8/271/576 | 17/22/67/152 |
| PUSH count |  | 2K | 2.45K |
| PUSH latency |  | 0.5/0.9/1.2/6.9 | 0.5/0.9/1/4.7 |
| Flush latency |  | 10.5/16.5/349/587 | 9/13/35/95 |
| TSO call rate/sec| | 255K | 274K |
| TSO latency |  | 8/10/16/25 | 7.5/9.5/15/23 |
| |
| |
| |
| <b>YCSB {50,50,szipfian}</b> ||||
| Txn/sec |  | 47.3K | 66K |
| Txn latency| | 55/80/112/716 | 55/77/105/375 |
| Abort rate/sec |  | 2.7K | 2.5K |
| server-side read rate/sec |  | 25K | 38.2K |
| server-side read latency |  | 2.2/3.1/8.4/13.4 | 2.1/2.9/5.2/10 |
| server-side query rate/sec | 0 | 0 | 0 |
| server-side query latency | N/A | N/A | N/A |
| server-side write rate/sec |  | 27K | 41K |
| server-side write latency |  | 17.4/32.5/402/488 | 16.6/21.6/204/275 |
| PUSH count |  | 3K | 3.11K |
| PUSH latency |  | 0.5/0.9/0.99/6.99 | 0.5/0.9/1/4.5 |
| Flush latency |  | 9.8/31.6/459/679 | 8.8/12.8/192/265 |
| TSO call rate/sec| | 51K | 80.4K |
| TSO latency |  | 8/10/16/31 | 7.7/9.3/16/27 |
