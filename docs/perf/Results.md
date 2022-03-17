# Setup

We used two different machine configurations for our experiments. Config A machines have dual Intel Xeon Silver 4114 CPUs with ten physical cores each, dual Mellanox ConnectX-4 40GbE NICs, and 192 GB of RAM. Config B machines have dual Intel Xeon Gold 5215L CPUs also with ten physical cores each, dual Mellanox ConnectX-5 NICs (each with two 25GbE ports), and 192 GB of RAM. Both configurations run Linux kernel 5.4.

All machines are connected to a Mellanox SN2700 100GbE switch. Ethernet flow control was enabled. Both ports of the Config B NICs are connected to the switch and we deploy one cluster process per port. In all cases we only use physical cores, the other SMT logical core is left idle.

In our experiments we use the following core to cluster component allocation: 1 Config A cores for CPO, 10 Config A cores for TSO, 100 Config A cores for clients, 20 Config B cores for persistence, and 100 Config B cores for servers.

## TPC-C
All transactions types with default transaction mix and 100 warehouses.

## YCSB
Total transaction throughput, one operation per transaction.

# Performance results (latencies with percentiles 50/90/99/999)
| Metric | 2022/01/30 | 2022/03/16 |
| --- | --- | --- |
| <b>TPCC standard {43,4,4,45,4}</b> |||
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
| <b>TPCC N-O/Payment {50,0,0,50,0} </b> |||
| txn/sec | 145K | 185K |
| NewOrder latency | 704/939/1330/3120 | 480/675/1020/2520 |
| Abort rate/sec | 7.37K | 9.2K |
| server-side read rate/sec | 2.69M | 3.43M |
| server-side read latency | 2.1/4.6/6.3/10.6 | 0.9/1.86/2.5/6.5 |
| server-side query rate/sec | 29.5K | 37.3K |
| server-side query latency | 10/16.3/34/75 | 5.8/11.6/30.3/67.4 |
| server-side write rate/sec | 2.84M | 3.62M |
| server-side write latency | 85.4/1360/2410/3220 | 109/1060/2040/2890 |
| PUSH latency | 1.2/1.66/2.53/2.94 | 1.3/1.85/2.3/2.47 |
| Flush latency | 60/1220/2350/3100 | 88.4/990/2000/2850 |
| TSO call rate/sec | 152K | 195K |
| TSO latency | 8.3/9.95/14.6/19.3 | 8.42/10.3/15.1/19.6 |
| |
| |
| |
| <b>YCSB {95,5,uniform}</b> |||
| Txn/sec |  | 2.55M |
| Txn latency| | 28/36/86/117 |
| Abort rate/sec |  | <10 |
| server-side read rate/sec |  | 2.43M |
| server-side read latency |  | 2/2.6/3.5/8.2 |
| server-side query rate/sec | 0 | 0 |
| server-side query latency | N/A | N/A |
| server-side write rate/sec |  | 128K |
| server-side write latency |  | 17.9/25.5/41.1/64.3 |
| PUSH count |  | <3 |
| PUSH latency |  | 0.5/0.9/0.99/2.99 |
| Flush latency |  | 10.5/16.5/30.4/51.4 |
| TSO call rate/sec| | 2.55M |
| TSO latency |  | 9.3/12.4/17.2/22.1 |
| |
| |
| |
| <b>YCSB {50,50,uniform}</b> |||
| Txn/sec |  | 1.43M |
| Txn latency| | 55.3/93.2/161/279 |
| Abort rate/sec |  | <12 |
| server-side read rate/sec |  | 715K |
| server-side read latency |  | 2.2/2.9/3.8/10.4 |
| server-side query rate/sec | 0 | 0 |
| server-side query latency | N/A | N/A |
| server-side write rate/sec |  | 715K |
| server-side write latency |  | 18.2/33.4/69.8/133 |
| PUSH count |  | <40 |
| PUSH latency |  | 1.5/1.8/2.1/2.1 |
| Flush latency |  | 11.9/26.6/62.3/126 |
| TSO call rate/sec| | 1.43M |
| TSO latency |  | 8/11/15/19 |
| |
| |
| |
| <b>YCSB {95,5,szipfian}</b> |||
| Txn/sec |  | 250K |
| Txn latency| | 26/36/86/216 |
| Abort rate/sec |  | 1.8K |
| server-side read rate/sec |  | 232K |
| server-side read latency |  | 1.8/2.6/4.9/10.5 |
| server-side query rate/sec | 0 | 0 |
| server-side query latency | N/A | N/A |
| server-side write rate/sec |  | 14K |
| server-side write latency |  | 17.8/30.8/271/576 |
| PUSH count |  | 2K |
| PUSH latency |  | 0.5/0.9/1.2/6.9 |
| Flush latency |  | 10.5/16.5/349/587 |
| TSO call rate/sec| | 255K |
| TSO latency |  | 8/10/16/25 |
| |
| |
| |
| <b>YCSB {50,50,szipfian}</b> |||
| Txn/sec |  | 47.3K |
| Txn latency| | 55/80/112/716 |
| Abort rate/sec |  | 2.7K |
| server-side read rate/sec |  | 25K |
| server-side read latency |  | 2.2/3.1/8.4/13.4 |
| server-side query rate/sec | 0 | 0 |
| server-side query latency | N/A | N/A |
| server-side write rate/sec |  | 27K |
| server-side write latency |  | 17.4/32.5/402/488 |
| PUSH count |  | 3K |
| PUSH latency |  | 0.5/0.9/0.99/6.99 |
| Flush latency |  | 9.8/31.6/459/679 |
| TSO call rate/sec| | 51K |
| TSO latency |  | 8/10/16/31 |
