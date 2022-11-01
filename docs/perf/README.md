# Setup

We used two different machine configurations for our experiments. Config A machines have dual Intel Xeon Silver 4114 CPUs with ten physical cores each, dual Mellanox ConnectX-4 40GbE NICs, and 192 GB of RAM. Config B machines have dual Intel Xeon Gold 5215L CPUs also with ten physical cores each, dual Mellanox ConnectX-5 NICs (each with two 25GbE ports), and 192 GB of RAM. Both configurations run Linux kernel 5.4.

All machines are connected to a Mellanox SN2700 100GbE switch. Ethernet flow control was enabled. Both ports of the Config B NICs are connected to the switch and we deploy one cluster process per port. In all cases we only use physical cores, the other SMT logical core is left idle.

In our experiments we use the following core to cluster component allocation: 2 Config A cores for CPO, 10 Config A cores for TSO, 100 Config A cores for clients, 20 Config B cores for persistence, and 100 Config B cores for servers.

## TPC-C
All transactions types with default transaction mix and 100 warehouses.

## YCSB
Total transaction throughput, one operation per transaction.

# Performance results (latencies in microseconds with percentiles 50 / 90 / 99 / 999)
| Metric | 2022/01/30 | 2022/03/16 | 2022/04/18 | 2022/10/27 |
| --- | --- | --- | --- | --- |
| <b>TPCC standard {43,4,4,45,4}</b> |||||
| txn/sec | 112K | 141K | 147K | 146K|
| NewOrder latency | 708 / 1010 / 1770 / 3240 | 478 / 680 / 1280 / 2550 | 455 / 650 / 1200 / 2100 | 463 / 657 / 1.15K / 2.10K |
| Abort rate/sec | 5.2K | 6.2K | 6.5K |6.55K|
| server-side read rate/sec | 2.78M | 3.45M | 3.62M |3.58M|
| server-side read latency | 3 / 7 / 11.2 / 16.5 | 1.1 / 1.8 / 2.5 / 6.4 | 1 / 1.8 / 2.4 / 6.2 | 1.05 / 1.80 / 2.44 / 6.43 |
| server-side query rate/sec | 221K | 277K | 290K | 290K |
| server-side query latency | 8.6 / 24 / 125 / 179 | 3.4 / 18 / 113 / 169 | 3.4 / 17 / 112 / 135 | 3.45 / 18.0 / 117 / 136 |
| server-side write rate/sec | 2.54M | 3.17M | 3.33M | 3.30M |
| server-side write latency | 226 / 1410 / 2200 / 3010 | 165 / 1120 / 2000 / 2830 | 148 / 994 / 1740 / 2200 | 148 / 980 / 1.72K / 2.12K |
| PUSH latency | 1.3 / 2.0 / 2.4 / 2.5 | 1.3 / 1.9 / 2.4 / 2.5 | 1 / 1.6 / 2 / 2 | 1.18 / 1.61 / 1.95 / 2.06 |
| Flush latency | 204 / 1400 / 2170 / 2940 | 204 / 1400 / 2170 / 2940 | 127 / 975 / 1720 / 2180 | 127 / 956 / 1.70K / 2.11K |
| TSO call rate/sec | 117K | 145K | 153K | 152K |
| TSO latency | 8.5 / 10.4 / 15 / 20 | 8.5 / 10.5 / 15 / 20 | 8.3 / 10 / 15 / 20 | 8.50 / 10.6 / 15.5 / 20.8 |
| |
| |
| |
| <b>TPCC N-O/Payment {50,0,0,50,0} </b> ||||
| txn/sec | 145K | 185K | 193K | 193K |
| NewOrder latency | 704 / 939 / 1330 / 3120 | 480 / 675 / 1020 / 2520 | 475 / 650 / 950 / 2250 | 473 / 652 / 947 / 2.22K |
| Abort rate/sec | 7.37K | 9.2K | 9.8K | 9.74K |
| server-side read rate/sec | 2.7M | 3.4M | 3.6M | 3.57M |
| server-side read latency | 2.1 / 4.6 / 6.3 / 10.6 | 0.9 / 1.9 / 2.5 / 6.5 | 0.9 / 1.8 / 2.4 / 6.3 | 0.870 / 1.84 / 2.42 / 6.46 |
| server-side query rate/sec | 29.5K | 37.3K | 39K | 39.0K |
| server-side query latency | 10 / 16 / 34 / 75 | 5.8 / 12 / 30 / 67 | 5.6 / 11 / 29 / 65 | 5.70 / 10.9 / 28.5 / 65.9 |
| server-side write rate/sec | 2.8M | 3.6M | 3.8M | 3.76M |
| server-side write latency | 85 / 1360 / 2410 / 3220 | 109 / 1060 / 2040 / 2890 | 86 / 970 / 1740 / 2230 | 93.7 / 949 / 1.73K / 2.17K |
| PUSH latency | 1.2 / 1.7 / 2.5 / 2.9 | 1.3 / 1.9 / 2.3 / 2.5 | 1.5 / 1.9 / 2 / 2 | 1.09 / 1.56 / 1.85 / 2.05 |
| Flush latency | 60 / 1220 / 2350 / 3100 | 88 / 990 / 2000 / 2850 | 67 / 907 / 1720 / 2180 | 73.3 / 893 / 1.71K / 2.13K |
| TSO call rate/sec | 152K | 195K | 203K | 202K |
| TSO latency | 8.3 / 10 / 14.6 / 19.3 | 8.4 / 10.3 / 15.1 / 19.6 | 8.2 / 9.6 / 14.5 / 19.3 | 8.38 / 10.2 / 15.1 / 20.2 |
| |
| |
| |
| <b>YCSB {95,5,uniform}</b> |||||
| Txn/sec |  | 2.55M | 2.62M | 2.43M |
| Txn latency| | 28 / 36 / 86 / 117 | 27 / 35 / 79 / 95 | 30.1 / 37.6 / 79.0 / 95.3 |
| Abort rate/sec |  | <10 | <10 | (0.776) <1|
| server-side read rate/sec |  | 2.43M | 2.49M | 2.32M |
| server-side read latency |  | 2 / 2.6 / 3.5 / 8.2 | 2 / 2.4 / 3 / 7.3 | 1.96 / 2.42 / 2.97 / 7.24 |
| server-side query rate/sec | 0 | 0 | 0 | 0 |
| server-side query latency | N/A | N/A | N/A | N/A |
| server-side write rate/sec |  | 128K | 131K | 122K |
| server-side write latency |  | 18 / 26 / 41 / 64 | 18 / 22 / 32 / 45 | 17.4 / 21.9 / 31.2 / 44.0 |
| PUSH count |  | < 3 | 65 | (2.0) < 3 |
| PUSH latency |  | 0.5 / 0.9 / 1 / 3 | 1.3 / 1.7 / 2 / 2 | 1.56 / 1.73 / 2.04 / 2.07 |
| Flush latency |  | 10.5 / 17 / 30 / 51 | 9.9 / 15 / 25 / 37 | 9.67 / 14.5 / 23.8 / 35.9 |
| TSO call rate/sec| | 2.55M | 2.62M | 2.43M |
| TSO latency |  | 9.3 / 12.4 / 17 / 22 | 9 / 13 / 18 / 24 | 8.89 / 12.0 / 17.8 / 25.6 |
| |
| |
| |
| <b>YCSB {50,50,uniform}</b> |||||
| Txn/sec |  | 1.43M | 1.52M | 1.50M |
| Txn latency| | 55 / 93 / 161 / 279 | 55 / 87 / 132 / 189 | 53.8 / 87.6 / 131 / 188 |
| Abort rate/sec |  | <12 | 25 | (4.09) <5 |
| server-side read rate/sec |  | 715K | 760K | 748K |
| server-side read latency |  | 2.2 / 2.9 / 3.8 / 10.4 | 2.2 / 2.7 / 3.4 / 7.4 | 2.19 / 2.70 / 3.43 / 7.52 |
| server-side query rate/sec | 0 | 0 | 0 | 0 |
| server-side query latency | N/A | N/A | N/A | N/A |
| server-side write rate/sec |  | 715K | 760K | 748K |
| server-side write latency |  | 18 / 33 / 70 / 133 | 18 / 30 / 53 / 82 | 17.8 / 29.6 / 52.2 / 80.0
| PUSH count |  | <40 | <40 | (11.4) <12 |
| PUSH latency |  | 1.5 / 1.8 / 2.1 / 2.1 | 1 / 1.7 / 2 / 2 | 1.24 / 1.64 / 1.97 / 2.06 |
| Flush latency |  | 12 / 27 / 62 / 126 | 11 / 24 / 47 / 77 | 11.0 / 23.2 / 45.8 / 75.9 |
| TSO call rate/sec| | 1.43M | 1.52M | 1.50M |
| TSO latency |  | 8 / 11 / 15 / 19 | 8 / 10 / 15 / 18 | 7.80 / 10.2 / 14.9 / 20.3 |
| |
| |
| |
| <b>YCSB {95,5,szipfian}</b> |||||
| Txn/sec |  | 250K | 258K | 269K |
| Txn latency| | 26 / 36 / 86 / 216 | 25 / 34 / 78 / 160 | 28.9 / 37.0 / 78.9 / 162 |
| Abort rate/sec |  | 1.8K | 1.61K | 1.76K |
| server-side read rate/sec |  | 232K | 261K | 259K |
| server-side read latency |  | 1.8 / 2.6 / 4.9 / 10.5 | 1.8 / 2.4 / 4.7 / 8.7 | 1.81 / 2.43 / 4.45 / 7.66 |
| server-side query rate/sec | 0 | 0 | 0 | 0 |
| server-side query latency | N/A | N/A | N/A | N/A |
| server-side write rate/sec |  | 14K | 16.5K | 16.3K |
| server-side write latency |  | 18 / 31 / 271 / 576 | 17 / 22 / 67 / 152 | 17.0 / 22.5/ 105 / 257 |
| PUSH count |  | 2K | 2.45K | 2.07K |
| PUSH latency |  | 0.5 / 0.9 / 1.2 / 6.9 | 0.5 / 0.9 / 1 / 4.7 | 0.506 / 0.910 / 1.05 / 4.61 |
| Flush latency |  | 10.5 / 17 / 349 / 587 | 9 / 13 / 35 / 95 | 8.91 / 13.5 / 76.2 / 220 |
| TSO call rate/sec| | 255K | 274K | 275K |
| TSO latency |  | 8 / 10 / 16 / 25 | 7.5 / 9.5 / 15 / 23 | 7.87 / 9.96 / 15.7 / 24.1 |
| |
| |
| |
| <b>YCSB {50,50,szipfian}</b> |||||
| Txn/sec |  | 47.3K | 66K | 59.5K |
| Txn latency| | 55 / 80 / 112 / 716 | 55 / 77 / 105 / 375 | 55.3 / 77.6 / 106 / 406 |
| Abort rate/sec |  | 2.7K | 2.5K | 2.92K |
| server-side read rate/sec |  | 25K | 38.2K | 30.7K |
| server-side read latency |  | 2.2 / 3.1 / 8.4 / 13.4 | 2.1 / 2.9 / 5.2 / 10 | 2.17 / 2.92 / 5.16 / 10.1 |
| server-side query rate/sec | 0 | 0 | 0 | 0 |
| server-side query latency | N/A | N/A | N/A | N/A |
| server-side write rate/sec |  | 27K | 41K | 33.8K |
| server-side write latency |  | 17.4 / 32.5 / 402 / 488 | 16.6 / 21.6 / 204 / 275 | 16.8 / 24.8 / 321 / 574 |
| PUSH count |  | 3K | 3.1K | 3.21K |
| PUSH latency |  | 0.5 / 0.9 / 1 / 7 | 0.5 / 0.9 / 1 / 4.5 | 0.502 / 0.904 / 0.994 / 4.43 |
| Flush latency |  | 10 / 32 / 459 / 679 | 9 / 13 / 192 / 265 | 8.83 / 14.5 / 316 / 668 |
| TSO call rate/sec| | 51K | 80.4K | 64.2K |
| TSO latency |  | 8 / 10 / 16 / 31 | 7.7 / 9.3 / 16 / 27 | 7.59 / 9.74 / 16.7 / 32.1 |
