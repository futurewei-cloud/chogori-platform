# Initial performance results as of 5/15/2020 with no performance tuning thus far

## Experimental setup
- Each cluster component is run with on a single NUMA node on a server with Intel Xeon Silver 4114 CPUs.
- Each CPU has 10 physical cores and 20 hardware threads running at 2.2Ghz base clock.
- Each NUMA node has 96GB of memory on six channels.
- The servers run Ubuntu 18.04.1 with Linux Kernel 5.0.
- The servers are connected with Mellanox 40Gb Connectx 4 EN RDMA NICs and a 40Gb switch.
- Seastar is configured to use 2MB hugepages, poll-mode, and Linux AIO backend.
- For tests running multiple cores per component, cores are selected such that physical cores are not shared.
- All connections are setup to use RDMA after initial negotiation over TCP.
- For K23SI and TPC-C tests, the K23SI server issues remote persistence calls to a server over RDMA. The persistence calls include the full data sent over the network but are not written to disk.


## Transport microbenchmark results (src/k2/cmd/txbench/txbench*)
### 10 cores each client and server, 1KB request and 10B response sizes:

30 Gb/sec aggregate throughput
Request Latency percentiles:
- p50:    1.6  usec
- p90:    2.3  usec
- p99:   13    usec
- p99.9: 15    usec


## RPC microbenchmark results (src/k2/cmd/txbench/rpcbench*)
### 10 cores each client and server, 1 connection per client core, 1KB user request size, 10B user response size:

26 Gbit/sec aggregate throughput
Request Latency percentiles:
- p50:   12 usec
- p90:   14 usec
- p99:   17 usec
- p99.9: 24 usec


### 10 cores each client and server, 4 connections per client core, 1KB user request size, 10B user response size:

30 Gbit/sec aggregate throughput
Request Latency percentiles:
- p50:   43 usec
- p90:   52 usec
- p99:   58 usec
- p99.9: 72 usec


## K23SI Transaction microbenchmark (src/k2/cmd/txbench/k23sibench_client.cpp)
These tests involve six serialized round-trip network requests, comprising of TSO, 3SI server,
and persistence requests.


### Latency test
1 client core, 1 server core, pipeline depth 1, 1 read and write per transaction:

16000 transactions/sec
Transaction Latency percentiles:
- p50:   50.6 usec
- p90:   54.4 usec
- p99:   59   usec
- p99.9: 66   usec


### Single server core throughput test
2 client cores, 1 server core, pipeline depth 2, 1 read and write per transaction:

19600 aggregate transactions/sec
Transaction Latency percentiles:
- p50:   178 usec
- p90:   194 usec
- p99:   198 usec
- p99.9: 232 usec


## TPC-C Benchmark, New Order and Payment transaction types (src/k2/cmd/tpcc/)

### 1 client core, 1 server core, 1 concurrent transaction, 1 warehouse:

2200 transactions/sec
70000 read or write operations/sec

New Order Transaction Latency percentiles:
- p50:   460 usec
- p90:   630 usec
- p99:   760 usec
- p99.9: 840 usec


### 4 client cores, 2 servers with 4 cores each, 12 warehouses:

7200 aggregate transactions/sec
208000 aggregate read or write operations/sec

New Order Transaction Latency percentiles:
- p50:    500 usec
- p90:    900 usec
- p99:   1200 usec
- p99.9: 1500 usec
