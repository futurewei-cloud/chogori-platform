## Performance Results, 9/23/2021

### Setup

We used two different machine configurations for our experiments. Config A machines have dual Intel Xeon Silver 4114 CPUs with ten physical cores each, dual Mellanox ConnectX-4 40GbE NICs, and 192 GB of RAM. Config B machines have dual Intel Xeon Gold 5215L CPUs also with ten physical cores each, dual Mellanox ConnectX-5 NICs (each with two 25GbE ports), and 192 GB of RAM. Both configurations run Linux kernel 5.4.

All machines are connected to a Mellanox SN2700 100GbE switch. We did not enable Ethernet flow control. Both ports of the Config B NICs are connected to the switch and we deploy one cluster process per port. In all cases we only use physical cores, the other SMT logical core is left idle.

In our experiments we use the following core to cluster component allocation: 1 Config A cores for CPO, 10 Config A cores for TSO, up to 80 Config A cores for clients, 20 Config B cores for persistence, and 80 Config B cores for servers.

### Benchmarks
TPC-C benchmarks include all transactions types with default transaction mix and 80 warehouses.

For YCSB we show results for both the 50\% read 50\% write Workload A and the 95\% read and 5\% write Workload B; both use Zipfian request distributions. We loaded the database with 150,000 1KB records per core (12M records total). One operation per transaction was used.

