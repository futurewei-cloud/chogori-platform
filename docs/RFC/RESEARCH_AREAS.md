This file lists some forward looking areas for research in the Chogori project. Within each area, we list possible topics

## Protocol optimizations
The current transaction protocol (K23SI) supports strong, external consistency, even for globally distributed data.

### Async writes
Currently, writes in a transaction are sent directly to the owning partition, which evaluates the request and sends the result back to the client. This allows the client to abort their transaction early in cases of write conflicts.

It is possible instead to send the writes as 1-way messages, and having the partition notify the TRH about the outcome of a write. At commit time, the client has to notify the TRH about all keys which were written as part of a transaction. This way the TRH can tell if all writes have made it successfully and allow/abort the commit as needed.

This approach can potentially reduce the network communication as part of a transaction for cases where the client logic isn't parallelized well (e.g. the client is sending writes in sequence instead of issuing them in parallel).

### Relaxed consistency transactions
Our current transaction protocol supports strong, external consistency for global transactions. In some scenarions (e.g. global transactions with multiple replicas), it may be beneficial to conduct transactions with reduced consistency requirements. 
Some possible topics to explore:
- CRDTs as first-class objects. CRDTs work really great with eventual consistency models as they guarantee convergence. 
- Support for multiple replicas of a partition (i.e. geo-distributed replicas)
- Providing different consistency abilities at the transaction level, not at the database as a whole.

## DRAM indexing
This topic covers strictly in-DRAM indexing of data for SKV. We currently use stl::map (as we need ordered key support). We would like to explore other data structures (HOT, tries, skip list, b-trees, LSM-allocator-friendly trees) and understand their benefits and drawbacks

## Tiered memory & persistence
### Using append-log storage for durability
- how to efficiently perform compaction and garbage collection
- speedup recovery time by allowing log pages to be used directly

### Using SCM for durability
- Hybrid data indexers, allowing in-DRAM traversal with redo-log-based durability
- Multi-replica partitions(master/slave) architecture, with support for fast failover
- Evaluate DRAM-SCM ratio for best performance (user/ML tunable?)

### Using local NVMe for durability
- Similar ideas and concerns to the SCM solution above
- Can use ZNS for mapping storage partitions to dedicated NVMe capacity
- experiment with NVMe as a volatile storage tier

### SCM for cost-reduction (non-persistent mode)
- there is prior work here but we're open to new suggestions in this space

## Generic push-down computation
- secondary indexes(with projection?)
- joins
- graph push-down

## Graph DB
- Investigate how to run a Graph database query engine on top of SKV.
- Evaluate standard graph workloads to determine how data is used to answer graph queries
- Easier support for different edge types (e.g. LIST data structure support in SKV)
- Investigate missing features in SKV for supporting standard graph workloads.
- Survey existing graph databases and their features.

## AI/ML workloads
- Survey existing solutions and identify features/patterns
- Determine missing features in SKV

## K23SI paper
We submitted a paper on K23SI transaction protocol which was rejected as it needs some additional comparison work:
- implement and evaluate other locking solutions
- wait vs no-wait conflict resolution
- newer vs older preference on conflict resolution

## Low level topics
Our system is designed to be message-oriented. Thus we are very flexible w.r.t. the network implementation that is used to deliver messages. Currently we support Posix TCP and RDMA networks. We would like to explore other high-performance network solutions, which are more scalable than RDMA while delivering similar performance:
- RDMA replacement
- Using programmable NICs for transport impl
- DPDK-based TCP? eRPC?

## Using local clock
The current system requires a roundtrip to our central clock sequencer (TSO) once for each transaction. While this cost is very low (5-10us), there are solutions which can allow us to have access to a low error(<1us) clock locally on each machine.
- explore tradeoffs in such a setup. What is the cost as cluster size increases (e.g. HYGENs approach requires growth in computational resources as cluster size increases).

## OLAP
This topic covers features to support analytical workloads

### SKV partition-level statistics
Partition-level statistics can be gathered in SKV(e.g. range of keys available, #keys, key distribution, etc). We can use this data in the query planning (cost estimation) phase of Chogori-SQL/opengauss to improve query performance

### columnar storage

## Applying ML internally
We can use ML to dynamically determine system configuration parameters, such as:
- Query page & row scan limits
- network configuration (timeouts, retries, rdma options, batch sizes)
- picking among different indexers
- apply in tiered storage for eviction/prefetch strategy tuneup
