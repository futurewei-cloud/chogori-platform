# SCM-Based Durability

## Overview

- DO use SCM persistence for durability
- Two hot replicas, three copies of data
- No Plog for now, later we could use async operations for cold storage / disaster recovery

Happy path write operation overview:
- SKVRecord written to SCM via ntstore
- Indexer (without read timestamps) mirrored on SCM
- Current persistence writes and flushes are sent to two replicas
- Replicas write to SCM locally and respond to master
- Leader responds to client if at least half of replicas respond

Benefits:
- Only two messages sent on writes or transaction end, instead of 4+ for Plog plus warm replica design.
- Replicas are hot so failover is faster and simpler.
- Possible cost reduction since we use SCM for durability and do not need an extra copy on disk, and fewer hosts are needed. More DRAM+SCM is required than a design with a single warm replica though.
- Copies on persistent media when we respond to client, instead of relying on copies that are temporarily only in memory.

## SCM Data format and GC

On SCM:
1. Write-Ahead Operation w/ index (single value)
2. Fence+flush
3. Persistent indexer update using general purpose or SCM-tuned allocator
5. Last index written to SCM (single value)
6. Fence+flush

If replica failure, leader notifies on next operation, then on other replica:
1. Start full WAL including data on SCM (e.g. deque)
2. Freeze SCM indexer, DRAM indexer updates as normal
3. Full SCM state minus WAL bulk copied to new replica (RDMA)
4. WAL replayed on new replica and old replica for SCM state, meanwhile new replica is also buffering requests from leader to be replayed in correct order
5. New replica notifies old replica and leader when it is caught up.

Snapshoting not needed but could reduce data needed for step 3 by reducing fragmentation, maybe need to look into a specialized allocator.

## Leader failure

- Heartbeat lost threshold (HLT) signals master failure.
- CPO stops heartbeats and waits for HLT heartbeats. This ensures master will self-down in the case of a network partition.
- As part of heartbeat response, replicas send WAL index (like Raft). By the end of the wait of the previous step this should be up-to-date and not changing.
- Pick the replica with the highest WAL index to be the new leader, if at least half of the replicas have the same index.
- CPO sends leader assignment message to choosen replica and waits for response.
- New leader sets its read watermark to the current time from TSO and enables serving requests.
- CPO updates partition map with new assignment version for failed partition
- In parallel, CPO initiates operations for replica failure in order to setup a new replica.

If at least half of replicas do not match the highest index, then a replica with the next highest index is chosen. In that case the operations that created the higher WAL index would not 
have been acked to the client.

Total downtime is 2xHLT plus the time it takes a client to retry and update from the CPO.

Some of the last operations in the WAL index may not have been acked to the client. This is the same situation as the current design with persistence, where some operations may be in an indeterminate state.

Ongoing transactions with new writes will be aborted due to the read watermark. Reads and transaction end requests from ongoing transactions can be served.

## Replica failure

Leader should tolerate replica failure if at least half are functional.

- Replica failure is detected by the leader with failed replication request
- Leader sends request to CPO for new replica. New replica gets hint on where in SCM to place new records.
- Leader sends new requests to new replica as normal.
- New replica back-fills old data from other replica.
- Indexer management on new replica?
- New replica cannot be considered online until back-fill is complete. For example, if the leader receives a replication ack from the new replica but a failure from the old replica, it cannot proceed.

## Majority replica failure

Leader can only serve read requests until at least half of replicas are available.

## Leader + half of replicas failure

Need to recover from SCM either leader or majority of replicas.

## Partition split

1. Determine split point
2. Add 3 replicas
3. For the 3 new replicas, start back-fill as in the replica failure case but only for one side of the split point.
4. Leader replicates ALL requests to original 3 replicas, and requests for the new split partition on the 3 new replicas.
5. When back-fill is complete, notify CPO.
6. CPO sends leader assignment of new partition, waits for response, and updates partition map.
7. CPO responds to leader of original partiton and original replicas.
8. Original leader and replicas drop data that is no longer owned.

## Partition merge

## Partition move
