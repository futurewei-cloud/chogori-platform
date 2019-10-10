[-UP-](./README.md)

[TOC]

# Goals
- benchmark based on YCSB+T (later TPC-C)
- Support serializable isolation level. Ideally, support DB-level isolation levels of SnapshotIsolation or Serializable
- Support global transactions
#### DC-local datasets
- Latency: 20us avg
- Throughput: 100K/sec per CPU core
#### mixed DC-local/global datasets
- Latency: ~ order of cross-dc ping(could be 100s ms)
- Throughput: ??

# Targeted assumptions
These are not limits of the system, but we design the system to operate under these conditions:
- Availability of high-bandwidth (>40gbit) RDMA/RoCEv2 compatible network for in-datacenter communication
- Optimize performance for DC-local operations but remain correct in mixed usage
- Single-threaded, concurrent architecture. This allows us not to worry about multi-threaded side-effects during tight sequence of operations on any node.

# Definitions
### ACID
- Atomicity: multiple keys either all get updated or none (2PC with logging)
- Consistency: (data consistency, e.g. conform to schema, or uniqueness restriction)
- Isolation: e.g. MVCC allows snapshot isolation
- Durability: (handle failures)

### Isolation levels:
- ReadUncommitted: a read may see value currently part of an open transaction. The value may actually be rolled back
- ReadCommitted: a read will only see values committed by other transactions. It may be possible to see different values in your own txn if another txn commits.
- ReadRepeatable: all reads during a txn will return the same value even if there are other commits.
- SnapshotIsolation: Snapshot isolation is a mechanism for providing ReadRepeatable isolation level. It provides an immutable snapshot-in-time view of the entire database. The database guarantees that reads against this snapshot will always return the same values. SI by itself does not provide any form of serialization among transactions. As we will discuss further in this document, we are providing a solution to achieve serializable SI.
- Serializable: all transactions are ordered and will never be observed in different order by different users. The order however may be observed to be different compared to the actual order in which the txns executed, provided the end result(consistency!) is the same. E.g. `-$50` followed by `+$100`
- Externally Serializable: all txns are observed in the order they were executed by all observers

### Consistency levels
- Internally Causal: Transactions are ordered in the system so that if e->f, then e is before f. if also e->g, there are no guarantees on f and g ordering, not even the order in which multiple observers see f and g.
- Convergent Causal: f and g are placed in some order and everyone observes this order. (essentially serializable causal)
- 

### Time
##### UniversalTime
This is the time that is universally accepted to be the correct time. It is a single number with sufficient resolution(e.g. pico-seconds) that represents time since epoch. In our desired deployment, the source for UT is an atomic clock hardware present near the timestamp generator.

##### Timestamp
In this document, a Timestamp is a window of time with some bounded uncertainty from UniversalTime. This is equivalent to TrueTime in [Spanner's TrueTime](https://cloud.google.com/spanner/docs/true-time-external-consistency). For optimization purposes, a Timestamp is a tuple of (int64 start, int64 end, int64 tsoid), which conveys the start/end time of the interval, as well as the identity of the TimeStampOracle which was used to estimate the error from UT.
Timestamps are compared as in Spanner by int compare(t1, t2) method which returns
- -1 if t1 is guaranteed to be before t2
- 1 if t2 is guaranteed to be before t1
- 0 if the comparison is uncertain and we cannot determine which timestamp came first

When we mention a timestamp in this document, we mean this tuple (int64 start, int64 end, int64 tsoid).

##### TSO (TimeStampOracle)
TSO is a separate service whose job is to provide a global clock. The design and scalability of this component is addressed in a [separate document](./TSO.md). For the purposes of this document it is sufficient to understand that the TSO service:
- syncs itself to UniversalTime and can emit Timestamps as described above
- guarantees strictly increasing TimeEnd in timestamps, which is important to efficiently sequence TSO-local events (that is events which use the same TSO to produce timestamps)

In our architecture we support multiple configurations, but our reference architecture has one TSO per data center, where DC-local operations always use their DC-local TSO.

Given these options, the TSO client can then choose to generate timestamps on the same machine for asynchronous timestamp generation (effectively making the machine its own TSO), or the client can choose to synchronously obtain timestamps from the DC-local TSO. The consideration here is that timestamps from the same TSO are compared without uncertainty which allows conflicting transactions to be ordered simply based on sequence number (the timestamp.end field). On the other hand, conflicting transactions which use different TSOs have to be sequenced while taking time uncertainty into account.

Our design is flexible to allow different architectures so that for example if a database's traffic is very contentious, it can use a single TSO to achieve better throughput at the expense of constant latency increase(+roundrip time to TSO).

Our reference deployment architecture uses DC-local TSO with syncronous timestamp generation over fast RDMA network.

### Operation
Operations are executed in the context of a transaction (using the TxnHandle returned by begin()). The operations are generally either constant(e.g. read-only) or mutating(e.g. writes). The operations are not limited to simple KV read/write and can be extended to allow any advanced single-node operations such as atomic ops(e.g. CAS) or even stored procedures. For simplified explanations, we use constant<->read and mutating<->write interchangeably in this document.

### Minimum Transaction Record (MTR)
This is a small tuple of metadata which must be specified with every operation issued by the client. This tuple conveys `(TxnID, Timestamp, Priority)`, and we use this to perform snapshot reads, associate writes with transactions, and conflict resolution.

### Transaction Record Holder (TRH)
Any transaction which performs a write requires a central coordinator for the transaction. The coordinator is just one of the write participants in the transaction. We call this coordinator the Transaction Record Holder (TRH). We'll explain how the TRH is used in each individual operation further in the document. The Client decides who the TRH will be for a given transaction, and sends a message to it to let it know that it is responsible for the coordination tasks. For performance reasons, the client lazily waits for its first write, and piggy-backs the TRH-assignment message onto this first write. Read-only transactions do not create a transaction record as it is not needed

### Write Intent (WI)
In order to detect potential conflicts, we use Write Intents(WI). WIs are the same as any other record in the system. They correspond to a version for some key, and are our way of keeping track of uncommitted writes. In other words, we call a standard record which has the "uncommitted" flag to ON a "Write Intent".
When the post-commit(i.e. finalize) phase of a mutating transaction goes through, the flag is cleared (set to false) making the record a committed value.
In the WI we also rembember the MTR and TRH for the transaction which created the intent in the node. So given a WI, we can find out the MTR(transaction ID, timestamp, priority), as well as the TRH(the participant which is responsible for maintaining the TXN state)

# Outline of approach
We've chosen a modified Serializable Snapshot Isolation approach. Roughly, we use MVCC to achieve snapshot isolation, and we enhance the server-side handling and book-keeping to make it serializable, as described in the paper by [Serializable SI - CockroachDB](https://www.cockroachlabs.com/blog/serializable-lockless-distributed-isolation-cockroachdb/) and  [Serializable Isolation for Snapshot Databases](./SerializableSnapshotIsolation-fekete-sigmod2008.pdf). Here is the outline:

## Reference Architecture
The reference architecture is designed so that we can take advantage of fast RDMA communication in datacenters, making a synchronous DC-local TSO a viable option. This allows TSO-local transactions to execute sequentially, without risk of serialization violations due to time uncertainty. Cross-datacenter (or global) operations are not necessarily handled with uncertainty either. Only when we detect that data contention occurs among participants who originate with different TSOs, then we deternmine the outcome of the contention based on uncertainty window.

![Architecture](./TxnArchitecture.png)

## Starting a transaction
![Begin](./TxnBegin.png)

The application initiates a transaction by calling the begin() client library API. In this call:
1. Obtain a timestamp from the TSO service.
    - this is a crucial step since proper timestamp generation is what guarantees serializability of the transactions in the system.
    - Timestamps are tuples (start, end, tso_id), which express an uncertainty window. That is, the TSO produces timestamps that cover the potential error of time between the TSO and UniversalTime.
    - There are multiple options for deployment here which depend on latency of access and throughput at the TSO. Our TSO supports ~20M ops/sec with latency of 10usec. Thus we expect DC-local transactions to make use of a central TSO as this can achieve very high throughput over conflicting datasets. However, in cases where latency is higher (e.g. DC-remote transactions), the timestamp can be produced by any TSO which can determine its error from UniversalTime. The downside is that the window for conflicts over the same data increases to cover the uncertainty window in the produced timestamp, which potentially can reduce transaction throughput and increase latency. See [TSO Design](./TSO.md) for further details.
    - This timestamp is used to stamp the entire transaction. That is, the commit time for all writes in this transaction will be recorded to occur at this timestamp, and all MVCC snapshot reads will be attempted at this timestamp.
1. Generate a transaction ID in the CL. This is generally a short ID (e.g. (IP + random)) used to distinguish transactions in the live system as well as at recovery time
1. Assign a priority to the transaction based on either priority class (LOW/MED/HIGH), or particular priority within the class. Priority classes map to particular priorities (e.g. Low=10, Med=20, High=30). When a transaction is started is usually picks a class. In cases when transactions are aborted due to conflicts, they get to inherit the higher priority and so when retried, they can specify a particular priority value. The priority is used server side to deterministicaly pick a winner in transaction conflict cases.

Further operations, including commit/abort have to be issued using the returned transaction handle. The Client Library keeps track of the MTR, TRH, and every write participant.

## Execution
- Each operation must specify an MTR(Minimum Transaction Record) tuple.
- A TransactionRecordHolder (TRH) is designated by the CL for mutating transactions. Read-only transactions do not create such transaction record as it is not needed. The TRH is is one of the write participants in the transaction. We pick the first such writer for most transactions, but it is possible to pick a more optimal TRH for certain workloads. The assignment is done lazily when the first write is encountered, and the assignment message is piggy-backed onto that first write.
- All write operations specify a TRH in addition to the MTR.
- each participant maintains an index of `MTR->WI` so that it can cleanup write intents when the TRH comes to finalize a transaction (i.e. after the application commits/aborts). There is also an LRU list on this index so that the participant can discover potentially abandoned or long-running WIs and communicate with the TRH to finalize them.

### Reads
![Read](./TxnRead.png)
The read operations are annotated with an MTR. The timestamp in this MTR is used as the snapshot version(SV) for MVCC snapshot read purposes. The reads are standard MVCC reads i.e. the returned value is the latest value such that `result.ts <= request.MTR.TimeStamp`.

#### Read Cache
In order to achieve SerializableSI, we maintain a read key cache. This cache is maintained in memory at each node/partition. When we receive a read request, we call the cache PUT(K, SV) api to remember that key K was read at snapshot version SV.
- The implementation of the read key cache is an interval tree. We require an interval tree to also remember key range reads.
- The cache is consulted at write time to determine if a write should be aborted. The reason we need to do this is that if we try to write an item with timestamp <= lastSVTimeTheKeyWasRead, then we are breaking a promise to whoever read the item - they saw some item version when they read at their snapshot time, and now we're trying to insert a newer version into their snapshot. This write should therefore be aborted.
- Entries are removed from the cache in an LRU fashion. We maintain a minSVTimestamp watermark for the cache, which tells us how old is the oldest entry in the cache. Any write before this timestamp (for any key) is aborted as we assume there may have been a read for it.
- Note that even though we call this data structure a cache, it is critical to keep in mind that it is a sliding window. We cannot afford to miss any reads that happen in this window - all reads MUST be recorded in the window. We can only afford to shrink this window as needed for performance reasons.

#### Read conflict potential
In the path of a read request, the only situation which may cause a conflict is when a read encounters a write intent. Reads do not conflict with each other. The situation happens when the latest version in the database before read.timestamp is a WI from a different transaction. If the intent is from the same transaction, we can return it as the current value. As we don't know yet if this write intent will be committed, we cannot return any value to the client - if we return the WI as the value, it is possible that the WI may be aborted, and so we would've lied to the client and potential future reads will see a previous version. On the other hand if we return the version before the WI, we break our promise to the client that their snapshot is immutable since the commit of the WI is equivalent to inserting a new record version into this snapshot.

To resolve this conflict situation, we perform a PUSH operation:

#### PUSH Operation (conflict resolution)
To resolve the conflict of `R`ead transaction encountering a `W`rite intent (`R->W`), we perform a PUSH operation. We use the same mechanism during `W->W` conflicts, but we'll expand on these in the write operation section further in the document

![Read Conflict PUSH](./TxnReadConflict.png)

- Note that if we try to perform a PUSH it is possible to encounter no record at the TRH (e.g. due to network message delay). In that situation, we assume the transaction in question has been aborted, and we generate a WAL entry with status=ABORTED for the transaction thus effectively aborting the transaction which failed to create its TRH in time

At the core, the resolution algorithm is as follows:

```python
# at participant which receives the read
def Read(key, MTR):
    readCache.registerRead(key, MTR.Timestamp)

    version = cache.getVersion(key, MTR.Timestamp)
    if version.isCommitted:
        return version

    response = version.TRH.Push(MTR)
    if response.isWriteTxnAborted: # case1
        cache.removeVersion(key, version) # remove write intent
        return Read(key, MTR) # read again
    if response.isWriteCommitted: # case2
        version.commit()
        cache.updateVersion(key, version)
        return version
    return abort # case3

def lookupTxnStatus(mtrRecord):
    # NB: A lookup for a txn we do not have a status for is resolved based on targetMTR.timestamp
    # if timestamp is within the txn liveliness window, it results in a WAL write for a Pending TR for this transaction.
    # else it results in a WAL write for an Abort TR for this transaction
    # at TRH participant, resolving a PUSH (could be either a R-W push or W-W push)
    if mtrRecord in txnsMap:
        return txnsMap[mtrRecord]
    else:
        if abs(time.NOW - mtrRecord.ts) < livelinessThreshold:
            txnRecord = newPendingRecord(mtrRecord)
        else:
            txnRecord = newAbortedRecord(mtrRecord)

        txnsMap.insert(txnRecord)
        return txnRecord

def Push(targetMTR, candidateMTR):
    txnStatus = lookupTxnStatus(targetMTR)

    if txnStatus.isCommitted: # case 1 (target TXN already committed)
        return TxnCommitMessage(targetMTR)
    elif txnStatus.isAborted: # case 2 (target TXN already aborted)
        return TxnAbortMessage(targetMTR)

    # TargetTXN is in progress. Must abort one of the transactions
    if targetMTR.Priority > candidateMTR.Priority: # case 3 (abort candidate TXN)
        return TxnAbortMessage(candidateMTR)
    elif targetMTR.Priority < candidateMTR.Priority: # same as in case 2
        abortTransaction(targetMTR)
        return TxnAbortMessage(targetMTR)

    # priorities match. Abort based on configurable, DB-level policy
    # example policy 1: abort the older TXN
    # example policy 2: abort lower TxnID to achieve deterministic but randomized abortion thus preventing starvation
    # in cases where there is no other distinguishing mechanism, the policy can pick the candidate(pusher) to abort
    loserMTR = TxnAbortDBConflictPolicy.pickLoser(targetMTR, candidateMTR)
    abortTransaction(loserMTR)
    return TxnAbortMessage(loserMTR)
```

### Writes
Writes are messages which want to mutate some data in the cluster. The messages are sent directly to the participant which owns the data. Note that we also lazily assign the TRH with the first write we see in a transaction (see [trh](#transaction-record-holder-(trh)) above). There is no need to communicate with any other participant during a write operation in the happy case.
![Write](./TxnWrite.png)

#### Participant execution logic
A write executed in a transaction is sent directly to the participant who owns the data. When the participant gets the write, it:
- checks to make sure it doesn't conflict with its read cache. That is, check if the key we're about to write was read such that `write.Timestamp <= read.SV`. This is a read conflict and is covered in the next section.
- checks to make sure it doesn't conflict with any ongoing writes (data store). Since our data store maintains multiple versions, we check to see if the latest version present in the data store for this key is a WriteIntent. If it is, we have a write conflict (covered in a further section below)
- inserts a WriteIntent entry into its data store.

#### Read conflict potential
If the incoming write for a given key discovers that the read cache contains an entry for this key such that `write.TS <= entry.TS`, we have detected a `W->R` conflict. The write is rejected and we send a message to the client to abort. This is the only choice here since we've already promised a snapshot view to some client as of time entry.TS and we cannot insert new versions into that history.

#### Write conflict potential
To determine if there is such a conflict, we look at the history of the key. If the latest version in the history is a committed value and `commit.TS >= write.TS` then we have no other choice but immediately abort the incoming write.

if the latest version in the history is a WI, then we have to perform a PUSH operation as we described in the `R->W` conflict resolution.

By construction then, there can only be one WI present in the version history and it has to be the latest version we have. It is not possible to commit while there is a WI in the record history and so we have to abort one of the transactions if they are both still in progress.

- NB: When selecting a victim transaction, any independent observer must deterministically arrive to the same conclusion in order to avoid starvation issues.

![W->W Conflict](./TxnWriteConflict.png)

The algorithm is identical to performing a `R->W` PUSH, where the existing WI corresponds to the `Write`, and the incoming write corresponds to the `Read`.

## Commit
The commit step is rather simple once we realize that if the client application has successfully performed all of its operations thus far, then it can just tell the TRH to finalize the transaction. There is a potential for discovering that the state of the transaction at the TRH is `Aborted`, in which case the application simply has to retry the transaction.

![Commit](./TxnCommit.png)

## Abort
Abort is performed identically to the commit - we send a message to the TRH, setting the state of the transaction to `Aborted`. The TRH then has to go and perform asynchronous cleanup of write intents (if any) at the transaction participants

## Transaction Heartbeat
There is a server-side timeout of 100ms per transaction, maintained at the TRH. The client is required to emit a heartbeat to the TRH to make sure the transaction is not marked as abandoned and thus aborted by the server automatically.
This heartbeat is also used by all of the participants in the transaction. They periodically check with the TRH to make sure the txn is still alive

## Transaction Finalization
This is an asynchronous process which is driven by the TRH to handle the transaction state in the entire system after the transaction is committed/aborted. This process involves communicating with all transaction write participants in order to commit/abort the WIs present in these participants.
The process is always driven by the client application since without a commit/abort message from the client, the TRH does not know what are all of the write participants - this information is sent to the TRH in the commit/abort message from the application.
For internally-driven abort (e.g. as a result of a PUSH), we simply mark the TRH state as aborted but we still do not kickoff the finalization process. We initiate the process when the client comes to commit/abort.
In the case when the client abandons a transaction, the heartbeat to the TRH will cease and the TRH will self-mark the transaction as aborted. Each participant independently will discover that it has WIs which are too old (>heartbeat_interval), and will contact the TRH to make sure the TXN is alive. They will discover that the transaction has been aborted due to the missing heartbeat and will cleanup their write intents. A good place to maintain this information at each participant is the `MTR->[WI]` cache as an LRU list.

## Recovery process
<mark>TODO</mark> describe handling of failures of TRH, Read participant, Write participant

## Integration with K2 Module Interface
<mark>TODO</mark> give details on implementation on top of K2 Module Interface

## GC process
<mark>TODO</mark> give details on GC for WALs

## Other ideas
- It may be helpful to allow appications to execute operations in batches so that we can group operations to the same node into single message
- Allow WI to be placed at any point in the history as long as they don't conflict with the read cache.
- Consider using separate WAL for intents. Potentially cheaper to GC since we can just maintain a watermarm and drop the tail past the watermark once WIs are finalized. May cause write amplification though
- provide atomic higher-level operations (sinfonia style):
    - swap
    - cas
    - atomic_read_many
    - acquire_lease
    - acquire_lease_many
    - update_if_lease_held
- We might achieve better throughput under standard benchmark if we consider allowing for a HOLD in cases of conflict resolution(PUSH operation). If we have a Candidate/Pusher which we think will succeed if we knew the outcome of an intent, we can hold onto the candidate operation for short period of time to allow for the intent to commit. For a better implementation, it maybe best to implement a solution which does a transparent hold - a hold that doesn't require special handling at the client (e.g. additional notification and heartbeating). THis could be achieved simply by re-queueing an incoming task once with a delay of potential 999 network round-trip latency (e.g. 10-20usecs).

# Detailed component design
## [TimeStamp Oracle](./TSO.md)
## [Transaction Client](./TXN_CLIENT.md)
## [Benchmark](./TXN_BENCHMARK.md)

# Benchmark
- [UW YCSB-T repo - requires account](https://syslab.cs.washington.edu/research/transtorm/)
- [UW YCSB-T paper](./YCSB+T.pdf)
