
# 1. Overview
For a typical K2-3SI distributed transaction, there are Transaction Record(TR) that is managed at TRH (designated Transaction Record holder partition) and MTR (MiniTransactionRecord) that is managed at participant partitions. Both TR and MTR are managed in memory and persisted in WAL. 

TR and MTR has less persisted states and simpler transition map, while their in memory states and transition map are much more complex. 

This document will describe these states and transition in following order:
1) Persisted Transaction Record (TR) states and transition map
2) In Memory Transaction Record (TR) states and transition map
3) Persisted MTR states and transition map
4) In Memory MTR states and transition map

<br/><br/>

# 2. Persisted Transaction Record (TR) states and transition map
There are only 5 Transaction states (of Transaction) that could be persisted in WAL:
1) T1 (InProgress)        - transaction is created
2) T2 (Committed)         - transaction is commited
3) T3 (Committed&Deleted) - transaction, after commited, complete finalization and is deleted from memory.
4) T4 (Aborted)           - transaction is aborted
5) T5 (Aborted&Deleted)   - transaction, after aborted, complete finalization and is deleted from memory.

The persisted state transition (completed) is following:
Two typical path:
<br/>
(a) T1 -> T2 -> T3 : transaction is started, commited and deleted(after sucessful finalization)
<br/>
(b) T1 -> T4 -> T5 : transaction is started, aborted and deleted (after sucessful finalization)

One rare but possible path:
<br/>
(c) T1 -> T2 -> T4 -> T5 : transaction is started, tried to commit but couldn't sucessfully got comit record persistance confirmed after max amount of retry, then start to abort the transaction. 

All position in above three path of state transition could happen to a transaction in WAL. During replay, depends on which state record is the last one, we will push the transaction to next sate according to replay rules. For example, in simplified discription, if replay process see last one is T1, it will follow (b) during replay. If see T2, it will try to follow (a), If see T4, it will follow (b). T3 and T5 means fully completed transaction and nothing need to be done future during the replay.  

Also notice that T3 and T5 are finalized state of transaction, which only exists in WAL, but never in memory. 

For a transaction, TR (and MTR) are persisted normally togeter with data (SKV record Write Intend, or SKV record status change record) for persistence optimization, but to simplify the document, we ignored the data (SKV record) persistence here. One fact we should point out about persistence is that when applicable, TR/SKV and MTR/SKV are persisted combined in one networked request to persistence layer and TR or MTR is before SKV record. So for any persisted state changes(of TR/MRT and SKV data), in WAL, new state records of TR/MTR always appears before those of SKV data records. This important fact can be used for correct execution (including state transition validation) as well as debugging validation.

<br/><br/>

# 3. In memory Transaction Record(TR) states and transition map at TRH(Transaction Record Holder)

## 3. 1 Transaction Record States and transition - basic cases
<br/>

### Table 3-1-1 - Transaction Record States <I>Typical transaction life cycle (committed)</I>

| Stages                    | Triggering Action                    | In Memory State(s)                        | WAL Last Persisted State | Triggered (Async) Action                                |
|---------------------------|--------------------------------------|-------------------------------------------|--------------------------|---------------------------------------------------------|
| 0 Starting Transaction    | Client Starts a TXN with first write | T0(Created <i>(NP)</i>) -> T0.9(InProgressPIP)| N/A                  | To persist TR in state T1() state into WAL              |
| 1. Transaction becomes InProgress| T1(InProgress) TR persisted          | T0.9(InProgressPIP) -> T1(InProgress)     | N/A                      | None                                                    |
| 1.9 Commmiting transaction| Commmit transaction requested        | T1(InProgress) -> T1.9(CommitPIP)         | T1(InProgress)           | To persist TR in state T2(Committed) state into WAL     |
| 2  Committed & finalizing | T2(Committed) TR persisted           | T1.9(CommitPIP) -> T2 (Committed)         | T2(Committed)            | Send finalizing requestes to participant partitions     |
| 2.9 Finalized & Deleting  | All finalizing requests succeed      | T2 (Committed)  ->T2.9(CommitDeletePIP)   | T2(Committed)            | To persist TR in state T3(Committed&Deleted) state into WAL|
| 3  (Committed)Deleted     | T3(Committed&Deleted TR persisted    | T2.9(CommitDeletePIP) -> {DELETED}        | T3(Committed&Deleted)    | None                                                    |
<br/>

Note: PIP stands for Persistence in Progress. NP stands for Non-Persistentable.


<br/><br/>

### Table 3-1-2 - Transaction Record States <I>Typical transaction life cycle (aborted)</I>

| Stages                    | Triggering Action                    | In Memory State(s)                        | WAL Last Persisted State | Triggered (Async) Action                                |
|---------------------------|--------------------------------------|-------------------------------------------|--------------------------|---------------------------------------------------------|
| 0 Starting Transaction    | Client Starts a TXN with first write | T0(Created <i>(NP)</i>) -> T0.9(InProgressPIP)| N/A                  | To persist TR in state T1() state into WAL              |
| 1. Transaction In Progress|T1.0(InProgress) TR persisted         | T0.9(InProgressPIP) -> T1(InProgress)     | N/A                      | None                                                    |
| 3.9 aborting transaction  |abort transaction requested           | T1(InProgress) -> T3.9(AbortPIP)          | T1(InProgress)           | To persist TR in state T4(Aborted) state into WAL       |
| 4. Aborted & finalizing   |T4(Aborted) TR persisted              | T3.9(AbortPIP) -> T4 (Aborted)            | T4(Aborted)              | Send finalizing requestes to participant partitions     |
| 4.9 Finalized & Deleting  |All finalizing requests succeed       | T4 (Aborted)  ->T4.9(AbortDeletePIP)      | T4(Aborted)              | To persist TR in state T5(Aborted&Deleted) state into WAL |
| 5 (Aborted)Deleted        |T5(Aborted&Deleted) TR persisted      | T4.9(AbortDeletePIP) -> {DELETED}         | T5(Aborted&Deleted)      | None                                                    |
<br/>

Note, Normal Committed state record T2 and Aborted state T2 are persisted together with transaction write-set, which are the Keys it wrote and need to send finalization requests. 

<br/><br/>

### Table 3-1-3 - Transaction Record State <I>Obsolete transaction life cycle (aborted)</I>
An ongoing transaction could become obsolete due to possible two reasons: 1) the client didn't sent transaction heartbeat promptly (likely client crashed or network disconnected) 2) a transaction that is long running, runs out of retention window. Obsolete transaction is detected at TRH with timer based check operation and will be triggered into abortion. Once a transaction starts commit( get into state T1.9 CommitPIP), or starts abort (get into state 3.9 AbortPIP), it will not become obsolete any more regardless how long it need to finish its state transitions. 
<br/>
It is common that a transaction becomes obosolete when it is at state of T1 (InProgress), but it is also possible, though very unlikely, it become obsolete when it is at state of T0.9 InProgressPIP., e.g. the client crashed/lost heartbeat when TRH is still trying to persist TR of state T1(InProgress). 

| Stages                    | Triggering Action                    | In Memory State(s)                        | WAL Last Persisted State | Triggered (Async) Action                                |
|---------------------------|--------------------------------------|-------------------------------------------|--------------------------|---------------------------------------------------------|
| T1 (InProgress)           | Become Obsolete / request abort      |  T1(InProgress) -> T3.9(AbortPIP)         | T1(InProgress)           | To persist TR in state T4(Aborted) state into WAL       |
| T0.9 (InProgressPIP)      | Become Obsolete / request abort      | T0.9 -> T3.9(AbortPIP)                    | None Or T1               | To persist TR in state T4(Aborted) state into WAL       |
| T3.9 (AbortPIP)           | T1(InProgress) TR persisted          | NoChange T3.9                             | T1 or T4                 | None                                                    |
<br/>

The common case of becoming obsolete, where state transits from T1 -> T3.9, is actually already covered in above Table 2. The difference is that when the transaction become obsolete, it aborts without knowning all participant thus may not sending finalizing requests to all participant partitiona. The unlikely case, T0.9 -> T3.9, is new transition route. In implementation, we can also add obsolete flag/bit in such abort record indicating it is obsoletion triggered for debugging/explicitness. 

<br/><br/>

## 3. 2 Push operation triggered Transaction Record States and transition

### Table 3-2-1 - Push Operation (being pushed) at Typical transaction life cycle triggered TR state change </I>

| In Memory State Push requested/Current | Triggering Action       | After Push In memory State   | WAL Last Persisted State     | Triggered (Async) Action                             |
|----------------------------------------|-------------------------|------------------------------|------------------------------|------------------------------------------------------|
| T0.9 (InProgressPIP)                   | Push & Win              | NoChange T0.9 (InProgressPIP)| NONE or T1(InProgress)       | None                                                 |
| T0.9 (InProgressPIP)                   | Push & Lose             | T0.9 -> T3.9 (AbortPIP)      | NONE or T1(InProgress)       | To persist TR in state T4(Aborted) state into WAL    |
| T0.9 (InProgressPIP) & Push lost(T3.9) | T1(InProgress) Persisted| NoChange T3.9 (AbortPIP)     | T1(InProgress) or T4(Aborted)| None                                                 |
| T0.9 (InProgressPIP) & Push lost(T3.9) | T4(Aborted) Persisted   | T3.9 -> T4(Aborted)          | T4(Aborted)                  | Send finalizing requestes to participant partitions  |
| T1 (InProgress)                        | Push & Win              | NoChange T1 (InProgress)     | T1(InProgress)               | None                                                 |
| T1 (InProgress)                        | Push & Lose             | T1 -> T3.9 (AbortPIP)        | T1(InProgress)               | To persist TR in state T4(Aborted) state into WAL    |
| T1.9 (CommitPIP)                       | Push & Win              | NoChange T1.9 (CommitPIP)    | T1 or T2                     | None                                                 |  
| T2 (Committed)                         | Push & Win              | NoChange T2 (Committed)      | T2                           | None                                                 |
| T2.9(CommitDeletePIP)                  | Push & Win              | NoChange T2.9                | T2 or T3                     | None                                                 |
| T3.9 (AbortPIP)                        | Push & Lose             | NoChange T3.9                | None, T1, T2, or T4          | None                                                 |
| T4 (Aborted)                           | Push & Lose             | NoChange T4                  | T4                           | None                                                 |
| T4.9 (AbortDeletePIP)                  | Push & Lose             | NoChange T4.9                | T4 or T5                     | None                                                 |
<br/>

<br/>

### Table 3-2-2 - ForcedAbort State T9.9 - Push on a non-exist transation
When a push operation gets to a TRH, the Transaction (record) may not exist in memory. The transaction may or may not exists in WAL either and even it does, there is no efficient way to find the transaction record from the WAL. Such situation could happen in a few uncommon cases, e.g. when client crashed (after started a transaction), the TRH after loose transaction heartbeat have to abort the transaction without completed finalization as TRH doesn't know other transaction participant partitions (to send finalizing request to), and if TRH reload, the transaction will not be in memory any more (as it is fully aborted and completed). If in this case, a push request comes from a partipant, we need to process it in following case. For the trasaction, we will recreate a Transaction Record in memory but in ForcedAbort state T9.9, and keep it in memory (up to retention window time). And for push operation, let the challenger win and incumbent is aborted already. Note, ForceAbort T9.9 is an in memory state, never persisted in WAL. (BTW, 9.9 is a strange number and means it is a strange state)

<br/>

| In Memory State Push requested/Current | Triggering Action       | After Push In memory State    | WAL Last Persisted State     | Triggered (Async) Action                             |
|----------------------------------------|-------------------------|-------------------------------|------------------------------|------------------------------------------------------|
| {DELTED}                               | Push & Lose             | {DELETED}-> T9.9 (ForcedAbort)| NONE or T5(AbortDeleted)     | None                                                 |
| T9.9 (ForceAbort)                      | Push & Lose             | NoChange T9.9                 | NONE or T5                   | None                                                 |
<br/><br/>


## 3. 3 Error handling in Transaction Record States and transition

### Table 3-3-1 - Persistence Error

There are two situations that persistence error we need to handle. Persistence failures are retryable, but should be within a configuable max allowance amount of times (e.g. 10 times) for system wide catastrophic error. The first situation is the retry times exceeds the max allowance. If retry succeeded, the persistence error can't be fully masked as the previous persistence requests may returned out of order. The second situation is to handle these out of order retried result. 

<br/>

For first situation, when retry exceeds the max allowance, following Table 5.1 describe the state transition. Note, we only consider configuable max allowance of retry when we try to persist T1(InProgress) T2 (Committed), in other 3 type of persisted record case, we ignore the failure and retry unlimited (with backoff).
| Stages                    | Triggering Action                    | In Memory State(s)                        | WAL Last Persisted State  | Triggered (Async) Action                               |
|---------------------------|--------------------------------------|-------------------------------------------|---------------------------|--------------------------------------------------------|
| T0.9 (InProgressPIP)      | Persists(T1) error max out           |  T0.9(InProgress) -> T3.9(AbortPIP)       | None or T1(InProgress)    | To persist TR state T4(Aborted) into WAL               |
| T1.9 (CommitPIP)          | Persists(T2) error max out           |  T1.9 -> T3.9(AbortPIP)                   | T1 or T2                  | To Perisit TR state T4(Aborted) into WAL               |
| T2.9 (CommitDeletePIP)    | Persists(T3) error ignoring max out  |  No change T2.9                           | T2 or T3                  | None (continue exponetial backoff and retry)           |
| T3.9 (AbortPIP)           | Persists(T4) error ignoring max out  |  No change T3.9                           | T1 or T4 (rarely T2)      | None (continue exponetial backoff and retry)           |
| T4.9 (AbortDeletePIP)     | Persists(T5) error ignoring max out  |  No change T4.9                           | T4(Aborted) or T5         | None (continue exponetial backoff and retry)           |
<br/>

Note, when Persistes (T2) error at T1.9 state (CommitPIP) maxed out, we transit to T 3.9, we do not notify client commit failed till T4 is persisted sucessful as T2 may be persisted already. Also once transit happens, even if the T2 pesistence success response message comes back (in case of time out), we ignore that as well. 

<br/>
<br/>

### Finalizing Error 
Finalization process is a requests fan out process from TRH to mulitple participants, potentially likehood of one or a few participants may not response successfully in time. Theoretically, such error are mostly timeout error and/or retryable and once a transaction is in state 2 or 4, where it start to sending out finalization requests, its state will only move to 2.9 or 4.9 respectively when all requests are responded with success asynchronously later. So Finalizing error will be ignored. 

But, in practice, when only one or just a minor few participants failed to finalizing, an optimization can be done is to record/persist (again) only these failed write-set subset keys(or key ranges), so, in case the TRH is reloaded, only these failed few partition(s) will be called to retry finalization.

## 3. 4 Optimization cases in K2-3SI and its impact on Transaction Record States and transitiion - Distributed multiple steps transaction
### Optimized case (where commit can be requested before all data is persisted)
For a distributed transaction contains mulitple steps of write (and read) operations, one optimization is for client to move to next step when previous one is acknowledged by participant partitions (the write operation is kept in memory and in parallel asyn persistence of the change record is happening). Further more, the participant can directly notify the TRH on the complete of persistence later, where the TRH was notified with each intended write by client at the same time when it was issued to the participant. Then whole transaction could have higher parallelism (of persistence and operations) and less network message roundtrip. In such optimization case, it is possible that when client requests commit, not all write-set was successfully persisted.   

For this case, a in memory TR state T1.8(CommitWait - ForReady) is introduced, which means the TR got request for commit, but not receiving all the sucessfull persistnce notice yet.
<br/>

Table 6.1 - common case commit requested at state 1 (InProgress) but not all persistence operations are done/notified to TRH

| Stages                        | Triggering Action                    | In Memory State(s)                        | WAL Persisted State  | Triggered (Async) Action                                |
|-------------------------------|--------------------------------------|-------------------------------------------|----------------------|---------------------------------------------------------|
| T1(InProgress)&CommitNotReady | Commmit transaction requested        | T1(InProgress) -> T1.8(CommitWaitForReady)| T1                   | None                                                    |
| 1.8 Wait for ready            | Last persist sucess notification     | T1.8(CommitWait) ->T1.9(CommitPIP)        | T1                   | To persist TR in state T2(Committed) state into WAL     |
<br/>

<br/>

Table 6.2 - Special (optimized/quick) commit requested at state 0.9 (InProgressPIP)

| Stages                        | Triggering Action                    | In Memory State(s)                        | WAL Persisted State  | Triggered (Async) Action                                |
|-------------------------------|--------------------------------------|-------------------------------------------|----------------------|---------------------------------------------------------|
| T0.9 Starting transaction     | Commmit transaction requested        | T0.9(InProgressPIP) -> T1.8(CommitWait)   |NONE or T1(InProgress)| None                                               |
| T1.8 Wait for ready           | Local T1(InProgress)TR persisted&not last persistence | NoChange T1.8(CommitWait)| T1                   | None                                                    |
| T1.8 Wait for ready           | last persistence notification local or remote |T1.8(CommitWait) ->T1.9(CommitPIP)| T1                   | To persist TR in state T2(Committed) state into WAL     |
<br/>

## 3. 5 Optimization cases in K2-3SI and its impact on Transaction Record States and transitiion - Non-Distributed transaction

For non-distribued transaction (where there is only one partition is involved), the final write operation (the last operation for a multiple step transaction or only write operation for a single step transaction) will be combined with commit request (in happen commit case). 

<br/>


# 4. Persisted MTR states and transition map
There are only 3 MTR states that could be persisted in WAL:
1) M1 (InProgress)  - transaction is created, MTR created in memory
2) M2 (Committed)   - transaction is commited, which means also finalized at this participant, and MTR deleted. 
3) M3 (Aborted)     - transaction is aborted, which means also finalized at this participant, and MTR deleted. 

The persisted MTR state transition (completed) contains two path following:
<br/>
(a) M1 -> M2 : transaction/MTR is started, commited and deleted(after sucessful finalization)
<br/>
(b) M1 -> M3 : transaction/MTR is started, aborted and deleted (after sucessful finalization)

During the partition reload and replay the WAL, if a MTR has reached M2 or M3 state, it is completed in this participant and nothing need to be done. If for a transaction in WAL, M1 is met without M2 or M3, at the end of replay, the participant will trigger a query to TRH about the transaction state. Based on current transaction state at TRH, the MTR and related SKV changes/WriteIntent will be handled differently.
1) If the transaction is ongoing, (i.e. in state of T1-InProgress, or T0.9-InProgressPIP, or T1.9), the MTR and related changed SKV will be left alone. 
2) If the transactin is in committed (i.e. in state of T2), the MTR and related write-set can move to state of committed&Deleted M2. Note, at TRH, state T2.9 is invalid in memory state for the transaction means this is a fatal error/bug, as this means the TRH already received successful Commit&finalization from this participants, but this participants sends out such notice without sucessful persistence M2.
3) If the transaction is in abort (i.e. in state of T3.9, T4, T9.9), the MTR and related write-set can move to state of aborted&Deleted M2. Note, at TRH, state T4.9 is invalid in memory state for the transaction means this is a fatal error/bug, as this means the TRH already received successful Abort&finalization from this participants, but this participants sends out such notice without sucessful persistence M3.
4) If the transaction is no in memory at TRH, A ForceAbort T9.9 will be generated (just like being pushed) at TRH, the  the MTR and related write-set can move to state of aborted&Deleted M2. Note, the TRH will not send out any finalization request in ForceAbort situation.  (Danger, if the system has bug here, it will cause data loss!!!).
<br/>

<br/>

# 5. In memory MTR states and transition map
There are 3 in memory MTR states:
1) M1 (InProgress) - in the process of transaction read/write operations
2) M1.9 (CommitPIP) - Received Commit request, processing it, e.g. persisting MTR and related SKV write-set (WriteIntent) state change to commited.
3) M2.9 (AbortPIP) - Received abort request

MTR is created in memory with first write operation of a transaction requested on the participant partition, and persisted first time with InProgress state together with first write operation SKV data/WriteIntent (in front of SKV data record in position) in WAL. Upon finalization requests from TRH arrives (for both commit and abort case), its in memory state changes to M1.9 or M2.9, and trigginger persistence of M2 or M3 record (together of SKV data/Write Intent state change to commit/abort) into WAL.

In participant partition, there is a container for all in memory MTR (on going transactions). Each in memory MTR, besides transaction ID info (where to find TRH etc), contains a list of WriteOperations, each WriteOperation contains a list of keys of SKV that is written in this WriteOperation. Each WriteOperation, same with the Write Intend/Keys it contains, has persistence state as PIP or Persisted. This also makes MTR in memory state doesn't need a InProgressPIP state, unlike TR which has such in memory state. 

## 5.1 MTR States and transition - basic cases

### Table 5-1 - MTR <I>Typical transaction life cycle (committed)</I>
| Stages                    | Triggering Action                    | In Memory State(s)                        | WAL Last Persisted State | Triggered (Async) Action                                |
|---------------------------|--------------------------------------|-------------------------------------------|--------------------------|---------------------------------------------------------|
| First write               | First write with MTR arrives         | {emptry} -> M1(InProgress)                | N/A                      | To persist MTR in state M1(InProgress) into WAL         |
| Consequent write          | Consequent write with same MTR arrives| Unchanged M1                             | N/A or M1                | None on MTR, but persisting new WriteOperation          |
| (WriteOperation) Persisted| Wrrite Persistence complete          | Unchanged M1                              | M1 (InProgress)          | None on MTR, But update WriteOperation and WI in memory state from PIP to persisted|
| Commit finalization       | CommitFinalization reqeust arrives   | M1 (InProgress) -> M1.9 (CommitPIP)       | M1


Note, when MTR in memory state is created upon write request, first WriteOperation with PIP is created under MTR and all its containing SKV records/WriteIntends are added in indexer in memoery with PIP state and are being persisted together (positionally behind) with MTR into WAL. 