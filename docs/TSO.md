[-UP-](./TXN.md)

# Timestamp Oracle Design
Service for providing timestamps to K2 clusters for transactional operations.

## Data Model
There are couple of approaches here:
1. Monotonically increasing sequence number.
2. Composite timestamp of millisecond resolution wall time combined with a sequence number.

To allow for alternatives to the approaches above such as Marzullo time derivatives, the timestamps will be provided as tuples:

`(T, E)`  

Where T is the time and E the maximum timestamp error. In our cases above, E = 0.

## Architecture
The K2-TSO service will be built over the K2 transport. A membership group will be formed for availability.
The membership recipe can be implemented over Zookeeper or etcd store.
- Implemented as a service over the K2 transport.
- Multiple replicas for availability. 
- Zookeeper/etcd to provide group membership and leader election.

## Timestamp Vending
A new timestamp will be generated on every request based on a starting time Tstart up to Tend.
A cursor will keep track of the last generated time.
A background task will periodically extend Tend when the cursor exceeds:

```(Tend - Tstart) / 2```
- Generated on every request.
- A cursor keeps track of the last time.

## Persistence
Every time Tend is extended, it will first be persisted into Zookeeper/etcd. This is not going to be an issue since we do not expect the
same call volume as service requests. However, this is highly dependent on ```Tstart - Tend```. To adjust for increasing 
traffic, Tend can be further pushed into the future.

* Periodically persisted into Zookeeper/etcd.

## Failover
When the leader dies, another node will become the leader and resume with ```Tstart = Tpersisted```. Before serving traffic a new Tend 
will be generated and persisted.

* New leader resumes from Tpersisted.
* A new Tend will be persisted before serving requests.
