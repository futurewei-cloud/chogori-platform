### Overview

With the Schema-aware Key-Value (SKV) interface we want to support not only schemas (for efficient 
server-side query operations) but schema versioning so that users have the tools to perform schema 
migration.


See src/k2/dto/ControlPlaneOracle.h for how a schema is defined in code.


This proposal for schema versioning relies on a few key assumptions:
* Simply creating a new schema version does not change any data in the cluster and does not change the result for any read or query request. This implies that no on-the-fly schema upgrade occurs.
* Following from above, schema migration is a client-driven process.
* Storage servers have limited understanding of schema version semantics. More precisely, the server can use the name and type of fields to assess compatibility (more details below).
* Key field types, names, and ordering cannot change between schema versions. This means that the same partition will own a record and the record will be in the same sorted order on the storage servers between schema versions, which makes the system easier to reason about.
* The only restriction on changing value fields between versions is that a field name must not be duplicated within the same version.
* A user can pick any version number when creating a new schema version as long as it is unique for the schema name.


### Basic SKV read operation

A basic SKV read operation does not need to know the version of the schema of the record it is trying 
to read. Since key fields do not change between versions, the client can use any version of a schema 
to create a read request. The read response will comprise of a SKVRecord, which includes the version 
number of the record as stored on the server.


If the returned version is unknown to the client, it will refresh the schema cache on behalf of the 
user. If it is still unknown after a refresh an error will be returned to the user. If the schema 
version is known by the client, then the user can use that to properly deserialize the SKVRecord.


### Basic SKV write operation

The user explicitly picks a schema version for the write operation. After serialization, the client 
sends the SKVRecord to the storage server. If the server is not aware of the schema version used, the 
write is rejected with an error and it does not attempt to refresh its schema cache. This is because we 
do not want the server to communicate with the CPO (a less-scalable component) during a data-path 
operation.


The user can pick any valid schema version to write. In other words, it does not have to be the latest 
version. This is to support a typical client-driven schema migration process:
1. Old user code is running, reading and writing records using an old schema version.
2. A new schema version is created.
3. User code is deployed that can read both schema versions, but writes records with the new version.
4. Background operation to re-write records to new schema version.

Note that between steps 2 and 3 user code must be able to write records with the old schema version.


### Query operations

Query operations (e.g. a prefix scan with predicates and projection) are one of the ways the SKV helps 
support schema migration, namely in the client-driven background operation to re-write records to a 
new schema version.


Query predicates consist of a field name, an expected field type, a relational operator, and a literal 
of the field type. For example, {"AGE", Int, LESS\_THAN, 25} could represent a predicate that 
selects for records where the AGE field is less than 25. By expressing predicate fields by name rather 
than positionally we can support queries across schema versions more easily. Similarly, projection is 
also expressed by field name and expected type.


The result set of a query (logically a list of SKVRecords), can contain SKVRecords written with 
different schema versions. There is also a boolean flag applicable to the whole query, called here 
IncludeVersionMismatchs, which determines whether records should be included that are not compatible 
with the specified predicates. For example:


Schema version 1: {"LastName":String, "FirstName":String, "Age":Int, "Balance":Int}

Schema version 2: {"LastName":String, "FirstName":String, "Balance":Int}


Here we dropped the Age field between versions. Our stored records are:


{"Bob", "Jones", 30, 120} at version 1

{"John", "Doe", 0} at version 2


Now we have the query with predicates:

{"LastName", String, STARTS\_WITH, ""}, {"Age", Int, GREATER\_THAN, 18}, {"Balance", Int, GREATER\_THAN, 0}


With IncludeVersionMismatchs=true, then both records are returned even though John's balance is 0. With 
IncludeVersionMismatch=false, then only Bob's record is returned. If instead we changed Age to a string 
in version 2, then the result will be the same, because the server will be able to find the Age field 
by name in both records but the expected type will not match for John's record.


On the other hand if our query does not include Age, only the Balance and LastName predicates, then it 
will work as expected across records from both schema verions because the server can match the fields 
by name and type. In this case the IncludeVersionMismatch flag would not affect the result.


### Partial Updates

Partial updates are expressed as a write operation with fields masked out to be ignored. Like a normal 
write operation, the user must pick a specific schema version that will be reflected in the resulting 
SKVRecord on the server. In other words, if a client does a read request to the same record following 
a partial update, that is the schema version it will get back.


A partial update must be both efficient in the typical use-case where the previous MVCC record and the 
partial update use the same schema version, and it must be usable in the case where the partial update 
is used to perform a schema migration for the record. To this end, the fields of a partial update are 
expressed positionally over the wire in the DTO but since the schema version is known the server can 
refer to the schema to get the names of each field if needed.


In the typical case where the schema versions of the previous MVCC record and the partial update match, 
the server can apply the update directly (or store a delta), without referencing the schema. This is the 
fast path.


The slow path is if the schema versions do not match, as in the case of schema migration. The server must 
be able to construct a complete record of the new version based on the partial update (semantically at 
least, even though it could still be stored as a delta physically). An example:


Schema version 1: {"LastName":String, "FirstName":String, "Age":String}

Schema version 2: {"LastName":String, "Age":Int, "Balance":Int, "FirstName":String}


Assume all of the currently stored records are version 1. Then we have a partial update on a record with: 
{Balance=100}, version 2


The server can carry over LastName and FirstName from the previous MVCC record by name and type because 
they did not change (even though FirstName changed position). Balance is a new field in version 2 and 
it is set by the partial update so that is covered. But Age has changed types and the partial update 
does not include Age, so the partial update will be rejected by the server and an error returned.


The server would also need to return an error if the partial update set Age but not Balance. Even though 
Balance is a newly added field in version 2, it is not reasonable to assume it should be NULL after this 
partial update. This is because the user might be assuming that the previous record is already version 2 
and that Balance will be carried over, so it would be error-prone to not reject the partial update.


To summarize, the rules for a partial update between two schema versions are: 
1. For each field included in the partial update add it to the resulting record
2. For remaining fields in the resulting record, lookup by name and type in the previous MVCC record. If fields match then add to the resulting record
3. If there are any remaining unset fields in the resulting record, reject the partial update and return an error
