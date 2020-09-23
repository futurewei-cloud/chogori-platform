# Schema-Aware Key-Value Interface (SKV)

## Motivation


For advanced workloads such as SQL, we need to be able to filter, project, and do partial updates on data 
in order to maintain good performance. These features allow the server to significantly reduce the 
amount of data sent back to the client.


To achieve this, the server needs to understand the schema of data so that it can operate on individual 
fields. Our solution to this is to convert the existing K23SI interface from a KV blob interface to a 
richer schema-aware interface.

## Overview

The interface between the K2 server and client is defined by the DTOs, and is similar to the previous. 
KV interface. Requests are routed to partitions based on the partitionKey String. Uniqueness of a record 
is based on the combined partitionKey String, rangeKey String, and schema name. The schema name and version 
are transmitted in the DTO as part of the SKVRecord. The partitionKey and rangeKey are separate 
fields in the request DTO but can be constructed from fields in the SKVRecord (more details in 
the SKVRecord section).


There are two main user-K23SI client interfaces. One is template-based record read/write operations where 
the schema is known at compile time and can be expressed as a C++ type. The other is the SKVRecord 
class which allows the user to serialize/deserialize one field at a time. In both cases, as part of the 
schema, the user defines which fields will be part of the partitionKey and which fields will be part of 
the rangeKey. The client will automatically construct the partitionKey and rangeKey from the fields 
serialized by the user. The two interfaces (template-based C++ types and SKVRecord) are cross-
compatible.

## Schemas (see src/k2/dto/ControlPlaneOracle.h)

Schemas consist of a name, version number, field definitions, and key definitions. Field definitions 
consist of a positional list of field types, field names, sort order, and NULL order. rangeKey and 
partitionKey definitions are a positional list of indexes into the field definitions. The user 
specifices the schema name and version. Schemas are attached to a particular collection.


For a schema field, the sort order defined does not affect the storage/indexer ordering of the field 
but it could be used as a default ordering for query results. All key fields will be stored in 
ascending order on the storage nodes. All fields are considered optional and may be NULL, even 
key fields. NULL order determines whether NULLs come first or last in query results, query predicates 
(e.g. if user specifies AGE < 25, are NULLs included), and it does affect ordering on the storage node 
for key fields.


At least one field must be designated a partitionKey field. Positionally, all partitionKey fields 
and rangeKey fields must come before value fields (i.e. fields not part of a key). The definition of 
key fields (order, name, and type) must not change between schema versions. This is so that a user 
can construct a valid read request from any version of the schema. More details on schema versioning 
are upcoming.


When the user creates a schema, it sends it as a request to the CPO. After validation, the CPO pushes 
the schema to all k2 storage nodes that own a partition of the collection. If this succeeds the CPO 
responds with success to the user and the user can begin using the new schema.

## SKVRecord (see src/k2/dto/SKVRecord.h)

The SKVRecord class is the main interface for the user to interact with the SKV. It is 
used for creating a read request, creating a write request, and for a read response.


The user starts by creating a SKVRecord and associating it with a particular Schema. 
Operations on the record will be validated against that schema to the extent possible (e.g. when 
serializing a field the type must match the schema type for that field).


Since the SKVRecord will be used for nearly all data-path operations in the SKV system, 
it must be efficient. To that end, the field type system is not inheritance-based and the 
serialization approach for field data is based on the K2 Payload, which is one-shot, in-order, and 
not self-describing. This requires some restrictions on the SKVRecord interface.


To create a SKVRecord for an SKV read or write request, the user must serialize the 
fields of the schema by calling serializeNext<> or skipNext (if the field data should be NULL). This 
must be done in order of the schema. For a read request, the user can stop after serializing the 
fields that are key fields (which must come first in the schema as described above). For a write 
request all fields must be serialized or explicitly skipped.


The SKVRecord handles converting key fields into partition and range strings. To do this it 
relies on the string conversion functions provided in src/k2/dto/FieldTypes.h.


When the user gets a SKVRecord back as part of a read or query response, they must 
deserialize to get the field data. The deserializeNextOptional function can be used to do this 
but since it is templated on the type of the field, the user needs to reference the schema, write a 
switch statement on the type, and then call the correct template instantiation for the type. As a 
convenience, the FOR\_EACH\_RECORD\_FIELD and DO\_ON\_NEXT\_RECORD\_FIELD macros can be used with a 
templated visitor function, which provides the functionality of the switch statement for the user. 
See the macro comments and definitions for more details.


## SKV Client


The intended use of the SKV Client is to create a TxnHandle with beginTxn, issue read, write, or erase 
requests, and then call end. End should be called exactly once by the user even if one of the read or 
write operations has failed. This is to ensure that the transaction gets cleaned up server-side.


If the transaction encounters an error on a read or write operation it will be recorded by the client, 
and the client will make sure an abort is issued to the server. If the user tried to commit, they will get 
an error status back. The SKV Client will try to prevent data inconsistency caused by user error. One 
example is that the user should always chain the end call after any potential read or write operations. 
This is so that any errors that should make the transaction abort are known by the client before the end 
call is made. The SKV Client will count ongoing read or write operations and throw an exception if the user 
did not wait for them. This indicates a bug in the user code and the transaction should be left to be 
cleaned up by the retention window. The SKV Client cannot let the user issue an end request in parallel 
with read or write requests because the SKV Client does not know if the user will issue more read or 
write requests in the future, and so it does not have the information needed to know when to issue the end 
request to the server. In the case where the user issues a read, write, or end request on a TxnHandle that 
has already ened, the client will also throw an exception as this is also a bug in the user code.


## SKV Query


An SKV Query is a read-only scan operation with predicates and projection. It operates over a single 
schema (but over all versions of that schema, see schema\_versioning.md for more details). The SKV 
Query interface is a fixed-function API, not generic pushdown.


Query predicates consist of a field name, an expected field type, a relational operator, and a literal 
of the field type. For example, {"AGE", Int, LESS\_THAN, 25} represents a predicate that 
selects for records where the AGE field is less than 25. By expressing predicate fields by name rather 
than positionally we can support queries across schema versions more easily. Similarly, projection is 
also expressed by field name. Predicates are logically ANDed together by the server.


The result of a Query is a set of SKVRecords, which is paginated. The server decides how many records to 
include in a single response but the user can specify a limit on the total number of records they want. 
Pagination is implemented by an opaque (to the user) continuation token and does not require saving any 
server-side state. A Query operation may span multiple partitions in which case it is possible for the 
user to get zero records in a single call but they should still continue making calls with the 
continuation token.


For multi-partition Queries, the read cache on the servers will be updated individually on each partition 
as the paginated request comes in. The alternative is to communicate with all potentially participating 
partitions and insert into the read cache before returning any records to the user, but our current 
understanding is that this is not necessary.
