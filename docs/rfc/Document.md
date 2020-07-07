## Motivation


For advanced workloads such as SQL, we need to be able to filter, project, and do partial updates on data 
in order to maintain good performance. These features allow the server to significantly reduce the 
amount of data sent back to the client.


To achieve this, the server needs to understand the schema of data so that it can operate on individual 
fields. Our solution to this is to convert the existing K23SI interface from a KV blob interface to a 
richer document interface.


## Overview


The interface between the K2 server and client is defined by the DTOs, and is similar to the previous. 
KV interface. Requests are routed to partitions based on the partitionKey String. Uniqueness of a document 
is based on the combined partitionKey and rangeKey Strings. The schema name is part of the rangeKey and 
the schema version is transmitted seperately as part of the DTO types.


There are two main user-K23SI client interfaces. One is template-based document read/write operations where 
the schema is known at compile time and can be expressed as a C++ type. The other is the SerializableDocument 
class which allows the user to serialize/deserialize one field at a time. In both cases, as part of the 
schema, the user defines which fields will be part of the partitionKey and which fields will be part of 
the rangeKey. The client will automatically construct the partitionKey and rangeKey from the fields 
serialized by the user.


## Supported Document Field Types


Every type used as a document field needs to be supported by us in code. We have different levels of support: 
 - Basic: Can read and write fields. We need to be able to serialize it to a payload.
 - Query: Can be used as part of a query predicate. We need to write relational operators for these types.
 - Key: Can be used as part of the partitionKey or rangeKey. We need to be able to convert it to a string that maintains a lexographic total order


## Query Operation


The query operation is semi-programmable. We support a list of predicates, each with a relational operator 
and an operand. The predicates will be applied and ANDed together. We also support a list of fields to 
project.


The query is a prefix scan with a key prefix, an exclusive start key (can be used to continue a scan), and 
an optional end key.
