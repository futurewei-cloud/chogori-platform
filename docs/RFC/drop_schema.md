## Introduction

This document describes a design for dropping SKV schemas and truncating the associated data records. It is non-transactional, destructive, and meant to be an admin operation (e.g. it won't be exposed on the SKV client API).

### Terminology

For this document:
- Schema: Only the schema description, not the data that refers to that schema.
- Truncate: A destructive delete, with no possibility of rollback and no ability to read old MVCC versions.

### Use Cases

- Benchmarking and testing. For example, dropping TPC-C tables to more quickly re-run the benchmark.
- User wants to re-architect an application. The user is responsible for migrating traffic and data to make the operation safe even though it is non-transactional.
- Quickly regain capacity in the cluster. Since the drop and truncate is non-transactional, the capacity is regained immediately instead of after retention window expiration.

### Rationale

In general, users and the system should not think of schemas like tables. Schemas do not own their data like tables do. They are merely a description of the data formats that can be stored on servers. So K23SI operations do not operate over schemas; instead schema operations are CPO operations, like create collection. We should think of schemas as a performance optimization over self-describing records.

## Design: truncateDataAndDropSchema

truncateDataAndDropSchema truncates all data and all schema references for a given schema name (i.e. it deletes all versions of the schema).

Operations:
1. User request: truncateDataAndDropSchema(name)
2. Client: the client library translates the name to the schema ID
3. Client: Request sent to CPO
4. CPO: drops schema locally (will no longer be returned by getSchema requests)
5. CPO: sends truncateDataAndDropSchema to all partitions
6. Partition: remove schema locally in-memory (no longer accepts requests for that schema)
7. Partition: responds to CPO
8. CPO: after receiving responses from all partitions, responds to client. If there are failures in the partition requests after retries, then the partitions must be considered dead and re-assigned which will remove the schema. At this point the user may recreate a schema with the same name
9. Partition (async): scan and truncate all records and all MVCC versions matching the schema. No persistence or tombstones are needed. If the in-memory data is saved in PLOG matching format then the truncation must wait until WAL cleanup.
10. CPO (async): Cleanup of WAL data. Some notes on this are below.

The current design will change so that schema communication with servers uses a schema ID instead of the schema name. The schema ID is vended by the CPO at schema creation time and will be unique within a collection. The user-level SKV API will still use schema names and the client library will translate the names to the ID when communicating with the cluster. This change lets the user safely recreate a schema with the same name.

### Recovery

Partitions get the list of valid schemas when recovery starts. WAL records that do not match a valid schema are ignored.

### WAL Garbage Collection

For the WAL records of truncated data, either the CPO can asynchronously delegate garbage collection, or they can be cleaned up during normal WAL garbage collection since WAL garbage collection should have the list of currently valid schemas.

### Hot Standby

When we design the hot standby feature we should consider how drop and truncate operations should function and how the standby will stay consistent.

### Other operations

We should also support a truncateData operation which takes a schema name and truncates all data using that schema. This would be the same as the operations above except that it skips steps 4 and 6.

We cannot support an independent dropSchema operation without truncating data because it would orphan the existing records using that schema without any way to delete or use them.
