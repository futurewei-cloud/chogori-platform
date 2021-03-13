## Drop Collection

### Overview of Steps

- Client issues drop collection request to CPO
- CPO moves the collection metadata to a deleted list. The collection will no longer be returned as part of get collection requests.
- CPO issues unload partition request to all collection partitions
- A server node receiving an unload partition request will immediately drop the partition, without waiting for ongoing operations and with no persistence needed.
- After receiving responses from all partitions, CPO responds to the client.

### Notes

The CPO is the authoratative truth of collection metadata such as the partition map, version, and the 
existence of a collection. Therefore the drop collection operations does not need to be persisted to the 
WAL on the individual partitions.

After the CPO moves the collection to the delete list, a new collection with the same name can be created. 
For our current use cases, we likely want to reuse the same server nodes for any new collection, so the CPO 
will wait to respond to the client until all partitions are unloaded.

The CPO must save the collection metadata on the deleted list because if a new collection is created with 
the same name, it needs a collection version higher than the previously deleted version. This is so that 
if an ongoing client can detect it needs to refresh from the CPO. Unlike a partition map version refresh, this 
refresh must abort any existing transactions (how?). The collection metadata could also be used to undo the 
collection delete or make it permanent and clean the WALs.

