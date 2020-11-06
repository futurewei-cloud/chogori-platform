[-UP-](./README.md)

# Plog
The persistence service in Chogori Project is called Plog. Plog will store all the data in Non-Volatile Memory(NVM), which provides similar performance comparing to the Dynamic random-access memory(DRAM), but can retrieve stored information even after having been power cycled. Since comparing to the DRAM, NVM has a higher random access latency. In our current design, this Plog is an append-only struct. The only operation that allowed to change the Plog is to append new data after current blocks. 

## Term
- Plog Client: Process all the operation requests regarding the Plog. It communicates with Plog Servers, where we store all the data in NVM. It supports 4 operations: create, append, read, and seal.
- Plog Server: Communicate with Plog Client and process all the requests. It reads/appends data from/to NVM storage media.
- Persistence Group: A Persistence Group consists of 3 Plog Servers. When a Plog Client will create/append/seal a plog, it will always communicate with all the Plog Servers in the same Persistence Group simultaneously in order to provide redundancy.
- Persistence Cluster: A Persistence Group consists of several Persistence Groups. Each Persistence Clusters is store in the CPO with a Persistence Cluster Name. When a client initializes, it will request one Persistence Cluster from the CPO with a given Persistence Cluster Name. The client can select different Persistence Groups in this Persistence Cluster in order to provide load balance.

## Plog Id
A Plog Id is a unique identifier for a Plog. This is a small tuple that conveys `(Persistence Cluster Name, Persistence Group Name, Random Identifier)`. From this tuple, the client can locate the Plog Servers that stored that Plog.

## Operations
The Plog Service provides 4 operations: create, append, read, and seal. The Plog Server uses the current offset to verify whether a request is valid or not. If the current request is invalid, it will reject this request and return an error.
- Create: The Plog client will generate a Plog Id, and send a create request to the 3 Plog Servers in the same Persistence Group. Each Plog Server checked whether the key conflict exists. If not, it will pre-allocate a fixed-size block in NVM to persist the incoming request. Then, it will initialize the current offset = 0 that associated with this block. Once the Plog Client receives the success message, it will return the generated Plog Id.
- Append: The Plog Server received an append request contains Plog Id, current offset, and the content that needs to be persisted. If the current offset is not equal to the offset the Plog Server holds, or after appending the new content, the overall size exceeds the pre-allocated block size, the Plog Server will return an error. Otherwise, the Plog Server will append the new content in this Plog's block, then return success.
- Read: Since we only use 3 Plog Servers to provide redundancy, we are imagining the Plogs with the same Plog Id in different Plog Servers are exactly the same. So when the client sends a read request, it will only send to one Plog Server in the Persistence Cluster. The Plog Server valid the reading request and return the corresponding response.
- Seal: The client will send the seal request to the Plog Servers with a sealed offset. If this offset is inconsistent with the current offset the Plog Server holds, it will seal this Plog with the smaller offset. Once a Plog is Sealed, the client will not be allowed to make any modifications. 

## Append Request Reassembly 
It is possible that the arriving append request will arrive at the Plog Server out of order due to bad networking conditions. Therefore, on the Plog Server-side, we should cache the out-of-order requests. Once the Plog Server receives an append request, it will first check whether the request's offset is equal to the current offset of the request Plog it holds. If they are the same, this is an in-order request, we can handle it with the normal logic. Otherwise, it is an out-of-order request, the Plog Server will first reply to the Plog Client, notify the client that this packet is out-of-order. Then, it will cache this request. The append request cache is a linked list. For each out-of-order request, we insert it to the cache, sorting with its offset. Then, the Plog Server checked the neighboring requests of the inserted request, checking whether the neighboring append requests can merge. We can merge the append requests with continuous offset. If the Plog Server receives the missing append request, it will process the requests in the cache as well. 


# Log Stream
TODO

# Persist Transaction Records
TODO
