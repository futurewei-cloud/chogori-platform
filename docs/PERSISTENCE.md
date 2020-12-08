[-UP-](./README.md)


# Plog
The persistence data model provided/used by Chogori Project is called Plog. In our current design, this Plog is an append-only struct. The only operation that allowed to change the Plog is to append new data after current blocks. 

## Term
- Plog Client: Process all the operation requests regarding the Plog. It communicates with Plog Servers. It supports 4 operations: create, append, read, and seal.
- Plog Server: Communicate with Plog Client and process all the requests. It reads/appends data from/to the storage media.
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

## Append Request Reassembly （TODO）
It is possible that the arriving append request will arrive at the Plog Server out of order due to bad networking conditions. Therefore, on the Plog Server-side, we should cache the out-of-order requests. Once the Plog Server receives an append request, it will first check whether the request's offset is equal to the current offset of the request Plog it holds. If they are the same, this is an in-order request, we can handle it with the normal logic. Otherwise, it is an out-of-order request, the Plog Server will first reply to the Plog Client, notify the client that this packet is out-of-order. Then, it will cache this request. The append request cache is a linked list. For each out-of-order request, we insert it to the cache, sorting with its offset. Then, the Plog Server checked the neighboring requests of the inserted request, checking whether the neighboring append requests can merge. We can merge the append requests with continuous offset. If the Plog Server receives the missing append request, it will process the requests in the cache as well. 

# LogStream
The Log Stream operates the Plog Client to communicate with the Plog Servers and provides interfaces for upper application to use. Each Log Stream maintenance two types of plogs: metadata plogs and WAL plogs. When we write contents to the log stream, it will always write to the WAL plogs. The metadata plogs are used to record the metadata information of WAL plogs in this log stream. Metadata plogs only store the plog id of each WAL plog and its offset. Also, we need to store the metadata of metadata plogs as well. We store this information in CPO. We will describe this process in the following contents.

## Operations
The log stream service provides 3 operations: create, write, and read_all. 
- Create: The log stream client receives the name of this log stream as input. It will first create an active metadata plog and a backup plog, then register the active metadata plog to CPO. After that, the log stream client will create an active WAL plog and a backup WAL plog, write the plog Id of this active WAL plog to the active metadata plog. Only by finishing the creating process can this log stream client accept other operations such as write and read_all.
- Write: This operation takes two inputs: `payload` and `writeToWAL`. If `writeToWAL=true`, this write operation will write to the active WAL plog. Otherwise, it will write to the active metadata plog. The log stream client will first check whether the current plog will excess the size limit if it appends the payload to the current plog. If it will not exceed the size limit, the log stream client will append the input payload to the corresponding plog and return success. Otherwise, it will first seal the old plog, write the sealed offset of the old plog to metadata plog/CPO, create a new plog, write the plog id of the new plog to metadata plog/CPO, then write the payload to the new plog. We call this process `Plog Switch`. During this process, all the upcoming write should wait until the switching process finished in order to ensure correctness.  
- Read_ALL: Currently we are using the log stream to fully replay the indexer, so we only support the operation that reads all the content of a log stream. Given the name of a specific log stream, this operation will first retrieve the metadata of metadata plogs from CPO. Then, it will read the metadata of WAL from metadata plogs. Finally, it will read the contents from WAL plogs and return a vector of payloads.

## Optimization
When we switch the old plog to the new plog for the write operation, all the upcoming write requests will be blocked until the switching process finished, which is very inefficient. We used several optimization methods to improve the performance of this write operation:
- Backup Plog: When we create the log stream, we will not only create one active metadata plog and one active WAL plog, we will also create a backup plog for each of them. Therefore, when we do the plog switch, we can use the backup plog as the new plog immediately, instead of waiting for creating a new plog. Then, we can create the new backup plog in the background afterward.
- Make the plog switch process to be asynchronous: When we switch the old plog to the new plog, it is not necessary for us to block the upcoming write operations until the switching process finished. Instead, we can send all the upcoming write operations to Plog Servers during the switching process. However, if the log stream receives the response from the Plog Servers, said the write operation has been executed successfully, it will not notify the client that sent this write request immediately. Instead, it will wait until the switching process finished. Once it finished, the log stream will send all the pending success responses to the client immediately. 

## TODO
Currently, we only implement this persistence part for testing the performance of our system. So there are several points we would like to implement later
- Handle the failure cases: Currently we only implemented the log stream for the happy cases. If there are some unexpected errors happen, we will not handle it, but only throw a LogStreamException. Later, we will implement handling the failures in the log stream. 
- Currently our test case cannot test the scenario that a log stream create multiple metadata plogs since we used a fixed plog size limit for now. We only write the plog id and offset to the metadata plog. The size of each plog id is less than 100 bytes, while the size of offset is 4 bytes. If the size of each plog is 16MB, then each metadata plog can store the metadata of more than 100k WAL plogs. Currently our testcase cannot create such large amount of WAL plogs due to memory limit. Therefore we cannot test this scenario. We plan to add this testcase later if we can support variable size of plogs.


# Persist Transaction Records
TODO

# Reply Indexer
TODO