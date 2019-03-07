#pragma once

#include "../common/Message.h"
#include "Partition.h"

namespace k2
{

class MemoryArena {};    // TODO: integrate Facebook Folly memory arena

//
//  The context of the task created by request to K2 Node. Request is always associated with partition.
//
class TaskRequest
{
protected:
    Partition& partition;
    MemoryArena arena;  //  Task local memory

    TaskRequest(Partition& partition) : partition(partition) {}

public:
    Partition& getPartition() { return partition; }

    void* taskLiveTimeMalloc(size_t size) { return std::malloc(size); } //  TODO: use arena to prevent memory leak

    template<typename T>
    T* taskLiveTimeNew(size_t size) { return std::malloc(size); } //  TODO: use arena to prevent memory leak

    void* malloc(size_t size) { return std::malloc(size); }
    void free(void* ptr, size_t size) { std::free(ptr); }

    //
    //  Check whether module is allowed to proceed with job execution or need to postpone the task
    //  because it's time quota is expired
    //
    bool canContinue() { return true; } //  TODO: use start time and quota to calculate

    //
    //  Make virtual distructor to enforce polymorphism
    //
    virtual ~TaskRequest() {}
};  //  class WorkRequest


//
//  Task request caused by some message obtained from Transport
//
class MessageInitiatedTaskRequest : public TaskRequest
{
protected:
    std::unique_ptr<Message> message;
public:
    MessageInitiatedTaskRequest(Partition& partition, std::unique_ptr<Message> message) : TaskRequest(partition),  message(std::move(message))
    {
        assert(message);
    }
};


//
//  Task created as a response to request from client
//
class ClientTask : public MessageInitiatedTaskRequest
{
public:
    const Message& getMessage() { return *message; }
    ClientTask(Partition& partition, std::unique_ptr<Message> message) : MessageInitiatedTaskRequest(partition, std::move(message)) {}
};  //  class ClientTask


//
//  Task created as a response to Partition Manager Partition Assign command
//
class AssignmentTask : public MessageInitiatedTaskRequest
{
protected:
    std::unique_ptr<PartitionMetadata> partitionMetadata;
    AssignmentTask(Partition& partition, std::unique_ptr<Message> message, std::unique_ptr<PartitionMetadata> partitionMetadata) : MessageInitiatedTaskRequest(partition, std::move(message)) {}
public:
    const PartitionMetadata& getPartitionMetadata() { return *partitionMetadata; }
    std::unique_ptr<AssignmentTask> parse(Partition& partition, std::unique_ptr<Message> message) { return nullptr; }    //  TODO: return status, implement parsing
};


//
//  Task created as a response to Partition Manager Partition Offload command
//
class OffloadTask : public MessageInitiatedTaskRequest
{
    OffloadTask(Partition& partition, std::unique_ptr<Message> message) : MessageInitiatedTaskRequest(partition, std::move(message)) {}
public:
    std::unique_ptr<OffloadTask> parse(Partition& partition, std::unique_ptr<Message> message) { return nullptr; }    //  TODO: return status, implement parsing
};


//
//  Task created as a response to request from client
//
class MaintainenceTask : public TaskRequest
{
public:
    MaintainenceTask(Partition& partition) : TaskRequest(partition) {}
};  //  class ClientTask

}   //  namespace k2
