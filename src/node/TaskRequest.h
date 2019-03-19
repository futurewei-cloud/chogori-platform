#pragma once

#include "common/Message.h"
#include "Partition.h"
#include <boost/intrusive/list.hpp>
#include "../common/MemoryArena.h"

namespace k2
{

class AssignmentManager;

//
//  List of know task types
//
enum class TaskType
{
    ClientRequest,
    PartitionAssign,
    PartitionOffload,
    Maintainence
};

//
//  Delete functor for objects allocated in task memory arena.
//
template<typename T>
struct TaskScopeArenaDeleter
{
    void operator()(T* obj) { if(obj) obj->~T(); }  //  No need to delete since whole arena will be deallocated - just call destructor
};

//
//  unique_ptr which just call destructor on release and no memory if freed.
//
template<typename T>
using TaskScopeArenaUniquePtr = std::unique_ptr<T, TaskScopeArenaDeleter<T>>;

//
//  The context of the task created by request to K2 Node. TaskRequest is always associated with partition.
//  TaskRequest
//
class TaskRequest
{
    friend class AssignmentManager;
protected:
    Partition& partition;
    MemoryArena arena;  //  Task local memory

    TimeTracker timeTracker;

    boost::intrusive::list_member_hook<> linkedListHook;

    TaskRequest(Partition& partition) : partition(partition) {}

    void setAllowedExecutionTime(std::chrono::nanoseconds maxExecutionTime)
    {
        timeTracker = TimeTracker(maxExecutionTime);
    }  //  TODO

public:
    //
    //  Each task is associated with particular partition. This property returns it
    //
    Partition& getPartition() { return partition; }

    //
    //  One of the know types of the task.
    //
    virtual TaskType getType() const = 0;

    //
    //  Check whether module is allowed to proceed with job execution or need to postpone the task
    //  because it's time quota is expired
    //
    bool canContinue() { return !timeTracker.exceeded(); }

    //
    //  Task has it's own associated memory arena. TODO: use Folly Memory Arena
    //
    void* taskScopeMalloc(size_t size) { return arena.alloc(size); }

    //
    //  Allocate new object in a task scope (memory will be released when task finishes)
    //
    template<typename T, typename... ArgT>
    TaskScopeArenaUniquePtr<T> taskScopeNew(ArgT&&... arg)
    {
        return TaskScopeArenaUniquePtr<T>(arena.newObject(std::forward<ArgT>(arg)...));
    }

    //
    //  Make virtual distructor to enforce polymorphism
    //
    virtual ~TaskRequest() {}

    //
    //  Module specific data
    //
    void* moduleData;
};  //  class WorkRequest


//
//  Task request caused by some message obtained from Transport
//
class MessageInitiatedTaskRequest : public TaskRequest
{
protected:
    std::unique_ptr<ClientConnection> client;
public:
    MessageInitiatedTaskRequest(Partition& partition, std::unique_ptr<ClientConnection> client) :
        TaskRequest(partition),  client(std::move(client))
    {
        assert(client);
    }

    ClientConnection& getClient() { return *client.get(); }
};


//
//  Task created as a response to request from client
//
class ClientTask : public MessageInitiatedTaskRequest
{
protected:
    Payload requestPayload;
    Payload responsePayload;
    PayloadWriter responseWriter;
public:
    ClientTask(Partition& partition, std::unique_ptr<ClientConnection> client, Payload&& requestPayload) :
        MessageInitiatedTaskRequest(partition, std::move(client)), requestPayload(std::move(requestPayload)),
        responseWriter(responsePayload, 0) {}

    TaskType getType() const override { return TaskType::ClientRequest; }

    const Payload& getRequestPayload() const { return requestPayload; }

    PayloadWriter& getResponseWriter() { return responseWriter; }

    void releaseRequestPayload()
    {
        requestPayload.clear();
    }
};  //  class ClientTask


//
//  Task created as a response to Partition Manager Partition Assign command
//
class AssignmentTask : public MessageInitiatedTaskRequest
{
protected:
    std::unique_ptr<PartitionMetadata> partitionMetadata;
public:
    AssignmentTask(Partition& partition, std::unique_ptr<ClientConnection> client, std::unique_ptr<PartitionMetadata> partitionMetadata) :
        MessageInitiatedTaskRequest(partition, std::move(client)), partitionMetadata(std::move(partitionMetadata)) {}

    PartitionMetadata& getPartitionMetadata() { return *partitionMetadata; }
    static std::unique_ptr<AssignmentTask> parse(Partition& partition, const Binary payload) { return nullptr; }    //  TODO: return status, implement parsing

    TaskType getType() const override { return TaskType::PartitionAssign; }
};


//
//  Task created as a response to Partition Manager Partition Offload command
//
class OffloadTask : public MessageInitiatedTaskRequest
{
    OffloadTask(Partition& partition, std::unique_ptr<ClientConnection> client) :
        MessageInitiatedTaskRequest(partition, std::move(client)) {}
public:
    TaskType getType() const override { return TaskType::PartitionOffload; }
};


//
//  Task created as a response to request from client
//
class MaintainenceTask : public TaskRequest
{
public:
    MaintainenceTask(Partition& partition) : TaskRequest(partition) {}

    TaskType getType() const override { return TaskType::Maintainence; }
};  //  class ClientTask

}   //  namespace k2
