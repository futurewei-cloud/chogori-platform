#pragma once

#include "../common/IntrusiveLinkedList.h"
#include "../common/MemoryArena.h"
#include "common/Message.h"

namespace k2
{

class AssignmentManager;
class Partition;

//
//
//
enum class TaskListType {
    None = 0,
    Active,
    Sleeping,

    NonExistingTaskList,
    TaskListCount = NonExistingTaskList - 1
};

//
//  List of know task types
//
enum class TaskType {
    ClientRequest,
    PartitionAssign,
    PartitionOffload,
    Maintainence
};

//
//  Delete functor for objects allocated in task memory arena.
//
template <typename T> struct TaskScopeArenaDeleter {
    void operator()(T *obj)
    {
        if (obj)
            obj->~T();
    } //  No need to delete since whole arena will be deallocated - just call
      //  destructor
};

//
//  unique_ptr which just call destructor on release and no memory if freed.
//
template <typename T>
using TaskScopeArenaUniquePtr = std::unique_ptr<T, TaskScopeArenaDeleter<T>>;

//
//  The context of the task created by request to K2 Node. TaskRequest is always
//  associated with partition. TaskRequest
//
class TaskRequest
{
    friend class AssignmentManager;
    friend class Partition;

protected:
    Partition &partition;
    MemoryArena arena; //  Task local memory

    TimeTracker timeTracker;

    K2_LINKED_LIST_NODE;

    TaskListType ownerTaskList; //  Task list in which this task resides

    TaskRequest(Partition &partition) : partition(partition) {}

    enum class ProcessResult { Done = 0, Sleep, Delay, DropPartition };

    ProcessResult process(std::chrono::nanoseconds maxExecutionTime)
    {
        timeTracker = TimeTracker(maxExecutionTime);
        return process();
    }

    virtual ProcessResult process() = 0;
    virtual void cancel() {}

    std::ostream &logger()
    {
        return std::cerr;
    } //  Change to something more appropriate

public:
    //
    //  Each task is associated with particular partition. This property returns
    //  it
    //
    Partition &getPartition() { return partition; }

    //
    //  One of the know types of the task.
    //
    virtual TaskType getType() const = 0;

    //
    //  Check whether module is allowed to proceed with job execution or need to
    //  postpone the task because it's time quota is expired
    //
    bool canContinue() { return !timeTracker.exceeded(); }

    //
    //  Task has it's own associated memory arena. TODO: use Folly Memory Arena
    //
    void *taskScopeMalloc(size_t size) { return arena.alloc(size); }

    //
    //  Allocate new object in a task scope (memory will be released when task
    //  finishes)
    //
    template <typename T, typename... ArgT>
    TaskScopeArenaUniquePtr<T> taskScopeNew(ArgT &&... arg)
    {
        return TaskScopeArenaUniquePtr<T>(
            arena.newObject(std::forward<ArgT>(arg)...));
    }

    //
    //  Make virtual distructor to enforce polymorphism
    //
    virtual ~TaskRequest() {}

    //
    //  Module specific data
    //
    void *moduleData;
}; //  class WorkRequest

} //  namespace k2
