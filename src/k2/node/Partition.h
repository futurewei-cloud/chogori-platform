#pragma once

#include <k2/common/IntrusiveLinkedList.h>
#include <k2/common/Log.h>
#include <k2/k2types/PartitionMetadata.h>
#include <k2/persistence/IPersistentLog.h>
#include <seastar/core/metrics.hh>
#include "Collection.h"
#include "TaskRequest.h"
#include <k2/transport/Prometheus.h>

namespace k2
{
class INodePool;

//
//  K2 Partition
//
class Partition
{
public:
    enum class State
    {
        Assigning,
        Offloading,
        Running,
        Offloaded
    };

protected:
    friend class AssignmentManager;

    typedef IntrusiveLinkedList<TaskRequest> TaskList;

    PartitionVersion version;
    State state = State::Assigning;
    INodePool& nodePool;
    std::array<TaskList, (size_t)TaskListType::TaskListCount> taskLists;  //  Partition tasks
    PartitionMetadata metadata;
    Collection& collection;
    seastar::metrics::metric_groups metricGroups;
    ExponentialHistogram taskRequestLifecycleHistogram; // Tracks the lifecycle of the TaskRequest

    static void removeFromList(TaskList& list, TaskRequest& task);

    void removeFromList(TaskRequest& task);

    TaskList& getTaskList(TaskListType type) { return taskLists[(int)type-1]; }

    void putToListBack(TaskRequest& task, TaskListType type);

    void activateTask(TaskRequest& task) { putToListBack(task, TaskListType::Active); }

    void putTaskToSleep(TaskRequest& task) { putToListBack(task, TaskListType::Sleeping); }

    template<typename T, typename... ArgT>
    typename std::enable_if<std::is_base_of<TaskRequest, T>::value, T*>::type createTask(ArgT&&... arg)  //  TODO: Change to unique_ptr
    {
        return new T(*this, std::forward<ArgT>(arg)...); //  TODO: Use partition memory
    }

    template<typename T, typename... ArgT>
    typename std::enable_if<std::is_base_of<TaskRequest, T>::value, T*>::type createAndActivateTask(ArgT&&... arg)  //  TODO: Change to unique_ptr
    {
        T* task = createTask<T>(std::forward<ArgT>(arg)...);
        activateTask(*task);
        return task;
    }

    void deleteTask(TaskRequest& task);

    void release();

    bool haveTasksToRun() { return !getTaskList(TaskListType::Active).isEmpty(); }

    bool processActiveTasks(Duration maxPartitionTime);  //  When return false, partition is deleted

public:
    Partition(INodePool& pool, PartitionMetadata&& metadata, Collection& collection, PartitionVersion version) :
        version(version), nodePool(pool), metadata(std::move(metadata)), collection(collection) { registerMetrics(); }

    ~Partition() { release(); }

    IPersistentLog& getLog(uint32_t logId);
    uint32_t getLogCount();

    void* moduleData = nullptr;   //  Module specific data, originally null

    const PartitionMetadata& getMetadata() const { return metadata; }
    const CollectionMetadata& getCollection() const { return collection.getMetadata(); }

    PartitionVersion getVersion() const { return version; }
    PartitionId getId() const { return metadata.getId(); }

    IModule& getModule() { return collection.getModule(); }

    INodePool& getNodePool() { return nodePool; }

    State getState() const { return state; }

    //
    //  TODO: hide below function from module somehow
    //
    void awakeTask(TaskRequest& task);

    void transitionToRunningState();

    void registerMetrics();
};  //  class Partition

}   //  namespace k2
