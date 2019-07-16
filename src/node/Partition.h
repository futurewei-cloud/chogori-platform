#pragma once

#include <seastar/core/metrics.hh>
#include "common/PartitionMetadata.h"
#include "Collection.h"
#include "persistence/IPersistentLog.h"
#include "TaskRequest.h"
#include "common/IntrusiveLinkedList.h"
#include "transport/Prometheus.h"

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


    static void removeFromList(TaskList& list, TaskRequest& task)
    {
        list.remove(task);
        task.ownerTaskList = TaskListType::None;
    }

    void removeFromList(TaskRequest& task)
    {
        if(task.ownerTaskList == TaskListType::None)
            return;

        removeFromList(getTaskList(task.ownerTaskList), task);
    }

    TaskList& getTaskList(TaskListType type) { return taskLists[(int)type-1]; }

    void putToListBack(TaskRequest& task, TaskListType type)
    {
        removeFromList(task);
        getTaskList(type).pushBack(task);
        task.ownerTaskList = type;
    }

    void activateTask(TaskRequest& task)
    {
        putToListBack(task, TaskListType::Active);
    }

    void putTaskToSleep(TaskRequest& task)
    {
        putToListBack(task, TaskListType::Sleeping);
    }

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

    void deleteTask(TaskRequest& task)
    {
        removeFromList(task);
        taskRequestLifecycleHistogram.add(task.getElapsedTime());
        delete &task;
    }

    void release()
    {
        state = State::Offloaded;   //  To mark it in memory
        for(TaskList& list : taskLists)
        {
            for(TaskRequest& task : list)
            {
                task.cancel();
                deleteTask(task);
            }
        }
    }

    bool haveTasksToRun()
    {
        return !getTaskList(TaskListType::Active).isEmpty();
    }

    bool processActiveTasks(std::chrono::nanoseconds maxPartitionTime)  //  When return false, partition is deleted
    {
        TimeTracker partitionTracker(maxPartitionTime);
        for(TaskRequest& task : getTaskList(TaskListType::Active))
        {
            std::chrono::nanoseconds remainingTime;
            if((remainingTime = partitionTracker.remaining()) > std::chrono::nanoseconds::zero())
                break;

            TaskRequest::ProcessResult response = task.process(remainingTime);  //  TODO: move to partition
            switch (response)
            {
                case TaskRequest::ProcessResult::Done:
                    deleteTask(task);
                    break;

                case TaskRequest::ProcessResult::Sleep:
                    putTaskToSleep(task);
                    break;

                case TaskRequest::ProcessResult::Delay:
                    activateTask(task);
                    break;

                case TaskRequest::ProcessResult::DropPartition:
                    release();
                    return false;

                default:
                    ASSERT(false);
            }
        }

        return true;
    }

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
    void awakeTask(TaskRequest& task)
    {
        assert(task.ownerTaskList == TaskListType::Sleeping);
        activateTask(task);
    }

    void transitionToRunningState()
    {
        assert(state == State::Assigning);
        state = State::Running;
    }

    void registerMetrics()
    {
        std::vector<seastar::metrics::label_instance> labels;
        labels.push_back(seastar::metrics::label_instance("partition_id", getId()));
        metricGroups.add_group("partition", {
            seastar::metrics::make_histogram("task_request_lifecycle_time", [this] { return taskRequestLifecycleHistogram.getHistogram(); }, seastar::metrics::description("The lifecycle time of a task request"), labels),
        });
    }
};  //  class Partition

}   //  namespace k2
