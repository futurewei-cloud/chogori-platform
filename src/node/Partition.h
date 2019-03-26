#pragma once

#include "../common/IntrusiveLinkedList.h"
#include "Collection.h"
#include "TaskRequest.h"
#include "common/PartitionMetadata.h"
#include "persistence/IPersistentLog.h"

namespace k2
{

//
//  K2 Partition
//
class Partition
{
public:
    enum class State { Assigning, Offloading, Running, Offloaded };

    State state;

protected:
    friend class AssignmentManager;

    typedef IntrusiveLinkedList<TaskRequest> TaskList;

    std::array<TaskList, (size_t)TaskListType::TaskListCount>
        taskLists; //  Partition tasks

    PartitionMetadata metadata;
    Collection &collection;
    PartitionVersion version;

    static void removeFromList(TaskList &list, TaskRequest &task)
    {
        list.remove(task);
        task.ownerTaskList = TaskListType::None;
    }

    void removeFromList(TaskRequest &task)
    {
        if (task.ownerTaskList == TaskListType::None)
            return;

        removeFromList(getTaskList(task.ownerTaskList), task);
    }

    TaskList &getTaskList(TaskListType type)
    {
        return taskLists[(int)type - 1];
    }

    void putToListBack(TaskRequest &task, TaskListType type)
    {
        removeFromList(task);
        getTaskList(type).pushBack(task);
        task.ownerTaskList = type;
    }

    void activateTask(TaskRequest &task)
    {
        putToListBack(task, TaskListType::Active);
    }

    void putTaskToSleep(TaskRequest &task)
    {
        putToListBack(task, TaskListType::Sleeping);
    }

    template <typename T, typename... ArgT>
    typename std::enable_if<std::is_base_of<TaskRequest, T>::value, T *>::type
    createTask(ArgT &&... arg) //  TODO: Change to unique_ptr
    {
        return new T(*this,
                     std::forward<ArgT>(arg)...); //  TODO: Use partition memory
    }

    template <typename T, typename... ArgT>
    typename std::enable_if<std::is_base_of<TaskRequest, T>::value, T *>::type
    createAndActivateTask(ArgT &&... arg) //  TODO: Change to unique_ptr
    {
        T *task = createTask<T>(std::forward<ArgT>(arg)...);
        activateTask(*task);
        return task;
    }

    void deleteTask(TaskRequest &task)
    {
        removeFromList(task);
        delete &task;
    }

    void release()
    {
        for (TaskList &list : taskLists) {
            for (TaskRequest &task : list) {
                task.cancel();
                deleteTask(task);
            }
        }
    }

    bool processActiveTasks(
        std::chrono::nanoseconds
            maxPartitionTime) //  When return false, partition is deleted
    {
        TimeTracker partitionTracker(maxPartitionTime);
        for (TaskRequest &task : getTaskList(TaskListType::Active)) {
            std::chrono::nanoseconds remainingTime;
            if ((remainingTime = partitionTracker.remaining()) >
                std::chrono::nanoseconds::zero())
                break;

            TaskRequest::ProcessResult response =
                task.process(remainingTime); //  TODO: move to partition
            switch (response) {
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
                    assert(false);
            }
        }

        return true;
    }

public:
    Partition(PartitionMetadata &&metadata, Collection &collection,
              PartitionVersion version)
        : metadata(std::move(metadata)), collection(collection),
          moduleData(nullptr), state(State::Assigning), version(version)
    {
    }

    ~Partition() { release(); }

    IPersistentLog &getLog(uint32_t logId);
    uint32_t getLogCount();

    void *moduleData; //  Module specific data, originally null

    const PartitionMetadata &getMetadata() const { return metadata; }
    const CollectionMetadata &getCollection() const
    {
        return collection.getMetadata();
    }

    PartitionVersion getVersion() const { return version; }
    PartitionId getId() const { return metadata.getId(); }

    IModule &getModule() { return collection.getModule(); }
}; //  class Partition

} //  namespace k2
