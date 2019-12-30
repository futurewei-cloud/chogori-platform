
#include "Partition.h"
#include <k2/common/TimeMeasure.h>

namespace k2
{

void Partition::removeFromList(TaskList& list, TaskRequest& task)
{
    list.remove(task);
    task.ownerTaskList = TaskListType::None;
}

void Partition::removeFromList(TaskRequest& task)
{
    if(task.ownerTaskList == TaskListType::None)
        return;

    removeFromList(getTaskList(task.ownerTaskList), task);
}

void Partition::putToListBack(TaskRequest& task, TaskListType type)
{
    removeFromList(task);
    getTaskList(type).pushBack(task);
    task.ownerTaskList = type;
}


void Partition::deleteTask(TaskRequest& task)
{
    removeFromList(task);
    taskRequestLifecycleHistogram.add(task.getElapsedTime());
    delete &task;
}

void Partition::release()
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

bool Partition::processActiveTasks(Duration maxPartitionTime)  //  When return false, partition is deleted
{
    Stopwatch stopWatch;
    TimeTracker partitionTracker(maxPartitionTime);
    for(TaskRequest& task : getTaskList(TaskListType::Active))
    {
        Duration remainingTime = partitionTracker.remaining();
        if(remainingTime == Duration::zero())
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
                assert(false);
        }
    }

    return true;
}

void Partition::awakeTask(TaskRequest& task)
{
    assert(task.ownerTaskList == TaskListType::Sleeping);
    activateTask(task);
}

void Partition::transitionToRunningState()
{
    assert(state == State::Assigning);
    state = State::Running;
}

void Partition::registerMetrics()
{
    std::vector<seastar::metrics::label_instance> labels;
    labels.push_back(seastar::metrics::label_instance("partition_id", getId()));
    metricGroups.add_group("partition", {
        seastar::metrics::make_histogram("task_request_lifecycle_time", [this] { return taskRequestLifecycleHistogram.getHistogram(); }, seastar::metrics::description("The lifecycle time of a task request"), labels),
    });
}

}   //  namespace k2
