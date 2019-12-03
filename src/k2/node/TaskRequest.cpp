#include "TaskRequest.h"

namespace k2 {

    TaskRequest::TaskRequest(Partition& partition) : partition(partition), ownerTaskList(TaskListType::None) {}

    TaskRequest::ProcessResult TaskRequest::process(std::chrono::nanoseconds maxExecutionTime) {
        timeTracker = TimeTracker(maxExecutionTime);
        return process();
    }

    void TaskRequest::cancel() {}

    std::ostream& TaskRequest::logger() { return std::cerr; }  //  Change to something more appropriate

    Partition& TaskRequest::getPartition() { return partition; }

    bool TaskRequest::canContinue() { return !timeTracker.exceeded(); }

    std::chrono::nanoseconds TaskRequest::getElapsedTime() { return timeTracker.elapsed(); }

    void* TaskRequest::taskScopeMalloc(size_t size) { return arena.alloc(size); }

    //
    //  Make virtual distructor to enforce polymorphism
    //
    TaskRequest::~TaskRequest() {}

}  //  namespace k2
