#pragma once

// boost
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/lockfree/queue.hpp>
// k2:client
#include "ExecutorTask.h"

namespace k2
{

struct ExecutorQueue
{
    // TODO: this is arbitrary defined
    static constexpr int _MAX_QUEUE_SIZE = 10;

    // shared members between the Client thread and the Seastar platform
    boost::lockfree::spsc_queue<ExecutorTaskPtr> _readyTasks{_MAX_QUEUE_SIZE}; // tasks that are ready to be executed
    boost::lockfree::spsc_queue<ExecutorTaskPtr> _completedTasks{_MAX_QUEUE_SIZE}; // tasks that have completed execution
    // the mutex here is not used for synchronization, but to prevent the threads from spinning and wasting resources
    std::mutex _mutex;
    std::condition_variable _conditional;
    std::vector<std::unique_ptr<ExecutorTask::ClientData>> _clientData;

    ExecutorQueue()
    {
        // create all the task objects
        for(int i = 0; i < _MAX_QUEUE_SIZE; i++) {
            _completedTasks.push(seastar::make_lw_shared<ExecutorTask>());
        }
    }

    ~ExecutorQueue()
    {
        // empty
    }

    ExecutorTaskPtr pop()
    {
        ExecutorTaskPtr pTask;
        if(_readyTasks.pop(pTask)) {

            return pTask;
        }

        conditionallyWait();

        return nullptr;
    }

    //
    // Push the client data to a ready task. If there are no ready tasks, the client data will be released since the request cannot be fulfilled.
    //
    bool push(std::unique_ptr<ExecutorTask::ClientData> pClientData) {
        ExecutorTaskPtr pTask;
        bool ret = false;
        if(_completedTasks.pop(pTask)) {
            pTask->_pClientData = std::move(pClientData);
            ret = _readyTasks.push(pTask);
            ASSERT(ret);
            notifyAll();
        }

        return ret;
    }

    void completeTask(ExecutorTaskPtr pTask)
    {
        bool ret = _completedTasks.push(pTask);
        ASSERT(ret);
    }

    bool empty()
    {
        return _readyTasks.empty();
    }

    void conditionallyWait()
    {
        std::unique_lock<std::mutex> lock(_mutex);
        // TODO: 1 millisecond is arbitrarily chosen
       _conditional.wait_for(lock, std::chrono::milliseconds(1));
    }

    void notifyAll()
    {
        _conditional.notify_all();
    }

    void collectClientData()
    {
        collectClientData(_readyTasks);
        collectClientData(_completedTasks);
    }

    void collectClientData(boost::lockfree::spsc_queue<ExecutorTaskPtr>& tasks)
    {
        ExecutorTaskPtr pTask;
        while(tasks.pop(pTask)) {
           _clientData.push_back(std::move(pTask->_pClientData));
        }
    }
};

}; // namespace k2
