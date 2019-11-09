#pragma once

// boost
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/lockfree/queue.hpp>
// k2:client
#include "ExecutorTask.h"

namespace k2
{

class ExecutorQueue
{
private:
    // No copy
    ExecutorQueue(const ExecutorQueue& q)
    {
        (void)q;
    }

public:
    // TODO: this is arbitrary defined
    static constexpr int _MAX_QUEUE_SIZE = 10;

    // queue of promises to be fullfilled
    boost::lockfree::spsc_queue<seastar::lw_shared_ptr<seastar::promise<ExecutorTaskPtr>>> _promises{_MAX_QUEUE_SIZE};
    // shared members between the Client thread and the Seastar platform
    boost::lockfree::spsc_queue<ExecutorTaskPtr> _readyTasks{_MAX_QUEUE_SIZE}; // tasks that are ready to be executed
    boost::lockfree::spsc_queue<ExecutorTaskPtr> _completedTasks{_MAX_QUEUE_SIZE}; // tasks that have completed execution
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

        return nullptr;
    }

    //
    // Returns a future which will be fullfilled once a task is available in the queue
    //
    seastar::future<ExecutorTaskPtr> popWithFuture()
    {
        ExecutorTaskPtr pTask = pop();
        if(!pTask) {
            auto pPromise = seastar::make_lw_shared<seastar::promise<ExecutorTaskPtr>>();
            if(_promises.push(pPromise)) {

                return pPromise->get_future();
            }
        }

        return seastar::make_ready_future<ExecutorTaskPtr>(pTask);
    }

    //
    // Push the client data to a ready task. If there are no ready tasks, the client data will be released since the request cannot be fulfilled.
    //
    bool push(std::unique_ptr<ExecutorTask::ClientData> pClientData) {
        ExecutorTaskPtr pTask;
        bool ret = false;
        if(_completedTasks.pop(pTask)) {
            pTask->_pClientData = std::move(pClientData);

            seastar::lw_shared_ptr<seastar::promise<ExecutorTaskPtr>> pPromise;
            if(_promises.pop(pPromise)) { // we have a promise to fulfill
                pPromise->set_value(pTask);
            }
            else {
                ret = _readyTasks.push(pTask);
                ASSERT(ret);
            }
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
