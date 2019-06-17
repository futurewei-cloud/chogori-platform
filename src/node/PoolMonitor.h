#pragma once

#include "NodePool.h"
#include <thread>
#include <common/PartitionManagerMessage.h>

namespace k2
{
//
//  Describe current node pool API accessible for tasks
//
class PoolMonitor
{
public:
    enum class State
    {
        waitingForInitialization,   //  Monitor is trying to connect to Partition Manager. All other work should be blocked at this point
        disabled,                   //  Monitor is not used, so any action is currently allowed
        active,                     //  Connection with PartitionManage is estable and operations are allowed
        failure                     //  Everybody observing such state should crash immediately
    };

    class TimePoint
    {
    protected:
        std::chrono::system_clock::time_point systemTime;
        std::chrono::steady_clock::time_point steadyTime;
    public:
        const std::chrono::system_clock::time_point& getSystem() const { return systemTime; }
        const std::chrono::steady_clock::time_point& getSteady() const { return steadyTime; }

        TimePoint() {}

        static TimePoint now()
        {
            TimePoint result;
            result.systemTime = std::chrono::system_clock::now();
            result.steadyTime = std::chrono::steady_clock::now();
            return result;
        }
    };
protected:
    INodePool& pool;
    State state = State::waitingForInitialization;
    std::thread monitorThread;

    TimePoint lastHeartbeat;

    long sessionId = 0;

    manager::PoolInfo poolInfo;

    void run();
    void sendHeartbeat();
    void registerNodePool();


    template<typename RequestT, typename ResponseT>
    Status sendMessage(const RequestT& request, ResponseT& response);

public:
    PoolMonitor(INodePool& pool) : pool(pool)
    {
        state = pool.getConfig().isMonitorEnabled() ? State::waitingForInitialization : State::disabled;
    }

    void start()
    {
        if(!pool.getConfig().isMonitorEnabled())
            return;

        monitorThread = std::thread([this]() { run(); });
    }

    const TimePoint& getLastHeartbeatTime() const { return lastHeartbeat; }

    State getState() const
    {
        ASSERT(state != State::failure);
        return state;
    }
};

}   //  namespace k2
