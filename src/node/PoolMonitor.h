#pragma once

#include "NodePool.h"
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

    struct TimePoint
    {
        std::chrono::system_clock::time_point systemTime;
        std::chrono::steady_clock::time_point steadyTime;
        static TimePoint now();
    };
protected:
    INodePool& pool;
    std::vector<String> nodeTCPHostAndPorts; //  TCP endpoints for each node that Partition Manager will use for comunications
    std::vector<String> nodeRegistrationIds; //  Ids which Partition Manager assigned to nodes
    State state = State::waitingForInitialization;

    TimePoint lastHeartbeat;

    long sessionId = 0;

    void run();
    void sendHeartbeat();
    void registerNodePool();


    template<typename RequestT, typename ResponseT>
    Status sendMessage(const RequestT& request, ResponseT& response);

public:
    PoolMonitor(INodePool& pool);

    void start();

    const TimePoint& getLastHeartbeatTime() const;

    State getState() const;
};

}   //  namespace k2
