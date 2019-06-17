#pragma once

#include "Module.h"

namespace k2
{

//
//  Static configuration of the node. TODO: loading, etc.
//
class NodePoolConfig
{
public:
    bool monitorEnabled = false;
    std::vector<String> partitionManagerSet;

    std::chrono::nanoseconds getTaskProcessingIterationMaxExecutionTime() const { return std::chrono::nanoseconds(10000); }

    std::chrono::microseconds getMonitorSleepTime() const { return std::chrono::milliseconds(5); }

    std::chrono::microseconds getNoHeartbeatGracefullPeriod() const { return std::chrono::milliseconds(30); }

    bool isMonitorEnabled() const { return monitorEnabled; }

    const std::vector<String> getPartitionManagerSet() const { return partitionManagerSet; }
};

}   //  namespace k2
