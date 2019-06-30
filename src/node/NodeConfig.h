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
    bool rdmaEnabled = false;
    std::vector<String> partitionManagerSet;
    std::string cpuSetStr;
    std::string cpuSetGeneralStr;

    std::chrono::nanoseconds getTaskProcessingIterationMaxExecutionTime() const { return std::chrono::nanoseconds(10000); }

    std::chrono::microseconds getMonitorSleepTime() const { return std::chrono::milliseconds(5); }

    std::chrono::microseconds getNoHeartbeatGracefullPeriod() const { return std::chrono::milliseconds(30); }

    bool isMonitorEnabled() const { return monitorEnabled; }
    bool isRDMAEnabled() const { return rdmaEnabled; }

    const std::vector<String> getPartitionManagerSet() const { return partitionManagerSet; }
    const std::string& getCpuSetString() const { return cpuSetStr; }
    const std::string& getCpuSetGeneralString() const { return cpuSetGeneralStr; }
};

}   //  namespace k2
