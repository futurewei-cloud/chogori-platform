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
    bool hugePagesEnabled = false;
    std::vector<String> partitionManagerSet;
    std::string cpuSetStr;
    std::string cpuSetGeneralStr;
    std::string rdmaNicId;
    std::string memorySizeStr;

    std::chrono::nanoseconds getTaskProcessingIterationMaxExecutionTime() const { return std::chrono::nanoseconds(10000); }

    std::chrono::microseconds getMonitorSleepTime() const { return std::chrono::milliseconds(5); }

    std::chrono::microseconds getNoHeartbeatGracefullPeriod() const { return std::chrono::milliseconds(30); }

    bool isMonitorEnabled() const { return monitorEnabled; }
    bool isRDMAEnabled() const { return rdmaEnabled; }
    bool isHugePagesEnabled() const { return hugePagesEnabled; }

    const std::vector<String> getPartitionManagerSet() const { return partitionManagerSet; }
    const std::string& getCpuSetString() const { return cpuSetStr; }
    const std::string& getCpuSetGeneralString() const { return cpuSetGeneralStr; }
    const std::string& getRdmaNicId() const { return rdmaNicId; }
    const std::string& getMemorySizeString() const { return memorySizeStr; }
};

}   //  namespace k2
