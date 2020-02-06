#pragma once

#include "Module.h"

// stl
#include <map>
#include <vector>
#include <string>

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

    Duration getTaskProcessingIterationMaxExecutionTime() const { return 10000ns; }

    Duration getMonitorSleepTime() const { return 5ms; }

    Duration getNoHeartbeatGracefullPeriod() const { return 30ms; }

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
