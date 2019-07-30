#pragma once

// k2
#include <node/Module.h>
// k2:config
#include "NodeEndpointConfig.h"

namespace k2
{

//
//  Static configuration of the node. TODO: loading, etc.
//
class NodePoolConfig
{
friend class ConfigLoader;
friend class Config;

protected:
    std::unordered_map<std::string, k2_shared_ptr<NodeEndpointConfig>> nodes;

public:
    bool monitorEnabled = false;
    bool rdmaEnabled = false;
    bool hugePagesEnabled = false;
    std::string id;
    std::vector<String> partitionManagerSet;
    std::string cpuSetStr;
    std::string cpuSetGeneralStr;
    std::string rdmaNicId;
    std::string memorySizeStr;

    NodePoolConfig() { }

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

    void addNode(k2_shared_ptr<NodeEndpointConfig> pNodeConfig)
    {
        std::string id = (pNodeConfig->_id).empty() ? std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) : pNodeConfig->_id;
        auto pair = std::make_pair<std::string, k2_shared_ptr<NodeEndpointConfig>>(std::move(id), std::move(pNodeConfig));
        ASSERT(nodes.insert(std::move(pair)).second);
    }

    std::vector<k2_shared_ptr<NodeEndpointConfig>> getNodes() const
    {
        std::vector<k2_shared_ptr<NodeEndpointConfig>> vector;

        for(auto entry : nodes) {
            vector.push_back(entry.second);
        }

        return std::move(vector);
    }

    std::vector<const char *> toArgv() const
    {
        std::vector<const char *> argv;
        if(!cpuSetStr.empty()) {
             argv.push_back("--cpuset");
	         argv.push_back(cpuSetStr.c_str());
        }
        if(!memorySizeStr.empty()) {
            argv.push_back("-m");
	        argv.push_back(memorySizeStr.c_str());
        }
        if(hugePagesEnabled) {
            argv.push_back("--hugepages");
        }
        if(rdmaEnabled && !rdmaNicId.empty()) {
            argv.push_back("--rdma");
	        argv.push_back(rdmaNicId.c_str());
        }

        return std::move(argv);
    }

}; // class NodePoolConfig

}; //  namespace k2
