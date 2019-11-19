#pragma once

#include "Tasks.h"
#include "NodeConfig.h"
#include "common/Status.h"
#include "NodePool.h"
#include "PoolMonitor.h"
#include "common/Log.h"

namespace k2
{
//
//  Node partition manager (single per Node)
//
class AssignmentManager
{
protected:
    INodePool& pool;
    std::array<std::pair<PartitionId, std::unique_ptr<Partition>>, Constants::MaxCountOfPartitionsPerNode> partitions;  //  List of currently assigned partitions
    int partitionCount = 0;             //  Count of currently assigned partitions
    uint32_t processedTaskRound = 0;    //  How many rounds was processed from the beginning. Used to track whether mananager is stall.

    Partition* getPartition(PartitionId id);

    bool hasPartition(PartitionId id) { return getPartition(id) != nullptr; }

    Status getAndCheckPartition(std::unique_ptr<PartitionMessage>& message, Partition*& partition);

    Status processPartitionAssignmentMessage(PartitionRequest& request);
    Status processPartitionOffloadMessage(PartitionRequest& request);
    Status processClientRequestMessage(PartitionRequest& request);

    Status _processMessage(PartitionRequest& request);

    std::chrono::steady_clock::time_point lastTaskProcessingCall ;
    std::chrono::steady_clock::time_point lastPoolMonitorSessionCheckTime;
    std::chrono::steady_clock::time_point lastPoolMonitorSessionCheckValue;

    void checkStateConsistency();
    void updateLiveness();

public:

    AssignmentManager(INodePool& pool) : pool(pool) {}

    void processMessage(PartitionRequest& request);

    bool isAlive() const { return true; }

    bool processTasks();    //  Returns true if some work was done, false otherwise

    const INodePool& getNodePool() { return pool; }
};

}   //  namespace k2
