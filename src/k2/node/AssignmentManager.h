#pragma once

#include <k2/transport/Status.h>
#include <k2/common/Log.h>
#include <k2/k2types/Constants.h>
#include "NodeConfig.h"
#include "NodePool.h"
#include "PoolMonitor.h"
#include "Tasks.h"

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

    TimePoint lastTaskProcessingCall ;
    TimePoint lastPoolMonitorSessionCheckTime;
    TimePoint lastPoolMonitorSessionCheckValue;

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
