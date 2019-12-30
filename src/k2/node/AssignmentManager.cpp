#include "AssignmentManager.h"
#include <k2/common/TimeMeasure.h>
#include <k2/common/Log.h>

namespace k2
{

Partition* AssignmentManager::getPartition(PartitionId id) {
    for (int i = 0; i < partitionCount; i++) {
        auto& kvp = partitions[i];
        if (kvp.first == id)
            return kvp.second.get();
    }

    return nullptr;
}

Status AssignmentManager::processPartitionAssignmentMessage(PartitionRequest& request)
{
    if(partitionCount >= Constants::MaxCountOfPartitionsPerNode)
        return Status::TooManyPartitionsAlready;

    if(hasPartition(request.message->getPartition().id))
        return Status::PartitionAlreadyAssigned;

    AssignmentMessage assignMessage;
    if(!request.message->getPayload().getReader().read(assignMessage))  //  TODO: allocate under partition memory arena
        return Status::MessageParsingError;

    Collection* collection = nullptr;
    RET_IF_BAD(pool.internalizeCollection(std::move(assignMessage.collectionMetadata), collection));

    partitions[partitionCount++] = std::make_pair(
        request.message->getPartition().id,
        std::make_unique<Partition>(pool, std::move(assignMessage.partitionMetadata), *collection, assignMessage.partitionVersion));
    Partition& partition = *partitions[partitionCount-1].second;

    partition.createAndActivateTask<AssignmentTask>(std::move(request.client));

    return Status::Ok;
}

Status AssignmentManager::getAndCheckPartition(std::unique_ptr<PartitionMessage>& message, Partition*& partition)
{
    partition = getPartition(message->getPartition().id);
    if(!partition)
        return Status::NodeNotServicePartition;

    if(partition->getVersion() != message->getPartition().version)
        return Status::PartitionVersionMismatch;

    return Status::Ok;
}

Status AssignmentManager::processPartitionOffloadMessage(PartitionRequest& request)
{
    Partition* partition;
    RET_IF_BAD(getAndCheckPartition(request.message, partition));

    partition->state = Partition::State::Offloading;
    partition->createAndActivateTask<OffloadTask>(std::move(request.client));

    return Status::Ok;
}

Status AssignmentManager::processClientRequestMessage(PartitionRequest& request)
{
    Partition* partition;
    RET_IF_BAD(getAndCheckPartition(request.message, partition));

    partition->createAndActivateTask<ClientTask>(std::move(request.client), std::move(request.message->getPayload()));

    return Status::Ok;
}

Status AssignmentManager::_processMessage(PartitionRequest& request)
{
    if(pool.getMonitor().getState() == PoolMonitor::State::waitingForInitialization)
        return Status::NodePoolHasNotYetBeenInitialized;

    switch(request.message->getMessageType())
    {
        case MessageType::PartitionAssign:
            return processPartitionAssignmentMessage(request);

        case MessageType::PartitionOffload:
            return processPartitionOffloadMessage(request);

        case MessageType::ClientRequest:
            return processClientRequestMessage(request);

        default:
            return Status::UnkownMessageType;
    }
}

void AssignmentManager::checkStateConsistency()
{
    if(pool.getMonitor().getState() == PoolMonitor::State::disabled)
        return;

    TimePoint currentTime = Clock::now();
    TimePoint heartbeatTime = pool.getMonitor().getLastHeartbeatTime();
    if(heartbeatTime > lastPoolMonitorSessionCheckValue)
    {
        lastPoolMonitorSessionCheckValue = heartbeatTime;
        lastPoolMonitorSessionCheckTime = Clock::now();
    }
    else
        assert(currentTime - lastPoolMonitorSessionCheckTime < pool.getConfig().getNoHeartbeatGracefullPeriod());
}

void AssignmentManager::updateLiveness()
{
    processedTaskRound++;
    checkStateConsistency();
}

void AssignmentManager::processMessage(PartitionRequest& request)
{
    Status status = _processMessage(request);
    if(status == Status::Ok)
        return;

    //  Some failure
    if(request.message && request.client)
        request.client->sendResponse(status);
    else
    {
        //  TODO: log error here
    }
}

bool AssignmentManager::processTasks()
{
    updateLiveness();

    if(!partitionCount)
        return false;

    Duration maxIterationTime = pool.getConfig().getTaskProcessingIterationMaxExecutionTime();
    TimeTracker iterationTracker(maxIterationTime);

    Duration remainingTime;
    bool workDone = false;
    while(partitionCount && (remainingTime = iterationTracker.remaining()) > Duration::zero())
    {
        bool hadActiveTasks = false;

        Duration maxPartitionTime = remainingTime/partitionCount;
        for(int i = 0; i < partitionCount; i++)
        {
            checkStateConsistency();

            if(!partitions[i].second->haveTasksToRun())
                continue;

            workDone = hadActiveTasks = true;

            if(!partitions[i].second->processActiveTasks(maxPartitionTime))
            {
                //  Partition was dropped
                std::move(partitions.begin()+i+1, partitions.begin()+partitionCount, partitions.begin()+i);
                i--;
                partitionCount--;
            }
        }

        if(!hadActiveTasks) //  If haven't found any tasks just skip the look
            break;
    }

    return workDone;
}

}   //  namespace k2
