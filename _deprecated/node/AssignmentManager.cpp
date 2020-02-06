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
        return Status::S422_Unprocessable_Entity("Partition cannot be assigned to the shard due exceeding the limit of partitions");

    if(hasPartition(request.message->getPartition().id))
        return Status::S422_Unprocessable_Entity("Attempt to assign partition which is already assigned");

    AssignmentMessage assignMessage;
    request.message->getPayload().seek(0);
    if(!request.message->getPayload().read(assignMessage)) {  //  TODO: allocate under partition memory arena
        K2ERROR("Unable to parse message");
        return Status::S400_Bad_Request("Unable to parse request");
    }
    Collection* collection = nullptr;
    RET_IF_BAD(pool.internalizeCollection(std::move(assignMessage.collectionMetadata), collection));

    partitions[partitionCount++] = std::make_pair(
        request.message->getPartition().id,
        std::make_unique<Partition>(pool, std::move(assignMessage.partitionMetadata), *collection, assignMessage.partitionVersion));
    Partition& partition = *partitions[partitionCount-1].second;

    partition.createAndActivateTask<AssignmentTask>(std::move(request.client));

    return Status::S200_OK();
}

Status AssignmentManager::getAndCheckPartition(std::unique_ptr<PartitionMessage>& message, Partition*& partition)
{
    partition = getPartition(message->getPartition().id);
    if(!partition)
        return Status::S416_Range_Not_Satisfiable("Request for partition that is not served by current Node");

    if(partition->getVersion() != message->getPartition().version)
        return Status::S416_Range_Not_Satisfiable("Partition version in request doesn't match served partition version");

    return Status::S200_OK();
}

Status AssignmentManager::processPartitionOffloadMessage(PartitionRequest& request)
{
    Partition* partition;
    RET_IF_BAD(getAndCheckPartition(request.message, partition));

    partition->state = Partition::State::Offloading;
    partition->createAndActivateTask<OffloadTask>(std::move(request.client));

    return Status::S200_OK();
}

Status AssignmentManager::processClientRequestMessage(PartitionRequest& request)
{
    Partition* partition;
    RET_IF_BAD(getAndCheckPartition(request.message, partition));

    partition->createAndActivateTask<ClientTask>(std::move(request.client), std::move(request.message->getPayload()));

    return Status::S200_OK();
}

Status AssignmentManager::_processMessage(PartitionRequest& request)
{
    if(pool.getMonitor().getState() == PoolMonitor::State::waitingForInitialization)
        return Status::S503_Service_Unavailable("Node pool has not been yet initialized");

    switch(request.message->getMessageType())
    {
        case MessageType::PartitionAssign:
            return processPartitionAssignmentMessage(request);

        case MessageType::PartitionOffload:
            return processPartitionOffloadMessage(request);

        case MessageType::ClientRequest:
            return processClientRequestMessage(request);

        default:
            return Status::S404_Not_Found("Unknown message type");
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
    if(status.is2xxOK())
        return;

    //  Some failure
    if(request.message && request.client)
        request.client->sendResponse(status);
    else
    {
        K2ERROR("no message or no client");
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
