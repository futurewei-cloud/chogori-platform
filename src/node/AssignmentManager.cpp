#include "AssignmentManager.h"
#include "common/TimeMeasure.h"

namespace k2
{

Status AssignmentManager::processPartitionAssignmentMessage(PartitionRequest& request)
{
    if(partitionCount >= Constants::MaxCountOfPartitionsPerNode)
        return LOG_ERROR(Status::TooManyPartitionsAlready);

    if(hasPartition(request.message->getPartition().id))
        return LOG_ERROR(Status::PartitionAlreadyAssigned);

    AssignmentMessage assignMessage;
    if(!request.message->getPayload().getReader().read(assignMessage))  //  TODO: allocate under partition memory arena
        return LOG_ERROR(Status::MessageParsingError);

    Collection* collection = nullptr;
    RIF(pool.internalizeCollection(std::move(assignMessage.collectionMetadata), collection));

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
        return LOG_ERROR(Status::NodeNotServicePartition);

    if(partition->getVersion() != message->getPartition().version)
        return LOG_ERROR(Status::PartitionVersionMismatch);

    return Status::Ok;
}

Status AssignmentManager::processPartitionOffloadMessage(PartitionRequest& request)
{
    Partition* partition;
    RIF(getAndCheckPartition(request.message, partition));

    partition->state = Partition::State::Offloading;
    partition->createAndActivateTask<OffloadTask>(std::move(request.client));

    return Status::Ok;
}

Status AssignmentManager::processClientRequestMessage(PartitionRequest& request)
{
    Partition* partition;
    RIF(getAndCheckPartition(request.message, partition));

    partition->createAndActivateTask<ClientTask>(std::move(request.client), std::move(request.message->getPayload()));

    return Status::Ok;
}

Status AssignmentManager::_processMessage(PartitionRequest& request)
{
    if(pool.getMonitor().getState() == PoolMonitor::State::waitingForInitialization)
        return LOG_ERROR(Status::NodePoolHasNotYetBeenInitialized);

    switch(request.message->getMessageType())
    {
        case MessageType::PartitionAssign:
            return processPartitionAssignmentMessage(request);

        case MessageType::PartitionOffload:
            return processPartitionOffloadMessage(request);

        case MessageType::ClientRequest:
            return processClientRequestMessage(request);

        default:
            return LOG_ERROR(Status::UnkownMessageType);
    }
}

void AssignmentManager::checkStateConsistency()
{
    if(pool.getMonitor().getState() == PoolMonitor::State::disabled)
        return;

    std::chrono::steady_clock::time_point currentTime = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point heartbeatTime = pool.getMonitor().getLastHeartbeatTime().getSteady();
    if(heartbeatTime > lastPoolMonitorSessionCheckValue)
    {
        lastPoolMonitorSessionCheckValue = heartbeatTime;
        lastPoolMonitorSessionCheckTime = std::chrono::steady_clock::now();
    }
    else
        ASSERT(currentTime - lastPoolMonitorSessionCheckTime < pool.getConfig().getNoHeartbeatGracefullPeriod());
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

    std::chrono::nanoseconds maxIterationTime = pool.getConfig().getTaskProcessingIterationMaxExecutionTime();
    TimeTracker iterationTracker(maxIterationTime);

    std::chrono::nanoseconds remainingTime;
    bool workDone = false;
    while(partitionCount && (remainingTime = iterationTracker.remaining()) > std::chrono::nanoseconds::zero())
    {
        bool hadActiveTasks = false;

        std::chrono::nanoseconds maxPartitionTime = remainingTime/partitionCount;
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
