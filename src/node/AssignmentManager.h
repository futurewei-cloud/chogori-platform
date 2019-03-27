#pragma once

#include "Tasks.h"
#include "NodeConfig.h"
#include "common/Status.h"
#include "NodePool.h"


namespace k2
{
//
//  Node partition manager (single per Node)
//
class AssignmentManager
{
protected:
    NodePool& pool;

    std::array<std::pair<PartitionId, std::unique_ptr<Partition>>, Constants::MaxCountOfPartitionsPerNode> partitions;  //  List of currently assigned partitions
    int partitionCount = 0; //  Count of currently assigned partitions

    Partition* getPartition(PartitionId id)
    {
        for(int i = 0; i < partitionCount; i++)
        {
            auto& kvp = partitions[i];
            if(kvp.first == id)
                return kvp.second.get();
        }

        return nullptr;
    }

    bool hasPartition(PartitionId id)
    {
        return getPartition(id) != nullptr;
    }

    Status processPartitionAssignmentMessage(PartitionRequest& request)
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
            std::make_unique<Partition>(std::move(assignMessage.partitionMetadata), *collection, assignMessage.partitionVersion));
        Partition& partition = *partitions[partitionCount-1].second;

        partition.createAndActivateTask<AssignmentTask>(std::move(request.client));

        return Status::Ok;
    }

    Status getAndCheckPartition(std::unique_ptr<PartitionMessage>& message, Partition*& partition)
    {
        partition = getPartition(message->getPartition().id);
        if(!partition)
            return LOG_ERROR(Status::NodeNotServicePartition);

        if(partition->getVersion() != message->getPartition().version)
            return LOG_ERROR(Status::PartitionVersionMismatch);

        return Status::Ok;
    }

    Status processPartitionOffloadMessage(PartitionRequest& request)
    {
        Partition* partition;
        RIF(getAndCheckPartition(request.message, partition));

        partition->state = Partition::State::Offloading;
        partition->createAndActivateTask<OffloadTask>(std::move(request.client));

        return Status::Ok;
    }

    Status processClientRequestMessage(PartitionRequest& request)
    {
        Partition* partition;
        RIF(getAndCheckPartition(request.message, partition));

        partition->createAndActivateTask<ClientTask>(std::move(request.client), std::move(request.message->getPayload()));

        return Status::Ok;
    }

    Status _processMessage(PartitionRequest& request)
    {
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

public:

    AssignmentManager(NodePool& pool) : pool(pool) {}

    void processMessage(PartitionRequest& request)
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

    void processTasks()
    {
        if(partitionCount == 0)
            return;

        std::chrono::nanoseconds maxIterationTime = NodeConfig::getTaskProcessingIterationMaxExecutionTime();
        TimeTracker iterationTracker(maxIterationTime);

        std::chrono::nanoseconds remainingTime;
        while((remainingTime = iterationTracker.remaining()) > std::chrono::nanoseconds::zero())
        {
            std::chrono::nanoseconds maxPartitionTime = remainingTime/partitionCount;
            for(int i = 0; i < partitionCount; i++)
            {
                if(!partitions[i].second->processActiveTasks(maxPartitionTime))
                {
                    //  Partition was dropped
                    std::move(partitions.begin()+i+1, partitions.begin()+partitionCount, partitions.begin()+i);
                    i--;
                    partitionCount--;
                }
            }
        }
    }
};

}   //  namespace k2
