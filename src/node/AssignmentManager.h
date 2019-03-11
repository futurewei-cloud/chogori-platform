#pragma once

#include "TaskRequest.h"
#include "NodeConfig.h"
#include "../common/Status.h"


namespace k2
{
//
//  Node partition manager (single per Node)
//
class AssignmentManager
{
protected:
    std::array<std::pair<PartitionId, std::unique_ptr<Partition>>, Constants::MaxCountOfPartitionsPerNode> partitions;  //  List of currently assigned partitions
    int partitionCount = 0; //  Count of currently assigned partitions

    typedef boost::intrusive::list<
        TaskRequest,
        boost::intrusive::member_hook<TaskRequest, boost::intrusive::list_member_hook<>, &TaskRequest::linkedListHook>
    > TaskList;

    std::array<TaskList, Constants::MaxCountOfPartitionsPerNode> partitionTasks;  //  List of currently assigned partitions

    Status errorResponse(std::unique_ptr<ReceivedMessage>& message, Status status)
    {
        //  TODO: Log error
        message->client->errorResponse(status);
        return status;
    }

    bool hasPartition(PartitionId id)
    {
        for(int i = 0; i < partitionCount; i++)
        {
            if(partitions[i].first == id)
                return true;
        }

        return false;        
    }

    Status processPartitionAssignmentMessage(std::unique_ptr<ReceivedMessage> message)
    {
        if(partitionCount >= Constants::MaxCountOfPartitionsPerNode)
            return errorResponse(message, Status::TooManyPartitionsAlready);

        if(hasPartition(message->getPartition().id))
            return errorResponse(message, Status::PartitionAlreadyAssigned);

        AssignMessage assignMessage;
        if(!assignMessage.parse(message->getPayload()))  //  TODO: allocate under partition memory arena
            return errorResponse(message, Status::MessageParsingError);           

        return Status::Ok;
    }

    void processPartitionOffloadMessage(std::unique_ptr<ReceivedMessage> message)
    {
        //  TODO
    }

    void processClientRequestMessage(std::unique_ptr<ReceivedMessage> message)
    {
        //  TODO
    }

public:
    void processMessage(std::unique_ptr<ReceivedMessage> message)
    {
        switch(message->getMessageType())
        {
            case MessageType::PartitionAssign:
            {
                processPartitionAssignmentMessage(std::move(message));
                break;
            }

            case MessageType::PartitionOffload:
            {
                processPartitionOffloadMessage(std::move(message));
                break;
            }
            
            case MessageType::ClientRequest:
            {
                processClientRequestMessage(std::move(message));
                break;
            }
        }
    }

    void processTasks()
    {
        if(partitionCount == 0)
            return;

        std::chrono::nanoseconds maxIterationTime = NodeConfig::getTaskProcessingIterationMaxExecutionTime();                
        TimeTracker iterationTracker(maxIterationTime);
/*
        std::chrono::nanoseconds remainingTime;
        while((remainingTime = iterationTracker.remaining()) > 0)
        {
            std::chrono::nanoseconds maxPartitionTime = remainingTime/partitionCount;
            for(int i = 0; i < partitionCount; i++)
            {                
                TimeTracker partitionTracker(maxPartitionTime);

                for(auto it : partitionTasks)
                {
                    TaskRequest* task = &*it;
                    if((remainingTime = partitionTracker.remaining()) > 0)
                        break;

                    task->setAllowedExecutionTime(remainingTime);
                    switch(task->getType())
                    {
                        case TaskType::Maintainence:
                        {
                            NodeConfig::getModule()->OnMaintainence(*(MaintainenceTask*)task);
                            break;
                        }

                        //
                        //  TODO: implement other tasks
                        //
                        default:
                            TERMINATE("Unexpected task type");
                            break;                        
                    }
                }
            }
        }*/
    }
};

}   //  namespace k2
