#pragma once

#include <boost/intrusive/list.hpp>
#include "Partition.h"
#include <node/NodePool.h>

namespace k2
{

//
//  Task request caused by some message obtained from Transport
//
class MessageInitiatedTaskRequest : public TaskRequest
{
protected:
    std::unique_ptr<IClientConnection> client;

    INodePool& getNodePool()
    {
        return getPartition().getNodePool();
    }

    void respondToSender(Status status, uint32_t moduleCode = 0)
    {
        client->sendResponse(status, moduleCode);
    }

    ProcessResult moduleResponseToProcessResult(ModuleResponse response)
    {
        switch (response.type)
        {
            case ModuleResponse::Ok:
                return ProcessResult::Done;

            case ModuleResponse::RescheduleAfterIOCompletion:
                return ProcessResult::Sleep;

            case ModuleResponse::Postpone:
            {
                if(response.postponeDelayUs)    //  If delay time if specified, let sleep for that time
                {
                    getNodePool().getScheduingPlatform().delay(std::chrono::microseconds(response.postponeDelayUs), [&]
                    {
                        //  TODO: fix case when task got cancelled before timer fired
                        awake();
                    });
                    return ProcessResult::Sleep;
                }

                return ProcessResult::Delay;    //  Just reschedule the task after all current tasks in the queue
            }

            default:
                assert(false);
                return ProcessResult::Done;
        }
    }

    IModule& getModule() { return partition.getModule(); }

    IClientConnection& getClient() { return *client.get(); }

    void awake() { partition.awakeTask(*this); }
public:
    MessageInitiatedTaskRequest(Partition& partition, std::unique_ptr<IClientConnection>&& connectionClient) :
        TaskRequest(partition),  client(std::move(connectionClient))
    {
        assert(client);
    }
};


//
//  Task created as a response to request from client
//
class ClientTask : public MessageInitiatedTaskRequest
{
protected:
    Payload requestPayload;
    PayloadWriter responseWriter;

    enum class State
    {
        Prepare = 0,
        Coordinate,
        Apply
    };

    State state;

    void respondToSender(Status status, uint32_t moduleCode = 0)
    {
        if(status != Status::Ok)
            getClient().getResponseWriter().truncateToCurrent();
        client->sendResponse(status, moduleCode);
    }

    ProcessResult onPrepare()
    {
        IOOperations ioOperations;

        ModuleResponse response = getModule().onPrepare(*this, ioOperations);
        switch(response.type)
        {
            case ModuleResponse::Ok: //  Can move to the next stage
            {
                state = State::Apply;
                return canContinue() ? onApply() : ProcessResult::Delay;
            }

            case ModuleResponse::ReturnToClient:
            {
                respondToSender(Status::Ok, response.resultCode);
                return ProcessResult::Done;
            }

            default:
                return moduleResponseToProcessResult(response);
        }
    }

    ProcessResult onCoordinate()
    {
        assert(false);  //  TODO: implement
        return ProcessResult::Done;
    }

    ProcessResult onApply()
    {
        assert(false);  //  TODO: implement
        return ProcessResult::Done;
    }

public:
    ClientTask(Partition& partition, std::unique_ptr<IClientConnection>&& client, Payload&& requestPayload) :
        MessageInitiatedTaskRequest(partition, std::move(client)), requestPayload(std::move(requestPayload)),
        responseWriter(getClient().getResponseWriter()), state(State::Prepare) {}

    TaskType getType() const override { return TaskType::ClientRequest; }

    const Payload& getRequestPayload() const { return requestPayload; }

    PayloadWriter& getResponseWriter() { return responseWriter; }

    ProcessResult process() override
    {
        switch (state)
        {
            case State::Prepare:
                return onPrepare();

            case State::Coordinate:
                return onCoordinate();

            case State::Apply:
                return onApply();

            default:
                assert(false);
                return ProcessResult::Done;
        }
    }

    void releaseRequestPayload()
    {
        requestPayload.clear();
    }
};  //  class ClientTask


//
//  Task created as a response to Partition Manager Partition Assign command
//
class AssignmentTask : public MessageInitiatedTaskRequest
{
public:
    AssignmentTask(Partition& partition, std::unique_ptr<IClientConnection> client) :
        MessageInitiatedTaskRequest(partition, std::move(client)) {}

    TaskType getType() const override { return TaskType::PartitionAssign; }

    ProcessResult process() override
    {
        if(partition.getState() == Partition::State::Offloading)
        {
            respondToSender(Status::AssignOperationIsBrokenByOffload);
            return ProcessResult::Done;
        }

        ModuleResponse response = getModule().onAssign(*this);
        if(response.type == ModuleResponse::Ok)
        {
            partition.transitionToRunningState();
            respondToSender(Status::Ok);
            return ProcessResult::Done;
        }

        return moduleResponseToProcessResult(response);
    }
};


//
//  Task created as a response to Partition Manager Partition Offload command
//
class OffloadTask : public MessageInitiatedTaskRequest
{
public:
    OffloadTask(Partition& partition, std::unique_ptr<IClientConnection> client) :
        MessageInitiatedTaskRequest(partition, std::move(client)) {}

    TaskType getType() const override { return TaskType::PartitionOffload; }

    ProcessResult process() override
    {
        ModuleResponse response = getModule().onOffload(*this);
        if(response.type == ModuleResponse::Ok)
        {
            respondToSender(Status::Ok);
            return ProcessResult::DropPartition;
        }

        return moduleResponseToProcessResult(response);
    }
};


//
//  Task created as a response to request from client
//
class MaintainenceTask : public TaskRequest
{
public:
    MaintainenceTask(Partition& partition) : TaskRequest(partition) {}

    TaskType getType() const override { return TaskType::Maintainence; }
};  //  class ClientTask

}   //  namespace k2
