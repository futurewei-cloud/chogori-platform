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

    INodePool& getNodePool();

    void respondToSender(Status status, uint32_t moduleCode = 0);

    ProcessResult moduleResponseToProcessResult(ModuleResponse response);

    IModule& getModule();

    IClientConnection& getClient();

    void awake();
public:
    MessageInitiatedTaskRequest(Partition& partition, std::unique_ptr<IClientConnection>&& connectionClient);
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

    void respondToSender(Status status, uint32_t moduleCode = 0);

    ProcessResult onPrepare();

    ProcessResult onCoordinate();

    ProcessResult onApply();

public:
    ClientTask(Partition& partition, std::unique_ptr<IClientConnection>&& client, Payload&& requestPayload);

    TaskType getType() const override;

    const Payload& getRequestPayload() const;

    PayloadWriter& getResponseWriter();

    ProcessResult process() override;

    void releaseRequestPayload();
};  //  class ClientTask


//
//  Task created as a response to Partition Manager Partition Assign command
//
class AssignmentTask : public MessageInitiatedTaskRequest
{
public:
    AssignmentTask(Partition& partition, std::unique_ptr<IClientConnection> client);

    TaskType getType() const override;

    ProcessResult process() override;
};


//
//  Task created as a response to Partition Manager Partition Offload command
//
class OffloadTask : public MessageInitiatedTaskRequest
{
public:
    OffloadTask(Partition& partition, std::unique_ptr<IClientConnection> client);

    TaskType getType() const override;

    ProcessResult process() override;
};


//
//  Task created as a response to request from client
//
class MaintainenceTask : public TaskRequest
{
public:
    MaintainenceTask(Partition& partition);

    TaskType getType() const override;
};  //  class ClientTask

}   //  namespace k2
