#include "Tasks.h"

namespace k2 {

INodePool& MessageInitiatedTaskRequest::getNodePool() {
    return getPartition().getNodePool();
}

void MessageInitiatedTaskRequest::respondToSender(Status status, uint32_t moduleCode) {
    client->sendResponse(status, moduleCode);
}

TaskRequest::ProcessResult MessageInitiatedTaskRequest::moduleResponseToProcessResult(ModuleResponse response) {
    switch (response.type) {
        case ModuleResponse::Ok:
            return TaskRequest::ProcessResult::Done;

        case ModuleResponse::RescheduleAfterIOCompletion:
            return TaskRequest::ProcessResult::Sleep;

        case ModuleResponse::Postpone: {
            if (response.postponeDelayUs)  //  If delay time if specified, let sleep for that time
            {
                getNodePool().getSchedulingPlatform().delay(std::chrono::microseconds(response.postponeDelayUs), [&] {
                    //  TODO: fix case when task got cancelled before timer fired
                    awake();
                });
                return TaskRequest::ProcessResult::Sleep;
            }

            return TaskRequest::ProcessResult::Delay;  //  Just reschedule the task after all current tasks in the queue
        }

        default:
            assert(false);
            return TaskRequest::ProcessResult::Done;
    }
}

IModule& MessageInitiatedTaskRequest::getModule() { return partition.getModule(); }

IClientConnection& MessageInitiatedTaskRequest::getClient() { return *client.get(); }

void MessageInitiatedTaskRequest::awake() { partition.awakeTask(*this); }

MessageInitiatedTaskRequest::MessageInitiatedTaskRequest(Partition& partition, std::unique_ptr<IClientConnection>&& connectionClient) : TaskRequest(partition), client(std::move(connectionClient)) {
    assert(client);
}

void ClientTask::respondToSender(Status status, uint32_t moduleCode) {
    if (!status.is2xxOK())
        getSendPayload().truncateToCurrent();
    client->sendResponse(status, moduleCode);
}

TaskRequest::ProcessResult ClientTask::onPrepare() {
    IOOperations ioOperations;

    ModuleResponse response = getModule().onPrepare(*this, ioOperations);
    switch (response.type) {
        case ModuleResponse::Ok:  //  Can move to the next stage
        {
            state = State::Apply;
            return canContinue() ? onApply() : TaskRequest::ProcessResult::Delay;
        }

        case ModuleResponse::ReturnToClient: {
            respondToSender(Status::S200_OK(), response.resultCode);
            return TaskRequest::ProcessResult::Done;
        }

        default:
            return moduleResponseToProcessResult(response);
    }
}

TaskRequest::ProcessResult ClientTask::onCoordinate() {
    assert(false);  //  TODO: implement
    return TaskRequest::ProcessResult::Done;
}

TaskRequest::ProcessResult ClientTask::onApply() {
    assert(false);  //  TODO: implement
    return TaskRequest::ProcessResult::Done;
}

ClientTask::ClientTask(Partition& partition, std::unique_ptr<IClientConnection> client, Payload&& receivedPayload) :
  MessageInitiatedTaskRequest(partition, std::move(client)), receivedPayload(std::move(receivedPayload)), state(State::Prepare) {
}

TaskType ClientTask::getType() const { return TaskType::ClientRequest; }

Payload& ClientTask::getReceivedPayload() { return receivedPayload; }
Payload& ClientTask::getSendPayload() { return client->getResponsePayload(); }

TaskRequest::ProcessResult ClientTask::process() {
    switch (state) {
        case State::Prepare:
            return onPrepare();

        case State::Coordinate:
            return onCoordinate();

        case State::Apply:
            return onApply();

        default:
            assert(false);
            return TaskRequest::ProcessResult::Done;
    }
}

void ClientTask::releaseReceivedPayload() {
    receivedPayload.clear();
}

AssignmentTask::AssignmentTask(Partition& partition, std::unique_ptr<IClientConnection> client) : MessageInitiatedTaskRequest(partition, std::move(client)) {}

TaskType AssignmentTask::getType() const { return TaskType::PartitionAssign; }

TaskRequest::ProcessResult AssignmentTask::process() {
    if (partition.getState() == Partition::State::Offloading) {
        respondToSender(Status::S503_Service_Unavailable("Offload command received during assign process"));
        return TaskRequest::ProcessResult::Done;
    }

    ModuleResponse response = getModule().onAssign(*this);
    if (response.type == ModuleResponse::Ok) {
        partition.transitionToRunningState();
        respondToSender(Status::S200_OK());
        return TaskRequest::ProcessResult::Done;
    }

    return moduleResponseToProcessResult(response);
}


OffloadTask::OffloadTask(Partition& partition, std::unique_ptr<IClientConnection> client) : MessageInitiatedTaskRequest(partition, std::move(client)) {}

TaskType OffloadTask::getType() const { return TaskType::PartitionOffload; }

TaskRequest::ProcessResult OffloadTask::process() {
    ModuleResponse response = getModule().onOffload(*this);
    if (response.type == ModuleResponse::Ok) {
        respondToSender(Status::S200_OK());
        return TaskRequest::ProcessResult::DropPartition;
    }

    return moduleResponseToProcessResult(response);
}

MaintainenceTask::MaintainenceTask(Partition& partition) : TaskRequest(partition) {}

TaskType MaintainenceTask::getType() const { return TaskType::Maintainence; }

}  //  namespace k2
