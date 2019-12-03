#pragma once

#include <k2/common/PartitionMessage.h>
#include <k2/indexer/IndexerInterface.h>
#include <k2/indexer/MapIndexer.h>
#include <k2/node/Module.h>
#include <k2/node/Tasks.h>

namespace k2
{
    enum ErrorCode
    {
        None = 0,
        ParingError,
        RequestUnknown,
        NoSuchKey
    };

//  Simple in-memory KV-store module
template <typename DerivedIndexer = MapIndexer>
class MemKVModule : public IModule
{
protected:
    static const uint32_t defaultKeepVersionCount = 0;

    class PartitionContext
    {
    public:
        DerivedIndexer memTable;
        uint64_t currentVersion = 0;
        uint64_t keepVersionCount = 1;

        uint64_t getNewVersion()
        {
            return ++currentVersion;
        }
    };

    PartitionContext* getPartitionContext(TaskRequest& task)
    {
        return (PartitionContext*)task.getPartition().moduleData;
    };

    DerivedIndexer& getIndexer(TaskRequest& task)
    {
        return getPartitionContext(task)->memTable;
    }

public:

    enum RequestType : uint8_t
    {
        Get = 0,
        Set,
        Delete
    };

    template<typename RequestT>
    class RequestWithType
    {
    public:
        RequestType type;
        RequestT& request;

        RequestWithType(RequestT& request) : type(RequestT::getType()), request(request) {}

        K2_PAYLOAD_FIELDS(type, request);
    };

    class GetRequest
    {
    public:
        static constexpr RequestType getType() { return RequestType::Get; }

        String key;
        uint64_t snapshotId;

        K2_PAYLOAD_FIELDS(key, snapshotId);
    };

    class GetResponse
    {
    public:
        String value;
        uint64_t version;

        K2_PAYLOAD_FIELDS(value, version);
    };

    class SetRequest
    {
    public:
        static constexpr RequestType getType() { return RequestType::Set; }

        String key;
        String value;

        K2_PAYLOAD_FIELDS(key, value);
    };

    class SetResponse
    {
    public:
        uint64_t version;

        K2_PAYLOAD_FIELDS(version);
    };

    class DeleteRequest
    {
    public:
        static constexpr RequestType getType() { return RequestType::Delete; }

        String key;

        K2_PAYLOAD_FIELDS(key);
    };

    template<typename T>
    static bool writeRequest(PayloadWriter& writer, const T& request) { return writer.write(T::getType()) && writer.write(request); }

    template<typename T>
    static std::unique_ptr<PartitionMessage> createMessage(const T& request, PartitionAssignmentId partitionId)
    {
        Payload payload;
        PayloadWriter writer = payload.getWriter();
        if(!writeRequest(writer, request))
            return nullptr;

        return std::make_unique<PartitionMessage>(MessageType::ClientRequest, partitionId, Endpoint(""), std::move(payload));
    }

    //
    //  Called when partition get assigned
    //
    ModuleResponse onAssign(AssignmentTask& task) override
    {
        ASSERT(task.getPartition().moduleData == nullptr);
        task.getPartition().moduleData = new PartitionContext();
        return ModuleResponse::Ok;
    }

    //
    //  Called when partition get offloaded
    //
    ModuleResponse onOffload(OffloadTask& task) override
    {
        delete getPartitionContext(task);
        task.getPartition().moduleData = nullptr;
        return ModuleResponse::Ok;
    }

#define MemKVModule_PARSE_RIF(res) { if(!res) return ModuleResponse(ModuleResponse::Error, ErrorCode::ParingError); }

    //
    //  Called when client request is received. In this function Module can check whether operation can be completed,
    //  set shared state (if there are some) and log the transaction record through ioOperations.
    //  Module can respond with:
    //      Ok: To wait for IO finished and move to the next stage (either Coordinate or Apply)
    //      Error: To cancel the task with provided error code
    //      Postpone: To reschedule the task for some later time
    //      RescheduleAfterIOCompletion: To wait while all IOs are done and schedule task again
    //
    ModuleResponse onPrepare(ClientTask& task, IOOperations& ioOperations) override
    {
        (void) ioOperations; // TODO use me
        const Payload& payload = task.getRequestPayload();
        PayloadReader reader = payload.getReader();
        PartitionContext* context = getPartitionContext(task);

        RequestType requestType;
        MemKVModule_PARSE_RIF(reader.read(requestType));

        DerivedIndexer& memTable = context->memTable;

        switch(requestType)
        {
            case RequestType::Get:
            {
                GetRequest request;
                MemKVModule_PARSE_RIF(reader.read(request));
                task.releaseRequestPayload();

                VersionedTreeNode* node = memTable.find(request.key, request.snapshotId);

                if(node == nullptr)
                    return ModuleResponse(ModuleResponse::ReturnToClient, ErrorCode::NoSuchKey);

                GetResponse response { node->value, node->version };
                task.getResponseWriter().write(response);

                return ModuleResponse(ModuleResponse::ReturnToClient, ErrorCode::None);
            }

            case RequestType::Set:
            {
                SetRequest request;
                MemKVModule_PARSE_RIF(reader.read(request));
                task.releaseRequestPayload();

                uint64_t version = getPartitionContext(task)->getNewVersion();
                memTable.insert(std::move(request.key), std::move(request.value), version);
                task.getResponseWriter().write(version);

                return ModuleResponse(ModuleResponse::ReturnToClient, ErrorCode::None);
            }

            case RequestType::Delete:
            {
                DeleteRequest request;
                MemKVModule_PARSE_RIF(reader.read(request));
                task.releaseRequestPayload();

                memTable.trim(request.key, std::numeric_limits<uint64_t>::max());

                return ModuleResponse(ModuleResponse::ReturnToClient, ErrorCode::None);
            }

            default:
                return ModuleResponse(ModuleResponse::ReturnToClient, ErrorCode::RequestUnknown);
        }
    }

    //
    //  Called either for for distributed transactions after responses from all participants have been received. Module
    //  can aggregate shared state and make a decision on whether to proceed with transaction.
    //
    ModuleResponse onCoordinate(ClientTask& task, SharedState& remoteSharedState, IOOperations& ioOperations) override
    {
        (void) task; // TODO  use me
        (void) remoteSharedState; // TODO use me
        (void) ioOperations; // TODO use me
        return ModuleResponse(ModuleResponse::Ok);
    }

    //
    //  Called after OnPrepare stage is done (Module returned Ok and all IOs are finished). On this stage Module can
    //  apply it's transaction to update in memory representation or release locks.
    //
    ModuleResponse onApply(ClientTask& task) override
    {
        (void) task; // TODO  use me
        return ModuleResponse(ModuleResponse::Ok);
    }

    //
    //  Called when Module requests some maintainence jobs (e.g. snapshoting).
    //
    ModuleResponse onMaintainence(MaintainenceTask& task) override
    {
        (void) task; // TODO  use me
        return ModuleResponse::Ok;
    }
};

}   //  namespace k2
