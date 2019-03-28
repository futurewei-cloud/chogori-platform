#pragma once

#include <node/Module.h>
#include <node/Tasks.h>

namespace k2
{

//  Simple in-memory KV-store module
class MemKVModule : public IModule
{
protected:
    struct Node
    {                
        uint64_t version;
        String value;
        std::unique_ptr<Node> next;
    };

    static const uint32_t defaultKeepVersionCount = 0;

    typedef std::map<String, std::unique_ptr<Node>> MemTable;

    class PartitionContext
    {
    public:
        MemTable memTable;
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

    MemTable& getMemtable(TaskRequest& task)
    {
        return getPartitionContext(task)->memTable;
    }

    enum ErrorCode
    {
        None = 0,
        ParingError,
        RequestUnknown,
        NoSuchKey
    };

public:

    enum RequestType : uint8_t
    {
        Get = 0,
        Set
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

    template<typename T>
    static bool writeRequest(PayloadWriter& writer, const T& request) { return writer.write(T::getType()) && writer.write(request); }

    template<typename T>
    static std::unique_ptr<PartitionMessage> createMessage(const T& request, PartitionAssignmentId partitionId)
    {
        Payload payload;
        PayloadWriter writer = payload.getWriter();
        //PartitionMessage::Header header;
        //if(!writer.getContiguousStructure())
        if(!writeRequest(writer, request))
            return nullptr;

        return std::make_unique<PartitionMessage>(MessageType::ClientRequest, partitionId, Endpoint(""), std::move(payload));
    }

    //
    //  Called when partition get assigned
    //
    ModuleResponse onAssign(AssignmentTask& task) override
    {
        assert(task.getPartition().moduleData == nullptr);
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
        const Payload& payload = task.getRequestPayload();
        PayloadReader reader = payload.getReader();
        PartitionContext* context = getPartitionContext(task);

        RequestType requestType;
        MemKVModule_PARSE_RIF(reader.read(requestType));

        MemTable& memTable = context->memTable;

        switch(requestType)
        {
            case RequestType::Get:
            {
                GetRequest request;
                MemKVModule_PARSE_RIF(reader.read(request));
                task.releaseRequestPayload();                

                auto it = memTable.find(request.key);
                if(it == memTable.end())
                    return ModuleResponse(ModuleResponse::ReturnToClient, ErrorCode::NoSuchKey);

                Node* node = it->second.get();
                while(node && node->version > request.snapshotId)
                    node = node->next.get();

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

                std::unique_ptr<Node> newNode(new Node);
                newNode->value = std::move(request.value);
                uint64_t version = getPartitionContext(task)->getNewVersion();
                newNode->version = version;

                auto emplaceResult = memTable.try_emplace(std::move(request.key), std::move(newNode));
                if(!emplaceResult.second)    //  Value already exists
                {
                    newNode->next = std::move(emplaceResult.first->second);
                    emplaceResult.first->second = std::move(newNode);
                }

                task.getResponseWriter().write(version);

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
        return ModuleResponse(ModuleResponse::Ok);
    }

    //
    //  Called after OnPrepare stage is done (Module returned Ok and all IOs are finished). On this stage Module can
    //  apply it's transaction to update in memory representation or release locks.
    //
    ModuleResponse onApply(ClientTask& task) override
    {
        return ModuleResponse(ModuleResponse::Ok);
    }

    //
    //  Called when Module requests some maintainence jobs (e.g. snapshoting).
    //
    ModuleResponse onMaintainence(MaintainenceTask& task) override
    {
        return ModuleResponse::Ok;
    }
};

}   //  namespace k2
