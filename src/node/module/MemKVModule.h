#pragma once

#include "../Module.h"
#include "../MemtableInterface.h"

namespace k2
{
//  Simple in-memory KV-store module
template <typename DerivedMemtable>
class MemKVModule : public IModule
{
    static const uint32_t defaultKeepVersionCount = 0;

    class PartitionContext
    {
    public:
        MemtableInterface<DerivedMemtable> memTable;
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

    MemtableInterface<DerivedMemtable>& getMemtable(TaskRequest& task)
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

    enum RequestType : uint8_t
    {
        Get = 0,
        Set
    };

public:
    //
    //  Called when partition get assigned
    //
    ModuleRespone OnAssign(AssignmentTask& task) override
    {
        assert(task.getPartition().moduleData == nullptr);
        task.getPartition().moduleData = new PartitionContext();
        return ModuleRespone::Ok;
    }

    //
    //  Called when partition get offloaded
    //
    ModuleRespone OnOffload(OffloadTask& task) override
    {
        delete getPartitionContext(task);
        task.getPartition().moduleData = nullptr;
        return ModuleRespone::Ok;
    }

#define MemKVModule_PARSE_RIF(res) { if(!res) return ModuleRespone(ModuleRespone::Error, ErrorCode::ParingError); }

    //
    //  Called when client request is received. In this function Module can check whether operation can be completed,
    //  set shared state (if there are some) and log the transaction record through ioOperations.
    //  Module can respond with:
    //      Ok: To wait for IO finished and move to the next stage (either Coordinate or Apply)
    //      Error: To cancel the task with provided error code
    //      Postpone: To reschedule the task for some later time
    //      RescheduleAfterIOCompletion: To wait while all IOs are done and schedule task again
    //
    ModuleRespone OnPrepare(ClientTask& task, IOOperations& ioOperations) override
    {
        const Payload& payload = task.getRequestPayload();
        PayloadReader reader = payload.getReader();
        PartitionContext* context = getPartitionContext(task);

        uint8_t requestType;
        MemKVModule_PARSE_RIF(reader.read(requestType));

        MemtableInterface<DerivedMemtable>& memTable = context->memTable;

        switch(requestType)
        {
            case RequestType::Get:
            {
                String key;
                MemKVModule_PARSE_RIF(reader.read(key));

                Node* node = memTable.find(key);
                if(node == nullptr)
                    return ModuleRespone(ModuleRespone::ReturnToClient, ErrorCode::NoSuchKey);

                uint64_t snapshotId;
                MemKVModule_PARSE_RIF(reader.read(snapshotId));

                task.releaseRequestPayload();

                while(node && node->version < snapshotId)
                    node = node->next.get();

                task.getResponseWriter().write(node->version);
                task.getResponseWriter().write(node->value);

                return ModuleRespone(ModuleRespone::ReturnToClient, ErrorCode::None);
            }

            case RequestType::Set:
            {
                String key;
                MemKVModule_PARSE_RIF(reader.read(key));

                String value;
                MemKVModule_PARSE_RIF(reader.read(value));

                task.releaseRequestPayload();

                std::unique_ptr<Node> newNode = memTable.insert(key, value, getPartitionContext(task)->getNewVersion());

                task.getResponseWriter().write(newNode->version);

                return ModuleRespone(ModuleRespone::ReturnToClient, ErrorCode::None);
            }

            default:
                return ModuleRespone(ModuleRespone::ReturnToClient, ErrorCode::RequestUnknown);
        }
    }

    //
    //  Called either for for distributed transactions after responses from all participants have been received. Module
    //  can aggregate shared state and make a decision on whether to proceed with transaction.
    //
    ModuleRespone OnCoordinate(ClientTask& task, SharedState& remoteSharedState, IOOperations& ioOperations) override
    {
        return ModuleRespone(ModuleRespone::Ok);
    }

    //
    //  Called after OnPrepare stage is done (Module returned Ok and all IOs are finished). On this stage Module can
    //  apply it's transaction to update in memory representation or release locks.
    //
    ModuleRespone OnApply(ClientTask& task) override
    {
        return ModuleRespone(ModuleRespone::Ok);
    }

    //
    //  Called when Module requests some maintainence jobs (e.g. snapshoting).
    //
    ModuleRespone OnMaintainence(MaintainenceTask& task) override
    {
        return ModuleRespone::Ok;
    }
};

}   //  namespace k2
