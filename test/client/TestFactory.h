#pragma once

// k2
#include <common/PartitionMetadata.h>
// k2:client
#include <client/Client.h>
#include "modules/memkv/server/MemKVModule.h"

using namespace k2;
using namespace k2::client;

class TestFactory
{
public:
    static void makeSetMessage(k2::Payload& payload, const std::string& key, const std::string& value)
    {
         MemKVModule<>::SetRequest setRequest { key, value };
         auto request = MemKVModule<>::RequestWithType(setRequest);
         payload.getWriter().write(request);
    }

    static void makeGetMessage(k2::Payload& payload)
    {
         MemKVModule<>::GetRequest getRequest { "key", std::numeric_limits<uint64_t>::max() };
         auto request = MemKVModule<>::RequestWithType(getRequest);
         payload.getWriter().write(request);
    }

    static void makePartitionPayload(k2::Payload& payload, const std::string& id, const k2::PartitionRange& range, MessageType messageType)
    {
        using namespace k2;

        k2::CollectionId collectionId = 3;
        k2::PartitionAssignmentId partitionId;
        partitionId.parse(id.c_str());

        k2::AssignmentMessage assignmentMessage;
        assignmentMessage.collectionMetadata = k2::CollectionMetadata(collectionId, k2::ModuleId::Default, {});
        assignmentMessage.partitionMetadata = k2::PartitionMetadata(partitionId.id, k2::PartitionRange(range), collectionId);
        assignmentMessage.partitionVersion = partitionId.version;

        PartitionMessage::Header* header;
        bool ret = payload.getWriter().reserveContiguousStructure(header);
        assert(ret);
        header->messageType = messageType;
        header->partition = partitionId;
        payload.getWriter().write(assignmentMessage);
        header->messageSize = payload.getSize() - txconstants::MAX_HEADER_SIZE - sizeof(PartitionMessage::Header);
    }

    static void offloadPartitionAsync(Executor& rExecutor, const std::string& partition, const k2::PartitionRange& range, std::function<void()> callback)
    {
        rExecutor.execute("tcp+k2rpc://127.0.0.1:11311", std::chrono::seconds(4),
        [&rExecutor, range, callback, partition] (k2::Payload& payload) {
            TestFactory::makePartitionPayload(payload, partition, k2::PartitionRange(range), MessageType::PartitionOffload);
        },
        [&rExecutor, range, callback, partition] (std::unique_ptr<ResponseMessage> response) {
            K2INFO("offloaded partition:" << partition << "; " << k2::getStatusText(response->status));

            callback();
        });
    }

    static void assignPartitionAsync(Executor& rExecutor, const std::string& url, const std::string& partition, const k2::PartitionRange& range, std::function<void()> callback)
    {
        rExecutor.execute(url, std::chrono::seconds(4),
        [&rExecutor, range, callback, partition] (k2::Payload& payload) {
            TestFactory::makePartitionPayload(payload, partition, k2::PartitionRange(range), MessageType::PartitionAssign);
        },
        [&rExecutor, range, callback, partition] (std::unique_ptr<ResponseMessage> response) {
            K2INFO("assigned partition:" << partition << "; " << k2::getStatusText(response->status));

            callback();
        });
    }


    static void offloadAndAssignPartitionAsync(Executor& rExecutor, const std::string& url, const std::string& partition, const k2::PartitionRange& range, std::function<void()> callback)
    {
        rExecutor.execute(url, std::chrono::seconds(4),
        [&rExecutor, url, range, callback, partition] (k2::Payload& payload) {
            TestFactory::makePartitionPayload(payload, partition, k2::PartitionRange(range), MessageType::PartitionOffload);
        },
        [&rExecutor, url, range, callback, partition] (std::unique_ptr<ResponseMessage> response) {
            (void)response;
            rExecutor.execute(url, std::chrono::seconds(4),
            [&rExecutor, range, callback, partition] (k2::Payload& payload) {
                TestFactory::makePartitionPayload(payload, partition, k2::PartitionRange(range), MessageType::PartitionAssign);
            },
            [&rExecutor, range, callback, partition] (std::unique_ptr<ResponseMessage> response) {
                K2INFO("assigned :" << partition << "; " << k2::getStatusText(response->status));
                callback();
            });
        });
    }

    static std::unique_ptr<OperationResult> invokeOperationSync(IClient& client, Operation&& operation)
    {
       bool callbackInvoked = false;
       std::mutex mutex;
       std::condition_variable conditional;
       std::unique_lock<std::mutex> lock(mutex);
       std::unique_ptr<OperationResult> pOperationResult;
       client.execute(std::move(operation), [&](IClient& client, OperationResult&& result) {
           // prevent compilation warnings
           (void)client;
           callbackInvoked = true;
           K2INFO("callback invoked: " << getStatusText(result._responses[0].status));
           ASSERT(result._responses[0].status == Status::Ok);
           pOperationResult = std::move(std::make_unique<OperationResult>(std::move(result)));
           conditional.notify_one();
       });

        // wait until the payload is created
        conditional.wait_for(lock, std::chrono::seconds(10));
        ASSERT(callbackInvoked);

        return pOperationResult;
    }

    static PartitionDescription createPartitionDescription(const std::string& endpointUrl, const std::string& partitionId, const PartitionRange& range) {
        PartitionDescription desc;
        PartitionAssignmentId id;
        id.parse(partitionId.c_str());
        desc.nodeEndpoint = endpointUrl;
        desc.id = id;
        desc.range = std::move(PartitionRange(range));

        return std::move(desc);
    }

    static Operation createOperation(const client::Range& rRange, Payload&& rrPayload)
    {
        Operation operation;
        client::Message message;
        message.content = std::move(rrPayload);
        message.ranges.push_back(rRange);
        operation.messages.push_back(std::move(message));

        return std::move(operation);
    }
};

class TestClient: public Client
{
public:
    TestClient()
    {
         // empty
    }

    TestClient(std::vector<PartitionDescription>& partitions)
    {
        for(PartitionDescription partition: partitions) {
            _partitionMap.map.insert(partition);
        }
    }

    Executor& getExecutor()
    {
        return _executor;
    }
};

class MockClient: public k2::client::Client
{
    virtual void init(const k2::client::ClientSettings& settings)
    {
        // used to ignore compilation warning
        (void)settings;
    }

    virtual void execute(k2::client::Operation&& settings, std::function<void(IClient&, k2::client::OperationResult&&)> onCompleted)
    {
        (void)settings.transactional;
        (void)onCompleted;
    }

    virtual void execute(PartitionDescription& partition, std::function<void(Payload&)> onPayload, std::function<void(IClient&, OperationResult&&)> onCompleted)
    {
        (void)partition;
        (void)onPayload;
        (void)onCompleted;
    }

    virtual void runInThreadPool(std::function<void(IClient&)> routine)
    {
        (void)routine;
    }

    virtual k2::Payload createPayload()
    {
        return std::move(Payload());
    }

    virtual void createPayload(std::function<void(IClient&, Payload&&)> onCompleted)
    {
        (void)onCompleted;
    }
};
