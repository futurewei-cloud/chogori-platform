#include <type_traits>
#include <iostream>

#include <node/AssignmentManager.h>
#include <node/MapIndexer.h>
#include <node/UnorderedMapIndexer.h>
#include <node/HOTIndexer.h>
#include <node/module/MemKVModule.h>
#include <node/NodePool.h>

#include "catch2/catch.hpp" 

using namespace k2; 

class FakeTransport
{
protected:
    AssignmentManager& assignmentManager;

    struct FakeConnectionState
    {
        bool responded = false;
        std::unique_ptr<ResponseMessage> message = std::make_unique<ResponseMessage>();
    };

    class FakeClientConnection : public IClientConnection
    {
    public:
        FakeConnectionState& state;

        FakeClientConnection(FakeConnectionState& state) : state(state) { }

        PayloadWriter getResponseWriter() override
        {
            return state.message->payload.getWriter();
        }

        void sendResponse(Status status, uint32_t code) override
        {
            state.responded = true;
            state.message->status = status;
            state.message->moduleCode = code;
        }
    };

public:
    FakeTransport(AssignmentManager& assignmentManager) : assignmentManager(assignmentManager) { }

    std::unique_ptr<ResponseMessage> send(std::unique_ptr<PartitionMessage>&& message)
    {
        FakeConnectionState connectionState;

        PartitionRequest request { std::move(message), std::make_unique<FakeClientConnection>(connectionState) };
        assignmentManager.processMessage(request);

        // TODO: Use count tracker instead in the processTasks to avoid flaky test
        for(int i = 0; i < 100000; i++)
        {
            assignmentManager.processTasks();
            if(connectionState.responded)
                return std::move(connectionState.message);
        }

        return nullptr;
    }
};

template <typename DerivedIndexer>
class MemKVClient
{
    FakeTransport& transport;
public:
    MemKVClient(FakeTransport& transport) : transport(transport) { }

    uint64_t set(PartitionAssignmentId partitionId, String key, String value)
    {
        typename MemKVModule<DerivedIndexer>::SetRequest setRequest { std::move(key), std::move(value) };
        auto result = transport.send(MemKVModule<DerivedIndexer>::createMessage(setRequest, partitionId));
        assert(result->getStatus() == Status::Ok);

        typename MemKVModule<DerivedIndexer>::SetResponse setResponse;
        result->payload.getReader().read(setResponse);
        return setResponse.version;
    }

    String get(PartitionAssignmentId partitionId, String key)
    {
        typename MemKVModule<DerivedIndexer>::GetRequest getRequest { std::move(key), std::numeric_limits<uint64_t>::max() };
        auto result = transport.send(MemKVModule<DerivedIndexer>::createMessage(getRequest, partitionId));
        assert(result->getStatus() == Status::Ok);

        typename MemKVModule<DerivedIndexer>::GetResponse getResponse;
        result->payload.getReader().read(getResponse);
        return getResponse.value;
    }

    void remove(PartitionAssignmentId partitionId, String key)
    {
        typename MemKVModule<DerivedIndexer>::DeleteRequest deleteRequest { std::move(key) };
        auto result = transport.send(MemKVModule<DerivedIndexer>::createMessage(deleteRequest, partitionId));
        assert(result->getStatus() == Status::Ok);
    }
};

TEST_CASE("Ordered Map Based Indexer Module Assignment Manager", "[OrderedMapBasedIndexerModuleAssignment]")
{
    NodePool pool;
    pool.registerModule(ModuleId::Default, std::make_unique<MemKVModule<MapIndexer>>());

    AssignmentManager assignmentManager(pool);
    FakeTransport transport(assignmentManager);

    const CollectionId collectionId = 3;
    const PartitionId partitionId = 10;
    const PartitionVersion partitionVersion = {101, 313};

    AssignmentMessage assignmentMessage;
    assignmentMessage.collectionMetadata = CollectionMetadata(collectionId, ModuleId::Default, {});
    assignmentMessage.partitionMetadata = PartitionMetadata(partitionId, PartitionRange("A", "C"), collectionId);
    assignmentMessage.partitionVersion = partitionVersion;

    REQUIRE(transport.send(assignmentMessage.createMessage(Endpoint("1")))->getStatus() == Status::Ok);

    SECTION("Module client KV set and get")
    {
        PartitionAssignmentId assignmentId(partitionId, partitionVersion);
        MemKVClient<MapIndexer> client(transport);
        client.set(assignmentId, "Arjan", "Xeka");
        client.set(assignmentId, "Ivan", "Avramov");

        REQUIRE(client.get(assignmentId, "Arjan") == "Xeka");
        REQUIRE(client.get(assignmentId, "Ivan") == "Avramov");

        client.remove(assignmentId, "Arjan");
        client.remove(assignmentId, "Ivan");
    }
}

TEST_CASE("Hash Table Based Indexer Module Assignment Manager", "[HashTableBasedIndexerModuleAssignment]")
{
    NodePool pool;
    pool.registerModule(ModuleId::Default, std::make_unique<MemKVModule<UnorderedMapIndexer>>());

    AssignmentManager assignmentManager(pool);
    FakeTransport transport(assignmentManager);

    const CollectionId collectionId = 4;
    const PartitionId partitionId = 10;
    const PartitionVersion partitionVersion = { 101, 313 };

    AssignmentMessage assignmentMessage;
    assignmentMessage.collectionMetadata = CollectionMetadata(collectionId, ModuleId::Default, {});
    assignmentMessage.partitionMetadata = PartitionMetadata(partitionId, PartitionRange("A", "C"), collectionId);
    assignmentMessage.partitionVersion = partitionVersion;
    assert(transport.send(assignmentMessage.createMessage(Endpoint("1")))->getStatus() == Status::Ok);

    PartitionAssignmentId assignmentId(partitionId, partitionVersion);

    SECTION("Module client KV set and get")
    {
        MemKVClient<UnorderedMapIndexer> client(transport);
        client.set(assignmentId, "Hao", "Feng");
        client.set(assignmentId, "Valentin", "Kuznetsov");

        REQUIRE(client.get(assignmentId, "Hao") == "Feng");
        REQUIRE(client.get(assignmentId, "Valentin") == "Kuznetsov");

        client.remove(assignmentId, "Hao");
        client.remove(assignmentId, "Valentin");
    }
}

TEST_CASE("HOT Based Indexer Module Assignment Manager", "[HOTBasedIndexerModuleAssignment]")
{
    NodePool pool;
    pool.registerModule(ModuleId::Default, std::make_unique<MemKVModule<HOTIndexer>>());

    AssignmentManager assignmentManager(pool);
    FakeTransport transport(assignmentManager);

    const CollectionId collectionId = 5;
    const PartitionId partitionId = 10;
    const PartitionVersion partitionVersion = { 101, 313 };

    AssignmentMessage assignmentMessage;
    assignmentMessage.collectionMetadata = CollectionMetadata(collectionId, ModuleId::Default, {});
    assignmentMessage.partitionMetadata = PartitionMetadata(partitionId, PartitionRange("A", "C"), collectionId);
    assignmentMessage.partitionVersion = partitionVersion;
    
    REQUIRE(transport.send(assignmentMessage.createMessage(Endpoint("1")))->getStatus() == Status::Ok);

    PartitionAssignmentId assignmentId(partitionId, partitionVersion);

    SECTION("Module client KV set and get")
    {
        MemKVClient<HOTIndexer> client(transport);
        client.set(assignmentId, "Xiangjun", "Shi");
        client.set(assignmentId, "Quan", "Zhang");

        REQUIRE(client.get(assignmentId, "Xiangjun") == "Shi");
        REQUIRE(client.get(assignmentId, "Quan") == "Zhang");

        client.remove(assignmentId, "Xiangjun");
        client.remove(assignmentId, "Quan");
    }
}
