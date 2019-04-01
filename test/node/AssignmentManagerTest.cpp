#include <type_traits>
#include <iostream>

#include <node/AssignmentManager.h>
#include <node/MapMemtable.h>
#include <node/HOTMemtable.h>
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

        for(int i = 0; i < 1000; i++)
        {
            assignmentManager.processTasks();
            if(connectionState.responded)
                return std::move(connectionState.message);
        }

        return nullptr;
    }
};

template <typename DerivedMemtable>
class MemKVClient
{
    FakeTransport& transport;
public:
    MemKVClient(FakeTransport& transport) : transport(transport) { }

    uint64_t set(PartitionAssignmentId partitionId, String key, String value)
    {
        typename MemKVModule<DerivedMemtable>::SetRequest setRequest { std::move(key), std::move(value) };
        auto result = transport.send(MemKVModule<DerivedMemtable>::createMessage(setRequest, partitionId));
        assert(result->getStatus() == Status::Ok);

        typename MemKVModule<DerivedMemtable>::SetResponse setResponse;
        result->payload.getReader().read(setResponse);
        return setResponse.version;
    }

    String get(PartitionAssignmentId partitionId, String key)
    {
        typename MemKVModule<DerivedMemtable>::GetRequest getRequest { std::move(key), std::numeric_limits<uint64_t>::max() };
        auto result = transport.send(MemKVModule<DerivedMemtable>::createMessage(getRequest, partitionId));
        assert(result->getStatus() == Status::Ok);

        typename MemKVModule<DerivedMemtable>::GetResponse getResponse;
        result->payload.getReader().read(getResponse);
        return getResponse.value;
    }
};

TEST_CASE("Map Based Indexer Module Assignment Message", "[MapBasedIndexerModuleAssignment]")
{
    NodePool pool;
    pool.registerModule(ModuleId::Default, std::make_unique<MemKVModule<MapMemtable>>());

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
        MemKVClient client(transport);
        client.set(assignmentId, "Arjan", "Xeka");
        client.set(assignmentId, "Ivan", "Avramov");

        REQUIRE(client.get(assignmentId, "Arjan") == "Xeka");
        REQUIRE(client.get(assignmentId, "Ivan") == "Avramov");
    }
}

TEST_CASE("HOT Based Indexer Module Assignment Message", "[HOTBasedIndexerModuleAssignment]")
{
    NodePool pool;
    pool.registerModule(ModuleId::Default, std::make_unique<MemKVModule<HOTMemtable>>());

    AssignmentManager assignmentManager(pool);
    FakeTransport transport(assignmentManager);

    const CollectionId collectionId = 4;
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
    	MemKVClient<HOTMemtable> client(transport);
    	client.set(assignmentId, "Xiangjun", "Shi");
    	client.set(assignmentId, "Quan", "Zhang");

    	assert(client.get(assignmentId, "Xiangjun") == "Shi");
    	assert(client.get(assignmentId, "Quan") == "Zhang");
    }
}
