#include <type_traits>
#include <iostream>

#include <k2/indexer/HOTIndexer.h>
#include <k2/indexer/MapIndexer.h>
#include <k2/indexer/UnorderedMapIndexer.h>
#include <k2/modules/memkv/server/MemKVModule.h>
#include <k2/node/AssignmentManager.h>
#include <k2/node/NodePool.h>
#include <k2/node/NodePoolImpl.h>

#include <catch2/catch.hpp>

#define REQUIRE_OK(status_pair) REQUIRE(checkStatus(status_pair,k2::Status::S200_OK()))
#define REQUIRE_NODENOTSERVICEPARTITION(status_pair) REQUIRE(checkStatus(status_pair, k2::Status::S416_Range_Not_Satisfiable()))

#define REQUIRE_VALUE(status_pair, value) REQUIRE(checkValue(status_pair,value))

using namespace k2;
using namespace std;


template<typename V>
bool checkStatus(const std::pair<Status, V>& st, Status status) { return st.first.code == status.code; }

bool checkStatus(Status actual, Status expected) { return actual.code == expected.code; }

bool checkValue(const std::pair<Status, String>& st, String str) { return st.first.is2xxOK() && st.second == str; }

Payload newPayload() {
    return Payload([]() {
        return Binary(1000);
    });
}

class FakeTransport
{
protected:
    AssignmentManager& assignmentManager;

    struct FakeConnectionState
    {
        bool responded = false;
        std::unique_ptr<ResponseMessage> message = std::make_unique<ResponseMessage>(
            "fake_endpoint:1234", newPayload(), ResponseMessage::Header());
    };

    class FakeClientConnection : public IClientConnection
    {
    public:
        FakeConnectionState& state;

        FakeClientConnection(FakeConnectionState& state) : state(state) { }

        Payload& getResponsePayload() override
        {
            return state.message->payload;
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

    std::pair<Status, uint64_t> set(PartitionAssignmentId partitionId, String key, String value)
    {
        typename MemKVModule<DerivedIndexer>::SetRequest setRequest { std::move(key), std::move(value) };
        auto result = transport.send(MemKVModule<DerivedIndexer>::createMessage(setRequest, partitionId, newPayload()));
        if (!result->getStatus().is2xxOK())
            return {result->getStatus(), 0};

        typename MemKVModule<DerivedIndexer>::SetResponse setResponse;
        result->payload.seek(0);
        result->payload.read(setResponse);
        return {result->getStatus(), setResponse.version};
    }

    std::pair<Status, String>  get(PartitionAssignmentId partitionId, String key)
    {
        typename MemKVModule<DerivedIndexer>::GetRequest getRequest { std::move(key), std::numeric_limits<uint64_t>::max() };
        auto result = transport.send(MemKVModule<DerivedIndexer>::createMessage(getRequest, partitionId, newPayload()));
        if(!result->getStatus().is2xxOK())
            return { result->getStatus(), {} };
        if(result->getModuleCode() != ErrorCode::None)
            return { Status::S500_Internal_Server_Error(), {} };

        typename MemKVModule<DerivedIndexer>::GetResponse getResponse;
        result->payload.seek(0);
        result->payload.read(getResponse);
        return { result->getStatus(), getResponse.value};
    }

    std::pair<Status, String> remove(PartitionAssignmentId partitionId, String key)
    {
        typename MemKVModule<DerivedIndexer>::DeleteRequest deleteRequest { std::move(key) };
        auto result = transport.send(MemKVModule<DerivedIndexer>::createMessage(deleteRequest, partitionId, newPayload()));
        return {result->getStatus(), {}};
    }
};

TEMPLATE_TEST_CASE("Single Partitions Assignment/Offload", "[SinglePartitions_Assignment/Offload]", HOTIndexer, MapIndexer, UnorderedMapIndexer)
{
    NodePoolImpl pool;
    pool.registerModule(ModuleId::Default, std::make_unique<MemKVModule<TestType>>());

    AssignmentManager assignmentManager(pool);
    FakeTransport transport(assignmentManager);

    const CollectionId collectionId = 3;
    const PartitionId partitionId = 10;
    const PartitionVersion partitionVersion = {101, 313};

    AssignmentMessage assignmentMessage;
    assignmentMessage.collectionMetadata = CollectionMetadata(collectionId, ModuleId::Default, {});
    assignmentMessage.partitionMetadata = PartitionMetadata(partitionId, PartitionRange("A", "C"), collectionId);
    assignmentMessage.partitionVersion = partitionVersion;

    REQUIRE_OK(transport.send(assignmentMessage.createMessage(Endpoint("1"), newPayload()))->getStatus());
    PartitionAssignmentId assignmentId(partitionId, partitionVersion);
    MemKVClient<TestType> client(transport);

    SECTION("Partition Assign: client KV set and get")
    {
        REQUIRE_OK(client.set(assignmentId, "Arjan", "Xeka"));
        REQUIRE_OK(client.set(assignmentId, "Ivan", "Avramov"));
        REQUIRE_OK(client.set(assignmentId, "Valentin", "Kuznetsov"));

        REQUIRE_VALUE(client.get(assignmentId, "Arjan"), "Xeka");
        REQUIRE_VALUE(client.get(assignmentId, "Ivan"), "Avramov");
        REQUIRE_VALUE(client.get(assignmentId, "Valentin"), "Kuznetsov");

        REQUIRE_OK(client.remove(assignmentId, "Arjan"));
        REQUIRE_OK(client.remove(assignmentId, "Ivan"));
        REQUIRE_OK(client.remove(assignmentId, "Valentin"));
    }

    REQUIRE_OK(transport.send(OffloadMessage::createMessage(Endpoint("1"), assignmentMessage.getPartitionAssignmentId(), newPayload()))->getStatus());

    SECTION("Partition Offload: client KV get and set")
    {
        REQUIRE_NODENOTSERVICEPARTITION(client.get(assignmentId, "Arjan"));
        REQUIRE_NODENOTSERVICEPARTITION(client.get(assignmentId, "Ivan"));

        REQUIRE_NODENOTSERVICEPARTITION(client.set(assignmentId, "Arjan", "Xeka"));
        REQUIRE_NODENOTSERVICEPARTITION(client.set(assignmentId, "Ivan", "Avramov"));
    }
}


TEMPLATE_TEST_CASE("Multiple Partitions Assignment/Offload", "[MultiplePartitions_Assignment/Offload]", HOTIndexer, MapIndexer, UnorderedMapIndexer)
{
    NodePoolImpl pool;
    pool.registerModule(ModuleId::Default, std::make_unique<MemKVModule<TestType>>());

    AssignmentManager assignmentManager(pool);
    FakeTransport transport(assignmentManager);

    const CollectionId collectionId = 10;
    const vector<PartitionId> partitionIds{10, 20, 30, 40, 50} ;
    const vector<PartitionVersion> partitionVersions{ {101, 313},{201, 313},{301, 313},{401, 313},{501, 313} };

    int num = partitionIds.size();
    if(num > Constants::MaxCountOfPartitionsPerNode) num=Constants::MaxCountOfPartitionsPerNode;

    vector<AssignmentMessage> assignmentMessages;
    vector<PartitionAssignmentId> assignmentIds;

    for(int i=0; i<num; i++)
    {
        AssignmentMessage assignmentMessage;
        assignmentMessage.collectionMetadata = CollectionMetadata(collectionId, ModuleId::Default, {});
        assignmentMessage.partitionMetadata = PartitionMetadata(partitionIds[i], PartitionRange("A", "C"), collectionId);
        assignmentMessage.partitionVersion = partitionVersions[i];
        assignmentMessages.push_back(std::move(assignmentMessage));
        assignmentIds.push_back(PartitionAssignmentId(partitionIds[i], partitionVersions[i]));
    }

    vector<int> ids(num, 0);
    for(int i=0; i<num; i++)
        ids[i] = i;

    auto rng = std::default_random_engine {};
    std::shuffle(std::begin(ids), std::end(ids), rng);

    for(int i=0; i<num; i++)
    {
        auto resp = transport.send(assignmentMessages[ids[i]].createMessage(Endpoint("1"), newPayload()));
        REQUIRE(resp != nullptr);
        REQUIRE_OK(resp->getStatus());
    }

    MemKVClient<TestType> client(transport);

    SECTION("Partition Assign: client KV set and get")
    {
        for(int i=0; i<num; i++)
        {
            String key{"Key_"+to_string(i)};
            String value{"Value_"+to_string(i)};
            REQUIRE_OK(client.set(assignmentIds[i], key, value));
            REQUIRE_VALUE(client.get(assignmentIds[i], key), value);
        }
    }

    SECTION("Partition Offload: client KV get and set")
    {
        for(int i=0; i<num; i++)
        {
            REQUIRE_OK(transport.send(OffloadMessage::createMessage(Endpoint("1"), assignmentMessages[i].getPartitionAssignmentId(), newPayload()))->getStatus());
            for(int j=0; j<=i; j++)
            {
                String section{"Partition Offload:"+to_string(i)+to_string(j)};
                DYNAMIC_SECTION( section )
                {
                    String key{"Key1_"+to_string(i)+to_string(j)};
                    String value{"Value1_"+to_string(i)+to_string(j)};
                    REQUIRE_NODENOTSERVICEPARTITION(client.set(assignmentIds[j], key, value));
                    REQUIRE_NODENOTSERVICEPARTITION(client.get(assignmentIds[j], key));
                }
            }
            for(int j=i+1; j<num; j++)
            {
                String section{"Partition Assign:"+to_string(i)+to_string(j)};
                DYNAMIC_SECTION( section )
                {
                    String key{"Key1_"+to_string(i)+to_string(j)};
                    String value{"Value1_"+to_string(i)+to_string(j)};
                    REQUIRE_OK(client.set(assignmentIds[j], key, value));
                    REQUIRE_VALUE(client.get(assignmentIds[j], key), value);
                }
            }
        }
    }
}
