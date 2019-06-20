// std
#include <iostream>
// k2:client
#include <client/lib/Client.h>

#include "node/module/MemKVModule.h"

using namespace k2;
using namespace k2::client;
using namespace std::chrono_literals;

static void makeSetMessage(k2::Payload& payload, std::string& key, std::string& value)
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

class MockClient: public Client
{
public:
    MockClient()
    {

        PartitionDescription desc;
        PartitionAssignmentId id;
        id.parse("1.1.1");
        desc.nodeEndpoint = "tcp+k2rpc://127.0.0.1:11311";
        desc.id = id;
        PartitionMapRange partitionRange;
        partitionRange.lowKey = "d";
        partitionRange.highKey = "f";
        desc.range = partitionRange;
        _partitionMap.map.insert(desc);

        desc.nodeEndpoint = "tcp+k2rpc://127.0.0.1:12345";
        id.parse("2.1.1");
        desc.id = id;
        partitionRange.lowKey = "f";
        partitionRange.highKey = "";
        desc.range = partitionRange;
        _partitionMap.map.insert(desc);
    }
};


std::unique_ptr<OperationResult> invokeOperation(Client& client, Operation&& operation)
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
        K2INFO("callback invoked!");
        ASSERT(result._responses[0].status == Status::Ok);
        pOperationResult = std::move(std::make_unique<OperationResult>(std::move(result)));
        conditional.notify_one();
    });

    // wait until the payload is created
    conditional.wait_for(lock, 10s);
    ASSERT(callbackInvoked);

    return std::move(pOperationResult);
}

int main()
{
    MockClient client;
    // initialize the client
    ClientSettings settings;
    settings.userInitThread = false;
    settings.networkProtocol = "tcp+k2rpc";
    client.init(settings);

    Range range = Range::close("d", "f");
    srand(time(0));
    std::string value = std::to_string(rand());
    std::string key = "key";

    // set key
    // create payload: the range is required since we need to find the endpoint; different type of endpoint have different payloads
    Payload payload = std::move(client.createPayload());
    makeSetMessage(payload, key, value);
    // execute operation: the Operation needs to be updated such that there's a separate payload for each range
    Operation setOperation;
    client::Message setMessage;
    setMessage.content = std::move(payload);
    setMessage.ranges.push_back(range);
    setOperation.messages.push_back(std::move(setMessage));
    invokeOperation(client, std::move(setOperation));

    // get the key
    payload = std::move(client.createPayload());
    makeGetMessage(payload);
    Operation getOperation;
    client::Message getMessage;
    getMessage.content = std::move(payload);
    getMessage.ranges.push_back(range);
    getOperation.messages.push_back(std::move(getMessage));
    auto result = std::move(invokeOperation(client, std::move(getOperation)));
    ASSERT(result->_responses.size() == 2)
    ASSERT(0 == result->_responses[0].moduleCode);
    // extract value
    MemKVModule<>::GetResponse getResponse;
    result->_responses[0].payload.getReader().read(getResponse);
    K2INFO("received value: " << getResponse.value << " for key: " << key);
    ASSERT(value==getResponse.value);

    return 0;
}
