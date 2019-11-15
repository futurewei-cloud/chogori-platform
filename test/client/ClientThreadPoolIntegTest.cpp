#define CATCH_CONFIG_MAIN
// std
#include <vector>
// catch
#include "catch2/catch.hpp"
// k2
#include <common/PartitionMetadata.h>
// k2:client
#include <client/lib/Client.h>
#include "modules/memkv/server/MemKVModule.h"
// test
#include "TestFactory.h"

using namespace k2;
using namespace k2::client;

SCENARIO("Client in a thread pool")
{
    std::string endpointUrl = "tcp+k2rpc://127.0.0.1:11311";
    std::vector<PartitionDescription> partitions;
    partitions.push_back(std::move(TestFactory::createPartitionDescription(endpointUrl, "1.1.1", PartitionRange("a", "d"))));
    partitions.push_back(std::move(TestFactory::createPartitionDescription(endpointUrl, "2.1.1", PartitionRange("d", "g"))));
    partitions.push_back(std::move(TestFactory::createPartitionDescription(endpointUrl, "3.1.1", PartitionRange("g", "j"))));
    partitions.push_back(std::move(TestFactory::createPartitionDescription(endpointUrl, "4.1.1", PartitionRange("j", "")))); // empty high key terminates the range
    TestClient client(partitions);
    ClientSettings settings;
    settings.networkProtocol = "tcp+k2rpc";
    Executor& rExecutor = client.getExecutor();

    std::string partitionId = "2.1.1";
    Range range = Range::close("d", "f");
    settings.userInitThread = false;
    client.init(settings);

    std::mutex _mutex;
    std::unique_lock<std::mutex> _lock(_mutex);
    std::condition_variable _conditional;

    // create partitions
    TestFactory::assignPartitionAsync(rExecutor, endpointUrl, partitionId, PartitionRange("d", "g"),
    [&] {
        _conditional.notify_all();
    });
    // wait for the partition to be created
    _conditional.wait_for(_lock, std::chrono::seconds(5));

    srand(time(0));
    std::string value = std::to_string(rand());
    std::string key = "key";

    // set key
    // create payload: the range is required since we need to find the endpoint; different type of endpoint have different payloads
    Payload payload = std::move(client.createPayload());
    TestFactory::makeSetMessage(payload, key, value);
    // execute operation: the Operation needs to be updated such that there's a separate payload for each range
    TestFactory::invokeOperationSync(client, std::move(TestFactory::createOperation(range, std::move(payload))));

    // get the key
    payload = std::move(client.createPayload());
    TestFactory::makeGetMessage(payload);
    auto result = std::move(TestFactory::invokeOperationSync(client, std::move(TestFactory::createOperation(range, std::move(payload)))));

    // assert
    ASSERT(result->_responses.size() == 1)
    ASSERT(0 == result->_responses[0].moduleCode);
    // extract value
    MemKVModule<>::GetResponse getResponse;
    result->_responses[0].payload.getReader().read(getResponse);
    K2INFO("received value: " << getResponse.value << " for key: " << key);
    ASSERT(value==getResponse.value);
}
