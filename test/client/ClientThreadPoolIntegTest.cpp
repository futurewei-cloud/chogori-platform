#define CATCH_CONFIG_MAIN
// std
#include <vector>
// catch
#include "catch2/catch.hpp"
// k2
#include <k2/k2types/PartitionMetadata.h>
// k2:client
#include <k2/client/Client.h>
#include <k2/modules/memkv/server/MemKVModule.h>
// test
#include "TestFactory.h"

using namespace k2;
using namespace k2::client;

SCENARIO("Client in a thread pool")
{
    const k2::String endpointUrl = "tcp+k2rpc://127.0.0.1:11311";
    std::vector<PartitionDescription> partitions;
    partitions.push_back(TestFactory::createPartitionDescription(endpointUrl, "1.1.1", PartitionRange("a", "d")));
    partitions.push_back(TestFactory::createPartitionDescription(endpointUrl, "2.1.1", PartitionRange("d", "g")));
    partitions.push_back(TestFactory::createPartitionDescription(endpointUrl, "3.1.1", PartitionRange("g", "j")));
    partitions.push_back(TestFactory::createPartitionDescription(endpointUrl, "4.1.1", PartitionRange("j", ""))); // empty high key terminates the range
    TestClient client(partitions);
    ClientSettings settings;
    settings.networkProtocol = "tcp+k2rpc";
    Executor& rExecutor = client.getExecutor();

    k2::String partitionId = "2.1.1";
    Range range = Range::close("d", "f");
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
    _conditional.wait_for(_lock, 5s);

    srand(time(0));
    k2::String value = std::to_string(rand());
    k2::String key = "key";

    // set key
    // create payload: the range is required since we need to find the endpoint; different type of endpoint have different payloads
    Payload payload = client.createPayload();
    TestFactory::makeSetMessage(payload, key, value);
    // execute operation: the Operation needs to be updated such that there's a separate payload for each range
    TestFactory::invokeOperationSync(client, TestFactory::createOperation(range, std::move(payload)));

    // get the key
    payload = client.createPayload();
    TestFactory::makeGetMessage(payload);
    auto result = TestFactory::invokeOperationSync(client, TestFactory::createOperation(range, std::move(payload)));

    // assert
    assert(result->_responses.size() == 1);
    assert(0 == result->_responses[0].moduleCode);
    // extract value
    MemKVModule<>::GetResponse getResponse;
    result->_responses[0].payload.getReader().read(getResponse);
    K2INFO("received value: " << getResponse.value << " for key: " << key);
    assert(value==getResponse.value);
}
