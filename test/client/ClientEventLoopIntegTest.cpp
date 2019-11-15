#define CATCH_CONFIG_MAIN
// std
#include <vector>
#include <utility>
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

static void setGetKeyScenario(IClient& rClient, const Range& range, std::function<void()> onCompleted)
{
    srand(time(0));
    std::string value = std::to_string(rand());
    std::string key = "key";

    // set key
    rClient.createPayload(
    [key, value, range, onCompleted] (IClient& rClient, k2::Payload&& payload) {
        // execute operation: the Operation needs to be updated such that there's a separate payload for each range
        TestFactory::makeSetMessage(payload, key, value);
        rClient.execute(std::move(TestFactory::createOperation(range, std::move(payload))),
        [key, value, range, onCompleted] (IClient& rClient, OperationResult&& result) {

            K2INFO("set value: " << value << " for key: " << key << " response: " << k2::getStatusText(result._responses[0].status));
            (void)rClient;
            ASSERT(result._responses[0].status == Status::Ok);

            // get the key
            rClient.createPayload(
            [key, value, range, onCompleted] (IClient& rClient, k2::Payload&& payload) {

                TestFactory::makeGetMessage(payload);
                rClient.execute(std::move(TestFactory::createOperation(range, std::move(payload))),
                [key, value, onCompleted] (IClient& rClient, OperationResult&& result) {
                    (void)rClient;

                    // assert
                    ASSERT(result._responses[0].status == Status::Ok);
                    ASSERT(result._responses.size() == 1)
                    ASSERT(0 == result._responses[0].moduleCode);
                    // extract value
                    MemKVModule<>::GetResponse getResponse;
                    result._responses[0].payload.getReader().read(getResponse);
                    K2INFO("received value: " << getResponse.value << " for key: " << key);
                    ASSERT(value==getResponse.value);

                    onCompleted();
                });
            });
        });
    });
}


SCENARIO("Client in a thread pool")
{
    std::string endpointUrl = "tcp+k2rpc://172.17.0.3:11311";
    std::vector<PartitionDescription> partitions;
    partitions.push_back(std::move(TestFactory::createPartitionDescription(endpointUrl, "1.1.1", PartitionRange("a", "d"))));
    partitions.push_back(std::move(TestFactory::createPartitionDescription(endpointUrl, "2.1.1", PartitionRange("d", "g"))));
    partitions.push_back(std::move(TestFactory::createPartitionDescription(endpointUrl, "3.1.1", PartitionRange("g", "j"))));
    partitions.push_back(std::move(TestFactory::createPartitionDescription(endpointUrl, "4.1.1", PartitionRange("j", "")))); // empty high key terminates the range
    TestClient client(partitions);
    ClientSettings settings;
    settings.networkProtocol = "tcp+k2rpc";
    Executor& rExecutor = client.getExecutor();

    settings.userInitThread = true;
    std::string partitionId = "1.1.1";
    Range range = Range::close("b", "c");

    bool partitionsCreated = false;
    bool stopFlag = false;
    int count = 0;

    settings.runInLoop = ([&] (k2::client::IClient& client) {
        (void)client;

        count++;
        if(stopFlag || count > 5000)
        {
            rExecutor.stop();
        }
        if(count > 1) {
            return 1000;
        }

        if(!partitionsCreated) {
            TestFactory::assignPartitionAsync(rExecutor, endpointUrl, partitionId, PartitionRange("a", "d"),
            [&] {
                setGetKeyScenario(client, range,
                [&] {
                    stopFlag = true;
                    });
                });
        }

        return 1000;
    });

    // blocks until stopped
    client.init(settings);

}
