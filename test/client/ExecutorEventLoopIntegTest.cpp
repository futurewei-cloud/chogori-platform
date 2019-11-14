#define CATCH_CONFIG_MAIN
// std
#include <vector>
// catch
#include "catch2/catch.hpp"
// k2
#include <common/PartitionMetadata.h>
// k2:client
#include <client/IClient.h>
#include <client/executor/Executor.h>
// test
#include "TestFactory.h"

using namespace k2;
using namespace k2::client;

SCENARIO("Executor with event loop")
{
    std::string endpointUrl = "tcp+k2rpc://172.17.0.2:11311";
    MockClient client;
    Executor executor(client);
    ClientSettings settings;
    settings.networkProtocol = "tcp+k2rpc";
    settings.userInitThread = true;
    const std::string partitionId = "3.1.1";
    int count = 0;
    bool stopFlag = false;

    settings.runInLoop = ([&] (k2::client::IClient& client) {
        (void)client;

        count++;
        if(stopFlag || count > 4000)
        {
            executor.stop();
        }
        if(count > 1) {
            return 1000;
        }

        K2INFO("executing loop once");

        // create partition
        K2INFO("Creating partition: " << partitionId);
        executor.execute(endpointUrl,
        [&] (std::unique_ptr<k2::Payload> payload) {
            TestFactory::makePartitionPayload(*(payload.get()), partitionId,  std::move(k2::PartitionRange("g", "j")), MessageType::PartitionAssign);
            K2INFO("payload created");

            // send the payload
            executor.execute(endpointUrl, std::chrono::seconds(5), std::move(payload),
            [&] (std::unique_ptr<ResponseMessage> response) {
                K2INFO("response from execute:" << k2::getStatusText(response->status));
                ASSERT(response->status == Status::Ok)
                stopFlag = true;
            });
        });

        return 1000;
    });

    executor.init(settings);
    // blocks until stopped
    executor.start();

    // verify
    ASSERT(stopFlag == true);
}
