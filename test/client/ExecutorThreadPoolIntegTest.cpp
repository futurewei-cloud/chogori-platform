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


SCENARIO("Executor in thread pool mode")
{
    std::string endpointUrl = "tcp+k2rpc://127.0.0.1:11311";
    MockClient client;
    Executor executor(client);
    ClientSettings settings;
    settings.networkProtocol = "tcp+k2rpc";
    const std::string partitionId = "4.1.1";
    // start executor in thread pool mode
    settings.userInitThread = false;
    executor.init(settings);
    // start executor in a thread pool
    executor.start();

    std::mutex _mutex;
    std::unique_lock<std::mutex> _lock(_mutex);
    std::condition_variable _conditional;
    // create payload
    std::unique_ptr<Payload> pPayload;
    executor.execute(endpointUrl,
    [&] (std::unique_ptr<k2::Payload> payload) {
        TestFactory::makePartitionPayload(*(payload.get()), partitionId,  std::move(k2::PartitionRange("j", "")), MessageType::PartitionAssign);
        pPayload = std::move(payload);
        K2INFO("payload1 created");
        _conditional.notify_one();
        }
    );
    // wait until the payload is created
    _conditional.wait_for(_lock, std::chrono::seconds(6));
    // verify the payload was created
    ASSERT(nullptr != pPayload.get())

    // send the payload
    bool flag = false;
    executor.execute(endpointUrl, std::chrono::seconds(4), std::move(pPayload),
    [&] (std::unique_ptr<ResponseMessage> response) {
        K2INFO("response from payload1:" << k2::getStatusText(response->status));
        ASSERT(response->status == Status::Ok)
        flag = true;
        _conditional.notify_one();
    });
    _conditional.wait_for(_lock, std::chrono::seconds(6));

    executor.stop();

    // verify the callback was invoked
    ASSERT(flag == true)
}
