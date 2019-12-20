#define CATCH_CONFIG_MAIN
// std
#include <vector>
// catch
#include "catch2/catch.hpp"
// k2
#include <k2/k2types/PartitionMetadata.h>
// k2:client
#include <k2/client/IClient.h>
#include <k2/executor/Executor.h>
// test
#include "TestFactory.h"

using namespace k2;
using namespace k2::client;


SCENARIO("Executor in thread pool mode")
{
    std::string endpointUrl = "tcp+k2rpc://127.0.0.1:11311";
    MockClient client;
    Executor executor;
    ClientSettings settings;
    settings.networkProtocol = "tcp+k2rpc";
    const std::string partitionId = "4.1.1";
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
    _conditional.wait_for(_lock, 6s);
    // verify the payload was created
    assert(nullptr != pPayload.get());

    // send the payload
    bool flag = false;
    executor.execute(endpointUrl, 4s, std::move(pPayload),
    [&] (std::unique_ptr<ResponseMessage> response) {
        K2INFO("response from payload1:" << k2::getStatusText(response->status));
        assert(response->status == Status::Ok);
        flag = true;
        _conditional.notify_one();
    });
    _conditional.wait_for(_lock, 6s);

    executor.stop();

    // verify the callback was invoked
    assert(flag == true);
}
