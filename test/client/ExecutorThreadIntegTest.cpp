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

using namespace k2;
using namespace k2::client;

class MockClient: public k2::client::IClient
{
    virtual void init(const k2::client::ClientSettings& settings)
    {
        // used to ignore compilation warning
        (void)settings.userInitThread;
    }

    virtual void execute(k2::client::Operation&& settings, std::function<void(IClient&, k2::client::OperationResult&&)> onCompleted)
    {
        (void)settings.transactional;
        (void)onCompleted;
    }

    virtual void execute(PartitionDescription& partition, std::function<void(Payload&)> onPayload, std::function<void(IClient&, OperationResult&&)> onCompleted)
    {
        (void)partition;
        (void)onPayload;
        (void)onCompleted;
    }

    virtual void runInThreadPool(std::function<void(IClient&)> routine)
    {
        (void)routine;
    }

    virtual k2::Payload createPayload()
    {
        return std::move(Payload());
    }

    virtual void createPayload(std::function<void(Payload&&)> onCompleted)
    {
        (void)onCompleted;
    }
};

static void makeAssignPartitionPayload(k2::Payload& payload, const std::string& id)
{
    using namespace k2;

    k2::CollectionId collectionId = 3;
    k2::PartitionAssignmentId partitionId;
    partitionId.parse(id.c_str());

    k2::AssignmentMessage assignmentMessage;
    assignmentMessage.collectionMetadata = k2::CollectionMetadata(collectionId, k2::ModuleId::Default, {});
    assignmentMessage.partitionMetadata = k2::PartitionMetadata(partitionId.id, k2::PartitionRange("A", "C"), collectionId);
    assignmentMessage.partitionVersion = partitionId.version;

    PartitionMessage::Header* header;
    bool ret = payload.getWriter().reserveContiguousStructure(header);
    assert(ret);
    header->messageType = MessageType::PartitionAssign;
    header->partition = partitionId;
    payload.getWriter().write(assignmentMessage);
    header->messageSize = payload.getSize() - txconstants::MAX_HEADER_SIZE - sizeof(PartitionMessage::Header);
}


SCENARIO("Executor in thread mode", "[payload]")
{
    MockClient client;

    WHEN("Start the executor in thread mode")
    {
        ClientSettings settings;
        Executor executor(client);
        settings.networkProtocol = "tcp+k2rpc";
        // start executor in thread pool mode
        settings.userInitThread = false;
        executor.init(settings);
        // start executor in a thread pool
        executor.start();

        THEN("Send payload; separate invocations")
        {
            // EXAMPLE 1: the payload is created in one task and the it is executed in another
            std::mutex _mutex;
            std::unique_lock<std::mutex> _lock(_mutex);
            std::condition_variable _conditional;
            // create payload
            std::unique_ptr<Payload> pPayload;
            executor.execute("tcp+k2rpc://127.0.0.1:11311",
            [&] (std::unique_ptr<k2::Payload> payload) mutable {
                makeAssignPartitionPayload(*payload, "3.1.1");
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
            executor.execute("tcp+k2rpc://127.0.0.1:11311", std::chrono::seconds(4), std::move(pPayload),
                [&] (std::unique_ptr<ResponseMessage> response) {
                K2INFO("response from payload1:" << k2::getStatusText(response->status));
                ASSERT(response->status == Status::Ok)
                flag = true;
                _conditional.notify_one();
                }
            );
            _conditional.wait_for(_lock, std::chrono::seconds(6));
            // verify the callback was invoked
            ASSERT(flag == true)
        }

        THEN("Send one shot")
        {
            std::mutex _mutex;
            std::unique_lock<std::mutex> _lock(_mutex);
            std::condition_variable _conditional;
            bool send = false;
            executor.execute("tcp+k2rpc://127.0.0.1:11311", std::chrono::seconds(4),
                [] (k2::Payload& payload) {
                    makeAssignPartitionPayload(payload, "3.2.1");
                    K2INFO("payload2 created")
                },
                [&] (std::unique_ptr<ResponseMessage> response) {
                    K2INFO("response from payload2:" << k2::getStatusText(response->status));
                    send = true;
                    _conditional.notify_one();
                }
            );

            // wait until we receive te response
            _conditional.wait_for(_lock, std::chrono::seconds(10));
            ASSERT(send == true);
        }
    }
}
