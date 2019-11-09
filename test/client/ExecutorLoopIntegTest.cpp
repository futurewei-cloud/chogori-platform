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

SCENARIO("Executor in the user's thread", "[payload]")
{
    MockClient client;

    WHEN("Start the executor the user's thread")
    {
        ClientSettings settings;
        Executor executor(client);
        settings.networkProtocol = "tcp+k2rpc";
        // start executor
        settings.userInitThread = true;

        int count = 0;
        bool flag = false;
        settings.runInLoop = ([&] (k2::client::IClient& client) {
            (void)client;

            if(count > 0) {
                return 1;
            }
            else if(count > 100)
            {
                executor.stop();
            }
            count++;

            K2INFO("executing loop");
            std::unique_ptr<Payload> pPayload;
            executor.execute("tcp+k2rpc://127.0.0.1:11311",
                [&] (std::unique_ptr<k2::Payload> payload) mutable {
                std::cout << "Payload Created" << std::endl;
                makeAssignPartitionPayload(*(payload.get()), "4.1.1");
                pPayload = std::move(payload);
                K2INFO("payload1 created");

                // send the payload
                executor.execute("tcp+k2rpc://127.0.0.1:11311", std::chrono::seconds(5), std::move(pPayload),
                    [&] (std::unique_ptr<ResponseMessage> response) {
                    K2INFO("response from payload1:" << k2::getStatusText(response->status));
                    ASSERT(response->status == Status::Ok)
                    flag = true;
                    executor.stop();
                });
            });

            return 1000;
        });

        executor.init(settings);
        // start executor in a thread pool
        executor.start();

        // verify
        ASSERT(flag == true);
    }
}
