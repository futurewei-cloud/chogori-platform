// std
#include <iostream>
// k2
#include "node/NodePool.h"
#include "client/executor/Executor.h"

using namespace k2;
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

    virtual void runInThreadPool(std::function<void(IClient&)> routine)
    {
        (void)routine;
    }

    virtual k2::Payload createPayload()
    {
        return std::move(Payload());
    }
};

static void makeAssignPartitionPayload(k2::Payload& payload)
{
    using namespace k2;

    k2::CollectionId collectionId = 3;
    k2::PartitionAssignmentId partitionId;
    partitionId.parse("1.1.1");

    k2::AssignmentMessage assignmentMessage;
    assignmentMessage.collectionMetadata = k2::CollectionMetadata(collectionId, k2::ModuleId::Default, {});
    assignmentMessage.partitionMetadata = k2::PartitionMetadata(partitionId.id, k2::PartitionRange("A", "B"), collectionId);
    assignmentMessage.partitionVersion = partitionId.version;

    PartitionMessage::Header* header;
    bool ret = payload.getWriter().reserveContiguousStructure(header);
    assert(ret);
    header->messageType = MessageType::PartitionAssign;
    header->partition = partitionId;
    payload.getWriter().write(assignmentMessage);
    header->messageSize = payload.getSize() - txconstants::MAX_HEADER_SIZE - sizeof(PartitionMessage::Header);
}

int main()
{
    using namespace k2;
    using namespace k2::client;

    MockClient client;
    Executor executor(client);
    ClientSettings settings;

    // start executor in thread pool mode
    settings.userInitThread = false;
    executor.init(settings);
    // start executor in a thread pool
    executor.start();

    // EXAMPLE 1: the payload is created in one task and the it is executed in another
    std::mutex _mutex;
    std::condition_variable _conditional;
    // create payload
    std::unique_ptr<Payload> pPayload;
    std::unique_lock<std::mutex> lock(_mutex);
    executor.execute("tcp+k2rpc://127.0.0.1:11311",
        [&] (std::unique_ptr<k2::Payload> payload) mutable {
            makeAssignPartitionPayload(*payload);
            pPayload = std::move(payload);
            K2INFO("payload1 created");
            _conditional.notify_one();
        }
    );

    // wait until the payload is created
    _conditional.wait_for(lock, std::chrono::seconds(2));

    // send the payload
    executor.execute("tcp+k2rpc://127.0.0.1:11311", std::chrono::microseconds(10000), std::move(pPayload),
        [] (std::unique_ptr<ResponseMessage> response) {
            K2INFO("response from payload1:" << k2::getStatusText(response->status));
        }
    );

    // EXAMPLE 2: payload is created and executed in the same task
    std::mutex _mutex2;
    std::unique_lock<std::mutex> lock2(_mutex2);
    std::condition_variable _conditional2;
    executor.execute("tcp+k2rpc://127.0.0.1:11311", std::chrono::microseconds(10000),
        [] (k2::Payload& payload) {
            makeAssignPartitionPayload(payload);
            K2INFO("payload2 created")
        },
        [&] (std::unique_ptr<ResponseMessage> response) {
            K2INFO("response from payload2:" << k2::getStatusText(response->status));
            _conditional2.notify_one();
        }
    );

    // wait until we receive te response
    _conditional2.wait_for(lock2, std::chrono::seconds(2));
    executor.stop();

    Executor userInitThreadExecutor(client);
    settings.userInitThread = true;
    settings.runInLoop = ([&] (k2::client::IClient& client) {
        K2INFO("executing loop");
        client.createPayload();
        userInitThreadExecutor.stop();

        return 10000;
    });

    // start executor in the user initialized thread
    userInitThreadExecutor.init(settings);
    userInitThreadExecutor.start();

    return 0;
}
