#define CATCH_CONFIG_MAIN
// std
#include <vector>
#include <utility>
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

static void setGetKeyScenario(IClient& rClient, const Range& range, std::function<void()> onCompleted)
{
    srand(time(0));
    k2::String value = std::to_string(rand());
    k2::String key = "key";

    // set key
    rClient.createPayload(
    [key, value, range, onCompleted] (IClient& rClient, k2::Payload&& payload) {
        // execute operation: the Operation needs to be updated such that there's a separate payload for each range
        TestFactory::makeSetMessage(payload, key, value);
        rClient.execute(TestFactory::createOperation(range, std::move(payload)),
        [key, value, range, onCompleted] (IClient& rClient, OperationResult&& result) {

            K2INFO("set value: " << value << " for key: " << key << " response: " << result._responses[0].status);
            (void)rClient;
            assert(result._responses[0].status.is2xxOK());

            // get the key
            rClient.createPayload(
            [key, value, range, onCompleted] (IClient& rClient, k2::Payload&& payload) {

                TestFactory::makeGetMessage(payload);
                rClient.execute(TestFactory::createOperation(range, std::move(payload)),
                [key, value, onCompleted] (IClient& rClient, OperationResult&& result) {
                    (void)rClient;

                    // assert
                    assert(result._responses[0].status.is2xxOK());
                    assert(result._responses.size() == 1);
                    assert(0 == result._responses[0].moduleCode);
                    // extract value
                    MemKVModule<>::GetResponse getResponse;
                    result._responses[0].payload.seek(0);
                    result._responses[0].payload.read(getResponse);
                    K2INFO("received value: " << getResponse.value << " for key: " << key);
                    assert(value==getResponse.value);

                    onCompleted();
                });
            });
        });
    });
}


SCENARIO("Client in a thread pool")
{
    const k2::String endpointUrl = "tcp+k2rpc://127.0.0.1:11311";
    std::vector<PartitionDescription> partitions;
    partitions.push_back(TestFactory::createPartitionDescription(endpointUrl, "1.1.1", PartitionRange("a", "d")));
    partitions.push_back(TestFactory::createPartitionDescription(endpointUrl, "2.1.1", PartitionRange("d", "g")));
    partitions.push_back(TestFactory::createPartitionDescription(endpointUrl, "3.1.1", PartitionRange("g", "j")));
    partitions.push_back(TestFactory::createPartitionDescription(endpointUrl, "4.1.1", PartitionRange("j", ""))); // empty high key terminates the range

    class TestContext: public IApplicationContext
    {
    public:
        IClient& _rClient;
        Executor& _rExecutor;
        k2::String _endpoint;

        TestContext(IClient& rClient, Executor& rExecutor, const k2::String& endpoint)
        : _rClient(rClient)
        , _rExecutor(rExecutor)
        , _endpoint(endpoint)
        {
            // empty
        }

        std::unique_ptr<TestContext> newInstance()
        {
            return std::make_unique<TestContext>(_rClient, _rExecutor, _endpoint);
        }
    };

    class TestApplication: public IApplication<TestContext>
    {
    protected:
        const k2::String partitionId = "1.1.1";
        const Range range = Range::close("b", "c");
        int count = 0;
        bool done = false;
        std::unique_ptr<TestContext> _pContext;

    public:
        TestApplication()
        {
            // empty
        }

        virtual Duration eventLoop()
        {
            count++;
            if(done || count > 5000)
            {
                 // verify
                assert(done);
                // stop client
               _pContext->_rClient.stop();
            }
            if(count > 1) {
                return 1000us;
            }

            TestFactory::assignPartitionAsync(_pContext->_rExecutor, _pContext->_endpoint, partitionId, PartitionRange("a", "d"),
            [&] {
                setGetKeyScenario(_pContext->_rClient, range,
                [&] {
                    done = true;
                });
            });

            return 1000us;
        }

        virtual void onInit(std::unique_ptr<TestContext> pContext)
        {
           _pContext = std::move(pContext);
        }

        virtual std::unique_ptr<IApplication> newInstance()
        {
            return std::unique_ptr<IApplication>(new TestApplication());
        }

        void onStart()
        {

        }

        virtual void onStop()
        {

        }
    };

    TestClient client(partitions);
    ClientSettings settings;
    settings.networkProtocol = "tcp+k2rpc";
    Executor& rExecutor = client.getExecutor();
    TestApplication app;
    TestContext context(client, rExecutor, endpointUrl);
    client.registerApplication(app, context);

    // start the client; blocks until stopped
    client.init(settings);

}
