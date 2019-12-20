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

SCENARIO("Executor with event loop")
{
    class Context: public IApplicationContext
    {
    public:
        Executor& _rExecutor;

        Context(Executor& rExecutor)
        : _rExecutor(rExecutor)
        {
         // empty
        }

        std::unique_ptr<Context> newInstance()
        {
            return std::move(std::make_unique<Context>(_rExecutor));
        }
    };

    class TestApplication: public IApplication<Context>
    {
    protected:
        const std::string endpointUrl = "tcp+k2rpc://127.0.0.1:11311";
        const std::string partitionId = "3.1.1";
        int count = 0;
        std::unique_ptr<Context> _pContext;
        bool done = false;

    public:
        virtual Duration eventLoop()
        {
            count++;
            if(done || count > 4000)
            {
                 // verify
                assert(done);
                // stop executor
                _pContext->_rExecutor.stop();
            }
            if(count > 1) {
                return 1000us;
            }

            K2INFO("executing loop once");

            // create partition
            K2INFO("Creating partition: " << partitionId);
            _pContext->_rExecutor.execute(endpointUrl,
            [&] (std::unique_ptr<k2::Payload> payload) {
                TestFactory::makePartitionPayload(*(payload.get()), partitionId, std::move(k2::PartitionRange("g", "j")), MessageType::PartitionAssign);
                K2INFO("payload created");

                // send the payload
                _pContext->_rExecutor.execute(endpointUrl, 5s, std::move(payload),
                [&] (std::unique_ptr<ResponseMessage> response) {
                    K2INFO("response from execute:" << k2::getStatusText(response->status));
                    assert(response->status == Status::Ok);
                    done = true;
                });
            });

            return 1000us;
        }

        virtual void onInit(std::unique_ptr<Context> pContext)
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

    TestApplication app;
    Executor executor;
    Context context(executor);
    executor.registerApplication(app, context);
    ClientSettings settings;
    settings.networkProtocol = "tcp+k2rpc";
    executor.init(settings);

    // blocks until stopped
    executor.start();
}
