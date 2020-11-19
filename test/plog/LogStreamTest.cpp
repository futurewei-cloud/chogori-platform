/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/persistence/logStream/LogStream.h>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <k2/dto/PersistenceCluster.h>
#include <k2/dto/MessageVerbs.h>

using namespace k2;

class PlogTest {

private:
    int exitcode = -1;
    std::unique_ptr<k2::TXEndpoint> _cpoEndpoint;
    k2::LogStream _client;
    k2::ConfigVar<std::vector<k2::String>> _plogConfigEps{"plog_server_endpoints"};
    seastar::future<> _testFuture = seastar::make_ready_future();
    seastar::timer<> _testTimer;
    
public:  // application lifespan
    PlogTest() {
        K2INFO("ctor");
    }
    ~PlogTest() {
        K2INFO("dtor");
    }

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2INFO("stop");
        return std::move(_testFuture);
    }

    seastar::future<> start() {
        K2INFO("start");
        ConfigVar<String> configEp("cpo_url");
        _cpoEndpoint = RPC().getTXEndpoint(configEp());

        // let start() finish and then run the tests
        _testTimer.set_callback([this] {
            _testFuture = runTest1()
            .then([this] { return runTest2(); })
            .then([this] { return runTest3(); })
            .then([this] { return runTest4(); })
            .then([this] {
                K2INFO("======= All tests passed ========");
                exitcode = 0;
            })
            .handle_exception([this](auto exc) {
                try {
                    std::rethrow_exception(exc);
                } catch (RPCDispatcher::RequestTimeoutException& exc) {
                    K2ERROR("======= Test failed due to timeout ========");
                    exitcode = -1;
                } catch (std::exception& e) {
                    K2ERROR("======= Test failed with exception [" << e.what() << "] ========");
                    exitcode = -1;
                }
            })
            .finally([this] {
                K2INFO("======= Test ended ========");
                seastar::engine().exit(exitcode);
            });
        });
        _testTimer.arm(0ms);
        return seastar::make_ready_future<>();
    }


    seastar::future<> runTest1() {
        K2INFO(">>> Test1: create a persistence cluster");
        dto::PersistenceGroup group1{.name="Group1", .plogServerEndpoints = _plogConfigEps()};
        dto::PersistenceCluster cluster1;
        cluster1.name="Persistence_Cluster_1";
        cluster1.persistenceGroupVector.push_back(group1);

        auto request = dto::PersistenceClusterCreateRequest{.cluster=std::move(cluster1)};
        return RPC()
        .callRPC<dto::PersistenceClusterCreateRequest, dto::PersistenceClusterCreateResponse>(dto::Verbs::CPO_PERSISTENCE_CLUSTER_CREATE, request, *_cpoEndpoint, 1s)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Statuses::S201_Created);
        });
    }

    seastar::future<> runTest2() {
        K2INFO(">>> Test2: init and create a logstream");
        ConfigVar<String> configEp("cpo_url");
        return _client.init(configEp(), "Persistence_Cluster_1")
        .then([this] (){
            return _client.create("LogStream1");
        });
    }

    seastar::future<> runTest3() {
        K2INFO(">>> Test3: Write and Read a Value from log stream");
        String header;
        for (uint32_t i = 0; i < 10000; ++i){
            header = header + "0";
        }
        Payload payload([] { return Binary(20000); });
        payload.write(header);
        return _client.write(std::move(payload))
        .then([this, header] (){
            return _client.read("LogStream1");
        })
        .then([this, header] (auto&& payloads){
            String str;
            for (auto& payload:payloads){
                payload.seek(0);
                payload.read(str);
                K2EXPECT(str, header);
            }
            return seastar::make_ready_future<>();
        });
    }

    seastar::future<> runTest4() {
        K2INFO(">>> Test4: Write and read huge data");
        String header;
        for (uint32_t i = 0; i < 10000; ++i){
            header = header + "0";
        }

        std::vector<seastar::future<> > writeFutures;
        for (uint32_t i = 0; i < 2000; ++i){
            Payload payload([] { return Binary(20000); });
            payload.write(header);
            writeFutures.push_back(_client.write(std::move(payload)));
        }
        return seastar::when_all_succeed(writeFutures.begin(), writeFutures.end())
        .then([this, header] (){
            return _client.read("LogStream1");
        })
        .then([this, header] (auto&& payloads){
            String str;
            uint32_t count = 0;
            for (auto& payload:payloads){
                payload.seek(0);
                while (payload.getDataRemaining() > 0){
                    payload.read(str);
                    K2EXPECT(str, header);
                    ++count;
                }
            }
            K2EXPECT(count, 2001);

            std::vector<seastar::future<> > writeFutures;
            for (uint32_t i = 0; i < 2000; ++i){
                Payload payload([] { return Binary(20000); });
                payload.write(header);
                writeFutures.push_back(_client.write(std::move(payload)));
            }
            return seastar::when_all_succeed(writeFutures.begin(), writeFutures.end());
        })
        .then([this, header] (){
            return _client.read("LogStream1");
        })
        .then([this, header] (auto&& payloads){
            String str;
            uint32_t count = 0;
            for (auto& payload:payloads){
                payload.seek(0);
                while (payload.getDataRemaining() > 0){
                    payload.read(str);
                    K2EXPECT(str, header);
                    ++count;
                }
            }
            K2EXPECT(count, 4001);
            return seastar::make_ready_future<>();
        });
    }

};

int main(int argc, char** argv) {
    k2::App app("LogStreamTest");
    app.addOptions()("cpo_url", bpo::value<k2::String>(), "The endpoint of the CPO service");
    app.addOptions()("plog_server_endpoints", bpo::value<std::vector<k2::String>>()->multitoken(), "The endpoints of the plog servers");
    app.addApplet<PlogTest>();
    return app.start(argc, argv);
}