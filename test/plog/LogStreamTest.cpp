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

namespace k2::log {
inline thread_local k2::logging::Logger ltest("k2::ltest");
}

class LogStreamTest {

private:
    int exitcode = -1;
    std::unique_ptr<k2::TXEndpoint> _cpoEndpoint;
    k2::MetadataMgr _mmgr;
    k2::LogStream* _logStream;
    k2::MetadataMgr _reload_mmgr;
    k2::LogStream* _reload_logStream;
    String _initPlogId;
    k2::ConfigVar<std::vector<k2::String>> _plogConfigEps{"plog_server_endpoints"};
    seastar::future<> _testFuture = seastar::make_ready_future();
    seastar::timer<> _testTimer;
    
public:  // application lifespan
    LogStreamTest() {
        K2LOG_I(log::ltest, "ctor");
    }
    ~LogStreamTest() {
        K2LOG_I(log::ltest, "ctor");
    }

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2LOG_I(log::ltest, "stop");
        return std::move(_testFuture);
    }

    seastar::future<> start() {
        K2LOG_I(log::ltest, "start");
        ConfigVar<String> configEp("cpo_url");
        _cpoEndpoint = RPC().getTXEndpoint(configEp());

        // let start() finish and then run the tests
        _testTimer.set_callback([this] {
            _testFuture = seastar::make_ready_future()
            .then([this] {
                // Create a persistence cluster
                dto::PersistenceGroup group1{.name="Group1", .plogServerEndpoints = _plogConfigEps()};
                dto::PersistenceCluster cluster1;
                cluster1.name="Persistence_Cluster_1";
                cluster1.persistenceGroupVector.push_back(group1);

                auto request = dto::PersistenceClusterCreateRequest{.cluster=std::move(cluster1)};
                return RPC().callRPC<dto::PersistenceClusterCreateRequest, dto::PersistenceClusterCreateResponse>(dto::Verbs::CPO_PERSISTENCE_CLUSTER_CREATE, request, *_cpoEndpoint, 1s);
            })
            .then([this] (auto&& response) {
                // Init and create a logstream
                auto& [status, resp] = response;
                K2EXPECT(log::ltest, status, Statuses::S201_Created);

                ConfigVar<String> configEp("cpo_url");
                String cpo_url = configEp();

                return _mmgr.init(cpo_url, "Partition-1", "Persistence_Cluster_1", false);
            })
            .then([this] { return runTest1();})
            .then([this] { return runTest2();})
            .then([this] { return runTest3();})
            .then([this] {
                K2LOG_I(log::ltest, "======= All tests passed ========");
                exitcode = 0;
            })
            .handle_exception([this](auto exc) {
                try {
                    std::rethrow_exception(exc);
                } catch (RPCDispatcher::RequestTimeoutException& exc) {
                    K2LOG_E(log::ltest, "======= Test failed due to timeout ========");
                    exitcode = -1;
                } catch (std::exception& e) {
                    K2LOG_E(log::ltest, "======= Test failed with exception [{}] ========", e.what());
                    exitcode = -1;
                }
            })
            .finally([this] {
                K2LOG_I(log::ltest, "======= Test ended ========");
                seastar::engine().exit(exitcode);
            });
        });
        _testTimer.arm(0ms);
        return seastar::make_ready_future<>();
    }

    seastar::future<> runTest1() {
        K2LOG_I(log::ltest, ">>> Test1: Write and Read a Value from/to one log stream");
        _logStream = _mmgr.obtainLogStream(LogStreamType::WAL);
        String header("0", 10000);
        Payload payload([] { return Binary(20000); });
        payload.write(header);
        // write a string to the first log stream
        return _logStream->append(std::move(payload))
        .then([this, header] (auto&& response){
            auto& [plogId, appended_offset] = response;
            _initPlogId = plogId;
            return _logStream->read(plogId, 0, appended_offset);
        })
        // read the string that we wrote before, check whether they are the same
        .then([this, header] (auto&& payloads){
            String str;
            for (auto& payload:payloads){
                payload.seek(0);
                payload.read(str);
                K2EXPECT(log::ltest, str, header);
            }
            return seastar::make_ready_future<>();
        });
    }

    seastar::future<> runTest2() {
        K2LOG_I(log::ltest, ">>> Test2: Write and read huge data");
         _logStream = _mmgr.obtainLogStream(LogStreamType::IndexerSnapshot);
        // write 1000 Strings to the second log stream, these writes will trigger a plog switch
        String header("0", 10000);
        std::vector<seastar::future<std::pair<String, uint32_t>> > writeFutures;
        for (uint32_t i = 0; i < 2000; ++i){
            Payload payload([] { return Binary(20000); });
            payload.write(header);
            writeFutures.push_back(_logStream->append(std::move(payload)));
        }
        // read all the Strings that we wrote before, check whether they are the same
        return seastar::when_all_succeed(writeFutures.begin(), writeFutures.end())
        .then([this, header] (std::vector<std::pair<String, uint32_t> >&& responses){
            bool first_plog = false;
            for (auto& response:responses){
                auto& [plogId, appended_offset] = response;
                if (!first_plog){
                    first_plog = true;
                    _initPlogId = plogId;
                }
                K2LOG_D(log::ltest, "{}, {}", plogId, appended_offset);
            }
            K2LOG_I(log::ltest, ">>> Test2.1: Write Done");
            return _logStream->read(_initPlogId, 0, 2000*10005);
        })
        // write another 1000 Strings to the same log stream, these writes will trigger a plog switch as well
        .then([this, header] (auto&& payloads){
            K2LOG_I(log::ltest, ">>> Test2.2: Read Done");
            String str;
            uint32_t count = 0;
            for (auto& payload:payloads){
                payload.seek(0);
                while (payload.getDataRemaining() > 0){
                    payload.read(str);
                    K2EXPECT(log::ltest, str, header);
                    ++count;
                }
            }
            K2EXPECT(log::ltest, count, 2000);
            std::vector<seastar::future<std::pair<String, uint32_t>> > writeFutures;
            for (uint32_t i = 0; i < 2000; ++i){
                Payload payload([] { return Binary(20000); });
                payload.write(header);
                writeFutures.push_back(_logStream->append(std::move(payload)));
            }
            return seastar::when_all_succeed(writeFutures.begin(), writeFutures.end());
        })
        // read all the Strings that we wrote before, check whether they are the same
        .then([this, header] (std::vector<std::pair<String, uint32_t> >&& responses){
            for (auto& response:responses){
                auto& [plogId, appended_offset] = response;
                K2LOG_D(log::ltest, "{}, {}", plogId, appended_offset);
            }
            K2LOG_I(log::ltest, ">>> Test2.3: Write Done");
            return _logStream->read(_initPlogId, 0, 4000*10005);
        })
        .then([this, header] (auto&& payloads){
            K2LOG_I(log::ltest, ">>> Test2.4: Read Done");
            String str;
            uint32_t count = 0;
            for (auto& payload:payloads){
                payload.seek(0);
                while (payload.getDataRemaining() > 0){
                    payload.read(str);
                    K2EXPECT(log::ltest, str, header);
                    ++count;
                }
            }
            K2EXPECT(log::ltest, count, 4000);
            return seastar::make_ready_future<>();
        });
    }

    seastar::future<> runTest3() {
        K2LOG_I(log::ltest, ">>> Test3: replay the metadata manager and read/write data from/to a specific log stream");
        ConfigVar<String> configEp("cpo_url");
        String cpo_url = configEp();
        String header("0", 10000);

        // Replay the entire metadata manager
        return _reload_mmgr.replay(cpo_url, "Partition-1", "Persistence_Cluster_1")
        // Read all the data from reloaded log stream and check weather they are the same
        .then([this] (auto&& response){
            auto& status  = response;
            K2LOG_D(log::ltest, "{}", status);
            K2LOG_I(log::ltest, ">>> Test3.1: Replay Done");

            _reload_logStream = _reload_mmgr.obtainLogStream(LogStreamType::IndexerSnapshot);
            return _reload_logStream->read(_initPlogId, 0, 4000*10005);
        })
        // Write data to the reloaded log stream
        .then([this, header] (auto&& payloads){
            String str;
            uint32_t count = 0;
            for (auto& payload:payloads){
                payload.seek(0);
                while (payload.getDataRemaining() > 0){
                    payload.read(str);
                    K2EXPECT(log::ltest, str, header);
                    ++count;
                }
            }
            K2EXPECT(log::ltest, count, 4000);
            K2LOG_I(log::ltest, ">>> Test3.2: Read Done");

            std::vector<seastar::future<std::pair<String, uint32_t>> > writeFutures;
            for (uint32_t i = 0; i < 2000; ++i){
                Payload payload([] { return Binary(20000); });
                payload.write(header);
                writeFutures.push_back(_reload_logStream->append(std::move(payload)));
            }
            return seastar::when_all_succeed(writeFutures.begin(), writeFutures.end());
        })
        // Read all the data from reloaded log stream and check weather they are the same
        .then([this, header] (std::vector<std::pair<String, uint32_t> >&& responses){
            for (auto& response:responses){
                auto& [plogId, appended_offset] = response;
                K2LOG_D(log::ltest, "{}, {}", plogId, appended_offset);
            }
            K2LOG_I(log::ltest, ">>> Test3.3: Write Done");
            return _reload_logStream->read(_initPlogId, 0, 6000*10005);
        })
        .then([this, header] (auto&& payloads){
            K2LOG_I(log::ltest, ">>> Test3.4: Read Done");
            String str;
            uint32_t count = 0;
            for (auto& payload:payloads){
                payload.seek(0);
                while (payload.getDataRemaining() > 0){
                    payload.read(str);
                    K2EXPECT(log::ltest, str, header);
                    ++count;
                }
            }
            K2EXPECT(log::ltest, count, 6000);
            return seastar::make_ready_future<>();
        });
    }
};

int main(int argc, char** argv) {
    k2::App app("LogStreamTest");
    app.addOptions()("cpo_url", bpo::value<k2::String>(), "The endpoint of the CPO service");
    app.addOptions()("plog_server_endpoints", bpo::value<std::vector<k2::String>>()->multitoken(), "The endpoints of the plog servers");
    app.addApplet<LogStreamTest>();
    return app.start(argc, argv);
}