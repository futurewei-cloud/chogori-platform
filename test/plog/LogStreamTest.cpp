/*
MIT License

Copyright(c) 2021 Futurewei Cloud

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
    std::shared_ptr<k2::PartitionMetadataMgr> _mmgr;
    std::shared_ptr<k2::LogStream> _logStream;
    std::shared_ptr<k2::PartitionMetadataMgr> _reload_mmgr;
    std::shared_ptr<k2::LogStream> _reload_logStream;
    String _initPlogId;
    k2::ConfigVar<std::vector<k2::String>> _plogConfigEps{"plog_server_endpoints"};
    seastar::future<> _testFuture = seastar::make_ready_future();
    seastar::timer<> _testTimer;

public:  // application lifespan
    LogStreamTest() {
        K2LOG_D(log::ltest, "dtor");
    }
    ~LogStreamTest() {
        K2LOG_D(log::ltest, "dtor");
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
                String cpoUrl = configEp();
                _mmgr = std::make_shared<k2::PartitionMetadataMgr>();
                return _mmgr->init(cpoUrl, "Partition-1", "Persistence_Cluster_1").discard_result();
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
                AppBase().stop(exitcode);
            });
        });
        _testTimer.arm(0ms);
        return seastar::make_ready_future<>();
    }

    seastar::future<> runTest1() {
        K2LOG_I(log::ltest, ">>> Test1: Write and Read a Value from/to one log stream");
        auto response = _mmgr->obtainLogStream(LogStreamType::WAL);
        auto status = std::get<0>(response);
        auto logStream = std::get<1>(response);
        if (!status.is2xxOK()){
            throw std::runtime_error("cannot obtain LogStream");
        }
        else{
            _logStream = logStream;
        }
        String data_to_append(10000, '2');
        Payload payload(Payload::DefaultAllocator(20000));
        payload.write(data_to_append);
        // write a string to the first log stream
        return _logStream->append_data_to_plogs(dto::AppendRequest{.payload=std::move(payload)})
        .then([this, data_to_append] (auto&& response){
            auto& [status, append_response] = response;
            K2ASSERT(log::ltest, status.is2xxOK(), "cannot append data!");
            _initPlogId = append_response.plogId;
            return _logStream->read_data_from_plogs(dto::ReadRequest{.start_plogId=append_response.plogId, .start_offset=0, .size=append_response.current_offset});
        })
        .then([this, data_to_append] (auto&& response){
            auto& [status, read_response] = response;
            K2ASSERT(log::ltest, status.is2xxOK(), "cannot read data!");
            K2EXPECT(log::ltest, read_response.payload.getSize(), 10005);
            String str;
            read_response.payload.seek(0);
            read_response.payload.read(str);
            K2EXPECT(log::ltest, str, data_to_append);
            return seastar::make_ready_future<>();
        });
    }

    seastar::future<> runTest2() {
        K2LOG_I(log::ltest, ">>> Test2: Write and read huge data");
        auto response = _mmgr->obtainLogStream(LogStreamType::IndexerSnapshot);
        auto status = std::get<0>(response);
        auto logStream = std::get<1>(response);
        if (!status.is2xxOK()){
            throw std::runtime_error("cannot obtain LogStream");
        }
        else{
            _logStream = logStream;
        }
        // write 1000 Strings to the second log stream, these writes will trigger a plog switch
        String data_to_append(10000, '3');
        std::vector<seastar::future<std::tuple<Status, dto::AppendResponse>> > writeFutures;
        for (uint32_t i = 0; i < 2000; ++i){
            Payload payload(Payload::DefaultAllocator(20000));
            payload.write(data_to_append);
            writeFutures.push_back(_logStream->append_data_to_plogs(dto::AppendRequest{.payload=std::move(payload)}));
        }
        // read all the Strings that we wrote before, check whether they are the same
        return seastar::when_all_succeed(writeFutures.begin(), writeFutures.end())
        .then([this, data_to_append] (std::vector<std::tuple<Status, dto::AppendResponse> >&& responses){
            bool first_plog = false;
            for (auto& response:responses){
                auto& [status, append_response] = response;
                K2ASSERT(log::ltest, status.is2xxOK(), "cannot append data!");
                if (!first_plog){
                    first_plog = true;
                    _initPlogId = append_response.plogId;
                }
                K2LOG_D(log::ltest, "{}, {}", append_response.plogId, append_response.current_offset);
            }
            K2LOG_I(log::ltest, ">>> Test2.1: Write Done");
            return _logStream->read_data_from_plogs(dto::ReadRequest{.start_plogId=_initPlogId, .start_offset=0, .size=2000*10005});
        })
        // read with continuation
        .then([this] (auto&& response){
            uint32_t request_size = 2000*10005;
            dto::LogStreamReadContinuationToken continuation_token;
            auto& [status, read_response] = response;
            continuation_token = std::move(read_response.token);

            K2ASSERT(log::ltest, status.is2xxOK(), "cannot read data!");
            request_size -= read_response.payload.getSize();
            Payload read_payload;
            for (auto& b: read_response.payload.shareAll().release()) {
                read_payload.appendBinary(std::move(b));
            }

            return seastar::do_with(std::move(read_payload), std::move(request_size), std::move(continuation_token), [&] (auto& read_payload, auto& request_size, auto& continuation_token){
                return seastar::do_until(
                    [&] { return request_size == 0; },
                    [&] {
                        return _logStream->read_data_from_plogs(dto::ReadWithTokenRequest{.token=continuation_token, .size=request_size})
                        .then([&] (auto&& response){
                            auto& [status, read_response] = response;
                            K2ASSERT(log::ltest, status.is2xxOK(), "cannot read data!");
                            request_size -= read_response.payload.getSize();
                            continuation_token = std::move(read_response.token);
                            for (auto& b: read_response.payload.shareAll().release()) {
                                read_payload.appendBinary(std::move(b));
                            }
                            return seastar::make_ready_future<>();
                        });
                    }
                )
                .then([&] (){
                    return seastar::make_ready_future<Payload>(std::move(read_payload));
                });
            });
        })
        // write another 1000 Strings to the same log stream, these writes will trigger a plog switch as well
        .then([this, data_to_append] (auto&& payload){
            K2LOG_I(log::ltest, ">>> Test2.2: Read Done");
            String str;
            uint32_t count = 0;
            payload.seek(0);
            while (payload.getDataRemaining() > 0){
                payload.read(str);
                K2EXPECT(log::ltest, str, data_to_append);
                ++count;
            }
            K2EXPECT(log::ltest, count, 2000);
            std::vector<seastar::future<std::tuple<Status, dto::AppendResponse>> > writeFutures;
            for (uint32_t i = 0; i < 2000; ++i){
                Payload payload(Payload::DefaultAllocator(20000));
                payload.write(data_to_append);
                writeFutures.push_back(_logStream->append_data_to_plogs(dto::AppendRequest{.payload=std::move(payload)}));
            }
            return seastar::when_all_succeed(writeFutures.begin(), writeFutures.end());
        })
        // read all the Strings that we wrote before, check whether they are the same
        .then([this, data_to_append] (std::vector<std::tuple<Status, dto::AppendResponse> >&& responses){
            for (auto& response:responses){
                auto& [status, append_response] = response;
                K2ASSERT(log::ltest, status.is2xxOK(), "cannot append data!");
                K2LOG_D(log::ltest, "{}, {}", append_response.plogId, append_response.current_offset);
            }
            K2LOG_I(log::ltest, ">>> Test2.3: Write Done");
            return _logStream->read_data_from_plogs(dto::ReadRequest{.start_plogId=_initPlogId, .start_offset=0, .size=4000*10005});
        })
        // read with continuation
        .then([this] (auto&& response){
            uint32_t request_size = 4000*10005;
            dto::LogStreamReadContinuationToken continuation_token;
            auto& [status, read_response] = response;
            continuation_token = std::move(read_response.token);

            K2ASSERT(log::ltest, status.is2xxOK(), "cannot read data!");
            request_size -= read_response.payload.getSize();
            Payload read_payload;
            for (auto& b: read_response.payload.shareAll().release()) {
                read_payload.appendBinary(std::move(b));
            }

            return seastar::do_with(std::move(read_payload), std::move(request_size), std::move(continuation_token), [&] (auto& read_payload, auto& request_size, auto& continuation_token){
                return seastar::do_until(
                    [&] { return request_size == 0; },
                    [&] {
                        return _logStream->read_data_from_plogs(dto::ReadWithTokenRequest{.token=continuation_token, .size=request_size})
                        .then([&] (auto&& response){
                            auto& [status, read_response] = response;
                            K2ASSERT(log::ltest, status.is2xxOK(), "cannot read data!");
                            request_size -= read_response.payload.getSize();
                            continuation_token = std::move(read_response.token);
                            for (auto& b: read_response.payload.shareAll().release()) {
                                read_payload.appendBinary(std::move(b));
                            }
                            return seastar::make_ready_future<>();
                        });
                    }
                )
                .then([&] (){
                    return seastar::make_ready_future<Payload>(std::move(read_payload));
                });
            });
        })
        // check weather the read results is the same as we wrote before
        .then([this, data_to_append] (auto&& payload){
            K2LOG_I(log::ltest, ">>> Test2.4: Read Done");
            String str;
            uint32_t count = 0;
            payload.seek(0);
            while (payload.getDataRemaining() > 0){
                payload.read(str);
                K2EXPECT(log::ltest, str, data_to_append);
                ++count;
            }
            K2EXPECT(log::ltest, count, 4000);
            return seastar::make_ready_future<>();
        });
    }

    seastar::future<> runTest3() {
        K2LOG_I(log::ltest, ">>> Test3: replay the metadata manager and read/write data from/to a specific log stream");
        ConfigVar<String> configEp("cpo_url");
        String cpoUrl = configEp();
        String data_to_append(10000, '3');

        // Replay the entire metadata manager
        _reload_mmgr = std::make_shared<k2::PartitionMetadataMgr>();
        return _reload_mmgr->init(cpoUrl, "Partition-1", "Persistence_Cluster_1")
        // Read all the data from reloaded log stream and check weather they are the same
        .then([this] (auto&& status){
            K2ASSERT(log::ltest, status.is2xxOK(), "cannot replay metadata manager!");
            K2LOG_I(log::ltest, ">>> Test3.1: Replay Done");

            auto response = _reload_mmgr->obtainLogStream(LogStreamType::IndexerSnapshot);
            auto return_status = std::get<0>(response);
            auto logStream = std::get<1>(response);
            if (!return_status.is2xxOK()){
                throw std::runtime_error("cannot obtain LogStream");
            }
            else{
                _reload_logStream = logStream;
            }
            return _reload_logStream->read_data_from_plogs(dto::ReadRequest{.start_plogId=_initPlogId, .start_offset=0, .size=4000*10005});
        })
        // read with continuation
        .then([this] (auto&& response){
            uint32_t request_size = 4000*10005;
            dto::LogStreamReadContinuationToken continuation_token;
            auto& [status, read_response] = response;
            continuation_token = std::move(read_response.token);

            K2ASSERT(log::ltest, status.is2xxOK(), "cannot read data!");
            request_size -= read_response.payload.getSize();
            Payload read_payload;
            for (auto& b: read_response.payload.shareAll().release()) {
                read_payload.appendBinary(std::move(b));
            }

            return seastar::do_with(std::move(read_payload), std::move(request_size), std::move(continuation_token), [&] (auto& read_payload, auto& request_size, auto& continuation_token){
                return seastar::do_until(
                    [&] { return request_size == 0; },
                    [&] {
                        return _reload_logStream->read_data_from_plogs(dto::ReadWithTokenRequest{.token=continuation_token, .size=request_size})
                        .then([&] (auto&& response){
                            auto& [status, read_response] = response;
                            K2ASSERT(log::ltest, status.is2xxOK(), "cannot read data!");
                            request_size -= read_response.payload.getSize();
                            continuation_token = std::move(read_response.token);
                            for (auto& b: read_response.payload.shareAll().release()) {
                                read_payload.appendBinary(std::move(b));
                            }
                            return seastar::make_ready_future<>();
                        });
                    }
                )
                .then([&] (){
                    return seastar::make_ready_future<Payload>(std::move(read_payload));
                });
            });
        })
        // check weather the read results is the same as we wrote before and write data to the reloaded log stream
        .then([this, data_to_append] (auto&& payload){
            String str;
            uint32_t count = 0;
            payload.seek(0);
            while (payload.getDataRemaining() > 0){
                payload.read(str);
                K2EXPECT(log::ltest, str, data_to_append);
                ++count;
            }
            K2EXPECT(log::ltest, count, 4000);
            K2LOG_I(log::ltest, ">>> Test3.2: Read Done");

            std::vector<seastar::future<std::tuple<Status, dto::AppendResponse>> > writeFutures;
            for (uint32_t i = 0; i < 2000; ++i){
                Payload payload(Payload::DefaultAllocator(20000));
                payload.write(data_to_append);
                writeFutures.push_back(_reload_logStream->append_data_to_plogs(dto::AppendRequest{.payload=std::move(payload)}));
            }
            return seastar::when_all_succeed(writeFutures.begin(), writeFutures.end());
        })
        // Read all the data from reloaded log stream
        .then([this, data_to_append] (std::vector<std::tuple<Status, dto::AppendResponse> >&& responses){
            for (auto& response:responses){
                auto& [status, append_response] = response;
                K2ASSERT(log::ltest, status.is2xxOK(), "cannot append data!");
                K2LOG_D(log::ltest, "{}, {}", append_response.plogId, append_response.current_offset);
            }
            K2LOG_I(log::ltest, ">>> Test3.3: Write Done");
            return _reload_logStream->read_data_from_plogs(dto::ReadRequest{.start_plogId=_initPlogId, .start_offset=0, .size=6000*10005});
        })
        // read with continuation
        .then([this] (auto&& response){
            uint32_t request_size = 6000*10005;
            dto::LogStreamReadContinuationToken continuation_token;
            auto& [status, read_response] = response;
            continuation_token = std::move(read_response.token);

            K2ASSERT(log::ltest, status.is2xxOK(), "cannot read data!");
            request_size -= read_response.payload.getSize();
            Payload read_payload;
            for (auto& b: read_response.payload.shareAll().release()) {
                read_payload.appendBinary(std::move(b));
            }

            return seastar::do_with(std::move(read_payload), std::move(request_size), std::move(continuation_token), [&] (auto& read_payload, auto& request_size, auto& continuation_token){
                return seastar::do_until(
                    [&] { return request_size == 0; },
                    [&] {
                        return _reload_logStream->read_data_from_plogs(dto::ReadWithTokenRequest{.token=continuation_token, .size=request_size})
                        .then([&] (auto&& response){
                            auto& [status, read_response] = response;
                            K2ASSERT(log::ltest, status.is2xxOK(), "cannot read data!");
                            request_size -= read_response.payload.getSize();
                            continuation_token = std::move(read_response.token);
                            for (auto& b: read_response.payload.shareAll().release()) {
                                read_payload.appendBinary(std::move(b));
                            }
                            return seastar::make_ready_future<>();
                        });
                    }
                )
                .then([&] (){
                    return seastar::make_ready_future<Payload>(std::move(read_payload));
                });
            });
        })
        // check weather the read results is the same as we wrote before
        .then([this, data_to_append] (auto&& payload){
            K2LOG_I(log::ltest, ">>> Test3.4: Read Done");
            String str;
            uint32_t count = 0;
            payload.seek(0);
            while (payload.getDataRemaining() > 0){
                payload.read(str);
                K2EXPECT(log::ltest, str, data_to_append);
                ++count;
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
