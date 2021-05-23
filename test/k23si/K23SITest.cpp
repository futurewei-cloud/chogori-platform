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

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/module/k23si/Module.h>
#include <k2/cpo/client/CPOClient.h>
#include <seastar/core/sleep.hh>

#include <k2/dto/K23SI.h>
#include <k2/dto/K23SIInspect.h>
#include <k2/dto/Collection.h>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/MessageVerbs.h>
#include "Log.h"

namespace k2 {
struct DataRec {
    String f1;
    String f2;

    bool operator==(const DataRec& o) {
        return f1 == o.f1 && f2 == o.f2;
    }
    K2_DEF_FMT(DataRec, f1, f2);
};

const char* collname = "k23si_test_collection";

class K23SITest {

public:  // application lifespan
    K23SITest() { K2LOG_I(log::k23si, "ctor");}
    ~K23SITest(){ K2LOG_I(log::k23si, "dtor");}

    static seastar::future<dto::Timestamp> getTimeNow() {
        // TODO call TSO service with timeout and retry logic
        auto nsecsSinceEpoch = sys_now_nsec_count();
        return seastar::make_ready_future<dto::Timestamp>(dto::Timestamp(nsecsSinceEpoch, 1550647543, 1000));
    }

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2LOG_I(log::k23si, "stop");
        return std::move(_testFuture);
    }

    seastar::future<> start(){
        K2LOG_I(log::k23si, "start");

        K2EXPECT(log::k23si, _k2ConfigEps().size(), 3);
        for (auto& ep: _k2ConfigEps()) {
            _k2Endpoints.push_back(RPC().getTXEndpoint(ep));
        }

        _cpo_client.init(_cpoConfigEp());
        _cpoEndpoint = RPC().getTXEndpoint(_cpoConfigEp());
        _testTimer.set_callback([this] {
            _testFuture = seastar::make_ready_future()
            .then([this] {
                K2LOG_I(log::k23si, "Creating test collection...");
                auto request = dto::CollectionCreateRequest{
                    .metadata{
                        .name = collname,
                        .hashScheme = dto::HashScheme::HashCRC32C,
                        .storageDriver = dto::StorageDriver::K23SI,
                        .capacity{
                            .dataCapacityMegaBytes = 1000,
                            .readIOPs = 100000,
                            .writeIOPs = 100000
                        },
                        .retentionPeriod = Duration(1h)*90*24
                    },
                    .clusterEndpoints = _k2ConfigEps(),
                    .rangeEnds{}
                };
                return RPC().callRPC<dto::CollectionCreateRequest, dto::CollectionCreateResponse>
                        (dto::Verbs::CPO_COLLECTION_CREATE, request, *_cpoEndpoint, 1s);
            })
            .then([](auto&& response) {
                // response for collection create
                auto& [status, resp] = response;
                K2EXPECT(log::k23si, status, Statuses::S201_Created);
                // wait for collection to get assigned
                return seastar::sleep(100ms);
            })
            .then([this] {
                // check to make sure the collection is assigned
                auto request = dto::CollectionGetRequest{.name = collname};
                return RPC().callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>
                    (dto::Verbs::CPO_COLLECTION_GET, request, *_cpoEndpoint, 100ms);
            })
            .then([this](auto&& response) {
                // check collection was assigned
                auto& [status, resp] = response;
                K2EXPECT(log::k23si, status, Statuses::S200_OK);
                _pgetter = dto::PartitionGetter(std::move(resp.collection));
            })
            .then([this] () {
                _schema.name = "schema";
                _schema.version = 1;
                _schema.fields = std::vector<dto::SchemaField> {
                        {dto::FieldType::STRING, "partition", false, false},
                        {dto::FieldType::STRING, "range", false, false},
                        {dto::FieldType::STRING, "f1", false, false},
                        {dto::FieldType::STRING, "f2", false, false},
                };

                _schema.setPartitionKeyFieldsByName(std::vector<String>{"partition"});
                _schema.setRangeKeyFieldsByName(std::vector<String> {"range"});

                dto::CreateSchemaRequest request{ collname, _schema };
                return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *_cpoEndpoint, 1s);
            })
            .then([] (auto&& response) {
                auto& [status, resp] = response;
                K2EXPECT(log::k23si, status, Statuses::S200_OK);
            })
            .then([this] { return runScenario00(); })
            .then([this] { return runScenario01(); })
            .then([this] { return runScenario02(); })
            .then([this] { return runScenario03(); })
            .then([this] { return runScenario04(); })
            .then([this] { return runScenario05(); })
            .then([this] {
                K2LOG_I(log::k23si, "======= All tests passed ========");
                exitcode = 0;
            })
            .handle_exception([this](auto exc) {
                try {
                    std::rethrow_exception(exc);
                } catch (RPCDispatcher::RequestTimeoutException& exc) {
                    K2LOG_E(log::k23si, "======= Test failed due to timeout ========");
                    exitcode = -1;
                } catch (std::exception& e) {
                    K2LOG_E(log::k23si, "======= Test failed with exception [{}] ========", e.what());
                    exitcode = -1;
                }
            })
            .finally([this] {
                K2LOG_I(log::k23si, "======= Test ended ========");
                seastar::engine().exit(exitcode);
            });
        });

        _testTimer.arm(0ms);
        return seastar::make_ready_future();
    }

private:
    int exitcode = -1;
    ConfigVar<std::vector<String>> _k2ConfigEps{"k2_endpoints"};
    ConfigVar<String> _cpoConfigEp{"cpo_endpoint"};

    std::vector<std::unique_ptr<k2::TXEndpoint>> _k2Endpoints;
    std::unique_ptr<k2::TXEndpoint> _cpoEndpoint;

    seastar::timer<> _testTimer;
    seastar::future<> _testFuture = seastar::make_ready_future();

    CPOClient _cpo_client;
    dto::PartitionGetter _pgetter;
    dto::Schema _schema;

    seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
    doWrite(const dto::Key& key, const DataRec& data, const dto::K23SI_MTR& mtr, const dto::Key& trh, const String& cname, bool isDelete, bool isTRH) {
        uint64_t id = 0;

        SKVRecord record(cname, std::make_shared<k2::dto::Schema>(_schema));
        record.serializeNext<String>(key.partitionKey);
        record.serializeNext<String>(key.rangeKey);
        record.serializeNext<String>(data.f1);
        record.serializeNext<String>(data.f2);
        K2LOG_D(log::k23si, "cname={}, key={}, phash={}", cname, key, key.partitionHash())
        auto& part = _pgetter.getPartitionForKey(key);
        dto::K23SIWriteRequest request {
            .pvid = part.partition->keyRangeV.pvid,
            .collectionName = cname,
            .mtr = mtr,
            .trh = trh,
            .trhCollection = cname,
            .isDelete = isDelete,
            .designateTRH = isTRH,
            .rejectIfExists = false,
            .request_id = id++,
            .key = key,
            .value = std::move(record.storage),
            .fieldsForPartialUpdate = std::vector<uint32_t>()
        };
        return RPC().callRPC<dto::K23SIWriteRequest, dto::K23SIWriteResponse>(dto::Verbs::K23SI_WRITE, request, *part.preferredEndpoint, 100ms);
    }

    seastar::future<std::tuple<Status, DataRec>>
    doRead(const dto::Key& key, const dto::K23SI_MTR& mtr, const String& cname) {
        K2LOG_D(log::k23si, "key={}, phash={}", key, key.partitionHash())
        auto& part = _pgetter.getPartitionForKey(key);
        dto::K23SIReadRequest request {
            .pvid = part.partition->keyRangeV.pvid,
            .collectionName = cname,
            .mtr =mtr,
            .key=key
        };

        return RPC().callRPC<dto::K23SIReadRequest, dto::K23SIReadResponse>
            (dto::Verbs::K23SI_READ, request, *part.preferredEndpoint, 100ms)
        .then([this] (auto&& response) {
            auto& [status, resp] = response;
            if (!status.is2xxOK()) {
                return std::make_tuple(std::move(status), DataRec{});
            }

            SKVRecord record(collname, std::make_shared<k2::dto::Schema>(_schema), std::move(resp.value), true);
            record.seekField(2);
            DataRec rec = { *(record.deserializeNext<String>()), *(record.deserializeNext<String>()) };
            return std::make_tuple(std::move(status), std::move(rec));
        });
    }

    seastar::future<std::tuple<Status, dto::K23SITxnEndResponse>>
    doEnd(dto::Key trh, dto::K23SI_MTR mtr, const String& cname, bool isCommit, std::vector<dto::Key> writeKeys) {
        K2LOG_D(log::k23si, "key={}, phash={}", trh, trh.partitionHash())
        auto& part = _pgetter.getPartitionForKey(trh);
        std::unordered_map<String, std::unordered_set<dto::KeyRangeVersion>> writeRanges;

        for (auto& key: writeKeys) {
            auto& krv = _pgetter.getPartitionForKey(key).partition->keyRangeV;
            writeRanges[cname].insert(krv);
        }
        dto::K23SITxnEndRequest request;
        request.pvid = part.partition->keyRangeV.pvid;
        request.collectionName = cname;
        request.mtr = mtr;
        request.key = trh;
        request.action = isCommit ? dto::EndAction::Commit : dto::EndAction::Abort;
        request.writeRanges = std::move(writeRanges);
        return RPC().callRPC<dto::K23SITxnEndRequest, dto::K23SITxnEndResponse>(dto::Verbs::K23SI_TXN_END, request, *part.preferredEndpoint, 100ms);
    }


    seastar::future<std::tuple<Status, dto::K23SIInspectRecordsResponse>>
    doRequestRecords(dto::Key key) {
        auto* request = new dto::K23SIInspectRecordsRequest {
            dto::PVID{}, // Will be filled in by PartitionRequest
            k2::String(collname),
            std::move(key)
        };

        return _cpo_client.partitionRequest
            <dto::K23SIInspectRecordsRequest, dto::K23SIInspectRecordsResponse, dto::Verbs::K23SI_INSPECT_RECORDS>
            (Deadline<>(1s), *request).
            finally([request] () { delete request; });
    }

    seastar::future<std::tuple<Status, dto::K23SIInspectTxnResponse>>
    doRequestTRH(dto::Key trh, dto::K23SI_MTR mtr) {
        auto* request = new dto::K23SIInspectTxnRequest {
            dto::PVID{}, // Will be filled in by PartitionRequest
            k2::String(collname),
            std::move(trh),
            mtr.timestamp
        };

        return _cpo_client.partitionRequest
            <dto::K23SIInspectTxnRequest, dto::K23SIInspectTxnResponse, dto::Verbs::K23SI_INSPECT_TXN>
            (Deadline<>(1s), *request).
            finally([request] () { delete request; });
    }

public: // tests

seastar::future<> runScenario00() {
    K2LOG_I(log::k23si, "Scenario 00: unassigned nodes");
    return seastar::make_ready_future();
}

seastar::future<> runScenario01() {
    K2LOG_I(log::k23si, "Scenario 01: empty node");
    return seastar::make_ready_future()
    .then([this] {
        return doRequestRecords({"schema", "Key1", "rKey1"}).
        then([this] (auto&& response) {
            auto& [status, k2response] = response;
            K2EXPECT(log::k23si, status, Statuses::S404_Not_Found);
            K2EXPECT(log::k23si, k2response.records.size(), 0);
            K2LOG_I(log::k23si, "doRequestRecords done");
            return seastar::make_ready_future<>();
        });
    })
    .then([this] {
        return doRead({"schema", "Key1","rKey1"},{dto::Timestamp(100000, 1, 1000),dto::TxnPriority::Medium}, "somebadcoll");
    })
    .then([](auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(log::k23si, status, Statuses::S410_Gone);
    });
    /*
    Scenario 1: empty node:

        - read wrong partition
            expect 410 Gone
        - read out-of-date partition version
            expect 410 Gone
        - read empty key wrong partition
            expect 410 Gone
        - read empty key out-of-date partition version
            expect 410 Gone
        - read with empty key
            expect 404 not found
        - read with only partitionKey
            expect 404 not found
        - read with partition and range key
            expect 404 not found
    */
}
seastar::future<> runScenario02() {
    K2LOG_I(log::k23si, "Scenario 02");
    /*
    Scenario 2: node with single version data:
            - ("pkey1","", v10) -> commited
            - ("pkey2","", v11) -> WI
            - ("pkey3","", v12) -> aborted but not cleaned
            - ("pkey3","", v13) -> aborted but not cleaned
cases requiring client to refresh collection pmap
        - read all valid keys; wrong collection, wrong partition
            expect 410 Gone
        - read invalid key; wrong collection, wrong partition
            expect 410 Gone
        - read all valid keys; wrong collection, out-of-date partition
            expect 410 Gone
        - read invalid key; wrong collection, out-of-date partition
            expect 410 Gone
        - read all valid keys; correct collection, wrong partition
            expect 410 Gone
        - read invalid key; correct collection, wrong partition
            expect 410 Gone
        - read all valid keys; correct collection, out-of-date partition version
            expect 410 Gone
        - read invalid key; correct collection, out-of-date partition version
            expect 410 Gone
        - read empty key wrong partition
            expect 410 Gone
        - read empty key out-of-date partition version
            expect 410 Gone

        - read ("pkey1", "", v10)
            expect 200 OK with data
        - read ("pkey1", "", v11)
            expect 200 OK with data
        - read ("pkey1", "", v9)
            expect 404 not found

        - read ("pkey1", "", v10)
            expect 200 OK with data
        - read ("pkey1", "", v11)
            expect 200 OK with data
        - read ("pkey1", "", v9)
            expect 404 not found


    write:
        - write inside an already read history
            expect S403_Forbidden
    */

    return seastar::make_ready_future()
    .then([this] {
        return getTimeNow();
    })
    .then([this] (dto::Timestamp&& ts) {
        return seastar::do_with(
            dto::K23SI_MTR{
                .timestamp = std::move(ts),
                .priority = dto::TxnPriority::Medium},
            dto::Key{.schemaName = "schema", .partitionKey = "Key1", .rangeKey = "rKey1"},
            dto::Key{.schemaName = "schema", .partitionKey = "Key1", .rangeKey = "rKey1"},
            DataRec{.f1="field1", .f2="field2"},
            [this] (dto::K23SI_MTR& mtr, dto::Key& key, dto::Key& trh, DataRec& rec) {
                return doWrite(key, rec, mtr, trh, collname, false, true)
                .then([this](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                // Verify there is one WI on node
                .then([this, &key] {
                    return doRequestRecords(key).
                    then([this] (auto&& response) {
                        auto& [status, k2response] = response;
                        K2EXPECT(log::k23si, status, Statuses::S200_OK);
                        K2EXPECT(log::k23si, k2response.records.size(), 1);
                        return seastar::make_ready_future<>();
                    });
                })
                // Verify the Txn is InProgress
                .then([this, &trh, &mtr] {
                        return doRequestTRH(trh, mtr).
                        then([this] (auto&& response) {
                            auto& [status, k2response] = response;
                            K2EXPECT(log::k23si, status, Statuses::S200_OK);
                            K2EXPECT(log::k23si, k2response.state, k2::dto::TxnRecordState::InProgress);
                            return seastar::make_ready_future<>();
                        });
                })
                .then([this, &trh, &mtr, &key] {
                    return doEnd(trh, mtr, collname, true, {key});
                })
                .then([this, &key, &mtr](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    return doRead(key, mtr, collname);
                })
                .then([&rec](auto&& response) {
                    auto& [status, value] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    K2EXPECT(log::k23si, value, rec);
                });
        });
    });
    return seastar::make_ready_future();
}
seastar::future<> runScenario03() {
    K2LOG_I(log::k23si, "Scenario 03");
    return seastar::make_ready_future();
}
seastar::future<> runScenario04() {
    K2LOG_I(log::k23si, "Scenario 04: concurrent transactions same keys");
    return seastar::do_with(
        dto::K23SI_MTR{},
        dto::Key{"schema", "s04-pkey1", "rkey1"},
        dto::K23SI_MTR{},
        dto::Key{"schema", "s04-pkey1", "rkey1"},
        [this](auto& m1, auto& k1, auto& m2, auto& k2) {
            return getTimeNow()
                .then([&](dto::Timestamp&& ts) {
                    m1.timestamp = ts;
                    m1.priority = dto::TxnPriority::Medium;
                    return doWrite(k1, {"fk1", "f2"}, m1, k1, collname, false, true);
                })
                .then([&](auto&& result) {
                    auto& [status, r] = result;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                    return getTimeNow();
                })
                .then([&](dto::Timestamp&& ts) {
                    m2.timestamp = ts;
                    m2.priority = dto::TxnPriority::Medium;
                    return doWrite(k2, {"fk2", "f2"}, m2, k2, collname, false, true);
                })
                .then([&](auto&& result) {
                    auto& [status, r] = result;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                .then([&] () {
                    return doRequestRecords(k2);
                })
                .then([&] (auto&& response) {
                    // Verify there is a single WI for key
                    auto& [status, k2response] = response;
                    K2EXPECT(log::k23si, status, Statuses::S200_OK);
                    K2EXPECT(log::k23si, k2response.records.size(), 1);

                    return doRequestTRH(k2, m2);
                })
                .then([&] (auto&& response) {
                    // Verify newer txn is still InProgress
                    auto& [status, k2response] = response;
                    K2EXPECT(log::k23si, status, Statuses::S200_OK);
                    K2EXPECT(log::k23si, k2response.state, k2::dto::TxnRecordState::InProgress);

                    return seastar::when_all(doEnd(k1, m1, collname, true, {k1}), doEnd(k2, m2, collname, true, {k2}));
                })
                .then([&](auto&& result) mutable {
                    auto& [r1, r2] = result;
                    // apparently, have to move these out of the incoming futures since get0() returns an rvalue
                    auto [status1, result1] = r1.get0();
                    auto [status2, result2] = r2.get0();
                    // first txn gets aborted in this scenario since on push, the newer txn wins. The status should not be OK
                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::OperationNotAllowed);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                    // do end for first txn with Abort
                    return doEnd(k1, m1, collname, false, {k1});
                })
                .then([&](auto&& result) {
                    auto& [status, resp] = result;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::KeyNotFound);

                    return seastar::when_all(doRead(k1, m1, collname), doRead(k2, m2, collname));
                })
                .then([&](auto&& result) mutable {
                    auto& [r1, r2] = result;
                    auto [status1, value1] = r1.get0();
                    auto [status2, value2] = r2.get0();
                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::KeyNotFound);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                    DataRec d2{"fk2", "f2"};
                    K2EXPECT(log::k23si, value2, d2);
                });
        });
}

seastar::future<> runScenario05() {
    K2LOG_I(log::k23si, "Scenario 05: concurrent transactions different keys");
    return seastar::do_with(
        dto::K23SI_MTR{},
        dto::Key{"schema", "s05-pkey1", "rkey1"},
        dto::K23SI_MTR{},
        dto::Key{"schema", "s05-pkey1", "rkey2"},
        [this](auto& m1, auto& k1, auto& m2, auto& k2) {
            return getTimeNow()
                .then([&](dto::Timestamp&& ts) {
                    m1.timestamp = ts;
                    m1.priority = dto::TxnPriority::Medium;
                    return doWrite(k1, {"fk1","f2"}, m1, k1, collname, false, true);
                })
                .then([&](auto&& result) {
                    auto& [status, r] = result;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                    return getTimeNow();
                })
                .then([&](dto::Timestamp&& ts) {
                    m2.timestamp = ts;
                    m2.priority = dto::TxnPriority::Medium;
                    return doWrite(k2, {"fk2", "f2"}, m2, k2, collname, false, true);
                })
                .then([&](auto&& result) {
                    auto& [status, r] = result;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);

                    return doRequestTRH(k1, m1);
                })
                // Verify both txns are InProgress
                .then([&] (auto&& response) {
                    auto& [status, k2response] = response;
                    K2EXPECT(log::k23si, status, Statuses::S200_OK);
                    K2EXPECT(log::k23si, k2response.state, k2::dto::TxnRecordState::InProgress);

                    return doRequestTRH(k2, m2);
                })
                .then([&] (auto&& response) {
                    auto& [status, k2response] = response;
                    K2EXPECT(log::k23si, status, Statuses::S200_OK);
                    K2EXPECT(log::k23si, k2response.state, k2::dto::TxnRecordState::InProgress);

                    return seastar::when_all(doEnd(k1, m1, collname, true, {k1}), doEnd(k2, m2, collname, true, {k2}));
                })
                .then([&](auto&& result) mutable {
                    auto& [r1, r2] = result;
                    auto [status1, result1] = r1.get0();
                    auto [status2, result2] = r2.get0();
                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::OK);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                    return seastar::when_all(doRead(k1, m1, collname), doRead(k2, m2, collname));
                })
                .then([&](auto&& result) mutable {
                    auto& [r1, r2] = result;
                    auto [status1, value1] = r1.get0();
                    auto [status2, value2] = r2.get0();
                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::OK);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                    DataRec d1{"fk1", "f2"};
                    DataRec d2{"fk2", "f2"};
                    K2EXPECT(log::k23si, value1, d1);
                    K2EXPECT(log::k23si, value2, d2);
                });
        });
}

};  // class K23SITest
} // ns k2

int main(int argc, char** argv) {
    k2::App app("K23SITest");
    app.addOptions()("k2_endpoints", bpo::value<std::vector<k2::String>>()->multitoken(), "The endpoints of the k2 cluster");
    app.addOptions()("cpo_endpoint", bpo::value<k2::String>(), "The endpoint of the CPO");
    app.addApplet<k2::K23SITest>();
    return app.start(argc, argv);
}
