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

#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>

#include <k2/dto/K23SI.h>
#include <k2/dto/K23SIInspect.h>
#include <k2/dto/Collection.h>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/module/k23si/Module.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/transport/RPCDispatcher.h>  // for RPC
#include "Log.h"

namespace k2{
const String badCname = "bad_collection_name";
const String collname = "3si_txn_collection";
const String s02nd_cname = "Second_" + collname;
dto::Schema _schema {
            .name = "schema",
            .version = 1,
            .fields = std::vector<dto::SchemaField> {
                {dto::FieldType::STRING, "partition", false, false},
                {dto::FieldType::STRING, "range", false, false},
                {dto::FieldType::STRING, "f1", false, false},
                {dto::FieldType::STRING, "f2", false, false}
            },
            .partitionKeyFields = std::vector<uint32_t> { 0 },
            .rangeKeyFields = std::vector<uint32_t> { 1 }
};


const dto::HashScheme hashScheme = dto::HashScheme::HashCRC32C;

struct DataRec {
    String f1;
    String f2;
    K2_PAYLOAD_FIELDS(f1, f2);
    bool operator==(const DataRec& o) {
        return f1 == o.f1 && f2 == o.f2;
    }
    K2_DEF_FMT(DataRec, f1, f2);
};
}

namespace k2 {
enum class ErrorCaseOpt: uint8_t {
    NoInjection,
    WrongPartId,        // wrong partition index
    WrongPartVer,       // wrong partition version
    PartMismatchKey,    // key doesn't belong to partition (based on hashing)
    ObsoletePart,       // out-of-date partition version
};


class txn_testing {

public: // application
    txn_testing() { K2LOG_I(log::k23si, "ctor"); }
    ~txn_testing(){ K2LOG_I(log::k23si, "dtor"); }

    static seastar::future<dto::Timestamp> getTimeNow() {
        // TODO call TSO service with timeout and retry logic
        auto nsecsSinceEpoch = sys_now_nsec_count();
        return seastar::make_ready_future<dto::Timestamp>(dto::Timestamp(nsecsSinceEpoch, 123, 1000));
    }

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2LOG_I(log::k23si, "stop");
        return std::move(_testFuture);
    }

    seastar::future<> start(){
        K2LOG_I(log::k23si, "start txn_testing..");

        K2EXPECT(log::k23si, _k2ConfigEps().size(), 3);
        for (auto& ep: _k2ConfigEps()) {
            _k2Endpoints.push_back(RPC().getTXEndpoint(ep));
        }

        _cpoEndpoint = RPC().getTXEndpoint(_cpoConfigEp());
        _testTimer.set_callback([this] {
            _testFuture = testScenario00()
            .then([this] { return testScenario01(); })
            .then([this] { return testScenario02(); })
            .then([this] { return testScenario03(); })
            .then([this] { return testScenario04(); })
            .then([this] { return testScenario05(); })
            .then([this] { return testScenario06(); })
            .then([this] { return testScenario07(); })
            .then([this] { return testScenario08(); })
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
    ConfigVar<std::vector<String>> _k2ConfigEps{"k2_endpoints"};
    ConfigVar<String> _cpoConfigEp{"cpo_endpoint"};

    seastar::future<> _testFuture = seastar::make_ready_future();
    seastar::timer<> _testTimer;

    std::vector<std::unique_ptr<k2::TXEndpoint>> _k2Endpoints;
    std::unique_ptr<k2::TXEndpoint> _cpoEndpoint;

    int exitcode = -1;

    dto::PartitionGetter _pgetter;

    // injection parameters for error cases
    dto::Key wrongkey{.schemaName = "schema", .partitionKey = "SC00_wrong_pKey1", .rangeKey = "SC00_wrong_rKey1"}; // wrong partition: id(p1) against p2

    seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
    doWrite(const dto::Key& key, const DataRec& data, const dto::K23SI_MTR mtr, const dto::Key& trh, const String& cname, bool isDelete, bool isTRH, ErrorCaseOpt errOpt) {
        static uint32_t id = 0;

        SKVRecord record(cname, std::make_shared<k2::dto::Schema>(_schema));
        record.serializeNext<String>(key.partitionKey);
        record.serializeNext<String>(key.rangeKey);
        record.serializeNext<String>(data.f1);
        record.serializeNext<String>(data.f2);
        K2LOG_D(log::k23si, "cname={}, key={}, phash={}", cname, key, key.partitionHash());
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

        switch (errOpt) {
        case ErrorCaseOpt::NoInjection:
            break;
        case ErrorCaseOpt::WrongPartId: {
            request.pvid.id = (request.pvid.id + 1) % 3;
            break;
        }
        case ErrorCaseOpt::PartMismatchKey: {
            request.key = wrongkey;
            break;
        }
        case ErrorCaseOpt::ObsoletePart: {
            request.pvid.rangeVersion -= 1;
            request.pvid.assignmentVersion -= 1;
            break;
        }
        default: {
            K2ASSERT(log::k23si, false, "doWrite() incorrect parameter ErrorCaseOpt.");
            break;
        } // end default
        } // end switch
        return RPC().callRPC<dto::K23SIWriteRequest, dto::K23SIWriteResponse>
                (dto::Verbs::K23SI_WRITE, request, *part.preferredEndpoint, 100ms);
    }

    seastar::future<std::tuple<Status, DataRec>>
    doRead(const dto::Key& key, const dto::K23SI_MTR& mtr, const String& cname, ErrorCaseOpt errOpt) {
        K2LOG_D(log::k23si, "key={}, phash={}", key, key.partitionHash());
        auto& part = _pgetter.getPartitionForKey(key);
        dto::K23SIReadRequest request {
            .pvid = part.partition->keyRangeV.pvid,
            .collectionName = cname,
            .mtr =mtr,
            .key=key
        };
        switch (errOpt) {
        case ErrorCaseOpt::NoInjection:
            break;
        case ErrorCaseOpt::WrongPartId: {
            request.pvid.id = (request.pvid.id + 1) % 3;
            break;
        }
        case ErrorCaseOpt::PartMismatchKey: {
            request.key = wrongkey;
            break;
        }
        case ErrorCaseOpt::ObsoletePart: {
            request.pvid.rangeVersion -= 1;
            request.pvid.assignmentVersion -= 1;
            break;
        }
        default: {
            K2ASSERT(log::k23si, false, "doWrite() incorrect parameter ErrorCaseOpt.");
            break;
        } // end default
        } // end switch
        return RPC().callRPC<dto::K23SIReadRequest, dto::K23SIReadResponse>
                (dto::Verbs::K23SI_READ, request, *part.preferredEndpoint, 100ms)
        .then([cname] (auto&& response) {
            auto& [status, resp] = response;
            if (!status.is2xxOK()) {
                return std::make_tuple(std::move(status), DataRec{});
            }

            SKVRecord record(cname, std::make_shared<k2::dto::Schema>(_schema));
            record.storage = std::move(resp.value);
            record.seekField(2);
            DataRec rec = { *(record.deserializeNext<String>()), *(record.deserializeNext<String>()) };
            return std::make_tuple(std::move(status), std::move(rec));
        });
    }

    seastar::future<std::tuple<Status, dto::K23SITxnPushResponse>>
    doPush(dto::Key key, String cname, dto::K23SI_MTR incumbent, dto::K23SI_MTR challenger, ErrorCaseOpt errOpt) {
        K2LOG_D(log::k23si, "key={}, phash={}", key, key.partitionHash());
        auto& part = _pgetter.getPartitionForKey(key);
        dto::K23SITxnPushRequest request;
        request.pvid = part.partition->keyRangeV.pvid;
        request.collectionName = cname;
        request.key = key;
        request.incumbentMTR = incumbent;
        request.challengerMTR = challenger;
        switch (errOpt) {
        case ErrorCaseOpt::NoInjection:
            break;
        case ErrorCaseOpt::WrongPartId: {
            request.pvid.id = (request.pvid.id + 1) % 3;
            break;
        }
        case ErrorCaseOpt::PartMismatchKey: {
            request.key = wrongkey;
            break;
        }
        case ErrorCaseOpt::ObsoletePart: {
            request.pvid.rangeVersion -= 1;
            request.pvid.assignmentVersion -= 1;
            break;
         }
        default: {
            K2ASSERT(log::k23si, false, "doWrite() incorrect parameter ErrorCaseOpt.");
            break;
        } // end default
        } // end switch
        return RPC().callRPC<dto::K23SITxnPushRequest, dto::K23SITxnPushResponse>
                (dto::Verbs::K23SI_TXN_PUSH, request, *part.preferredEndpoint, 100ms);
    }

    seastar::future<std::tuple<Status, dto::K23SITxnEndResponse>>
    doEnd(dto::Key trh, dto::K23SI_MTR mtr, String cname, bool isCommit, std::vector<dto::Key> wkeys, Duration dur, ErrorCaseOpt errOpt) {
        K2LOG_D(log::k23si, "key={}, phash={}", trh, trh.partitionHash());

        auto& part = _pgetter.getPartitionForKey(trh);
        std::unordered_map<String, std::unordered_set<dto::KeyRangeVersion>> writeRanges;

        for (auto& key: wkeys) {
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
        if(dur == Duration(0)) {
            request.syncFinalize = true;
            request.timeToFinalize = Duration(0);
        } else {
            request.syncFinalize = false;
            request.timeToFinalize = dur;
        }

        switch (errOpt) {
        case ErrorCaseOpt::NoInjection:
            break;
        case ErrorCaseOpt::WrongPartId: {
            request.pvid.id = (request.pvid.id + 1) % 3;
            break;
        }
        case ErrorCaseOpt::PartMismatchKey: {
            request.key = wrongkey;
            break;
        }
        case ErrorCaseOpt::ObsoletePart: {
            request.pvid.rangeVersion -= 1;
            request.pvid.assignmentVersion -= 1;
            break;
        }
        default: {
            K2ASSERT(log::k23si, false, "doWrite() incorrect parameter ErrorCaseOpt.");
            break;
        } // end default
        } // end switch
        return RPC().callRPC<dto::K23SITxnEndRequest, dto::K23SITxnEndResponse>
                (dto::Verbs::K23SI_TXN_END, request, *part.preferredEndpoint, 100ms);
    }

    seastar::future<std::tuple<Status, dto::K23SITxnFinalizeResponse>>
    doFinalize(dto::Key key, dto::K23SI_MTR mtr, String cname, bool isCommit, ErrorCaseOpt errOpt) {
        K2LOG_D(log::k23si, "key={}, phash={}", key, key.partitionHash());
        auto& part = _pgetter.getPartitionForKey(key);
        dto::PVID pvid{};
        if (!key.partitionKey.empty() && part.partition != nullptr) {
            pvid = part.partition->keyRangeV.pvid;
        }
        dto::K23SITxnFinalizeRequest request {
            .pvid = pvid,
            .collectionName = cname,
            .txnTimestamp = mtr.timestamp,
            .action = isCommit ? dto::EndAction::Commit : dto::EndAction::Abort
        };
        switch (errOpt) {
        case ErrorCaseOpt::NoInjection:
            break;
        case ErrorCaseOpt::WrongPartId: {
            request.pvid.id = (request.pvid.id + 1) % 3;
            break;
        }
        case ErrorCaseOpt::PartMismatchKey: {
            request.pvid.id = (request.pvid.id + 1) % 3;
            break;
        }
        case ErrorCaseOpt::ObsoletePart: {
            request.pvid.rangeVersion -= 1;
            request.pvid.assignmentVersion -= 1;
            break;
        }
        default: {
            K2ASSERT(log::k23si, false, "doWrite() incorrect parameter ErrorCaseOpt.");
            break;
        } // end default
        } // end switch
        return RPC().callRPC<dto::K23SITxnFinalizeRequest, dto::K23SITxnFinalizeResponse>
                (dto::Verbs::K23SI_TXN_FINALIZE, request, *part.preferredEndpoint, 100ms);
    }

    seastar::future<std::tuple<Status, dto::K23SITxnHeartbeatResponse>>
    doHeartbeat(dto::Key key, dto::K23SI_MTR mtr, String cname, ErrorCaseOpt errOpt) {
        K2LOG_D(log::k23si, "key={}, phash={}", key, key.partitionHash());
        auto& part = _pgetter.getPartitionForKey(key);
        dto::K23SITxnHeartbeatRequest request{
            .pvid = part.partition->keyRangeV.pvid,
            .collectionName = cname,
            .key = key,
            .mtr = mtr
        };
        switch (errOpt) {
        case ErrorCaseOpt::NoInjection:
            break;
        case ErrorCaseOpt::WrongPartId: {
            request.pvid.id = (request.pvid.id + 1) % 3;
            break;
        }
        case ErrorCaseOpt::PartMismatchKey: {
            request.key = wrongkey;
            break;
        }
        case ErrorCaseOpt::ObsoletePart: {
            request.pvid.rangeVersion -= 1;
            request.pvid.assignmentVersion -= 1;
            break;
        }
        default: {
            K2ASSERT(log::k23si, false, "doWrite() incorrect parameter ErrorCaseOpt.");
            break;
        } // end default
        } // end switch
        return RPC().callRPC<dto::K23SITxnHeartbeatRequest, dto::K23SITxnHeartbeatResponse>
                (dto::Verbs::K23SI_TXN_HEARTBEAT, request, *part.preferredEndpoint, 100ms);
    }

    seastar::future<std::tuple<Status, dto::K23SIInspectRecordsResponse>>
    doInspectRecords(const dto::Key& key, const String& cname) {
        auto& part = _pgetter.getPartitionForKey(key);
        dto::K23SIInspectRecordsRequest request{
            .pvid = part.partition->keyRangeV.pvid,
            .collectionName = cname,
            .key = key
        };
        return RPC().callRPC<dto::K23SIInspectRecordsRequest, dto::K23SIInspectRecordsResponse>
                (dto::Verbs::K23SI_INSPECT_RECORDS, request, *part.preferredEndpoint, 100ms);
    }

    // The key parameter is only used for routing, it is not part of the request
    seastar::future<std::tuple<Status, dto::K23SIInspectWIsResponse>>
    doInspectWIs(const dto::Key& key) {
        auto& part = _pgetter.getPartitionForKey(key);
        dto::K23SIInspectWIsRequest request;
        return RPC().callRPC<dto::K23SIInspectWIsRequest, dto::K23SIInspectWIsResponse>
                (dto::Verbs::K23SI_INSPECT_WIS, request, *part.preferredEndpoint, 100ms);
    }

    seastar::future<std::tuple<Status, dto::K23SIInspectTxnResponse>>
    doInspectTxn(const dto::Key& key, const dto::K23SI_MTR& mtr, const String& cname) {
        auto& part = _pgetter.getPartitionForKey(key);
        dto::K23SIInspectTxnRequest request {
            .pvid = part.partition->keyRangeV.pvid,
            .collectionName = cname,
            .key = key,
            .timestamp = mtr.timestamp
        };
        return RPC().callRPC<dto::K23SIInspectTxnRequest, dto::K23SIInspectTxnResponse>
                (dto::Verbs::K23SI_INSPECT_TXN, request, *part.preferredEndpoint, 100ms);
    }


public: // test scenario

// Any request (READ, WRITE, PUSH, END, FINALIZE, HEARTBEAT) should observe a timeout(404_not_found)
// example of command: CPO_COLLECTION_GET & K23SI_WRITE
seastar::future<> testScenario00() {
    K2LOG_I(log::k23si, "+++++++ TestScenario 00: unassigned nodes +++++++");
    K2LOG_I(log::k23si, "--->Test SETUP: start a cluster but don't create a collection. Any requests observe a timeout.");

    return seastar::make_ready_future()
    .then([this] {
        // command: K23SI_WRITE
        K2LOG_I(log::k23si, "Test case SC00_1: K23SI_WRITE");
        return getTimeNow()
        .then([&](dto::Timestamp&& ts) {
            return seastar::do_with(
                dto::K23SI_MTR{
                    .timestamp = std::move(ts),
                    .priority = dto::TxnPriority::Medium
                },
                dto::Key{.schemaName = "schema", .partitionKey = "SC00_pKey1", .rangeKey = "SC00_rKey1"},
                dto::Key{.schemaName = "schema", .partitionKey = "SC00_pKey1", .rangeKey = "SC00_rKey1"},
                [this](dto::K23SI_MTR& mtr, dto::Key& key, dto::Key& trh){
                    dto::K23SIWriteRequest request {
                        .pvid{},
                        .collectionName = collname,
                        .mtr = mtr,
                        .trh = trh,
                        .trhCollection = collname,
                        .isDelete = false,
                        .designateTRH = true,
                        .rejectIfExists=false,
                        .request_id=0,
                        .key = key,
                        .value{},
                        .fieldsForPartialUpdate{}
                    };
                    return RPC().callRPC<dto::K23SIWriteRequest, dto::K23SIWriteResponse>(dto::Verbs::K23SI_WRITE, request, *_k2Endpoints[0], 100ms)
                    .then([this](auto&& response) {
                        // response: K23SI_WRITE
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, Statuses::S503_Service_Unavailable);
                    });
                }
            );
        });
    }) // end K23SI_WRITE
    .then([this] {
        // command: K23SI_READ
        K2LOG_I(log::k23si, "Test case SC00_2: K23SI_READ");
        dto::K23SIReadRequest request {
            .pvid{},
            .collectionName = collname,
            .mtr{dto::Timestamp(20200828, 1, 1000), dto::TxnPriority::Medium},
            .key{"schema", "SC00_pKey1", "SC00_rKey1"}
        };
        return RPC().callRPC<dto::K23SIReadRequest, dto::K23SIReadResponse>
                (dto::Verbs::K23SI_READ, request, *_k2Endpoints[0], 100ms)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S503_Service_Unavailable);
        });
    }) // end  K23SI_READ
    .then([this] {
        // command: K23SI_TXN_PUSH
        K2LOG_I(log::k23si, "Test case SC00_3: K23SI_TXN_PUSH");
        dto::K23SITxnPushRequest request {
            .pvid{},
            .collectionName = collname,
            .key{"schema", "SC00_pKey1", "SC00_rKey1"},
            .incumbentMTR{dto::Timestamp(20200828, 1, 1000), dto::TxnPriority::Medium},
            .challengerMTR{dto::Timestamp(20200101, 1, 1000), dto::TxnPriority::Medium}
        };
        return RPC().callRPC<dto::K23SITxnPushRequest, dto::K23SITxnPushResponse>
                (dto::Verbs::K23SI_TXN_PUSH, request, *_k2Endpoints[0], 100ms)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S503_Service_Unavailable);
        });
    }) // end K23SI_TXN_PUSH
    .then([this] {
        // command: K23SI_TXN_END
        K2LOG_I(log::k23si, "Test case SC00_4: K23SI_TXN_END");
        dto::K23SITxnEndRequest request {
            .pvid{},
            .collectionName = collname,
            .key{"schema", "SC00_pKey1", "SC00_rKey1"},
            .mtr{dto::Timestamp(20200828, 1, 1000), dto::TxnPriority::Medium},
            .action = dto::EndAction::Abort,
            .writeRanges={{collname,
                          {{}}
                        }},
            .syncFinalize = false
        };
        return RPC().callRPC<dto::K23SITxnEndRequest, dto::K23SITxnEndResponse>
                (dto::Verbs::K23SI_TXN_END, request, *_k2Endpoints[0], 100ms)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S503_Service_Unavailable);
        });
    }) // end K23SI_TXN_END
    .then([this] {
        // command: K23SI_TXN_FINALIZE
        K2LOG_I(log::k23si, "Test case SC00_5: K23SI_TXN_FINALIZE");
        dto::K23SITxnFinalizeRequest request {
            .pvid={},
            .collectionName = collname,
            .txnTimestamp = dto::Timestamp(20200828, 1, 1000),
            .action = dto::EndAction::Abort
        };
        return RPC().callRPC<dto::K23SITxnFinalizeRequest, dto::K23SITxnFinalizeResponse>
                (dto::Verbs::K23SI_TXN_FINALIZE, request, *_k2Endpoints[0], 100ms)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S503_Service_Unavailable);
        });
    }) // end K23SI_TXN_FINALIZE
    .then([this] {
        // command: K23SI_TXN_HEARTBEAT
        K2LOG_I(log::k23si, "Test case SC00_6: K23SI_TXN_HEARTBEAT");
        dto::K23SITxnHeartbeatRequest request {
            .pvid{},
            .collectionName = collname,
            .key{"schema", "SC00_pKey1", "SC00_rKey1"},
            .mtr{dto::Timestamp(20200828, 1, 1000), dto::TxnPriority::Medium},
        };
        return RPC().callRPC<dto::K23SITxnHeartbeatRequest, dto::K23SITxnHeartbeatResponse>
                (dto::Verbs::K23SI_TXN_HEARTBEAT, request, *_k2Endpoints[0], 100ms)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S503_Service_Unavailable);
        });

    });
}

seastar::future<> testScenario01() {
    K2LOG_I(log::k23si, "+++++++ TestScenario 01: assigned node with no data +++++++");
    K2LOG_I(log::k23si, "--->Test SETUP: start a cluster and assign collection. Do not write any data.");

    return seastar::make_ready_future()
    .then([this] {
        // setup: assigned node with no data
        auto request = dto::CollectionCreateRequest{
            .metadata{
                .name = collname,
                .hashScheme = hashScheme,
                .storageDriver = dto::StorageDriver::K23SI,
                .capacity{
                    .dataCapacityMegaBytes = 100,
                    .readIOPs = 100000,
                    .writeIOPs = 100000
                },
                .retentionPeriod = Duration(1s)
            },
            .clusterEndpoints = _k2ConfigEps(),
            .rangeEnds{}
        };
        return RPC().callRPC<dto::CollectionCreateRequest, dto::CollectionCreateResponse>
                (dto::Verbs::CPO_COLLECTION_CREATE, request, *_cpoEndpoint, 1s)
        .then([](auto&& response) {
            // response for collection create
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
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
            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
            _pgetter = dto::PartitionGetter(std::move(resp.collection));
        })
        .then([this] () {
            dto::CreateSchemaRequest request{ collname, _schema };
            return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *_cpoEndpoint, 1s);
        })
        .then([] (auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S200_OK);
        });
    })
    .then([] {
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
    // SC01 case1: OP with bad collection name
        K2LOG_I(log::k23si, "------- SC01.case 01 (OP with bad collection name) -------");
        return seastar::do_with(
            dto::K23SI_MTR {
                .timestamp = std::move(ts),
                .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            DataRec {.f1="SC01_field1", .f2="SC01_field2"},
            [this] (dto::K23SI_MTR& mtr, dto::Key& key, dto::Key& trh, DataRec& rec) {
                // case"bad collection name"  --> OP:WRITE
                return doWrite(key, rec, mtr, trh, badCname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                })
                // case"bad collection name"  --> OP:READ
                .then([this, &key, &mtr] {
                    return doRead(key, mtr, badCname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                })
                // case"bad collection name"  --> OP:PUSH
                .then([this, &key, &mtr] {
                    return doPush(key, badCname, mtr, mtr, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                })
                // case"bad collection name"  --> OP:END
                .then([this, &trh, &key, &mtr] {
                    return doEnd(trh, mtr, badCname, false, {key}, Duration(0s), ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                })
                // case"bad collection name"  --> OP:FINALIZE
                .then([this, &key, &mtr] {
                    return doFinalize(key, mtr, badCname, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                })
                // case"bad collection name"  --> OP:HEARTBEAT
                .then([this, &key, &mtr] {
                    return doHeartbeat(key, mtr, badCname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                });
        }); // end do_with
    }) // end SC-01 case-01
    .then([this] {
    // SC01 case2: OP outside retention window
        K2LOG_I(log::k23si, "------- SC01.case 02 (OP outside retention window) -------");
        K2LOG_I(log::k23si, "Get a stale timestamp(1,000,000) as the stale_ts");
        return seastar::do_with(
            dto::K23SI_MTR {
                .timestamp = {1000000, 123, 1000}, // 1,000,000 is old enough
                .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            DataRec {.f1="SC01_field1", .f2="SC01_field2"},
            [this] (dto::K23SI_MTR& mtr, dto::Key& key, dto::Key& trh, DataRec& rec) {
                // case"stale request"  --> OP:WRITE
                return doWrite(key, rec, mtr, trh, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::AbortRequestTooOld);
                })
                // case"stale request"  --> OP:READ
                .then([this, &key, &mtr] {
                    return doRead(key, mtr, collname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::AbortRequestTooOld);
                    });
                })
                // case"stale request"  --> OP:END
                .then([this, &mtr, &trh, &key] {
                    return doEnd(trh, mtr, collname, false, {key}, Duration(0s), ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::AbortRequestTooOld);
                    });
                })
                // case"stale request"  --> OP:HEARTBEAT
                .then([this, &mtr, &key] {
                    return doHeartbeat(key, mtr, collname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::AbortRequestTooOld);
                    });
                })
                // stale request for PUSH, only validate challenger MTRs
                .then([this, &mtr, &key] {
                    return doPush(key, collname, mtr, mtr, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::AbortRequestTooOld)
                    });
                })
                // stale request for FINALIZE, test Finalize-commit & Finalize-abort
                .then([this, &key, &mtr] {
                    return doFinalize(key, mtr, collname, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::KeyNotFound)
                    })
                    .then([this, &mtr, &key] {
                        return doFinalize(key, mtr, collname, false, ErrorCaseOpt::NoInjection)
                        .then([](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::KeyNotFound)
                        });
                    });
                });
        }); // end do-with
    }) // end SC-01 case-02
    .then([] {
    // SC01 case3: OP with wrong partition
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2LOG_I(log::k23si, "------- SC01.case 03 (OP with wrong partition) -------");
        return seastar::do_with(
            dto::K23SI_MTR {
                .timestamp = std::move(ts),
                .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            DataRec {.f1="SC01_field1", .f2="SC01_field2"},
            [this] (dto::K23SI_MTR& mtr, dto::Key& key, dto::Key& trh, DataRec& rec) {
                // case"wrong partition"  --> OP:WRITE
                return doWrite(key, rec, mtr, trh, collname, false, true, ErrorCaseOpt::WrongPartId)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                })
                // case"wrong partition"  --> OP:READ
                .then([this, &key, &mtr] {
                    return doRead(key, mtr, collname, ErrorCaseOpt::WrongPartId)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                })
                // case"wrong partition"  --> OP:PUSH
                .then([this, &key, &mtr] {
                    return doPush(key, collname, mtr, mtr, ErrorCaseOpt::WrongPartId)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                })
                // case"wrong partition"  --> OP:END
                .then([this, &trh, &key, &mtr] {
                    return doEnd(trh, mtr, collname, false, {key}, Duration(0s), ErrorCaseOpt::WrongPartId)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                })
                // case"wrong partition"  --> OP:FINALIZE
                .then([this, &key, &mtr] {
                    return doFinalize(key, mtr, collname, false, ErrorCaseOpt::WrongPartId)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                })
                // case"wrong partition"  --> OP:HEARTBEAT
                .then([this, &key, &mtr] {
                    return doHeartbeat(key, mtr, collname, ErrorCaseOpt::WrongPartId)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                });

        }); // end do_with
    }) // end sc-01 case-03
    .then([] {
    // SC01 case4: OP key which doesn't belong to partition (based on hashing)
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2LOG_I(log::k23si, "------- SC01.case 04 (key doesn't belong to the partition) -------");
        return seastar::do_with(
            dto::K23SI_MTR {
                .timestamp = std::move(ts),
                .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            DataRec {.f1="SC01_field1", .f2="SC01_field2"},
            [this] (dto::K23SI_MTR& mtr, dto::Key& key, dto::Key& trh, DataRec& rec) {
                // case"wrong partition"  --> OP:WRITE
                return doWrite(key, rec, mtr, trh, collname, false, true, ErrorCaseOpt::PartMismatchKey)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                })
                // case"wrong partition"  --> OP:READ
                .then([this, &key, &mtr] {
                    return doRead(key, mtr, collname, ErrorCaseOpt::PartMismatchKey)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                })
                // case"wrong partition"  --> OP:PUSH
                .then([this, &key, &mtr] {
                    return doPush(key, collname, mtr, mtr, ErrorCaseOpt::PartMismatchKey)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                })
                // case"wrong partition"  --> OP:END
                .then([this, &trh, &key, &mtr] {
                    return doEnd(trh, mtr, collname, false, {key}, Duration(0s), ErrorCaseOpt::PartMismatchKey)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                })
                // case"wrong partition"  --> OP:FINALIZE
                .then([this, &key, &mtr] {
                    return doFinalize(key, mtr, collname, false, ErrorCaseOpt::PartMismatchKey)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                })
                // case"wrong partition"  --> OP:HEARTBEAT
                .then([this, &key, &mtr] {
                    return doHeartbeat(key, mtr, collname, ErrorCaseOpt::PartMismatchKey)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                });

        }); // end do_with
    }) // end sc-01 case-04
    .then([] {
    // SC01 case5: OP out-of-date partition version
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2LOG_I(log::k23si, "------- SC01.case 05 (out-of-date partition version) -------");
        return seastar::do_with(
            dto::K23SI_MTR {
                .timestamp = std::move(ts),
                .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            DataRec {.f1="SC01_field1", .f2="SC01_field2"},
            [this] (dto::K23SI_MTR& mtr, dto::Key& key, dto::Key& trh, DataRec& rec) {
                // case"wrong partition"  --> OP:WRITE
                return doWrite(key, rec, mtr, trh, collname, false, true, ErrorCaseOpt::ObsoletePart)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                })
                // case"wrong partition"  --> OP:READ
                .then([this, &key, &mtr] {
                    return doRead(key, mtr, collname, ErrorCaseOpt::ObsoletePart)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                })
                // case"wrong partition"  --> OP:PUSH
                .then([this, &key, &mtr] {
                    return doPush(key, collname, mtr, mtr, ErrorCaseOpt::ObsoletePart)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                })
                // case"wrong partition"  --> OP:END
                .then([this, &trh, &key, &mtr] {
                    return doEnd(trh, mtr, collname, false, {key}, Duration(0s), ErrorCaseOpt::ObsoletePart)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                })
                // case"wrong partition"  --> OP:FINALIZE
                .then([this, &key, &mtr] {
                    return doFinalize(key, mtr, collname, false, ErrorCaseOpt::ObsoletePart)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                })
                // case"wrong partition"  --> OP:HEARTBEAT
                .then([this, &key, &mtr] {
                    return doHeartbeat(key, mtr, collname, ErrorCaseOpt::ObsoletePart)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                });

        }); // end do_with
    }) // end sc-01 case-05
    .then([] {
    // SC01 case06: READ/WRITE/FINALIZE empty partition key, empty range key
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2LOG_I(log::k23si, "------- SC01.case 06 (empty partition key, empty range key) -------");
        return seastar::do_with(
            dto::K23SI_MTR {.timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {},
            dto::Key {},
            DataRec {.f1="SC01_field1", .f2="SC01_field2"},
            [this](dto::K23SI_MTR& mtr, dto::Key& key, dto::Key& trh, DataRec& rec) {
                return doWrite(key, rec, mtr, trh, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::BadParameter);
                })
                .then([this, &key, &mtr] {
                    return doRead(key, mtr, collname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::BadParameter);
                    });
                })
                .then([this, &key, &mtr] {
                    return doFinalize(key, mtr, collname, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                });
        }); // end do-with
    }) // end sc-01 case-06
    .then([] {
    // SC01 case07: READ/WRITE/FINALIZE empty partition key, non-empty range key
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2LOG_I(log::k23si, "------- SC01.case 07 (empty partition key, non-empty range key) -------");
        return seastar::do_with(
            dto::K23SI_MTR {.timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            DataRec {.f1="SC01_field1", .f2="SC01_field2"},
            [this](dto::K23SI_MTR& mtr, DataRec& rec) {
                dto::Key missPartKey;
                missPartKey.rangeKey = "SC01_rKey1";
                return doWrite(missPartKey, rec, mtr, missPartKey, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::BadParameter);
                })
                .then([this, &mtr] {
                    dto::Key missPartKey;
                    missPartKey.rangeKey = "SC01_rKey1";
                    return doRead(missPartKey, mtr, collname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::BadParameter);
                    });
                })
                .then([this, &mtr] {
                    dto::Key missPartKey;
                    missPartKey.rangeKey = "SC01_rKey1";
                    return doFinalize(missPartKey, mtr, collname, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                });
        }); // end do-with
    }) // end sc-01 case-07
    .then([] {
    // SC01 case08: READ/WRITE/FINALIZE with only partitionKey
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2LOG_I(log::k23si, "------- SC01.case 08 (READ/WRITE/FINALIZE with only partitionKey) -------");
        return seastar::do_with(
            dto::K23SI_MTR {.timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            DataRec {.f1="SC01_field1", .f2="SC01_field2"},
            [this](dto::K23SI_MTR& mtr, DataRec& rec) {
                dto::Key onlyPartKey;
                onlyPartKey.schemaName = "schema";
                onlyPartKey.partitionKey = "SC01_pKey1";
                return doWrite(onlyPartKey, rec, mtr, onlyPartKey, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                })
                .then([this, &mtr] {
                    dto::Key onlyPartKey;
                    onlyPartKey.schemaName = "schema";
                    onlyPartKey.partitionKey = "SC01_pKey1";
                    return doRead(onlyPartKey, mtr, collname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                })
                .then([this, &mtr] {
                    dto::Key onlyPartKey;
                    onlyPartKey.schemaName = "schema";
                    onlyPartKey.partitionKey = "SC01_pKey1";
                    return doFinalize(onlyPartKey, mtr, collname, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                });
        }); // end do-with
    }) // end sc-01 case-08
    .then([] {
    // SC01 case09: READ/WRITE/FINALIZE with partition and range key
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2LOG_I(log::k23si, "------- SC01.case 09 (READ/WRITE/FINALIZE with partition and range key) -------");
        return seastar::do_with(
            dto::K23SI_MTR {.timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            DataRec {.f1="SC01_field1", .f2="SC01_field2"},
            [this](dto::K23SI_MTR& mtr, dto::Key& key, dto::Key& trh, DataRec& rec) {
                return doWrite(key, rec, mtr, trh, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                })
                .then([this, &key, &mtr] {
                    return doRead(key, mtr, collname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                })
                .then([this, &key, &mtr] {
                    return doFinalize(key, mtr, collname, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                });
        }); // end do-with
    }) // end sc-01 case-09
    .then([] {
    // SC01 case10: cascading error: READ/WRITE/FINALIZE with bad collection name AND missing partition key
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2LOG_I(log::k23si, "------- SC01.case 10 (bad coll name & missing partition key) -------");
        return seastar::do_with(
            dto::K23SI_MTR {
                .timestamp = std::move(ts),
                .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            DataRec {.f1="SC01_field1", .f2="SC01_field2"},
            [this] (dto::K23SI_MTR& mtr, dto::Key& trh, DataRec& rec) {
                // case"wrong partition"  --> OP:WRITE
                dto::Key missPartKey;
                missPartKey.rangeKey = "SC01_rKey1";
                return doWrite(missPartKey, rec, mtr, trh, badCname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                })
                // case"wrong partition"  --> OP:READ
                .then([this, &mtr] {
                    dto::Key missPartKey;
                    missPartKey.rangeKey = "SC01_rKey1";
                    return doRead(missPartKey, mtr, badCname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                })
                // case"wrong partition"  --> OP:FINALIZE
                .then([this, &mtr] {
                    dto::Key missPartKey;
                    missPartKey.rangeKey = "SC01_rKey1";
                    return doFinalize(missPartKey, mtr, badCname, false, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                });
        }); // end do_with
    }) // end sc-01 case-10
    .then([] {
    // SC01 case11: TXN with 2 writes for 2 different partitions ends with Commit. Validate with a read txn afterward
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2LOG_I(log::k23si, "------- SC01.case 11 (TXN with 2 writes for 2 different partitions ends with Commit) -------");
        return seastar::do_with(
        // #1 write Txn in two partitions
            dto::K23SI_MTR {.timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_diff_pKey2", .rangeKey = "SC01_diff_rKey2" },
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            DataRec {.f1="SC01_field1", .f2="SC01_field2"},
            DataRec {.f1="SC01_field3", .f2="SC01_field4"},
            [this](dto::K23SI_MTR& mtr, dto::Key& key1, dto::Key& key2, dto::Key& trh, DataRec& rec1, DataRec& rec2) {
                return doWrite(key1, rec1, mtr, trh, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                })
                .then([this, &key2, &rec2, &mtr, &trh] {
                    return doWrite(key2, rec2, mtr, trh, collname, false, false, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                    });
                })
                .then([this, &trh, &mtr, &key1, &key2] {
                    return doEnd(trh, mtr, collname, true, {key1, key2}, Duration(0s), ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                })
                .then([] {
                    return seastar::sleep(200ms);
                });
        }) // end do-with
        // #2 read Txn to validate
        .then([this] {
            return getTimeNow();
        })
        .then([this](dto::Timestamp&& ts) {
            return seastar::do_with(
                dto::K23SI_MTR {.timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
                dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
                dto::Key {.schemaName = "schema", .partitionKey = "SC01_diff_pKey2", .rangeKey = "SC01_diff_rKey2" },
                DataRec {.f1="SC01_field1", .f2="SC01_field2"},
                DataRec {.f1="SC01_field3", .f2="SC01_field4"},
                [this](auto& mtr, auto& key1, auto& key2, auto& cmpRec1, auto& cmpRec2) {
                    return seastar::when_all(doRead(key1, mtr, collname, ErrorCaseOpt::NoInjection), doRead(key2, mtr, collname, ErrorCaseOpt::NoInjection))
                    .then([&](auto&& response) mutable {
                        auto& [resp1, resp2] = response;
                        // move resp out of the incoming futures sice get0() returns an rvalue
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        K2EXPECT(log::k23si, status1, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, val1, cmpRec1);
                        K2EXPECT(log::k23si, val2, cmpRec2);
                    });
            });
        });
    }) // end sc-01 case-11
    .then([] {
    // SC01 case12: TXN with 2 writes for 2 different partitions ends with Abort. Validate with a read txn afterward
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2LOG_I(log::k23si, "------- SC01.case 12 (TXN with 2 writes for 2 different partitions ends with Abort) -------");
        return seastar::do_with(
        // #1 write Txn in two partitions and the abort
            dto::K23SI_MTR {.timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_diff_pKey2", .rangeKey = "SC01_diff_rKey2" },
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            // write same Keys with different Values
            DataRec {.f1="SC01_field_abort1", .f2="SC01_field_abort2"},
            DataRec {.f1="SC01_field_abort3", .f2="SC01_field_abort4"},
            [this](dto::K23SI_MTR& mtr, dto::Key& key1, dto::Key& key2, dto::Key& trh, DataRec& rec1, DataRec& rec2) {
                return doWrite(key1, rec1, mtr, trh, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                })
                .then([this, &key2, &rec2, &mtr, &trh] {
                    return doWrite(key2, rec2, mtr, trh, collname, false, false, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                    });
                })
                .then([this, &trh, &mtr, &key1, &key2] {
                    return doEnd(trh, mtr, collname, false, {key1, key2}, Duration(0s), ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                })
                .then([] {
                    return seastar::sleep(200ms);
                });
        }) // end do-with
        // #2 read Txn to validate
        .then([this] {
            return getTimeNow();
        })
        .then([this](dto::Timestamp&& ts) {
            return seastar::do_with(
                dto::K23SI_MTR {.timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
                dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
                dto::Key {.schemaName = "schema", .partitionKey = "SC01_diff_pKey2", .rangeKey = "SC01_diff_rKey2" },
                DataRec {.f1="SC01_field1", .f2="SC01_field2"},
                DataRec {.f1="SC01_field3", .f2="SC01_field4"},
                [this](auto& mtr, auto& key1, auto& key2, auto& cmpRec1, auto& cmpRec2) {
                    return seastar::when_all(doRead(key1, mtr, collname, ErrorCaseOpt::NoInjection), doRead(key2, mtr, collname, ErrorCaseOpt::NoInjection))
                    .then([&](auto&& response) mutable {
                        auto& [resp1, resp2] = response;
                        // move resp out of the incoming futures sice get0() returns an rvalue
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        K2EXPECT(log::k23si, status1, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, val1, cmpRec1);
                        K2EXPECT(log::k23si, val2, cmpRec2);
                    });
            });
        });
    }); // end sc-01 case-12
}

seastar::future<> testScenario02() {
    K2LOG_I(log::k23si, "+++++++ TestScenario 02: assigned node with single version data +++++++");
    K2LOG_I(log::k23si, "--->Test SETUP: The following data have been written in the given state.");
    K2LOG_I(log::k23si, "('T1, SC02_pkey1','', v1) -> commited");
    K2LOG_I(log::k23si, "('T1, SC02_pkey2','range1', v1) -> commited");
    K2LOG_I(log::k23si, "('T2, SC02_pkey3','', v1) -> WI");
    K2LOG_I(log::k23si, "('T3, SC02_pkey4','', v1) -> aborted but not cleaned");

    return seastar::make_ready_future()
    // test setup
    .then([this] {
        auto request = dto::CollectionCreateRequest{
            .metadata{
                .name = s02nd_cname,
                .hashScheme = dto::HashScheme::HashCRC32C,
                .storageDriver = dto::StorageDriver::K23SI,
                .capacity{
                    .dataCapacityMegaBytes = 10,
                    .readIOPs = 1000,
                    .writeIOPs = 1000
                },
                .retentionPeriod = 5h,
            },
            .clusterEndpoints = _k2ConfigEps(),
            .rangeEnds{}
        };
        return RPC().callRPC<dto::CollectionCreateRequest, dto::CollectionCreateResponse>
                (dto::Verbs::CPO_COLLECTION_CREATE, request, *_cpoEndpoint, 1s)
        .then([](auto&& response) {
            // response for collection create
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
            // wait for collection to get assigned
            return seastar::sleep(100ms);
        })
        .then([this] {
            // check to make sure the collection is assigned
            auto request = dto::CollectionGetRequest{.name = s02nd_cname};
            return RPC().callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>
                (dto::Verbs::CPO_COLLECTION_GET, request, *_cpoEndpoint, 100ms);
        })
        .then([this](auto&& response) {
            // check collection was assigned
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
            K2EXPECT(log::k23si, resp.collection.partitionMap.partitions[0].astate, dto::AssignmentState::FailedAssignment);
            K2EXPECT(log::k23si, resp.collection.partitionMap.partitions[1].astate, dto::AssignmentState::FailedAssignment);
            K2EXPECT(log::k23si, resp.collection.partitionMap.partitions[2].astate, dto::AssignmentState::FailedAssignment);
        });
    })
    .then([] {
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey1", .rangeKey = ""},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey2", .rangeKey = "range1"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey3", .rangeKey = ""},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey4", .rangeKey = ""},
            DataRec {.f1="SC02_f1", .f2="SC02_f2"},
            [this, ts=std::move(ts)](auto& k1, auto& k2, auto& k3, auto& k4, auto& v1) mutable {
                auto mtr = dto::K23SI_MTR{.timestamp = std::move(ts), .priority = dto::TxnPriority::Medium};

                return seastar::when_all(
                    doWrite(k1, v1, mtr, k1, collname, false, true, ErrorCaseOpt::NoInjection),
                    doWrite(k2, v1, mtr, k1, collname, false, false, ErrorCaseOpt::NoInjection)
                )
                .then([&, mtr] (auto&& response) mutable {
                    auto& [resp1, resp2] = response;
                    // move resp out of the incoming futures sice get0() returns an rvalue
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::Created);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::Created);

                    // commit k1 and k2
                    return doEnd(k1, mtr, collname, true, {k1,k2}, Duration(0s), ErrorCaseOpt::NoInjection);
                })
                .then([&] (auto&& response) mutable {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);

                    // start a new txn for creating a WI for k3
                    return getTimeNow();
                })
                .then([&] (auto&& ts) mutable {
                    // create WI for k3
                    auto mtr = dto::K23SI_MTR {
                            .timestamp = std::move(ts),
                            .priority = dto::TxnPriority::Medium
                        };
                    return doWrite(k3, v1, mtr, k3, collname, false, true, ErrorCaseOpt::NoInjection);
                })
                .then([&] (auto&& response) {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);

                    return getTimeNow(); // start a new txn for k4
                })
                .then([&] (auto&& ts) {
                    // write k4 in a new txn
                    auto mtr = dto::K23SI_MTR {
                            .timestamp = std::move(ts),
                            .priority = dto::TxnPriority::Medium};

                    return doWrite(k4, v1, mtr, k4, collname, false, true, ErrorCaseOpt::NoInjection);
                })
                .then([&, mtr] (auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);

                    // abort the k4 write
                    return doEnd(k4, mtr, collname, false, {k4}, Duration(500ms), ErrorCaseOpt::NoInjection);
                })
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                });
            });
    }) // end test setup
    .then([] {
        K2LOG_I(log::k23si, "Scenario-02 test setup done.");
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
    // SC02 case1: WRITE/READ with bad collection name
        K2LOG_I(log::k23si, "------- SC02.case 01 (WRITE/READ with bad collection name) -------");
        return seastar::do_with(
            dto::K23SI_MTR {
                .timestamp = std::move(ts),
                .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey1", .rangeKey = ""},
            DataRec {.f1="SC02_f3", .f2="SC02_f4"},
            [this] (dto::K23SI_MTR& mtr, dto::Key& k1, DataRec& v2) {
                // case"bad collection name"  --> OP:WRITE
                return doWrite(k1, v2, mtr, k1, badCname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                })
                // case"bad collection name"  --> OP:READ
                .then([this, &k1, &mtr] {
                    return doRead(k1, mtr, badCname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                });
            }
        );
    }) // end sc-02 case-01
    .then([] {
    // SC02 case02: READ/WRITE with wrong partition
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2LOG_I(log::k23si, "------- SC02.case 02 (READ/WRITE with wrong partition index) -------");
        return seastar::do_with(
            dto::K23SI_MTR {
                .timestamp = std::move(ts),
                .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey1", .rangeKey = ""},
            DataRec {.f1="SC02_f3", .f2="SC02_f4"},
            [this] (dto::K23SI_MTR& mtr, dto::Key& k1, DataRec& v2) {
                // case"wrong partition"  --> OP:WRITE
                return doWrite(k1, v2, mtr, k1, collname, false, true, ErrorCaseOpt::WrongPartId)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                })
                // case"wrong partition"  --> OP:READ
                .then([this, &k1, &mtr] {
                    return doRead(k1, mtr, collname, ErrorCaseOpt::WrongPartId)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                });
            }
        );
    }) // end sc-02 case-02
    .then([] {
    // SC02 case03: READ/WRITE with wrong partition version
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2LOG_I(log::k23si, "------- SC02.case 03 (READ/WRITE with wrong partition version) -------");
        return seastar::do_with(
            dto::K23SI_MTR {.timestamp=std::move(ts), .priority=dto::TxnPriority::Medium},
            dto::Key {.schemaName="schema", .partitionKey="SC02_pkey1", .rangeKey=""},
            DataRec {.f1="SC02_f3", .f2="SC02_f4"},
            [this] (dto::K23SI_MTR& mtr, dto::Key& k1, DataRec& v2) {
                // case"wrong partition"  --> OP:WRITE
                return doWrite(k1, v2, mtr, k1, collname, false, true, ErrorCaseOpt::ObsoletePart)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                })
                // case"wrong partition"  --> OP:READ
                .then([this, &k1, &mtr] {
                    return doRead(k1, mtr, collname, ErrorCaseOpt::ObsoletePart)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::RefreshCollection);
                    });
                });
            }
        );
    }) // end sc-02 case-03
    .then([] {
    // SC02 case04: READ of all data records
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2LOG_I(log::k23si, "------- SC02.case 04 (READ of all data records) -------");
        return seastar::do_with(
            dto::K23SI_MTR {.timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey1", .rangeKey = ""},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey2", .rangeKey = "range1"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey3", .rangeKey = ""},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey4", .rangeKey = ""},
            DataRec {.f1="SC02_f1", .f2="SC02_f2"},
            [this](auto& mtr, auto& k1, auto& k2, auto& k3, auto& k4, auto& v1) {
                return seastar::when_all(
                    doRead(k1, mtr, collname, ErrorCaseOpt::NoInjection),
                    doRead(k2, mtr, collname, ErrorCaseOpt::NoInjection),
                    doRead(k3, mtr, collname, ErrorCaseOpt::NoInjection),
                    doRead(k4, mtr, collname, ErrorCaseOpt::NoInjection))
                .then([&](auto&& response) mutable {
                    auto& [resp1, resp2, resp3, resp4] = response;
                    // move resp out of the incoming futures since get0() returns an rvalue
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    auto [status3, val3] = resp3.get0();
                    auto [status4, val4] = resp4.get0();

                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::OK);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                    K2EXPECT(log::k23si, status3, dto::K23SIStatus::KeyNotFound);
                    K2EXPECT(log::k23si, status4, dto::K23SIStatus::KeyNotFound);
                    K2EXPECT(log::k23si, val1, v1);
                    K2EXPECT(log::k23si, val2, v1);
                    K2EXPECT(log::k23si, val3.f1, "");
                    K2EXPECT(log::k23si, val3.f2, "");
                    K2EXPECT(log::k23si, val4.f1, "");
                    K2EXPECT(log::k23si, val4.f2, "");
                });
            }
        );
    }) // end sc-02 case-04
    .then([] {
        // SC02 case05&06: attempt to write in the past and write at same time
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2LOG_I(log::k23si, "------- SC02.case 05&06 (for an existing key that has never been read, attempt to write in the past and write at same time) -------");

        return seastar::do_with(
            dto::K23SI_MTR {.timestamp = {1000000, 123, 1000}, .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR {.timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey5", .rangeKey = "range6"},
            DataRec {.f1="SC02_f05", .f2="SC02_f06"},
            DataRec {.f1="SC02_f07", .f2="SC02_f08"},
            [this](auto& staleMTR, auto& incumbentMTR, auto& k5, auto& v2, auto& v3) {
                 return doWrite(k5, v2, incumbentMTR, k5, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                })
               .then([this, &k5, &incumbentMTR] {
                    return doEnd(k5, incumbentMTR, collname, true, {k5}, Duration(0us), ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                })
                .then([this, &k5, &v3, &staleMTR] {
                    return doWrite(k5, v3, staleMTR, k5, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::AbortRequestTooOld);
                    });
                })
                .then([this, &k5, &v3, &incumbentMTR] {
                    dto::K23SI_MTR challengerMTR{
                        .timestamp = incumbentMTR.timestamp,
                        .priority = dto::TxnPriority::Medium
                    };
                    return doWrite(k5, v3, challengerMTR, k5, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::AbortRequestTooOld);
                    });
                })
                ;
        });
    }) // end sc-02 case-05 & 06
    .then([] {
        // SC02 case07: attempt to write in the future
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2LOG_I(log::k23si, "------- SC02.case 07 (for an existing key that has never been read, attempt to write in the future) -------");

        return seastar::do_with(
            dto::K23SI_MTR {.timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey5", .rangeKey = "range6"},
            DataRec {.f1="SC02_f07", .f2="SC02_f08"},
            [this](auto& futureMTR, auto& k5, auto& v3) {
                 return doWrite(k5, v3, futureMTR, k5, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                })
                .then([this, &k5, &futureMTR, &v3] {
                    return doRead(k5, futureMTR, collname, ErrorCaseOpt::NoInjection)
                    .then([&](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, resp, v3);
                    });
                });
            }
        );
    }) // end sc-02 case-07
    .then([] {
        // SC02 case08 & 09 & 10: READ existing key at time before/equal/after the key
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2LOG_I(log::k23si, "------- SC02.case08 & 09 & 10 (READ existing key at time before/equal/after the key) -------");

        return seastar::do_with(
            dto::K23SI_MTR {.timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey8", .rangeKey = "range8"},
            DataRec {.f1="SC02_f08", .f2="SC02_f09"},
            [this](auto& eqMTR, auto& k6, auto& v1) {
                return doWrite(k6, v1, eqMTR, k6, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                     auto& [status, resp] = response;
                     K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                })
                .then([this, &k6, &eqMTR] {
                    return doEnd(k6, eqMTR, collname, true, {k6}, Duration(0us), ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                })
                .then([this, &k6, &eqMTR] {
                    dto::K23SI_MTR bfMTR{
                        .timestamp = {(eqMTR.timestamp.tEndTSECount() - 500000000), 123, 1000}, // 500ms earlier
                        .priority = dto::TxnPriority::Medium};
                    return doRead(k6, bfMTR, collname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::KeyNotFound);
                    });
                }) // end sc-02 case-08
                .then([this, &k6, &eqMTR, &v1] {
                    return doRead(k6, eqMTR, collname, ErrorCaseOpt::NoInjection)
                    .then([&v1](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, resp, v1);
                    });
                }) // end sc-02 case-09
                .then([this, &k6, &eqMTR, &v1] {
                    dto::K23SI_MTR afMTR{
                        .timestamp = {(eqMTR.timestamp.tEndTSECount() + 500000000), 123, 1000}, // 500ms later
                        .priority = dto::TxnPriority::Medium};
                    return doRead(k6, afMTR, collname, ErrorCaseOpt::NoInjection)
                    .then([&v1](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, resp, v1);
                    });
                }); // end sc-02 case-10
            }
        );
    }) // end sc-02 case-08-09-10
    .then([] {
    // SC02 case11 & 12 & 13 & 14: Async END test (end-but-not-finalized key )
    // case11: TXN with a WRITE and then END asynchronously with Commit. Finalize with abort for the same key at time interval.
    // case12: TXN with a WRITE and then END asynchronously with Abort. Finalize with commit for the same key at time interval.
    // case13: TXN with a WRITE and then END asynchronously with Abort. Validate with a read for the async_end_key.
        return seastar::when_all_succeed(getTimeNow(), getTimeNow(), getTimeNow());
    })
    .then([this](auto&& timestamps) {
        K2LOG_I(log::k23si, "------- SC02.case11 & 12 & 13 (Async END test (end-but-not-finalized)) -------");
        auto& [ts1, ts2, ts3] = timestamps;

        return seastar::do_with(
            dto::K23SI_MTR {.timestamp = ts1, .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR {.timestamp = ts2, .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR {.timestamp = ts3, .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey11", .rangeKey = "range11"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey12", .rangeKey = "range12"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey13", .rangeKey = "range13"},
            DataRec {.f1="SC02_f_end", .f2="SC02_f_end"}, // new value
            [this](auto& mtr11, auto& mtr12, auto& mtr13, auto& k11, auto& k12, auto& k13, auto& v1) {
                return seastar::when_all(
                    doWrite(k11, v1, mtr11, k11, collname, false, true, ErrorCaseOpt::NoInjection),
                    doWrite(k12, v1, mtr12, k12, collname, false, true, ErrorCaseOpt::NoInjection),
                    doWrite(k13, v1, mtr13, k13, collname, false, true, ErrorCaseOpt::NoInjection))
                .then([&](auto&& response) mutable {
                    auto& [resp1, resp2, resp3] = response;
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    auto [status3, val3] = resp3.get0();
                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::Created);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::Created);
                    K2EXPECT(log::k23si, status3, dto::K23SIStatus::Created);
                })
                .then([this, &mtr11, &mtr12,&mtr13, &k11, &k12, &k13] {
                    return seastar::when_all(
                        doEnd(k11, mtr11, collname, true, {k11}, Duration(111ms),ErrorCaseOpt::NoInjection),
                        doEnd(k12, mtr12, collname, false, {k12}, Duration(112ms),ErrorCaseOpt::NoInjection),
                        doEnd(k13, mtr13, collname, false, {k13}, Duration(113ms),ErrorCaseOpt::NoInjection))
                    .then([&](auto&& response) mutable {
                        auto& [resp1, resp2, resp3] = response;
                        // move resp out of the incoming futures sice get0() returns an rvalue
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        auto [status3, val3] = resp3.get0();
                        K2EXPECT(log::k23si, status1, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, status3, dto::K23SIStatus::OK);
                    });
                })
                .then([this, &k11, &k12, &k13, &mtr11, &mtr12, &mtr13, &v1] {
                    return seastar::when_all(
                        doFinalize(k11, mtr11, collname, false, ErrorCaseOpt::NoInjection),
                        doFinalize(k12, mtr12, collname, true, ErrorCaseOpt::NoInjection),
                        doRead(k13, mtr13, collname, ErrorCaseOpt::NoInjection))
                    .then([&](auto&& response) mutable {
                        auto& [resp1, resp2, resp3] = response;
                        // move resp out of the incoming futures sice get0() returns an rvalue
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        auto [status3, val3] = resp3.get0();
                        K2EXPECT(log::k23si, status1, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, status3, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, val3, v1);
                    });
                });
            }
        );
    }); // end sc-02 case-11-12-13-14
}

seastar::future<> testScenario03() {
    return seastar::make_ready_future()
    .then([] {
        return seastar::sleep(300ms);
    });
}

seastar::future<> testScenario04() {
    K2LOG_I(log::k23si, "+++++++ TestScenario 04: read your writes and read-committed isolation +++++++");
    K2LOG_I(log::k23si, "--->Test SETUP: initialization record, pKey('SC04_pkey1'), rKey('rKey1'), v0{{f1=SC04_f1_zero, f2=SC04_f2_zero}} -> committed");

    return seastar::make_ready_future()
    .then([] {
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            dto::K23SI_MTR {.timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC04_pkey1", .rangeKey = "rKey1"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC04_pkey1", .rangeKey = "rKey1"},
            DataRec {.f1="SC04_f1_zero", .f2="SC04_f2_zero"},
            [this](auto& mtr, auto& k1, auto& trh, auto& v0) {
                return doWrite(k1, v0, mtr, trh, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                })
                .then([this, &trh, &mtr, &k1] {
                    return doEnd(trh, mtr, collname, true, {k1}, Duration(0s), ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                });
            }
        );
    })
    .then([]{
        K2LOG_I(log::k23si, "Scenario 04 setup done.");
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            // The test logic of this scenario is shown as a diagram in document in K23SI_testing.md.
            // In short, Important events (txn-begin/write/end) are marked on the timeline. We test the
            // read-committed isolation by doing transactions reads on the gaps. READs are not marked on the axes.
            // NOTE: short forms: A/B/C/D-->Txn(A/B/C/D), bg-->Txn-begin, WI-->Txn-write-intent, end-->Txn-end.
            //
            //   init    B-bg   A-bg     A-WI     C-bg     B-WI     A-end    D-bg
            // ----|------|------|--------|--------|--------|--------|--------|--------> timeline

            dto::K23SI_MTR { // txn(B) is 1ms older than txn(A)
                .timestamp = {(ts.tEndTSECount() - 1000000), 123, 1000},
                .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR { // txn(A)
                .timestamp = {(ts.tEndTSECount()), 123, 1000},
                .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR { // txn(C) is newer than txn(A), also newer than txn(B)
                .timestamp = {(ts.tEndTSECount() + 1000000), 123, 1000},
                .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR { // txn(D) is newest of all transactions
                .timestamp = {(ts.tEndTSECount() + 5000000), 123, 1000},
                .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC04_pkey1", .rangeKey = "rKey1"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC04_pkey2", .rangeKey = "rKey2"},
            DataRec {.f1="SC04_f1_zero", .f2="SC04_f2_zero"},
            DataRec {.f1="SC04_f1_one", .f2="SC04_f2_one"},
            [this](auto& mtrB, auto& mtrA, auto& mtrC, auto& mtrD, auto& k1, auto& k2, auto& v0, auto& v1) {
                K2LOG_I(log::k23si, "------- SC04.case1 (txns READ record that has been committed before it starts) -------");
                return seastar::when_all(
                    doRead(k1, mtrA, collname, ErrorCaseOpt::NoInjection),
                    doRead(k1, mtrB, collname, ErrorCaseOpt::NoInjection))
                .then([&v0](auto&& response) mutable {
                    auto& [resp1, resp2] = response;
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::OK);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                    K2EXPECT(log::k23si, val1, v0);
                    K2EXPECT(log::k23si, val2, v0);
                })
                .then([this, &k1, &v1, &mtrA] {
                    return doWrite(k1, v1, mtrA, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                    })
                    .then([&] {
                        return doRead(k1, mtrA, collname, ErrorCaseOpt::NoInjection)
                        .then([&](auto&& response)  {
                            auto& [status, val] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            K2EXPECT(log::k23si, val, v1);
                        });
                    });
                })
                .then([&] {
                    return doRead(k1, mtrB, collname, ErrorCaseOpt::NoInjection)
                    .then([&](auto&& response)  {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, val, v0);
                    });
                })
                .then([&] {
                    return doWrite(k2, v1, mtrB, k2, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([&](auto&& response) {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                    })
                    .then([&] {
                        // this aborts the incumbent (mtrB) and so we should just see a KeyNotFound after the abort is cleaned
                        return doRead(k2, mtrC, collname, ErrorCaseOpt::NoInjection)
                        .then([&](auto&& response)  {
                            auto& [status, val] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::KeyNotFound);
                            K2EXPECT(log::k23si, val.f1, "");
                            K2EXPECT(log::k23si, val.f2, "");
                        });
                    })
                    .then([&] {
                        return doInspectRecords(k2, collname)
                        .then([&](auto&& response)  {
                            auto& [status, val] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::KeyNotFound);
                            K2EXPECT(log::k23si, val.records.empty(), true);
                        });
                    })
                    .then([&] {
                        return doInspectTxn(k2, mtrB, collname)
                        .then([&](auto&& response)  {
                            auto& [status, val] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            K2EXPECT(log::k23si, val.state, dto::TxnRecordState::ForceAborted);
                        });
                    });
                })
                .then([&] {
                    return doEnd(k1, mtrA, collname, true, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    })
                    .then([&] {
                        return doRead(k1, mtrB, collname, ErrorCaseOpt::NoInjection)
                        .then([&](auto&& response)  {
                            auto& [status, val] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            K2EXPECT(log::k23si, val, v0);
                        });
                    });
                })
                .then([&] {
                    return seastar::when_all(doRead(k1, mtrC, collname, ErrorCaseOpt::NoInjection), doRead(k1, mtrD, collname, ErrorCaseOpt::NoInjection))
                    .then([&](auto&& response) {
                        auto& [resp1, resp2] = response;
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        K2EXPECT(log::k23si, status1, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, val1, v1);
                        K2EXPECT(log::k23si, val2, v1);
                    });
                });
            }
        ); // end do-with
    }); // end SC-04
}

seastar::future<> testScenario05() {
    K2LOG_I(log::k23si, "+++++++ TestScenario 05: concurrent transactions +++++++");
    K2LOG_I(log::k23si, "--->Test SETUP: initialization record, pKey('SC05_pkey1'), rKey('rKey1'), v0{{f1=SC05_f1_zero, f2=SC05_f2_zero}} -> committed");

    return seastar::make_ready_future()
    .then([] {
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            dto::K23SI_MTR {.timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC05_pkey1", .rangeKey = "rKey1"},
            DataRec {.f1="SC05_f1_zero", .f2="SC04_f2_zero"},
            [this](auto& mtr, auto& k1, auto& v0) {
                return doWrite(k1, v0, mtr, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                })
                .then([this, &mtr, &k1] {
                    return doEnd(k1, mtr, collname, true, {k1}, Duration(0s), ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                });
            }
        ); // end do-with
    })
    .then([]{
        K2LOG_I(log::k23si, "Scenario 05 setup done.");
        return getTimeNow();
    })
    .then([&](dto::Timestamp&& ts) {
        // ts will be the timestamp of the incumbent txn
        return seastar::do_with(
            // clarify: For the sake of code brevity, priority and Timestamp in the following
            // MTR parameters are directly related to who wins Push() in the test cases. So that the
            // next test case (write-write conflict) can directly compare with the winner mtr.
            // So the following statement may cause some confusion, which will be cleared
            // in combination with the test cases.
            dto::K23SI_MTR{// mtr_ts_Med_ts incumbent mtr
                           .timestamp = ts,
                           .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR{// mtr_m1000_Med_123 - txn which started earlier than incumbent
                           .timestamp = {(ts.tEndTSECount() - 1000), 123, 1000},
                           .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR{// mtr_m900_High_123 - another txn which started earlier than incumbent
                           .timestamp = {(ts.tEndTSECount() - 900), 123, 1000},
                           .priority = dto::TxnPriority::High},
            dto::K23SI_MTR{// mtr_1000_High_123: txn with higher priority
                           .timestamp = {(ts.tEndTSECount() + 1000), 123, 1000},
                           .priority = dto::TxnPriority::High},
            dto::K23SI_MTR{// mtr_1010_High_123: txn with higher priority and newer than before
                           .timestamp = {(ts.tEndTSECount() + 1010), 123, 1000},
                           .priority = dto::TxnPriority::High},
            dto::K23SI_MTR{// mtr_1020_Low_123: txn lower priority
                           .timestamp = {(ts.tEndTSECount() + 1020), 123, 1000},
                           .priority = dto::TxnPriority::Low},
            dto::K23SI_MTR{// mtr_1010_High_100: txn with same ts, same priority, smaller tso id
                           .timestamp = {(ts.tEndTSECount() + 1010), 100, 1000},
                           .priority = dto::TxnPriority::High},
            dto::K23SI_MTR{// mtr_1010_High_200: txn with same ts, same priority, bigger tso id
                           .timestamp = {(ts.tEndTSECount() + 1010), 200, 1000},
                           .priority = dto::TxnPriority::High},
            dto::Key{.schemaName = "schema", .partitionKey = "SC05_pkey1", .rangeKey = "rKey1"},
            DataRec{.f1 = "SC05_f1_one", .f2 = "SC04_f2_one"},
            DataRec{.f1 = "SC05_f1_zero", .f2 = "SC04_f2_zero"},
            [&](
                auto& mtr_ts_Med_ts,
                auto& mtr_m1000_Med_123, auto& mtr_m900_High_123,
                auto& mtr_1000_High_123, auto& mtr_1010_High_123, auto& mtr_1020_Low_123,
                auto& mtr_1010_High_100, auto& mtr_1010_High_200,
                auto& k1, auto& v1, auto& v0) {
                return doWrite(k1, v1, mtr_ts_Med_ts, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                })
                .then([&] {
                    K2LOG_I(log::k23si, "------- SC05.case1 (non-conflict read before WI) -------");
                    return doRead(k1, mtr_m1000_Med_123, collname, ErrorCaseOpt::NoInjection)
                    .then([&](auto&& response) {
                        // mtr_m1000_Med_123 is earlier than incumbent and so should not interfere with it and
                        // should see the data from the test setup(v0)
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, val, v0);
                    })
                    .then([&] {
                        return doInspectTxn(k1, mtr_ts_Med_ts, collname)
                        .then([&](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            K2EXPECT(log::k23si, resp.mtr.timestamp, mtr_ts_Med_ts.timestamp);
                            K2EXPECT(log::k23si, resp.state, dto::TxnRecordState::InProgress);
                        });
                    });
                })
                .then([&] {
                    // rec is now from mtr_ts_Med_ts
                    K2LOG_I(log::k23si, "------- SC05.case2 (Txn with higher priority encounters a WI) -------");
                    return doWrite(k1, v1, mtr_1000_High_123, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        // op_WRITE in txn(mtr_1000_High_123) wins push(), so the old WI is cleared and k1-WI of txn(mtr_1000_High_123) is created
                        // current MTR of k1-WI is mtr_1000_High_123.
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                    });
                })
                .then([&] {
                    return doInspectTxn(k1, mtr_1000_High_123, collname)
                    .then([&](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, resp.mtr.timestamp, mtr_1000_High_123.timestamp);
                        K2EXPECT(log::k23si, resp.state, dto::TxnRecordState::InProgress);
                    });
                })
                .then([&] {
                    // mtr is now mtr_1000_High_123
                    K2LOG_I(log::k23si, "------- SC05.case3 (Txn with lower priority encounters a WI) -------");
                    return doWrite(k1, v1, mtr_1020_Low_123, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        // op_WRITE in txn(mtr_1000_High_123) lose push(), so the k1-WI still belongs to txn(mtr_1000_High_123)
                        // current MTR of k1-WI is mtr_1000_High_123.
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::AbortConflict);
                    })
                    .then([&] {
                        return doInspectTxn(k1, mtr_1020_Low_123, collname)
                        .then([&](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            K2EXPECT(log::k23si, resp.mtr, mtr_1020_Low_123);
                            K2EXPECT(log::k23si, resp.state, dto::TxnRecordState::InProgress);
                        });
                    });
                })
                .then([&] {
                    // mtr is now mtr_1000_High_123
                    K2LOG_I(log::k23si, "------- SC05.case4 (Earlier WRITE Txn encounters a WI with the same priority) -------");
                    return doWrite(k1, v1, mtr_m900_High_123, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        // op_WRITE in txn(mtr_m900_High_123) lose push(), so the k1-WI still belongs to txn(mtr_1000_High_123)
                        // current MTR of k1-WI is mtr_1000_High_123.
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::AbortConflict);
                    })
                    .then([&] {
                        return doInspectTxn(k1, mtr_m900_High_123, collname)
                        .then([&](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            K2EXPECT(log::k23si, resp.mtr, mtr_m900_High_123);
                            K2EXPECT(log::k23si, resp.state, dto::TxnRecordState::InProgress);
                        });
                    });
                })
                .then([&] {
                    K2LOG_I(log::k23si, "------- SC05.case5 (newer WRITE Txn encounters a WI with the same priority) -------");
                    // at this time, mtr_1000_High_123 still has the WI
                    return doWrite(k1, v1, mtr_1010_High_123, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        // op_WRITE in txn(mtr_1010_High_123) wins push(), so the old WI is cleared and
                        // k1-WI of txn(mtr_1010_High_123) is created
                        // current MTR of k1-WI is mtr_1010_High_123.
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                    })
                    .then([&] {
                        return doInspectTxn(k1, mtr_1000_High_123, collname)
                        .then([&](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            K2EXPECT(log::k23si, resp.mtr, mtr_1000_High_123);
                            K2EXPECT(log::k23si, resp.state, dto::TxnRecordState::ForceAborted);
                        });
                    });
                })
                .then([&] {
                    // rec is now from mtr_1010_High_123
                    K2LOG_I(log::k23si, "------- SC05.case6 (WRITE Txn with smaller tso ID encounters a WI) -------");
                    return doWrite(k1, v1, mtr_1010_High_100, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        // op_WRITE in txn(mtr_1010_High_100) lose push(), so the k1-WI still belongs to txn(mtr_1010_High_123)
                        // current MTR of k1-WI is mtr_1010_High_123.
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::AbortConflict);
                    })
                    .then([&] {
                        return doInspectTxn(k1, mtr_1010_High_100, collname)
                        .then([&](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            K2EXPECT(log::k23si, resp.mtr, mtr_1010_High_100);
                            K2EXPECT(log::k23si, resp.state, dto::TxnRecordState::InProgress);
                        });
                    });
                })
                .then([&] {
                    // rec is still from mtr_1010_High_123
                    K2LOG_I(log::k23si, "------- SC05.case7 (WRITE Txn with bigger tso ID encounters a WI) -------");
                    return doWrite(k1, v1, mtr_1010_High_200, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        // op_WRITE in txn(mtr_1010_High_200) wins push(), so the old WI is cleared and k1-WI of txn(mtr_1010_High_200) is created
                        // current MTR of k1-WI is mtr_1010_High_200.
                        auto& [status, resp] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                    })
                    .then([&] {
                        return doInspectTxn(k1, mtr_1010_High_123, collname)
                        .then([&](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            K2EXPECT(log::k23si, resp.mtr, mtr_1010_High_123);
                            K2EXPECT(log::k23si, resp.state, dto::TxnRecordState::ForceAborted);
                        });
                    })
                    .then([&] {
                        return doInspectTxn(k1, mtr_1010_High_200, collname)
                        .then([&](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            K2EXPECT(log::k23si, resp.mtr, mtr_1010_High_200);
                            K2EXPECT(log::k23si, resp.state, dto::TxnRecordState::InProgress);
                        });
                    });
                });
            });
    }); // end sc-05
}

seastar::future<> testScenario06() {
    K2LOG_I(log::k23si, "+++++++ TestScenario 06: finalization +++++++");

    return seastar::make_ready_future()
    .then([] {
        return seastar::when_all_succeed(getTimeNow(), getTimeNow());
    })
    .then([this](auto&& timestamps) {
        auto& [ts1, ts2] = timestamps;
        return seastar::do_with(
            dto::K23SI_MTR {.timestamp = ts2, .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR {.timestamp = ts1, .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkek1", .rangeKey = "rKey1"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkey2", .rangeKey = "rKey2"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkey3", .rangeKey = "rKey3"},
            DataRec {.f1="SC05_f1_zero", .f2="SC04_f2_zero"},
            [this](auto& mtr, auto& otherMtr, auto& k1, auto& k2, auto& k3, auto& v0) {
            return doWrite(k1, v0, mtr, k1, collname, false, true, ErrorCaseOpt::NoInjection)
            .then([](auto&& response) {
                auto& [status, resp] = response;
                K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC06.case1 (Finalize a non-exist record in this transaction) -------");
                return doFinalize(k3, mtr, collname, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::KeyNotFound);
                });
            })
            .then([&] {
                return doWrite(k3, v0, mtr, k1, collname, false, false, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) mutable {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                });
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC06.case2 ( Finalize_Commit partial record within this transaction ) -------");
                return doFinalize(k3, mtr, collname, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                });
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC06.case4 ( Other transactions read finalize_commit record ) -------");
                return doRead(k3, otherMtr, collname, ErrorCaseOpt::NoInjection)
                .then([&](auto&& response) {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, val, v0);
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                });
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC06.case5 ( After partial Finalize, txn continues and then Commit all records ) -------");
                return doWrite(k2, v0, mtr, k1, collname, false, false, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                })
                .then([&] {
                    return doEnd(k1, mtr, collname, true, {k1, k2, k3}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                });
            });
        }); // end do-with
    }) // end case 01-05
    .then([&] {
        K2LOG_I(log::k23si, "------- SC06.case6 ( After partial Finalization_commit, txn continues and then End_Abort all records ) -------");
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            dto::K23SI_MTR {.timestamp = ts, .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR {.timestamp = ts, .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkek1_sec", .rangeKey = "rKey1_sec"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkey2_sec", .rangeKey = "rKey2_sec"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkey3_sec", .rangeKey = "rKey3_sec"},
            DataRec {.f1="SC05_f1_zero", .f2="SC04_f2_zero"},
            [this](auto& mtr, auto& otherMtr, auto& k1, auto& k2, auto& k3, auto& v0) {
            return seastar::when_all(doWrite(k1, v0, mtr, k1, collname, false, true, ErrorCaseOpt::NoInjection), \
                    doWrite(k2, v0, mtr, k1, collname, false, false, ErrorCaseOpt::NoInjection))
            .then([](auto&& response) mutable {
                auto& [resp1, resp2] = response;
                auto [status1, val1] = resp1.get0();
                auto [status2, val2] = resp2.get0();
                K2EXPECT(log::k23si, status1, dto::K23SIStatus::Created);
                K2EXPECT(log::k23si, status2, dto::K23SIStatus::Created);
            })
            .then([&] {
                return doFinalize(k2, mtr, collname, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                });
            })
            .then([&] {
                return doWrite(k3, v0, mtr, k1, collname, false, false, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                });
            })
            .then([&] {
                return doEnd(k1, mtr, collname, false, {k1, k2, k3}, Duration{0s}, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                });
            })
            .then([&] {
                return seastar::when_all(doRead(k1, otherMtr, collname, ErrorCaseOpt::NoInjection), doRead(k2, otherMtr, collname, ErrorCaseOpt::NoInjection), \
                        doRead(k3, otherMtr, collname, ErrorCaseOpt::NoInjection))
                .then([&](auto&& response) mutable {
                    auto& [resp1, resp2, resp3] = response;
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    auto [status3, val3] = resp3.get0();
                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::KeyNotFound);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                    K2EXPECT(log::k23si, status3, dto::K23SIStatus::KeyNotFound);
                    K2EXPECT(log::k23si, val1.f1, "");
                    K2EXPECT(log::k23si, val1.f2, "");
                    K2EXPECT(log::k23si, val2, v0);
                    K2EXPECT(log::k23si, val3.f1, "");
                    K2EXPECT(log::k23si, val3.f2, "");
                });
            });
        }); // end do-with
    }) // end case 06
    .then([&] {
        K2LOG_I(log::k23si, "------- SC06.case7 ( Finalize_Abort partial record within this transaction ) -------");
        return seastar::when_all_succeed(getTimeNow(), getTimeNow());
    })
    .then([this](auto&& timestamps) {
        auto& [ts1, ts2] = timestamps;
        return seastar::do_with(
            dto::K23SI_MTR {.timestamp = std::move(ts1), .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR {.timestamp = std::move(ts2), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkek4", .rangeKey = "rKey4"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkey5", .rangeKey = "rKey5"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkey6", .rangeKey = "rKey6"},
            DataRec {.f1="SC05_f1_zero", .f2="SC04_f2_zero"},
            [this](auto& mtr, auto& otherMtr, auto& k4, auto& k5, auto& k6, auto& v0) {
            return seastar::when_all(
                doWrite(k4, v0, mtr, k4, collname, false, true, ErrorCaseOpt::NoInjection),
                doWrite(k5, v0, mtr, k4, collname, false, false, ErrorCaseOpt::NoInjection))
            .then([](auto&& response) {
                auto& [resp1, resp2] = response;
                auto [status1, val1] = resp1.get0();
                auto [status2, val2] = resp2.get0();
                K2EXPECT(log::k23si, status1, dto::K23SIStatus::Created);
                K2EXPECT(log::k23si, status2, dto::K23SIStatus::Created);
            })
            .then([&] {
                return doFinalize(k4, mtr, collname, false, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                });
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC06.case8 ( Record is read after it is Finalize_Abort  within the txn ) -------");
                return doRead(k4, mtr, collname, ErrorCaseOpt::NoInjection)
                .then([&](auto&& response) {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, val.f1, "");
                    K2EXPECT(log::k23si, val.f2, "");
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::KeyNotFound);
                });
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC06.case9 ( Finalize a record who has already been finalized ) -------");
                return doFinalize(k4, mtr, collname, false, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::KeyNotFound);
                });
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC06.case10 ( After Finalize_abort, txn continues and then Commit all records ) -------");
                return doWrite(k6, v0, mtr, k4, collname, false, false, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                })
                .then([&] {
                    return doEnd(k4, mtr, collname, true, {k4, k5, k6}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                })
                .then([&] {
                    return seastar::when_all(
                        doRead(k4, otherMtr, collname, ErrorCaseOpt::NoInjection),
                        doRead(k5, otherMtr, collname, ErrorCaseOpt::NoInjection),
                        doRead(k6, otherMtr, collname, ErrorCaseOpt::NoInjection))
                    .then([&](auto&& response) mutable {
                        auto& [resp1, resp2, resp3] = response;
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        auto [status3, val3] = resp3.get0();
                        K2EXPECT(log::k23si, status1, dto::K23SIStatus::KeyNotFound);
                        K2EXPECT(log::k23si, status2, dto::K23SIStatus::KeyNotFound);
                        K2EXPECT(log::k23si, status2, dto::K23SIStatus::KeyNotFound);
                    });
                });
            });
        }); // end do-with
    }) // end case 07-10
    .then([&] {
        K2LOG_I(log::k23si, "------- SC06.case11 ( After partial Finalization_abort, txn continues and then End_Abort all records ) -------");
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            dto::K23SI_MTR {.timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkek4", .rangeKey = "rKey4"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkey5", .rangeKey = "rKey5"},
            DataRec {.f1="SC05_f1_zero", .f2="SC04_f2_zero"},
            [this](auto& mtr, auto& k4, auto& k5, auto& v0) {
            return seastar::when_all(doWrite(k4, v0, mtr, k4, collname, false, true, ErrorCaseOpt::NoInjection), \
                    doWrite(k5, v0, mtr, k5, collname, false, false, ErrorCaseOpt::NoInjection))
            .then([](auto&& response) mutable {
                auto& [resp1, resp2] = response;
                auto [status1, val1] = resp1.get0();
                auto [status2, val2] = resp2.get0();
                K2EXPECT(log::k23si, status1, dto::K23SIStatus::Created);
                K2EXPECT(log::k23si, status2, dto::K23SIStatus::Created);
            })
            .then([&] {
                return doFinalize(k4, mtr, collname, false, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                });
            })
            .then([&] {
                return doEnd(k4, mtr, collname, false, {k4, k5}, Duration{0s}, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                });
            });
        }); // end do-with
    }) // end case 11
    .then([&] {
        K2LOG_I(log::k23si, "------- SC06.case12 ( The MTR parameters of Finalize do not match ) -------");
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            dto::K23SI_MTR {.timestamp = ts, .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR {.timestamp = ts, .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkek7", .rangeKey = "rKey7"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkey8", .rangeKey = "rKey8"},
            DataRec {.f1="SC05_f1_zero", .f2="SC04_f2_zero"},
            [this](auto& mtr, auto& otherMtr, auto& k7, auto& k8, auto& v0) {
            return seastar::when_all(doWrite(k7, v0, mtr, k7, collname, false, true, ErrorCaseOpt::NoInjection), \
                    doWrite(k8, v0, mtr, k7, collname, false, false, ErrorCaseOpt::NoInjection))
            .then([](auto&& response) mutable {
                auto& [resp1, resp2] = response;
                auto [status1, val1] = resp1.get0();
                auto [status2, val2] = resp2.get0();
                K2EXPECT(log::k23si, status1, dto::K23SIStatus::Created);
                K2EXPECT(log::k23si, status2, dto::K23SIStatus::Created);
            })
            .then([&] {
                return doFinalize(k8, K23SI_MTR_ZERO, collname, true, ErrorCaseOpt::NoInjection);
            })
            .then([](auto&& response)  {
                auto& [status, val] = response;
                K2EXPECT(log::k23si, status, dto::K23SIStatus::KeyNotFound);
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC06.case13 ( During async end_abort interval, finalize_commit those keys ) -------");
                return doEnd(k7, mtr, collname, false, {k7, k8}, Duration{200ms}, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                });
            })
            .then([&] {
                return doFinalize(k7, mtr, collname, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                });
            })
            .then([] {
                return seastar::sleep(200ms);
            })
            .then([&] {
                return seastar::when_all(doRead(k7, otherMtr, collname, ErrorCaseOpt::NoInjection), doRead(k8, otherMtr, collname, ErrorCaseOpt::NoInjection))
                .then([&](auto&& response) mutable {
                    auto& [resp1, resp2] = response;
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::OK);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::KeyNotFound);
                    K2EXPECT(log::k23si, val1, v0);
                    K2EXPECT(log::k23si, val2.f1, "");
                    K2EXPECT(log::k23si, val2.f2, "");
                });
            });
        }); // end do-with
    }); // end case 12-13 end sc-06
}

seastar::future<> testScenario07() {
    K2LOG_I(log::k23si, "+++++++ TestScenario 07: client-initiated txn abort +++++++");

    return seastar::make_ready_future()
    .then([] {
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            // multi MTRs are used for multiple transactions to execute. We use these MTRs in sequence
            dto::K23SI_MTR { .timestamp = ts, .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { .timestamp = {(ts.tEndTSECount() + 20000), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { .timestamp = {(ts.tEndTSECount() + 30000), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { .timestamp = {(ts.tEndTSECount() + 40000), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { .timestamp = {(ts.tEndTSECount() + 50000), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { .timestamp = {(ts.tEndTSECount() + 60000), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::Key {.schemaName = "schema", .partitionKey = "SC07_pkey1", .rangeKey = "rKey1"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC07_pkey4", .rangeKey = "rKey2"},
            DataRec {.f1="SC05_f1_zero", .f2="SC04_f2_zero"},
            [this](auto& mtr, auto& mtr2, auto& mtr3, auto& mtr4, auto& mtr5, auto& mtr6, auto& k1, auto& k2, auto& v0) {
            K2LOG_I(log::k23si, "------- SC07.case01 ( Commit-End a transaction before it has any operations ) -------");
            return doEnd(k1, mtr, collname, true, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
            .then([](auto&& response)  {
                auto& [status, val] = response;
                K2EXPECT(log::k23si, status, dto::K23SIStatus::OperationNotAllowed);
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC07.case02 ( Abort-End a transaction before it has any operations ) -------");
                return doEnd(k1, mtr, collname, false, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                });
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC07.case03 ( Commit-End a transaction missing some of the non-trh keys ) -------");
                return seastar::when_all(
                    doWrite(k1, v0, mtr, k1, collname, false, true, ErrorCaseOpt::NoInjection),
                    doWrite(k2, v0, mtr, k1, collname, false, false, ErrorCaseOpt::NoInjection))
                .then([&](auto&& response) {
                    auto& [resp1, resp2] = response;
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::Created);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::Created);
                })
                .then([&] {
                    return doEnd(k1, mtr, collname, true, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                })
                .then([&] {
                    return doInspectTxn(k1, mtr, collname)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::KeyNotFound);
                    });
                })
                .then([&] {
                    return seastar::when_all(doInspectRecords(k1, collname), doInspectWIs(k2))
                    .then([&](auto&& response)  {
                        auto& [resp1, resp2] = response;
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        K2EXPECT(log::k23si, status1, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, val1.records.size(), 1);
                        K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                        bool found = false;
                        for (const k2::dto::WriteIntent& WI : val2.WIs) {
                            if (WI.data.timestamp == mtr.timestamp) {
                                found = true;
                                break;
                            }
                        }
                        K2EXPECT(log::k23si, found, true);
                    });
                });
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC07.case04 ( Commit-End a transaction missing the trh key ) -------");
                return seastar::when_all(
                    doWrite(k1, v0, mtr2, k1, collname, false, true, ErrorCaseOpt::NoInjection),
                    doWrite(k2, v0, mtr2, k1, collname, false, false, ErrorCaseOpt::NoInjection))
                .then([&](auto&& response) {
                    auto& [resp1, resp2] = response;
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::Created);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::Created);
                })
                .then([&] {
                    return doEnd(k1, mtr2, collname, true, {k2}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                })
                .then([&] {
                    return seastar::when_all(doInspectWIs(k1), doInspectRecords(k2, collname))
                    .then([&](auto&& response)  {
                        auto& [resp1, resp2] = response;
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        K2EXPECT(log::k23si, status1, dto::K23SIStatus::OK);
                        bool found = false;
                        for (const k2::dto::WriteIntent& WI : val1.WIs) {
                            if (WI.data.timestamp == mtr2.timestamp) {
                                found = true;
                                break;
                            }
                        }
                        K2EXPECT(log::k23si, found, true);
                        K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, val2.records.size(), 1);
                    });
                });
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC07.case05 ( Abort-End a transaction missing some of the non-trh keys ) -------");
                return seastar::when_all(
                    doWrite(k1, v0, mtr3, k1, collname, false, true, ErrorCaseOpt::NoInjection),
                    doWrite(k2, v0, mtr3, k1, collname, false, false, ErrorCaseOpt::NoInjection))
                .then([&](auto&& response) {
                    auto& [resp1, resp2] = response;
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::Created);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::Created);
                })
                .then([&] {
                    return doEnd(k1, mtr3, collname, false, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                })
                .then([&] {
                    return seastar::when_all(doInspectRecords(k1, collname), doInspectWIs(k2))
                    .then([&](auto&& response)  {
                        auto& [resp1, resp2] = response;
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();

                        K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, val1.records.size(), 1);

                        K2EXPECT(log::k23si, status1, dto::K23SIStatus::OK);
                        bool found = false;
                        for (const k2::dto::WriteIntent& WI : val2.WIs) {
                            if (WI.data.timestamp == mtr3.timestamp) {
                                found = true;
                                break;
                            }
                        }
                        K2EXPECT(log::k23si, found, true);

                    });
                });
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC07.case06 ( Abort-End a transaction missing the trh key ) -------");
                return seastar::when_all(
                    doWrite(k1, v0, mtr4, k1, collname, false, true, ErrorCaseOpt::NoInjection),
                    doWrite(k2, v0, mtr4, k1, collname, false, false, ErrorCaseOpt::NoInjection))
                .then([&](auto&& response) {
                    auto& [resp1, resp2] = response;
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::Created);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::Created);
                })
                .then([&] {
                    return doEnd(k1, mtr4, collname, false, {k2}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                })
                .then([&] {
                    return seastar::when_all(doInspectWIs(k1), doInspectRecords(k2, collname))
                    .then([&](auto&& response)  {
                        auto& [resp1, resp2] = response;
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();

                        K2EXPECT(log::k23si, status1, dto::K23SIStatus::OK);
                        bool found = false;
                        for (const k2::dto::WriteIntent& WI : val1.WIs) {
                            if (WI.data.timestamp == mtr4.timestamp) {
                                found = true;
                                break;
                            }
                        }
                        K2EXPECT(log::k23si, found, true);
                        K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, val2.records.size(), 1);
                    });
                });
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC07.case07 ( Abort-Finalize all the keys in the transaction ) -------");
                return seastar::when_all(doWrite(k1, v0, mtr5, k1, collname, false, true, ErrorCaseOpt::NoInjection), \
                        doWrite(k2, v0, mtr5, k1, collname, false, false, ErrorCaseOpt::NoInjection))
                .then([&](auto&& response) {
                    auto& [resp1, resp2] = response;
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::Created);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::Created);
                })
                .then([&] {
                    return seastar::when_all(doFinalize(k1, mtr5, collname, false, ErrorCaseOpt::NoInjection), \
                            doFinalize(k2, mtr5, collname, false, ErrorCaseOpt::NoInjection))
                    .then([&](auto&& response) {
                        auto& [resp1, resp2] = response;
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        K2EXPECT(log::k23si, status1, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                    });
                })
                .then([&] {
                    return doInspectTxn(k1, mtr5, collname)
                    .then([&](auto&& response)  {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, val.state, dto::TxnRecordState::InProgress);
                    });
                });
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC07.case08 ( Using a transaction with newer timestamp to Push() an old one ) -------");
                return doPush(k1, collname, mtr5, mtr6, ErrorCaseOpt::NoInjection)
                .then([&](auto&& response)  {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    K2EXPECT(log::k23si, val.allowChallengerRetry, true);
                    K2EXPECT(log::k23si, val.incumbentFinalization, dto::EndAction::Abort);
                })
                .then([&] {
                    return seastar::when_all(doInspectTxn(k1, mtr5, collname), doInspectTxn(k1, mtr6, collname))
                    .then([&](auto&& response) {
                        auto& [resp1, resp2] = response;
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        K2EXPECT(log::k23si, status1, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, status2, dto::K23SIStatus::KeyNotFound);
                        K2EXPECT(log::k23si, val1.state, dto::TxnRecordState::ForceAborted);
                        K2EXPECT(log::k23si, val2.state, dto::TxnRecordState::Created);
                    });
                });
            });
        }); // end do-with
    }); // end sc-07
}

seastar::future<> testScenario08() {
    K2LOG_I(log::k23si, "+++++++ TestScenario 08: server-initiated txn abort +++++++");

    return seastar::make_ready_future()
    .then([] {
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            // multi MTRs are used for multiple transactions to execute. We use these MTRs in sequence
            dto::K23SI_MTR { .timestamp = {(ts.tEndTSECount()), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { .timestamp = {(ts.tEndTSECount() + 20000), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { .timestamp = {(ts.tEndTSECount() + 30000), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { .timestamp = {(ts.tEndTSECount() + 40000), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { .timestamp = {(ts.tEndTSECount() + 50000), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::Key {.schemaName = "schema", .partitionKey = "SC08_pkey1", .rangeKey = "rKey1"},
            DataRec {.f1="SC05_f1_zero", .f2="SC04_f2_zero"},
            [this](auto& mtr1, auto& mtr2, auto& mtr3, auto& mtr4, auto& mtr5, auto& k1, auto& v0) {
            K2LOG_I(log::k23si, "------- SC08.case01 ( Txn with a old timestamp WI is PUSHed by another txn's READ ) -------");
            return doWrite(k1, v0, mtr1, k1, collname, false, true, ErrorCaseOpt::NoInjection)
            .then([](auto&& response)  {
                auto& [status, val] = response;
                K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
            })
            .then([&] {
                return doRead(k1, mtr2, collname, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::KeyNotFound);
                });
            })
            .then([&] {
                return doInspectTxn(k1, mtr1, collname)
                .then([&](auto&& response)  {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    K2EXPECT(log::k23si, val.state, dto::TxnRecordState::ForceAborted);
                });
            })
            .then([&] {
                return doEnd(k1, mtr1, collname, false, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                });
            })
            .then([&] {
                return doInspectTxn(k1, mtr1, collname)
                .then([&](auto&& response)  {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::KeyNotFound);
                    K2EXPECT(log::k23si, val.state, dto::TxnRecordState::Created);
                });
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC08.case02 ( Txn WRITE happens with the time older than the Read-Cache record ) -------");
                return doWrite(k1, v0, mtr1, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([&](auto&& response) {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::AbortRequestTooOld);
                })
                .then([&] {
                    return doInspectTxn(k1, mtr1, collname)
                    .then([&](auto&& response)  {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, val.state, dto::TxnRecordState::InProgress);
                    });
                })
                .then([&] {
                    return doEnd(k1, mtr1, collname, false, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                });
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC08.case03 ( The timestamp is older than the latest committed record ) -------");
                return doWrite(k1, v0, mtr3, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([&](auto&& response) {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                })
                .then([&] {
                    return doEnd(k1, mtr3, collname, true, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([&](auto&& response) {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                })
                .then([&]{
                    return doWrite(k1, v0, mtr1, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([&](auto&& response) {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::AbortRequestTooOld);
                    });
                })
                .then([&] {
                    return doInspectTxn(k1, mtr1, collname)
                    .then([&](auto&& response)  {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, val.state, dto::TxnRecordState::InProgress);
                    });
                })
                .then([&] {
                    return doEnd(k1, mtr1, collname, false, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                });
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC08.case04 ( WRITE timestamp is older than the second latest version of the record, where the latest version of the record is WI ) -------");
                return doWrite(k1, v0, mtr5, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([&](auto&& response) {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                })
                .then([&]{
                    return doWrite(k1, v0, mtr1, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([&](auto&& response) {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::AbortRequestTooOld);
                    });
                })
                .then([&] {
                    return doInspectTxn(k1, mtr1, collname)
                    .then([&](auto&& response)  {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, val.state, dto::TxnRecordState::InProgress);
                    });
                })
                .then([&] {
                    return doEnd(k1, mtr1, collname, false, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                    });
                })
                .then([&] {
                    return doInspectTxn(k1, mtr5, collname)
                    .then([&](auto&& response)  {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, val.state, dto::TxnRecordState::InProgress);
                    });
                });
            })
            .then([&] {
                K2LOG_I(log::k23si, "------- SC08.case05 ( The timestamp is newer than all the  committed version of the record, but earlier than the WI of the record ) -------");
                return doWrite(k1, v0, mtr4, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([&](auto&& response) {
                    auto& [status, val] = response;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::AbortConflict);
                })
                .then([&] {
                    return doInspectTxn(k1, mtr4, collname)
                    .then([&](auto&& response)  {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, val.state, dto::TxnRecordState::InProgress);
                    });
                })
                .then([&] {
                    return doInspectTxn(k1, mtr5, collname)
                    .then([&](auto&& response)  {
                        auto& [status, val] = response;
                        K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                        K2EXPECT(log::k23si, val.state, dto::TxnRecordState::InProgress);
                    });
                });
            });
        }); // end do-with
    }); // end sc-08
}


}; // class k23si_testing
} // ns k2

int main(int argc, char** argv){
    k2::App app("txn_testing");
    app.addOptions()("cpo_endpoint", bpo::value<k2::String>(), "The endpoint of the CPO service");
    app.addOptions()("k2_endpoints", bpo::value<std::vector<k2::String>>()->multitoken(), "The endpoints of the k2 cluster");
    app.addApplet<k2::txn_testing>();
    return app.start(argc, argv);
}

