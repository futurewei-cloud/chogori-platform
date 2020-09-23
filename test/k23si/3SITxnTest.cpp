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
    friend std::ostream& operator<<(std::ostream& os, const DataRec& r) {
        return os << "{f1=" << r.f1 << ", f2=" << r.f2 << "}";
    }
};

enum class ErrorCaseOpt: uint8_t {
    NoInjection,
    WrongPartId,        // wrong partition index
    WrongPartVer,       // wrong partition version
    PartMismatchKey,    // key doesn't belong to partition (based on hashing)
    ObsoletePart,       // out-of-date partition version
};


class txn_testing {

public:		// application
	txn_testing() { K2INFO("ctor"); }
	~txn_testing(){ K2INFO("dtor"); }

	static seastar::future<dto::Timestamp> getTimeNow() {
        // TODO call TSO service with timeout and retry logic
        auto nsecsSinceEpoch = sys_now_nsec_count();
        return seastar::make_ready_future<dto::Timestamp>(dto::Timestamp(nsecsSinceEpoch, 123, 1000));
    }

	// required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2INFO("stop");
        return std::move(_testFuture);
    }

	seastar::future<> start(){
        K2INFO("start txn_testing..");

        K2EXPECT(_k2ConfigEps().size(), 3);
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
    uint64_t txnids = 1029;

    dto::PartitionGetter _pgetter;

    // injection parameters for error cases
    dto::Key wrongkey{.schemaName = "schema", .partitionKey = "SC00_wrong_pKey1", .rangeKey = "SC00_wrong_rKey1"}; // wrong partition: id(p1) against p2

    seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
    doWrite(const dto::Key& key, const DataRec& data, const dto::K23SI_MTR mtr, const dto::Key& trh, const String& cname, bool isDelete, bool isTRH, ErrorCaseOpt errOpt) {
        SKVRecord record(cname, std::make_shared<k2::dto::Schema>(_schema));
        record.serializeNext<String>(key.partitionKey);
        record.serializeNext<String>(key.rangeKey);
        record.serializeNext<String>(data.f1);
        record.serializeNext<String>(data.f2);
        K2DEBUG("cname: " << cname << " key=" << key << ",partition hash=" << key.partitionHash())
        auto& part = _pgetter.getPartitionForKey(key);
        dto::K23SIWriteRequest request {
            .pvid = part.partition->pvid,
            .collectionName = cname,
            .mtr = mtr,
            .trh = trh,
            .isDelete = isDelete,
            .designateTRH = isTRH,
            .key = key,
            .value = std::move(record.storage)
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
            K2ASSERT(false, "doWrite() incorrect parameter ErrorCaseOpt.");
            break;
        } // end default
        } // end switch
        return RPC().callRPC<dto::K23SIWriteRequest, dto::K23SIWriteResponse>
                (dto::Verbs::K23SI_WRITE, request, *part.preferredEndpoint, 100ms);
    }

    seastar::future<std::tuple<Status, DataRec>>
    doRead(const dto::Key& key, const dto::K23SI_MTR& mtr, const String& cname, ErrorCaseOpt errOpt) {
        K2DEBUG("key=" << key << ",partition hash=" << key.partitionHash())
        auto& part = _pgetter.getPartitionForKey(key);
        dto::K23SIReadRequest request {
            .pvid = part.partition->pvid,
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
            K2ASSERT(false, "doWrite() incorrect parameter ErrorCaseOpt.");
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
        K2DEBUG("key=" << key << ",partition hash=" << key.partitionHash())
        auto& part = _pgetter.getPartitionForKey(key);
        dto::K23SITxnPushRequest request;
        request.pvid = part.partition->pvid;
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
            K2ASSERT(false, "doWrite() incorrect parameter ErrorCaseOpt.");
            break;
        } // end default
        } // end switch
        return RPC().callRPC<dto::K23SITxnPushRequest, dto::K23SITxnPushResponse>
                (dto::Verbs::K23SI_TXN_PUSH, request, *part.preferredEndpoint, 100ms);
    }

    seastar::future<std::tuple<Status, dto::K23SITxnEndResponse>>
    doEnd(dto::Key trh, dto::K23SI_MTR mtr, String cname, bool isCommit, std::vector<dto::Key> wkeys, Duration dur, ErrorCaseOpt errOpt) {
        K2DEBUG("key=" << trh << ",partition hash=" << trh.partitionHash())
        auto& part = _pgetter.getPartitionForKey(trh);
        dto::K23SITxnEndRequest request;
        request.pvid = part.partition->pvid;
        request.collectionName = cname;
        request.mtr = mtr;
        request.key = trh;
        request.action = isCommit ? dto::EndAction::Commit : dto::EndAction::Abort;
        request.writeKeys = wkeys;
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
            K2ASSERT(false, "doWrite() incorrect parameter ErrorCaseOpt.");
            break;
        } // end default
        } // end switch
        return RPC().callRPC<dto::K23SITxnEndRequest, dto::K23SITxnEndResponse>
                (dto::Verbs::K23SI_TXN_END, request, *part.preferredEndpoint, 100ms);
    }

    seastar::future<std::tuple<Status, dto::K23SITxnFinalizeResponse>>
    doFinalize(dto::Key trh, dto::Key key, dto::K23SI_MTR mtr, String cname, bool isCommit, ErrorCaseOpt errOpt) {
        K2DEBUG("key=" << key << ",partition hash=" << key.partitionHash())
        auto& part = _pgetter.getPartitionForKey(key);
        dto::K23SITxnFinalizeRequest request;
        request.pvid = part.partition->pvid;
        request.collectionName = cname;
        request.trh = trh;
        request.key = key;
        request.mtr = mtr;
        request.action = isCommit ? dto::EndAction::Commit : dto::EndAction::Abort;
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
            K2ASSERT(false, "doWrite() incorrect parameter ErrorCaseOpt.");
            break;
        } // end default
        } // end switch
        return RPC().callRPC<dto::K23SITxnFinalizeRequest, dto::K23SITxnFinalizeResponse>
                (dto::Verbs::K23SI_TXN_FINALIZE, request, *part.preferredEndpoint, 100ms);
    }

    seastar::future<std::tuple<Status, dto::K23SITxnHeartbeatResponse>>
    doHeartbeat(dto::Key key, dto::K23SI_MTR mtr, String cname, ErrorCaseOpt errOpt) {
        K2DEBUG("key=" << key << ",partition hash=" << key.partitionHash())
        auto& part = _pgetter.getPartitionForKey(key);
        dto::K23SITxnHeartbeatRequest request;
        request.pvid = part.partition->pvid;
        request.collectionName = cname;
        request.key = key;
        request.mtr = mtr;
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
            K2ASSERT(false, "doWrite() incorrect parameter ErrorCaseOpt.");
            break;
        } // end default
        } // end switch
        return RPC().callRPC<dto::K23SITxnHeartbeatRequest, dto::K23SITxnHeartbeatResponse>
                (dto::Verbs::K23SI_TXN_HEARTBEAT, request, *part.preferredEndpoint, 100ms);
    }

    seastar::future<std::tuple<Status, dto::K23SIInspectRecordsResponse>>
    doInspectRecords(const dto::Key& key, const String& cname) {
        auto& part = _pgetter.getPartitionForKey(key);
        dto::K23SIInspectRecordsRequest request;
        request.pvid = part.partition->pvid;
        request.collectionName = cname;
        request.key = key;
        return RPC().callRPC<dto::K23SIInspectRecordsRequest, dto::K23SIInspectRecordsResponse>
                (dto::Verbs::K23SI_INSPECT_RECORDS, request, *part.preferredEndpoint, 100ms);
    }

    seastar::future<std::tuple<Status, dto::K23SIInspectTxnResponse>>
    doInspectTxn(const dto::Key& key, const dto::K23SI_MTR& mtr, const String& cname) {
        auto& part = _pgetter.getPartitionForKey(key);
        dto::K23SIInspectTxnRequest request;
        request.pvid = part.partition->pvid;
        request.collectionName = cname;
        request.key = key;
        request.mtr = mtr;
        return RPC().callRPC<dto::K23SIInspectTxnRequest, dto::K23SIInspectTxnResponse>
                (dto::Verbs::K23SI_INSPECT_TXN, request, *part.preferredEndpoint, 100ms);
    }


public:		// test	scenario

// Any request (READ, WRITE, PUSH, END, FINALIZE, HEARTBEAT) should observe a timeout(404_not_found)
// example of command: CPO_COLLECTION_GET & K23SI_WRITE
seastar::future<> testScenario00() {
    K2INFO("+++++++ TestScenario 00: unassigned nodes +++++++");
	K2INFO("--->Test SETUP: start a cluster but don't create a collection. Any requests observe a timeout.");

	return seastar::make_ready_future()
	.then([this] {
		// command: K23SI_WRITE
		K2INFO("Test case SC00_1: K23SI_WRITE");
		return getTimeNow()
		.then([&](dto::Timestamp&& ts) {
			return seastar::do_with(
				dto::K23SI_MTR{
					.txnid = txnids++,
					.timestamp = std::move(ts),
					.priority = dto::TxnPriority::Medium
				},
				dto::Key{.schemaName = "schema", .partitionKey = "SC00_pKey1", .rangeKey = "SC00_rKey1"},
				dto::Key{.schemaName = "schema", .partitionKey = "SC00_pKey1", .rangeKey = "SC00_rKey1"},
				[this](dto::K23SI_MTR& mtr, dto::Key& key, dto::Key& trh){
                    dto::Partition::PVID pvid0;
                    dto::K23SIWriteRequest request;
                    request.pvid = pvid0;
                    request.collectionName = collname;
                    request.mtr = mtr;
                    request.trh = trh;
                    request.isDelete = false;
                    request.designateTRH = true;
                    request.key = key;
                    request.value = SKVRecord::Storage{};
                    return RPC().callRPC<dto::K23SIWriteRequest, dto::K23SIWriteResponse>(dto::Verbs::K23SI_WRITE, request, *_k2Endpoints[0], 100ms)
                    .then([this](auto&& response) {
                		// response: K23SI_WRITE
                		auto& [status, resp] = response;
                        K2EXPECT(status, Statuses::S503_Service_Unavailable);
                		K2INFO("response: K23SI_WRITE. " << "status: " << status.code << " with MESG: " << status.message);
                	});
				}
			);
		});
	}) // end K23SI_WRITE
    .then([this] {
        // command: K23SI_READ
        K2INFO("Test case SC00_2: K23SI_READ");
        dto::Partition::PVID pvid0;
        dto::K23SIReadRequest request {
            .pvid = pvid0,
            .collectionName = collname,
            .mtr = {txnids++, dto::Timestamp(20200828, 1, 1000), dto::TxnPriority::Medium},
            .key = {"schema", "SC00_pKey1", "SC00_rKey1"}
        };
        return RPC().callRPC<dto::K23SIReadRequest, dto::K23SIReadResponse>
                (dto::Verbs::K23SI_READ, request, *_k2Endpoints[0], 100ms)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Statuses::S503_Service_Unavailable);
            K2INFO("response: K23SI_READ. " << "status: " << status.code << " with MESG: " << status.message);
        });
    }) // end  K23SI_READ
    .then([this] {
        // command: K23SI_TXN_PUSH
        K2INFO("Test case SC00_3: K23SI_TXN_PUSH");
        dto::Partition::PVID pvid0;
        dto::K23SITxnPushRequest request {
            .pvid = pvid0,
            .collectionName = collname,
            .key = {"schema", "SC00_pKey1", "SC00_rKey1"},
            .incumbentMTR = {txnids-1, dto::Timestamp(20200828, 1, 1000), dto::TxnPriority::Medium},
            .challengerMTR = {txnids-2, dto::Timestamp(20200101, 1, 1000), dto::TxnPriority::Medium}
        };
        return RPC().callRPC<dto::K23SITxnPushRequest, dto::K23SITxnPushResponse>
                (dto::Verbs::K23SI_TXN_PUSH, request, *_k2Endpoints[0], 100ms)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Statuses::S503_Service_Unavailable);
            K2INFO("response: K23SI_TXN_PUSH. " << "status: " << status.code << " with MESG: " << status.message);
        });
    }) // end K23SI_TXN_PUSH
    .then([this] {
        // command: K23SI_TXN_END
        K2INFO("Test case SC00_4: K23SI_TXN_END");
        dto::Partition::PVID pvid0;
        dto::K23SITxnEndRequest request {
            .pvid = pvid0,
            .collectionName = collname,
            .key = {"schema", "SC00_pKey1", "SC00_rKey1"},
            .mtr = {txnids-1, dto::Timestamp(20200828, 1, 1000), dto::TxnPriority::Medium},
            .action = dto::EndAction::Abort,
            .writeKeys = {{"schema", "SC00_pKey1", "SC00_rKey1"}},
            .syncFinalize = false
        };
        return RPC().callRPC<dto::K23SITxnEndRequest, dto::K23SITxnEndResponse>
                (dto::Verbs::K23SI_TXN_END, request, *_k2Endpoints[0], 100ms)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Statuses::S503_Service_Unavailable);
            K2INFO("response: K23SI_TXN_END. " << "status: " << status.code << " with MESG: " << status.message);
        });
    }) // end K23SI_TXN_END
    .then([this] {
        // command: K23SI_TXN_FINALIZE
        K2INFO("Test case SC00_5: K23SI_TXN_FINALIZE");
        dto::Partition::PVID pvid0;
        dto::K23SITxnFinalizeRequest request {
            .pvid = pvid0,
            .collectionName = collname,
            .trh = {"schema", "SC00_pKey1", "SC00_rKey1"},
            .mtr = {txnids-1, dto::Timestamp(20200828, 1, 1000), dto::TxnPriority::Medium},
            .key = {"schema", "SC00_pKey1", "SC00_rKey1"},
            .action = dto::EndAction::Abort
        };
        return RPC().callRPC<dto::K23SITxnFinalizeRequest, dto::K23SITxnFinalizeResponse>
                (dto::Verbs::K23SI_TXN_FINALIZE, request, *_k2Endpoints[0], 100ms)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Statuses::S503_Service_Unavailable);
            K2INFO("response: K23SI_TXN_FINALIZE. " << "status: " << status.code << " with MESG: " << status.message);
        });
    }) // end K23SI_TXN_FINALIZE
    .then([this] {
        // command: K23SI_TXN_HEARTBEAT
        K2INFO("Test case SC00_6: K23SI_TXN_HEARTBEAT");
        dto::Partition::PVID pvid0;
        dto::K23SITxnHeartbeatRequest request {
            .pvid = pvid0,
            .collectionName = collname,
            .key = {"schema", "SC00_pKey1", "SC00_rKey1"},
            .mtr = {txnids-1, dto::Timestamp(20200828, 1, 1000), dto::TxnPriority::Medium},
        };
        return RPC().callRPC<dto::K23SITxnHeartbeatRequest, dto::K23SITxnHeartbeatResponse>
                (dto::Verbs::K23SI_TXN_HEARTBEAT, request, *_k2Endpoints[0], 100ms)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Statuses::S503_Service_Unavailable);
            K2INFO("response: K23SI_TXN_HEARTBEAT. " << "status: " << status.code << " with MESG: " << status.message);
        });

    });
}

seastar::future<> testScenario01() {
	K2INFO("+++++++ TestScenario 01: assigned node with no data +++++++");
	K2INFO("--->Test SETUP: start a cluster and assign collection. Do not write any data.");

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
            K2EXPECT(status, dto::K23SIStatus::Created);
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
            K2EXPECT(status, dto::K23SIStatus::OK);
            _pgetter = dto::PartitionGetter(std::move(resp.collection));
        })
        .then([this] () {
            dto::CreateSchemaRequest request{ collname, _schema };
            return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *_cpoEndpoint, 1s);
        })
        .then([] (auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Statuses::S200_OK);
        });
    })
    .then([] {
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
    // SC01 case1: OP with bad collection name
        K2INFO("------- SC01.case 01 (OP with bad collection name) -------");
        return seastar::do_with(
            dto::K23SI_MTR {
                .txnid = txnids++,
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
                    K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                    K2INFO("SC01.case01(bad collection name)::OP_Write. " << "status: " << status.code << " with MESG: " << status.message);
                })
                // case"bad collection name"  --> OP:READ
                .then([this, &key, &mtr] {
                    return doRead(key, mtr, badCname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case01(bad collection name)::OP_READ. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"bad collection name"  --> OP:PUSH
                .then([this, &key, &mtr] {
                    return doPush(key, badCname, mtr, mtr, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case01(bad collection name)::OP_PUSH. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"bad collection name"  --> OP:END
                .then([this, &trh, &key, &mtr] {
                    return doEnd(trh, mtr, badCname, false, {key}, Duration(0s), ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case01(bad collection name)::OP_END. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"bad collection name"  --> OP:FINALIZE
                .then([this, &trh, &key, &mtr] {
                    return doFinalize(trh, key, mtr, badCname, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case01(bad collection name)::OP_FINALIZE. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"bad collection name"  --> OP:HEARTBEAT
                .then([this, &key, &mtr] {
                    return doHeartbeat(key, mtr, badCname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case01(bad collection name)::OP_HEARTBEAT. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                });
        }); // end do_with
    }) // end SC-01 case-01
    .then([this] {
    // SC01 case2: OP outside retention window
        K2INFO("------- SC01.case 02 (OP outside retention window) -------");
        K2INFO("Get a stale timestamp(1,000,000) as the stale_ts");
        return seastar::do_with(
            dto::K23SI_MTR {
                .txnid = txnids++,
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
                    K2INFO("SC01.case02(stale request)::OP_WRITE. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::AbortRequestTooOld);
                })
                // case"stale request"  --> OP:READ
                .then([this, &key, &mtr] {
                    return doRead(key, mtr, collname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::AbortRequestTooOld);
                        K2INFO("SC01.case02(stale request)::OP_READ. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"stale request"  --> OP:END
                .then([this, &mtr, &trh, &key] {
                    return doEnd(trh, mtr, collname, false, {key}, Duration(0s), ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::AbortRequestTooOld);
                        K2INFO("SC01.case02(stale request)::OP_END. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"stale request"  --> OP:HEARTBEAT
                .then([this, &mtr, &key] {
                    return doHeartbeat(key, mtr, collname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::AbortRequestTooOld);
                        K2INFO("SC01.case02(stale request)::OP_HEARTBEAT. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // stale request for PUSH, only validate challenger MTRs
                .then([this, &mtr, &key] {
                    return doPush(key, collname, mtr, mtr, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2INFO("SC01.case02(stale request)::OP_PUSH. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::AbortRequestTooOld)
                    });
                })
                // stale request for FINALIZE, test Finalize-commit & Finalize-abort
                .then([this, &key, &trh, &mtr] {
                    return doFinalize(trh, key, mtr, collname, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2INFO("SC01.case02::OP_Finalize_Commit. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::OperationNotAllowed)
                    })
                    .then([this, &mtr, &key, &trh] {
                        return doFinalize(trh, key, mtr, collname, false, ErrorCaseOpt::NoInjection)
                        .then([](auto&& response) {
                            auto& [status, resp] = response;
                            K2INFO("SC01.case02::OP_Finalize_Abort. " << "status: " << status.code << " with MESG: " << status.message);
                            K2EXPECT(status, dto::K23SIStatus::OK)
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
        K2INFO("------- SC01.case 03 (OP with wrong partition) -------");
        return seastar::do_with(
            dto::K23SI_MTR {
                .txnid = txnids++,
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
                    K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                    K2INFO("SC01.case03(wrong partition)::OP_Write. " << "status: " << status.code << " with MESG: " << status.message);
                })
                // case"wrong partition"  --> OP:READ
                .then([this, &key, &mtr] {
                    return doRead(key, mtr, collname, ErrorCaseOpt::WrongPartId)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case03(bad collection name)::OP_READ. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"wrong partition"  --> OP:PUSH
                .then([this, &key, &mtr] {
                    return doPush(key, collname, mtr, mtr, ErrorCaseOpt::WrongPartId)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case03(bad collection name)::OP_PUSH. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"wrong partition"  --> OP:END
                .then([this, &trh, &key, &mtr] {
                    return doEnd(trh, mtr, collname, false, {key}, Duration(0s), ErrorCaseOpt::WrongPartId)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case03(bad collection name)::OP_END. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"wrong partition"  --> OP:FINALIZE
                .then([this, &trh, &key, &mtr] {
                    return doFinalize(trh, key, mtr, collname, false, ErrorCaseOpt::WrongPartId)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case03(bad collection name)::OP_FINALIZE. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"wrong partition"  --> OP:HEARTBEAT
                .then([this, &key, &mtr] {
                    return doHeartbeat(key, mtr, collname, ErrorCaseOpt::WrongPartId)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case03(bad collection name)::OP_HEARTBEAT. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                });

        }); // end do_with
    }) // end sc-01 case-03
    .then([] {
    // SC01 case4: OP key which doesn't belong to partition (based on hashing)
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2INFO("------- SC01.case 04 (key doesn't belong to the partition) -------");
        return seastar::do_with(
            dto::K23SI_MTR {
                .txnid = txnids++,
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
                    K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                    K2INFO("SC01.case04(mismatch of partition and key)::OP_Write. " << "status: " << status.code << " with MESG: " << status.message);
                })
                // case"wrong partition"  --> OP:READ
                .then([this, &key, &mtr] {
                    return doRead(key, mtr, collname, ErrorCaseOpt::PartMismatchKey)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case04(mismatch of partition and key)::OP_READ. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"wrong partition"  --> OP:PUSH
                .then([this, &key, &mtr] {
                    return doPush(key, collname, mtr, mtr, ErrorCaseOpt::PartMismatchKey)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case04(mismatch of partition and key)::OP_PUSH. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"wrong partition"  --> OP:END
                .then([this, &trh, &key, &mtr] {
                    return doEnd(trh, mtr, collname, false, {key}, Duration(0s), ErrorCaseOpt::PartMismatchKey)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case04(mismatch of partition and key)::OP_END. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"wrong partition"  --> OP:FINALIZE
                .then([this, &trh, &key, &mtr] {
                    return doFinalize(trh, key, mtr, collname, false, ErrorCaseOpt::PartMismatchKey)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case04(mismatch of partition and key)::OP_FINALIZE. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"wrong partition"  --> OP:HEARTBEAT
                .then([this, &key, &mtr] {
                    return doHeartbeat(key, mtr, collname, ErrorCaseOpt::PartMismatchKey)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case04(mismatch of partition and key)::OP_HEARTBEAT. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                });

        }); // end do_with
    }) // end sc-01 case-04
    .then([] {
    // SC01 case5: OP out-of-date partition version
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2INFO("------- SC01.case 05 (out-of-date partition version) -------");
        return seastar::do_with(
            dto::K23SI_MTR {
                .txnid = txnids++,
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
                    K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                    K2INFO("SC01.case05(Obsolete Partition)::OP_Write. " << "status: " << status.code << " with MESG: " << status.message);
                })
                // case"wrong partition"  --> OP:READ
                .then([this, &key, &mtr] {
                    return doRead(key, mtr, collname, ErrorCaseOpt::ObsoletePart)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case05(Obsolete Partition)::OP_READ. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"wrong partition"  --> OP:PUSH
                .then([this, &key, &mtr] {
                    return doPush(key, collname, mtr, mtr, ErrorCaseOpt::ObsoletePart)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case05(Obsolete Partition)::OP_PUSH. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"wrong partition"  --> OP:END
                .then([this, &trh, &key, &mtr] {
                    return doEnd(trh, mtr, collname, false, {key}, Duration(0s), ErrorCaseOpt::ObsoletePart)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case05(Obsolete Partition)::OP_END. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"wrong partition"  --> OP:FINALIZE
                .then([this, &trh, &key, &mtr] {
                    return doFinalize(trh, key, mtr, collname, false, ErrorCaseOpt::ObsoletePart)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case05(Obsolete Partition)::OP_FINALIZE. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"wrong partition"  --> OP:HEARTBEAT
                .then([this, &key, &mtr] {
                    return doHeartbeat(key, mtr, collname, ErrorCaseOpt::ObsoletePart)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case05(Obsolete Partition)::OP_HEARTBEAT. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                });

        }); // end do_with
    }) // end sc-01 case-05
    .then([] {
    // SC01 case06: READ/WRITE/FINALIZE empty partition key, empty range key
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2INFO("------- SC01.case 06 (empty partition key, empty range key) -------");
        return seastar::do_with(
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {},
            dto::Key {},
            DataRec {.f1="SC01_field1", .f2="SC01_field2"},
            [this](dto::K23SI_MTR& mtr, dto::Key& key, dto::Key& trh, DataRec& rec) {
                return doWrite(key, rec, mtr, trh, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(status, dto::K23SIStatus::BadParameter);
                    K2INFO("SC01.case06(empty partition key, empty range key)::OP_Write. " << "status: " << status.code << " with MESG: " << status.message);
                })
                .then([this, &key, &mtr] {
                    return doRead(key, mtr, collname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::BadParameter);
                        K2INFO("SC01.case06(empty partition key, empty range key)::OP_READ. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                .then([this, &trh, &key, &mtr] {
                    return doFinalize(trh, key, mtr, collname, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::BadParameter);
                        K2INFO("SC01.case06(empty partition key, empty range key)::OP_FINALIZE. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                });
        }); // end do-with
    }) // end sc-01 case-06
    .then([] {
    // SC01 case07: READ/WRITE/FINALIZE empty partition key, non-empty range key
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2INFO("------- SC01.case 07 (empty partition key, non-empty range key) -------");
        return seastar::do_with(
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            DataRec {.f1="SC01_field1", .f2="SC01_field2"},
            [this](dto::K23SI_MTR& mtr, DataRec& rec) {
                dto::Key missPartKey;
                missPartKey.rangeKey = "SC01_rKey1";
                return doWrite(missPartKey, rec, mtr, missPartKey, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(status, dto::K23SIStatus::BadParameter);
                    K2INFO("SC01.case07(empty partition key, non-empty range key)::OP_Write. " << "status: " << status.code << " with MESG: " << status.message);
                })
                .then([this, &mtr] {
                    dto::Key missPartKey;
                    missPartKey.rangeKey = "SC01_rKey1";
                    return doRead(missPartKey, mtr, collname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::BadParameter);
                        K2INFO("SC01.case07(empty partition key, non-empty range key)::OP_READ. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                .then([this, &mtr] {
                    dto::Key missPartKey;
                    missPartKey.rangeKey = "SC01_rKey1";
                    return doFinalize(missPartKey, missPartKey, mtr, collname, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::BadParameter);
                        K2INFO("SC01.case07(empty partition key, non-empty range key)::OP_FINALIZE. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                });
        }); // end do-with
    }) // end sc-01 case-07
    .then([] {
    // SC01 case08: READ/WRITE/FINALIZE with only partitionKey
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2INFO("------- SC01.case 08 (READ/WRITE/FINALIZE with only partitionKey) -------");
        return seastar::do_with(
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            DataRec {.f1="SC01_field1", .f2="SC01_field2"},
            [this](dto::K23SI_MTR& mtr, DataRec& rec) {
                dto::Key onlyPartKey;
                onlyPartKey.schemaName = "schema";
                onlyPartKey.partitionKey = "SC01_pKey1";
                return doWrite(onlyPartKey, rec, mtr, onlyPartKey, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(status, dto::K23SIStatus::Created);
                    K2INFO("SC01.case08(only partitionKey)::OP_Write. " << "status: " << status.code << " with MESG: " << status.message);
                })
                .then([this, &mtr] {
                    dto::Key onlyPartKey;
                    onlyPartKey.schemaName = "schema";
                    onlyPartKey.partitionKey = "SC01_pKey1";
                    return doRead(onlyPartKey, mtr, collname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::OK);
                        K2INFO("SC01.case08(only partitionKey)::OP_READ. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                .then([this, &mtr] {
                    dto::Key onlyPartKey;
                    onlyPartKey.schemaName = "schema";
                    onlyPartKey.partitionKey = "SC01_pKey1";
                    return doFinalize(onlyPartKey, onlyPartKey, mtr, collname, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::OK);
                        K2INFO("SC01.case08(only partitionKey)::OP_FINALIZE. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                });
        }); // end do-with
    }) // end sc-01 case-08
    .then([] {
    // SC01 case09: READ/WRITE/FINALIZE with partition and range key
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2INFO("------- SC01.case 09 (READ/WRITE/FINALIZE with partition and range key) -------");
        return seastar::do_with(
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            DataRec {.f1="SC01_field1", .f2="SC01_field2"},
            [this](dto::K23SI_MTR& mtr, dto::Key& key, dto::Key& trh, DataRec& rec) {
                return doWrite(key, rec, mtr, trh, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(status, dto::K23SIStatus::Created);
                    K2INFO("SC01.case09(partition and range key)::OP_Write. " << "status: " << status.code << " with MESG: " << status.message);
                })
                .then([this, &key, &mtr] {
                    return doRead(key, mtr, collname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::OK);
                        K2INFO("SC01.case09(partition and range key)::OP_READ. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                .then([this, &trh, &key, &mtr] {
                    return doFinalize(trh, key, mtr, collname, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::OK);
                        K2INFO("SC01.case09(partition and range key)::OP_FINALIZE. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                });
        }); // end do-with
    }) // end sc-01 case-09
    .then([] {
    // SC01 case10: cascading error: READ/WRITE/FINALIZE with bad collection name AND missing partition key
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2INFO("------- SC01.case 10 (bad coll name & missing partition key) -------");
        return seastar::do_with(
            dto::K23SI_MTR {
                .txnid = txnids++,
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
                    K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                    K2INFO("SC01.case10(bad coll name & missing partition key)::OP_Write. " << "status: " << status.code << " with MESG: " << status.message);
                })
                // case"wrong partition"  --> OP:READ
                .then([this, &mtr] {
                    dto::Key missPartKey;
                    missPartKey.rangeKey = "SC01_rKey1";
                    return doRead(missPartKey, mtr, badCname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case10(bad coll name & missing partition key)::OP_READ. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                // case"wrong partition"  --> OP:FINALIZE
                .then([this, &trh, &mtr] {
                    dto::Key missPartKey;
                    missPartKey.rangeKey = "SC01_rKey1";
                    return doFinalize(trh, missPartKey, mtr, badCname, false, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC01.case10(bad coll name & missing partition key)::OP_FINALIZE. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                });
        }); // end do_with
    }) // end sc-01 case-10
    .then([] {
    // SC01 case11: TXN with 2 writes for 2 different partitions ends with Commit. Validate with a read txn afterward
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2INFO("------- SC01.case 11 (TXN with 2 writes for 2 different partitions ends with Commit) -------");
        return seastar::do_with(
        // #1 write Txn in two partitions
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_diff_pKey2", .rangeKey = "SC01_diff_rKey2" },
            dto::Key {.schemaName = "schema", .partitionKey = "SC01_pKey1", .rangeKey = "SC01_rKey1" },
            DataRec {.f1="SC01_field1", .f2="SC01_field2"},
            DataRec {.f1="SC01_field3", .f2="SC01_field4"},
            [this](dto::K23SI_MTR& mtr, dto::Key& key1, dto::Key& key2, dto::Key& trh, DataRec& rec1, DataRec& rec2) {

                auto& part = _pgetter.getPartitionForKey(key1);
                K2INFO("------- Partition Info printer for key1("<< key1 << "), partition: " << part);
                auto& part2 = _pgetter.getPartitionForKey(key2);
                K2INFO("------- Partition Info printer for key1("<< key2 << "), partition: " << part2);

                return doWrite(key1, rec1, mtr, trh, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2INFO("SC01.case11::OP_Write_Key1_in_Part2. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::Created);
                })
                .then([this, &key2, &rec2, &mtr, &trh] {
                    return doWrite(key2, rec2, mtr, trh, collname, false, false, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2INFO("SC01.case11::OP_Write_Key2_in_Part1. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::Created);
                    });
                })
                .then([this, &trh, &mtr, &key1, &key2] {
                    return doEnd(trh, mtr, collname, true, {key1, key2}, Duration(0s), ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2INFO("SC01.case11::OP_End_Commit. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                    });
                })
                .then([] {
                    return seastar::sleep(500ms);
                });
        }) // end do-with
        // #2 read Txn to validate
        .then([this] {
            return getTimeNow();
        })
        .then([this](dto::Timestamp&& ts) {
            return seastar::do_with(
                dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
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
                        K2INFO("SC01.case11::OP_READ_Key1. " << "status: " << status1.code << " with MESG: " << status1.message);
                        K2INFO("SC01.case11::OP_READ_Key2. " << "status: " << status2.code << " with MESG: " << status2.message);
                        K2INFO("Value of Key1: " << val1 << ". Value of key2: " << val2);
                        K2EXPECT(status1, dto::K23SIStatus::OK);
                        K2EXPECT(status2, dto::K23SIStatus::OK);
                        K2EXPECT(val1, cmpRec1);
                        K2EXPECT(val2, cmpRec2);
                    });
            });
        });
    }) // end sc-01 case-11
    .then([] {
    // SC01 case12: TXN with 2 writes for 2 different partitions ends with Abort. Validate with a read txn afterward
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2INFO("------- SC01.case 12 (TXN with 2 writes for 2 different partitions ends with Abort) -------");
        return seastar::do_with(
        // #1 write Txn in two partitions and the abort
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
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
                    K2INFO("SC01.case12::OP_Write_Key1_in_Part2. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::Created);
                })
                .then([this, &key2, &rec2, &mtr, &trh] {
                    return doWrite(key2, rec2, mtr, trh, collname, false, false, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2INFO("SC01.case12::OP_Write_Key2_in_Part1. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::Created);
                    });
                })
                .then([this, &trh, &mtr, &key1, &key2] {
                    return doEnd(trh, mtr, collname, false, {key1, key2}, Duration(0s), ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2INFO("SC01.case12::OP_End_Abort. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                    });
                })
                .then([] {
                    return seastar::sleep(500ms);
                });
        }) // end do-with
        // #2 read Txn to validate
        .then([this] {
            return getTimeNow();
        })
        .then([this](dto::Timestamp&& ts) {
            return seastar::do_with(
                dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
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
                        K2INFO("SC01.case11::OP_READ_Key1. " << "status: " << status1.code << " with MESG: " << status1.message);
                        K2INFO("SC01.case11::OP_READ_Key2. " << "status: " << status2.code << " with MESG: " << status2.message);
                        K2INFO("Value of Key1: " << val1 << ". Value of key2: " << val2);
                        K2EXPECT(status1, dto::K23SIStatus::OK);
                        K2EXPECT(status2, dto::K23SIStatus::OK);
                        K2EXPECT(val1, cmpRec1);
                        K2EXPECT(val2, cmpRec2);
                    });
            });
        });
    }); // end sc-01 case-12
}

seastar::future<> testScenario02() {
	K2INFO("+++++++ TestScenario 02: assigned node with single version data +++++++");
	K2INFO("--->Test SETUP: The following data have been written in the given state.");
    K2INFO("('SC02_pkey1','', v1) -> commited");
    K2INFO("('SC02_pkey2','range1', v1) -> commited");
    K2INFO("('SC02_pkey3','', v1) -> WI");
    K2INFO("('SC02_pkey4','', v1) -> aborted but not cleaned");

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
            K2EXPECT(status, dto::K23SIStatus::Created);
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
            K2INFO("SC02. " << "status: " << status.code << " with MESG: " << status.message);
            K2INFO("sc02 collection name|hashScheme|retention|heartbeat: " << resp.collection.metadata.name << " | " << resp.collection.metadata.hashScheme << " | "\
                    << resp.collection.metadata.retentionPeriod << " | " << resp.collection.metadata.heartbeatDeadline);
            K2INFO("partition assignment state: p1|p2|p3: " << resp.collection.partitionMap.partitions[0].astate << " | "  \
                    << resp.collection.partitionMap.partitions[1].astate << " | " << resp.collection.partitionMap.partitions[2].astate);
            K2EXPECT(status, dto::K23SIStatus::OK);
            K2EXPECT(resp.collection.partitionMap.partitions[0].astate, dto::AssignmentState::FailedAssignment);
            K2EXPECT(resp.collection.partitionMap.partitions[1].astate, dto::AssignmentState::FailedAssignment);
            K2EXPECT(resp.collection.partitionMap.partitions[2].astate, dto::AssignmentState::FailedAssignment);
        });
    })
    .then([] {
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey1", .rangeKey = ""},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey2", .rangeKey = "range1"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey3", .rangeKey = ""},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey4", .rangeKey = ""},
            DataRec {.f1="SC02_f1", .f2="SC02_f2"},
            [this](auto& mtr, auto& k1, auto& k2, auto& k3, auto& k4, auto& v1) {
                return seastar::when_all(doWrite(k1, v1, mtr, k1, collname, false, true, ErrorCaseOpt::NoInjection), doWrite(k2, v1, mtr, k1, collname, false, false, ErrorCaseOpt::NoInjection))
                .then([&](auto&& response) mutable {
                    auto& [resp1, resp2] = response;
                    // move resp out of the incoming futures sice get0() returns an rvalue
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    K2EXPECT(status1, dto::K23SIStatus::Created);
                    K2EXPECT(status2, dto::K23SIStatus::Created);
                })
                .then([this, &mtr, &k1, &k2, &k3, &v1] {
                    return seastar::when_all(doEnd(k1, mtr, collname, true, {k1,k2}, Duration(0s),ErrorCaseOpt::NoInjection), doWrite(k3, v1, mtr, k3, collname, false, true, ErrorCaseOpt::NoInjection));
                })
                .then([&](auto&& response) mutable {
                    auto& [resp1, resp2] = response;
                    // move resp out of the incoming futures sice get0() returns an rvalue
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    K2EXPECT(status1, dto::K23SIStatus::OK);
                    K2EXPECT(status2, dto::K23SIStatus::Created);
                })
                .then([this, &mtr, &k4, &v1] {
                    return doWrite(k4, v1, mtr, k4, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::Created);
                    })
                    .then([this, &mtr, &k4, &v1] {
                        return doEnd(k4, mtr, collname, false, {k4}, Duration(500ms), ErrorCaseOpt::NoInjection)
                        .then([](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(status, dto::K23SIStatus::OK);
                        });
                    });
                });
            }
        );
    }) // end test setup
    .then([] {
        K2INFO("Scenario-02 test setup done.");
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
    // SC02 case1: WRITE/READ with bad collection name
        K2INFO("------- SC02.case 01 (WRITE/READ with bad collection name) -------");
        return seastar::do_with(
            dto::K23SI_MTR {
                .txnid = txnids++,
                .timestamp = std::move(ts),
                .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey1", .rangeKey = ""},
            DataRec {.f1="SC02_f3", .f2="SC02_f4"},
            [this] (dto::K23SI_MTR& mtr, dto::Key& k1, DataRec& v2) {
                // case"bad collection name"  --> OP:WRITE
                return doWrite(k1, v2, mtr, k1, badCname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                    K2INFO("SC02.case01(bad collection name)::OP_Write. " << "status: " << status.code << " with MESG: " << status.message);
                })
                // case"bad collection name"  --> OP:READ
                .then([this, &k1, &mtr] {
                    return doRead(k1, mtr, badCname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC02.case01(bad collection name)::OP_READ. " << "status: " << status.code << " with MESG: " << status.message);
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
        K2INFO("------- SC02.case 02 (READ/WRITE with wrong partition index) -------");
        return seastar::do_with(
            dto::K23SI_MTR {
                .txnid = txnids++,
                .timestamp = std::move(ts),
                .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey1", .rangeKey = ""},
            DataRec {.f1="SC02_f3", .f2="SC02_f4"},
            [this] (dto::K23SI_MTR& mtr, dto::Key& k1, DataRec& v2) {
                // case"wrong partition"  --> OP:WRITE
                return doWrite(k1, v2, mtr, k1, collname, false, true, ErrorCaseOpt::WrongPartId)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                    K2INFO("SC02.case02(wrong part id)::OP_Write. " << "status: " << status.code << " with MESG: " << status.message);
                })
                // case"wrong partition"  --> OP:READ
                .then([this, &k1, &mtr] {
                    return doRead(k1, mtr, collname, ErrorCaseOpt::WrongPartId)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC02.case02(bad part id)::OP_READ. " << "status: " << status.code << " with MESG: " << status.message);
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
        K2INFO("------- SC02.case 03 (READ/WRITE with wrong partition version) -------");
        return seastar::do_with(
            dto::K23SI_MTR {
                .txnid = txnids++,
                .timestamp = std::move(ts),
                .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey1", .rangeKey = ""},
            DataRec {.f1="SC02_f3", .f2="SC02_f4"},
            [this] (dto::K23SI_MTR& mtr, dto::Key& k1, DataRec& v2) {
                // case"wrong partition"  --> OP:WRITE
                return doWrite(k1, v2, mtr, k1, collname, false, true, ErrorCaseOpt::ObsoletePart)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                    K2INFO("SC02.case03(wrong part version)::OP_Write. " << "status: " << status.code << " with MESG: " << status.message);
                })
                // case"wrong partition"  --> OP:READ
                .then([this, &k1, &mtr] {
                    return doRead(k1, mtr, collname, ErrorCaseOpt::ObsoletePart)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::RefreshCollection);
                        K2INFO("SC02.case03(bad part version)::OP_READ. " << "status: " << status.code << " with MESG: " << status.message);
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
        K2INFO("------- SC02.case 04 (READ of all data records) -------");
        return seastar::do_with(
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey1", .rangeKey = ""},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey2", .rangeKey = "range1"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey3", .rangeKey = ""},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey4", .rangeKey = ""},
            DataRec {.f1="SC02_f1", .f2="SC02_f2"},
            [this](auto& mtr, auto& k1, auto& k2, auto& k3, auto& k4, auto& v1) {
                return seastar::when_all(doRead(k1, mtr, collname, ErrorCaseOpt::NoInjection), doRead(k2, mtr, collname, ErrorCaseOpt::NoInjection), \
                        doRead(k3, mtr, collname, ErrorCaseOpt::NoInjection), doRead(k4, mtr, collname, ErrorCaseOpt::NoInjection))
                .then([&](auto&& response) mutable {
                    auto& [resp1, resp2, resp3, resp4] = response;
                    // move resp out of the incoming futures sice get0() returns an rvalue
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    auto [status3, val3] = resp3.get0();
                    auto [status4, val4] = resp4.get0();
                    K2INFO("SC02.case04::OP_READ_Key1. " << "status: " << status1.code << " with MESG: " << status1.message);
                    K2INFO("SC02.case04::OP_READ_Key2. " << "status: " << status2.code << " with MESG: " << status2.message);
                    K2INFO("SC02.case04::OP_READ_Key3. " << "status: " << status3.code << " with MESG: " << status3.message);
                    K2INFO("SC02.case04::OP_READ_Key4. " << "status: " << status4.code << " with MESG: " << status4.message);
                    K2INFO("Value of Key1: " << val1 << ". Value of key2: " << val2);
                    K2INFO("Value of Key3: " << val3 << ". Value of key4: " << val4);
                    K2EXPECT(status1, dto::K23SIStatus::OK);
                    K2EXPECT(status2, dto::K23SIStatus::OK);
                    K2EXPECT(status3, dto::K23SIStatus::KeyNotFound);
                    K2EXPECT(status4, dto::K23SIStatus::AbortConflict);
                    K2EXPECT(val1, v1);
                    K2EXPECT(val2, v1);
                    K2EXPECT(val3.f1, "");
                    K2EXPECT(val3.f2, "");
                    K2EXPECT(val4.f1, "");
                    K2EXPECT(val4.f2, "");
                });
            }
        );
    }) // end sc-02 case-04
    .then([] {
        // SC02 case05&06: attempt to write in the past and write at same time
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2INFO("------- SC02.case 05&06 (for an existing key that has never been read, attempt to write in the past and write at same time) -------");

        return seastar::do_with(
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = {1000000, 123, 1000}, .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey5", .rangeKey = "range6"},
            DataRec {.f1="SC02_f05", .f2="SC02_f06"},
            DataRec {.f1="SC02_f07", .f2="SC02_f08"},
            [this](auto& staleMTR, auto& incumbentMTR, auto& k5, auto& v2, auto& v3) {
                 return doWrite(k5, v2, incumbentMTR, k5, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(status, dto::K23SIStatus::Created);
                })
               .then([this, &k5, &incumbentMTR] {
                    return doEnd(k5, incumbentMTR, collname, true, {k5}, Duration(0us), ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::OK);
                    });
                })
                .then([this, &k5, &v3, &staleMTR] {
                    return doWrite(k5, v3, staleMTR, k5, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::AbortRequestTooOld);
                        K2INFO("SC02.case05(write in the past)::OP_Write. " << "status: " << status.code << " with MESG: " << status.message);
                    });
                })
                .then([this, &k5, &v3, &incumbentMTR] {
                    dto::K23SI_MTR challengerMTR{
                        .txnid = txnids++,
                        .timestamp = incumbentMTR.timestamp,
                        .priority = dto::TxnPriority::Medium
                    };
                    return doWrite(k5, v3, challengerMTR, k5, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::AbortRequestTooOld);
                        K2INFO("SC02.case06(write at the same time)::OP_Write. " << "status: " << status.code << " with MESG: " << status.message);
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
        K2INFO("------- SC02.case 07 (for an existing key that has never been read, attempt to write in the future) -------");

        return seastar::do_with(
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey5", .rangeKey = "range6"},
            DataRec {.f1="SC02_f07", .f2="SC02_f08"},
            [this](auto& futureMTR, auto& k5, auto& v3) {
                 return doWrite(k5, v3, futureMTR, k5, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(status, dto::K23SIStatus::Created);
                    K2INFO("SC02.case07(write in the future)::OP_Write. " << "status: " << status.code << " with MESG: " << status.message);
                })
                .then([this, &k5, &futureMTR, &v3] {
                    return doRead(k5, futureMTR, collname, ErrorCaseOpt::NoInjection)
                    .then([&](auto&& response) {
                        auto& [status, resp] = response;
                        K2INFO("SC02.case07()::OP_READ. " << "status: " << status.code << " with MESG: " << status.message);
                        K2INFO("Value of Key5: " << resp);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                        K2EXPECT(resp, v3);
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
        K2INFO("------- SC02.case08 & 09 & 10 (READ existing key at time before/equal/after the key) -------");

        return seastar::do_with(
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey8", .rangeKey = "range8"},
            DataRec {.f1="SC02_f08", .f2="SC02_f09"},
            [this](auto& eqMTR, auto& k6, auto& v1) {
                return doWrite(k6, v1, eqMTR, k6, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                     auto& [status, resp] = response;
                     K2EXPECT(status, dto::K23SIStatus::Created);
                })
                .then([this, &k6, &eqMTR] {
                    return doEnd(k6, eqMTR, collname, true, {k6}, Duration(0us), ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::OK);
                    });
                })
                .then([this, &k6, &eqMTR] {
                    dto::K23SI_MTR bfMTR{
                        .txnid = txnids++,
                        .timestamp = {(eqMTR.timestamp.tEndTSECount() - 500000000), 123, 1000}, // 500ms earlier
                        .priority = dto::TxnPriority::Medium};
                    return doRead(k6, bfMTR, collname, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2INFO("SC02.case08(before)::OP_READ. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::KeyNotFound);
                    });
                }) // end sc-02 case-08
                .then([this, &k6, &eqMTR, &v1] {
                    return doRead(k6, eqMTR, collname, ErrorCaseOpt::NoInjection)
                    .then([&v1](auto&& response) {
                        auto& [status, resp] = response;
                        K2INFO("SC02.case09(equal)::OP_READ. " << "status: " << status.code << " with MESG: " << status.message);
                        K2INFO("Value of Key6: " << resp);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                        K2EXPECT(resp, v1);
                    });
                }) // end sc-02 case-09
                .then([this, &k6, &eqMTR, &v1] {
                    dto::K23SI_MTR afMTR{
                        .txnid = txnids++,
                        .timestamp = {(eqMTR.timestamp.tEndTSECount() + 500000000), 123, 1000}, // 500ms later
                        .priority = dto::TxnPriority::Medium};
                    return doRead(k6, afMTR, collname, ErrorCaseOpt::NoInjection)
                    .then([&v1](auto&& response) {
                        auto& [status, resp] = response;
                        K2INFO("SC02.case10(after)::OP_READ. " << "status: " << status.code << " with MESG: " << status.message);
                        K2INFO("Value of Key6: " << resp);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                        K2EXPECT(resp, v1);
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
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        K2INFO("------- SC02.case11 & 12 & 13 (Async END test (end-but-not-finalized)) -------");

        return seastar::do_with(
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = ts, .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = ts, .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = ts, .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey11", .rangeKey = "range11"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey12", .rangeKey = "range12"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC02_pkey13", .rangeKey = "range13"},
            DataRec {.f1="SC02_f_end", .f2="SC02_f_end"}, // new value
            [this](auto& mtr11, auto& mtr12, auto& mtr13, auto& k11, auto& k12, auto& k13, auto& v1) {
                return seastar::when_all(doWrite(k11, v1, mtr11, k11, collname, false, true, ErrorCaseOpt::NoInjection), doWrite(k12, v1, mtr12, k12, collname, false, true, ErrorCaseOpt::NoInjection), \
                            doWrite(k13, v1, mtr13, k13, collname, false, true, ErrorCaseOpt::NoInjection))
                .then([&](auto&& response) mutable {
                    auto& [resp1, resp2, resp3] = response;
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    auto [status3, val3] = resp3.get0();
                    K2EXPECT(status1, dto::K23SIStatus::Created);
                    K2EXPECT(status2, dto::K23SIStatus::Created);
                    K2EXPECT(status3, dto::K23SIStatus::Created);
                })
                .then([this, &mtr11, &mtr12,&mtr13, &k11, &k12, &k13] {
                    return seastar::when_all(doEnd(k11, mtr11, collname, true, {k11}, Duration(111ms),ErrorCaseOpt::NoInjection),\
                            doEnd(k12, mtr12, collname, false, {k12}, Duration(112ms),ErrorCaseOpt::NoInjection), doEnd(k13, mtr13, collname, false, {k13}, Duration(113ms),ErrorCaseOpt::NoInjection))
                    .then([&](auto&& response) mutable {
                        auto& [resp1, resp2, resp3] = response;
                        // move resp out of the incoming futures sice get0() returns an rvalue
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        auto [status3, val3] = resp3.get0();
                        K2EXPECT(status1, dto::K23SIStatus::OK);
                        K2EXPECT(status2, dto::K23SIStatus::OK);
                        K2EXPECT(status3, dto::K23SIStatus::OK);
                    });
                })
                .then([this, &k11, &k12, &k13, &mtr11, &mtr12, &mtr13, &v1] {
                    return seastar::when_all(doFinalize(k11, k11, mtr11, collname, false, ErrorCaseOpt::NoInjection), doFinalize(k12, k12, mtr12, collname, true, ErrorCaseOpt::NoInjection), \
                            doRead(k13, mtr13, collname, ErrorCaseOpt::NoInjection))
                    .then([&](auto&& response) mutable {
                        auto& [resp1, resp2, resp3] = response;
                        // move resp out of the incoming futures sice get0() returns an rvalue
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        auto [status3, val3] = resp3.get0();
                        K2INFO("SC02.case11::OP_Finalize(abort)_a_async_END(commit)_key. " << "status: " << status1.code << " with MESG: " << status1.message);
                        K2INFO("SC02.case12::OP_Finalize(commit)_a_async_END(abort)_key. " << "status: " << status2.code << " with MESG: " << status2.message);
                        K2INFO("SC02.case13::OP_READ_a_async_END(abort)_key. " << "status: " << status3.code << " with MESG: " << status3.message);
                        K2INFO("Value of Key13: " << val3);
                        K2EXPECT(status1, dto::K23SIStatus::OK);
                        K2EXPECT(status2, dto::K23SIStatus::OK);
                        K2EXPECT(status3, dto::K23SIStatus::OK);
                        K2EXPECT(val3, v1);
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
	K2INFO("+++++++ TestScenario 04: read your writes and read-committed isolation +++++++");
	K2INFO("--->Test SETUP: initialization record, pKey('SC04_pkey1'), rKey('rKey1'), v0{f1=SC04_f1_zero, f2=SC04_f2_zero} -> committed");

    return seastar::make_ready_future()
    .then([] {
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC04_pkey1", .rangeKey = "rKey1"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC04_pkey1", .rangeKey = "rKey1"},
            DataRec {.f1="SC04_f1_zero", .f2="SC04_f2_zero"},
            [this](auto& mtr, auto& k1, auto& trh, auto& v0) {
                return doWrite(k1, v0, mtr, trh, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(status, dto::K23SIStatus::Created);
                })
                .then([this, &trh, &mtr, &k1] {
                    return doEnd(trh, mtr, collname, true, {k1}, Duration(0s), ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::OK);
                    });
                });
            }
        );
    })
    .then([]{
        K2INFO("Scenario 04 setup done.");
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
                .txnid = txnids++,
                .timestamp = {(ts.tEndTSECount() - 1000000), 123, 1000},
                .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR { // txn(A)
                .txnid = txnids++,
                .timestamp = {(ts.tEndTSECount()), 123, 1000},
                .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR { // txn(C) is newer than txn(A), also newer than txn(B)
                .txnid = txnids++,
                .timestamp = {(ts.tEndTSECount() + 1000000), 123, 1000},
                .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR { // txn(D) is newest of all transactions
                .txnid = txnids++,
                .timestamp = {(ts.tEndTSECount() + 5000000), 123, 1000},
                .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC04_pkey1", .rangeKey = "rKey1"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC04_pkey2", .rangeKey = "rKey2"},
            DataRec {.f1="SC04_f1_zero", .f2="SC04_f2_zero"},
            DataRec {.f1="SC04_f1_one", .f2="SC04_f2_one"},
            [this](auto& mtrB, auto& mtrA, auto& mtrC, auto& mtrD, auto& k1, auto& k2, auto& v0, auto& v1) {
                K2INFO("------- SC04.case1 (txns READ record that has been committed before it starts) -------");
                return seastar::when_all(doRead(k1, mtrA, collname, ErrorCaseOpt::NoInjection), doRead(k1, mtrB, collname, ErrorCaseOpt::NoInjection))
                .then([&v0](auto&& response) mutable {
                    auto& [resp1, resp2] = response;
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    K2INFO("SC04.case01.Txn(A)::OP_read_init_record. " << "status: " << status1.code << " with MESG: " << status1.message);
                    K2INFO("SC04.case01.Txn(B)::OP_read_init_record. " << "status: " << status2.code << " with MESG: " << status2.message);
                    K2INFO("Value in Txn(A): " << val1);
                    K2INFO("Value in Txn(B): " << val2);
                    K2EXPECT(status1, dto::K23SIStatus::OK);
                    K2EXPECT(status2, dto::K23SIStatus::OK);
                    K2EXPECT(val1, v0);
                    K2EXPECT(val2, v0);
                })
                .then([this, &k1, &v1, &mtrA] {
                    K2INFO("------- SC04.case 02 ( Txn READ its own pending writes ) -------");
                    return doWrite(k1, v1, mtrA, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, resp] = response;
                        K2INFO("SC04.prepare.Txn(A)::OP_write_intent_k1_v1. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::Created);
                    })
                    .then([&] {
                        return doRead(k1, mtrA, collname, ErrorCaseOpt::NoInjection)
                        .then([&](auto&& response)  {
                            auto& [status, val] = response;
                            K2INFO("SC04.case02.Txn(A)::OP_read_own_WI_k1. " << "status: " << status.code << " with MESG: " << status.message);
                            K2INFO("Value in Txn(A): " << val);
                            K2EXPECT(status, dto::K23SIStatus::OK);
                            K2EXPECT(val, v1);
                        });
                    });
                })
                .then([&] {
                    K2INFO("------- SC04.case 03 ( Pending writes are not shown to other transactions whose ts is older ) -------");
                    return doRead(k1, mtrB, collname, ErrorCaseOpt::NoInjection)
                    .then([&](auto&& response)  {
                        auto& [status, val] = response;
                        K2INFO("SC04.case03.Txn(B)::OP_read_older_WI_k1. " << "status: " << status.code << " with MESG: " << status.message);
                        K2INFO("Value in Txn(B): " << val);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                        K2EXPECT(val, v0);
                    });
                })
                .then([&] {
                    K2INFO("------- SC04.case 04 ( Pending writes are not shown to other transactions whose ts is newer ) -------");
                    return doWrite(k2, v1, mtrB, k2, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([&](auto&& response) {
                        auto [status, val] = response;
                        K2INFO("SC04.prepare.Txn(B)::OP_write_WI_k2_v1. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::Created);
                    })
                    .then([&] {
                        return doRead(k2, mtrC, collname, ErrorCaseOpt::NoInjection)
                        .then([&](auto&& response)  {
                            auto& [status, val] = response;
                            K2INFO("SC04.case04.Txn(C)::OP_read_older_WI_k2. " << "status: " << status.code << " with MESG: " << status.message);
                            K2INFO("Value in Txn(C): " << val);
                            K2EXPECT(status, dto::K23SIStatus::KeyNotFound);
                            K2EXPECT(val.f1, "");
                            K2EXPECT(val.f2, "");
                        });
                    })
                    .then([&] {
                        return doInspectRecords(k2, collname)
                        .then([&](auto&& response)  {
                            auto& [status, val] = response;
                            K2INFO("Due to READ() -> Push() lost, txn(B) is force_aborted and its WI(k2,v1) is cleared");
                            K2INFO("SC04.case04.OP_inspect_record_WI_k2. " << "status: " << status.code << " with MESG: " << status.message);
                            for(auto& e : val.records) {
                                K2INFO("Versions of k2: { txnid(" << e.txnId.mtr.txnid << "), status(" << e.status << ") }");
                            }
                            K2EXPECT(status, dto::K23SIStatus::OK);
                            K2EXPECT(val.records.empty(), true);
                        });
                    })
                    .then([&] {
                        return doInspectTxn(k2, mtrB, collname)
                        .then([&](auto&& response)  {
                            auto& [status, val] = response;
                            K2INFO("SC04.case04.OP_inspect_Txn(B). " << "status: " << status.code << " with MESG: " << status.message);
                            K2INFO("State of txn(B): " << val.state);
                            K2EXPECT(status, dto::K23SIStatus::OK);
                            K2EXPECT(val.state, dto::TxnRecordState::ForceAborted);
                        });
                    });
                })
                .then([&] {
                    K2INFO("------- SC04.case 05 ( Txn with older timestamp READ other txn's committed records ) -------");
                    return doEnd(k1, mtrA, collname, true, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2INFO("SC04.prepare.Txn(A)::OP_end_commit. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                    })
                    .then([&] {
                        return doRead(k1, mtrB, collname, ErrorCaseOpt::NoInjection)
                        .then([&](auto&& response)  {
                            auto& [status, val] = response;
                            K2INFO("SC04.case05.Txn(B)::OP_read_newer_commit_record. " << "status: " << status.code << " with MESG: " << status.message);
                            K2INFO("Value in Txn(C): " << val);
                            K2EXPECT(status, dto::K23SIStatus::OK);
                            K2EXPECT(val, v0);
                        });
                    });
                })
                .then([&] {
                    K2INFO("------- SC04.case 06 ( Txn with newer timestamp READ other txn's committed writes ) -------");
                    return seastar::when_all(doRead(k1, mtrC, collname, ErrorCaseOpt::NoInjection), doRead(k1, mtrD, collname, ErrorCaseOpt::NoInjection))
                    .then([&](auto&& response) {
                        auto& [resp1, resp2] = response;
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        K2INFO("SC04.case06.Txn(C)::OP_read_older_commit_record. " << "status: " << status1.code << " with MESG: " << status1.message);
                        K2INFO("SC04.case06.Txn(D)::OP_read_older_commit_record. " << "status: " << status2.code << " with MESG: " << status2.message);
                        K2INFO("Value in Txn(C): " << val1);
                        K2INFO("Value in Txn(D): " << val2);
                        K2EXPECT(status1, dto::K23SIStatus::OK);
                        K2EXPECT(status2, dto::K23SIStatus::OK);
                        K2EXPECT(val1, v1);
                        K2EXPECT(val2, v1);
                    });
                });
            }
        ); // end do-with
    }); // end SC-04
}

seastar::future<> testScenario05() {
    K2INFO("+++++++ TestScenario 05: concurrent transactions +++++++");
    K2INFO("--->Test SETUP: initialization record, pKey('SC05_pkey1'), rKey('rKey1'), v0{f1=SC05_f1_zero, f2=SC05_f2_zero} -> committed");

    return seastar::make_ready_future()
    .then([] {
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC05_pkey1", .rangeKey = "rKey1"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC05_pkey1", .rangeKey = "rKey1"},
            DataRec {.f1="SC05_f1_zero", .f2="SC04_f2_zero"},
            [this](auto& mtr, auto& k1, auto& trh, auto& v0) {
                return doWrite(k1, v0, mtr, trh, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(status, dto::K23SIStatus::Created);
                })
                .then([this, &trh, &mtr, &k1] {
                    return doEnd(trh, mtr, collname, true, {k1}, Duration(0s), ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        auto& [status, resp] = response;
                        K2EXPECT(status, dto::K23SIStatus::OK);
                    });
                });
            }
        ); // end do-with
    })
    .then([]{
        K2INFO("Scenario 05 setup done.");
        return getTimeNow();
    })
    .then([&](dto::Timestamp&& ts) {
        return seastar::do_with(
            // clarify: For the sake of code brevity,  priority, Timestamp, and tsoId in the following
            // MTR parameters are directly related to who wins Push() in the test cases. So that the
            // next test case (write-write conflict) can directly compares with the winner mtr.
            // So the following statement may cause some confusion, which will be cleard in combination with the test cases.
            dto::K23SI_MTR { // fakeCfxMtr: 10us earlier
                .txnid = txnids++,
                .timestamp = {(ts.tEndTSECount() - 10000), 123, 1000},
                .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { // prioHighMtr: txn with same ts, higher priority, same tso id
                .txnid = txnids++,
                .timestamp = {(ts.tEndTSECount()), 123, 1000},
                .priority = dto::TxnPriority::High },
            dto::K23SI_MTR { // prioLowMtr: txn with same ts, higher priority, same tso id
                .txnid = txnids++,
                .timestamp = {(ts.tEndTSECount()), 123, 1000},
                .priority = dto::TxnPriority::Low} ,
            dto::K23SI_MTR { // oldMtr: txn with earlier ts, same priority, same tso id
                .txnid = txnids++,
                .timestamp = {(ts.tEndTSECount() - 10000), 123, 1000},
                .priority = dto::TxnPriority::High },
            dto::K23SI_MTR { // newMtr: txn with later ts, same priority, same tso id
                .txnid = txnids++,
                .timestamp = {(ts.tEndTSECount() + 10000), 123, 1000},
                .priority = dto::TxnPriority::High },
            dto::K23SI_MTR { // tsoSmMtr: txn with same ts, same priority, smaller tso id
                .txnid = txnids++,
                .timestamp = {(ts.tEndTSECount() + 10000), 100, 1000},
                .priority = dto::TxnPriority::High },
            dto::K23SI_MTR { // tsoBgMtr: txn with same ts, same priority, bigger tso id
                .txnid = txnids++,
                .timestamp = {(ts.tEndTSECount() + 10000), 200, 1000},
                .priority = dto::TxnPriority::High },
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = ts, .priority = dto::TxnPriority::Medium}, // incumbent mtr
            dto::Key {.schemaName = "schema", .partitionKey = "SC05_pkey1", .rangeKey = "rKey1"},
            DataRec {.f1="SC05_f1_one", .f2="SC04_f2_one"},
            DataRec {.f1="SC05_f1_zero", .f2="SC04_f2_zero"},
            [&](auto& fakeCfxMtr, auto& prioHighMtr, auto& prioLowMtr, auto& oldMtr, auto& newMtr, auto& tsoSmMtr, auto& tsoBgMtr, \
                auto& incMtr, auto& k1, auto& v1, auto& v0) {
                return doWrite(k1, v1, incMtr, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2EXPECT(status, dto::K23SIStatus::Created);
                })
                .then([&] {
                    K2INFO("------- SC05.case1 (fake-read-write conflict) -------");
                    return doRead(k1, fakeCfxMtr, collname, ErrorCaseOpt::NoInjection)
                    .then([&](auto&& response) {
                        // op_READ in txn(fakeCfxMtr) has no effect on the k1-WI of txn(incMtr)
                        // current MTR of k1-WI is incMtr.
                        auto& [status, val] = response;
                        K2INFO("SC05.case01::OP_read_earlier. " << "status: " << status.code << " with MESG: " << status.message);
                        K2INFO("Value of k1 in fakeCfxMtr: " << val);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                        K2EXPECT(val, v0);
                    });
                })
                .then([&] {
                    K2INFO("------- SC05.case2 (Txn with higher priority encounters a WI) -------");
                    return doWrite(k1, v1, prioHighMtr, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        // op_WRITE in txn(prioHighMtr) wins push(), so the old WI is cleared and k1-WI of txn(prioHighMtr) is created
                        // current MTR of k1-WI is prioHighMtr.
                        auto& [status, resp] = response;
                        K2INFO("SC05.case02::OP_push_prioHighMtr. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::Created);
                    })
                    .then([&] {
                        return doInspectTxn(k1, incMtr, collname)
                        .then([&](auto&& response) {
                            auto& [status, resp] = response;
                            K2INFO("SC05.case2::OP_inspectTxn_incMtr. " << "status: " << status.code << " with MESG: " << status.message \
                                    << ". state of incMtr(push loser): " << resp.state);
                            K2EXPECT(status, dto::K23SIStatus::OK);
                            K2EXPECT(resp.txnId.mtr.txnid, incMtr.txnid);
                            K2EXPECT(resp.state, dto::TxnRecordState::ForceAborted);
                        });
                    });
                })
                .then([&] {
                    K2INFO("------- SC05.case3 (Txn with lower priority encounters a WI) -------");
                    return doWrite(k1, v1, prioLowMtr, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        // op_WRITE in txn(prioHighMtr) lose push(), so the k1-WI still belongs to txn(prioHighMtr)
                        // current MTR of k1-WI is prioHighMtr.
                        auto& [status, resp] = response;
                        K2INFO("SC05.case03::OP_push_prioLowMtr. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::AbortConflict);
                    })
                    .then([&] {
                        return doInspectTxn(k1, prioLowMtr, collname)
                        .then([&](auto&& response) {
                            auto& [status, resp] = response;
                            K2INFO("SC05.case3::OP_inspectTxn_prioLowMtr. " << "status: " << status.code << " with MESG: " << status.message \
                                    << ". state of prioLowMtr(push loser): " << resp.state);
                            K2EXPECT(status, dto::K23SIStatus::OK);
                            K2EXPECT(resp.txnId.mtr.txnid, prioLowMtr.txnid);
                            K2EXPECT(resp.state, dto::TxnRecordState::InProgress);
                        });
                    });
                })
                .then([&] {
                    K2INFO("------- SC05.case4 (Earlier WRITE Txn encounters a WI with the same priority) -------");
                    return doWrite(k1, v1, oldMtr, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        // op_WRITE in txn(oldMtr) lose push(), so the k1-WI still belongs to txn(prioHighMtr)
                        // current MTR of k1-WI is prioHighMtr.
                        auto& [status, resp] = response;
                        K2INFO("SC05.case04::OP_push_oldMtr. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::AbortConflict);
                    })
                    .then([&] {
                        return doInspectTxn(k1, oldMtr, collname)
                        .then([&](auto&& response) {
                            auto& [status, resp] = response;
                            K2INFO("SC05.case4::OP_inspectTxn_oldMtr. " << "status: " << status.code << " with MESG: " << status.message \
                                    << ". state of oldMtr(push loser): " << resp.state);
                            K2EXPECT(status, dto::K23SIStatus::OK);
                            K2EXPECT(resp.txnId.mtr.txnid, oldMtr.txnid);
                            K2EXPECT(resp.state, dto::TxnRecordState::InProgress);
                        });
                    });
                })
                .then([&] {
                    K2INFO("------- SC05.case5 (newer WRITE Txn encounters a WI with the same priority) -------");
                    return doWrite(k1, v1, newMtr, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        // op_WRITE in txn(newMtr) wins push(), so the old WI is cleared and k1-WI of txn(newMtr) is created
                        // current MTR of k1-WI is newMtr.
                        auto& [status, resp] = response;
                        K2INFO("SC05.case05::OP_push_newMtr. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::Created);
                    })
                    .then([&] {
                        return doInspectTxn(k1, prioHighMtr, collname)
                        .then([&](auto&& response) {
                            auto& [status, resp] = response;
                            K2INFO("SC05.case5::OP_inspectTxn_prioHighMtr. " << "status: " << status.code << " with MESG: " << status.message \
                                    << ". state of prioHighMtr(push loser): " << resp.state);
                            K2EXPECT(status, dto::K23SIStatus::OK);
                            K2EXPECT(resp.txnId.mtr.txnid, prioHighMtr.txnid);
                            K2EXPECT(resp.state, dto::TxnRecordState::ForceAborted);
                        });
                    });
                })
                .then([&] {
                    K2INFO("------- SC05.case6 (WRITE Txn with smaller tso ID encounters a WI) -------");
                    return doWrite(k1, v1, tsoSmMtr, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        // op_WRITE in txn(tsoSmMtr) lose push(), so the k1-WI still belongs to txn(newMtr)
                        // current MTR of k1-WI is newMtr.
                        auto& [status, resp] = response;
                        K2INFO("SC05.case06::OP_push_smaller_tsoid_Mtr. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::AbortConflict);
                    })
                    .then([&] {
                        return doInspectTxn(k1, tsoSmMtr, collname)
                        .then([&](auto&& response) {
                            auto& [status, resp] = response;
                            K2INFO("SC05.case6::OP_inspectTxn_tsoSmMtr. " << "status: " << status.code << " with MESG: " << status.message \
                                    << ". state of tsoSmMtr(push loser): " << resp.state);
                            K2EXPECT(status, dto::K23SIStatus::OK);
                            K2EXPECT(resp.txnId.mtr.txnid, tsoSmMtr.txnid);
                            K2EXPECT(resp.state, dto::TxnRecordState::InProgress);
                        });
                    });
                })
                .then([&] {
                    K2INFO("------- SC05.case7 (WRITE Txn with bigger tso ID encounters a WI) -------");
                    return doWrite(k1, v1, tsoBgMtr, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response) {
                        // op_WRITE in txn(tsoBgMtr) wins push(), so the old WI is cleared and k1-WI of txn(tsoBgMtr) is created
                        // current MTR of k1-WI is tsoBgMtr.
                        auto& [status, resp] = response;
                        K2INFO("SC05.case07::OP_push_bigger_tsoid_Mtr. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::Created);
                    })
                    .then([&] {
                        return doInspectTxn(k1, newMtr, collname)
                        .then([&](auto&& response) {
                            auto& [status, resp] = response;
                            K2INFO("SC05.case7::OP_inspectTxn_newMtr. " << "status: " << status.code << " with MESG: " << status.message \
                                    << ". state of newMtr(push loser): " << resp.state);
                            K2EXPECT(status, dto::K23SIStatus::OK);
                            K2EXPECT(resp.txnId.mtr.txnid, newMtr.txnid);
                            K2EXPECT(resp.state, dto::TxnRecordState::ForceAborted);
                        });
                    })
                    .then([&] {
                        return doInspectTxn(k1, tsoBgMtr, collname)
                        .then([&](auto&& response) {
                            auto& [status, resp] = response;
                            K2INFO("SC05.case7::OP_inspectTxn_tsoBgMtr. " << "status: " << status.code << " with MESG: " << status.message \
                                    << ". state of tsoBgMtr(push winner): " << resp.state);
                            K2EXPECT(status, dto::K23SIStatus::OK);
                            K2EXPECT(resp.txnId.mtr.txnid, tsoBgMtr.txnid);
                            K2EXPECT(resp.state, dto::TxnRecordState::InProgress);
                        });
                    });
                });
            }
        );
    }); // end sc-05
}

seastar::future<> testScenario06() {
    K2INFO("+++++++ TestScenario 06: finalization +++++++");

    return seastar::make_ready_future()
    .then([] {
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = ts, .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = ts, .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkek1", .rangeKey = "rKey1"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkey2", .rangeKey = "rKey2"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkey3", .rangeKey = "rKey3"},
            DataRec {.f1="SC05_f1_zero", .f2="SC04_f2_zero"},
            [this](auto& mtr, auto& otherMtr, auto& k1, auto& k2, auto& k3, auto& v0) {
            return doWrite(k1, v0, mtr, k1, collname, false, true, ErrorCaseOpt::NoInjection)
            .then([](auto&& response) {
                auto& [status, resp] = response;
                K2INFO("SC06.setup::OP_WI_k1. " << "status: " << status.code << " with MESG: " << status.message);
                K2EXPECT(status, dto::K23SIStatus::Created);
            })
            .then([&] {
                K2INFO("------- SC06.case1 (Finalize a non-exist record in this transaction) -------");
                return doFinalize(k1, k2, mtr, collname, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2INFO("SC06.case1::OP_finalize_nonExist_rec. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::OperationNotAllowed);
                });
            })
            .then([&] {
                return doWrite(k2, v0, mtr, k1, collname, false, false, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) mutable {
                    auto& [status, val] = response;
                    K2INFO("SC06.setup::OP_WI_k2. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::Created);
                });
            })
            .then([&] {
                K2INFO("------- SC06.case2 ( Finalize_Commit partial record within this transaction ) -------");
                return doFinalize(k1, k2, mtr, collname, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response) {
                    auto& [status, resp] = response;
                    K2INFO("SC06.case2::OP_finalize_partial_rec. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::OK);
                });
            })
            .then([&] {
                K2INFO("------- SC06.case3 ( Finalize a record whose status is commit ) -------");
                return doFinalize(k3, k3, mtr, collname, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC06.case3::OP_finalize_Commit_key. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::OperationNotAllowed);
                });
            })
            .then([&] {
                K2INFO("------- SC06.case4 ( Other transactions read finalize_commit record ) -------");
                return doRead(k2, otherMtr, collname, ErrorCaseOpt::NoInjection)
                .then([&](auto&& response) {
                    auto& [status, val] = response;
                    K2INFO("SC06.case4::OP_read_by_other_Txn. " << "status: " << status.code << " with MESG: " << status.message);
                    K2INFO("Value of k2: " << val);
                    K2EXPECT(val, v0);
                    K2EXPECT(status, dto::K23SIStatus::OK);
                });
            })
            .then([&] {
                K2INFO("------- SC06.case5 ( After partial Finalize, txn continues and then Commit all records ) -------");
                return doWrite(k3, v0, mtr, k1, collname, false, false, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC06.case5::OP_WI_k3. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::Created);
                })
                .then([&] {
                    return doEnd(k1, mtr, collname, true, {k1, k2, k3}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2INFO("SC06.case5::OP_End_Commit_all_keys. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                    });
                });
            });
        }); // end do-with
    }) // end case 01-05
    .then([&] {
        K2INFO("------- SC06.case6 ( After partial Finalization_commit, txn continues and then End_Abort all records ) -------");
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
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
                K2INFO("SC06.case6::OP_WI_k1. " << "status: " << status1.code << " with MESG: " << status1.message);
                K2INFO("SC06.case6::OP_WI_k2. " << "status: " << status2.code << " with MESG: " << status2.message);
                K2EXPECT(status1, dto::K23SIStatus::Created);
                K2EXPECT(status2, dto::K23SIStatus::Created);
            })
            .then([&] {
                return doFinalize(k1, k2, mtr, collname, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC06.case6::OP_finalize_commit_k2. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::OK);
                });
            })
            .then([&] {
                return doWrite(k3, v0, mtr, k1, collname, false, false, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC06.case6::OP_WI_k3. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::Created);
                });
            })
            .then([&] {
                return doEnd(k1, mtr, collname, false, {k1, k2, k3}, Duration{0s}, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC06.case6::OP_End_abort_all_keys. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, Statuses::S500_Internal_Server_Error);
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
                    K2INFO("SC06.case6::OP_READ_end_abort_k1. " << "status: " << status1.code << " with MESG: " << status1.message);
                    K2INFO("SC06.case6::OP_READ_finalize_commit_k2. " << "status: " << status2.code << " with MESG: " << status2.message);
                    K2INFO("SC06.case6::OP_READ_end_abort_k3. " << "status: " << status3.code << " with MESG: " << status3.message);
                    K2INFO("Value of k1: " << val1);
                    K2INFO("Value of k2: " << val2);
                    K2INFO("Value of k3: " << val3);
                    K2EXPECT(status1, dto::K23SIStatus::KeyNotFound);
                    K2EXPECT(status2, dto::K23SIStatus::OK);
                    K2EXPECT(status3, dto::K23SIStatus::KeyNotFound);
                    K2EXPECT(val1.f1, "");
                    K2EXPECT(val1.f2, "");
                    K2EXPECT(val2, v0);
                    K2EXPECT(val3.f1, "");
                    K2EXPECT(val3.f2, "");
                });
            });
        }); // end do-with
    }) // end case 06
    .then([&] {
        K2INFO("------- SC06.case7 ( Finalize_Abort partial record within this transaction ) -------");
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkek4", .rangeKey = "rKey4"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkey5", .rangeKey = "rKey5"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC06_pkey6", .rangeKey = "rKey6"},
            DataRec {.f1="SC05_f1_zero", .f2="SC04_f2_zero"},
            [this](auto& mtr, auto& otherMtr, auto& k4, auto& k5, auto& k6, auto& v0) {
            return seastar::when_all(doWrite(k4, v0, mtr, k4, collname, false, true, ErrorCaseOpt::NoInjection), \
                    doWrite(k5, v0, mtr, k4, collname, false, false, ErrorCaseOpt::NoInjection))
            .then([](auto&& response) {
                auto& [resp1, resp2] = response;
                auto [status1, val1] = resp1.get0();
                auto [status2, val2] = resp2.get0();
                K2INFO("SC06.case7::OP_WI_k4(trh). " << "status: " << status1.code << " with MESG: " << status1.message);
                K2INFO("SC06.case7::OP_WI_k5. " << "status: " << status2.code << " with MESG: " << status2.message);
                K2EXPECT(status1, dto::K23SIStatus::Created);
                K2EXPECT(status2, dto::K23SIStatus::Created);
            })
            .then([&] {
                return doFinalize(k4, k4, mtr, collname, false, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC06.case7::OP_finalize_Abort_k4. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::OK);
                });
            })
            .then([&] {
                K2INFO("------- SC06.case8 ( Record is read after it is Finalize_Abort  within the txn ) -------");
                return doRead(k4, mtr, collname, ErrorCaseOpt::NoInjection)
                .then([&](auto&& response) {
                    auto& [status, val] = response;
                    K2INFO("SC06.case8::OP_read_aborted_key. " << "status: " << status.code << " with MESG: " << status.message);
                    K2INFO("Value of k4: " << val);
                    K2EXPECT(val.f1, "");
                    K2EXPECT(val.f2, "");
                    K2EXPECT(status, dto::K23SIStatus::KeyNotFound);
                });
            })
            .then([&] {
                K2INFO("------- SC06.case9 ( Finalize a record who has already been finalized ) -------");
                return doFinalize(k4, k4, mtr, collname, false, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC06.case9::OP_finalize_finalized_k4. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::OK);
                });
            })
            .then([&] {
                K2INFO("------- SC06.case10 ( After Finalize_abort, txn continues and then Commit all records ) -------");
                return doWrite(k6, v0, mtr, k4, collname, false, false, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC06.case10::OP_WI_k6. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::Created);
                })
                .then([&] {
                    return doEnd(k4, mtr, collname, true, {k4, k5, k6}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2INFO("SC06.case10::OP_End_Commit_all_keys. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, Statuses::S500_Internal_Server_Error);
                    });
                })
                .then([&] {
                    return seastar::when_all(doRead(k4, otherMtr, collname, ErrorCaseOpt::NoInjection), doRead(k5, otherMtr, collname, ErrorCaseOpt::NoInjection), \
                            doRead(k6, otherMtr, collname, ErrorCaseOpt::NoInjection))
                    .then([&](auto&& response) mutable {
                        auto& [resp1, resp2, resp3] = response;
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        auto [status3, val3] = resp3.get0();
                        K2INFO("SC06.case10::OP_READ_finalize_abort_k4. " << "status: " << status1.code << " with MESG: " << status1.message);
                        K2INFO("SC06.case10::OP_READ_end_commit_k5. " << "status: " << status2.code << " with MESG: " << status2.message);
                        K2INFO("SC06.case10::OP_READ_end_commit_k6. " << "status: " << status3.code << " with MESG: " << status3.message);
                        K2INFO("Value of k4: " << val1);
                        K2INFO("Value of k5: " << val2);
                        K2INFO("Value of k6: " << val3);
                        K2EXPECT(status1, dto::K23SIStatus::KeyNotFound);
                        K2EXPECT(status2, dto::K23SIStatus::OK);
                        K2EXPECT(status2, dto::K23SIStatus::OK);
                        K2EXPECT(val1.f1, "");
                        K2EXPECT(val1.f2, "");
                        K2EXPECT(val2, v0);
                        K2EXPECT(val3, v0);
                    });
                });
            });
        }); // end do-with
    }) // end case 07-10
    .then([&] {
        K2INFO("------- SC06.case11 ( After partial Finalization_abort, txn continues and then End_Abort all records ) -------");
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = std::move(ts), .priority = dto::TxnPriority::Medium},
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
                K2INFO("SC06.case11::OP_WI_k4. " << "status: " << status1.code << " with MESG: " << status1.message);
                K2INFO("SC06.case11::OP_WI_k5. " << "status: " << status2.code << " with MESG: " << status2.message);
                K2EXPECT(status1, dto::K23SIStatus::Created);
                K2EXPECT(status2, dto::K23SIStatus::Created);
            })
            .then([&] {
                return doFinalize(k4, k4, mtr, collname, false, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC06.case11::OP_finalize_abort_k4. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::OK);
                });
            })
            .then([&] {
                return doEnd(k4, mtr, collname, false, {k4, k5}, Duration{0s}, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC06.case11::OP_End_abort_all_keys. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::OK);
                });
            });
        }); // end do-with
    }) // end case 11
    .then([&] {
        K2INFO("------- SC06.case12 ( The TRH and MTR parameters of Finalize do not match ) -------");
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = ts, .priority = dto::TxnPriority::Medium},
            dto::K23SI_MTR {.txnid = txnids++, .timestamp = ts, .priority = dto::TxnPriority::Medium},
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
                K2INFO("SC06.case12::OP_WI_k7. " << "status: " << status1.code << " with MESG: " << status1.message);
                K2INFO("SC06.case12::OP_WI_k8. " << "status: " << status2.code << " with MESG: " << status2.message);
                K2EXPECT(status1, dto::K23SIStatus::Created);
                K2EXPECT(status2, dto::K23SIStatus::Created);
            })
            .then([&] {
                return doFinalize(k8, k8, mtr, collname, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC06.case12::OP_finalize_mtr_trh_not_match. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::OperationNotAllowed);
                });
            })
            .then([&] {
                K2INFO("------- SC06.case13 ( During async end_abort interval, finalize_commit those keys ) -------");
                return doEnd(k7, mtr, collname, false, {k7, k8}, Duration{200ms}, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC06.case13::OP_async_end_abort_k7_k8. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::OK);
                });
            })
            .then([&] {
                return doFinalize(k7, k7, mtr, collname, true, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC06.case13::OP_finalize_commit_k7. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::OK);
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
                    K2INFO("SC06.case13::OP_read_k7. " << "status: " << status1.code << " with MESG: " << status1.message);
                    K2INFO("SC06.case13::OP_read_k8. " << "status: " << status2.code << " with MESG: " << status2.message);
                    K2INFO("Value of k7: " << val1);
                    K2INFO("Value of k8: " << val2);
                    K2EXPECT(status1, dto::K23SIStatus::OK);
                    K2EXPECT(status2, dto::K23SIStatus::KeyNotFound);
                    K2EXPECT(val1, v0);
                    K2EXPECT(val2.f1, "");
                    K2EXPECT(val2.f2, "");
                });
            });
        }); // end do-with
    }); // end case 12-13 end sc-06
}

seastar::future<> testScenario07() {
    K2INFO("+++++++ TestScenario 07: client-initiated txn abort +++++++");

    return seastar::make_ready_future()
    .then([] {
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            // multi MTRs are used for multiple transactions to execute. We use these MTRs in sequence
            dto::K23SI_MTR { .txnid = txnids++, .timestamp = ts, .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { .txnid = txnids++, .timestamp = {(ts.tEndTSECount() + 20000), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { .txnid = txnids++, .timestamp = {(ts.tEndTSECount() + 30000), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { .txnid = txnids++, .timestamp = {(ts.tEndTSECount() + 40000), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { .txnid = txnids++, .timestamp = {(ts.tEndTSECount() + 50000), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { .txnid = txnids++, .timestamp = {(ts.tEndTSECount() + 60000), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::Key {.schemaName = "schema", .partitionKey = "SC07_pkey1", .rangeKey = "rKey1"},
            dto::Key {.schemaName = "schema", .partitionKey = "SC07_pkey2", .rangeKey = "rKey2"},
            DataRec {.f1="SC05_f1_zero", .f2="SC04_f2_zero"},
            [this](auto& mtr, auto& mtr2, auto& mtr3, auto& mtr4, auto& mtr5, auto& mtr6, auto& k1, auto& k2, auto& v0) {
            K2INFO("------- SC07.case01 ( Commit-End a transaction before it has any operations ) -------");
            return doEnd(k1, mtr, collname, true, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
            .then([](auto&& response)  {
                auto& [status, val] = response;
                K2INFO("SC07.case01::OP_end_commit_onCreate. " << "status: " << status.code << " with MESG: " << status.message);
                K2EXPECT(status, dto::K23SIStatus::OperationNotAllowed);
            })
            .then([&] {
                K2INFO("------- SC07.case02 ( Abort-End a transaction before it has any operations ) -------");
                return doEnd(k1, mtr, collname, false, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC07.case02::OP_end_abort_onCreate. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::OK);
                });
            })
            .then([&] {
                K2INFO("------- SC07.case03 ( Commit-End a transaction missing some of the non-trh keys ) -------");
                return seastar::when_all(doWrite(k1, v0, mtr, k1, collname, false, true, ErrorCaseOpt::NoInjection), \
                        doWrite(k2, v0, mtr, k1, collname, false, false, ErrorCaseOpt::NoInjection))
                .then([&](auto&& response) {
                    auto& [resp1, resp2] = response;
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    K2INFO("SC07.prepare::OP_WI_k1. " << "status: " << status1.code << " with MESG: " << status1.message);
                    K2INFO("SC07.prepare::OP_WI_k2. " << "status: " << status2.code << " with MESG: " << status2.message);
                    K2EXPECT(status1, dto::K23SIStatus::Created);
                    K2EXPECT(status2, dto::K23SIStatus::Created);
                })
                .then([&] {
                    return doEnd(k1, mtr, collname, true, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2INFO("SC07.case03::OP_end_commit_miss_non_trh_keys. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                    });
                })
                .then([&] {
                    return doInspectTxn(k1, mtr, collname)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2INFO("SC07.case03::OP_inspect_txn_miss_non_trh_keys. " << "status: " << status.code << " with MESG: " << status.message);
                        K2INFO("State of txn: " << val.state);
                        K2EXPECT(status, dto::K23SIStatus::KeyNotFound);
                    });
                })
                .then([&] {
                    return seastar::when_all(doInspectRecords(k1, collname), doInspectRecords(k2, collname))
                    .then([&](auto&& response)  {
                        auto& [resp1, resp2] = response;
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        K2INFO("SC07.case03.OP_inspect_record_k1. " << "status: " << status1.code << " with MESG: " << status1.message);
                        K2INFO("SC07.case03.OP_inspect_record_k2. " << "status: " << status2.code << " with MESG: " << status2.message);
                        for(auto& e : val1.records) {
                            K2INFO("Versions of k1: { key" << e.key << ", txnid(" << e.txnId.mtr.txnid << "), status(" << e.status << ") }");
                        }
                        for(auto& e : val2.records) {
                            K2INFO("Versions of k2: { key" << e.key << ", txnid(" << e.txnId.mtr.txnid << "), status(" << e.status << ") }");
                        }
                        K2EXPECT(status1, dto::K23SIStatus::OK);
                        K2EXPECT(status2, dto::K23SIStatus::OK);
                        K2EXPECT(val1.records[0].txnId.mtr.txnid, mtr.txnid);
                        K2EXPECT(val1.records[0].status, dto::DataRecord::Committed);
                        K2EXPECT(val2.records[0].txnId.mtr.txnid, mtr.txnid);
                        K2EXPECT(val2.records[0].status, dto::DataRecord::WriteIntent);
                    });
                });
            })
            .then([&] {
                K2INFO("------- SC07.case04 ( Commit-End a transaction missing the trh key ) -------");
                return seastar::when_all(doWrite(k1, v0, mtr2, k1, collname, false, true, ErrorCaseOpt::NoInjection), \
                        doWrite(k2, v0, mtr2, k1, collname, false, false, ErrorCaseOpt::NoInjection))
                .then([&](auto&& response) {
                    auto& [resp1, resp2] = response;
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    K2INFO("SC07.prepare::OP_WI_k1. " << "status: " << status1.code << " with MESG: " << status1.message);
                    K2INFO("SC07.prepare::OP_WI_k2. " << "status: " << status2.code << " with MESG: " << status2.message);
                    K2EXPECT(status1, dto::K23SIStatus::Created);
                    K2EXPECT(status2, dto::K23SIStatus::Created);
                })
                .then([&] {
                    return doEnd(k1, mtr2, collname, true, {k2}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2INFO("SC07.case04::OP_end_commit_miss_trh_key. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                    });
                })
                .then([&] {
                    return seastar::when_all(doInspectRecords(k1, collname), doInspectRecords(k2, collname))
                    .then([&](auto&& response)  {
                        auto& [resp1, resp2] = response;
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        K2INFO("SC07.case04.OP_inspect_record_k1. " << "status: " << status1.code << " with MESG: " << status1.message);
                        K2INFO("SC07.case04.OP_inspect_record_k2. " << "status: " << status2.code << " with MESG: " << status2.message);
                        for(auto& e : val1.records) {
                            K2INFO("Versions of k1: { key" << e.key << ", txnid(" << e.txnId.mtr.txnid << "), status(" << e.status << ") }");
                        }
                        for(auto& e : val2.records) {
                            K2INFO("Versions of k2: { key" << e.key << ", txnid(" << e.txnId.mtr.txnid << "), status(" << e.status << ") }");
                        }
                        K2EXPECT(status1, dto::K23SIStatus::OK);
                        K2EXPECT(status2, dto::K23SIStatus::OK);
                        K2EXPECT(val1.records[0].txnId.mtr.txnid, mtr2.txnid);
                        K2EXPECT(val1.records[0].status, dto::DataRecord::WriteIntent);
                        K2EXPECT(val2.records[0].txnId.mtr.txnid, mtr2.txnid);
                        K2EXPECT(val2.records[0].status, dto::DataRecord::Committed);
                    });
                });
            })
            .then([&] {
                K2INFO("------- SC07.case05 ( Abort-End a transaction missing some of the non-trh keys ) -------");
                return seastar::when_all(doWrite(k1, v0, mtr3, k1, collname, false, true, ErrorCaseOpt::NoInjection), \
                        doWrite(k2, v0, mtr3, k1, collname, false, false, ErrorCaseOpt::NoInjection))
                .then([&](auto&& response) {
                    auto& [resp1, resp2] = response;
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    K2INFO("SC07.prepare::OP_WI_k1. " << "status: " << status1.code << " with MESG: " << status1.message);
                    K2INFO("SC07.prepare::OP_WI_k2. " << "status: " << status2.code << " with MESG: " << status2.message);
                    K2EXPECT(status1, dto::K23SIStatus::Created);
                    K2EXPECT(status2, dto::K23SIStatus::Created);
                })
                .then([&] {
                    return doEnd(k1, mtr3, collname, false, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2INFO("SC07.case05::OP_end_commit_miss_non_trh_keys. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                    });
                })
                .then([&] {
                    return seastar::when_all(doInspectRecords(k1, collname), doInspectRecords(k2, collname))
                    .then([&](auto&& response)  {
                        auto& [resp1, resp2] = response;
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        K2INFO("SC07.case05.OP_inspect_record_k1. " << "status: " << status1.code << " with MESG: " << status1.message);
                        K2INFO("SC07.case05.OP_inspect_record_k2. " << "status: " << status2.code << " with MESG: " << status2.message);
                        for(auto& e : val1.records) {
                            K2INFO("Versions of k1: { key" << e.key << ", txnid(" << e.txnId.mtr.txnid << "), status(" << e.status << ") }");
                        }
                        for(auto& e : val2.records) {
                            K2INFO("Versions of k2: { key" << e.key << ", txnid(" << e.txnId.mtr.txnid << "), status(" << e.status << ") }");
                        }
                        K2EXPECT(status1, dto::K23SIStatus::OK);
                        K2EXPECT(status2, dto::K23SIStatus::OK);
                        K2EXPECT(val1.records[0].txnId.mtr.txnid, mtr.txnid); // last txn id who committed k1 is mtr
                        K2EXPECT(val1.records[0].status, dto::DataRecord::Committed);
                        K2EXPECT(val2.records[0].txnId.mtr.txnid, mtr3.txnid);
                        K2EXPECT(val2.records[0].status, dto::DataRecord::WriteIntent);
                    });
                });
            })
            .then([&] {
                K2INFO("------- SC07.case06 ( Abort-End a transaction missing the trh key ) -------");
                return seastar::when_all(doWrite(k1, v0, mtr4, k1, collname, false, true, ErrorCaseOpt::NoInjection), \
                        doWrite(k2, v0, mtr4, k1, collname, false, false, ErrorCaseOpt::NoInjection))
                .then([&](auto&& response) {
                    auto& [resp1, resp2] = response;
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    K2INFO("SC07.prepare::OP_WI_k1. " << "status: " << status1.code << " with MESG: " << status1.message);
                    K2INFO("SC07.prepare::OP_WI_k2. " << "status: " << status2.code << " with MESG: " << status2.message);
                    K2EXPECT(status1, dto::K23SIStatus::Created);
                    K2EXPECT(status2, dto::K23SIStatus::Created);
                })
                .then([&] {
                    return doEnd(k1, mtr4, collname, false, {k2}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2INFO("SC07.case06::OP_end_commit_miss_trh_key. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                    });
                })
                .then([&] {
                    return seastar::when_all(doInspectRecords(k1, collname), doInspectRecords(k2, collname))
                    .then([&](auto&& response)  {
                        auto& [resp1, resp2] = response;
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        K2INFO("SC07.case06.OP_inspect_record_k1. " << "status: " << status1.code << " with MESG: " << status1.message);
                        K2INFO("SC07.case06.OP_inspect_record_k2. " << "status: " << status2.code << " with MESG: " << status2.message);
                        for(auto& e : val1.records) {
                            K2INFO("Versions of k1: { key" << e.key << ", txnid(" << e.txnId.mtr.txnid << "), status(" << e.status << ") }");
                        }
                        for(auto& e : val2.records) {
                            K2INFO("Versions of k2: { key" << e.key << ", txnid(" << e.txnId.mtr.txnid << "), status(" << e.status << ") }");
                        }
                        K2EXPECT(status1, dto::K23SIStatus::OK);
                        K2EXPECT(status2, dto::K23SIStatus::OK);
                        K2EXPECT(val1.records[0].txnId.mtr.txnid, mtr4.txnid);
                        K2EXPECT(val1.records[0].status, dto::DataRecord::WriteIntent);
                        K2EXPECT(val2.records[0].txnId.mtr.txnid, mtr2.txnid); // last txn id who committed k2 is mtr2
                        K2EXPECT(val2.records[0].status, dto::DataRecord::Committed);
                    });
                });
            })
            .then([&] {
                K2INFO("------- SC07.case07 ( Abort-Finalize all the keys in the transaction ) -------");
                return seastar::when_all(doWrite(k1, v0, mtr5, k1, collname, false, true, ErrorCaseOpt::NoInjection), \
                        doWrite(k2, v0, mtr5, k1, collname, false, false, ErrorCaseOpt::NoInjection))
                .then([&](auto&& response) {
                    auto& [resp1, resp2] = response;
                    auto [status1, val1] = resp1.get0();
                    auto [status2, val2] = resp2.get0();
                    K2INFO("SC07.prepare::OP_WI_k1. " << "status: " << status1.code << " with MESG: " << status1.message);
                    K2INFO("SC07.prepare::OP_WI_k2. " << "status: " << status2.code << " with MESG: " << status2.message);
                    K2EXPECT(status1, dto::K23SIStatus::Created);
                    K2EXPECT(status2, dto::K23SIStatus::Created);
                })
                .then([&] {
                    return seastar::when_all(doFinalize(k1, k1, mtr5, collname, false, ErrorCaseOpt::NoInjection), \
                            doFinalize(k1, k2, mtr5, collname, false, ErrorCaseOpt::NoInjection))
                    .then([&](auto&& response) {
                        auto& [resp1, resp2] = response;
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        K2INFO("SC07.prepare::OP_finalize_abort_k1. " << "status: " << status1.code << " with MESG: " << status1.message);
                        K2INFO("SC07.prepare::OP_finalize_abort_k2. " << "status: " << status2.code << " with MESG: " << status2.message);
                        K2EXPECT(status1, dto::K23SIStatus::OK);
                        K2EXPECT(status2, dto::K23SIStatus::OK);
                    });
                })
                .then([&] {
                    return doInspectTxn(k1, mtr5, collname)
                    .then([&](auto&& response)  {
                        auto& [status, val] = response;
                        K2INFO("SC07.case07::OP_inspect_txn_5. " << "status: " << status.code << " with MESG: " << status.message);
                        K2INFO("State of txn_5: " << val.state);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                        K2EXPECT(val.state, dto::TxnRecordState::InProgress);
                    });
                });
            })
            .then([&] {
                K2INFO("------- SC07.case08 ( Using a transaction with newer timestamp to Push() an old one ) -------");
                return doPush(k1, collname, mtr5, mtr6, ErrorCaseOpt::NoInjection)
                .then([&](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC07.case08::OP_Push_txn5_txn6(older as challenger). " << "status: " << status.code << " with MESG: " << status.message);
                    K2INFO(val);
                    K2EXPECT(status, dto::K23SIStatus::OK);
                    K2EXPECT(val.winnerMTR, mtr6);
                })
                .then([&] {
                    return seastar::when_all(doInspectTxn(k1, mtr5, collname), doInspectTxn(k1, mtr6, collname))
                    .then([&](auto&& response) {
                        auto& [resp1, resp2] = response;
                        auto [status1, val1] = resp1.get0();
                        auto [status2, val2] = resp2.get0();
                        K2INFO("SC07.case08::OP_inspect_txn_5. " << "status: " << status1.code << " with MESG: " << status1.message);
                        K2INFO("SC07.case08::OP_inspect_txn_6. " << "status: " << status2.code << " with MESG: " << status2.message);
                        K2INFO("State of txn_5: " << val1.state);
                        K2INFO("State of txn_6: " << val2.state);
                        K2EXPECT(status1, dto::K23SIStatus::OK);
                        K2EXPECT(status2, dto::K23SIStatus::KeyNotFound);
                        K2EXPECT(val1.state, dto::TxnRecordState::ForceAborted);
                        K2EXPECT(val2.state, dto::TxnRecordState::Created);
                    });
                });
            });
        }); // end do-with
    }); // end sc-07
}

seastar::future<> testScenario08() {
    K2INFO("+++++++ TestScenario 08: server-initiated txn abort +++++++");

    return seastar::make_ready_future()
    .then([] {
        return getTimeNow();
    })
    .then([this](dto::Timestamp&& ts) {
        return seastar::do_with(
            // multi MTRs are used for multiple transactions to execute. We use these MTRs in sequence
            dto::K23SI_MTR { .txnid = txnids++, .timestamp = {(ts.tEndTSECount()), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { .txnid = txnids++, .timestamp = {(ts.tEndTSECount() + 20000), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { .txnid = txnids++, .timestamp = {(ts.tEndTSECount() + 30000), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { .txnid = txnids++, .timestamp = {(ts.tEndTSECount() + 40000), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::K23SI_MTR { .txnid = txnids++, .timestamp = {(ts.tEndTSECount() + 50000), 123, 1000}, .priority = dto::TxnPriority::Medium },
            dto::Key {.schemaName = "schema", .partitionKey = "SC08_pkey1", .rangeKey = "rKey1"},
            DataRec {.f1="SC05_f1_zero", .f2="SC04_f2_zero"},
            [this](auto& mtr1, auto& mtr2, auto& mtr3, auto& mtr4, auto& mtr5, auto& k1, auto& v0) {
            K2INFO("------- SC08.case01 ( Txn with a old timestamp WI is PUSHed by another txn's READ ) -------");
            return doWrite(k1, v0, mtr1, k1, collname, false, true, ErrorCaseOpt::NoInjection)
            .then([](auto&& response)  {
                auto& [status, val] = response;
                K2INFO("SC08.prepare::OP_WI_old_k1. " << "status: " << status.code << " with MESG: " << status.message);
                K2EXPECT(status, dto::K23SIStatus::Created);
            })
            .then([&] {
                return doRead(k1, mtr2, collname, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC08.cases01::OP_read_face_old_WI. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::KeyNotFound);
                });
            })
            .then([&] {
                return doInspectTxn(k1, mtr1, collname)
                .then([&](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC08.case01::OP_inspect_txn_1. " << "status: " << status.code << " with MESG: " << status.message);
                    K2INFO("State of txn_1: " << val.state);
                    K2EXPECT(status, dto::K23SIStatus::OK);
                    K2EXPECT(val.state, dto::TxnRecordState::ForceAborted);
                });
            })
            .then([&] {
                return doEnd(k1, mtr1, collname, false, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
                .then([](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC08.cases01::OP_End_txn_1. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::OK);
                });
            })
            .then([&] {
                return doInspectTxn(k1, mtr1, collname)
                .then([&](auto&& response)  {
                    auto& [status, val] = response;
                    K2INFO("SC08.case01::OP_inspect_txn_1. " << "status: " << status.code << " with MESG: " << status.message);
                    K2INFO("State of txn_1: " << val.state);
                    K2EXPECT(status, dto::K23SIStatus::KeyNotFound);
                    K2EXPECT(val.state, dto::TxnRecordState::Created);
                });
            })
            .then([&] {
                K2INFO("------- SC08.case02 ( Txn WRITE happens with the time older than the Read-Cache record ) -------");
                return doWrite(k1, v0, mtr1, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([&](auto&& response) {
                    auto& [status, val] = response;
                    K2INFO("SC08.cases02::OP_WRITE_ts_before_readCache. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::AbortRequestTooOld);
                })
                .then([&] {
                    return doInspectTxn(k1, mtr1, collname)
                    .then([&](auto&& response)  {
                        auto& [status, val] = response;
                        K2INFO("SC08.case02::OP_inspect_txn_1. " << "status: " << status.code << " with MESG: " << status.message);
                        K2INFO("State of txn_1: " << val.state);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                        K2EXPECT(val.state, dto::TxnRecordState::InProgress);
                    });
                })
                .then([&] {
                    return doEnd(k1, mtr1, collname, false, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2INFO("SC08.cases02::OP_End_txn_1. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                    });
                });
            })
            .then([&] {
                K2INFO("------- SC08.case03 ( The timestamp is older than the latest committed record ) -------");
                return doWrite(k1, v0, mtr3, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([&](auto&& response) {
                    auto& [status, val] = response;
                    K2INFO("SC08.prepare::OP_WI_mtr3_k1. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::Created);
                })
                .then([&] {
                    return doEnd(k1, mtr3, collname, true, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([&](auto&& response) {
                        auto& [status, val] = response;
                        K2INFO("SC08.prepare::OP_END_Commit_mtr3_k1. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                    });
                })
                .then([&]{
                    return doWrite(k1, v0, mtr1, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([&](auto&& response) {
                        auto& [status, val] = response;
                        K2INFO("SC08.case03::OP_write_TsOlder_than_committed_record. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::AbortRequestTooOld);
                    });
                })
                .then([&] {
                    return doInspectTxn(k1, mtr1, collname)
                    .then([&](auto&& response)  {
                        auto& [status, val] = response;
                        K2INFO("SC08.case03::OP_inspect_txn_1. " << "status: " << status.code << " with MESG: " << status.message);
                        K2INFO("State of txn_1: " << val.state);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                        K2EXPECT(val.state, dto::TxnRecordState::InProgress);
                    });
                })
                .then([&] {
                    return doEnd(k1, mtr1, collname, false, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2INFO("SC08.cases03::OP_End_txn_1. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                    });
                });
            })
            .then([&] {
                K2INFO("------- SC08.case04 ( WRITE timestamp is older than the second latest version of the record, where the latest version of the record is WI ) -------");
                return doWrite(k1, v0, mtr5, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([&](auto&& response) {
                    auto& [status, val] = response;
                    K2INFO("SC08.prepare::OP_WI_mtr5_k1. " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::Created);
                })
                .then([&]{
                    return doWrite(k1, v0, mtr1, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                    .then([&](auto&& response) {
                        auto& [status, val] = response;
                        K2INFO("SC08.case04::OP_write_olderTs. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::AbortRequestTooOld);
                    });
                })
                .then([&] {
                    return doInspectTxn(k1, mtr1, collname)
                    .then([&](auto&& response)  {
                        auto& [status, val] = response;
                        K2INFO("SC08.case04::OP_inspect_txn_1. " << "status: " << status.code << " with MESG: " << status.message);
                        K2INFO("State of txn_1: " << val.state);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                        K2EXPECT(val.state, dto::TxnRecordState::InProgress);
                    });
                })
                .then([&] {
                    return doEnd(k1, mtr1, collname, false, {k1}, Duration{0s}, ErrorCaseOpt::NoInjection)
                    .then([](auto&& response)  {
                        auto& [status, val] = response;
                        K2INFO("SC08.cases04::OP_End_txn_1. " << "status: " << status.code << " with MESG: " << status.message);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                    });
                })
                .then([&] {
                    return doInspectTxn(k1, mtr5, collname)
                    .then([&](auto&& response)  {
                        auto& [status, val] = response;
                        K2INFO("SC08.case04::OP_inspect_txn_5. " << "status: " << status.code << " with MESG: " << status.message);
                        K2INFO("State of txn_5: " << val.state);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                        K2EXPECT(val.state, dto::TxnRecordState::InProgress);
                    });
                });
            })
            .then([&] {
                K2INFO("------- SC08.case05 ( The timestamp is newer than all the  committed version of the record, but earlier than the WI of the record ) -------");
                return doWrite(k1, v0, mtr4, k1, collname, false, true, ErrorCaseOpt::NoInjection)
                .then([&](auto&& response) {
                    auto& [status, val] = response;
                    K2INFO("SC08.case05::OP_WI_mtr4_k1(push inc mtr5). " << "status: " << status.code << " with MESG: " << status.message);
                    K2EXPECT(status, dto::K23SIStatus::AbortConflict);
                })
                .then([&] {
                    return doInspectTxn(k1, mtr4, collname)
                    .then([&](auto&& response)  {
                        auto& [status, val] = response;
                        K2INFO("SC08.case05::OP_inspect_txn_4(push loser). " << "status: " << status.code << " with MESG: " << status.message);
                        K2INFO("State of txn_4: " << val.state);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                        K2EXPECT(val.state, dto::TxnRecordState::InProgress);
                    });
                })
                .then([&] {
                    return doInspectTxn(k1, mtr5, collname)
                    .then([&](auto&& response)  {
                        auto& [status, val] = response;
                        K2INFO("SC08.case05::OP_inspect_txn_5(push winner). " << "status: " << status.code << " with MESG: " << status.message);
                        K2INFO("State of txn_5: " << val.state);
                        K2EXPECT(status, dto::K23SIStatus::OK);
                        K2EXPECT(val.state, dto::TxnRecordState::InProgress);
                    });
                });
            });
        }); // end do-with
    }); // end sc-08
}


};	// class k23si_testing
}	// ns k2

int main(int argc, char** argv){
	k2::App app("txn_testing");
	app.addOptions()("cpo_endpoint", bpo::value<k2::String>(), "The endpoint of the CPO service");
    app.addOptions()("k2_endpoints", bpo::value<std::vector<k2::String>>()->multitoken(), "The endpoints of the k2 cluster");
	app.addApplet<k2::txn_testing>();
	return app.start(argc, argv);
}

