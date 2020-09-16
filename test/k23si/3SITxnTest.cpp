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
        SKVRecord record(cname, seastar::make_lw_shared(_schema));
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

            SKVRecord record(cname, seastar::make_lw_shared(_schema));
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
        request.key = trh;
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


public:		// test	scenario

// Any request (READ, WRITE, PUSH, END, FINALIZE, HEARTBEAT) should observe a timeout(404_not_found)
// example of command: CPO_COLLECTION_GET & K23SI_WRITE
seastar::future<> testScenario00() {
    std::cout << std::endl;
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
    std::cout << std::endl << std::endl;
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
        std::cout << std::endl;
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
        std::cout << std::endl;
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
        std::cout << std::endl;
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
        std::cout << std::endl;
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
        std::cout << std::endl;
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
        std::cout << std::endl;
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
        std::cout << std::endl;
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
        std::cout << std::endl;
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
        std::cout << std::endl;
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
        std::cout << std::endl;
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
                std::cout << "------- Partition Info printer for key1("<< key1 << ") -------" << std::endl;
                std::cout << "pvid(id|rangeVersion|assignmentVersion): " <<part.partition->pvid.id << part.partition->pvid.rangeVersion << part.partition->pvid.assignmentVersion <<std::endl;
                std::cout << "startKey(" << part.partition->startKey << ") endKey(" << part.partition->endKey << ") astate(" << part.partition->astate << ")." << std::endl;
                std::cout << "preferEP: " << (*part.preferredEndpoint).getURL() << "  EP set size: "<< part.partition->endpoints.size() << "  endpoints: ";
                for (auto& eps : part.partition->endpoints) {
                    std::cout << eps << ";  ";
                }
                std::cout << std::endl;

                auto& part2 = _pgetter.getPartitionForKey(key2);
                std::cout << std::endl << "------- Partition Info printer for key2("<< key2 << ") -------" << std::endl;
                std::cout << "pvid(id|rangeVersion|assignmentVersion): " <<part2.partition->pvid.id << part2.partition->pvid.rangeVersion << part2.partition->pvid.assignmentVersion <<std::endl;
                std::cout << "startKey(" << part2.partition->startKey << ") endKey(" << part2.partition->endKey << ") astate(" << part2.partition->astate << ")." << std::endl;
                std::cout << "preferEP: " << (*part2.preferredEndpoint).getURL() << "  EP set size: "<< part2.partition->endpoints.size() << "  endpoints: ";
                for (auto& eps : part2.partition->endpoints) {
                    std::cout << eps << ";  ";
                }
                std::cout << std::endl << std::endl;

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
        std::cout << std::endl;
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
    std::cout << std::endl << std::endl;
	K2INFO("+++++++ TestScenario 02: assigned node with single version data +++++++");
	K2INFO("--->Test SETUP: The following data have been written in the given state.");
    std::cout << "(\"SC02_pkey1\",\"\", v1) -> commited" << std::endl \
            << "(\"SC02_pkey2\",\"range1\", v1) -> commited" << std::endl \
            << "(\"SC02_pkey3\",\"\", v1) -> WI" << std::endl \
            << "(\"SC02_pkey4\",\"\", v1) -> aborted but not cleaned" << std::endl;

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
        std::cout << std::endl;
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
        std::cout << std::endl;
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
        std::cout << std::endl;
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
        std::cout << std::endl;
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
        std::cout << std::endl;
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
        std::cout << std::endl;
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
        std::cout << std::endl;
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
        std::cout << std::endl;
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
        return seastar::sleep(1s);
    });
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

