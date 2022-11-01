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

#include "Service.h"

#include <k2/appbase/Appbase.h>
#include <k2/infrastructure/APIServer.h>
#include <k2/transport/Payload.h>  // for payload construction
#include <k2/transport/Status.h>  // for RPC
#include <k2/transport/RPCDispatcher.h>  // for RPC
#include <k2/dto/ControlPlaneOracle.h> // our DTO
#include <k2/dto/AssignmentManager.h> // our DTO
#include <k2/dto/MessageVerbs.h> // our DTO
#include <k2/dto/K23SI.h> // our DTO
#include <k2/dto/LogStream.h>
#include <k2/transport/PayloadFileUtil.h>
#include <k2/transport/RetryStrategy.h>
#include <seastar/core/sleep.hh>

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
namespace k2::cpo {

CPOService::CPOService() {
    K2LOG_I(log::cposvr, "ctor");
}

CPOService::~CPOService() {
    K2LOG_I(log::cposvr, "dtor");
}

seastar::future<> CPOService::gracefulStop() {
    K2LOG_I(log::cposvr, "stop");
    std::vector<seastar::future<>> futs;
    for (auto& [k,v]: _assignments) {
        futs.push_back(std::move(v));
    }
    futs.push_back(_tsoAssignTimer.stop());
    _assignments.clear();
    return seastar::when_all_succeed(futs.begin(), futs.end()).discard_result();
}

void CPOService::_registerMetrics() {
    _metricGroups.clear();
    std::vector<sm::label_instance> labels;
    labels.push_back(sm::label_instance("total_cores", seastar::smp::count));
    _metricGroups.add_group("CPO", {
        sm::make_gauge("assigned tso instances", [this] {return _healthyTSOs.size();}, sm::description("number of tso instances currently assigned"), labels),
        sm::make_gauge("unassigned tso instances",[this] {return _failedTSOs.size();}, sm::description("number of tso instances currently unassigned"), labels)
    });

}

seastar::future<> CPOService::start() {
    K2LOG_I(log::cposvr, "CPOService start");
    _registerMetrics();
    APIServer& api_server = AppBase().getDist<APIServer>().local();

    if (_heartbeatMonitorShardId() >= seastar::smp::count) {
        K2LOG_W(log::cposvr, "Specified shard ID for heartbeat monitoring is unavailable. Health monitoring is disabled");
    }

    K2LOG_I(log::cposvr, "Registering message handlers");
    RPC().registerRPCObserver<dto::CollectionCreateRequest, dto::CollectionCreateResponse>(dto::Verbs::CPO_COLLECTION_CREATE, [this](dto::CollectionCreateRequest&& request) {
        return AppBase().getDist<CPOService>().invoke_on(0, &CPOService::handleCreate, std::move(request));
    });
    api_server.registerAPIObserver<k2::Statuses, dto::CollectionCreateRequest, dto::CollectionCreateResponse>("CollectionCreate", "CPO CollectionCreate", [this] (dto::CollectionCreateRequest&& request) {
        return AppBase().getDist<CPOService>().invoke_on(0, &CPOService::handleCreate, std::move(request));
    });

    RPC().registerRPCObserver<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET, [this](dto::CollectionGetRequest&& request) {
        return AppBase().getDist<CPOService>().invoke_on(0, &CPOService::handleGet, std::move(request));
    });
    api_server.registerAPIObserver<k2::Statuses, dto::CollectionGetRequest, dto::CollectionGetResponse>("CollectionGet", "CPO CollectionGet", [this](dto::CollectionGetRequest&& request) {
        return AppBase().getDist<CPOService>().invoke_on(0, &CPOService::handleGet, std::move(request));
    });

    RPC().registerRPCObserver<dto::CollectionDropRequest, dto::CollectionDropResponse>(dto::Verbs::CPO_COLLECTION_DROP, [this](dto::CollectionDropRequest&& request) {
        return AppBase().getDist<CPOService>().invoke_on(0, &CPOService::handleCollectionDrop, std::move(request));
    });
    api_server.registerAPIObserver<k2::Statuses, dto::CollectionDropRequest, dto::CollectionDropResponse>("CollectionDrop", "CPO CollectionDrop", [this](dto::CollectionDropRequest&& request) {
        return AppBase().getDist<CPOService>().invoke_on(0, &CPOService::handleCollectionDrop, std::move(request));
    });

    RPC().registerRPCObserver<dto::PersistenceClusterCreateRequest, dto::PersistenceClusterCreateResponse>(dto::Verbs::CPO_PERSISTENCE_CLUSTER_CREATE, [this](dto::PersistenceClusterCreateRequest&& request) {
        return AppBase().getDist<CPOService>().invoke_on(0, &CPOService::handlePersistenceClusterCreate, std::move(request));
    });

    RPC().registerRPCObserver<dto::PersistenceClusterGetRequest, dto::PersistenceClusterGetResponse>(dto::Verbs::CPO_PERSISTENCE_CLUSTER_GET, [this](dto::PersistenceClusterGetRequest&& request) {
        return AppBase().getDist<CPOService>().invoke_on(0, &CPOService::handlePersistenceClusterGet, std::move(request));
    });

    RPC().registerRPCObserver<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, [this] (dto::CreateSchemaRequest&& request) {
        return AppBase().getDist<CPOService>().invoke_on(0, &CPOService::handleCreateSchema, std::move(request));
    });
    api_server.registerAPIObserver<k2::Statuses, dto::CreateSchemaRequest, dto::CreateSchemaResponse>("SchemaCreate", "CPO SchemaCreate", [this] (dto::CreateSchemaRequest&& request) {
        return AppBase().getDist<CPOService>().invoke_on(0, &CPOService::handleCreateSchema, std::move(request));
    });

    RPC().registerRPCObserver<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET,
    [this] (dto::GetSchemasRequest&& request) {
        return AppBase().getDist<CPOService>().invoke_on(0, &CPOService::handleSchemasGet, std::move(request));
    });

    RPC().registerRPCObserver<dto::MetadataPutRequest, dto::MetadataPutResponse>(dto::Verbs::CPO_PARTITION_METADATA_PUT, [this](dto::MetadataPutRequest&& request) {
        return AppBase().getDist<CPOService>().invoke_on(0, &CPOService::handleMetadataPut, std::move(request));
    });

    RPC().registerRPCObserver<dto::MetadataGetRequest, dto::MetadataGetResponse>(dto::Verbs::CPO_PARTITION_METADATA_GET, [this](dto::MetadataGetRequest&& request) {
        return AppBase().getDist<CPOService>().invoke_on(0, &CPOService::handleMetadataGet, std::move(request));
    });

    RPC().registerRPCObserver<dto::GetPersistenceEndpointsRequest, dto::GetPersistenceEndpointsResponse>(dto::Verbs::CPO_GET_PERSISTENCE_ENDPOINTS, [this] (dto::GetPersistenceEndpointsRequest&& request) {
        (void) request;
        return AppBase().getDist<HealthMonitor>().local().getPersistEndpoints()
        .then([this] (std::vector<String>&& eps) {
            return RPCResponse(Statuses::S200_OK("GetPersistenceEndpoints success"),
                               dto::GetPersistenceEndpointsResponse{std::move(eps)});
        });
    });


    RPC().registerRPCObserver<dto::GetTSOEndpointsRequest, dto::GetTSOEndpointsResponse>(dto::Verbs::CPO_GET_TSO_ENDPOINTS, [this] (dto::GetTSOEndpointsRequest&& ) {
        K2LOG_I(log::cposvr, "healthy TSOs: {}", _healthyTSOs);
        if (_healthyTSOs.size() > 0) {
            return RPCResponse(Statuses::S200_OK("GetTSOEndpoints success"),dto::GetTSOEndpointsResponse{_healthyTSOs, _TSOErrorBound()});
        } else {
            return RPCResponse(Statuses::S503_Service_Unavailable("Unable to get TSO endpoints: no healthy TSO"), dto::GetTSOEndpointsResponse());
        }
    });

    api_server.registerAPIObserver<k2::Statuses, dto::GetSchemasRequest, dto::GetSchemasResponse>("GetSchemas", "CPO get all schemas for a collection",
    [this] (dto::GetSchemasRequest&& request) {
        return AppBase().getDist<CPOService>().invoke_on(0, &CPOService::handleSchemasGet, std::move(request));
    });

    if (seastar::this_shard_id() == 0) {
        // only core 0 handles CPO business
        if (!fileutil::makeDir(_dataDir())) {
            return seastar::make_exception_future<>(std::runtime_error("unable to create data directory"));
        }
    }

    return AppBase().getDist<HealthMonitor>().local().getTSOEndpoints()
    .then([this] (std::vector<String>&& eps) {
        size_t tsoID{1000};
        for (const auto & ep : eps) {
            _failedTSOs[ep] = tsoID++;
        }
        _tsoAssignTimer.setCallback([this] {
            return _assignAllTSOs()
            .then([this] (bool success) {
                if (!success) {
                    K2LOG_W(log::cposvr, "Failed to assign all TSOs");
                    _tsoAssignTimer.arm(_assignTimeout());
                }
            });
        });
        _tsoAssignTimer.arm(0s);
    });

}

seastar::future<bool> CPOService::_assignAllTSOs() {
    std::vector<seastar::future<>> futs;
    for (auto tso=_failedTSOs.begin(); tso!=_failedTSOs.end(); ++tso) {
        futs.push_back(
            _assignTSO(tso->first, tso->second)
        );
    }
    return seastar::when_all_succeed(futs.begin(), futs.end()).discard_result()
    .then([this] {
        return _failedTSOs.empty();
    });
}

seastar::future<> CPOService::_doAssignTSO(const String &ep, size_t tsoID) {
    k2::dto::AssignTSORequest request{.tsoID = tsoID, .tsoErrBound=_TSOErrorBound()};
    auto txep = RPC().getTXEndpoint(ep);
    K2LOG_D(log::cposvr, "assigning TSO: {}", ep);
    if (!txep) {
        K2LOG_W(log::cposvr, "unable to obtain endpoint for {}", ep);
        return seastar::make_exception_future(StopRetryException{});
    }
    return RPC().callRPC<dto::AssignTSORequest, dto::AssignTSOResponse>(
        dto::Verbs::TSO_ASSIGNMENT, request, *txep, _perCallTSOAssignTimeout()
    ).then([ep,tsoID,this] (auto &&result) {
        auto& [status, resp] = result;
        if (status.is2xxOK()) {
            K2LOG_D(log::cposvr, "tso successfully assigned for endpoint: {}", ep);
            if (_failedTSOs.count(ep) > 0) {
                _healthyTSOs.push_back(ep);
                _failedTSOs.erase(ep);
            }
            K2LOG_D(log::cposvr, "assigned TSOs: {}", _healthyTSOs);
            return seastar::make_ready_future();
        }
        else {
            K2LOG_W(log::cposvr, "tso assignment unsuccessful for endpoint: {} due to {}", ep, status);
            if (status.is5xxRetryable()) {
                // retry TSO assignment
                K2LOG_D(log::cposvr, "retrying TSO assignment for endpoint: {} due to 500 retryable", ep);
                return seastar::make_exception_future<>(std::runtime_error(status.message));
            } else {
                return seastar::make_exception_future(StopRetryException{});
            }
        }
    });
}

seastar::future<> CPOService::_assignTSO(const String &ep, size_t tsoID) {
    return seastar::do_with(ExponentialBackoffStrategy().withRetries(_maxAssignRetries()).withBaseBackoffTime(100ms).withRate(2), [&ep, tsoID, this](auto& retryStrategy) {
        return retryStrategy.run([&ep,tsoID,this] (size_t retriesLeft, Duration backoffTime) {
            K2LOG_I(log::cposvr, "Sending TSO assignment with retriesLeft={}, and backoffTime={}, with {}", retriesLeft, backoffTime, ep);
            return _doAssignTSO(ep, tsoID);
        })
        .handle_exception([&ep,tsoID,this] (auto exc) {
            K2LOG_W_EXC(log::cposvr, exc, "Failed to assign TSO for endpoint after retry: {}", ep);
        });
    });
}


seastar::future<> CPOService::_getNodes() {
    return AppBase().getDist<HealthMonitor>().invoke_on(_heartbeatMonitorShardId(), &HealthMonitor::getNodepoolEndpoints)
    .then([this] (std::vector<String>&& nodes) {
        for (const String& node : nodes) {
            if (_nodesToCollection.find(node) == _nodesToCollection.end()) {
                _nodesToCollection[node] = NodeAssignmentEntry{.collection = "", .assigned = false};
            }
        }

        return seastar::make_ready_future<>();
    });
}

String CPOService::_assignToFreeNode(String collection) {
    auto it = _nodesToCollection.begin();
    for (; it != _nodesToCollection.end(); ++it) {
        if (!(it->second.assigned)) {
            it->second.assigned = true;
            it->second.collection = collection;
            return it->first;
        }
    }
    // TODO: when we have CPO persistence figured out (replacing current fileutil::writeFile method),
    // this needs to be persisted

    return "";
}

int CPOService::_makeRangePartitionMap(dto::Collection& collection, const std::vector<String>& rangeEnds) {
    String lastEnd = "";

    // The partition for a key is found using lower_bound on the start keys,
    // so it is OK for the last range end key to be ""
    if (rangeEnds[rangeEnds.size()-1] != "") {
        K2LOG_W(log::cposvr, "Error in client's make collection request: the last rangeEnd key is not an empty string");
        return -1;
    }

    for (uint64_t i = 0; i < rangeEnds.size(); ++i) {
        String node = _assignToFreeNode(collection.metadata.name);
        if (node == "") {
            K2LOG_W(log::cposvr, "No free nodes for assignment");
            return -1;
        }

        dto::Partition part {
            .keyRangeV{
                .startKey=lastEnd,
                .endKey=rangeEnds[i],
                .pvid{
                    .id = i,
                    .rangeVersion=1,
                    .assignmentVersion=1
                },
            },
            .endpoints={node},
            .astate=dto::AssignmentState::PendingAssignment
        };

        lastEnd = rangeEnds[i];
        collection.partitionMap.partitions.push_back(std::move(part));
    }

    collection.partitionMap.version++;
    return 0;
}

int CPOService::_makeHashPartitionMap(dto::Collection& collection, uint32_t numNodes) {
    const uint64_t max = std::numeric_limits<uint64_t>::max();

    uint64_t partSize = (numNodes > 0) ? (max / numNodes) : (max);
    for (uint64_t i =0; i < numNodes; ++i) {
        uint64_t start = i*partSize;
        uint64_t end = (i == numNodes -1) ? (max) : ((i+1)*partSize - 1);
        String node = _assignToFreeNode(collection.metadata.name);
        if (node == "") {
            K2LOG_W(log::cposvr, "No free nodes for assignment");
            return -1;
        }

        dto::Partition part{
            .keyRangeV{
                .startKey=std::to_string(start),
                .endKey=std::to_string(end),
                .pvid{
                    .id = i,
                    .rangeVersion=1,
                    .assignmentVersion=1
                },
            },
            .endpoints={node},
            .astate=dto::AssignmentState::PendingAssignment
        };
        collection.partitionMap.partitions.push_back(std::move(part));
    }

    collection.partitionMap.version++;

    return 0;
}

seastar::future<std::tuple<Status, dto::CollectionCreateResponse>>
CPOService::handleCreate(dto::CollectionCreateRequest&& request) {
    return _getNodes().then([this, request=std::move(request)] () mutable {
        K2LOG_I(log::cposvr, "Received collection create request for name={}", request.metadata.name);
        auto cpath = _getCollectionPath(request.metadata.name);
        if (fileutil::fileExists(cpath)) {
            return RPCResponse(Statuses::S403_Forbidden("collection already exists"), dto::CollectionCreateResponse());
        }
        request.metadata.heartbeatDeadline = _collectionHeartbeatDeadline();
        // create a collection from the incoming request
        dto::Collection collection;
        collection.metadata = request.metadata;

        int err = 0;
        if (collection.metadata.hashScheme == dto::HashScheme::HashCRC32C) {
            err = _makeHashPartitionMap(collection, request.metadata.capacity.minNodes);
        }
        else if (collection.metadata.hashScheme == dto::HashScheme::Range) {
            err = _makeRangePartitionMap(collection, request.rangeEnds);
        }
        else {
            return RPCResponse(Statuses::S403_Forbidden("Unknown hashScheme"), dto::CollectionCreateResponse());
        }

        if (err) {
            return RPCResponse(Statuses::S403_Forbidden("Could not find free nodes"), dto::CollectionCreateResponse());
        }

        schemas[collection.metadata.name] = std::vector<dto::Schema>();

        auto status = _saveCollection(collection);
        if (!status.is2xxOK()) {
            return RPCResponse(std::move(status), dto::CollectionCreateResponse());
        }

        K2LOG_I(log::cposvr, "Created collection {}", cpath);
        _assignCollection(collection);
        return RPCResponse(std::move(status), dto::CollectionCreateResponse());
    });
}

seastar::future<std::tuple<Status, dto::CollectionGetResponse>>
CPOService::handleGet(dto::CollectionGetRequest&& request) {
    K2LOG_I(log::cposvr, "Received collection get request for {}", request.name);
    auto [status, collection] = _getCollection(request.name);

    dto::CollectionGetResponse response{};

    if (collection.metadata.deleted) {
        return RPCResponse(Statuses::S404_Not_Found("Collection is being deleted"), std::move(response));
    }

    if (status.is2xxOK()) {
        response.collection = std::move(collection);
    }
    return RPCResponse(std::move(status), std::move(response));
}

seastar::future<std::tuple<Status, dto::CollectionDropResponse>>
CPOService::handleCollectionDrop(dto::CollectionDropRequest&& request) {
    K2LOG_I(log::cposvr, "Received collection drop request for {}", request.name);
    auto [status, collection] = _getCollection(request.name);

    if (!status.is2xxOK()) {
        return RPCResponse(std::move(status), dto::CollectionDropResponse());
    }

    // Setting the deleted flag will make the CPO not return the collection for getCOllection
    // requests, but it will let the user retry the drop collection if not all of the
    // offload requests succeeded
    collection.metadata.deleted = true;
    status = _saveCollection(collection);
    if (!status.is2xxOK()) {
        return RPCResponse(std::move(status), dto::CollectionDropResponse());
    }
    K2LOG_D(log::cposvr, "Collection deleted flag persisted for {}, starting partition offload", request.name);

    return _offloadCollection(collection).then([this, name=request.name] (bool allOffloaded) {
        if (!allOffloaded) {
            return RPCResponse(Statuses::S503_Service_Unavailable("Not all partitions offloaded"), dto::CollectionDropResponse());
        }

        // TODO implement clean up of persistence data
        schemas.erase(name);
        String collPath = _getCollectionPath(name);
        remove(collPath.c_str());
        String schemaPath = _getSchemasPath(name);
        remove(schemaPath.c_str());

        auto it = _nodesToCollection.begin();
        for (; it != _nodesToCollection.end(); ++it) {
            if (it->second.collection == name) {
                it->second.assigned = false;
                it->second.collection = "";
            }
        }
        // TODO: when we have CPO persistence figured out (replacing current fileutil::writeFile method),
        // this needs to be persisted

        return RPCResponse(Statuses::S200_OK("Offload successful"), dto::CollectionDropResponse());
    });
}

seastar::future<Status> CPOService::_pushSchema(const dto::Collection& collection, const dto::Schema& schema) {
    std::vector<seastar::future<std::tuple<Status, dto::K23SIPushSchemaResponse>>> pushFutures;

    for (const dto::Partition& part : collection.partitionMap.partitions) {
        auto endpoint = RPC().getTXEndpoint(*(part.endpoints.begin()));
        if (!endpoint) {
            return seastar::make_ready_future<Status>(Statuses::S422_Unprocessable_Entity("Partition endpoint was null"));
        }

        dto::K23SIPushSchemaRequest request { collection.metadata.name, schema };

        pushFutures.push_back(RPC().callRPC<dto::K23SIPushSchemaRequest, dto::K23SIPushSchemaResponse>
            (dto::Verbs::K23SI_PUSH_SCHEMA, request, *endpoint, 10s));
    }

    return when_all_succeed(pushFutures.begin(), pushFutures.end())
    .then([] (auto&& doneFutures) {
        for (const auto& doneFuture : doneFutures) {
            auto& [status, k2response] = doneFuture;
            if (!status.is2xxOK()) {
                K2LOG_W(log::cposvr, "Failed to push schema update: status={}", status);
                return seastar::make_ready_future<Status>(status);
            }
        }

        return seastar::make_ready_future<Status>(Statuses::S200_OK);
    })
    .handle_exception([](auto exc) {
        K2LOG_W_EXC(log::cposvr, exc, "Failed to push schema update");
        return seastar::make_ready_future<Status>(Statuses::S500_Internal_Server_Error("Failed to push schema"));
    });
}

seastar::future<std::tuple<Status, dto::GetSchemasResponse>>
CPOService::handleSchemasGet(dto::GetSchemasRequest&& request) {
    auto it = schemas.find(request.collectionName);
    if (it == schemas.end()) {
        auto [status, collection] = _getCollection(request.collectionName);
        if (!status.is2xxOK()) {
            return RPCResponse(std::move(status), dto::GetSchemasResponse{});
        }

        it = schemas.find(request.collectionName);
        K2ASSERT(log::cposvr, it != schemas.end(), "Schemas iterator is end after collection refresh");
    }

    return RPCResponse(Statuses::S200_OK("SchemasGet success"), dto::GetSchemasResponse { it->second });
}

seastar::future<std::tuple<Status, dto::CreateSchemaResponse>>
CPOService::handleCreateSchema(dto::CreateSchemaRequest&& request) {
    auto name = request.schema.name;
    auto ver = request.schema.version;
    auto cname = request.collectionName;
    K2LOG_I(log::cposvr, "Received schema create request for {} : {}@{}", cname, name, ver);

    // 1. Stateless validation of request

    Status validation = request.schema.basicValidation();
    if (!validation.is2xxOK()) {
        return RPCResponse(std::move(validation), dto::CreateSchemaResponse{});
    }

    // 2. Stateful validation

    // getCollection to make sure in memory cache of schemas is up to date, and we need the
    // collection with partition map anyway to do the schema push
    auto [status, collection] = _getCollection(cname);
    if (!status.is2xxOK()) {
        return RPCResponse(std::move(status), dto::CreateSchemaResponse{});
    }

    bool validatedKeys = false;
    for (const dto::Schema& otherSchema : schemas[cname]) {
        if (otherSchema.name == request.schema.name && otherSchema.version == request.schema.version) {
            return RPCResponse(Statuses::S403_Forbidden("Schema name and version already exist"), dto::CreateSchemaResponse{});
        }

        // As long as we validate that the key fields match with at least one
        // other schema with the same name then they will always match
        if (!validatedKeys && otherSchema.name == request.schema.name) {
            validation = otherSchema.canUpgradeTo(request.schema);
            if (!validation.is2xxOK()) {
                return RPCResponse(std::move(validation), dto::CreateSchemaResponse{});
            }

            validatedKeys = true;
        }
    }

    // 3. Save to disk
    Status saved = _saveSchemas(request.collectionName);
    if (!saved.is2xxOK()) {
        return RPCResponse(std::move(saved), dto::CreateSchemaResponse{});
    }

    // 4. Update in memory
    schemas[request.collectionName].push_back(std::move(request.schema));

    // 5. Push to K2 nodes and respond to client
    return _pushSchema(collection, schemas[request.collectionName].back())
    .then([cname, name, ver] (Status&& status) {
        K2LOG_I(log::cposvr, "CreateSchema  {} : {}@{} completed with status {}", cname, name, ver, status);
        return RPCResponse(std::move(status), dto::CreateSchemaResponse{});
    });
}

String CPOService::_getCollectionPath(String name) {
    return _dataDir() + "/" + name + ".collection";
}

String CPOService::_getPersistenceClusterPath(String clusterName) {
    return _dataDir() + "/" + clusterName + ".persistence";
}

String CPOService::_getSchemasPath(String collectionName) {
    return _getCollectionPath(collectionName) + ".schemas";
}

seastar::future<> CPOService::_doAssignCollection(dto::AssignmentCreateRequest &request, const String &name, const String &ep) {
    auto txep = RPC().getTXEndpoint(ep);
    if (txep == nullptr) {
        return seastar::make_exception_future<>(std::runtime_error("unable to assign collection due to wrong endpoint"));
    }
    return RPC().callRPC<dto::AssignmentCreateRequest, dto::AssignmentCreateResponse>
                (dto::K2_ASSIGNMENT_CREATE, request, *txep, _assignTimeout())
    .then([this, name, ep](auto&& result) {
        auto& [status, resp] = result;
        if (status.is2xxOK()) {
            K2LOG_I(log::cposvr, "assignment successful for collection {}, for partition {}", name, resp.assignedPartition);
            _handleCompletedAssignment(name, std::move(resp));
            return seastar::make_ready_future();     
        }
        else if (status.is4xxNonRetryable()) {
            // The node refused to accept the assignment. For now, just ignore this
            K2LOG_W(log::cposvr, "assignment for collection {} was refused by {}, due to: {}", name, ep, status);
            _handleCompletedAssignment(name, std::move(resp));
            return seastar::make_exception_future<>(std::runtime_error("unable to assign collection"));
        }
        else {
            K2LOG_W(log::cposvr, "assignment for collection {} failed because of server error by {}, due to: {}", name, ep, status);
            return seastar::make_exception_future<>(std::runtime_error("unable to assign collection"));
        }
    });
}

void CPOService::_assignCollection(dto::Collection& collection) {
    auto &name = collection.metadata.name;
    K2LOG_I(log::cposvr, "Assigning collection {}, to {} nodes", name, collection.partitionMap.partitions.size());
    std::vector<seastar::future<>> futs;
    for (auto& part : collection.partitionMap.partitions) {
        if (part.endpoints.size() == 0) {
            K2LOG_E(log::cposvr, "empty endpoint for partition assignment: {}", part);
            continue;
        }
        auto ep = *part.endpoints.begin();
        K2LOG_I(log::cposvr, "Assigning collection {}, to {}", name, part);
        auto txep = RPC().getTXEndpoint(ep);
        if (!txep) {
            K2LOG_W(log::cposvr, "unable to obtain endpoint for {}", ep);
            continue;
        }
        dto::AssignmentCreateRequest request;
        request.collectionMeta = collection.metadata;
        request.partition = part;

        auto my_eps = k2::RPC().getServerEndpoints();
        for (const auto& ep : my_eps) {
            if (ep) {
                request.cpoEndpoints.push_back(ep->url);
            }
        }

        K2LOG_I(log::cposvr, "Sending assignment for partition: {}", request.partition);
        futs.push_back(
            seastar::do_with(ExponentialBackoffStrategy().withRetries(_maxAssignRetries()).withBaseBackoffTime(_assignTimeout()).withRate(2), std::move(request), [name, ep, this](auto& retryStrategy, auto &request) {
                return retryStrategy.run([&request, name, ep,this] (size_t retriesLeft, Duration timeout) {
                    K2LOG_I(log::cposvr, "Sending partition assignment with retriesLeft={}, and timeout={}, with {}", retriesLeft, timeout, request.partition);
                    return _doAssignCollection(request, name, ep);
                });
            })
        );
    }
    _assignments.emplace(name, seastar::when_all_succeed(futs.begin(), futs.end()).discard_result()
        .then([this, name] {
            _assignments.erase(name);
            return seastar::make_ready_future();
        }));
}

seastar::future<bool> CPOService::_offloadCollection(dto::Collection& collection) {
    auto &name = collection.metadata.name;
    K2LOG_I(log::cposvr, "Offload collection {}, from {} nodes", name, collection.partitionMap.partitions.size());
    std::vector<seastar::future<bool>> futs;
    for (auto& part : collection.partitionMap.partitions) {
        if (part.endpoints.size() == 0) {
            K2LOG_E(log::cposvr, "empty endpoint for partition assignment: {}", part);
            continue;
        }
        auto ep = *part.endpoints.begin();
        K2LOG_I(log::cposvr, "Offloading collection {}, to {}", name, part);
        auto txep = RPC().getTXEndpoint(ep);
        if (!txep) {
            K2LOG_W(log::cposvr, "unable to obtain endpoint for {}", ep);
            continue;
        }
        dto::AssignmentOffloadRequest request{.collectionName = collection.metadata.name};

        futs.push_back(
        RPC().callRPC<dto::AssignmentOffloadRequest, dto::AssignmentOffloadResponse>
                (dto::K2_ASSIGNMENT_OFFLOAD, request, *txep, _assignTimeout())
        .then([this, name, ep](auto&& result) {
            auto& [status, resp] = result;
            if (status.is2xxOK() || status == Statuses::S404_Not_Found) {
                K2LOG_I(log::cposvr, "partition offload successful");
                return seastar::make_ready_future<bool>(true);
            }
            else {
                K2LOG_W(log::cposvr, "offload for collection {} was refused by {}, due to: {}", name, ep, status);
                return seastar::make_ready_future<bool>(false);
            }
        })
        );
    }

    return seastar::when_all(futs.begin(), futs.end())
    .then([] (std::vector<seastar::future<bool>>&& futures) {
        for (auto& fut : futures) {
            if (fut.failed() || !fut.get0()) {
                return false;
            }
        }

        return true;
    });
}

void CPOService::_handleCompletedAssignment(const String& cname, dto::AssignmentCreateResponse&& request) {
    auto [status, haveCollection] = _getCollection(cname);
    if (!status.is2xxOK()) {
        K2LOG_E(log::cposvr, "unable to find collection which reported assignment {}", cname);
        return;
    }
    for (auto& part: haveCollection.partitionMap.partitions) {
        if (part.keyRangeV == request.assignedPartition.keyRangeV) {
                K2LOG_I(log::cposvr, "Assignment received for active partition {}", request.assignedPartition);
                part.astate = request.assignedPartition.astate;
                part.endpoints = std::move(request.assignedPartition.endpoints);
                _saveCollection(haveCollection);
                return;
        }
    }
    K2LOG_E(log::cposvr, "assignment completion does not match any stored partitions: {}", request.assignedPartition);
}

std::tuple<Status, dto::Collection> CPOService::_getCollection(String name) {
    auto cpath = _getCollectionPath(name);
    std::tuple<Status, dto::Collection> result;
    Payload p;
    if (!fileutil::readFile(p, cpath)) {
        std::get<0>(result) = Statuses::S404_Not_Found("collection not found");
        return result;
    }
    if (!p.read(std::get<1>(result))) {
        std::get<0>(result) = Statuses::S500_Internal_Server_Error("unable to read collection data");
        return result;
    };
    K2LOG_I(log::cposvr, "Found collection in: {}", cpath);
    std::get<0>(result) = Statuses::S200_OK("collection found");

    // Check to see if we need to load schemas from file too
    auto it = schemas.find(name);
    if (it == schemas.end()) {
        Status schemaStatus = _loadSchemas(name);
        if (!schemaStatus.is2xxOK()) {
            std::get<0>(result) = Statuses::S500_Internal_Server_Error("unable to read schema data");
            return result;
        }
    }

    return result;
}

Status CPOService::_loadSchemas(const String& collectionName) {
    auto cpath = _getSchemasPath(collectionName);
    Payload p;
    std::vector<dto::Schema> loadedSchemas;
    if (!fileutil::readFile(p, cpath)) {
        return Statuses::S404_Not_Found("schemas not found");
    }
    if (!p.read(loadedSchemas)) {
        return Statuses::S500_Internal_Server_Error("unable to read schema data");
    }

    schemas[collectionName] = std::move(loadedSchemas);

    return Statuses::S200_OK("Schemas loaded");
}

Status CPOService::_saveCollection(dto::Collection& collection) {
    auto cpath = _getCollectionPath(collection.metadata.name);
    Payload p(Payload::DefaultAllocator(4096));
    p.write(collection);
    if (!fileutil::writeFile(std::move(p), cpath)) {
        return Statuses::S500_Internal_Server_Error("unable to write collection data");
    }

    K2LOG_D(log::cposvr, "saved collection: {}", cpath);
    return _saveSchemas(collection.metadata.name);
}

Status CPOService::_saveSchemas(const String& collectionName) {
    auto cpath = _getSchemasPath(collectionName);
    Payload p(Payload::DefaultAllocator(4096));
    p.write(schemas[collectionName]);
    if (!fileutil::writeFile(std::move(p), cpath)) {
        return Statuses::S500_Internal_Server_Error("unable to write schema data");
    }

    K2LOG_D(log::cposvr, "saved schemas: {}", cpath);
    return Statuses::S201_Created("schema written");
}

seastar::future<std::tuple<Status, dto::PersistenceClusterCreateResponse>>
CPOService::handlePersistenceClusterCreate(dto::PersistenceClusterCreateRequest&& request){
    K2LOG_D(log::cposvr, "Received persistence cluster create request for {}", request.cluster.name);
    auto cpath = _getPersistenceClusterPath(request.cluster.name);
    dto::PersistenceCluster persistenceCluster;
    Payload p;
    if (fileutil::readFile(p, cpath)) {
        return RPCResponse(Statuses::S409_Conflict("persistence cluster already exists"), dto::PersistenceClusterCreateResponse());
    }

    Payload q(Payload::DefaultAllocator(4096));
    q.write(request.cluster);
    if (!fileutil::writeFile(std::move(q), cpath)) {
        return RPCResponse(Statuses::S500_Internal_Server_Error("unable to write persistence cluster data"), dto::PersistenceClusterCreateResponse());
    }
    return RPCResponse(Statuses::S201_Created("persistence cluster creates successfully"), dto::PersistenceClusterCreateResponse());
}

seastar::future<std::tuple<Status, dto::PersistenceClusterGetResponse>>
CPOService::handlePersistenceClusterGet(dto::PersistenceClusterGetRequest&& request) {
    K2LOG_D(log::cposvr, "Received persistence cluster get request with name {}", request.name);
    auto cpath = _getPersistenceClusterPath(request.name);
    dto::PersistenceCluster persistenceCluster;
    Payload p;
    if (!fileutil::readFile(p, cpath)) {
        return RPCResponse(Statuses::S404_Not_Found("persistence cluster not found"), dto::PersistenceClusterGetResponse());
    }
    if (!p.read(persistenceCluster)) {
        return RPCResponse(Statuses::S500_Internal_Server_Error("unable to read persistence cluster data"), dto::PersistenceClusterGetResponse());
    };

    K2LOG_D(log::cposvr, "Found persistence cluster in: {}", cpath);
    dto::PersistenceClusterGetResponse response{.cluster=std::move(persistenceCluster)};
    return RPCResponse(Statuses::S200_OK("persistence cluster found"), std::move(response));
}

// When the metadata manager seals the old plog and use a new plod to persist metadata, this will be called
// persist the old plog sealed offest and the new plog id
// For the first put request, it will receive old_plog_sealed=0, new_plogId = id1 since it has no previous plogs
// For the second put request, it will receive old_plog_sealed=100, new_plogId = id2
// For the third put request, it will receive old_plog_sealed=200, new_plogId = id3
// Then the plog chain will be ([plog_id, sealed_offset]):
// [id1, 100], [id2, 200], [id3, 0]
seastar::future<std::tuple<Status, dto::MetadataPutResponse>>
CPOService::handleMetadataPut(dto::MetadataPutRequest&& request){
    K2LOG_D(log::cposvr, "Received metadata persist request for partition {}", request.partitionName);
    auto records = _metadataRecords.find(request.partitionName);
    if (records == _metadataRecords.end()) {
        std::vector<dto::PartitionMetdataRecord> metadataRecords;
        dto::PartitionMetdataRecord element{.plogId=std::move(request.new_plogId), .sealed_offset=0};
        metadataRecords.push_back(std::move(element));
        _metadataRecords[std::move(request.partitionName)] = std::move(metadataRecords);
    }
    else{
        records->second.back().sealed_offset = request.sealed_offset;
        dto::PartitionMetdataRecord element{.plogId=std::move(request.new_plogId), .sealed_offset=0};
        records->second.push_back(std::move(element));
    }

    return RPCResponse(Statuses::S201_Created("metadata log updates successfully"), dto::MetadataPutResponse{});
}

seastar::future<std::tuple<Status, dto::MetadataGetResponse>>
CPOService::handleMetadataGet(dto::MetadataGetRequest&& request){
    K2LOG_D(log::cposvr, "Received metadata get request for partition {}", request.partitionName);
    auto records = _metadataRecords.find(request.partitionName);
    if (records == _metadataRecords.end()) {
        return RPCResponse(Statuses::S404_Not_Found("request parition's metadata does not exist"), dto::MetadataGetResponse{});
    }

    dto::MetadataGetResponse response{.records=records->second};
    return RPCResponse(Statuses::S200_OK(""), std::move(response));
}


} // namespace k2
