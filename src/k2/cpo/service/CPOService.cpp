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

#include "CPOService.h"
#include <k2/transport/Payload.h>  // for payload construction
#include <k2/transport/Status.h>  // for RPC
#include <k2/transport/RPCDispatcher.h>  // for RPC
#include <k2/dto/ControlPlaneOracle.h> // our DTO
#include <k2/dto/AssignmentManager.h> // our DTO
#include <k2/dto/MessageVerbs.h> // our DTO
#include <k2/transport/PayloadFileUtil.h>

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

namespace k2 {

CPOService::CPOService(DistGetter distGetter) : _dist(distGetter) {
    K2INFO("ctor");
}

CPOService::~CPOService() {
    K2INFO("dtor");
}

seastar::future<> CPOService::gracefulStop() {
    K2INFO("stop");
    std::vector<seastar::future<>> futs;
    for (auto& [k,v]: _assignments) {
        futs.push_back(std::move(v));
    }
    _assignments.clear();
    return seastar::when_all_succeed(futs.begin(), futs.end()).discard_result();
}

seastar::future<> CPOService::start() {
    K2INFO("Registering message handlers");
    RPC().registerRPCObserver<dto::CollectionCreateRequest, dto::CollectionCreateResponse>(dto::Verbs::CPO_COLLECTION_CREATE, [this](dto::CollectionCreateRequest&& request) {
        return _dist().invoke_on(0, &CPOService::handleCreate, std::move(request));
    });

    RPC().registerRPCObserver<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET, [this](dto::CollectionGetRequest&& request) {
        return _dist().invoke_on(0, &CPOService::handleGet, std::move(request));
    });

    RPC().registerRPCObserver<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, [this] (dto::CreateSchemaRequest&& request) {
        return _dist().invoke_on(0, &CPOService::handleCreateSchema, std::move(request));
    });

    if (seastar::engine().cpu_id() == 0) {
        // only core 0 handles CPO business
        if (!fileutil::makeDir(_dataDir())) {
            throw std::runtime_error("unable to create data directory");
        }
    }
    return seastar::make_ready_future<>();
}

int makeRangePartitionMap(dto::Collection& collection, const std::vector<String>& eps, const std::vector<String>& rangeEnds) {
    String lastEnd = "";

    if (rangeEnds.size() != eps.size()) {
        K2WARN("Error in client's make collection request: collection endpoints size does not equal rangeEnds size");
        return -1;
    }
    // The partition for a key is found using lower_bound on the start keys, 
    // so it is OK for the last range end key to be ""
    if (rangeEnds[rangeEnds.size()-1] != "") {
        K2WARN("Error in client's make collection request: the last rangeEnd key is not an empty string");
        return -1;
    }

    for (uint64_t i = 0; i < eps.size(); ++i) {
        dto::Partition part {
            .pvid{
                .id = i,
                .rangeVersion=1,
                .assignmentVersion=1
            },
            .startKey=lastEnd,
            .endKey=rangeEnds[i],
            .endpoints={eps[i]},
            .astate=dto::AssignmentState::PendingAssignment
        };

        lastEnd = rangeEnds[i];
        collection.partitionMap.partitions.push_back(std::move(part));
    }

    collection.partitionMap.version++;
    return 0;
}

void makeHashPartitionMap(dto::Collection& collection, const std::vector<String>& eps) {
    const uint64_t max = std::numeric_limits<uint64_t>::max();

    uint64_t partSize = (eps.size() > 0) ? (max / eps.size()) : (max);
    for (uint64_t i =0; i < eps.size(); ++i) {
        uint64_t start = i*partSize;
        uint64_t end = (i == eps.size() -1) ? (max) : ((i+1)*partSize - 1);

        dto::Partition part{
            .pvid{
                .id = i,
                .rangeVersion=1,
                .assignmentVersion=1
            },
            .startKey=std::to_string(start),
            .endKey=std::to_string(end),
            .endpoints={eps[i]},
            .astate=dto::AssignmentState::PendingAssignment
        };
        collection.partitionMap.partitions.push_back(std::move(part));
    }

    collection.partitionMap.version++;
}

seastar::future<std::tuple<Status, dto::CollectionCreateResponse>>
CPOService::handleCreate(dto::CollectionCreateRequest&& request) {
    K2INFO("Received collection create request for " << request.metadata.name);
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
        makeHashPartitionMap(collection, request.clusterEndpoints);
    } 
    else if (collection.metadata.hashScheme == dto::HashScheme::Range) {
        err = makeRangePartitionMap(collection, request.clusterEndpoints, request.rangeEnds);
    }
    else {
        return RPCResponse(Statuses::S403_Forbidden("Unknown hashScheme"), dto::CollectionCreateResponse());
    }

    if (err) {
        return RPCResponse(Statuses::S400_Bad_Request("Bad rangeEnds"), dto::CollectionCreateResponse());
    }

    auto status = _saveCollection(collection);
    if (!status.is2xxOK()) {
        return RPCResponse(std::move(status), dto::CollectionCreateResponse());
    }

    schemas[collection.metadata.name] = std::vector<Schema>();
    lastSchemaID[collection.metadata.name] = 0;

    K2INFO("Created collection: " << cpath);
    _assignCollection(collection);
    return RPCResponse(std::move(status), dto::CollectionCreateResponse());
}

seastar::future<std::tuple<Status, dto::CollectionGetResponse>>
CPOService::handleGet(dto::CollectionGetRequest&& request) {
    K2INFO("Received collection get request for " << request.name);
    auto [status, collection] = _getCollection(request.name);

    dto::CollectionGetResponse response;
    if (status.is2xxOK()) {
        response.collection = std::move(collection);
    }
    return RPCResponse(std::move(status), std::move(response));
}

// Checks if the schema itself is well-formed (e.g. fields and fieldNames sizes match)
// and returns a 400 status if not
Status basicSchemaValidation(const dto::Schema& schema) {
    if (schema.fields.size() != schema.fieldNames.size()) {
        K2WARN("Bad CreateSchemaRequest: fields and fieldNames are not equal size");
        return Statuses::S400_Bad_Request("fields and fieldNames are not equal size");
    }

    if (schema.partitionKeyFields.size() == 0) {
        K2WARN("Bad CreateSchemaRequest: No partitionKeyFields defined");
        return Statuses::S400_Bad_Request("No partitionKeyFields defined");
    }

    uint32_t last = 0;
    for (const KeyFieldDef& keyField : schema.partitionKeyFields) {
        if (last < keyField.index) {
            K2WARN("Bad CreateSchemaRequest: partitionKeyFields not in order");
            return Statuses::S400_Bad_Request("partitionKeyFields not in order");
        }

        if (keyField.index >= schema.fields.size()) {
            K2WARN("Bad CreateSchemaRequest: partitionKeyField index out of bounds");
            return Statuses::S400_Bad_Request("partitionKeyField index out of bounds");
        }

        last = keyField.index;
    }

    last = 0;
    for (const KeyFieldDef& keyField : schema.rangeKeyFields) {
        if (last < keyField.index) {
            K2WARN("Bad CreateSchemaRequest: rangeKeyFields not in order");
            return Statuses::S400_Bad_Request("rangeKeyFields not in order");
        }

        if (keyField.index >= schema.fields.size()) {
            K2WARN("Bad CreateSchemaRequest: rangeKeyField index out of bounds");
            return Statuses::S400_Bad_Request("rangeKeyField index out of bounds");
        }

        last = keyField.index;
    }

    return Statuses::S200_OK("");
}

seastar::future<Status> CPOService::_pushSchema(const dto::Collection& collection, const Schema& schema) {
    std::vector<seastar::future<std::tuple<Status, dto::PushSchemaResponse>>> pushFutures;
            
    for (const dto::Partition& part : collection.partitionMap.partitions) {
        auto endpoint = RPC().getTXEndpoint(*(part.endpoints.begin()));
        dto::K23SIPushSchemaRequest request { schema };

        pushFutures.push_back(PRC().callRPC<dto::K23SIPushSchemaRequest, dto::K23SIPushSchemaResponse>
            (dto::Verbs::K23SI_SCHEMA_PUSH, request, *endpoint, 1s));
    }

    return when_all_succeed(pushFutures.begin(), pushFutures.end())
    .then([] (auto&& doneFutures) {
        for (const auto& doneFuture : doneFutures) {
            auto& [status, k2response] = doneFuture;
            if (!status.is2xxOK()) {
                K2WARN("Failed to push schema update: " << status);
                return seastar::make_ready_future<Status>(status);
            }
        }

        return seastar::make_ready_future<Status>(Statuses::S200_OK(""));
    })
    .handle_exception([](auto exc) {
        K2ERROR_EXC("Failed to push schema update: ", exc);
        return seastar::make_ready_future<Status>(Statuses::S500_Internal_Server_Error("Failed to push schema"));
    });
}

seastar::future<std::tuple<Status, dto::CreateSchemaResponse>>
CPOService::handleCreateSchema(dto::CreateSchemaRequest&& request) {
    K2INFO("Received schema create request for " << request.collectionName << " : " request.schema.name);

    // Start of validation

    Status validation = basicSchemaValidation(request.schema);
    if (!validation.is2xxOK()) {
        return RPCResponse(std::move(validation), dto::CreateSchemaResponse{});
    }

    std::tuple<Status, dto::Collection> getResponse = _getCollection(request.collectionName);
    if (!std::get<0>(getResponse).is2xxOK()) {
        return RPCResponse(std::get<0>(getResponse), dto::CreateSchemaResponse{});
    }
    dto::Collection& collection = std::get<1>(getResponse);

    auto lastSchemaIt = lastSchemaID.find(request.collectionName);
    K2ASSERT(lastSchemaIt != lastSchemaID.end());

    auto lastVersionIt = lastVersionID.find(std::make_pair(request.collectionName, request.schema.name));

    if ((lastVersionIt == lastVersionID.end() && request.schema.version != 1) ||
        (lastVesrionIt != lastVersionID.end() && lastVersionIt->second + 1 != request.schema.version)) {
        K2INFO("Wrong version in CreateSchemaRequest");
        return RPCResponse(Statuses::S403_Forbidden("Expected schema version does not match"), dto::CreateSchemaResponse());
    }

    // End of validation

    uint64_t schemaID = request.schema.id;
    // If new schema, update lastSchemaID and insert new lastVersion
    if (lastVersionIt == lastVersionID.end()) {
        schemaID = lastSchemaIt->second + 1;
        request.schema.id = schemaID;
        lastSchemaID[request.collectionName] = schemaID;
        lastVersionID[std::make_pair(request.collectionName, request.schema.name)] = 1;
    } else {
        lastVersionID[std::make_pair(request.collectionName, request.schema.name)] = request.schema.version;
    }

    schemas[request.collectionName].push_back(std::move(request.schema));
    Status saved = _saveSchemas(request.collectionName);
    if (!saved.is2xxOK()) {
        schemas[request.collectionName].pop_back();
        return RPCResponse(std::move(saved), dto::CreateSchemaResponse{});
    }

    return _pushSchema(collection, schemas[request.collectionName].back())
    .then([schemaID] (Status&& status) {
        return RPCResponse(std::move(status), dto::CreateSchemaResponse{ schemaID });
    });
}

String CPOService::_getCollectionPath(String name) {
    return _dataDir() + "/" + name + ".collection";
}

void CPOService::_assignCollection(dto::Collection& collection) {
    auto &name = collection.metadata.name;
    K2INFO("Assigning collection " << name << ", to " << collection.partitionMap.partitions.size() << " nodes");
    std::vector<seastar::future<>> futs;
    for (auto& part : collection.partitionMap.partitions) {
        if (part.endpoints.size() == 0) {
            K2ERROR("empty endpoint for partition assignment: " << part);
            continue;
        }
        auto ep = *part.endpoints.begin();
        K2INFO("Assigning collection " << name << ", to " << part);
        auto txep = RPC().getTXEndpoint(ep);
        if (!txep) {
            K2WARN("unable to obtain endpoint for " << ep);
            continue;
        }
        dto::AssignmentCreateRequest request;
        request.collectionMeta = collection.metadata;
        request.partition = part;

        K2INFO("Sending assignment for partition: " << request.partition);
        futs.push_back(
        RPC().callRPC<dto::AssignmentCreateRequest, dto::AssignmentCreateResponse>
                (dto::K2_ASSIGNMENT_CREATE, request, *txep, _assignTimeout())
        .then([this, name, ep](auto&& result) {
            auto& [status, resp] = result;
            if (status.is2xxOK()) {
                K2INFO("assignment successful for collection " << name << ", for partition " << resp.assignedPartition);
                _handleCompletedAssignment(name, std::move(resp));
            }
            else {
                // The node refused to accept the assignment. For now, just ignore this
                K2WARN("assignment for collection " << name << " was refused by " << ep << ", due to: " << status);
            }
            return seastar::make_ready_future();
        })
        );
    }
    _assignments.emplace(name, seastar::when_all_succeed(futs.begin(), futs.end()).discard_result()
        .then([this, name] {
            _assignments.erase(name);
            return seastar::make_ready_future();
        }));
}

void CPOService::_handleCompletedAssignment(const String& cname, dto::AssignmentCreateResponse&& request) {
    auto [status, haveCollection] = _getCollection(cname);
    if (!status.is2xxOK()) {
        K2ERROR("unable to find collection which reported assignment " << cname);
        return;
    }
    for (auto& part: haveCollection.partitionMap.partitions) {
        if (part.startKey == request.assignedPartition.startKey &&
            part.endKey == request.assignedPartition.endKey &&
            part.pvid.id == request.assignedPartition.pvid.id &&
            part.pvid.assignmentVersion == request.assignedPartition.pvid.assignmentVersion &&
            part.pvid.rangeVersion == request.assignedPartition.pvid.rangeVersion) {
                K2INFO("Assignment received for active partition " << request.assignedPartition);
                part.astate = request.assignedPartition.astate;
                part.endpoints = std::move(request.assignedPartition.endpoints);
                _saveCollection(haveCollection);
                return;
        }
    }
    K2ERROR("assignment completion does not match any stored partitions: " << request.assignedPartition);
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
    K2INFO("Found collection in: " << cpath);
    std::get<0>(result) = Statuses::S200_OK("collection found");

    // TODO read and save schemas

    return result;
}

Status CPOService::_saveCollection(dto::Collection& collection) {
    auto cpath = _getCollectionPath(collection.metadata.name);
    Payload p([] { return Binary(4096); });
    p.write(collection);
    if (!fileutil::writeFile(std::move(p), cpath)) {
        return Statuses::S500_Internal_Server_Error("unable to write collection data");
    }

    K2DEBUG("saved collection: " << cpath);
    return Statuses::S201_Created("collection created");
}

Status CPOService::_saveSchemas(const String& collectionName) {
    auto cpath = _getCollectionPath(collectionName) + ".schemas";
    Payload p([] { return Binary(4096); });
    p.write(schemas[collection.metadata.name]);
    if (!fileutil::writeFile(std::move(p), cpath)) {
        return Statuses::S500_Internal_Server_Error("unable to write schema data");
    }

    K2DEBUG("saved schemas: " << cpath);
    return Statuses::S201_Created("schema written");
}

} // namespace k2
