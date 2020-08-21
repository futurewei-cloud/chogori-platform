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

    RPC().registerRPCObserver<dto::PartitionGroupCreateRequest, dto::PartitionGroupCreateResponse>(dto::Verbs::CPO_PERSISTENCE_REGISTER, [this](dto::PartitionGroupCreateRequest&& request) {
        return _dist().invoke_on(0, &CPOService::handlePartitionGroupCreate, std::move(request));
    });

    RPC().registerRPCObserver<dto::PartitionMapGetRequest, dto::PartitionMapGetResponse>(dto::Verbs::CPO_PERSISTENCE_GET, [this](dto::PartitionMapGetRequest&& request) {
        return _dist().invoke_on(0, &CPOService::handlePartitionMapGet, std::move(request));
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

String CPOService::_getCollectionPath(String name) {
    return _dataDir() + "/" + name + ".collection";
}

String CPOService::_getPartitionMapPath() {
    return _dataDir() + "/partition_map.txt";
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

seastar::future<std::tuple<Status, dto::PartitionGroupCreateResponse>>
CPOService::handlePartitionGroupCreate(dto::PartitionGroupCreateRequest&& request){
    K2INFO(request.partitionName);

    auto cpath = _getPartitionMapPath();
    std::unordered_map<String, std::vector<String>> partitionMap;
    Payload p;
    if (!fileutil::readFile(p, cpath)) {
        partitionMap.clear();
    }
    else{
        if (!p.read(partitionMap)) {
            return RPCResponse(Statuses::S500_Internal_Server_Error("unable to read partition map data"), dto::PartitionGroupCreateResponse());
        };
    }

    partitionMap[std::move(request.partitionName)] = std::move(request.plogServerEndpoints);

    Payload q([] { return Binary(4096); });
    q.write(partitionMap);
    if (!fileutil::writeFile(std::move(q), cpath)) {
        return RPCResponse(Statuses::S500_Internal_Server_Error("unable to write partition map data"), dto::PartitionGroupCreateResponse());
    }
    return RPCResponse(Statuses::S201_Created("partition group create successfully"), dto::PartitionGroupCreateResponse());
}

seastar::future<std::tuple<Status, dto::PartitionMapGetResponse>>
CPOService::handlePartitionMapGet(dto::PartitionMapGetRequest&& request) {
    K2INFO("Received partition map get request with offset " << request.offset);
    auto cpath = _getPartitionMapPath();
    std::unordered_map<String, std::vector<String>> partitionMap;
    Payload p;
    if (!fileutil::readFile(p, cpath)) {
        return RPCResponse(Statuses::S404_Not_Found("partition map not found"), dto::PartitionMapGetResponse());
    }
    if (!p.read(partitionMap)) {
        return RPCResponse(Statuses::S500_Internal_Server_Error("unable to read partition map data"), dto::PartitionMapGetResponse());
    };

    K2INFO("Found partition map in: " << cpath);
    dto::PartitionMapGetResponse response{.partitionMap=std::move(partitionMap)};
    return RPCResponse(Statuses::S200_OK("partition map found"), std::move(response));
}

} // namespace k2
