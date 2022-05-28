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

#pragma once

// third-party
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>  // for future stuff

#include <k2/appbase/AppEssentials.h>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/AssignmentManager.h>
#include <k2/dto/PersistenceCluster.h>
#include <k2/dto/LogStream.h>
#include <k2/transport/Status.h>

#include "Log.h"
#include "HealthMonitor.h"

namespace k2::cpo {

// Used by the CPOService class to track mapping of nodes to assigned collections
class NodeAssignmentEntry {
public:
    String collection;
    bool assigned{false};
};

class CPOService {
private:
    ConfigVar<String> _dataDir{"data_dir"};
    ConfigVar<uint32_t> _heartbeatMonitorShardId{"heartbeat_monitor_shard_id"};
    ConfigDuration _TSOErrorBound{"tso_error_bound", 20us};
    SingleTimer _tsoAssignTimer;
    std::vector<String> _healthyTSOs;
    String _getCollectionPath(String name);
    String _getPersistenceClusterPath(String clusterName);
    String _getSchemasPath(String collectionName);
    void _assignCollection(dto::Collection& collection);
    seastar::future<bool> _offloadCollection(dto::Collection& collection);
    ConfigDuration _assignTimeout{"assignment_timeout", 100ms};
    ConfigDuration _collectionHeartbeatDeadline{"txn_heartbeat_deadline", 100ms};
    ConfigVar<int> _maxAssignRetries{"max_assign_retries", 3};

    std::unordered_map<String, seastar::future<>> _assignments;
    std::unordered_map<String, std::vector<dto::PartitionMetdataRecord>> _metadataRecords;
    std::map<String, NodeAssignmentEntry> _nodesToCollection;
    std::tuple<Status, dto::Collection> _getCollection(String name);
    Status _saveCollection(dto::Collection& collection);
    Status _saveSchemas(const String& collectionName);
    Status _loadSchemas(const String& collectionName);
    seastar::future<Status> _pushSchema(const dto::Collection& collection, const dto::Schema& schema);
    void _handleCompletedAssignment(const String& cname, dto::AssignmentCreateResponse&& request);
    seastar::future<> _getNodes(); // Get list of nodes from health monitor
    String _assignToFreeNode(String collection);
    int _makeHashPartitionMap(dto::Collection& collection, uint32_t numNodes);
    int _makeRangePartitionMap(dto::Collection& collection, const std::vector<String>& rangeEnds);
    seastar::future<> _assignAllTSOs();
    seastar::future<> _assignTSO(String &ep, size_t tsoID, int retry);
    // Collection name -> schemas
    std::unordered_map<String, std::vector<dto::Schema>> schemas;

   public:  // application lifespan
    CPOService();
    ~CPOService();

    // required for seastar::distributed interface
    seastar::future<> gracefulStop();
    seastar::future<> start();

    seastar::future<std::tuple<Status, dto::CollectionCreateResponse>>
    handleCreate(dto::CollectionCreateRequest&& request);

    seastar::future<std::tuple<Status, dto::CollectionGetResponse>>
    handleGet(dto::CollectionGetRequest&& request);

    seastar::future<std::tuple<Status, dto::CollectionDropResponse>>
    handleCollectionDrop(dto::CollectionDropRequest&& request);

    seastar::future<std::tuple<Status, dto::PersistenceClusterCreateResponse>>
    handlePersistenceClusterCreate(dto::PersistenceClusterCreateRequest&& request);

    seastar::future<std::tuple<Status, dto::PersistenceClusterGetResponse>>
    handlePersistenceClusterGet(dto::PersistenceClusterGetRequest&& request);

    seastar::future<std::tuple<Status, dto::CreateSchemaResponse>>
    handleCreateSchema(dto::CreateSchemaRequest&& request);

    seastar::future<std::tuple<Status, dto::GetSchemasResponse>>
    handleSchemasGet(dto::GetSchemasRequest&& request);

    seastar::future<std::tuple<Status, dto::MetadataPutResponse>>
    handleMetadataPut(dto::MetadataPutRequest&& request);

    seastar::future<std::tuple<Status, dto::MetadataGetResponse>>
    handleMetadataGet(dto::MetadataGetRequest&& request);
};  // class CPOService

} // namespace k2
