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

namespace k2 {
namespace log {
inline thread_local k2::logging::Logger cposvr("k2::cpo_service");
}
class CPOService {
private:
    typedef std::function<seastar::distributed<CPOService>&()> DistGetter;
    DistGetter _dist;
    ConfigVar<String> _dataDir{"data_dir"};
    String _getCollectionPath(String name);
    String _getPersistenceClusterPath(String clusterName);
    String _getSchemasPath(String collectionName);
    void _assignCollection(dto::Collection& collection);
    seastar::future<bool> _offloadCollection(dto::Collection& collection);
    ConfigDuration _assignTimeout{"assignment_timeout", 10ms};
    ConfigDuration _collectionHeartbeatDeadline{"heartbeat_deadline", 100ms};
    std::unordered_map<String, seastar::future<>> _assignments;
    std::unordered_map<String, std::vector<dto::PartitionMetdataRecord>> _metadataRecords;
    std::tuple<Status, dto::Collection> _getCollection(String name);
    Status _saveCollection(dto::Collection& collection);
    Status _saveSchemas(const String& collectionName);
    Status _loadSchemas(const String& collectionName);
    seastar::future<Status> _pushSchema(const dto::Collection& collection, const dto::Schema& schema);
    void _handleCompletedAssignment(const String& cname, dto::AssignmentCreateResponse&& request);

    // Collection name -> schemas
    std::unordered_map<String, std::vector<dto::Schema>> schemas;

   public:  // application lifespan
    CPOService(DistGetter distGetter);
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
