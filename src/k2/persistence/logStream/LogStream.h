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

#pragma once

#include <k2/transport/PayloadSerialization.h>
#include <seastar/core/sharded.hh>
#include <k2/transport/Payload.h>
#include <k2/transport/Status.h>
#include <k2/common/Common.h>
#include <k2/config/Config.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/transport/BaseTypes.h>
#include <k2/transport/TXEndpoint.h>
#include <k2/persistence/plog_client/PlogClient.h>


namespace k2 {

namespace log {
inline thread_local k2::logging::Logger lgbase("k2::logstream_base");
}

K2_DEF_ENUM(LogStreamType,
    WAL,
    IndexerSnapshot,
    Aux);

// This is a base class that provide common plog operations for both logstream and metadata manager.
// Provide public APIs: append_data_to_plogs, read_data_from_plogs
// Store the used plog informations of a log stream
// This class is a base class, it should not be used by users
class LogStreamBase{
private:
    // to store the information of each plog id used by the log stream
    // only used internally
    struct _PlogInfo {
        uint32_t currentOffset;
        bool sealed;
        String nextPlogId;
    };
public:
    LogStreamBase();
    ~LogStreamBase();

    // write data to the the current plog, return the latest Plog ID and latest offset
    seastar::future<std::tuple<Status, dto::AppendResponse> > append_data_to_plogs(dto::AppendRequest request);

    // write data to the the current plog with the start offset, return the latest Plog ID and latest offset
    seastar::future<std::tuple<Status, dto::AppendResponse> > append_data_to_plogs(dto::AppendWithIdAndOffsetRequest request);

    // read the data from the log stream
    seastar::future<std::tuple<Status, dto::ReadResponse> > read_data_from_plogs(dto::ReadRequest request);

    // read with continuation
    seastar::future<std::tuple<Status, dto::ReadResponse> > read_data_from_plogs(dto::ReadWithTokenRequest request);

    // rebuild the _usedPlogInfo, _firstPlogId, _currentPlogId
    // TODO: This function will only be called internally, will move to protected later
    seastar::future<Status> _reload(std::vector<dto::PartitionMetdataRecord> plogsOfTheStream);

    // obtain the target plog status
    // TODO: This function will only be called internally, will move to protected later
    seastar::future<std::tuple<Status, dto::PlogGetStatusResponse>> _getPlogStatus(dto::PlogGetStatusRequest request);
private:
    // Record the fisrt Plog Id and the current used Plog Id of a Logstream
    String _firstPlogId, _currentPlogId, _preallocatedPlogId;

    // Plog Client Instance
    PlogClient _client;

    // the maximum size of each plog
    constexpr static uint32_t PLOG_MAX_SIZE = 16 * 1024 * 1024;

    // the maximun bytes a read command could read
    constexpr static uint32_t PLOG_MAX_READ_SIZE = 2*1024*1024;

    // whether this log stream base has been _initialized
    bool _initialized = false;

    // The map to store the used plog information
    std::unordered_map<String, _PlogInfo> _usedPlogInfo;

    // whether the logstream is switching the plog
    bool _switchingInProgress;

    std::vector<seastar::promise<>> _switchRequestWaiters;

    // a virtual API that used to persist the Plog Id and sealed offset of each used plog
    // For Metadata Manager: persist these information to CPO
    // For Logstream: persist these information to Metadata Manager
    virtual seastar::future<Status> _addNewPlog(uint32_t sealedOffset, String newPlogId)=0;

    // when exceed the size limit of current plog, we need to
    // 1. seal the current plog
    // 2. write the contents to the new plog
    // 3. persist the sealed offset of the old plog and the new Plog Id
    // 4. create a new preallocate plog
    // All the steps can do in parallel
    seastar::future<std::tuple<Status, dto::AppendResponse> > _switchPlogAndAppend(Payload payload);

protected:
     // preallocate a plog. So when we switch a plog, we can switch to this preallocated plog immediately instead of waiting for creating a new one
    seastar::future<Status> _preallocatePlog();

    // init the plog client client
    seastar::future<> _initPlogClient(String persistenceClusterName);

    // create a plog as the current used plog, and persist its PlogId
    seastar::future<Status> _activeAndPersistTheFirstPlog();

};

class PartitionMetadataMgr;

// provided unique operations for logstream.
// A logstream class owns a pointer to the metadata manager
// When a logstream persist its own metadata, it will persist these information to metadata manager
// TODO: Test the performance of the Inheritance
class LogStream:public LogStreamBase{
public:
    LogStream();
    ~LogStream();

    // set the name of this log stream and the meta data manager pointer
    // Should only be called by PartitionMetadataMgr
    seastar::future<Status> init(LogStreamType name, std::shared_ptr<PartitionMetadataMgr> metadataMgr, String persistenceClusterName, bool reload);
private:
    // the name of this log stream, such as "WAL", "IndexerSnapshot", "Aux", etc
    LogStreamType _name;

    // the pointer to the metadata manager
    // instead of raw pointer, using shared pointer
    std::shared_ptr<PartitionMetadataMgr> _metadataMgr;

    // persist metadata to Metadata Manager
    virtual seastar::future<Status> _addNewPlog(uint32_t sealedOffset, String newPlogId);
};

// provided unique operations for metadata manager
// A metadata manager manages all the logstreams in a partition, and persist the metadata of all these logstreams
// When a metadata manager persist its own metadata, it will persist these information to CPO
// TODO: Test the performance of the Inheritance
class PartitionMetadataMgr: public std::enable_shared_from_this<PartitionMetadataMgr>, public LogStreamBase {

public:
    PartitionMetadataMgr();
    ~PartitionMetadataMgr();

    // set the partition name, initialize all the log streams this metadata mgr used
    seastar::future<Status> init(String cpoUrl, String partitionName, String persistenceClusterName);

    // handle the persistence requests from all the logstreams it manages
    seastar::future<Status> addNewPLogIntoLogStream(LogStreamType name, uint32_t sealed_offset, String new_plogId);

    // return the request logstream pointer
    std::tuple<Status, std::shared_ptr<LogStream>> obtainLogStream(LogStreamType log_stream_name);

private:
    // a map to store all the log streams managed by this metadata manager
    // instead of raw pointer, using shared pointer
    std::unordered_map<LogStreamType, std::shared_ptr<LogStream>> _logStreamMap;
    CPOClient _cpo;
    String _partitionName;
    ConfigDuration _cpo_timeout {"cpo_timeout", 1s};

    // persist metadata to CPO
    virtual seastar::future<Status> _addNewPlog(uint32_t sealeOffset, String newPlogId);

    // replay the entire Metadata Manager
    // When we do replay, we need to to the following steps:
    // 1. reload the _usedPlogInfo, _firstPlogId, _currentPlogId for metadata manager itself
    // 2. retrive the metadata of all the logstreams from metadata plogs
    // 3. reload the _usedPlogInfo, _firstPlogId, _currentPlogId for all the logstreams
    // we have two helpers functions for some steps.
    // step 1 is implemented in this function
    // step 2 is implemented in _readMetadataPlogs
    // step 3 is implemented _reloadLogStreams
    seastar::future<Status> _replay(std::vector<dto::PartitionMetdataRecord> records);

    // helper method for replay
    // retrive the metadata of all the logstreams from metadata plogs
    seastar::future<Status> _readMetadataPlogs(std::vector<dto::PartitionMetdataRecord> records, uint32_t read_size);

    // helper method for replay
    // reload the _usedPlogInfo, _firstPlogId, _currentPlogId for all the logstreams
    seastar::future<Status> _reloadLogStreams(Payload payload);
};

} // k2
