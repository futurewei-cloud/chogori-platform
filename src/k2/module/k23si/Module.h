#pragma once
#include <k2/appbase/AppEssentials.h>
#include <k2/dto/Collection.h>
#include <k2/dto/K23SI.h>
#include <k2/indexer/MapIndexer.h>
#include <k2/indexer/HOTIndexer.h>
#include <k2/indexer/UnorderedMapIndexer.h>

#include "ReadCache.h"
#include "TransactionRecord.h"

namespace k2 {

class K23SIPartitionModule {
public:
    K23SIPartitionModule(dto::CollectionMetadata cmeta, dto::Partition partition);
    ~K23SIPartitionModule();

    seastar::future<> stop();
public:
    // verb handlers
    seastar::future<std::tuple<Status, dto::K23SIReadResponse<Payload>>>
    handleRead(dto::K23SIReadRequest&& request);

    seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
    handleWrite(dto::K23SIWriteRequest<Payload>&& request);

    seastar::future<std::tuple<Status, dto::K23SIPushResponse>>
    handlePush(dto::K23SIPushRequest&& request);

private:
    dto::CollectionMetadata _cmeta;
    dto::Partition _partition;
    MapIndexer<Payload> _indexer;
    UnorderedMapIndexer<TransactionRecord> _transactions;
    std::unique_ptr<ReadCache<String, dto::Timestamp>> _readCache;
};

} // ns k2
