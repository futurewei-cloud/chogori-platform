#pragma once

#include "../../common/Common.h"
#include "../../common/plog/IPlog.h"

namespace k2
{

typedef uint32_t ChunkId;

struct RecordPosition {
    ChunkId chunkId;
    uint32_t offset;
};

struct ChunkInfo {
    ChunkId chunkId;
    uint32_t size;
    uint32_t actualSize;
};

//
//  Persistent volume represent set of append-only chunks, which forms a data
//  volume. Data can be accessed by chunk id and offset within a chunk.
//
class IPersistentVolume
{
public:
    class IIterator
    {
    public:
        virtual bool next() = 0;
        virtual ChunkInfo getCurrent() = 0;
    };

    virtual IOResult<RecordPosition> append(Binary binary) = 0;
    virtual IOResult<Binary> read(const RecordPosition &position) = 0;
    virtual ChunkInfo getInfo(ChunkId chunkId) = 0;
    virtual ChunkInfo decreaseUsage(ChunkId chunkId, uint32_t usage) = 0;
    virtual IOResult<void> drop(ChunkId chunkId) = 0;
    virtual uint64_t totalUsage() = 0;
    virtual uint64_t totalSize() = 0;
    virtual std::unique_ptr<IIterator> getChunks() = 0;
    virtual ~IPersistentVolume() = 0;
};

} //  namespace k2
