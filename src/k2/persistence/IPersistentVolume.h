#pragma once

#include <k2/common/Common.h>
#include <k2/common/plog/IPlog.h>

namespace k2
{

typedef uint32_t ChunkId;

struct RecordPosition
{
    ChunkId chunkId;
    uint32_t offset;
};

struct ChunkInfo
{
    ChunkId chunkId;
    uint32_t size;
    uint32_t actualSize;
    PlogId plogId;

    ChunkInfo() : chunkId(0), size(0), actualSize(0) {}

    ChunkInfo(const PlogId& _plogId) : chunkId(0), size(0), actualSize(0)
    {
        memcpy(plogId.id, _plogId.id, PLOG_ID_LEN);
    }
};


//
//  General iterator
//
template<typename ItemT>
class IIterator
{
public:
    //
    //  Return true, if there are no more record available
    //
    virtual bool isEnd() const = 0;

    //
    //  Return current item
    //
    virtual ItemT getCurrent() const = 0;

    //
    //  Compare iterator positions
    //
    virtual bool equal(const IIterator* it) const = 0;

    //
    //  Move to the next item
    //
    virtual bool advance() = 0;

    //
    //  Destructor
    //
    virtual ~IIterator() {}

    bool isValid() const { return !isEnd(); }

    ItemT getCurrentAndMoveNext()
    {
        ItemT item = getCurrent();
        advance();
        return item;
    }
};

//
//  Persistent volume represent set of append-only chunks, which forms a data volume.
//  Data can be accessed by chunk id and offset within a chunk.
//
class IPersistentVolume
{
public:
    class IIterator : public k2::IIterator<ChunkInfo> { };

    virtual seastar::future<RecordPosition> append(Binary binary) = 0;
    virtual seastar::future<uint32_t> read(const RecordPosition& position, const uint32_t sizeToRead, Binary& buffer) = 0;
    virtual ChunkInfo getInfo(ChunkId chunkId) = 0;
    virtual ChunkInfo setUsage(ChunkId chunkId, uint32_t usage) = 0;
    virtual seastar::future<> drop(ChunkId chunkId) = 0;
    virtual uint64_t totalUsage() = 0;
    virtual uint64_t totalSize() = 0;
    virtual std::unique_ptr<IIterator> getChunks() = 0;
    virtual seastar::future<> close() = 0;
    virtual ~IPersistentVolume() {}
};

}   //  namespace k2
