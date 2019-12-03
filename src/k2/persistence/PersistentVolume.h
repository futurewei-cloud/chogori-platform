#pragma once

#include <k2/common/plog/PlogMock.h>
#include <algorithm>
#include <numeric>
#include "IPersistentVolume.h"

#ifdef EXPOSE_PRIVATES
#define PRIVATE public
#define PROTECTED public
#else
#define PRIVATE private
#define PROTECTED protected
#endif

namespace k2
{
class ChunkException : public std::exception {
public:
    virtual const char* what() const noexcept { return  _msg.c_str(); }

    ChunkException(const String& msg, ChunkId chunkId) : _msg(msg), _chunkId(chunkId) { }

    virtual const String& str() const { return _msg; }

    ChunkId chunkId() const { return _chunkId; }
private:
    String _msg;
    ChunkId _chunkId;
};

class EntryService;

class PersistentVolume : public IPersistentVolume
{
PROTECTED:
    std::vector<ChunkInfo> m_chunkList; //  TODO: this need to be map to LinkedList nodes
    std::shared_ptr<IPlog> m_plog;
    std::unique_ptr<EntryService> entryService;

    class _Iterator : public IPersistentVolume::IIterator
    {
        const PersistentVolume& parent;
        size_t m_chunkIndex;

        bool _isEnd() const;

    public:
        bool isEnd() const override;

        ChunkInfo getCurrent() const override;

        bool equal(const k2::IIterator<ChunkInfo>* it) const override;

        bool advance() override;

        _Iterator(const PersistentVolume& x);

        friend PersistentVolume;
    };

    // append a new chunk to the chunk set, the chunk id is monotonically increased,
    seastar::future<> addNewChunk();

    PersistentVolume();

    PersistentVolume(std::shared_ptr<IPlog> plog);

    PersistentVolume(String plogPath);

    seastar::future<> close() {return m_plog->close();};

    DISABLE_COPY_MOVE(PersistentVolume);

public:
    //
    // append a binary data to chunk
    // binary - the Binary buffer to append
    // return - a Record Position
    // Exception - throw an ChunkException if the size of binary is too large to append a chunk.
    //
    seastar::future<RecordPosition> append(Binary binary) override;

    //
    // read a binary data to buffer from given chunk
    // position - indicates the chunk Id and offset to read
    // sizeToRead - expected size to read
    // buffer - to store the reading data
    // return - the actually size read from the chunk
    // Exception - throw a ChunkException if the chunk Id doesn't exist.
    //
    seastar::future<uint32_t> read(const RecordPosition& position, const uint32_t sizeToRead, Binary& buffer) override;

    //
    // retrieve chunk information for given chunk Id
    // chunkId - indicates the chunk to retrieve
    // return - the chunk information
    // Exception - throw a ChunkException if the chunk Id doesn't exist.
    //
    ChunkInfo getInfo(ChunkId chunkId) override;

    //
    // decrease the actual size of a chunk for given chunk Id
    // chunkId - indicates the chunk to decrease the usage size
    // usage - the size to decrease
    // return - the chunk information after usage size is decreased
    // Exception - throw a ChunkException if the chunk Id doesn't exist.
    //
    ChunkInfo setUsage(ChunkId chunkId, uint32_t usage) override;

    //
    // drop a chunk for given chunk Id, and drop the corresponding plog file also
    // chunkId - indicates the chunk to decrease the usage size
    // return - no return
    // Exception - throw a ChunkException if the chunk Id doesn't exist.
    //
    seastar::future<> drop(ChunkId chunkId) override;

    // accumulate the total actual size in chunk set
    uint64_t totalUsage() override;

    // accumulate the total size in chunk set
    uint64_t totalSize() override;

    // set the iterator point to first chunk, and return the iterator.
    std::unique_ptr<IIterator> getChunks() override;

    //
    //  Open volume
    //      plogService - plog interface implementation
    //      entryPlogs - plogs to store metadata. TODO: replace with PlogEntry Service
    //
    static seastar::future<std::shared_ptr<PersistentVolume>> open(std::shared_ptr<IPlog> plogService, std::vector<PlogId> entryPlogs);

    static seastar::future<std::shared_ptr<PersistentVolume>> create(std::shared_ptr<IPlog> plogService);

    ~PersistentVolume();

};   //  class PersistentVolume


}   //  namespace k2
