#pragma once

#include <algorithm>
#include <numeric>
#include "common/plog/PlogMock.h"
#include "IPersistentVolume.h"


#ifdef EXPOSE_PRIVATES
#define PRIVATE public
#else
#define PRIVATE private
#endif

namespace k2
{
class ChunkException : public std::exception {
public:
    virtual const char* what() const noexcept {
        return  _msg.c_str();
    }

    ChunkException(const String& msg, ChunkId chunkId)
            : _msg(msg), _chunkId(chunkId) {
    }

    virtual const std::string& str() const {
        return _msg;
    }
    ChunkId chunkId() const {
        return _chunkId;
    }

private:
    std::string  _msg;
    ChunkId _chunkId;
};

class PersistentVolume : public IPersistentVolume
{
public:
    // Iteration interface for Chunk set
    class Iterator : public IPersistentVolume::IIterator
    {
    public:
        // return true if current iterator doesn't reach the end of the Chunk set
        bool next() override { 
            return m_chunkIndex < parent.m_chunkList.size(); 
        }

        // return current chunk information
        ChunkInfo getCurrent() override {
            if(next()){
                return parent.m_chunkList[m_chunkIndex++];
            }else{
                return ChunkInfo{};
            }
        }

        Iterator(PersistentVolume& x): parent(x), m_chunkIndex(0) {}

        ~Iterator(){}
    
    PRIVATE:
        PersistentVolume& parent;
        size_t m_chunkIndex;
        friend PersistentVolume;
    };

    /**
     * append a binary data to chunck
     * binary - the Binary buffer to append
     * return - a Record Position
     * Exception - throw an ChunkException if the size of binary is too large to append a chunk.
     */
    IOResult<RecordPosition> append(Binary binary) override;

    /**
     * read a binary data to buffer from given chunk
     * position - indicates the chunk Id and offset to read
     * sizeToRead - expected size to read
     * buffer - to store the reading data
     * return - the actually size read from the chunk
     * Exception - throw a ChunkException if the chunk Id doesn't exist.
     */
    IOResult<uint32_t> read(const RecordPosition& position, const uint32_t sizeToRead, Binary& buffer) override;

    /**
     * retrieve chunk information for given chunk Id
     * chunkId - indicates the chunk to retrieve
     * return - the chunk information
     * Exception - throw a ChunkException if the chunk Id doesn't exist.
     */
    ChunkInfo getInfo(ChunkId chunkId) override;

    /**
     * decrease the actual size of a chunk for given chunk Id
     * chunkId - indicates the chunk to decrease the usage size
     * usage - the size to decrease
     * return - the chunk information after usage size is decreased
     * Exception - throw a ChunkException if the chunk Id doesn't exist.
     */
    ChunkInfo decreaseUsage(ChunkId chunkId, uint32_t usage) override;

    /**
     * drop a chunk for given chunk Id, and drop the corresponding plog file also
     * chunkId - indicates the chunk to decrease the usage size
     * return - no return
     * Exception - throw a ChunkException if the chunk Id doesn't exist.
     */
    IOResult<> drop(ChunkId chunkId) override;

    // accumulate the total actual size in chunk set
    uint64_t totalUsage() override;

    // accumulate the total size in chunk set
    uint64_t totalSize() override;

    // set the iterator point to first chunk, and return the iterator.
    std::unique_ptr<IIterator> getChunks() override;

    PersistentVolume(String plogPath = String{std::filesystem::current_path().concat("/plogs/")});

    ~PersistentVolume(); 

PRIVATE:
    // append a new chunk to the chunk set, the chunk id is monotonically increased,
    IOResult<> addNewChunk();

PRIVATE:
    std::vector<ChunkInfo> m_chunkList;
    seastar::lw_shared_ptr<k2::PlogMock> m_plog;
    Iterator iterator;

};   //  class PersistentVolume


}   //  namespace k2