#pragma once

#include "CollectionMetadata.h"
#include "Common.h"
#include <map>

namespace k2
{
//
//  Plain and simple memory arena. TODO: need to do reasonable allocation or use
//  Facebook Polly
//
class MemoryArena
{
    struct Chunk {
        char *data = nullptr;
        uint32_t used = 0;
        uint32_t size = 0;
    } currentChunk;

    size_t totalAllocated = 0;

    static constexpr size_t chunkSizes[]{
        1024,    4096,    16384,   65536, 262144,
        1048576, 4194304, 16777216}; // TODO: consider "size" variable which
                                     // prefixes each buffer

    void allocNewChunk(size_t size)
    {
        size_t actualSize = size + sizeof(void *);
        size_t chunkSize = 0;
        for (int i = 0; i < sizeof(chunkSizes) / sizeof(chunkSizes[0]); i++) {
            if (chunkSizes[i] >= actualSize) {
                chunkSize = chunkSizes[i];
            }
        }

        if (chunkSize == 0)
            throw std::bad_alloc();

        char *newChunk = new char[chunkSize];
        totalAllocated += chunkSize;
        *((char **)newChunk) = currentChunk.data;
        currentChunk.size = chunkSize;
        currentChunk.used += sizeof(void *);
    }

    static void align8(size_t &size) { size += (size & 7); }

public:
    void *alloc(size_t size)
    {
        align8(size);

        if (currentChunk.data == nullptr ||
            currentChunk.used + size > currentChunk.size)
            allocNewChunk(size);

        return currentChunk.data + currentChunk.used;
    }

    template <typename T, typename... ArgT> T newObject(ArgT &&... arg)
    {
        return new (alloc(sizeof(T))) T(std::forward<ArgT>(arg)...);
    }

    void release()
    {
        char *current = currentChunk.data;
        while (current) {
            char *toDelete = current;
            current = *(char **)current;
            delete[] toDelete;
        }
    }

    ~MemoryArena() { release(); }
};

} //  namespace k2
