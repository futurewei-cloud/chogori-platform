#include "PersistentVolume.h"

namespace k2
{

enum class LogType : uint8_t
{
    Add,
    Remove
};

struct EntryRecord
{
    LogType logType;
    PlogId plogId;

    K2_PAYLOAD_COPYABLE;
};

class EntryService
{
protected:
    std::vector<PlogId> plogId;
    PlogId currentPlog;
    std::shared_ptr<PersistentVolume> volume;
public:
    EntryService(std::vector<PlogId> plogId) : plogId(std::move(plogId)) { }

    IOResult<std::vector<EntryRecord>> loadRecords()
    {
        return seastar::make_ready_future<std::vector<EntryRecord>>(std::vector<EntryRecord>());
    }

    IOResult<> logRecord(EntryRecord)
    {
        return seastar::make_ready_future<>();
    }
};

const uint32_t MaxPlogSize = 32*1024;

IOResult<RecordPosition> PersistentVolume::append(Binary binary)
{
    auto appendSize = binary.size();
    return [appendSize, this] {
        if(m_chunkList.empty())
        {
            // chunk set is empty in persistent volume, need a new chunk to append
            return addNewChunk();
        }else {
            // retrieve last chunk information
            auto& plogId = m_chunkList.back().plogId;
            return m_plog->getInfo(plogId)
            .then([&plogId, appendSize, this](auto plogInfo){
                if(plogInfo.sealed || plogInfo.size+appendSize > MaxPlogSize)
                    // current chunk doesn't have enough space, need a new chunk to append
                    return m_plog->seal(plogId).then([this]{ return addNewChunk(); });
                else
                    // current chunk has enough space to append
                    return seastar::make_ready_future<>();
            });
        }
    }()
    .then([appendSize, binary{std::move(binary)}, this] () mutable {
        if(appendSize+plogInfoSize > MaxPlogSize)
        {
            // the binary buffer is too large to append to plog, throw a ChunkException.
            auto msg = "Append buffer[" + std::to_string(binary.size())
                       + "] exceeds the limit of chunk capability: " + std::to_string(MaxPlogSize-plogInfoSize);
            return seastar::make_exception_future<uint32_t>(ChunkException(msg, m_chunkList.back().chunkId));
        }else {
            // write the binary buffer to plog
            std::vector<Binary> bufferList;
            bufferList.push_back(std::move(binary));
            return m_plog->append(m_chunkList.back().plogId, std::move(bufferList));
        }
    })
    .then([appendSize, this](auto offset) {
        // update chunk Information
        auto& chunkInfo = m_chunkList.back();
        chunkInfo.size += appendSize;
        chunkInfo.actualSize += appendSize;
        return seastar::make_ready_future<RecordPosition>(RecordPosition{chunkInfo.chunkId, offset});
    });
}


IOResult<uint32_t> PersistentVolume::read(const RecordPosition& position, const uint32_t sizeToRead, Binary& buffer)
{
    ChunkInfo chunkInfo;

    try{
        // try to retrieve chunk information for given chunk id
        chunkInfo = getInfo(position.chunkId);
    }catch(ChunkException& e){
        // failure to retrieve  chunk information, rethrow the ChunkException
        return seastar::make_exception_future<uint32_t>(e);
    }

    // determine the actual size to read
    auto readSize = std::min(sizeToRead, plogInfoSize+chunkInfo.size-position.offset);
    IPlog::ReadRegions plogDataToReadList;
    plogDataToReadList.push_back(IPlog::ReadRegion{position.offset, readSize});
    // read records to buffer
    return m_plog->read(chunkInfo.plogId, std::move(plogDataToReadList))
    .then([&buffer](auto readRegions) {
        buffer = std::move(readRegions[0].buffer);
        return seastar::make_ready_future<uint32_t>(buffer.size());
    });
}


ChunkInfo PersistentVolume::getInfo(ChunkId chunkId)
{
    int i = std::min((size_t)chunkId, m_chunkList.size()-1);
    for( ; i>=0 && (m_chunkList[i].chunkId != chunkId); i--);

    if(i<0)
    {
        auto msg = "ChunkId: " + std::to_string(chunkId) + " not found.";
        throw ChunkException(msg, chunkId);
    }else {
        return m_chunkList[i];
    }
}


ChunkInfo PersistentVolume::setUsage(ChunkId chunkId, uint32_t usage)
{
    int i = std::min((size_t)chunkId, m_chunkList.size()-1);
    for( ; i>=0 && (m_chunkList[i].chunkId != chunkId); i--);

    if(i<0)
    {
        auto msg = "ChunkId: " + std::to_string(chunkId) + " not found.";
        throw ChunkException(msg, chunkId);
    }else {
        m_chunkList[i].actualSize = usage;
        return m_chunkList[i];
    }
}


IOResult<> PersistentVolume::drop(ChunkId chunkId)
{
    int i = std::min((size_t)chunkId, m_chunkList.size()-1);
    for( ; i>=0 && (m_chunkList[i].chunkId != chunkId); i--);

    if(i<0)
    {
        auto msg = "ChunkId: " + std::to_string(chunkId) + " not found.";
        return seastar::make_exception_future<>(ChunkException(msg, chunkId));
    } else {
        // drop corresponding plog Id
        return m_plog->drop(m_chunkList[i].plogId)
        .then([i, this]() {
            // drop this chunk from chunk set
            m_chunkList.erase(m_chunkList.begin()+i);
            return seastar::make_ready_future<>();
        });
    }
}


uint64_t PersistentVolume::totalUsage()
{
    return std::accumulate(m_chunkList.begin(), m_chunkList.end(), 0, [](uint64_t sum, ChunkInfo& chunkInfo){
        return sum + chunkInfo.actualSize;
    });
 }


uint64_t PersistentVolume::totalSize()
{
    return std::accumulate(m_chunkList.begin(), m_chunkList.end(), 0, [](uint64_t sum, ChunkInfo& chunkInfo){
        return sum + chunkInfo.size;
    });
}

std::unique_ptr<IPersistentVolume::IIterator> PersistentVolume::getChunks()
{
    return std::make_unique<PersistentVolume::_Iterator>(*this);
}

IOResult<> PersistentVolume::addNewChunk()
{
    return m_plog->create(1)
    .then([this](auto plogIds){
        ChunkInfo chunkInfo{plogIds[0]};
        chunkInfo.chunkId = m_chunkList.size()==0 ? 1 : (m_chunkList.back().chunkId + 1);
        m_chunkList.push_back(std::move(chunkInfo));

        return seastar::make_ready_future<>();
    });
}

IOResult<std::unique_ptr<IPersistentVolume>> PersistentVolume::open(std::shared_ptr<IPlog> plogService, std::vector<PlogId> entryPlogs)
{
    std::unique_ptr<PersistentVolume> volume(new PersistentVolume(plogService));
    volume->entryService = std::make_unique<EntryService>(std::move(entryPlogs));
    return volume->entryService->loadRecords().then([volume = std::move(volume)](std::vector<EntryRecord> records) mutable
        {
            for(auto& record : records)
            {
                ChunkInfo chunkInfo;
                chunkInfo.plogId = record.plogId;   //  TODO: set size
                volume->m_chunkList.push_back(std::move(chunkInfo));
            }

            return std::unique_ptr<IPersistentVolume>(volume.release());
        });
}

PersistentVolume::PersistentVolume() {}

PersistentVolume::~PersistentVolume() {}

PersistentVolume::PersistentVolume(std::shared_ptr<IPlog> plog) : m_plog(plog) {}

PersistentVolume::PersistentVolume(String plogPath) : PersistentVolume(std::make_shared<PlogMock>(std::move(plogPath))) {}

}   //  namespace k2
