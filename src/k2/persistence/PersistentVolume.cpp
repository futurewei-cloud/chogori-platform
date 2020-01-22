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
    std::shared_ptr<IPlog> plogService;
    std::vector<PlogId> plogs;
    std::vector<size_t> sizes;
    size_t activePlog = 0;
    size_t maxPlogSize = 16*1024;

    bool parse(Payload& payload, std::vector<EntryRecord>& records)
    {
        payload.seek(0);
        return payload.read(records);
    }

    seastar::future<> append(Binary& bin)
    {
        if(activePlog >= plogs.size())
            return seastar::make_exception_future<>(PlogException("", P_CAPACITY_NOT_ENOUGH));

        if(sizes[activePlog] + bin.size() > maxPlogSize)
        {
            activePlog++;
            return append(bin);
        }

        return plogService->append(plogs[activePlog], binaryReference(bin.get_write(), bin.size()))
            .discard_result()
            .handle_exception([this, &bin](std::exception_ptr)
            {
                activePlog++;   //  Plog failure - switch to next one
                return append(bin);
            })
            .then([this, size = bin.size()]
            {
                sizes[activePlog] += size;
                return seastar::make_ready_future<>();
            });
    }

    seastar::future<> log(const void* data, size_t size)
    {
        return seastar::do_with(Binary((const char*)data, size), [this](Binary& bin)
        {
            return append(bin);
        });
    }

public:
    EntryService(std::shared_ptr<IPlog> plogService, std::vector<PlogId> plogs) : plogService(plogService), plogs(std::move(plogs)) { }

    seastar::future<std::vector<EntryRecord>> init()
    {
        std::vector<seastar::future<Payload>> loadPlogFutures;
        activePlog = 0;
        for(const PlogId& plogId : plogs)
            loadPlogFutures.push_back(plogService->readAll(plogId));

        return seastar::when_all_succeed(loadPlogFutures.begin(), loadPlogFutures.end())
            .then([this](std::vector<Payload> payloads) mutable
            {
                std::vector<EntryRecord> records;
                for(size_t i = 0; i < plogs.size(); i++)
                {
                    Payload& payload = payloads[i];
                    if(payload.getSize() > 0)
                    {
                        if(!parse(payload, records)) {
                            K2ERROR("Unable to parse records");
                            throw std::move(payload);
                        }
                        activePlog = i;
                    }
                    sizes.push_back(payload.getSize());
                }

                return seastar::make_ready_future<std::vector<EntryRecord>>(std::move(records));
            });
    }

    seastar::future<> logRecord(EntryRecord record) { return log(&record, sizeof(record)); }

    seastar::future<> logRecords(std::vector<EntryRecord> records) { return log(records.data(), records.size()*sizeof(EntryRecord)); }
};

const uint32_t MaxPlogSize = 32*1024;

seastar::future<RecordPosition> PersistentVolume::append(Binary binary)
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
        }else
            return m_plog->append(m_chunkList.back().plogId, std::move(binary));
    })
    .then([appendSize, this](auto offset) {
        // update chunk Information
        auto& chunkInfo = m_chunkList.back();
        chunkInfo.size += appendSize;
        chunkInfo.actualSize += appendSize;
        return seastar::make_ready_future<RecordPosition>(RecordPosition{chunkInfo.chunkId, offset});
    });
}


seastar::future<uint32_t> PersistentVolume::read(const RecordPosition& position, const uint32_t sizeToRead, Binary& buffer)
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
    return m_plog->read(chunkInfo.plogId, ReadRegion{position.offset, readSize})
    .then([&buffer](auto region) {
        buffer = std::move(region.buffer);
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


seastar::future<> PersistentVolume::drop(ChunkId chunkId)
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

seastar::future<> PersistentVolume::addNewChunk()
{
    return m_plog->create(1)
        .then([this](std::vector<PlogId> plogIds)
        {
            EntryRecord record;
            record.plogId = plogIds[0];
            record.logType = LogType::Add;

            return entryService->logRecord(record)
                .then([this, plogId = record.plogId]
                {
                    ChunkInfo chunkInfo{plogId};
                    chunkInfo.chunkId = m_chunkList.size()==0 ? 1 : (m_chunkList.back().chunkId + 1);
                    m_chunkList.push_back(std::move(chunkInfo));

                    return seastar::make_ready_future<>();
                });
            });
}

namespace {

struct PersistentVolumeWithConstructorAccess : public PersistentVolume
{
    PersistentVolumeWithConstructorAccess(std::shared_ptr<IPlog> plog) : PersistentVolume(plog) {}
};

}

seastar::future<std::shared_ptr<PersistentVolume>> PersistentVolume::create(std::shared_ptr<IPlog> plogService)
{
    return plogService->create(16)
        .then([plogService](std::vector<PlogId> plogIds)
        {
            auto volume = std::make_shared<PersistentVolumeWithConstructorAccess>(plogService);
            volume->entryService = std::make_unique<EntryService>(plogService, std::move(plogIds));

            return volume->entryService->init().then([volume](std::vector<EntryRecord>)
            {
                return std::dynamic_pointer_cast<PersistentVolume>(volume);
            });
        });
}

seastar::future<std::shared_ptr<PersistentVolume>> PersistentVolume::open(std::shared_ptr<IPlog> plogService, std::vector<PlogId> entryPlogs)
{
    auto volume = std::make_shared<PersistentVolumeWithConstructorAccess>(plogService);
    volume->entryService = std::make_unique<EntryService>(plogService, std::move(entryPlogs));
    return volume->entryService->init().then([volume = std::move(volume)](std::vector<EntryRecord> records) mutable
        {
            for(auto& record : records)
            {
                ChunkInfo chunkInfo;
                chunkInfo.plogId = record.plogId;   //  TODO: set size
                volume->m_chunkList.push_back(std::move(chunkInfo));
            }

            return std::dynamic_pointer_cast<PersistentVolume>(volume);
        });
}

PersistentVolume::PersistentVolume() {}

PersistentVolume::~PersistentVolume() {}

PersistentVolume::PersistentVolume(std::shared_ptr<IPlog> plog) : m_plog(plog) {}

PersistentVolume::PersistentVolume(String plogPath) : PersistentVolume(std::make_shared<PlogMock>(std::move(plogPath))) {}

bool PersistentVolume::_Iterator::_isEnd() const {
    return m_chunkIndex >= parent.m_chunkList.size();
}

bool PersistentVolume::_Iterator::isEnd() const {
    return _isEnd();
}

ChunkInfo PersistentVolume::_Iterator::getCurrent() const {
    assert(!_isEnd());
    return parent.m_chunkList[m_chunkIndex];
}

bool PersistentVolume::_Iterator::equal(const k2::IIterator<ChunkInfo>* it) const {
    if (_isEnd())
        return it == nullptr || it->isEnd();

    if (!it)
        return _isEnd();

    auto other = dynamic_cast<const _Iterator*>(it);
    assert(other);

    return &parent == &other->parent && m_chunkIndex == other->m_chunkIndex;
}

bool PersistentVolume::_Iterator::advance() {
    if (m_chunkIndex < parent.m_chunkList.size())
        m_chunkIndex++;
    return !_isEnd();
}

PersistentVolume::_Iterator::_Iterator(const PersistentVolume& x) : parent(x), m_chunkIndex(0) {

}

}  //  namespace k2
