#include "PlogMock.h"

namespace k2
{

seastar::future<> PlogFileDescriptor::close(){
    return ssfile.flush().then([this] {
        return this->ssfile.close();
    });
}

//
//  Plog file structure:
//      HEADER of size DMA_ALIGNMENT
//      Data
//

PlogMock::PlogMock(String plogPath) {
    m_plogPath = plogPath;
    m_plogMaxSize = PLOG_MAX_SIZE;
    m_plogFileNamePrefix = String{"plogid_"};

    std::random_device rd;
    m_generator = std::default_random_engine{rd()};
    m_distribution = std::uniform_int_distribution<int>{0,9};

    std::filesystem::path p{m_plogPath};
    if(!std::filesystem::exists(p)){
        std::filesystem::create_directories(p);
    }
}

PlogMock::~PlogMock()
{
}

seastar::future<> PlogMock::close() {
    std::vector<seastar::future<>> futs;
    for (auto it = m_plogFileDescriptors.begin(); it != m_plogFileDescriptors.end(); it++) {
        futs.push_back(it->second.close());
    }
    return seastar::when_all(futs.begin(), futs.end()).discard_result();
}

seastar::future<std::vector<PlogId>> PlogMock::create(uint plogCount)
{
    return seastar::do_with(std::vector<PlogId>(), [plogCount, this] (auto& plogIds) mutable
    {
        return seastar::repeat([&plogIds, plogCount, this]() mutable
        {
            if(plogIds.size() >= plogCount)
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);

            auto plogId = generatePlogId();
            auto plogFileName = getPlogFileName(plogId);
            return seastar::file_exists(plogFileName)
                .then([&plogIds, plogId, plogFileName{std::move(plogFileName)}, this](bool isFound) mutable
                {
                    if(isFound) //  Such plog name already exists
                        return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);

                    return seastar::open_file_dma(plogFileName, seastar::open_flags::rw | seastar::open_flags::create)
                        .then([&plogIds, plogId, this](seastar::file new_ssfile) {
                            auto plogFileDescriptor = seastar::make_lw_shared<PlogFileDescriptor>();
                            plogFileDescriptor->ssfile = std::move(new_ssfile);

                            // write head information (plogInfo) to plog file
                            return plogFileDescriptor->ssfile.dma_write(0, plogFileDescriptor->headBuffer.get(), DMA_ALIGNMENT)
                                .then([&plogIds, plogId, plogFileDescriptor, this](auto) mutable {
                                    return plogFileDescriptor->ssfile.flush();
                                })
                                .then([&plogIds, plogId, plogFileDescriptor, this]() mutable {
                                    assert(m_plogFileDescriptors.find(plogId) == m_plogFileDescriptors.end());
                                    m_plogFileDescriptors[plogId] = std::move(*(plogFileDescriptor.release()));
                                    plogIds.push_back(std::move(plogId));
                                    return seastar::stop_iteration::no;
                                });
                        });
                });
        })
        .then([&plogIds] { return std::move(plogIds); });
    });
}


seastar::future<PlogFileDescriptor*> PlogMock::getDescriptor(const PlogId& plogId)
{
    auto it = m_plogFileDescriptors.find(plogId);
    if (it != m_plogFileDescriptors.end())
        return seastar::make_ready_future<PlogFileDescriptor*>(&it->second);

    //  TODO: need to handle concurrency, e.g. another task access PLOG while we still loading buffers.
    // Can be done by storing future in PlogFileDescriptor
    auto plogFileName = getPlogFileName(plogId);
    return seastar::file_exists(plogFileName)
        .then([plogId, plogFileName, this](bool isFound) mutable
        {
            if(!isFound)
            {
                auto msg = "PLogId "+String(plogId.id, PLOG_ID_LEN)+" does not exist.";
                return seastar::make_exception_future<PlogFileDescriptor*>(PlogException(msg, P_PLOG_ID_NOT_EXIST));
            }

            // load plog info from the plog file, then return the plog info
            return seastar::open_file_dma(plogFileName, seastar::open_flags::rw)
                .then([plogId{std::move(plogId)}, this](seastar::file new_ssfile) mutable {
                    auto plogFileDescriptor = seastar::make_lw_shared<PlogFileDescriptor>();

                    // read header
                    return new_ssfile.dma_read(0, plogFileDescriptor->headBuffer.get_write(), DMA_ALIGNMENT)
                        .then([plogId{std::move(plogId)}, new_ssfile, plogFileDescriptor, this](auto) mutable {
                            if(!plogFileDescriptor->getInfo().size)
                                return seastar::make_ready_future<>();

                            //  Need to load tail segment
                            size_t tailOffset = seastar::align_down(plogFileDescriptor->getInfo().size, DMA_ALIGNMENT);
                            size_t tailOffsetInFile = DMA_ALIGNMENT + tailOffset;
                            return new_ssfile.dma_read(tailOffsetInFile, plogFileDescriptor->tailBuffer.get_write(), DMA_ALIGNMENT).discard_result();
                        })
                        .then([plogId{std::move(plogId)}, plogFileDescriptor, new_ssfile, this]() mutable {
                            plogFileDescriptor->ssfile = std::move(new_ssfile);
                            m_plogFileDescriptors[plogId] = std::move(*(plogFileDescriptor.release()));
                            return seastar::make_ready_future<PlogFileDescriptor*>(&m_plogFileDescriptors[plogId]);
                        });
                });
        });
}

seastar::future<PlogInfo> PlogMock::getInfo(const PlogId& plogId)
{
    return getDescriptor(plogId).then([](PlogFileDescriptor* descr) { return descr->getInfo(); });
}

seastar::future<uint32_t> PlogMock::append(const PlogId& plogId, Binary bin)
{
    return getDescriptor(plogId)    //  TODO: concurency
        .then([bin=std::move(bin), plogId, this](PlogFileDescriptor* descr) mutable
        {
            if(descr->getInfo().sealed)
            {
                auto msg = "PLogId "+String(plogId.id, PLOG_ID_LEN)+" is sealed.";
                return seastar::make_exception_future<uint32_t>(PlogException(msg, P_PLOG_SEALED));
            }

            if(descr->getInfo().size + bin.size() > m_plogMaxSize)
            {
                auto msg = "PLogId "+String(plogId.id, PLOG_ID_LEN)+" exceeds PLog limit.";
                return seastar::make_exception_future<uint32_t>(PlogException(msg, P_EXCEED_PLOGID_LIMIT));
            }

            //
            //  Buffer needs to be split into 3 parts: part that can be fit into current tail buffer up to DMA_ALIGNMENT,
            //  central part having size proportional to DMA_ALIGNMENT, last part that is less thatn DMA_ALIGNMENT and
            //  which will become new tail buffer
            //
            size_t size = bin.size();
            size_t remainingWriteSize = size;

            //  Part that needs to be written in current head segment
            size_t leftTailSize = std::min(descr->getTailRemaining(), remainingWriteSize);
            remainingWriteSize -= leftTailSize;

            //  Part needed to be written as a last tail
            size_t rightTailSize = remainingWriteSize % DMA_ALIGNMENT;
            remainingWriteSize -= rightTailSize;

            if(leftTailSize)
                std::memcpy(descr->tailBuffer.get_write()+descr->getTailSize(), bin.get(), leftTailSize);

            seastar::future<size_t> writeTail = leftTailSize ?
                descr->ssfile.dma_write(descr->getTailOffsetInFile(), descr->tailBuffer.get(), DMA_ALIGNMENT) :
                seastar::make_ready_future<size_t>(0);

            return writeTail
                .then([remainingWriteSize, buf = bin.get()+leftTailSize, descr](auto)
                {
                    return remainingWriteSize ?
                        descr->ssfile.dma_write(descr->getTailOffsetInFile()+DMA_ALIGNMENT, buf, remainingWriteSize) :
                        seastar::make_ready_future<size_t>(0);
                })
                .then([rightTailSize, leftTailSize, bin = std::move(bin), remainingWriteSize, descr](size_t writtenSize)
                {
                    assert(writtenSize == remainingWriteSize);

                    if(!rightTailSize)
                        return seastar::make_ready_future<size_t>(0);

                    std::memcpy(descr->tailBuffer.get_write(), bin.get()+leftTailSize+remainingWriteSize, rightTailSize);

                    return descr->ssfile.dma_write(descr->getTailOffsetInFile()+DMA_ALIGNMENT+remainingWriteSize, descr->tailBuffer.get(), DMA_ALIGNMENT);
                })
                .then([descr, size](auto)
                {
                    descr->getInfo().size += size;
                    return descr->ssfile.dma_write(0, descr->headBuffer.get(), DMA_ALIGNMENT).discard_result();
                })
                .then([descr] { return descr->ssfile.flush(); })
                .then([descr, size]
                {
                    return seastar::make_ready_future<uint32_t>(descr->getInfo().size-size);
                });
        });
}

seastar::future<> readFull(seastar::file& f, size_t position, char* buffer, size_t size)
{
    assert((position % DMA_ALIGNMENT) == 0);
    assert((size % DMA_ALIGNMENT) == 0);

    return seastar::do_with(size, position, buffer, [&f](size_t& size, size_t& position, char* &buffer) mutable
    {
        return seastar::repeat([&size, &position, &buffer, &f]
        {
            if(size == 0)
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);

            return f.dma_read(position, buffer, size)
                .then([&size, &position, &buffer](size_t readSize)
                {
                    if(!readSize)
                        return seastar::make_exception_future<seastar::stop_iteration>(PlogException("Read failure: dma_read=0", P_ERROR));

                    assert(size >= readSize);
                    size -= readSize;
                    position += readSize;
                    buffer += readSize;
                    return size ?
                        seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no) :
                        seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                });
        });
    });
}

seastar::future<ReadRegion> PlogMock::read(const PlogId& plogId, ReadRegion region)
{
    assert(region.size && (!region.buffer || region.buffer.size() >= region.size));

    return getDescriptor(plogId)
        .then([region = std::move(region), plogId] (PlogFileDescriptor* descr) mutable
        {
            if(region.offset >= descr->getInfo().size)
                return seastar::make_exception_future<ReadRegion>(PlogException("Plog doesn't have data at such offset", P_CAPACITY_NOT_ENOUGH));

            size_t sizeToRead = std::min(region.size, descr->getInfo().size-region.offset);
            if(!region.buffer)
                region.buffer = Binary(sizeToRead);
            else
            {
                if(region.buffer.size() > sizeToRead)
                    region.buffer.trim(sizeToRead);
            }

            //  TODO: can be optimized
            size_t blockOffset = seastar::align_down(region.offset, DMA_ALIGNMENT);
            size_t offsetInBlock = region.offset - blockOffset;
            size_t blockSize = seastar::align_up(region.offset+sizeToRead, (size_t)DMA_ALIGNMENT) - blockOffset;

            blockOffset += DMA_ALIGNMENT;   //  Skip header

            Binary blockBuffer(blockSize);

            return seastar::do_with(std::move(region), std::move(blockBuffer), [descr, plogId, blockOffset, blockSize, offsetInBlock](auto& region, auto& blockBuffer) mutable
            {
                return readFull(descr->ssfile, blockOffset, blockBuffer.get_write(), blockSize)
                    .then([&region, &blockBuffer, offsetInBlock, blockSize]
                    {
                        std::memcpy(region.buffer.get_write(), blockBuffer.get()+offsetInBlock, region.buffer.size());
                        return seastar::make_ready_future<ReadRegion>(std::move(region));
                    });
            });
        });
}

seastar::future<>  PlogMock::seal(const PlogId& plogId)
{
    return getInfo(plogId)
    .then([plogId, this] (auto) mutable {
        auto ptr = reinterpret_cast<PlogInfo*>(m_plogFileDescriptors[plogId].headBuffer.get_write());
        ptr->sealed = true;

        return m_plogFileDescriptors[plogId].ssfile.dma_write(0, m_plogFileDescriptors[plogId].headBuffer.get(), DMA_ALIGNMENT).then([plogId, this](auto) mutable {
            return m_plogFileDescriptors[plogId].ssfile.flush();
        });
    });
}

seastar::future<>  PlogMock::drop(const PlogId& plogId)
{
    return getInfo(plogId)
        .then([plogId, this](auto) mutable {
            return m_plogFileDescriptors[plogId].close();
        })
        .then([plogId, this] {
            m_plogFileDescriptors.erase(plogId);
            return seastar::make_ready_future<>();
        })
        .then([plogId, this] {
            auto plogFileName = getPlogFileName(plogId);
            return seastar::remove_file(plogFileName);
        });
}

PlogId PlogMock::generatePlogId()
{
    PlogId plogId;

    for(uint i=0; i<PLOG_ID_LEN; i++) {
        plogId.id[i] = '0' + m_distribution(m_generator);
    }

    return std::move(plogId);
}


}   //  namespace k2
