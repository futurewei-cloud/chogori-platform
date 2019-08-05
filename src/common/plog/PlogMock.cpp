#include "PlogMock.h"

namespace k2
{

/**********************************************************
 * constructor: initialize the default member values, create plog path if not exists
 ***********************************************************/

PlogMock::PlogMock(String plogPath)
{
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

/**********************************************************
 *   destructor
***********************************************************/

PlogMock::~PlogMock()
{
}


/**********************************************************
 *   public member methods
***********************************************************/
IOResult<std::vector<PlogId>>  PlogMock::create(uint plogCount)
{
    return seastar::do_with(std::vector<PlogId>(), [plogCount, this] (auto& plogIds) mutable {
        return seastar::repeat([&plogIds, plogCount, this] () mutable {
            if(plogIds.size() < plogCount)
            {
                auto plogId = generatePlogId();
                auto plogFileName = getPlogFileName(plogId);
                return seastar::file_exists(plogFileName)
                .then([&plogIds, plogId{std::move(plogId)}, plogFileName{std::move(plogFileName)}, this](bool isFound) mutable {
                    if(!isFound)
                    {
                        return seastar::open_file_dma(plogFileName, seastar::open_flags::rw | seastar::open_flags::create)
                        .then([&plogIds, plogId{std::move(plogId)}, this](seastar::file f)  {
                            auto plogFileDescriptor = seastar::make_lw_shared<PlogFileDescriptor>();
                            // write head information (plogInfo) to plog file
                            return f.dma_write(0, plogFileDescriptor->headBuffer.get(), DMA_ALIGNMENT)
                            .then([&plogIds, plogId{std::move(plogId)}, plogFileDescriptor, f, this](auto) mutable {
                                plogFileDescriptor->f = std::move(f);
                                m_plogFileDescriptorList[plogId] = std::move(*plogFileDescriptor);
                                return m_plogFileDescriptorList[plogId].f.flush();
                            })
                            .then([&plogIds, plogId{std::move(plogId)}]() {
                                plogIds.push_back(std::move(plogId));
                                return seastar::make_ready_future<>();
                            });
                        });
                    } else {
                        return seastar::make_ready_future<>();
                    }
                })
                .then([]{
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                });
            } else {
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
            }
        })
        .then([&plogIds] () mutable {
            return seastar::make_ready_future<std::vector<PlogId> >(std::move(plogIds));
        });
    });
}


IOResult<PlogInfo>  PlogMock::getInfo(const PlogId& plogId)
{
    if (m_plogFileDescriptorList.find(plogId) != m_plogFileDescriptorList.end()) {
        // plogId exists in the list, return the plog info
        auto ptr = reinterpret_cast<const PlogInfo*>(m_plogFileDescriptorList[plogId].headBuffer.get());
        auto plogInfo = PlogInfo{ptr->size, ptr->sealed};
        return seastar::make_ready_future<PlogInfo>(std::move(plogInfo));
    } else {
        auto plogFileName = getPlogFileName(plogId);
        return seastar::file_exists(plogFileName)
        .then([plogId, plogFileName, this](bool isFound) mutable {
            if(!isFound) {
                auto msg = "PLogId "+String(plogId.id, PLOG_ID_LEN)+" does not exist.";
                return seastar::make_exception_future<PlogInfo>(PlogException(msg, P_PLOG_ID_NOT_EXIST));
            }
            // load plog info from the plog file, then return the plog info
            return seastar::open_file_dma(plogFileName, seastar::open_flags::rw)
            .then([plogId{std::move(plogId)}, this](seastar::file f) mutable {
                auto plogFileDescriptor = seastar::make_lw_shared<PlogFileDescriptor>();
                // read head to buffer
                return f.dma_read(0, plogFileDescriptor->headBuffer.get_write(), DMA_ALIGNMENT)
                .then([plogId{std::move(plogId)}, f, plogFileDescriptor,  this](auto ret) mutable {
                    auto ptr = reinterpret_cast<const PlogInfo*>(plogFileDescriptor->headBuffer.get());
                    auto offset = seastar::align_down((uint64_t)ptr->size, (uint64_t)DMA_ALIGNMENT);

                    if(offset == ret){
                        // tail buffer is empty
                        memset(plogFileDescriptor->tailBuffer.get_write(), (uint8_t)0, DMA_ALIGNMENT);
                        return seastar::make_ready_future<PlogInfo>(PlogInfo{ptr->size, ptr->sealed});
                    }else{
                        // read tail to buffer
                        return f.dma_read(offset, plogFileDescriptor->tailBuffer.get_write(), DMA_ALIGNMENT)
                        .then([plogId{std::move(plogId)}, plogFileDescriptor, ptr, f, this](auto) mutable {
                            plogFileDescriptor->f = std::move(f);
                            m_plogFileDescriptorList[plogId] = std::move(*plogFileDescriptor);
                        })
                        .then([ptr]{
                            return seastar::make_ready_future<PlogInfo>(PlogInfo{ptr->size, ptr->sealed});
                        });
                    }
                });
            });
        });
    }
}


IOResult<uint32_t>  PlogMock::append(const PlogId& plogId, std::vector<Binary> bufferList)
{
    return getInfo(plogId)
    .then([bufferList(std::move(bufferList)), plogId, this](PlogInfo origPlogInfo) mutable {
        if(origPlogInfo.sealed) {
            auto msg = "PLogId "+String(plogId.id, PLOG_ID_LEN)+" is sealed.";
            return seastar::make_exception_future<uint32_t>(PlogException(msg, P_PLOG_SEALED));
        }

        size_t writeBufferSize = 0;
        for(auto& writeBuffer : bufferList){
            writeBufferSize += writeBuffer.size();
        }

        if(origPlogInfo.size + writeBufferSize > m_plogMaxSize)  {
            auto msg = "PLogId "+String(plogId.id, PLOG_ID_LEN)+" exceeds PLog limit.";
            return seastar::make_exception_future<uint32_t>(PlogException(msg, P_EXCEED_PLOGID_LIMIT));
        }

        /** construct dma write_buffer:
         * in seastar file system, when writing to a file, the offset and the number of bytes must
         * be aligned to dma_alignment (4k currently),
         * - the offset (the position starting to append) must be aligned down from current plog size.
         * - the bytes of write_buffer must be aligned up from expected plog size
         */
        auto offset = seastar::align_down((uint64_t) origPlogInfo.size, (uint64_t)DMA_ALIGNMENT);
        auto bufferSize = seastar::align_up(origPlogInfo.size+writeBufferSize, (uint64_t)DMA_ALIGNMENT) - offset;
        auto buffer = seastar::make_lw_shared<Binary>(Binary{bufferSize});
        size_t pos = 0;

        if(offset < origPlogInfo.size){
            /**the current actual bytes (origPlogInfo.size) in plog file is not aligned,  so
             * the exsiting bytes starting from offset must be copied to the beginning of the write_buffer for alignment
            */
            pos = origPlogInfo.size-offset;
            memcpy(buffer->get_write(), m_plogFileDescriptorList[plogId].tailBuffer.get(), pos);
        }

        // append all buffers in buffer list to write_buffer
        for(auto& writeBuffer : bufferList){
            memcpy(buffer->get_write()+pos, writeBuffer.get(), writeBuffer.size());
            pos += writeBuffer.size();
        }

        return [buffer,writeBufferSize, plogId, origPlogInfo, this](size_t offset){
        // update head buffer for head information (plogInfo)
            if(offset){
                /**head buffer is not the same as tail buffer,
                 * update the plog size in head buffer, and write to the beginning of the plog file
                */
                auto ptr = reinterpret_cast<PlogInfo*>(m_plogFileDescriptorList[plogId].headBuffer.get_write());
                ptr->size = origPlogInfo.size + writeBufferSize;

                return m_plogFileDescriptorList[plogId].f.dma_write(0, m_plogFileDescriptorList[plogId].headBuffer.get(), DMA_ALIGNMENT)
                .then([plogId, this](auto) mutable {
                    return m_plogFileDescriptorList[plogId].f.flush();
                });

            }else {
                /**head buffer is the same as tail buffer
                 * update the plog size in write_buffer,  then copy to head buffer.
                 * later on, will write the write_buffer to plog file
                */
                auto ptr = reinterpret_cast<PlogInfo*>(buffer->get_write());
                ptr->size = origPlogInfo.size + writeBufferSize;
                memcpy(m_plogFileDescriptorList[plogId].headBuffer.get_write(), buffer->get(), DMA_ALIGNMENT);
                return seastar::make_ready_future<>();
            }

        }(offset)
        .then([buffer, plogId, offset, this](){
            // update tail buffer from write_buffer
            memcpy(m_plogFileDescriptorList[plogId].tailBuffer.get_write(), buffer->get()+buffer->size()-DMA_ALIGNMENT, DMA_ALIGNMENT);

            // write plogs from write_buffer to plog file
            return m_plogFileDescriptorList[plogId].f.dma_write(offset, buffer->get(), buffer->size())
            .then([plogId, this](auto) mutable {
                return m_plogFileDescriptorList[plogId].f.flush();
            });
        })
        .then([origPlogInfo](){
            return seastar::make_ready_future<uint32_t>(origPlogInfo.size);
        });
    });
}


IOResult<PlogMock::ReadRegions>  PlogMock::read(const PlogId& plogId, PlogMock::ReadRegions plogDataToReadList) {
    return getInfo(plogId)
    .then([plogDataToReadList{std::move(plogDataToReadList)}, plogId, this] (PlogInfo plogInfo) mutable  {
        // validate read regions
        for(PlogMock::ReadRegion& readRegion : plogDataToReadList)
        {
            if(readRegion.offset+readRegion.size > plogInfo.size) {
                auto msg = "PLogId "+String(plogId.id, PLOG_ID_LEN)+" capacity not enough.";
                return seastar::make_exception_future<PlogMock::ReadRegions>(PlogException(msg, P_CAPACITY_NOT_ENOUGH));
                break;
            }
        }

        return seastar::do_with(std::move(plogDataToReadList), std::move(plogId), uint32_t(0), [this](auto& readRegions, auto& plogId, auto& count) mutable {
            return seastar::repeat([&readRegions, &plogId, &count, this] () mutable {
                if(count < readRegions.size())
                {
                    /** construct dma read buffer:
                     * in seastar file system, when reading from a file, the offset and the number of bytes must
                     * be aligned to dma_alignment (4k currently),
                     * - the offset (the position starting to read) must be aligned down from expected offset.
                     * - the reading buffer size must be aligned up from expected end position
                     */
                    auto startPos = readRegions[count].offset;
                    auto offset = seastar::align_down((uint64_t)startPos, (uint64_t)DMA_ALIGNMENT);
                    auto bufferSize = seastar::align_up((uint64_t)startPos+readRegions[count].size, (uint64_t)DMA_ALIGNMENT);
                    readRegions[count].buffer = Binary{bufferSize};

                    return m_plogFileDescriptorList[plogId].f.dma_read(offset, readRegions[count].buffer.get_write(), bufferSize)
                    .then([&readRegions,  &count,startPos, offset] (auto) mutable {
                        readRegions[count].buffer.trim_front(startPos-offset);
                        readRegions[count].buffer.trim(readRegions[count].size);
                        count++;
                        return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                    });
                } else {
                    return  seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                }
            })
            .then([&readRegions]() {
                return seastar::make_ready_future<PlogMock::ReadRegions>(std::move(readRegions));
            });
        });
    });
}


IOResult<>  PlogMock::seal(const PlogId& plogId)
{
    return getInfo(plogId)
    .then([plogId, this] (auto) mutable {
        auto ptr = reinterpret_cast<PlogInfo*>(m_plogFileDescriptorList[plogId].headBuffer.get_write());
        ptr->sealed = true;

        return m_plogFileDescriptorList[plogId].f.dma_write(0, m_plogFileDescriptorList[plogId].headBuffer.get(), DMA_ALIGNMENT)
        .then([plogId, this](auto) mutable {
            return m_plogFileDescriptorList[plogId].f.flush();
        });
    });
}


IOResult<>  PlogMock::drop(const PlogId& plogId)
{
    return getInfo(plogId)
    .then([plogId, this] (auto) mutable {
        return m_plogFileDescriptorList[plogId].f.close();
    })
    .then([plogId, this]{
        m_plogFileDescriptorList.erase(plogId);
        return seastar::make_ready_future<>();
    })
    .then([plogId, this]{
        auto plogFileName = getPlogFileName(plogId);
        return seastar::remove_file(plogFileName);
    });
}


/**********************************************************
 *   private member methods
***********************************************************/

PlogId PlogMock::generatePlogId()
{
    PlogId plogId;

    for(uint i=0; i<PLOG_ID_LEN; i++) {
        plogId.id[i] = '0' + m_distribution(m_generator);
    }

    return std::move(plogId);
}


}   //  namespace k2