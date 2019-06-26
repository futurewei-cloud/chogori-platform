#include "PlogMock.h"

namespace k2
{

/**********************************************************
 *   constructor and destructor
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
                            auto plogFD = seastar::make_lw_shared<PlogFD>();
                            // write head information (plogInfo) to plog file
                            return f.dma_write(0, plogFD->headBuffer.get(), DMA_ALIGNMENT)
                            .then([&plogIds, plogId{std::move(plogId)}, plogFD, f, this](uint64_t ret) mutable {
                                plogFD->f = std::move(f);
                                m_plogFDList[plogId] = std::move(*plogFD);                                
                                return m_plogFDList[plogId].f.flush();
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
    if (m_plogFDList.find(plogId) != m_plogFDList.end()) {
        // plogId exists in the list, return the plog info
        auto ptr = reinterpret_cast<const PlogInfo*>(m_plogFDList[plogId].headBuffer.get());
        auto plogInfo = PlogInfo{ptr->size, ptr->sealed};
        return seastar::make_ready_future<PlogInfo>(std::move(plogInfo));
    } else {
        auto plogFileName = getPlogFileName(plogId);
        return seastar::file_exists(plogFileName)
        .then([plogId, plogFileName, this](bool isFound) mutable {
            if(!isFound) {
                return seastar::make_exception_future<PlogInfo>(PlogException("PLogId does not exist.", P_PLOG_ID_NOT_EXIST));
            }
            // load plog info from the plog file, then return the plog info
            return seastar::open_file_dma(plogFileName, seastar::open_flags::rw)
            .then([plogId{std::move(plogId)}, this](seastar::file f) mutable {
                auto plogFD = seastar::make_lw_shared<PlogFD>();
                // read head to buffer 
                return f.dma_read(0, plogFD->headBuffer.get_write(), DMA_ALIGNMENT)
                .then([plogId{std::move(plogId)}, f, plogFD,  this](uint64_t ret) mutable {                       
                    auto ptr = reinterpret_cast<const PlogInfo*>(plogFD->headBuffer.get());
                    auto offset = seastar::align_down(ptr->size, DMA_ALIGNMENT);

                    if(offset == ret){
                        // tail buffer is empty
                        memset(plogFD->tailBuffer.get_write(), (uint8_t)0, DMA_ALIGNMENT);
                        return seastar::make_ready_future<PlogInfo>(PlogInfo{ptr->size, ptr->sealed});
                    }else{
                        // read tail to buffer
                        return f.dma_read(offset, plogFD->tailBuffer.get_write(), DMA_ALIGNMENT)
                        .then([plogId{std::move(plogId)}, plogFD, ptr, f, this](uint64_t ret) mutable {
                            plogFD->f = std::move(f);
                            m_plogFDList[plogId] = std::move(*plogFD);                                       
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


IOResult<uint64_t>  PlogMock::append(const PlogId& plogId, std::vector<Binary> bufferList) 
{
    return getInfo(plogId)
    .then([bufferList(std::move(bufferList)), plogId, this](PlogInfo origPlogInfo) mutable {
        if(origPlogInfo.sealed) {
            return seastar::make_exception_future<uint64_t>(PlogException("PLog is sealed.", P_PLOG_SEALED));
        }

        size_t writeBufferSize = 0;
        for(auto& writeBuffer : bufferList){
            writeBufferSize += writeBuffer.size();
        }

        if(origPlogInfo.size + writeBufferSize > m_plogMaxSize)  {
            return seastar::make_exception_future<uint64_t>(PlogException("Exceed PLog limit.", P_EXCEED_PLOGID_LIMIT));
        } 
        
        // construct dma write buffer
        auto offset = seastar::align_down(origPlogInfo.size, DMA_ALIGNMENT);
        auto bufferSize = seastar::align_up(origPlogInfo.size+writeBufferSize, DMA_ALIGNMENT) - offset;
        auto buffer = seastar::make_lw_shared<Binary>(Binary{bufferSize});
        size_t pos = 0;

        if(offset < origPlogInfo.size){
            pos = origPlogInfo.size-offset;
            memcpy(buffer->get_write(), m_plogFDList[plogId].tailBuffer.get(), pos);
        }

        for(auto& writeBuffer : bufferList){
            memcpy(buffer->get_write()+pos, writeBuffer.get(), writeBuffer.size());
            pos += writeBuffer.size();
        }

        return [buffer,writeBufferSize, plogId, origPlogInfo, this](size_t offset){
        // update head buffer for head inforamtion (plogInfo)
            if(offset){
                auto ptr = reinterpret_cast<PlogInfo*>(m_plogFDList[plogId].headBuffer.get_write());
                ptr->size = origPlogInfo.size + writeBufferSize;

                return m_plogFDList[plogId].f.dma_write(0, m_plogFDList[plogId].headBuffer.get(), DMA_ALIGNMENT)
                .then([plogId, this](uint64_t ret) mutable {
                    return m_plogFDList[plogId].f.flush();
                });

            }else {                 
                auto ptr = reinterpret_cast<PlogInfo*>(buffer->get_write());
                ptr->size = origPlogInfo.size + writeBufferSize;
                memcpy(m_plogFDList[plogId].headBuffer.get_write(), buffer->get(), DMA_ALIGNMENT);
                return seastar::make_ready_future<>();
            }

        }(offset)
        .then([buffer, plogId, offset, this](){
            // update tail buffer
            memcpy(m_plogFDList[plogId].tailBuffer.get_write(), buffer->get()+buffer->size()-DMA_ALIGNMENT, DMA_ALIGNMENT);

            // write plogs
            return m_plogFDList[plogId].f.dma_write(offset, buffer->get(), buffer->size())
            .then([plogId, this](uint64_t ret) mutable {
                return m_plogFDList[plogId].f.flush();
            });
        })
        .then([origPlogInfo](){
            return seastar::make_ready_future<uint64_t>(origPlogInfo.size);
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
                return seastar::make_exception_future<PlogMock::ReadRegions>(PlogException("PLog capacity not enough.", P_CAPACITY_NOT_ENOUGH));
                break;
            }
        }

        return seastar::do_with(std::move(plogDataToReadList), std::move(plogId), size_t(0), [this](auto& readRegions, auto& plogId, auto& count) mutable {
            return seastar::repeat([&readRegions, &plogId, &count, this] () mutable {
                if(count < readRegions.size())
                {
                    auto startPos = readRegions[count].offset;
                    auto offset = seastar::align_down(startPos, DMA_ALIGNMENT);
                    auto bufferSize = seastar::align_up(startPos+readRegions[count].size, DMA_ALIGNMENT);
                    readRegions[count].buffer = Binary{bufferSize};

                    return m_plogFDList[plogId].f.dma_read(offset, readRegions[count].buffer.get_write(), bufferSize)
                    .then([&readRegions,  &count,startPos, offset] (size_t ret) mutable {
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
    .then([plogId, this] (PlogInfo plogInfo) mutable {
        auto ptr = reinterpret_cast<PlogInfo*>(m_plogFDList[plogId].headBuffer.get_write());
        ptr->sealed = true;

        return m_plogFDList[plogId].f.dma_write(0, m_plogFDList[plogId].headBuffer.get(), DMA_ALIGNMENT)
        .then([plogId, this](uint64_t ret) mutable {
            return m_plogFDList[plogId].f.flush();
        });
    })
    .then([plogId, this]{
        return seastar::make_ready_future<>();
    }); 
}


IOResult<>  PlogMock::drop(const PlogId& plogId) 
{
    return getInfo(plogId)
    .then([plogId, this] (PlogInfo plogInfo) mutable {
        return m_plogFDList[plogId].f.close();
    })
    .then([plogId, this]{
        m_plogFDList.erase(plogId);
        return seastar::make_ready_future<>();
    })
    .then([plogId, this]{
        auto plogFileName = getPlogFileName(plogId);
        return seastar::remove_file(plogFileName);
    })
    .then([plogId, this]{
        return seastar::make_ready_future<>();
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

bool PlogMock::getPlogId(const String& plogFileName, PlogId& plogId){
    auto prefixLength = m_plogFileNamePrefix.length();

    if(plogFileName.length() != prefixLength+PLOG_ID_LEN || plogFileName.substr(prefixLength) != m_plogFileNamePrefix)
    {
        return false;
    }
    
    strncpy(plogId.id, plogFileName.substr(prefixLength,PLOG_ID_LEN).c_str(), PLOG_ID_LEN );
    return true;
}


}   //  namespace k2