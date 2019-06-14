#include "PlogMock.hpp"

namespace k2
{

/**********************************************************
 *   constructor and destructor
***********************************************************/

PlogMock::PlogMock(String plogPath)
{
    m_plogPath = plogPath;
    m_plogMaxSize = PLOG_MAX_SIZE; 
    m_plogFileNamePrefix = String("plogid_");

    std::random_device rd;
    m_generator = std::mt19937(rd());

    std::filesystem::path p = m_plogPath.c_str();    
    if(!std::filesystem::exists(p)){
        std::filesystem::create_directory(p);
    }else {
        std::filesystem::path fname = (m_plogPath+"/plog.yaml").c_str();
        if(std::filesystem::exists(fname)){
            loadConfig(fname);
            writeConfig(std::string(fname)+"_prev");
        }     
    }  

    m_retStatus = P_OK;
}  

PlogMock::~PlogMock()
{
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    writeConfig((m_plogPath+"/plog.yaml").c_str());
}  


/**********************************************************
 *   public member methods
***********************************************************/

IOResult<std::vector<PlogId>>  PlogMock::create(uint plogCount) 
{
    std::vector<PlogId> plogIds;
    
    return seastar::do_with(std::move(plogIds), plogCount, [this] (std::vector<PlogId>& plogIds, uint plogCount) mutable {
        return seastar::repeat([&plogIds, plogCount, this] () mutable { 
            if(plogIds.size() < plogCount)
            {
                return generatePlogId().then([&plogIds, this](PlogId* plogId) mutable { 
                    String plogFileName = getPlogFileName(*plogId);
                    return seastar::file_exists(plogFileName).then([&plogIds, plogId, plogFileName, this](bool isFound) mutable {
                        if(!isFound) 
                        {
                           return seastar::open_file_dma(plogFileName, seastar::open_flags::rw | seastar::open_flags::create).then([&plogIds, plogId, plogFileName, this](seastar::file f)  {
                                m_plogInfos[*plogId] = PlogInfo{0, false}; 
                                plogIds.push_back(std::move(*plogId));
                                return f.close();
                             });                   
                        } else {
                            delete plogId;
                            return seastar::make_ready_future<>();
                        }
                    });
                }).then([]{
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no); 
                });
            } else {
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes); 
            }
        }).then([&plogIds, this] () mutable {
            m_retStatus = P_OK;
            return seastar::make_ready_future<std::vector<PlogId> >(std::move(plogIds));
        });
    });
}

IOResult<PlogInfo>  PlogMock::getInfo(const PlogId& plogId) {
    PlogInfo plogInfo{0, true};

    if (m_plogInfos.find(plogId) == m_plogInfos.end()) {
        m_retStatus = P_PLOG_ID_NOT_EXIST;
    } else {
        plogInfo = m_plogInfos[plogId];
        m_retStatus = P_OK;
    }
    
    return seastar::make_ready_future<PlogInfo>(std::move(plogInfo));
}

IOResult<uint64_t>  PlogMock::append(const PlogId& plogId, std::vector<Binary> bufferList) 
{
    uint64_t pos = 0;

    if (m_plogInfos.find(plogId) == m_plogInfos.end()) 
    {
        m_retStatus = P_PLOG_ID_NOT_EXIST;
        return seastar::make_ready_future<uint64_t>(pos);
    }else if(m_plogInfos[plogId].sealed) {
        m_retStatus = P_PLOG_SEALED;
        return seastar::make_ready_future<uint64_t>(pos);
    }else {
        pos = m_plogInfos[plogId].size;
        auto writeBufferSize = 0;
        for(auto& writeBuffer : bufferList){
            writeBufferSize += writeBuffer.size();
        }

        if(writeBufferSize < m_plogMaxSize)
        {
            String plogFileName = getPlogFileName(plogId);
            return seastar::do_with(std::move(bufferList), pos, plogFileName, [this, plogId] (auto& bufferList, auto& pos, auto& plogFileName) mutable {
                return seastar::open_file_dma(plogFileName, seastar::open_flags::wo).then([&bufferList, &pos, this, plogId](seastar::file f) mutable {
                    return seastar::do_with(std::move(bufferList), f, pos, uint(0), [this, plogId] (auto& bufferList, auto& f, auto& pos,  auto& i) mutable {
                        return seastar::repeat([&bufferList, &pos,f, &i, this, plogId] () mutable {
                            if(i< bufferList.size())
                            {
                                return f.dma_write(pos, bufferList[i].get(), bufferList[i].size()).then([&pos, &i](uint64_t ret) mutable {
                                    pos += ret;
                                    i++;
                                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                                }); 
                            } else {
                                return f.flush().then([&f]() {
                                    return f.close();
                                }).then([]{
                                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                                });
                            }  
                        }).then([&pos, this, plogId]() {
                            m_plogInfos[plogId].size = pos;
                            m_retStatus = P_OK;
                            return seastar::make_ready_future<uint64_t>(pos);
                        });                        
                    });
                });
            });      
        } else {
            m_retStatus = P_EXCEED_PLOGID_LIMIT;
            return seastar::make_ready_future<uint64_t>(pos);
        }
    }   
}

IOResult<PlogMock::ReadRegions>  PlogMock::read(const PlogId& plogId, PlogMock::ReadRegions plogDataToReadList) {
    if (m_plogInfos.find(plogId) == m_plogInfos.end()) 
    {
        m_retStatus = P_PLOG_ID_NOT_EXIST;
        return seastar::make_ready_future<PlogMock::ReadRegions>(std::move(plogDataToReadList));
    }else {
        auto plogSize = m_plogInfos[plogId].size;
        m_retStatus = P_OK;
        for(auto&readRegion : plogDataToReadList) 
        {
            if(readRegion.offset+readRegion.size > plogSize) {
                m_retStatus = P_CAPACITY_NOT_ENOUGH;
                break;
            }
        }

        if(m_retStatus == P_CAPACITY_NOT_ENOUGH){
            return seastar::make_ready_future<PlogMock::ReadRegions>(std::move(plogDataToReadList));
        }else {
            String plogFileName = getPlogFileName(plogId);
            return seastar::do_with(std::move(plogDataToReadList), plogFileName, plogSize, [this] (auto& plogDataToReadList, auto& plogFileName, auto& plogSize) mutable {
                return seastar::open_file_dma(plogFileName, seastar::open_flags::ro).then([&plogDataToReadList, plogSize, this](seastar::file f) mutable {
                    return seastar::do_with(std::move(plogDataToReadList), plogSize, f, uint(0), [this] (auto& readRegions, auto& plogSize, auto& f, auto& i) mutable {
                        return seastar::repeat([&readRegions, plogSize, f, &i, this] () mutable {
                            if(i< readRegions.size())
                            {
                                return f.dma_read(readRegions[i].offset, readRegions[i].buffer.get_write(), readRegions[i].size).then([&i](uint64_t ret) mutable {
                                    i++;
                                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                                }); 
                            } else {
                                return f.flush().then([&f]() {
                                    return f.close();
                                }).then([]{
                                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                                });
                            }  
                        }).then([&readRegions]() {
                            return seastar::make_ready_future<PlogMock::ReadRegions>(std::move(readRegions));
                        });                        
                    });
                });
            });                  
        }
    }
}

IOResult<>  PlogMock::seal(const PlogId plogId) 
{  
    if (m_plogInfos.find(plogId) == m_plogInfos.end()) {
        m_retStatus = P_PLOG_ID_NOT_EXIST;
    }else{
        m_plogInfos[plogId].sealed = true;
        m_retStatus = P_OK;
    }
    
    return seastar::make_ready_future<>();
}

IOResult<>  PlogMock::drop(const PlogId plogId) 
{
    if (m_plogInfos.find(plogId) == m_plogInfos.end()) 
    {
        m_retStatus = P_PLOG_ID_NOT_EXIST;
        return seastar::make_ready_future<>();
    }else {
        m_plogInfos.erase(plogId);
        String plogFileName = getPlogFileName(plogId);
        return seastar::remove_file(plogFileName).then([this]{
            m_retStatus = P_OK;
            return seastar::make_ready_future<>();
        });
    }
}



/**********************************************************
 *   private member methods
***********************************************************/

void PlogMock::loadConfig(std::string filename)
{
    YAML::Node config;
    try
    {
        config = YAML::LoadFile(filename);
    }
    catch (YAML::BadFile &e)
    {
        std::cout << e.msg << ": " << filename.c_str() << std::endl;
        return;
    }

    m_plogMaxSize = config["plogMaxSize"].as<size_t>();
    m_plogFileNamePrefix = config["plogFileNamePrefix"].as<std::string>();;

    size_t nodes = config["plogInfos"].size();
    for(size_t i=0; i<nodes; i++) 
    {
        std::string key = config["plogInfos"][i]["plogId"].as<std::string>();      
        PlogId plogId;
        strncpy(plogId.id, key.c_str(), PLOG_ID_LEN);
        YAML::Node plogInfoNode = config["plogInfos"][i]["plogInfo"];               
        PlogInfo plogInfo;
        plogInfo.size = plogInfoNode["size"].as<uint64_t>();
        plogInfo.sealed = plogInfoNode["sealed"].as<unsigned>();
        m_plogInfos[plogId] = plogInfo;
    }
    
}


void PlogMock::writeConfig(std::string filename)
{
    std::ofstream myfile(filename);

    myfile << "plogMaxSize: " << m_plogMaxSize << std::endl;
    myfile << "plogFileNamePrefix: " << m_plogFileNamePrefix << std::endl;

    myfile << "plogInfos: [" << std::endl;
    for(auto& p : m_plogInfos)
    {
        myfile << "    {plogId: " << std::string(p.first.id, PLOG_ID_LEN) << ", ";
        myfile << "plogInfo: {size: " << p.second.size << ", sealed: " << p.second.sealed << "}}," << std::endl;
    }
    myfile << "]" << std::endl;
    myfile.close();
    
}


IOResult<PlogId*> PlogMock::generatePlogId()
{
    PlogId* plogId = new PlogId();
    generateRandBuff(plogId->id, PLOG_ID_LEN);
    return seastar::make_ready_future<PlogId*>(plogId);
}


bool PlogMock::getPlogId(String plogFileName, PlogId& plogId){
    auto prefixLength = m_plogFileNamePrefix.length();
    if(plogFileName.length() != prefixLength+PLOG_ID_LEN || plogFileName.substr(prefixLength) != m_plogFileNamePrefix)
    {
        return false;
    }
    
    strncpy(plogId.id, plogFileName.substr(prefixLength,PLOG_ID_LEN).c_str(), PLOG_ID_LEN );
    return true;
}


}   //  namespace k2