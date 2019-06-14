#pragma once

#include <map>
#include <random>
#include <thread>
#include <chrono>
#include <string>
#include <fstream>
#include <filesystem>
#include <yaml-cpp/yaml.h>

#include <seastar/core/seastar.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/future-util.hh>

#include <common/Common.h>
#include "IPlog.h"


#ifdef EXPOSE_PRIVATES
#define PRIVATE public
#else
#define PRIVATE private
#endif

namespace k2
{
const size_t PLOG_MAX_SIZE = 2<<20;    // default limit of 2MB

struct plogIdComp {
    bool operator() (const PlogId& lhs, const PlogId& rhs) const
    {
        uint i=0;
        while (i<PLOG_ID_LEN && lhs.id[i]==rhs.id[i]) {
            i++;
        }

        return (i<PLOG_ID_LEN) ? lhs.id[i]<rhs.id[i] : false;
    }
};


class PlogMock : public IPlog {
public:
    
    IOResult<std::vector<PlogId>> create(uint plogCount) override;

    IOResult<PlogInfo> getInfo(const PlogId& plogId) override;

    IOResult<uint64_t> append(const PlogId& plogId, std::vector<Binary> bufferList) override;

    IOResult<ReadRegions> read(const PlogId& plogId, ReadRegions plogDataToReadList) override;

    IOResult<> seal(const PlogId plogId) override;

    IOResult<> drop(const PlogId plogId) override;

    void setPlogMaxSize(size_t plogMaxSize) { m_plogMaxSize = plogMaxSize; }

    void setPlogPath(String plogPath) {m_plogPath = plogPath; }

    void setPlogFileNamePrefix(String plogFileNamePrefix) { m_plogFileNamePrefix = plogFileNamePrefix; }

    size_t getPlogMaxSize() { return m_plogMaxSize; }

    String getPlogPath() { return m_plogPath; }

    String getPlogFileNamePrefix() { return m_plogFileNamePrefix; }

    plog_ret_code getReturnStatus(){ return m_retStatus; }

    PlogMock(String plogPath = String("./plogData")); 

    ~PlogMock();

PRIVATE:
    IOResult<PlogId*> generatePlogId();

    bool getPlogId(String plogFileName, PlogId& plogId);

    void loadConfig(std::string filename);

    void writeConfig(std::string filename);

    inline String getPlogFileName(const PlogId& plogId) {
        return (m_plogPath + "/" + m_plogFileNamePrefix + String(plogId.id, PLOG_ID_LEN));
    } 

    inline void generateRandBuff(char* buff, size_t n){
        std::uniform_int_distribution<int> distribution(0, 9);
        for(uint i=0; i<n; i++) {
            buff[i] = '0' + distribution(m_generator);
        }
    }

    
PRIVATE:
    size_t m_plogMaxSize;
    String m_plogFileNamePrefix;
    String m_plogPath;
    
    plog_ret_code m_retStatus;
    std::map<PlogId, PlogInfo, plogIdComp> m_plogInfos;
    
    std::mt19937 m_generator;
 
};  //  class PlogMock


}   //  namespace k2