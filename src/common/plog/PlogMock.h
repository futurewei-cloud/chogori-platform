#pragma once

#include <map>
#include <random>
#include <thread>
#include <chrono>
#include <string>
#include <fstream>
#include <filesystem>

#include <seastar/core/seastar.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/align.hh>

#include <common/Common.h>
#include "IPlog.h"


#ifdef EXPOSE_PRIVATES
#define PRIVATE public
#else
#define PRIVATE private
#endif

namespace k2
{
constexpr size_t PLOG_MAX_SIZE = 16<<20;    // default limit
constexpr size_t DMA_ALIGNMENT = 4096;

constexpr size_t plogInfoSize = sizeof(PlogInfo);

struct PlogIdComp {
    bool operator() (const PlogId& lhs, const PlogId& rhs) const
    {
        uint i=0;
        while (i<PLOG_ID_LEN && lhs.id[i]==rhs.id[i]) {
            i++;
        }

        return (i<PLOG_ID_LEN) ? lhs.id[i]<rhs.id[i] : false;
    }
};

struct PlogFD 
{
    seastar::file f;
    Binary headBuffer{DMA_ALIGNMENT};
    Binary tailBuffer{DMA_ALIGNMENT};

    PlogFD(){
        memset(headBuffer.get_write(), (uint8_t)0, DMA_ALIGNMENT);
        memset(tailBuffer.get_write(), (uint8_t)0, DMA_ALIGNMENT);

        PlogInfo plogInfo {plogInfoSize, false};  // plog file head information
        memcpy(headBuffer.get_write(), &plogInfo, plogInfoSize);
        memcpy(tailBuffer.get_write(), &plogInfo, plogInfoSize);
    }
};

class PlogException : public std::exception {
public:
    virtual const char* what() const noexcept {
        return  _msg.c_str();
    }

    PlogException(const String& msg, plog_ret_code status)
            : _msg(msg), _status(status) {
    }

    plog_ret_code status() const {
        return _status;
    }

    virtual const std::string& str() const {
        return _msg;
    }

private:
    std::string  _msg;
    plog_ret_code _status;
};

class PlogMock : public IPlog {
public:
    
    IOResult<std::vector<PlogId>> create(uint plogCount) override;

    IOResult<PlogInfo> getInfo(const PlogId& plogId) override;

    IOResult<uint64_t> append(const PlogId& plogId, std::vector<Binary> bufferList) override;

    IOResult<ReadRegions> read(const PlogId& plogId, ReadRegions plogDataToReadList) override;

    IOResult<> seal(const PlogId& plogId) override;

    IOResult<> drop(const PlogId& plogId) override;

    void setPlogMaxSize(size_t plogMaxSize) { m_plogMaxSize = plogMaxSize; }

    void setPlogPath(String plogPath) {m_plogPath = plogPath; }

    void setPlogFileNamePrefix(String plogFileNamePrefix) { m_plogFileNamePrefix = plogFileNamePrefix; }

    size_t getPlogMaxSize() { return m_plogMaxSize; }

    String getPlogPath() { return m_plogPath; }

    String getPlogFileNamePrefix() { return m_plogFileNamePrefix; }

    PlogMock(String plogPath = String("./plogData")); 

    ~PlogMock();

PRIVATE:
    PlogId generatePlogId();

    bool getPlogId(const String& plogFileName, PlogId& plogId);

    String getPlogFileName(const PlogId& plogId) {
        return (m_plogPath + "/" + m_plogFileNamePrefix + String(plogId.id, PLOG_ID_LEN));
    } 
    
PRIVATE:
    size_t m_plogMaxSize;
    String m_plogFileNamePrefix;
    String m_plogPath;
    
    std::map<PlogId, PlogFD, PlogIdComp> m_plogFDList;    
    std::default_random_engine m_generator;
    std::uniform_int_distribution<int> m_distribution;
 
};  //  class PlogMock


}   //  namespace k2