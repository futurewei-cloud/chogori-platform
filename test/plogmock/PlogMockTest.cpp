#define EXPOSE_PRIVATES
#include <seastar/testing/test_case.hh>

#include <iostream>
#include <seastar/core/app-template.hh>
#include "../../src/common/plog/PlogMock.hpp"


using namespace k2;
using namespace std;

static PlogMock gPlog(String( "./plogs" ));

class TestData
{
public:
    uint plogCount;
    std::vector<PlogId> plogIds;
    std::vector<Binary> writeBufferList;
    PlogMock::ReadRegions plogDataToReadList;
    std::vector<int> readOrder;

    TestData(int _plogCount = 5) : plogCount(_plogCount)
    {
        writeBufferList.push_back(Binary(4096));
        writeBufferList.push_back(Binary(20480));
        writeBufferList.push_back(Binary(40960));

        for(auto& writeBuffer : writeBufferList) {
            gPlog.generateRandBuff((char*)writeBuffer.get_write(), writeBuffer.size());
        }

        readOrder = std::vector<int>{1, 0, 2};
        plogDataToReadList.push_back(PlogMock::ReadRegion(4096, 20480));
        plogDataToReadList.push_back(PlogMock::ReadRegion(0, 4096));
        plogDataToReadList.push_back(PlogMock::ReadRegion(24576, 40960));
    }
};

TestData testData;


SEASTAR_TEST_CASE(test_create)
{
    return gPlog.create(testData.plogCount).then([this](std::vector<PlogId> plogIds) {
        testData.plogIds = std::move(plogIds);
        BOOST_REQUIRE(gPlog.getReturnStatus() == P_OK);
        BOOST_REQUIRE(testData.plogIds.size() == testData.plogCount);        
        std::cout << get_name() << ":\tthe number of plogIds: " << testData.plogIds.size() << std::endl;
        return seastar::make_ready_future<>();
    });       
}


SEASTAR_TEST_CASE(test_getinfo)
{
    return seastar::do_with(uint(0), [this] (auto& i) mutable {
        return seastar::repeat([&i, this] () mutable {
            if(i<testData.plogIds.size())
            {
                return gPlog.getInfo(testData.plogIds[i]).then([&i](PlogInfo plogInfo) {
                    BOOST_REQUIRE(gPlog.getReturnStatus() == P_OK);
                    BOOST_REQUIRE(plogInfo.sealed == false);
                    BOOST_REQUIRE(plogInfo.size == 0);
                    i++;
                    return seastar::make_ready_future<>();
                }).then([]{
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                });
            } else {
                std::cout << get_name() << ":\tthe number of plogIds: " << i << std::endl;
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes); 
            }
        }).then([]{
            return seastar::make_ready_future<>();
        });  
    });     
}


SEASTAR_TEST_CASE(test_append_successed)
{
   return seastar::do_with(uint(0), [this] (auto& i) mutable {
        return seastar::repeat([&i,this] () mutable {
            if(i<testData.plogIds.size())
            {
                std::vector<Binary>  writeBufferList;
                for(auto& writeBuffer : testData.writeBufferList){
                    writeBufferList.push_back(writeBuffer.clone());
                }
                
                auto originPlogSize = gPlog.m_plogInfos[testData.plogIds[i]].size;
                return gPlog.append(testData.plogIds[i], std::move(writeBufferList)).then([&i, originPlogSize](uint64_t ret) {
                    BOOST_REQUIRE(gPlog.getReturnStatus() == P_OK);
                    BOOST_REQUIRE(ret == originPlogSize+4096+20480+40960);
                    i++;                
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                });
            } else {
                std::cout << get_name() << ":\tthe number of plogIds: " << i << std::endl;
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes); 
            }
        }).then([](){
            return seastar::make_ready_future<>();
        });
    });
}


SEASTAR_TEST_CASE(test_append_plogId_not_exist) 
{
    return seastar::repeat([this](){
        return gPlog.generatePlogId().then([this](PlogId* plogId) {
            if(gPlog.m_plogInfos.find(*plogId) != gPlog.m_plogInfos.end()) 
            {
                delete plogId;
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
            } else {
                std::vector<Binary>  writeBufferList;
                for(auto& writeBuffer : testData.writeBufferList) {
                    writeBufferList.push_back(writeBuffer.clone());
                }

                return gPlog.append(*plogId, std::move(writeBufferList)).then([plogId, this](uint64_t ret) {
                    delete plogId;
                    BOOST_REQUIRE(gPlog.getReturnStatus() == P_PLOG_ID_NOT_EXIST);
                    std::cout << get_name() << ":\tPLOG_ID_NOT_EXIST" << std::endl;
                }).then([] {
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                });
            }
        });
    }).then([]{
        return seastar::make_ready_future<>();
    });   
}


SEASTAR_TEST_CASE(test_append_exceed_plog_limit) 
{
    std::vector<Binary> writeBufferList;
    writeBufferList.push_back(Binary(4096)); 
    writeBufferList.push_back(Binary(gPlog.m_plogMaxSize)); 
    for(auto& writeBuffer : writeBufferList) {
        gPlog.generateRandBuff((char*)writeBuffer.get_write(), writeBuffer.size());
    }

    return gPlog.append(testData.plogIds[0], std::move(writeBufferList)).then([this](uint64_t ret) {
        BOOST_REQUIRE(gPlog.getReturnStatus() == P_EXCEED_PLOGID_LIMIT);
        std::cout << get_name() << ":\tEXCEED_PLOGID_LIMIT" << std::endl;
    }).then([]{
        return seastar::make_ready_future<>();
    });
}


SEASTAR_TEST_CASE(test_read_successed)
{
    PlogMock::ReadRegions plogDataToReadList;
    for(auto& plogDataToRead : testData.plogDataToReadList){
       plogDataToReadList.push_back(PlogMock::ReadRegion(plogDataToRead.offset, plogDataToRead.size, Binary(plogDataToRead.size))); 
    }

    return gPlog.read(testData.plogIds[0], std::move(plogDataToReadList)).then([this](PlogMock::ReadRegions readRegions) {
        std::cout << get_name() << ":\tread_size: ";

        int i = 0;
        for(auto& readRegion : readRegions)
        {
            auto readSize = readRegion.buffer.size();
            auto read_ptr = readRegion.buffer.get();
            auto writeSize = testData.writeBufferList[testData.readOrder[i]].size();
            auto write_ptr = testData.writeBufferList[testData.readOrder[i]].get();
            BOOST_REQUIRE(gPlog.getReturnStatus() == P_OK);
            BOOST_REQUIRE(readSize == writeSize);
            BOOST_REQUIRE(strncmp((const char*)read_ptr, (const char*)write_ptr, readSize)  == 0);

            std::cout << readSize << ", ";
            i++;
        }
        std::cout << std::endl;
        return seastar::make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_read_plogId_not_exist) 
{
    return seastar::repeat([this](){
        return gPlog.generatePlogId().then([this](PlogId* plogId) {
            if(gPlog.m_plogInfos.find(*plogId) != gPlog.m_plogInfos.end()) 
            {
                delete plogId;
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
            } else {
                PlogMock::ReadRegions plogDataToReadList;
                plogDataToReadList.push_back(PlogMock::ReadRegion(0, 4096, Binary(4096))); 

                return gPlog.read(*plogId, std::move(plogDataToReadList)).then([plogId, this](PlogMock::ReadRegions readRegions) {
                    delete plogId;
                    BOOST_REQUIRE(gPlog.getReturnStatus() == P_PLOG_ID_NOT_EXIST);
                    std::cout << get_name() << ":\tPLOG_ID_NOT_EXIST" << std::endl;
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                });
           }
        });
    }).then([]{
        return seastar::make_ready_future<>();
    });   
}


SEASTAR_TEST_CASE(test_read_capacity_not_enough) 
{
    PlogMock::ReadRegions plogDataToReadList;
    plogDataToReadList.push_back(PlogMock::ReadRegion(40960, 40960, Binary(40960))); 

    return gPlog.read(testData.plogIds[0], std::move(plogDataToReadList)).then([this](PlogMock::ReadRegions readRegions) {
        BOOST_REQUIRE(gPlog.getReturnStatus() == P_CAPACITY_NOT_ENOUGH);
        std::cout << get_name() << ":\tCAPACITY_NOT_ENOUGH" << std::endl;
        return seastar::make_ready_future<>();
    });
}


SEASTAR_TEST_CASE(test_seal_successed) 
{
    return gPlog.seal(testData.plogIds[0]).then([this]() {
        BOOST_REQUIRE(gPlog.getReturnStatus() == P_OK);

        std::vector<Binary> writeBufferList;
        writeBufferList.push_back(Binary(4096)); 
        gPlog.generateRandBuff((char*)writeBufferList[0].get_write(), writeBufferList[0].size());

        return gPlog.append(testData.plogIds[0], std::move(writeBufferList)).then([this](uint64_t ret) {
            BOOST_REQUIRE(gPlog.getReturnStatus() == P_PLOG_SEALED);
            std::cout << get_name() << ":\tPLOG_SEALED" << std::endl;
        }).then([]{
            return seastar::make_ready_future<>();
        });
    });
}


SEASTAR_TEST_CASE(test_seal_plogId_not_exist) 
{
    return seastar::repeat([this](){
        return gPlog.generatePlogId().then([this](PlogId* plogId) {
            if(gPlog.m_plogInfos.find(*plogId) != gPlog.m_plogInfos.end()) 
            {
                delete plogId;
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
            } else {
                return gPlog.seal(*plogId).then([plogId, this]() {
                    delete plogId;
                    BOOST_REQUIRE(gPlog.getReturnStatus() == P_PLOG_ID_NOT_EXIST);
                    std::cout << get_name() << ":\tPLOG_ID_NOT_EXIST" << std::endl;
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                });
            }
        });
    }).then([]{
        return seastar::make_ready_future<>();
    });   
}


SEASTAR_TEST_CASE(test_drop_successed) 
{
    return gPlog.drop(testData.plogIds[1]).then([this]() {
        BOOST_REQUIRE(gPlog.getReturnStatus() == P_OK);
        BOOST_REQUIRE(gPlog.m_plogInfos.find(testData.plogIds[1]) == gPlog.m_plogInfos.end());

        String plogFileName = gPlog.getPlogFileName(testData.plogIds[1]);
        return seastar::file_exists(plogFileName).then([this](bool isFound){
            BOOST_REQUIRE(!isFound);
            std::cout << get_name() << ":\tPLOG_ID_DROPED: " << std::endl;
            return seastar::make_ready_future<>();
        });
    });
}


SEASTAR_TEST_CASE(test_drop_plogId_not_exist) 
{
    return seastar::repeat([this](){
        return gPlog.generatePlogId().then([this](PlogId* plogId) {
            if(gPlog.m_plogInfos.find(*plogId) != gPlog.m_plogInfos.end()) 
            {
                delete plogId;
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
            } else {
                return gPlog.drop(*plogId).then([plogId, this]() {
                    delete plogId;
                    BOOST_REQUIRE(gPlog.getReturnStatus() == P_PLOG_ID_NOT_EXIST);
                    std::cout << get_name() << ":\tPLOG_ID_NOT_EXIST" << std::endl;
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                });
            }
        });
    }).then([]{
        return seastar::make_ready_future<>();
    });   
}

