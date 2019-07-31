#define EXPOSE_PRIVATES
#include <seastar/testing/test_case.hh>

#include <iostream>
#include <filesystem>
#include "../../src/common/plog/PlogMock.h"


using namespace k2;
using namespace std;

const auto plogBaseDir = std::filesystem::temp_directory_path().concat("/plogs/");

SEASTAR_TEST_CASE(test_create_plogFileHeader)
{
    std::cout << get_name() << "...... ";

    PlogFileDescriptor plogFileDescriptor;

    auto ptr = reinterpret_cast<const PlogInfo*>(plogFileDescriptor.headBuffer.get());
    BOOST_REQUIRE(ptr->size == plogInfoSize);
    BOOST_REQUIRE(ptr->sealed == false);

    ptr = reinterpret_cast<const PlogInfo*>(plogFileDescriptor.tailBuffer.get());
    BOOST_REQUIRE(ptr->size == plogInfoSize);
    BOOST_REQUIRE(ptr->sealed == false);

    std::cout << "done." << std::endl;
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_create_plog_file)
{
    std::cout << get_name() << "...... ";

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir.c_str() + String(get_name()));

    auto plogId = plogMock->generatePlogId();
    auto plogFileName = plogMock->getPlogFileName(plogId);

    return seastar::open_file_dma(plogFileName, seastar::open_flags::rw | seastar::open_flags::create)
    .then([](seastar::file f) {
        return seastar::do_with(PlogFileDescriptor(),[f](auto& plogFileDescriptor) mutable {
            return f.dma_write(0, plogFileDescriptor.headBuffer.get(), DMA_ALIGNMENT)
            .then([f](auto) mutable {
                return f.flush();
            })
            .then([f] () mutable {
                return f.close();
            });
        });
    })
    .then([plogFileName] {
        return seastar::file_exists(plogFileName)
        .then([](bool isFound){
            BOOST_REQUIRE(isFound);
            std::cout << "done." << std::endl;
        });
    });
}


SEASTAR_TEST_CASE(test_create_plogs)
{
    std::cout << get_name() << "...... ";

    const size_t plogCount = 5;
    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir.c_str() + String(get_name()));

    return plogMock->create(plogCount)
    .then([plogMock, plogCount](std::vector<PlogId> plogIds) {
        BOOST_REQUIRE(plogIds.size() == plogCount);

        std::cout << "done." << std::endl;
        return seastar::make_ready_future<>();
    });
}


SEASTAR_TEST_CASE(test_getinfo)
{
    std::cout << get_name() << "...... ";

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir.c_str() + String(get_name()));

    return plogMock->create(1)
    .then([plogMock](std::vector<PlogId> plogIds) {
        return plogMock->getInfo(plogIds[0])
        .then([plogMock, plogId=plogIds[0]](PlogInfo plogInfo) {
            BOOST_REQUIRE(plogInfo.sealed == false);
            BOOST_REQUIRE(plogInfo.size == plogInfoSize);

            std::cout << "done." << std::endl;
            return seastar::make_ready_future<>(); // reach here
        });
    });
}


SEASTAR_TEST_CASE(test_load_then_getinfo)
{
    std::cout << get_name() << "...... ";

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir.c_str() + String(get_name()));

    auto plogId = plogMock->generatePlogId();
    auto plogFileName = plogMock->getPlogFileName(plogId);

    return seastar::open_file_dma(plogFileName, seastar::open_flags::rw | seastar::open_flags::create)
    .then([](seastar::file f)  {
        return seastar::do_with(PlogFileDescriptor(),[f](auto& plogFileDescriptor) mutable {
            return f.dma_write(0, plogFileDescriptor.headBuffer.get(), DMA_ALIGNMENT)
            .then([f](auto) mutable {
                return f.flush();
            })
            .then([f] () mutable {
                return f.close();
            });
        });
    })
    .then([plogMock, plogId, this] {
        return plogMock->getInfo(plogId).then([plogMock, plogId](PlogInfo plogInfo) {
            BOOST_REQUIRE(plogInfo.sealed == false);
            BOOST_REQUIRE(plogInfo.size == plogInfoSize);

            std::cout << "done." << std::endl;
            return seastar::make_ready_future<>(); // reach here
        });
    });
}


SEASTAR_TEST_CASE(test_getinfo_plogId_not_exist)
{
    std::cout << get_name() << "...... ";

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir.c_str() + String(get_name()));
    auto plogId = plogMock->generatePlogId();

    return plogMock->getInfo(plogId)
    .then([](auto){
        BOOST_FAIL("Expected exception");
    })
    .handle_exception([](auto e){
        try{
            std::rethrow_exception(e);
        } catch (PlogException& e) {
            BOOST_REQUIRE(e.status() == P_PLOG_ID_NOT_EXIST);

            std::cout << "done." << std::endl;
            return seastar::make_ready_future<>();
        } catch (...) {
            BOOST_FAIL("Incorrect exception type.");
            return seastar::make_ready_future<>();
        }
    });
}



SEASTAR_TEST_CASE(test_append_upto_4k)
{
    std::cout << get_name() << "...... ";

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir.c_str() + String(get_name()));

    return plogMock->create(1)
    .then([plogMock](std::vector<PlogId> plogIds) {
        std::vector<Binary>  writeBufferList;
        writeBufferList.push_back(Binary{1000});
        writeBufferList.push_back(Binary{2000});
        for(uint i=0;i<writeBufferList.size();i++){
            std::fill(writeBufferList[i].get_write(), writeBufferList[i].get_write()+writeBufferList[i].size(), i);
        }

        auto ptr = reinterpret_cast<const PlogInfo*>(plogMock->m_plogFileDescriptorList[plogIds[0]].headBuffer.get());
        auto originPlogSize = ptr->size;

        return plogMock->append(plogIds[0], std::move(writeBufferList))
        .then([plogMock, plogId = plogIds[0], originPlogSize](auto offset) {
            BOOST_REQUIRE(offset == originPlogSize);
            auto ptr = reinterpret_cast<const PlogInfo*>(plogMock->m_plogFileDescriptorList[plogId].headBuffer.get());
            size_t writeBufferSize = 1000+2000;
            BOOST_REQUIRE(offset + writeBufferSize == ptr->size);

            std::cout << "done." << std::endl;
            return seastar::make_ready_future<>();
        });
    });
}

SEASTAR_TEST_CASE(test_append_more_than_4k)
{
    std::cout << get_name() << "...... ";

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir.c_str() + String(get_name()));

    return plogMock->create(1)
    .then([plogMock](std::vector<PlogId> plogIds) {
        std::vector<Binary>  writeBufferList;
        writeBufferList.push_back(Binary{1000});
        writeBufferList.push_back(Binary{2000});
        writeBufferList.push_back(Binary{4000});
        writeBufferList.push_back(Binary{8000});
        writeBufferList.push_back(Binary{15000});

        for(uint i=0;i<writeBufferList.size();i++){
            std::fill(writeBufferList[i].get_write(), writeBufferList[i].get_write()+writeBufferList[i].size(), i);
        }

        auto ptr = reinterpret_cast<const PlogInfo*>(plogMock->m_plogFileDescriptorList[plogIds[0]].headBuffer.get());
        auto originPlogSize = ptr->size;

        return plogMock->append(plogIds[0], std::move(writeBufferList))
        .then([plogMock, plogId = plogIds[0], originPlogSize](auto offset) {
            BOOST_REQUIRE(offset == originPlogSize);
            auto ptr = reinterpret_cast<const PlogInfo*>(plogMock->m_plogFileDescriptorList[plogId].headBuffer.get());
            size_t writeBufferSize = 1000+2000+4000+8000+15000;
            BOOST_REQUIRE(offset + writeBufferSize == ptr->size);

            std::cout << "done." << std::endl;
            return seastar::make_ready_future<>();
        });
    });
}

SEASTAR_TEST_CASE(test_load_then_append)
{
    std::cout << get_name() << "...... ";

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir.c_str() + String(get_name()));

    auto plogId = plogMock->generatePlogId();
    auto plogFileName = plogMock->getPlogFileName(plogId);

    return seastar::open_file_dma(plogFileName, seastar::open_flags::rw | seastar::open_flags::create)
    .then([](seastar::file f)  {
        return seastar::do_with(PlogFileDescriptor(),[f](auto& plogFileDescriptor) mutable {
            return f.dma_write(0, plogFileDescriptor.headBuffer.get(), DMA_ALIGNMENT)
            .then([f](auto) mutable {
                return f.flush();
            })
            .then([f] () mutable {
                return f.close();
            });
        });
    })
    .then([plogMock, plogId] {
        return plogMock->getInfo(plogId)
        .then([plogMock, plogId](PlogInfo plogInfo){
            BOOST_REQUIRE(plogInfo.sealed == false);
            BOOST_REQUIRE(plogInfo.size == plogInfoSize);
            return seastar::make_ready_future<PlogInfo>(plogInfo);
        });
    })
    .then([plogMock, plogId] (PlogInfo plogInfo) {
        std::vector<Binary>  writeBufferList;
        writeBufferList.push_back(Binary{1000});
        writeBufferList.push_back(Binary{2000});
        writeBufferList.push_back(Binary{4000});
        writeBufferList.push_back(Binary{8000});
        writeBufferList.push_back(Binary{15000});

        for(uint i=0;i<writeBufferList.size();i++){
            std::fill(writeBufferList[i].get_write(), writeBufferList[i].get_write()+writeBufferList[i].size(), i);
        }

        return plogMock->append(plogId, std::move(writeBufferList))
        .then([plogMock, plogId, plogInfo](auto offset) {
            BOOST_REQUIRE(offset == plogInfo.size);
            auto ptr = reinterpret_cast<const PlogInfo*>(plogMock->m_plogFileDescriptorList[plogId].headBuffer.get());
            size_t writeBufferSize = 1000+2000+4000+8000+15000;
            BOOST_REQUIRE(offset + writeBufferSize == ptr->size);

            std::cout << "done." << std::endl;
            return seastar::make_ready_future<>();
        });
    });
}


SEASTAR_TEST_CASE(test_append_plogId_not_exist)
{
    std::cout << get_name() << "...... ";

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir.c_str() + String(get_name()));

    auto plogId = plogMock->generatePlogId();
    std::vector<Binary>  writeBufferList;
    writeBufferList.push_back(Binary{1000});
    writeBufferList.push_back(Binary{2000});

    return plogMock->append(plogId, std::move(writeBufferList))
    .then([plogMock, plogId](auto){
        BOOST_FAIL("Expected exception");
    })
    .handle_exception([](auto e){
        try{
            std::rethrow_exception(e);
        } catch (PlogException& e) {
            BOOST_REQUIRE(e.status() == P_PLOG_ID_NOT_EXIST);
            std::cout << "done." << std::endl;
            return seastar::make_ready_future<>();
        } catch (...) {
            BOOST_FAIL("Incorrect exception type.");
            return seastar::make_ready_future<>();
        }
    });
}

SEASTAR_TEST_CASE(test_append_exceed_plog_limit)
{
    std::cout << get_name() << "...... ";

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir.c_str() + String(get_name()));

    return plogMock->create(1)
    .then([plogMock, this](std::vector<PlogId> plogIds) {
        return seastar::make_ready_future<PlogId>(plogIds[0]);
    }).then([plogMock, this](PlogId plogId){
        std::vector<Binary>  writeBufferList;
        writeBufferList.push_back(Binary{1000});
        writeBufferList.push_back(Binary{plogMock->m_plogMaxSize});

        return plogMock->append(plogId, std::move(writeBufferList))
        .then([plogMock, plogId](auto){
            BOOST_FAIL("Expected exception");
        })
        .handle_exception([](auto e){
            try{
                std::rethrow_exception(e);
            } catch (PlogException& e) {
                BOOST_REQUIRE(e.status() == P_EXCEED_PLOGID_LIMIT);
                std::cout << "done." << std::endl;
                return seastar::make_ready_future<>();
            } catch (...) {
                BOOST_FAIL("Incorrect exception type.");
                return seastar::make_ready_future<>();
            }
        });
    });
}


SEASTAR_TEST_CASE(test_read)
{
    std::cout << get_name() << "...... ";

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir.c_str() + String(get_name()));

    return plogMock->create(1).then([plogMock](std::vector<PlogId> plogIds) {
        std::vector<Binary>  writeBufferList;
        writeBufferList.push_back(Binary{1000});
        writeBufferList.push_back(Binary{2000});
        writeBufferList.push_back(Binary{4000});
        writeBufferList.push_back(Binary{8000});
        writeBufferList.push_back(Binary{15000});

        for(uint8_t i=0;i<writeBufferList.size();i++){
            std::fill(writeBufferList[i].get_write(), writeBufferList[i].get_write()+writeBufferList[i].size(), i);
        }

        auto ptr = reinterpret_cast<const PlogInfo*>(plogMock->m_plogFileDescriptorList[plogIds[0]].headBuffer.get());
        auto originPlogSize = ptr->size;

        return plogMock->append(plogIds[0], std::move(writeBufferList))
        .then([plogMock, plogId = plogIds[0], originPlogSize](auto startPos) {
            BOOST_REQUIRE(startPos == originPlogSize);
            auto ptr = reinterpret_cast<const PlogInfo*>(plogMock->m_plogFileDescriptorList[plogId].headBuffer.get());
            uint32_t writeBufferSize = 1000+2000+4000+8000+15000;
            BOOST_REQUIRE(startPos + writeBufferSize == ptr->size);

            return seastar::make_ready_future<std::pair<PlogId, uint32_t> >(make_pair(plogId, startPos));
        });

    }).then([plogMock](std::pair<PlogId, uint32_t> p){
        PlogMock::ReadRegions plogDataToReadList;
        auto offset = p.second;
        plogDataToReadList.push_back(PlogMock::ReadRegion{offset, 1000});
        offset += 1000;
        plogDataToReadList.push_back(PlogMock::ReadRegion{offset, 2000});
        offset += 2000;
        plogDataToReadList.push_back(PlogMock::ReadRegion{offset, 4000});
        offset += 4000;
        plogDataToReadList.push_back(PlogMock::ReadRegion{offset, 8000});
        offset += 8000;
        plogDataToReadList.push_back(PlogMock::ReadRegion{offset, 15000});

        return plogMock->read(p.first, std::move(plogDataToReadList))
        .then([plogMock, plogId=p.first](PlogMock::ReadRegions readRegions) {
            std::vector<size_t> readSizes{1000,2000,4000,8000,15000};

            for(uint8_t i=0; i< readRegions.size(); i++)
            {
                auto read_ptr = readRegions[i].buffer.get();
                BOOST_REQUIRE(readRegions[i].buffer.size() == readSizes[i]);
                for(size_t j=0;j<readSizes[i];j++){
                    BOOST_REQUIRE(read_ptr[j] == i);
                }
            }

            std::cout << "done." << std::endl;
            return seastar::make_ready_future<>();
        });
    });
}


SEASTAR_TEST_CASE(test_read_plogId_not_exist)
{
    std::cout << get_name() << "...... ";

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir.c_str() + String(get_name()));

    auto plogId = plogMock->generatePlogId();
    PlogMock::ReadRegions plogDataToReadList;
    plogDataToReadList.push_back(PlogMock::ReadRegion{0, 1000});

    return plogMock->read(plogId, std::move(plogDataToReadList))
    .then([plogMock](auto) {
        BOOST_FAIL("Expected exception");
    })
    .handle_exception([](auto e){
        try{
            std::rethrow_exception(e);
        } catch (PlogException& e) {
            BOOST_REQUIRE(e.status() == P_PLOG_ID_NOT_EXIST);
            std::cout << "done." << std::endl;
            return seastar::make_ready_future<>();
        } catch (...) {
            BOOST_FAIL("Incorrect exception type.");
            return seastar::make_ready_future<>();
        }
    });
}


SEASTAR_TEST_CASE(test_read_capacity_not_enough)
{
    std::cout << get_name() << "...... ";

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir.c_str() + String(get_name()));

    return plogMock->create(1).then([plogMock](std::vector<PlogId> plogIds) {
        std::vector<Binary>  writeBufferList;
        writeBufferList.push_back(Binary{1000});
        writeBufferList.push_back(Binary{2000});

        for(uint8_t i=0;i<writeBufferList.size();i++){
            std::fill(writeBufferList[i].get_write(), writeBufferList[i].get_write()+writeBufferList[i].size(), i);
        }

        auto ptr = reinterpret_cast<const PlogInfo*>(plogMock->m_plogFileDescriptorList[plogIds[0]].headBuffer.get());
        auto originPlogSize = ptr->size;

        return plogMock->append(plogIds[0], std::move(writeBufferList))
        .then([plogMock, plogId = plogIds[0], originPlogSize](auto startPos) mutable {
           return seastar::make_ready_future<std::pair<PlogId, uint32_t> >(make_pair(plogId, startPos));
        });
    })
    .then([plogMock, this](auto p){
        PlogMock::ReadRegions plogDataToReadList;
        auto plogId = p.first;
        auto offset = p.second;
        plogDataToReadList.push_back(PlogMock::ReadRegion{offset, 1000});
        offset += 1000;
        plogDataToReadList.push_back(PlogMock::ReadRegion{offset, 2001});

        return plogMock->read(plogId, std::move(plogDataToReadList))
        .then([plogMock](auto) {
            BOOST_FAIL("Expected exception");
        })
        .handle_exception([](auto e){
            try{
                std::rethrow_exception(e);
            } catch (PlogException& e) {
                BOOST_REQUIRE(e.status() == P_CAPACITY_NOT_ENOUGH);
                std::cout << "done." << std::endl;
                return seastar::make_ready_future<>();
            } catch (...) {
                BOOST_FAIL("Incorrect exception type.");
                return seastar::make_ready_future<>();
            }
        });
    });
}


SEASTAR_TEST_CASE(test_seal)
{
    std::cout << get_name() << "...... ";

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir.c_str() + String(get_name()));

    return plogMock->create(1).then([plogMock](std::vector<PlogId> plogIds) {
        std::vector<Binary>  writeBufferList;
        writeBufferList.push_back(Binary{1000});
        writeBufferList.push_back(Binary{2000});

        for(uint i=0;i<writeBufferList.size();i++){
            std::fill(writeBufferList[i].get_write(), writeBufferList[i].get_write()+writeBufferList[i].size(), i);
        }

        return plogMock->append(plogIds[0], std::move(writeBufferList))
        .then([plogId=plogIds[0]](auto){
            return seastar::make_ready_future<PlogId>(plogId);
        });
    })
    .then([plogMock, this](PlogId plogId){
        return plogMock->seal(plogId)
        .then([plogMock, plogId, this]() {
            std::vector<Binary> writeBufferList;
            writeBufferList.push_back(Binary{1000});
            writeBufferList.push_back(Binary{2000});

            for(uint i=0;i<writeBufferList.size();i++){
                std::fill(writeBufferList[i].get_write(), writeBufferList[i].get_write()+writeBufferList[i].size(), i);
            }

            return plogMock->append(plogId, std::move(writeBufferList))
            .then([plogMock](auto) {
                BOOST_FAIL("Expected exception");
            })
            .handle_exception([](auto e){
                try{
                    std::rethrow_exception(e);
                } catch (PlogException& e) {
                    BOOST_REQUIRE(e.status() == P_PLOG_SEALED);
                    std::cout << "done." << std::endl;
                    return seastar::make_ready_future<>();
                } catch (...) {
                    BOOST_FAIL("Incorrect exception type.");
                    return seastar::make_ready_future<>();
                }
            });
        });
    });
}


SEASTAR_TEST_CASE(test_seal_plogId_not_exist)
{
    std::cout << get_name() << "...... ";

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir.c_str() + String(get_name()));

    auto plogId = plogMock->generatePlogId();
    PlogMock::ReadRegions plogDataToReadList;
    plogDataToReadList.push_back(PlogMock::ReadRegion{0, 1000});

    return plogMock->seal(plogId)
    .then([]() {
        BOOST_FAIL("Expected exception");
    })
    .handle_exception([](auto e){
        try{
            std::rethrow_exception(e);
        } catch (PlogException& e) {
            BOOST_REQUIRE(e.status() == P_PLOG_ID_NOT_EXIST);
            std::cout << "done." << std::endl;
            return seastar::make_ready_future<>();
        } catch (...) {
            BOOST_FAIL("Incorrect exception type.");
            return seastar::make_ready_future<>();
        }
    });
}


SEASTAR_TEST_CASE(test_drop)
{
    std::cout << get_name() << "...... ";

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir.c_str() + String(get_name()));

    return plogMock->create(1)
    .then([plogMock](std::vector<PlogId> plogIds) {
        std::vector<Binary>  writeBufferList;
        writeBufferList.push_back(Binary{1000});
        writeBufferList.push_back(Binary{2000});

        for(uint i=0;i<writeBufferList.size();i++){
            std::fill(writeBufferList[i].get_write(), writeBufferList[i].get_write()+writeBufferList[i].size(), i);
        }
        return plogMock->append(plogIds[0], std::move(writeBufferList))
        .then([plogId=plogIds[0]](auto){
            return seastar::make_ready_future<PlogId>(plogId);
        });
    })
    .then([plogMock, this](PlogId plogId){
        return plogMock->drop(plogId)
        .then([plogMock, plogId, this]() {
            return plogMock->getInfo(plogId)
            .then([](auto){
                BOOST_FAIL("Expected exception");
            })
            .handle_exception([](auto e){
                try{
                    std::rethrow_exception(e);
                } catch (PlogException& e) {
                    BOOST_REQUIRE(e.status() == P_PLOG_ID_NOT_EXIST);
                    std::cout << "done." << std::endl;
                    return seastar::make_ready_future<>();
                } catch (...) {
                    BOOST_FAIL("Incorrect exception type.");
                    return seastar::make_ready_future<>();
                }
            });
        });
    });
}


SEASTAR_TEST_CASE(test_drop_plogId_not_exist)
{
    std::cout << get_name() << "...... ";

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir.c_str() + String(get_name()));

    auto plogId = plogMock->generatePlogId();

    return plogMock->drop(plogId)
    .then([](){
        BOOST_FAIL("Expected exception");
    })
    .handle_exception([](auto e){
        try{
            std::rethrow_exception(e);
        } catch (PlogException& e) {
            BOOST_REQUIRE(e.status() == P_PLOG_ID_NOT_EXIST);
            std::cout << "done." << std::endl;
            return seastar::make_ready_future<>();
        } catch (...) {
            BOOST_FAIL("Incorrect exception type.");
            return seastar::make_ready_future<>();
        }
    });
}


SEASTAR_TEST_CASE(Remove_test_folders)
{
    std::cout << get_name() << std::endl;

    if(std::filesystem::exists(plogBaseDir)){
        std::filesystem::remove_all(plogBaseDir);
    }

    return seastar::make_ready_future<>();
}
