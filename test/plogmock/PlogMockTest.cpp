#define EXPOSE_PRIVATES
#include <seastar/testing/test_case.hh>

#include <iostream>
#include <filesystem>
#include <k2/persistence/plog/PlogMock.h>
#include <k2/common/Common.h>
#include <TestUtil.h>

using namespace k2;
using namespace std;

const auto plogBaseDir = generateTempFolderPath("plogmock_test");

auto createPlogMock(std::string testName)
{
    String folder = plogBaseDir + testName;
    K2INFO(testName << ".....");
    return seastar::make_lw_shared<PlogMock>(std::move(folder));
}

#define GET_PLOG_MOCK() createPlogMock(get_name())

SEASTAR_TEST_CASE(test_create_plogs)
{
    K2INFO(get_name() << "...... ");

    const size_t plogCount = 5;
    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir + get_name());

    return plogMock->create(plogCount)
        .then([plogMock, plogCount](std::vector<PlogId>&& plogIds) {
            BOOST_REQUIRE(plogIds.size() == plogCount);

            std::vector<seastar::future<>> infoFuture;
            for(auto& plogId : plogIds)
                infoFuture.push_back(plogMock->getInfo(plogId).then([plogMock, plogId](PlogInfo&& plogInfo)
                {
                    BOOST_REQUIRE(!plogInfo.sealed);
                    BOOST_REQUIRE(!plogInfo.size);

                    return seastar::make_ready_future<>();
                }));

            return seastar::when_all_succeed(infoFuture.begin(), infoFuture.end()).discard_result();
        })
        .then([] { K2INFO("done");})
        .finally([plogMock]() mutable {
            return plogMock->close();
        }).then([plogMock](){});
}

SEASTAR_TEST_CASE(test_getinfo_plogId_not_exist)
{
    K2INFO(get_name() << "...... ");

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir + get_name());
    PlogId plogId;
    std::memset(&plogId, 1, sizeof(plogId));

    return plogMock->getInfo(plogId)
        .then([](auto&&) {
            BOOST_FAIL("Expected exception");
            return seastar::make_ready_future<>();
        })
        .handle_exception([](auto e) {
            try {
                std::rethrow_exception(e);
            } catch (PlogException& e) {
                BOOST_REQUIRE(e.status() == P_PLOG_ID_NOT_EXIST);

                K2INFO("done");
                return seastar::make_ready_future<>();
            } catch (...) {
                BOOST_FAIL("Incorrect exception type.");
                return seastar::make_ready_future<>();
            }
        })
        .finally([plogMock]() mutable {
            return plogMock->close();
        }).then([plogMock](){});
    ;
}

SEASTAR_TEST_CASE(test_append_upto_4k)
{
    K2INFO(get_name() << "...... ");

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir + get_name());

    return plogMock->create(1)
        .then([plogMock](std::vector<PlogId>&& plogIds) {
            std::vector<Binary> writeBufferList;
            writeBufferList.push_back(Binary{1000});
            writeBufferList.push_back(Binary{2000});
            for (uint i = 0; i < writeBufferList.size(); i++) {
                std::fill(writeBufferList[i].get_write(), writeBufferList[i].get_write() + writeBufferList[i].size(), i);
            }

            return plogMock->appendMany(plogIds[0], std::move(writeBufferList))
                .then([plogMock, plogId = plogIds[0]](auto&& offset) {
                    BOOST_REQUIRE(offset == 0);
                    auto ptr = reinterpret_cast<const PlogInfo*>(plogMock->m_plogFileDescriptors[plogId].headBuffer.get());
                    size_t writeBufferSize = 1000 + 2000;
                    BOOST_REQUIRE(offset + writeBufferSize == ptr->size);

                    K2INFO("done");
                    return seastar::make_ready_future<>();
                });
        })
        .finally([plogMock]() mutable {
            return plogMock->close();
        }).then([plogMock](){});
    ;
}

SEASTAR_TEST_CASE(test_append_more_than_4k)
{
    K2INFO(get_name() << "...... ");

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir + get_name());

    return plogMock->create(1)
        .then([plogMock](std::vector<PlogId>&& plogIds) {
            std::vector<Binary> writeBufferList;
            writeBufferList.push_back(Binary{1000});
            writeBufferList.push_back(Binary{2000});
            writeBufferList.push_back(Binary{4000});
            writeBufferList.push_back(Binary{8000});
            writeBufferList.push_back(Binary{15000});

            for (uint i = 0; i < writeBufferList.size(); i++) {
                std::fill(writeBufferList[i].get_write(), writeBufferList[i].get_write() + writeBufferList[i].size(), i);
            }

            auto ptr = reinterpret_cast<const PlogInfo*>(plogMock->m_plogFileDescriptors[plogIds[0]].headBuffer.get());
            auto originPlogSize = ptr->size;

            return plogMock->appendMany(plogIds[0], std::move(writeBufferList))
                .then([plogMock, plogId = plogIds[0], originPlogSize](auto&& offset) {
                    BOOST_REQUIRE(offset == originPlogSize);
                    auto ptr = reinterpret_cast<const PlogInfo*>(plogMock->m_plogFileDescriptors[plogId].headBuffer.get());
                    size_t writeBufferSize = 1000 + 2000 + 4000 + 8000 + 15000;
                    BOOST_REQUIRE(offset + writeBufferSize == ptr->size);

                    K2INFO("done");
                    return seastar::make_ready_future<>();
                });
        })
        .finally([plogMock]() mutable {
            return plogMock->close();
        }).then([plogMock](){});
    ;
}



SEASTAR_TEST_CASE(test_append_plogId_not_exist)
{
    K2INFO(get_name() << "...... ");

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir + get_name());

    PlogId plogId;
    std::memset(&plogId, 1, sizeof(plogId));

    std::vector<Binary>  writeBufferList;
    writeBufferList.push_back(Binary{1000});
    writeBufferList.push_back(Binary{2000});

    return plogMock->appendMany(plogId, std::move(writeBufferList))
        .then([plogMock, plogId](auto&&) {
            BOOST_FAIL("Expected exception");
        })
        .handle_exception([](auto&& e) {
            try {
                std::rethrow_exception(e);
            } catch (PlogException& e) {
                BOOST_REQUIRE(e.status() == P_PLOG_ID_NOT_EXIST);
                K2INFO("done");
                return seastar::make_ready_future<>();
            } catch (...) {
                BOOST_FAIL("Incorrect exception type.");
                return seastar::make_ready_future<>();
            }
        })
        .finally([plogMock]() mutable {
            return plogMock->close();
        }).then([plogMock](){});
    ;
}

SEASTAR_TEST_CASE(test_append_exceed_plog_limit)
{
    K2INFO(get_name() << "...... ");

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir + get_name());

    return plogMock->createOne().then([plogMock, this](PlogId&& plogId){
            std::vector<Binary>  writeBufferList;

            return plogMock->append(plogId, Binary{4*1024*1024})
            .then([plogMock, plogId](auto&&){
                BOOST_FAIL("Expected exception");
            })
            .handle_exception([](auto&& e){
                try{
                    std::rethrow_exception(e);
                } catch (PlogException& e) {
                    BOOST_REQUIRE(e.status() == P_EXCEED_PLOGID_LIMIT);
                    K2INFO("done");
                    return seastar::make_ready_future<>();
                } catch (...) {
                    BOOST_FAIL("Incorrect exception type.");
                    return seastar::make_ready_future<>();
                }
            });
        })
        .finally([plogMock]() mutable {
            return plogMock->close();
        }).then([plogMock](){});
}


SEASTAR_TEST_CASE(test_read)
{
    K2INFO(get_name() << "...... ");
    auto plogMock = GET_PLOG_MOCK();

    return plogMock->createOne().then([plogMock](PlogId&& plogId)
    {
        std::vector<size_t> sizes{167,2334,4374,8635,15667};
        std::vector<Binary> writeBufferList;

        size_t totalSize = 0;
        for(size_t i=0; i < sizes.size(); i++)
        {
            writeBufferList.push_back(Binary(sizes[i]));
            totalSize += sizes[i];
            std::fill(writeBufferList[i].get_write(), writeBufferList[i].get_write()+writeBufferList[i].size(), (char)i);
        }
        K2INFO("Total size=" << totalSize);

        return plogMock->appendMany(plogId, std::move(writeBufferList))
            .then([plogMock, plogId, totalSize](auto&& startPos)
            {
                BOOST_REQUIRE(startPos == 0);
                return plogMock->getInfo(plogId).then([totalSize](PlogInfo&& info)
                {
                    BOOST_REQUIRE(info.size == totalSize);
                    return seastar::make_ready_future<>();
                });
            })
            .then([plogMock, plogId, sizes = std::move(sizes)]
            {
                ReadRegions plogDataToReadList;
                uint32_t offset = 0;
                for(size_t size : sizes)
                {
                    plogDataToReadList.push_back(ReadRegion{offset, (uint32_t) size});
                    offset += size;
                }

                return plogMock->readMany(plogId, std::move(plogDataToReadList))
                    .then([plogMock, plogId, sizes = std::move(sizes)](ReadRegions&& readRegions)
                    {
                        BOOST_REQUIRE(readRegions.size() == sizes.size());
                        for(size_t i = 0; i < readRegions.size(); i++)
                        {
                            auto read_ptr = readRegions[i].buffer.get();
                            BOOST_REQUIRE(readRegions[i].buffer.size() == sizes[i]);
                            for(size_t j = 0; j<sizes[i]; j++)
                            {
                                if((size_t)read_ptr[j] != i)
                                {
                                    K2INFO("!!!" << i << " " << j << " " << (uint32_t)read_ptr[j]);
                                }

                                BOOST_REQUIRE((size_t)read_ptr[j] == i);
                            }
                        }

                        return plogMock->readAll(plogId)
                            .then([sizes = std::move(sizes), plogMock](Payload&& payload) {
                                payload.seek(0);
                                size_t totalSize = 0;
                                for(size_t i = 0; i < sizes.size(); i++)
                                {
                                    for(size_t j = 0; j < sizes[i]; j++)
                                    {
                                        char b;
                                        BOOST_REQUIRE(payload.read(b));
                                        BOOST_REQUIRE((size_t)b == i);
                                    }

                                    totalSize += sizes[i];
                                }

                                BOOST_REQUIRE(payload.getSize() == totalSize);

                                return seastar::make_ready_future<>();
                            });
                    });
            });
    })
    .then([] { K2INFO("done"); })

    .finally([plogMock]() mutable {
        return plogMock->close();
    }).then([plogMock](){});
}

SEASTAR_TEST_CASE(test_read_plogId_not_exist)
{
    K2INFO(get_name() << "...... ");

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir + get_name());

    auto plogId = plogMock->generatePlogId();

    return plogMock->read(plogId, ReadRegion{0, 1000})
    .then([plogMock](auto&&) {
        BOOST_FAIL("Expected exception");
    })
    .handle_exception([](auto e){
        try{
            std::rethrow_exception(e);
        } catch (PlogException& e) {
            BOOST_REQUIRE(e.status() == P_PLOG_ID_NOT_EXIST);
            K2INFO("done");
            return seastar::make_ready_future<>();
        } catch (...) {
            BOOST_FAIL("Incorrect exception type.");
            return seastar::make_ready_future<>();
        }
    })
    .finally([plogMock]() mutable {
        return plogMock->close();
    }).then([plogMock](){});
}

SEASTAR_TEST_CASE(test_read_capacity_not_enough)
{
    K2INFO(get_name() << "...... ");
    auto plogMock = GET_PLOG_MOCK();

    return plogMock->create(1).then([plogMock](std::vector<PlogId>&& plogIds) {
        std::vector<Binary>  writeBufferList;
        writeBufferList.push_back(Binary{1000});
        writeBufferList.push_back(Binary{2000});

        for(uint8_t i=0;i<writeBufferList.size();i++){
            std::fill(writeBufferList[i].get_write(), writeBufferList[i].get_write()+writeBufferList[i].size(), i);
        }

        auto ptr = reinterpret_cast<const PlogInfo*>(plogMock->m_plogFileDescriptors[plogIds[0]].headBuffer.get());
        auto originPlogSize = ptr->size;

        return plogMock->appendMany(plogIds[0], std::move(writeBufferList))
        .then([plogMock, plogId = plogIds[0], originPlogSize](uint32_t startPos) mutable {
           return seastar::make_ready_future<std::pair<PlogId, uint32_t> >(make_pair(plogId, startPos));
        });
    })
    .then([plogMock, this](auto&& p){
        ReadRegions plogDataToReadList;
        auto plogId = p.first;
        plogDataToReadList.push_back(ReadRegion{0, 1000});
        plogDataToReadList.push_back(ReadRegion{3000, 2001});

        return plogMock->readMany(plogId, std::move(plogDataToReadList))
        .then([plogMock](auto&&) {
            BOOST_FAIL("Expected exception");
        })
        .handle_exception([](auto e){
            try{
                std::rethrow_exception(e);
            } catch (PlogException& e) {
                BOOST_REQUIRE(e.status() == P_CAPACITY_NOT_ENOUGH);
                K2INFO("done");
                return seastar::make_ready_future<>();
            } catch (...) {
                BOOST_FAIL("Incorrect exception type.");
                return seastar::make_ready_future<>();
            }
        });
    })
    .finally([plogMock]() mutable {
        return plogMock->close();
    }).then([plogMock](){});
}


SEASTAR_TEST_CASE(test_seal)
{
    K2INFO(get_name() << "...... ");

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir + get_name());

    return plogMock->create(1).then([plogMock](std::vector<PlogId>&& plogIds) {
        std::vector<Binary>  writeBufferList;
        writeBufferList.push_back(Binary{1000});
        writeBufferList.push_back(Binary{2000});

        for(uint i=0;i<writeBufferList.size();i++){
            std::fill(writeBufferList[i].get_write(), writeBufferList[i].get_write()+writeBufferList[i].size(), i);
        }

        return plogMock->appendMany(plogIds[0], std::move(writeBufferList))
        .then([plogId=plogIds[0]](auto&&){
            return seastar::make_ready_future<PlogId>(plogId);
        });
    })
    .then([plogMock, this](PlogId&& plogId){
        return plogMock->seal(plogId)
        .then([plogMock, plogId, this]() {
            std::vector<Binary> writeBufferList;
            writeBufferList.push_back(Binary{1000});
            writeBufferList.push_back(Binary{2000});

            for(uint i=0;i<writeBufferList.size();i++){
                std::fill(writeBufferList[i].get_write(), writeBufferList[i].get_write()+writeBufferList[i].size(), i);
            }

            return plogMock->appendMany(plogId, std::move(writeBufferList))
            .then([plogMock](auto&&) {
                BOOST_FAIL("Expected exception");
            })
            .handle_exception([](auto e){
                try{
                    std::rethrow_exception(e);
                } catch (PlogException& e) {
                    BOOST_REQUIRE(e.status() == P_PLOG_SEALED);
                    K2INFO("done");
                    return seastar::make_ready_future<>();
                } catch (...) {
                    BOOST_FAIL("Incorrect exception type.");
                    return seastar::make_ready_future<>();
                }
            });
        });
    })
    .finally([plogMock]() mutable {
        return plogMock->close();
    }).then([plogMock](){});
}


SEASTAR_TEST_CASE(test_seal_plogId_not_exist)
{
    K2INFO(get_name() << "...... ");

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir + get_name());

    auto plogId = plogMock->generatePlogId();

    return plogMock->seal(plogId)
    .then([]() {
        BOOST_FAIL("Expected exception");
    })
    .handle_exception([](auto e){
        try{
            std::rethrow_exception(e);
        } catch (PlogException& e) {
            BOOST_REQUIRE(e.status() == P_PLOG_ID_NOT_EXIST);
            K2INFO("done");
            return seastar::make_ready_future<>();
        } catch (...) {
            BOOST_FAIL("Incorrect exception type.");
            return seastar::make_ready_future<>();
        }
    })
    .finally([plogMock]() mutable {
        return plogMock->close();
    }).then([plogMock](){});
}


SEASTAR_TEST_CASE(test_drop)
{
    K2INFO(get_name() << "...... ");

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir + get_name());

    return plogMock->create(1)
    .then([plogMock](std::vector<PlogId>&& plogIds) {
        std::vector<Binary>  writeBufferList;
        writeBufferList.push_back(Binary{1000});
        writeBufferList.push_back(Binary{2000});

        for(uint i=0;i<writeBufferList.size();i++){
            std::fill(writeBufferList[i].get_write(), writeBufferList[i].get_write()+writeBufferList[i].size(), i);
        }
        return plogMock->appendMany(plogIds[0], std::move(writeBufferList))
        .then([plogId=plogIds[0]](auto&&){
            return seastar::make_ready_future<PlogId>(plogId);
        });
    })
    .then([plogMock, this](PlogId&& plogId){
        return plogMock->drop(plogId)
        .then([plogMock, plogId, this]() {
            return plogMock->getInfo(plogId)
            .then([](auto&&){
                BOOST_FAIL("Expected exception");
            })
            .handle_exception([](auto e){
                try{
                    std::rethrow_exception(e);
                } catch (PlogException& e) {
                    BOOST_REQUIRE(e.status() == P_PLOG_ID_NOT_EXIST);
                    K2INFO("done");
                    return seastar::make_ready_future<>();
                } catch (...) {
                    BOOST_FAIL("Incorrect exception type.");
                    return seastar::make_ready_future<>();
                }
            });
        });
    })
    .finally([plogMock]() mutable {
        return plogMock->close();
    }).then([plogMock](){});
}


SEASTAR_TEST_CASE(test_drop_plogId_not_exist)
{
    K2INFO(get_name() << "...... ");

    auto plogMock = seastar::make_lw_shared<PlogMock>(plogBaseDir + get_name());

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
            K2INFO("done");
            return seastar::make_ready_future<>();
        } catch (...) {
            BOOST_FAIL("Incorrect exception type.");
            return seastar::make_ready_future<>();
        }
    })
    .finally([plogMock]() mutable {
        return plogMock->close();
    }).then([plogMock](){});
}


SEASTAR_TEST_CASE(Remove_test_folders)
{
    K2INFO(get_name());

    if(std::filesystem::exists(plogBaseDir)){
        std::filesystem::remove_all(plogBaseDir);
    }

    return seastar::make_ready_future<>();
}
