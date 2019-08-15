#define EXPOSE_PRIVATES
#include <seastar/testing/test_case.hh>

#include <iostream>
#include <node/persistence/PersistentVolume.h>

using namespace k2;
using namespace std;

const auto plogBaseDir = std::filesystem::temp_directory_path().concat("/plogs/");
uint32_t constexpr BUFFERSIZE = 100;

SEASTAR_TEST_CASE(test_addNewChunk)
{
    std::cout << get_name() << "...... " << std::flush;
    auto persistentVolume = seastar::make_lw_shared<PersistentVolume>(plogBaseDir.c_str() + String(get_name()));

    return persistentVolume->addNewChunk()
        .then([persistentVolume]() {
            BOOST_REQUIRE(persistentVolume->m_chunkList.size() == 1);
            auto& chunkInfo = persistentVolume->m_chunkList.back();

            BOOST_REQUIRE(chunkInfo.chunkId == 1);
            BOOST_REQUIRE(chunkInfo.size == 0);
            BOOST_REQUIRE(chunkInfo.actualSize == 0);
            std::cout << "done." << std::endl;
            return seastar::make_ready_future<>(); // reach here
        });
}


SEASTAR_TEST_CASE(test_getInfo)
{
    std::cout << get_name() << "...... " << std::flush;
    auto persistentVolume = seastar::make_lw_shared<PersistentVolume>(plogBaseDir.c_str() + String(get_name()));

    for (size_t i=0; i<5; i++){
        ChunkInfo chunkInfo;
        chunkInfo.chunkId = i;
        chunkInfo.size = i;
        chunkInfo.actualSize = i;
        persistentVolume->m_chunkList.push_back(chunkInfo);
    }

    for (ChunkId i=0; i<5; i++){
        auto chunkInfo = persistentVolume->getInfo(i);
        BOOST_REQUIRE(chunkInfo.chunkId == i);
        BOOST_REQUIRE(chunkInfo.size == i);
        BOOST_REQUIRE(chunkInfo.actualSize == i );
    }

    std::cout << "done." << std::endl;
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_getInfo_ChunkIdNotFound)
{
    std::cout << get_name() << "...... " << std::flush;
    auto persistentVolume = seastar::make_lw_shared<PersistentVolume>(plogBaseDir.c_str() + String(get_name()));

    for (size_t i=0; i<5; i++){
        ChunkInfo chunkInfo;
        chunkInfo.chunkId = i;
        chunkInfo.size = i;
        chunkInfo.actualSize = i;
        persistentVolume->m_chunkList.push_back(chunkInfo);
    }

    try{
        persistentVolume->getInfo(persistentVolume->m_chunkList.back().chunkId+1);
        BOOST_FAIL("Expected exception.");
    }catch(ChunkException& e){
        BOOST_REQUIRE(e.chunkId() == persistentVolume->m_chunkList.back().chunkId+1);
        std::cout << "done." << std::endl;    // reach here
    }catch (...) {
        BOOST_FAIL("Incorrect exception type.");
    }

    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_decreaseUsage_ChunkIdNotFound)
{
    std::cout << get_name() << "...... " << std::flush;
    auto persistentVolume = seastar::make_lw_shared<PersistentVolume>(plogBaseDir.c_str() + String(get_name()));

    for (size_t i=0; i<2; i++){
        ChunkInfo chunkInfo;
        chunkInfo.chunkId = i;
        chunkInfo.size = 100;
        chunkInfo.actualSize = 100;
        persistentVolume->m_chunkList.push_back(chunkInfo);
    }

    try{
        persistentVolume->setUsage(persistentVolume->m_chunkList.back().chunkId+1, 30);
        BOOST_FAIL("Expected exception.");
    }catch(ChunkException& e){
        BOOST_REQUIRE(e.chunkId() == persistentVolume->m_chunkList.back().chunkId+1);
        std::cout << "done." << std::endl; // reach here
    }catch (...) {
        BOOST_FAIL("Incorrect exception type.");
    }

    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_totaUsage)
{
    std::cout << get_name() << "...... " << std::flush;
    auto persistentVolume = seastar::make_lw_shared<PersistentVolume>(plogBaseDir.c_str() + String(get_name()));

    uint64_t sum = 0;
    for (size_t i=0; i<10; i++){
        ChunkInfo chunkInfo;
        chunkInfo.chunkId = i;
        chunkInfo.size = i*100;
        chunkInfo.actualSize = i*90;
        sum += i*90;
        persistentVolume->m_chunkList.push_back(chunkInfo);
    }

     BOOST_REQUIRE(persistentVolume->totalUsage() == sum);

    std::cout << "done." << std::endl;
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_totaSize)
{
    std::cout << get_name() << "...... " << std::flush;
    auto persistentVolume = seastar::make_lw_shared<PersistentVolume>(plogBaseDir.c_str() + String(get_name()));

    uint64_t sum = 0;
    for (size_t i=0; i<10; i++){
        ChunkInfo chunkInfo;
        chunkInfo.chunkId = i;
        chunkInfo.size = i*100;
        sum += i*100;
        chunkInfo.actualSize = i*90;
        persistentVolume->m_chunkList.push_back(chunkInfo);
    }

    BOOST_REQUIRE(persistentVolume->totalSize() == sum);

    std::cout << "done." << std::endl;
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_getChunks)
{
    std::cout << get_name() << "...... " << std::flush;
    auto persistentVolume = seastar::make_lw_shared<PersistentVolume>(plogBaseDir.c_str() + String(get_name()));

    for (size_t i=0; i<10; i++){
        ChunkInfo chunkInfo;
        chunkInfo.chunkId = i;
        chunkInfo.size = i*100;
        chunkInfo.actualSize = i*90;
        persistentVolume->m_chunkList.push_back(chunkInfo);
    }

    auto iter = persistentVolume->getChunks();
    size_t i = 0;

    for(; iter->isValid(); iter->advance()){
        auto chunkInfo = iter->getCurrent();
        BOOST_REQUIRE(chunkInfo.chunkId == i);
        BOOST_REQUIRE(chunkInfo.size == i*100);
        BOOST_REQUIRE(chunkInfo.actualSize == i*90);
        i++;
    }

    BOOST_REQUIRE(i==10);

    std::cout << "done." << std::endl;
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_getChunks_EmptyChunkSet)
{
    std::cout << get_name() << "...... " << std::flush;
    auto persistentVolume = seastar::make_lw_shared<PersistentVolume>(plogBaseDir.c_str() + String(get_name()));

    auto iter = persistentVolume->getChunks();
    BOOST_REQUIRE(iter->isEnd());

    std::cout << "done." << std::endl;
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_append)
{
    std::cout << get_name() << "...... " << std::flush;
    auto persistentVolume = seastar::make_lw_shared<PersistentVolume>(plogBaseDir.c_str() + String(get_name()));

    return seastar::do_with(std::vector<RecordPosition>{}, [persistentVolume](auto& recordPositions) mutable {
        return seastar::repeat([&recordPositions, persistentVolume] () mutable {
            if(persistentVolume->m_chunkList.size() >= 5)
            {
                return  seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
            }else {
                auto binary = std::make_unique<Binary>(Binary{BUFFERSIZE});
                std::fill(binary->get_write(), binary->get_write()+binary->size(), (uint8_t)(recordPositions.size()%256));

                return persistentVolume->append(std::move(*binary))
                .then([&recordPositions](auto recordPosition) {
                    recordPositions.push_back(recordPosition);
                    return  seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                });
            }
        })
        .then([&recordPositions, persistentVolume]{
            BOOST_REQUIRE(recordPositions.size() > 0);

            auto chunkId = recordPositions[0].chunkId;
            auto offset = recordPositions[0].offset;
            for(size_t i=0; i<recordPositions.size(); i++)
            {
                if(recordPositions[i].chunkId == chunkId+1)
                {
                    chunkId++;
                    offset = recordPositions[i].offset;
                }

                BOOST_REQUIRE(recordPositions[i].chunkId == chunkId);
                BOOST_REQUIRE(recordPositions[i].offset == offset);
                offset += BUFFERSIZE;
            }

            BOOST_REQUIRE(chunkId == 5);
        });
    })
    .then([persistentVolume]{
        std::cout << "done." << std::endl;
        return seastar::make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_read)
{
    std::cout << get_name() << "...... " << std::flush;
    auto persistentVolume = seastar::make_lw_shared<PersistentVolume>(plogBaseDir.c_str() + String(get_name()));

    return seastar::do_with(std::vector<RecordPosition>{}, [persistentVolume](auto& recordPositions) mutable {
        return seastar::repeat([&recordPositions, persistentVolume] () mutable {
            if(recordPositions.size() >= 30)
            {
                return  seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
            }else {
                auto binary = std::make_unique<Binary>(Binary{BUFFERSIZE});
                std::fill(binary->get_write(), binary->get_write()+binary->size(), (uint8_t)(recordPositions.size()%256));
                return persistentVolume->append(std::move(*binary))
                .then([&recordPositions](auto recordPosition) {
                    recordPositions.push_back(recordPosition);
                    //std::cout << recordPosition.chunkId << ":" << recordPosition.offset << std::endl;
                    return  seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                });
            }
        })
        .then([&recordPositions, persistentVolume]{
            std::vector<uint32_t> range(recordPositions.size());
            std::iota(range.begin(), range.end(), 0);
            auto range_ptr = seastar::make_lw_shared<std::vector<uint32_t>>(std::move(range));

            return seastar::do_for_each(range_ptr->begin(),range_ptr->end(), [&recordPositions, persistentVolume, range_ptr](auto i){
                auto buffer = seastar::make_lw_shared<Binary>() ;
                //std::cout << i << ":" << recordPositions[i].chunkId << ":" << recordPositions[i].offset << std::endl;
                return persistentVolume->read(recordPositions[i], BUFFERSIZE, *buffer)
                .then([buffer, i](auto readSize){
                    BOOST_REQUIRE(readSize == BUFFERSIZE);
                    //std::cout << (uint32_t)(i%256) << std::endl;
                    BOOST_REQUIRE(std::all_of(buffer->begin(), buffer->end(), [expectedValue{uint8_t(i%256)}](auto& value){
                        if(value != expectedValue)
                            std::cout << (uint32_t)value << ":" << (uint32_t)expectedValue << std::endl;
                        return value == expectedValue;
                    }));
                    return seastar::make_ready_future<>();
                });
            });
        });
    })
    .then([persistentVolume]{
        std::cout << "done." << std::endl;
        return seastar::make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_read_ActualSize_LT_ExpectedSize)
{
    std::cout << get_name() << "...... " << std::flush;
    auto persistentVolume = seastar::make_lw_shared<PersistentVolume>(plogBaseDir.c_str() + String(get_name()));

    return seastar::do_with(std::vector<RecordPosition>{}, [persistentVolume](auto& recordPositions) mutable {
        return seastar::repeat([&recordPositions, persistentVolume] () mutable {
            if(persistentVolume->m_chunkList.size() >= 5)
            {
                return  seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
            }else {
                auto binary = std::make_unique<Binary>(Binary{BUFFERSIZE});
                std::fill(binary->get_write(), binary->get_write()+binary->size(), (uint8_t)(recordPositions.size()%256));
                return persistentVolume->append(std::move(*binary))
                .then([&recordPositions](auto recordPosition) {
                    recordPositions.push_back(recordPosition);
                    return  seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                });
            }
        })
        .then([&recordPositions]{
            return  seastar::make_ready_future<std::pair<RecordPosition, uint8_t> >(std::pair{recordPositions.back(), (recordPositions.size()-1)%256});
        });
    })
    .then([persistentVolume](auto p){
        auto buffer = seastar::make_lw_shared<Binary>();
        return persistentVolume->read(p.first, BUFFERSIZE+1, *buffer)
        .then([buffer, expectedValue{p.second}](auto readSize){
            BOOST_REQUIRE(readSize == BUFFERSIZE); // actually read size is smaller than expected read size
            BOOST_REQUIRE(std::all_of(buffer->begin(), buffer->end(), [expectedValue](auto& value){
                return value == expectedValue;
            }));
            return seastar::make_ready_future<>();
        });
    })
    .then([persistentVolume]{
        std::cout << "done." << std::endl;
        return seastar::make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_read_ChunkIdNotFound)
{
    std::cout << get_name() << "...... " << std::flush;
    auto persistentVolume = seastar::make_lw_shared<PersistentVolume>(plogBaseDir.c_str() + String(get_name()));

    return seastar::do_with(std::vector<RecordPosition>{}, [persistentVolume](auto& recordPositions) mutable {
        return seastar::repeat([&recordPositions, persistentVolume] () mutable {
            if(persistentVolume->m_chunkList.size() >= 5)
            {
                return  seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
            }else {
                auto binary = std::make_unique<Binary>(Binary{BUFFERSIZE});
                std::fill(binary->get_write(), binary->get_write()+binary->size(), (uint8_t)(recordPositions.size()%256));
                return persistentVolume->append(std::move(*binary))
                .then([&recordPositions](auto recordPosition) {
                    recordPositions.push_back(recordPosition);
                    return  seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                });
            }
        });
    })
    .then([persistentVolume](){
        RecordPosition recordPosition{persistentVolume->m_chunkList.back().chunkId+1, 0};
        auto buffer = seastar::make_lw_shared<Binary>();
        return persistentVolume->read(recordPosition, BUFFERSIZE, *buffer)
        .then([](auto){
            BOOST_FAIL("Expected exception.");
        })
        .handle_exception([recordPosition, persistentVolume](auto e){
            try{
                std::rethrow_exception(e);
            } catch (ChunkException& e) {
                BOOST_REQUIRE(e.chunkId() == recordPosition.chunkId);
                std::cout << "done." << std::endl;  // reach here
            } catch (...) {
                BOOST_FAIL("Incorrect exception type.");
            }
        });
     })
    .then([persistentVolume]{
        return seastar::make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_drop)
{
    std::cout << get_name() << "...... " << std::flush;
    auto persistentVolume = seastar::make_lw_shared<PersistentVolume>(plogBaseDir.c_str() + String(get_name()));

    return seastar::do_with(std::vector<RecordPosition>{}, [persistentVolume](auto& recordPositions) mutable {
        return seastar::repeat([&recordPositions, persistentVolume] () mutable {
            if(persistentVolume->m_chunkList.size() >= 5)
            {
                return  seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
            }else {
                auto binary = std::make_unique<Binary>(Binary{BUFFERSIZE});
                std::fill(binary->get_write(), binary->get_write()+binary->size(), (uint8_t)(recordPositions.size()%256));
                return persistentVolume->append(std::move(*binary))
                .then([&recordPositions](auto recordPosition) {
                    recordPositions.push_back(recordPosition);
                    return  seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                });
            }
        });
    })
    .then([persistentVolume]{
        srand (time(NULL));
        auto i = rand() % persistentVolume->m_chunkList.size();
        auto chunkId = persistentVolume->m_chunkList[i].chunkId;
        return persistentVolume->drop(chunkId)
        .then([chunkId, persistentVolume]{
            BOOST_REQUIRE(none_of(persistentVolume->m_chunkList.begin(), persistentVolume->m_chunkList.end(),[chunkId](auto& chunkInfo){
                return chunkInfo.chunkId == chunkId;
            }));
        });
    })
    .then([persistentVolume]{
        std::cout << "done." << std::endl;
        return seastar::make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_drop_ChunkIdNotFound)
{
    std::cout << get_name() << "...... " << std::flush;
    auto persistentVolume = seastar::make_lw_shared<PersistentVolume>(plogBaseDir.c_str() + String(get_name()));

    for (size_t i=0; i<2; i++){
        ChunkInfo chunkInfo;
        chunkInfo.chunkId = i;
        chunkInfo.size = 100;
        chunkInfo.actualSize = 100;
        persistentVolume->m_chunkList.push_back(chunkInfo);
    }

    return persistentVolume->drop(persistentVolume->m_chunkList.back().chunkId+1)
    .then([]{
        BOOST_FAIL("Expected exception.");
    })
    .handle_exception([persistentVolume](auto e){
        try{
            std::rethrow_exception(e);
        } catch (ChunkException& e) {
            BOOST_REQUIRE(e.chunkId() == persistentVolume->m_chunkList.back().chunkId+1);
            std::cout << "done." << std::endl;  // reach here
        } catch (...) {
            BOOST_FAIL("Incorrect exception type.");
        }
    })
    .then([persistentVolume]{
        return seastar::make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(Remove_test_folders)
{
    std::cout << get_name() << std::endl;
    std::cout << "Remove folder " << plogBaseDir << std::endl;

    if(std::filesystem::exists(plogBaseDir)){
        std::filesystem::remove_all(plogBaseDir);
    }

    return seastar::make_ready_future<>();
}