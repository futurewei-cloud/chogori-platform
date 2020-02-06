#define CATCH_CONFIG_MAIN
// std
#include <vector>
// catch
#include <catch2/catch.hpp>
// k2
#include <k2/k2types/PartitionMetadata.h>
// k2:client
#include <k2/client/IClient.h>
#include <k2/client/PartitionMap.h>

using namespace k2;
using namespace k2::client;


PartitionDescription createPartition(PartitionRange&& range, const std::string& endpoint)
{
    PartitionDescription desc;
    desc.nodeEndpoint = endpoint;
    desc.range = std::move(range);

    return desc;
}

PartitionRange createPartitionRange(const std::string& lowKey, const std::string& highKey)
{
    PartitionRange range;
    range.lowKey = lowKey;
    range.highKey = highKey;

    return range;
}

client::PartitionMap& addPartition(client::PartitionMap& partitionMap, PartitionDescription&& partition)
{
    partitionMap.map.insert(std::move(partition));

    return partitionMap;
}

std::vector<PartitionDescription> getPartitionsForRange(const Range& range, client::PartitionMap& partitionMap)
{
    std::vector<PartitionDescription> partitions;
    auto it = partitionMap.find(range);

    for(;it!=partitionMap.end(); ++it) {
        partitions.push_back(*it);
    }

    return partitions;
}

SCENARIO("Iterator", "[partitionMap]")
{
    WHEN("There are no partitions")
    {
         client::PartitionMap partitionMap;

        THEN("If we try to find a partition it should return the end iterator")
        {
            REQUIRE(partitionMap.find(Range::singleKey("a")) == partitionMap.end());
        }
    }
    WHEN("There's a single partition")
    {
        client::PartitionMap partitionMap;
        std::string endpoint = "endpoint1";
        std::string endpoint2 = "endpoint2";
        addPartition(partitionMap, createPartition(createPartitionRange("b", "c"), endpoint));
        addPartition(partitionMap, createPartition(createPartitionRange("d", ""), endpoint2));

        THEN("We expect to find it")
        {
            auto it = partitionMap.find(Range::singleKey("b"));
            REQUIRE((*it).nodeEndpoint == endpoint);
        }
        THEN("The iterator should point at the end if we increment it")
        {
            auto it = partitionMap.find(Range::singleKey("d"));
            ++it;
            REQUIRE(it==partitionMap.end());
        }
        THEN("Two iterators pointing at the end should be equal")
        {
            auto it1 = partitionMap.find(Range::singleKey("d"));
            ++it1;
            auto it2 = partitionMap.find(Range::singleKey("d"));
            ++it2;
            REQUIRE(it1 == it2);
        }
        THEN("Two iterators pointing at the same partition are equal")
        {
            auto it1 = partitionMap.find(Range::singleKey("b"));
            auto it2 = partitionMap.find(Range::singleKey("b"));
            REQUIRE(it1 == it2);
            REQUIRE(it1 != partitionMap.end());
        }
         THEN("Two iterators pointing at different partition are not equal")
        {
            auto it1 = partitionMap.find(Range::singleKey("b"));
            auto it2 = partitionMap.find(Range::singleKey("d"));
            REQUIRE(it1 != it2);
        }
    }
}

SCENARIO("Partition", "[client]")
{
    WHEN("There are no partitions")
    {
        client::PartitionMap partitionMap;

        THEN("We should not find a range")
        {
            REQUIRE(getPartitionsForRange(Range::singleKey("a"), partitionMap).empty() == true);
        }
    }
    WHEN("There's a single partition")
    {
        client::PartitionMap partitionMap;
        std::string endpoint = "endpoint1";
        addPartition(partitionMap, createPartition(createPartitionRange("c", "f"), endpoint));

        THEN("We expect to find that range when the lower bounds are equal (inclusive)")
        {
            REQUIRE(getPartitionsForRange(Range::singleKey("c"), partitionMap)[0].nodeEndpoint == endpoint);
        }
        THEN("We expect to find that range when the lower and upper bounds are equal (inclusive)")
        {
            REQUIRE(getPartitionsForRange(Range::close("c", "f"), partitionMap)[0].nodeEndpoint == endpoint);
        }
        THEN("We expect to find that partition when the range is less or equal to a key greater than the upper bound")
        {
            REQUIRE(getPartitionsForRange(Range::lessOrEqual("g"), partitionMap)[0].nodeEndpoint == endpoint);
        }
        THEN("We expect to find that partition when the range is less or equal to a key is equal to the upper bound")
        {
            REQUIRE(getPartitionsForRange(Range::lessOrEqual("f"), partitionMap)[0].nodeEndpoint == endpoint);
        }
        THEN("We expect to find that partition when the range is less or equal to a key is in the middle of the partition")
        {
            REQUIRE(getPartitionsForRange(Range::lessOrEqual("d"), partitionMap)[0].nodeEndpoint == endpoint);
        }
        THEN("We expect to find that partition when the range is less or equal to a key is equal to the lower bound")
        {
            REQUIRE(getPartitionsForRange(Range::lessOrEqual("c"), partitionMap)[0].nodeEndpoint == endpoint);
        }
        THEN("We expect to find that partition when the range is greater or equal to a key which smaller than the lower bound")
        {
            REQUIRE(getPartitionsForRange(Range::greaterOrEqual("b"), partitionMap)[0].nodeEndpoint == endpoint);
        }
        THEN("We expect to find that partition when the range is greater or equal to a key which is equal to the lower bound")
        {
            REQUIRE(getPartitionsForRange(Range::greaterOrEqual("c"), partitionMap)[0].nodeEndpoint== endpoint);
        }
        THEN("We expect to find that partition when the range is greater or equal to a key which is in the middle of the partition")
        {
            REQUIRE(getPartitionsForRange(Range::greaterOrEqual("d"), partitionMap)[0].nodeEndpoint == endpoint);
        }
        THEN("We expect to find that range when the lower and upper bounds are equal (non inclusive)")
        {
            REQUIRE(getPartitionsForRange(Range::open("c", "f"), partitionMap)[0].nodeEndpoint == endpoint);
        }
        THEN("We expect to find that range when the lower and upper bounds are greater")
        {
            REQUIRE(getPartitionsForRange(Range::close("a", "g"), partitionMap)[0].nodeEndpoint == endpoint);
        }
        THEN("We expect to find that range when the lower and upper bounds are smaller")
        {
            REQUIRE(getPartitionsForRange(Range::close("d", "e"), partitionMap)[0].nodeEndpoint== endpoint);
        }
        THEN("We expect to find that range when the lower bound is in the middle and the upper bound is greater")
        {
            REQUIRE(getPartitionsForRange(Range::close("d", "g"), partitionMap)[0].nodeEndpoint == endpoint);
        }
        THEN("We expect to find that range when the upper bound is in the middle and the lower bound is smaller")
        {
            REQUIRE(getPartitionsForRange(Range::close("a", "d"), partitionMap)[0].nodeEndpoint == endpoint);
        }
        THEN("We do not expect to find the range when the upper bound is greater")
        {
            REQUIRE(getPartitionsForRange(Range::singleKey("g"), partitionMap).empty() == true);
        }
        THEN("We do not expect to find the partition when the range is less or equal to a key which is smaller than the lower bound")
        {
            REQUIRE(getPartitionsForRange(Range::lessOrEqual("b"), partitionMap).empty() == true);
        }
        THEN("We expect not to find that partition when the range is greater or equal to a key which is greater than the upper bound")
        {
            REQUIRE(getPartitionsForRange(Range::greaterOrEqual("g"), partitionMap).empty() == true);
        }
        THEN("We expect not to find a partition when the range is greater to a key which is equal to the upper bound (non inclusive)")
        {
            REQUIRE(getPartitionsForRange(Range::greater("f"), partitionMap).empty() == true);
        }
        THEN("We expect not to find a partition when the range is less to a key which is equal to the lower bound (non inclusive)")
        {
            REQUIRE(getPartitionsForRange(Range::less("c"), partitionMap).empty() == true);
        }
        THEN("We do not expect to find the range when the upper bound is greater")
        {
            REQUIRE(getPartitionsForRange(Range::open("f", "g"), partitionMap).empty() == true);
        }
        THEN("We do not expect to find that range when the upper bounds are equal (upper partition bound is not inclusive)")
        {
            REQUIRE(getPartitionsForRange(Range::singleKey("f"), partitionMap).empty() == true);
        }
        THEN("We do not expect to find that partition when the range is greater or equal to a key which is equal to the upper bound")
        {
            REQUIRE(getPartitionsForRange(Range::greaterOrEqual("f"), partitionMap).empty() == true);
        }
    }
    WHEN("There are two consecutive closed partitions")
    {
        client::PartitionMap partitionMap;
        std::string endpoint1 = "endpoint1";
        std::string endpoint2 = "endpoint2";
        addPartition(partitionMap, createPartition(createPartitionRange("b", "d"), endpoint1));
        addPartition(partitionMap, createPartition(createPartitionRange("d", "g"), endpoint2));

        THEN("We expect to find the partition when the range is in between the second partition")
        {
            REQUIRE(getPartitionsForRange(Range::singleKey("f"), partitionMap)[0].nodeEndpoint == endpoint2);
        }
        THEN("We expect to find the second partition when the range is in between the second partition lower bound and less then the higher bound")
        {
            auto partitions = getPartitionsForRange(Range::close("d", "e"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint2);
        }
        THEN("We expect to find the second partition when the range is in between the second partition lower bound and less then the higher bound (open)")
        {
           auto partitions = getPartitionsForRange(Range::open("d", "e"), partitionMap);
           REQUIRE(partitions[0].nodeEndpoint == endpoint2);
        }
        THEN("We to find the partition when a single key falls on the lower bound of the second partition")
        {
           auto partitions = getPartitionsForRange(Range::singleKey("d"), partitionMap);
           REQUIRE(partitions[0].nodeEndpoint == endpoint2);
        }
        THEN("We to find both partitions when the closed range falls between first low key and last high key")
        {
           auto partitions = getPartitionsForRange(Range::close("b", "g"), partitionMap);
           REQUIRE(partitions[0].nodeEndpoint == endpoint1);
           REQUIRE(partitions[1].nodeEndpoint == endpoint2);
        }
        THEN("We to find both partitions when the open range falls within the first low key and last high key")
        {
           auto partitions = getPartitionsForRange(Range::open("b", "g"), partitionMap);
           REQUIRE(partitions[0].nodeEndpoint == endpoint1);
           REQUIRE(partitions[1].nodeEndpoint == endpoint2);
        }
        THEN("We to find both partitions when the closed range falls within the first low key and last high key")
        {
           auto partitions = getPartitionsForRange(Range::close("c", "f"), partitionMap);
           REQUIRE(partitions[0].nodeEndpoint == endpoint1);
           REQUIRE(partitions[1].nodeEndpoint == endpoint2);
        }
        THEN("We to find both partitions when the open range falls within the first low key and last high key")
        {
           auto partitions = getPartitionsForRange(Range::open("c", "f"), partitionMap);
           REQUIRE(partitions[0].nodeEndpoint == endpoint1);
           REQUIRE(partitions[1].nodeEndpoint == endpoint2);
        }
    }
     WHEN("There are four consecutive open partitions")
    {
        client::PartitionMap partitionMap;
        std::string endpoint1 = "endpoint1";
        std::string endpoint2 = "endpoint2";
        std::string endpoint3 = "endpoint3";
        std::string endpoint4 = "endpoint4";
        addPartition(partitionMap, createPartition(createPartitionRange("", "b"), endpoint1));
        addPartition(partitionMap, createPartition(createPartitionRange("b", "d"), endpoint2));
        addPartition(partitionMap, createPartition(createPartitionRange("d", "g"), endpoint3));
        addPartition(partitionMap, createPartition(createPartitionRange("g", ""), endpoint4));

        THEN("We expect to find the first partition when the low key of the first partition is empty")
        {   auto partitions = getPartitionsForRange(Range::singleKey("a"), partitionMap);
            REQUIRE(partitions.size() == 1);
            REQUIRE(partitions[0].nodeEndpoint == endpoint1);
        }
        THEN("We expect to find the lat partition when the high key of the last partition is empty")
        {
            REQUIRE(getPartitionsForRange(Range::singleKey("z"), partitionMap)[0].nodeEndpoint == endpoint4);
        }
        THEN("We expect to find all partitions when the range spans all partitions")
        {
            auto partitions = getPartitionsForRange(Range::close("a", "z"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint1);
            REQUIRE(partitions[1].nodeEndpoint == endpoint2);
            REQUIRE(partitions[2].nodeEndpoint == endpoint3);
            REQUIRE(partitions[3].nodeEndpoint == endpoint4);
        }
        THEN("We expect to find the partition for a closed range")
        {
            auto partitions = getPartitionsForRange(Range::close("b", "c"), partitionMap);
            REQUIRE(partitions.size() == 1);
            REQUIRE(partitions[0].nodeEndpoint == endpoint2);
        }
        THEN("We expect to find all partitions when the range spans all partitions")
        {
            auto partitions = getPartitionsForRange(Range::lessOrEqual("z"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint1);
            REQUIRE(partitions[1].nodeEndpoint == endpoint2);
            REQUIRE(partitions[2].nodeEndpoint == endpoint3);
            REQUIRE(partitions[3].nodeEndpoint == endpoint4);
        }
        THEN("We expect to find all partitions when the range spans all partitions")
        {
            auto partitions = getPartitionsForRange(Range::greaterOrEqual("a"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint1);
            REQUIRE(partitions[1].nodeEndpoint == endpoint2);
            REQUIRE(partitions[2].nodeEndpoint == endpoint3);
            REQUIRE(partitions[3].nodeEndpoint == endpoint4);
        }
        THEN("We expect to find the last three partitions")
        {
            auto partitions = getPartitionsForRange(Range::greaterOrEqual("c"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint2);
            REQUIRE(partitions[1].nodeEndpoint == endpoint3);
            REQUIRE(partitions[2].nodeEndpoint == endpoint4);
        }
        THEN("We expect to find the first three partitions")
        {
            auto partitions = getPartitionsForRange(Range::lessOrEqual("e"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint1);
            REQUIRE(partitions[1].nodeEndpoint == endpoint2);
            REQUIRE(partitions[2].nodeEndpoint == endpoint3);
        }
    }
    WHEN("There are two non consecutive partitions")
    {
        client::PartitionMap partitionMap;
        std::string endpoint1 = "endpoint1";
        std::string endpoint2 = "endpoint2";
        addPartition(partitionMap, createPartition(createPartitionRange("b", "d"), endpoint1));
        addPartition(partitionMap, createPartition(createPartitionRange("f", "h"), endpoint2));

        THEN("We expect to find the range if a single key falls into the second partition")
        {
            auto partitions = getPartitionsForRange(Range::singleKey("g"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint2);
        }
        THEN("We expect to find the range if range is between partitions")
        {
            auto partitions = getPartitionsForRange(Range::close("c", "g"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint1);
            REQUIRE(partitions[1].nodeEndpoint == endpoint2);
        }
        THEN("We expect to find that range if it includes both partitions")
        {
            auto partitions = getPartitionsForRange(Range::close("b", "h"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint1);
            REQUIRE(partitions[1].nodeEndpoint == endpoint2);
        }
        THEN("We expect to find the second partition if the range if it includes the lower bound")
        {
            auto partitions = getPartitionsForRange(Range::close("d", "f"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint2);
        }
        THEN("We expect to find a single partition when it touches it on the upper lower")
        {
            auto partitions = getPartitionsForRange(Range::close("e", "f"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint2);
            REQUIRE(partitions.size() == 1);
        }
        THEN("We expect to include all partitions when the range is greater or equal to a key that is smaller than the lower bound")
        {
            auto partitions = getPartitionsForRange(Range::greaterOrEqual("a"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint1);
            REQUIRE(partitions[1].nodeEndpoint == endpoint2);
        }
        THEN("We expect to include all partitions when the range is less or equal to a key that is greater than the high bound")
        {
            auto partitions = getPartitionsForRange(Range::lessOrEqual("k"), partitionMap);
            // the order will be in reverse
            REQUIRE(partitions[0].nodeEndpoint == endpoint1);
            REQUIRE(partitions[1].nodeEndpoint == endpoint2);
        }
        THEN("We do not expect to find the partition if a single key falls outside the second partition")
        {
            auto partitions = getPartitionsForRange(Range::singleKey("k"), partitionMap);
            REQUIRE(partitions.empty() == true);
        }
        THEN("We do not expect to find a single partition when it touches on the outter higher bound (open)")
        {
            auto partitions = getPartitionsForRange(Range::open("h", "k"), partitionMap);
            REQUIRE(partitions.empty() == true);
        }
        THEN("We do not expect to find a partition when the range lower bound is equal to the last partition upper bound and the range upper bound is greater")
        {
            auto partitions = getPartitionsForRange(Range::close("h", "k"), partitionMap);
            REQUIRE(partitions.empty() == true);
        }
    }
    WHEN("There are three consecutive partitions")
    {
        client::PartitionMap partitionMap;
        std::string endpoint1 = "endpoint1";
        std::string endpoint2 = "endpoint2";
        std::string endpoint3 = "endpoint3";
        addPartition(partitionMap, createPartition(createPartitionRange("b", "d"), endpoint1));
        addPartition(partitionMap, createPartition(createPartitionRange("e", "h"), endpoint2));
        addPartition(partitionMap, createPartition(createPartitionRange("i", "k"), endpoint3));

        THEN("Scan all partitions from the lower bound")
        {
            auto partitions = getPartitionsForRange(Range::greaterOrEqual("a"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint1);
            REQUIRE(partitions[1].nodeEndpoint == endpoint2);
            REQUIRE(partitions[2].nodeEndpoint == endpoint3);
        }
        THEN("Scan all partitions from the upper bound")
        {
            auto partitions = getPartitionsForRange(Range::lessOrEqual("m"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint1);
            REQUIRE(partitions[1].nodeEndpoint == endpoint2);
            REQUIRE(partitions[2].nodeEndpoint == endpoint3);
        }
        THEN("Get all partitions from the lower and upper bound")
        {
            auto partitions = getPartitionsForRange(Range::close("a", "m"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint1);
            REQUIRE(partitions[1].nodeEndpoint == endpoint2);
            REQUIRE(partitions[2].nodeEndpoint == endpoint3);
        }
        THEN("Get the 2 left partitions")
        {
            auto partitions = getPartitionsForRange(Range::close("a", "h"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint1);
            REQUIRE(partitions[1].nodeEndpoint == endpoint2);
            REQUIRE(partitions.size()==2);
        }
        THEN("Get the 2 right partitions")
        {
            auto partitions = getPartitionsForRange(Range::close("e", "m"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint2);
            REQUIRE(partitions[1].nodeEndpoint == endpoint3);
            REQUIRE(partitions.size()==2);
        }
        THEN("Get middle partitions")
        {
            auto partitions = getPartitionsForRange(Range::close("e", "h"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint2);
            REQUIRE(partitions.size()==1);
        }
        THEN("Get middle partition")
        {
            auto partitions = getPartitionsForRange(Range::singleKey("f"), partitionMap);
            REQUIRE(partitions[0].nodeEndpoint == endpoint2);
            REQUIRE(partitions.size()==1);
        }
    }
}
