/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#define CATCH_CONFIG_MAIN

#include <k2/module/k23si/Indexer.h>
#include "Log.h"
#include "catch2/catch.hpp"
namespace k2 {
SCENARIO("test01 empty indexer") {
    auto indexer = Indexer();
    indexer.start(dto::Timestamp{.endCount=100000, .tsoId=1, .startDelta=1000}).get0();
    REQUIRE(indexer.size() == 0);
    REQUIRE_THROWS(indexer.find(dto::Key{}));
}

SCENARIO("test02 empty schema") {
    auto indexer = Indexer();
    dto::Timestamp older{.endCount=50000, .tsoId=1, .startDelta=1000};
    dto::Timestamp start{.endCount=60000, .tsoId=1, .startDelta=1000};
    dto::Timestamp newer{.endCount=70000, .tsoId=1, .startDelta=1000};
    dto::Timestamp newest{.endCount=80000, .tsoId=1, .startDelta=1000};
    indexer.start(start).get0();
    dto::Schema sch;
    sch.name = "schema1";
    dto::Key k1{.schemaName = sch.name, .partitionKey = "Key1", .rangeKey = "rKey1"};

    indexer.createSchema(sch);
    REQUIRE(indexer.size() == 0);
    {
        auto iter = indexer.find(k1);
        REQUIRE(!iter.hasData());
        REQUIRE(iter.atEnd());
        REQUIRE(iter.getWI() == nullptr);
        REQUIRE(iter.getLastCommittedTime() == dto::Timestamp::ZERO);
        REQUIRE(iter.getAllDataRecords().size() == 0);
        REQUIRE(iter.getLastReadTime() == start);
        REQUIRE(iter.getLatestDataRecord() == nullptr);
        iter.observeAt(older);
        REQUIRE(iter.getLastReadTime() == start);
        iter.observeAt(start);
        REQUIRE(iter.getLastReadTime() == start);
        iter.observeAt(newer);
        REQUIRE(iter.getLastReadTime() == newer);
    }
    {
        auto iter = indexer.find(k1, true);
        REQUIRE(!iter.hasData());
        REQUIRE(iter.atEnd());
        REQUIRE(iter.getWI() == nullptr);
        REQUIRE(iter.getLastCommittedTime() == dto::Timestamp::ZERO);
        REQUIRE(iter.getAllDataRecords().size() == 0);
        REQUIRE(iter.getLastReadTime() == newer);
        REQUIRE(iter.getLatestDataRecord() == nullptr);
        iter.observeAt(older);
        REQUIRE(iter.getLastReadTime() == newer);
        iter.observeAt(newer);
        REQUIRE(iter.getLastReadTime() == newer);
        iter.observeAt(newest);
        REQUIRE(iter.getLastReadTime() == newest);
    }
    REQUIRE(indexer.size() == 0);
}

SCENARIO("test03 add, abort, commit new key") {
    auto indexer = Indexer();

    dto::Timestamp older{.endCount = 50000, .tsoId = 1, .startDelta = 1000};
    dto::Timestamp start{.endCount = 60000, .tsoId = 1, .startDelta = 1000};
    dto::Timestamp newer{.endCount = 70000, .tsoId = 1, .startDelta = 1000};
    dto::Timestamp newest{.endCount = 80000, .tsoId = 1, .startDelta = 1000};
    indexer.start(start).get0();
    dto::Schema sch;
    sch.name = "schema1";
    dto::Key k1{.schemaName = sch.name, .partitionKey = "KeyAAA", .rangeKey = "rKey1"};
    dto::Key k2{.schemaName = sch.name, .partitionKey = "KeyABA", .rangeKey = "rKey1"};
    dto::Key k3{.schemaName = sch.name, .partitionKey = "KeyACA", .rangeKey = "rKey1"};

    indexer.createSchema(sch);
    REQUIRE(indexer.size() == 0);
    {
        auto iter = indexer.find(k2);
        dto::DataRecord rec;
        rec.isTombstone = false;
        rec.timestamp = newest;

        iter.addWI(k2, std::move(rec), 10);
        REQUIRE(iter.hasData());
        REQUIRE(!iter.atEnd());
        REQUIRE(iter.getWI() != nullptr);
        REQUIRE(iter.getLastCommittedTime() == dto::Timestamp::ZERO);
        REQUIRE(iter.getAllDataRecords().size() == 1);
        REQUIRE(iter.getLastReadTime() == start);
        REQUIRE(iter.getLatestDataRecord() != nullptr);
        REQUIRE(iter.getKey() == k2);
        iter.observeAt(older);
        REQUIRE(iter.getLastReadTime() == start);
        iter.observeAt(start);
        REQUIRE(iter.getLastReadTime() == start);
        iter.observeAt(newer);
        REQUIRE(iter.getLastReadTime() == newer);
    }
    REQUIRE(indexer.size() == 1);
    {
        auto iter = indexer.find(k2);
        REQUIRE(iter.hasData());
        REQUIRE(!iter.atEnd());
        REQUIRE(iter.getWI() != nullptr);
        REQUIRE(iter.getLastCommittedTime() == dto::Timestamp::ZERO);
        REQUIRE(iter.getAllDataRecords().size() == 1);
        REQUIRE(iter.getLastReadTime() == newer);
        REQUIRE(iter.getLatestDataRecord() != nullptr);
        REQUIRE(iter.getLatestDataRecord()->timestamp == iter.getWI()->data.timestamp);
        REQUIRE(iter.getKey() == k2);

        iter.abortWI();
        REQUIRE(!iter.hasData());
        REQUIRE(iter.atEnd());
        REQUIRE(iter.getWI() == nullptr);
        REQUIRE(iter.getLastCommittedTime() == dto::Timestamp::ZERO);
        REQUIRE(iter.getAllDataRecords().size() == 0);
        REQUIRE(iter.getLastReadTime() == newer);
        REQUIRE(iter.getLatestDataRecord() == nullptr);
    }
    REQUIRE(indexer.size() == 0);
    {
        auto iter = indexer.find(k2);
        REQUIRE(!iter.hasData());
        REQUIRE(iter.atEnd());
        REQUIRE(iter.getWI() == nullptr);
        REQUIRE(iter.getLastCommittedTime() == dto::Timestamp::ZERO);
        REQUIRE(iter.getAllDataRecords().size() == 0);
        REQUIRE(iter.getLastReadTime() == newer);
        REQUIRE(iter.getLatestDataRecord() == nullptr);

        dto::DataRecord rec;
        rec.isTombstone = false;
        rec.timestamp = newest;

        iter.addWI(k2, std::move(rec), 10);
        REQUIRE(iter.hasData());
        REQUIRE(!iter.atEnd());
        REQUIRE(iter.getWI() != nullptr);
        REQUIRE(iter.getLastCommittedTime() == dto::Timestamp::ZERO);
        REQUIRE(iter.getAllDataRecords().size() == 1);
        REQUIRE(iter.getLastReadTime() == newer);
        REQUIRE(iter.getLatestDataRecord() != nullptr);
        REQUIRE(iter.getKey() == k2);
    }
    REQUIRE(indexer.size() == 1);
    {
        auto iter = indexer.find(k2);
        REQUIRE(iter.hasData());
        REQUIRE(!iter.atEnd());
        REQUIRE(iter.getWI() != nullptr);
        REQUIRE(iter.getLastCommittedTime() == dto::Timestamp::ZERO);
        REQUIRE(iter.getAllDataRecords().size() == 1);
        REQUIRE(iter.getLastReadTime() == newer);
        REQUIRE(iter.getLatestDataRecord() != nullptr);

        iter.commitWI();
        REQUIRE(iter.hasData());
        REQUIRE(!iter.atEnd());
        REQUIRE(iter.getWI() == nullptr);
        REQUIRE(iter.getLastCommittedTime() == newest);
        REQUIRE(iter.getAllDataRecords().size() == 1);
        REQUIRE(iter.getLastReadTime() == newer);
        REQUIRE(iter.getLatestDataRecord() != nullptr);
        REQUIRE(iter.getLatestDataRecord()->timestamp == newest);
        REQUIRE(iter.getKey() == k2);
    }
}

SCENARIO("test04 add, abort, commit with existing key") {
    auto indexer = Indexer();
    dto::Timestamp start{.endCount = 60000, .tsoId = 1, .startDelta = 1000};
    dto::Timestamp newer{.endCount = 70000, .tsoId = 1, .startDelta = 1000};
    dto::Timestamp newest{.endCount = 80000, .tsoId = 1, .startDelta = 1000};
    indexer.start(start).get0();
    dto::Schema sch;
    sch.name = "schema1";
    dto::Key k1{.schemaName = sch.name, .partitionKey = "KeyAAA", .rangeKey = "rKey1"};
    dto::Key k2{.schemaName = sch.name, .partitionKey = "KeyABA", .rangeKey = "rKey1"};
    dto::Key k3{.schemaName = sch.name, .partitionKey = "KeyACA", .rangeKey = "rKey1"};

    indexer.createSchema(sch);
    REQUIRE(indexer.size() == 0);
    {
        auto iter = indexer.find(k2);
        dto::DataRecord rec;
        rec.isTombstone = false;
        rec.timestamp = newer;

        iter.addWI(k2, std::move(rec), 10);
        iter.commitWI();
        REQUIRE(iter.getKey() == k2);
    }
    REQUIRE(indexer.size() == 1);
    {
        auto iter = indexer.find(k2);
        dto::DataRecord rec;
        rec.isTombstone = false;
        rec.timestamp = newest;

        iter.addWI(k2, std::move(rec), 11);
        REQUIRE(iter.hasData());
        REQUIRE(!iter.atEnd());
        REQUIRE(iter.getWI() != nullptr);
        REQUIRE(iter.getLastCommittedTime() == newer);
        REQUIRE(iter.getAllDataRecords().size() == 2);
        REQUIRE(iter.getLastReadTime() == start);
        REQUIRE(iter.getLatestDataRecord()->timestamp == newest);
        REQUIRE(iter.getKey() == k2);
    }
    REQUIRE(indexer.size() == 1);
    {
        auto iter = indexer.find(k2);
        iter.observeAt(newer);
        iter.abortWI();
        REQUIRE(iter.hasData());
        REQUIRE(!iter.atEnd());
        REQUIRE(iter.getWI() == nullptr);
        REQUIRE(iter.getLastCommittedTime() == newer);
        REQUIRE(iter.getAllDataRecords().size() == 1);
        REQUIRE(iter.getLastReadTime() == newer);
        REQUIRE(iter.getLatestDataRecord()->timestamp == newer); // back to previous version
        REQUIRE(iter.getKey() == k2);
    }
    REQUIRE(indexer.size() == 1);
    {
        auto iter = indexer.find(k2);
        dto::DataRecord rec;
        rec.isTombstone = false;
        rec.timestamp = newest;

        iter.addWI(k2, std::move(rec), 11);
        iter.commitWI();
        REQUIRE(iter.hasData());
        REQUIRE(!iter.atEnd());
        REQUIRE(iter.getWI() == nullptr);
        REQUIRE(iter.getLastCommittedTime() == newest);
        REQUIRE(iter.getAllDataRecords().size() == 2);
        REQUIRE(iter.getLastReadTime() == newer);
        REQUIRE(iter.getLatestDataRecord()->timestamp == newest);
        REQUIRE(iter.getKey() == k2);
    }
}

SCENARIO("test05 read in multiple versions") {
    auto indexer = Indexer();
    dto::Timestamp oldest{.endCount=40000, .tsoId=1, .startDelta=1000};
    dto::Timestamp older{.endCount = 50000, .tsoId = 1, .startDelta = 1000};
    dto::Timestamp older_p{.endCount = 50005, .tsoId = 1, .startDelta = 1000};
    dto::Timestamp start{.endCount=60000, .tsoId=1, .startDelta=1000};
    dto::Timestamp start_p{.endCount=60005, .tsoId=1, .startDelta=1000};
    dto::Timestamp newer{.endCount = 70000, .tsoId = 1, .startDelta = 1000};
    dto::Timestamp newer_p{.endCount = 70005, .tsoId = 1, .startDelta = 1000};
    dto::Timestamp newest{.endCount = 80000, .tsoId = 1, .startDelta = 1000};
    dto::Timestamp newest_p{.endCount = 80005, .tsoId = 1, .startDelta = 1000};

    indexer.start(start).get0();
    dto::Schema sch;
    sch.name = "schema1";
    dto::Key k1{.schemaName = sch.name, .partitionKey = "KeyAAA", .rangeKey = "rKey1"};

    indexer.createSchema(sch);
    {   // insert data
        auto iter = indexer.find(k1);
        {
            dto::DataRecord rec;
            rec.isTombstone = false;
            rec.timestamp = older;
            iter.addWI(k1, std::move(rec), 10);
            iter.commitWI();
        }
        {
            dto::DataRecord rec;
            rec.isTombstone = false;
            rec.timestamp = start;
            iter.addWI(k1, std::move(rec), 10);
            iter.commitWI();
        }
        {
            dto::DataRecord rec;
            rec.isTombstone = false;
            rec.timestamp = newer;
            iter.addWI(k1, std::move(rec), 10);
            iter.commitWI();
        }
        {
            dto::DataRecord rec;
            rec.isTombstone = false;
            rec.timestamp = newest;
            iter.addWI(k1, std::move(rec), 10);
            // leave newest as a WI
        }
    }

    {
        auto iter = indexer.find(k1);
        REQUIRE(iter.hasData());
        REQUIRE(iter.getWI()->data.timestamp == newest);
        REQUIRE(iter.getLastCommittedTime() == newer);
        REQUIRE(iter.getAllDataRecords().size() == 4);
        REQUIRE(iter.getLastReadTime() == start);
        REQUIRE(iter.getLatestDataRecord()->timestamp == newest);
        REQUIRE(iter.getKey() == k1);
        if (auto [rec, conflict] = iter.getDataRecordAt(newest_p); true) {
            REQUIRE(rec->timestamp == newest);
            REQUIRE(conflict);
        }
        if (auto [rec, conflict] = iter.getDataRecordAt(newest); true) {
            REQUIRE(rec->timestamp == newest);
            REQUIRE(!conflict);
        }
        if (auto [rec, conflict] = iter.getDataRecordAt(newer_p); true) {
            REQUIRE(rec->timestamp == newer);
            REQUIRE(!conflict);
        }
        if (auto [rec, conflict] = iter.getDataRecordAt(newer); true) {
            REQUIRE(rec->timestamp == newer);
            REQUIRE(!conflict);
        }
        if (auto [rec, conflict] = iter.getDataRecordAt(start_p); true) {
            REQUIRE(rec->timestamp == start);
            REQUIRE(!conflict);
        }
        if (auto [rec, conflict] = iter.getDataRecordAt(start); true) {
            REQUIRE(rec->timestamp == start);
            REQUIRE(!conflict);
        }
        if (auto [rec, conflict] = iter.getDataRecordAt(older_p); true) {
            REQUIRE(rec->timestamp == older);
            REQUIRE(!conflict);
        }
        if (auto [rec, conflict] = iter.getDataRecordAt(older); true) {
            REQUIRE(rec->timestamp == older);
            REQUIRE(!conflict);
        }
        if (auto [rec, conflict] = iter.getDataRecordAt(oldest); true) {
            REQUIRE(rec == nullptr);
            REQUIRE(!conflict);
        }
    }
}

SCENARIO("test 06 multi-key iterate and observation") {
    auto indexer = Indexer();
    std::vector<dto::Timestamp> ts;
    for (uint32_t i = 1000; i < 1020; ++i) {
        ts.push_back(dto::Timestamp{.endCount=i, .tsoId=1, .startDelta=1000});
    }
    indexer.start(ts[0]).get0();
    dto::Schema sch;
    sch.name = "schema1";
    indexer.createSchema(sch);

    dto::Key k0{.schemaName = sch.name, .partitionKey = "Key", .rangeKey = "rKey1"};
    dto::Key k1{.schemaName = sch.name, .partitionKey = "KeyAAA", .rangeKey = "rKey1"};
    dto::Key k1_p{.schemaName = sch.name, .partitionKey = "KeyAAB", .rangeKey = "rKey1"};
    dto::Key k2{.schemaName = sch.name, .partitionKey = "KeyABA", .rangeKey = "rKey1"};
    dto::Key k2_p{.schemaName = sch.name, .partitionKey = "KeyABB", .rangeKey = "rKey1"};
    dto::Key k3{.schemaName = sch.name, .partitionKey = "KeyACA", .rangeKey = "rKey1"};
    dto::Key k3_p{.schemaName = sch.name, .partitionKey = "KeyACB", .rangeKey = "rKey1"};
    {
        auto iter = indexer.find(k1);
        dto::DataRecord rec;
        rec.isTombstone = false;
        rec.timestamp = ts[0];
        iter.addWI(k1, std::move(rec), 10);
        iter.commitWI();
    }

    {
        auto iter = indexer.find(k2);
        dto::DataRecord rec;
        rec.isTombstone = false;
        rec.timestamp = ts[0];
        iter.addWI(k2, std::move(rec), 10);
        iter.commitWI();
    }

    {
        auto iter = indexer.find(k3);
        dto::DataRecord rec;
        rec.isTombstone = false;
        rec.timestamp = ts[0];
        iter.addWI(k3, std::move(rec), 10);
        iter.commitWI();
    }

    {
        // iterate forward
        auto iter = indexer.find(k3_p);
        REQUIRE(!iter.hasData());
        REQUIRE(iter.atEnd());
    }

    {
        // expected observations at edges, and actual data in-between
        // observations:  t0| t0, t0, t0 | t0
        // iterate forward
        auto iter = indexer.find(k0);
        REQUIRE(!iter.hasData());
        REQUIRE(!iter.atEnd());
        REQUIRE(iter.getWI() == nullptr);
        REQUIRE(iter.getLastReadTime() == ts[0]);
        iter.observeAt(ts[1]);
        // observations:  t1| t1, t0, t0 | t0

        iter.next();
        REQUIRE(iter.hasData());
        REQUIRE(!iter.atEnd());
        REQUIRE(iter.getKey() == k1);
        REQUIRE(iter.getLastReadTime() == ts[1]);
        iter.observeAt(ts[2]);
        // observations:  t1| t2, t0, t0 | t0

        iter.next();
        REQUIRE(iter.hasData());
        REQUIRE(!iter.atEnd());
        REQUIRE(iter.getKey() == k2);
        REQUIRE(iter.getLastReadTime() == ts[0]);
        iter.observeAt(ts[3]);
        // observations:  t1| t2, t3, t0 | t0

        iter.next();
        REQUIRE(iter.hasData());
        REQUIRE(!iter.atEnd());
        REQUIRE(iter.getKey() == k3);
        REQUIRE(iter.getLastReadTime() == ts[0]);
        iter.observeAt(ts[4]);
        // observations:  t1| t2, t3, t4 | t0

        iter.next();
        REQUIRE(!iter.hasData());
        REQUIRE(iter.atEnd());
        REQUIRE(iter.getLastReadTime() == ts[0]);
        iter.observeAt(ts[5]);
        // observations:  t1| t2, t3, t4 | t5
    }

    {
        // iterate in reverse
        auto iter = indexer.find(k0, true);
        REQUIRE(!iter.hasData());
        REQUIRE(iter.atEnd());
        REQUIRE(iter.getLastReadTime() == ts[1]); // we updated this
    }

    {
        // iterate in reverse
        auto iter = indexer.find(k3_p, true);
        REQUIRE(!iter.hasData());
        REQUIRE(!iter.atEnd());
        REQUIRE(iter.getWI() == nullptr);
        REQUIRE(iter.getLastReadTime() == ts[5]);
        iter.observeAt(ts[6]);
        // observations:  t1| t2, t3, t6 | t6

        iter.next();
        REQUIRE(iter.hasData());
        REQUIRE(!iter.atEnd());
        REQUIRE(iter.getKey() == k3);
        REQUIRE(iter.getLastReadTime() == ts[6]);
        iter.observeAt(ts[7]);
        // observations:  t1| t2, t3, t7 | t6

        iter.next();
        REQUIRE(iter.hasData());
        REQUIRE(!iter.atEnd());
        REQUIRE(iter.getKey() == k2);
        REQUIRE(iter.getLastReadTime() == ts[3]);
        iter.observeAt(ts[8]);
        // observations:  t1| t2, t8, t7 | t6

        iter.next();
        REQUIRE(iter.hasData());
        REQUIRE(!iter.atEnd());
        REQUIRE(iter.getKey() == k1);
        REQUIRE(iter.getLastReadTime() == ts[2]);
        iter.observeAt(ts[9]);
        // observations:  t1| t9, t8, t7 | t6

        iter.next();
        REQUIRE(!iter.hasData());
        REQUIRE(iter.atEnd());
        REQUIRE(iter.getLastReadTime() == ts[1]);
        iter.observeAt(ts[10]);
        // observations:  t10| t10, t8, t7 | t6
    }

    {
        // point checks
        {
            auto iter = indexer.find(k0);
            REQUIRE(iter.getLastReadTime() == ts[10]);
        }
        {
            auto iter = indexer.find(k1);
            REQUIRE(iter.getLastReadTime() == ts[10]);
        }
        {
            auto iter = indexer.find(k1_p);
            REQUIRE(iter.getLastReadTime() == ts[8]);
        }
        {
            auto iter = indexer.find(k2);
            REQUIRE(iter.getLastReadTime() == ts[8]);
        }
        {
            auto iter = indexer.find(k2_p);
            REQUIRE(iter.getLastReadTime() == ts[7]);
        }
        {
            auto iter = indexer.find(k3);
            REQUIRE(iter.getLastReadTime() == ts[7]);
        }
        {
            auto iter = indexer.find(k3_p);
            REQUIRE(iter.getLastReadTime() == ts[6]);
        }

    }
}

    }  // namespace k2
    /*
    // 404 read between two values updates the ends to the max(existing, ts)
    KL(T10) | ..................| KH(T10)
    R(T100, K15)
    KL(T100) | ..................| KH(T100)

    KL(T10) | ..................| KH(T200)
    R(T100, K15)
    KL(T100) | ..................| KH(T200)

    KL(T10) | ......K15(T20)............| KH(T200)
    R(T100, K15)
    KL(T10) | ........K15(T100)..........| KH(T200)

    // new write between two values keeps the min of the two neighbors
    KL(T100) | ..................| KH(T130)
    W(T150, K15)
    KL(T100) | .........K15(T100=min(100,130), v=T150).........| KH(T130)

    // update doesn't change read timestamp
    KL(T100) | .........K15(T100=min(100,130), v=T150).........| KH(T130)
    W(T250, K15)
    KL(T100) | .........K15(T100=min(100,130), v=T250).........| KH(T130)

    // Query updates both ends as well as any keys in-between to max(existing, req.ts)
    KL(T10) | .............K15(T200,v=T100)...............| KH(T150)
    Query(k10,k20,T100)
    KL(T100) | .............K15(T200,v=T100)...............| KH(T150)
    Query(k10, k20, T300)
    KL(T300) | .............K15(T300,v=T100)...........| KH(T300)

    // Writes check to see if they are
    // - newer than current value
    // - or if no current value then newer than min(neighbors))
    Write(K10, T200)
    // accepted cases
    KL(T10) | ....................................| KH(T10)
    KL(T10) | ....................................| KH(T410)
    KL(T10) | .........K(10, T100, v=T20).........| KH(T10)
    KL(T400)| .........K(10, T100, v=T20).........| KH(T400)
    // rejected cases
    KL(T310) | ....................................| KH(T310) // newer potential query read
    KL(T10)  | .........K(10, T300, v=T20).........| KH(T10) // newer read
    KL(T10)  | .........K(10, T100, v=T300)........| KH(T10) //  newer value

    // Eviction of an entry updates both neighbors to the max(current, evicted)
    KL(T10)  | .........K(10, T300, v=T20).........| KH(T10)
    evict(K10)
    KL(T300)  | ...................................| KH(T300)

    KL(T400)  | .........K(10, T300, v=T20).........| KH(T10)
    evict(K10)
    KL(T400)  | ...................................| KH(T300)

    // Improvement: store dummy records - records which have no write intent nor commited values.
    // This helps limit the affected range of keys to only the keys that were read, so that a small
    // scan doesn't end up impacting a large key range
    // The solution would then require bounded number of such dummy entries. The proposal is to also
    // add an insertion queue so that we can garbage-collect entries when we have accumulated above
    // the desired threshold.

    // The logic from above is modified so that if a key is not found, we insert a dummy entry with
    // timestamp =max(min(neighbors), ts). We then apply the same rules as above:

    // 404 read between two values
    KL(T10) | ..................| KH(T10)
    R(T50, K15)
    KL(T10) | ........DK(15, T50)..........| KH(T10)

    KL(T10) | ..................| KH(T200)
    R(T50, K15)
    KL(T10) | ........DK(15, T50)..........| KH(T200)

    KL(T100) | ..................| KH(T200)
    R(T50, K15)
    KL(T100) | ........DK(15, T100)..........| KH(T200) // maybe just not insert an entry in this case

    // Query inserts at both ends as well as updates any keys in-between to max(existing, req.ts)
    KL(T10) | .............K15(T200,v=T100)...............| KH(T300)
    Query(k10,k20,T100)
    KL(T10) | .....K10(T100)........K15(T200,v=T100)...............| KH(T300)

    KL(T10) | .....K10(T100)........K15(T200,v=T100)...............| KH(T300)
    Query(k10, k20, T400)
    KL(T10) | .....K10(T400)........K15(T400,v=T100)..............| KH(T300)

    KL(T10) | .............K15(T200,v=T100)...............| KH(T300)
    Query(k10, k20, T400)
    KL(T10) | .....K10(T400)........K15(T400,v=T100)..............| KH(T300)

    // Eviction updates the neighbors as before
    */
