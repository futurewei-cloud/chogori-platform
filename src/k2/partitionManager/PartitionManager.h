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

#pragma once

// third-party
#include <k2/common/Common.h>
#include <k2/dto/Collection.h>
#include <k2/module/k23si/Module.h>
#include <seastar/core/distributed.hh>  // for dist stuff
#include <seastar/core/future.hh>       // for future stuff

namespace k2 {

class PartitionManager {
public: // application lifespan
    PartitionManager();
    ~PartitionManager();
    seastar::future<dto::Partition> assignPartition(dto::CollectionMetadata meta, dto::Partition partition);

    // required for seastar::distributed interface
    seastar::future<> gracefulStop();
    seastar::future<> start();

private:
    std::unique_ptr<K23SIPartitionModule> _pmodule;
}; // class PartitionManager

// per-thread/reactor instance of the partition manager
extern thread_local PartitionManager * __local_pmanager;
inline PartitionManager& PManager() { return *__local_pmanager; }
} // namespace k2
