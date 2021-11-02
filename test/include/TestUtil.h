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
#include <k2/common/Common.h>
#include <k2/common/Chrono.h>
#include <k2/common/Log.h>
#include <k2/dto/Timestamp.h>
#include <pthread.h>
#include <filesystem>
#include <iostream>
namespace k2::log {
inline thread_local k2::logging::Logger testutil("k2::testutil");
}
std::string generateTempFolderPath(const char* id)
{
    char folder[255];
    int res = snprintf(folder, sizeof(folder), "/%s_%lx%x%x/", id ? id : "", pthread_self(), (uint32_t)time(nullptr), (uint32_t)rand());
    K2ASSERT(k2::log::testutil, res > 0 && res < (int)sizeof(folder), "unable to construct folder name");

    return std::filesystem::temp_directory_path().concat(folder);
}

class TimestampWorker {
    public:
    TimestampWorker() {
       last = k2::sys_now_nsec_count();
    }

    uint64_t getTimeNow() {
        auto current = k2::sys_now_nsec_count();
        // check if the timestamp is the same as the last one. This could happen and it was reported by some users
        while (current == last) {
            // sleep for 1 nanosecond to make sure that we have an increasing timestamp
            std::this_thread::sleep_for(std::chrono::nanoseconds(1));
            current = k2::sys_now_nsec_count();
        }
        last = current;
        return last;
    }

    private:
    uint64_t last;
};

static class TimestampWorker defaultTimestampWorker;

seastar::future<k2::dto::Timestamp> getTimeNow() {
    // TODO call TSO service with timeout and retry logic
    auto nsecsSinceEpoch = defaultTimestampWorker.getTimeNow();
    return seastar::make_ready_future<k2::dto::Timestamp>(k2::dto::Timestamp(nsecsSinceEpoch, 123, 1000));
}
