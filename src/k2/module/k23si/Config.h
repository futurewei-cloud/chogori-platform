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
#include <k2/config/Config.h>

namespace k2 {

// Deadline based on the cached clock
typedef Deadline<CachedSteadyClock> FastDeadline;

// configuration for K23SI
struct K23SIConfig {
    // the minimum retention window we can support. it puts a lower limit on the retention window from the collection
    // to protect us and make sure we don't ddos the TSO while at the same time we keep records around long enough for
    // transactions to execute. If the value is too big, then we force our memory requirements to be higher -
    // we (keep more versions). If the value is too small, then we will have to refresh current time from the TSO too
    // often and cause load issues.
    ConfigDuration minimumRetentionPeriod{"retention_minimum", 1h};

    // how often to update our retention timestamp from the TSO.
    ConfigDuration retentionTimestampUpdateInterval{"retention_ts_update_interval", 60s};

    // timeout for read requests (including potential PUSH operation)
    ConfigDuration readTimeout{"read_timeout", 100ms};

    // timeout for write requests (including potential PUSH operations)
    ConfigDuration writeTimeout{"write_timeout", 150ms};

    // what is our read cache size in number of entries
    ConfigVar<uint64_t> readCacheSize{"k23si_read_cache_size", 10000};

    // how many times to try and finalize a transaction
    ConfigVar<uint64_t> finalizeRetries{"k23si_txn_finalize_retries", 10};

    // how many writes to finalize in parallel
    ConfigVar<uint64_t> finalizeBatchSize{"k23si_txn_finalize_batch_size", 20};

    // Max number of records to return in a single query response
    ConfigVar<uint32_t> paginationLimit{"k23si_query_pagination_limit", 10};

    // Min records in response needed to avoid a push during query processing,
    // and instead returning a paginated response early
    ConfigVar<uint32_t> queryPushLimit{"k23si_query_push_limit", 1};

    // the endpoint for our persistence
    ConfigVar<std::vector<String>> persistenceEndpoint{"k23si_persistence_endpoints"};
    ConfigDuration persistenceTimeout{"k23si_persistence_timeout", 10s};

    // the endpoint for the CPO
    ConfigVar<String> cpoEndpoint{"k23si_cpo_endpoint", "tcp+k2rpc://127.0.0.1:12345"};
};
}
