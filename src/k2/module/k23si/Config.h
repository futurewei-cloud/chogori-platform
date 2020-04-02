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
    ConfigDuration readTimeout{"read_timeout", 100us};

    // timeout for write requests (including potential PUSH operations)
    ConfigDuration writeTimeout{"write_timeout", 150us};

    // what is our read cache size in number of entries
    ConfigVar<uint64_t> readCacheSize{"k23si_read_cache_size", 10000};

    // how many times to try and finalize a transaction
    ConfigVar<uint64_t> finalizeRetries{"k23si_txn_finalize_retries", 10};

    // the endpoint for our persistence
    ConfigVar<String> persistenceEndpoint{"k23si_persistence_endpoint", "tcp+k2rpc://127.0.0.1:12345"};

    // the endpoint for the CPO
    ConfigVar<String> cpoEndpoint{"k23si_cpo_endpoint", "tcp+k2rpc://127.0.0.1:12345"};
};
}
