#pragma once

// std
#include <random>
#include <string>
#include <chrono>
#include <ctime>
#include <unordered_map>
// k2
#include "transport/Prometheus.h"
#include <benchmarker/generator/Generator.h>
#include "KeySpace.h"

namespace k2
{
namespace benchmarker
{

namespace metrics = seastar::metrics;

class Session
{
protected:
    std::mutex _mutex;
    std::vector<std::string> _failedKeys;
    std::vector<std::string> _retryKeys;
    volatile uint64_t _ackCounter = 0;
    volatile uint64_t _pendingCounter = 0;
    volatile uint64_t _retryCounter = 0;
    metrics::metric_groups _metricGroups;
    std::chrono::time_point<std::chrono::system_clock> _startTime;
    std::chrono::time_point<std::chrono::system_clock> _endTime;
    std::unordered_map<std::string, std::chrono::time_point<std::chrono::steady_clock>> _timestampMap;
    k2::ExponentialHistogram _latency;
    // arguments
    const std::string _sessionId;
    const std::string _type;
    KeySpace _keySpace;
    KeySpace::iterator _it;
    volatile uint64_t _targetCount = 0;
public:
    Session(std::string sessionId, std::string type, KeySpace&& keySpace)
    : _sessionId(std::move(sessionId))
    , _type(std::move(type))
    , _keySpace(std::forward<KeySpace>(keySpace))
    , _it(_keySpace.begin())
    {
        _startTime = std::chrono::system_clock::now();
        _endTime = _startTime;

        std::vector<metrics::label_instance> labels;
        labels.push_back(metrics::label_instance("session_id", _sessionId));

        _metricGroups.add_group("session", {
            metrics::make_counter("total_count", [this] { return _ackCounter;}, metrics::description("Total requests send"), labels),
        });
    }

    void setTargetCount(uint64_t count)
    {
        _targetCount = count;
        _ackCounter = 0;
    }

    uint64_t getTargetCount() const
    {
        return _targetCount;
    }

    uint64_t getPendingCounter() const
    {
        return _pendingCounter;
    }

    uint64_t getAckCounter() const
    {
        return _ackCounter;
    }

    uint64_t getRetryCounter() const
    {
        return _retryCounter;
    }

    const std::string& getType() const
    {
        return _type;
    }

    const std::string& getSessionId() const
    {
        return _sessionId;
    }

    const std::vector<std::string>& getFailedKeys() const
    {
        return _failedKeys;
    }

    std::chrono::time_point<std::chrono::system_clock> getStartTime() const
    {
        return _startTime;
    }

    std::chrono::time_point<std::chrono::system_clock> getEndTime() const
    {
        return _endTime;
    }

    seastar::metrics::histogram& getLatencyHistogram()
    {
        std::lock_guard<std::mutex> lock(_mutex);

        return _latency.getHistogram();
    }

    //
    // Return true of the session is complete
    //
    bool isDone()
    {
        std::lock_guard<std::mutex> lock(_mutex);

        bool doneFlag =  _ackCounter >= _targetCount;
        if(doneFlag) {
            _endTime = std::chrono::system_clock::now();
        }

        return doneFlag;
    }

    //
    // Peek the current key
    //
    const std::string& peek()
    {
        std::lock_guard<std::mutex> lock(_mutex);

        return *_it;
    }

    //
    // Get the next key. Will return empty string if the keyspace is exhausted.
    //
    std::string next()
    {
        std::lock_guard<std::mutex> lock(_mutex);

        std::string key;

        if(!_retryKeys.empty()) {
            key = _retryKeys.back();
            _retryKeys.pop_back();
            _recordTime(key);

            return key;
        }

        if(_it == _keySpace.end() || _pendingCounter >= _targetCount) {

            return std::string(); // iterator exhausted
        }

        key = std::move(*_it);
        ++_it;
        ++_pendingCounter;
        _recordTime(key);

        return key;
    }

    //
    // Retry this key; this can happen in the client is busy
    //
    void retry(const std::string& key)
    {
         std::lock_guard<std::mutex> lock(_mutex);

        _retryKeys.push_back(key);
        ++_retryCounter;
        _deleteTime(key);
    }

    //
    // Record the key as success
    //
    void success(const std::string& key)
    {
        std::lock_guard<std::mutex> lock(_mutex);

        (void)key;
        ++_ackCounter;
        _reportTime(key);
    }

    //
    // Record the key as failed; it will not be retried in the future
    //
    void failed(const std::string& key)
    {
        std::lock_guard<std::mutex> lock(_mutex);

        ++_ackCounter;
        _failedKeys.push_back(key);
        _deleteTime(key);
    }

protected:
    void _recordTime(const std::string& key)
    {
        _timestampMap.insert(std::pair<std::string, std::chrono::time_point<std::chrono::steady_clock>>(key, std::chrono::steady_clock::now()));
    }

    void _deleteTime(const std::string& key)
    {
        auto it = _timestampMap.find(key);
        if(it == _timestampMap.end()) {
            return;
        }

        _timestampMap.erase(it);
    }

    void _reportTime(const std::string& key)
    {
        auto it = _timestampMap.find(key);
        if(it == _timestampMap.end()) {
            return;
        }

        auto duration = std::chrono::steady_clock::now() - it->second;
        _timestampMap.erase(it);
        _latency.add(std::move(duration));
    }
};

};
};
