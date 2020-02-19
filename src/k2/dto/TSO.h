#pragma once

#include <k2/transport/PayloadSerialization.h>

namespace k2 {
namespace dto {
// K2Timestamp - a TrueTime uncertainty window and TSOId
// internally keep TEndTSECount and tStartDelta for efficient serialization and comparison
class Timestamp{
public:
    enum CompareResult : int8_t {
        UN = -2,  // Unknown      - when compare Timestamp from different TSO and uncertainty window overlaps
        LT = -1,  // Less than    - if two K2Timestamp are of same TSOId,  this.Te < other.Te. If different TSOId, this.Te < other.Ts
        EQ = 0,  // Equal        - if two K2Timestamp are of same TSOId, this.Te == other.Te. This should be unusual.
        GT = 1,  // Greater than - if of same TSOId, this.Te > other.Te. If of different TSOId, this.Ts > other.Te.
    };

public:
    // default ctor
    Timestamp() = default;

    // ctor
    Timestamp(uint64_t tEndTSECount, uint32_t tsoId, uint32_t tStartDelta);

    // end time of uncertainty window;
    uint64_t tEndTSECount() const;

    // start time of uncertainty window;
    uint64_t tStartTSECount() const;

    // global unique Id of the TSO issuing this TS
    uint32_t tsoId() const;

    // Uncertainty-compatible comparison
    CompareResult compare(const Timestamp& other) const;

    // provides orderable comparison between timestamps. It returns LT, EQ, or GT - never UN
    // Two timestamps are EQ if they are from same TSO and their start/end times are identical
    // This is useful for comparisons which want to ignore potential uncertainty.
    CompareResult compareCertain(const Timestamp& other) const;
private:
    uint64_t _tEndTSECount = 0;  // nanosec count of tEnd's TSE
    uint32_t _tsoId = 0;
    uint32_t _tStartDelta = 0;  // TStart delta from TEnd in nanoseconds, std::numeric_limits<T>::max() nanoseconds max

public:
    K2_PAYLOAD_FIELDS(_tEndTSECount, _tsoId, _tStartDelta);
};

} // ns dto
} // ns k2
