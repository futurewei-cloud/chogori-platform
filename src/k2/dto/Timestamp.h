#pragma once

#include <k2/transport/PayloadSerialization.h>
#include <k2/common/Chrono.h>

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

    // end time of uncertainty window - TAI TimeSinceEpoch count in nanoseconds since Jan. 1, 1970;
    uint64_t tEndTSECount() const;

    // start time of uncertainty window - TAI nanoseconds count
    uint64_t tStartTSECount() const;

    // global unique Id of the TSO issuing this TS
    uint32_t tsoId() const;

    // Uncertainty-compatible comparison
    // this should be used when the user cares if due to uncertainty, two timestamps may not be orderable
    CompareResult compareUncertain(const Timestamp& other) const;

    // TODO: After merger, change return type to int
    // provides orderable comparison between timestamps. It returns LT, EQ, or GT - never UN
    // Two timestamps are EQ if they are from same TSO and their start/end times are identical
    // Normally we do certain ordering just based on the END value. In cases of different TSO
    // issuing timestamps with exact same END value, we order based on the TSO id
    // this should be used when the user does not care about potential uncertainty in the ordering and
    // just wants some consistent(to any observer) ordering among all timestamps even from different TSOs
    CompareResult compareCertain(const Timestamp& other) const;

    // returns a timestamp shifted with the given duration
    Timestamp operator-(const Duration d) const;
    Timestamp operator+(const Duration d) const;

    size_t hash() const;

    friend std::ostream& operator<<(std::ostream& os, const Timestamp& ts) {
        return os << "{tsoId=" << ts._tsoId << ", endCount=" << ts._tEndTSECount
                  << "(" << printTime(TimePoint{} + 1ns*ts._tEndTSECount)
                  << "), delta=" << ts._tStartDelta << "}";
    }

   private:
    uint64_t _tEndTSECount = 0;  // nanosec count of tEnd's TSE in TAI 
    uint32_t _tsoId = 0;
    uint32_t _tStartDelta = 0;  // TStart delta from TEnd in nanoseconds, std::numeric_limits<T>::max() nanoseconds max

public:
    K2_PAYLOAD_FIELDS(_tEndTSECount, _tsoId, _tStartDelta);
};

} // ns dto
} // ns k2
