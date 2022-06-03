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

#include <k2/transport/PayloadSerialization.h>
#include <k2/logging/Chrono.h>

namespace k2 {
namespace dto {
// K2Timestamp - a TrueTime uncertainty window and TSOId
// internally keep endCount and startDelta for efficient serialization and comparison
class Timestamp{
public:
    enum CompareResult : int8_t {
        UN = -2,  // Unknown      - when compare Timestamp from different TSO and uncertainty window overlaps
        LT = -1,  // Less than    - if two K2Timestamp are of same TSOId,  this.Te < other.Te. If different TSOId, this.Te < other.Ts
        EQ = 0,  // Equal        - if two K2Timestamp are of same TSOId, this.Te == other.Te. This should be unusual.
        GT = 1,  // Greater than - if of same TSOId, this.Te > other.Te. If of different TSOId, this.Ts > other.Te.
    };

public:
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

    // (in)equality comparison
    bool operator==(const Timestamp& other) const noexcept;
    bool operator!=(const Timestamp& other) const noexcept;

    // returns a timestamp shifted with the given duration
    Timestamp operator-(const Duration d) const;
    Timestamp operator+(const Duration d) const;

    size_t hash() const;

    String print() const {
        return printTime(TimePoint{} + 1ns*endCount);
    }

    // set this timestamp to min of this and other, based on certain comparison
    void minEq(const Timestamp& other);

    // set this timestamp to max of this and other, based on certain comparison
    void maxEq(const Timestamp& other);

    // return the min of this and other
    Timestamp min(const Timestamp& other) const;

    // return the max of this and other
    Timestamp max(const Timestamp& other) const;

    uint64_t endCount = 0;  // nanosec count of tEnd's TSE in TAI
    uint32_t tsoId = 0;
    uint32_t startDelta = 0;  // TStart delta from TEnd in nanoseconds, std::numeric_limits<T>::max() nanoseconds max

public:
    K2_PAYLOAD_FIELDS(endCount, tsoId, startDelta);
    K2_DEF_FMT(Timestamp, endCount, tsoId, startDelta);
    // Zero timestamp
    static const Timestamp ZERO;
    static const Timestamp INF;
};
inline const Timestamp Timestamp::ZERO = Timestamp{.endCount = 1, .tsoId = 0, .startDelta = 1};
inline const Timestamp Timestamp::INF = Timestamp{.endCount = std::numeric_limits<uint64_t>::max(),
                                                  .tsoId = 0,
                                                  .startDelta = std::numeric_limits<uint32_t>::max()};

} // ns dto
} // ns k2

namespace std {
template <>
struct hash<k2::dto::Timestamp> {
    size_t operator()(const k2::dto::Timestamp& ts) const {
        return ts.hash();
   }
}; // hash
} // ns std
