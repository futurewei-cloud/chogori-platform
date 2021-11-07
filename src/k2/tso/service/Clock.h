/*
MIT License

Copyright(c) 2021 Futurewei Cloud

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

#include <k2/common/Chrono.h>
#include <k2/common/LFMutex.h>

namespace k2 {
// Standardize on using GPS timestamps due to no other reason other than the epoch is a nice value
// epoch = 01-01-1980:00:00:00.000000000, and GPS clocks are the most-likely deployment scenario for
// cloud infrastructure. They are easily convertable to other GNSS or atomic standards
// if needed (constant addition/subtraction), or can be smeared to UTC
// For now, add our own implementation here. With c++20 we should migrate to the types defined there

// A GPS timepoint represents a point in time from a time authority(atomic/GNSS clock).
// It provides
// -real time point: the last value read from a GNSS clock as measured since GPS Epoch
// -steady time point: the correlated local CPU steady count value at the time we updated the real value
// -error: the total possible error at the time we observed the real value.
struct GPSTimePoint {
    TimePoint steady;
    TimePoint real;
    Duration error;
    bool operator==(const GPSTimePoint& o) const;
};

// The GPS clock can be used to obtain timestamps from a GPS clock
// Note that this class internally operates in terms of nanoseconds-since-epoch instead
// of using std::chrono datatypes since the stl datatypes are not volatile-friendly
class GPSClock {
   public:
    // Generates GPS timepoints. These are guaranteed to be monotonically increasing.
    static GPSTimePoint now();

    // Should be called often to poll for new GPS clock values.
    // The assumption here is that the GPS hardware will be able to DMA a value in some memory
    // location, which we can poll here and associate with a steady clock value ASAP.
    static void poll();

    // busy-loop poll to detect at least one update to the gps clock value within a reasonable error
    // This method should be called before the application proceeds with its startup (i.e. in main())
    static void initialize();

    // the base error tolerance for the gps device
    // TODO this should come based on particular manufacturer guarantees and calibrated values
    static constexpr inline uint32_t BASE_ERROR_NANOS = 1'000;
    // sentinel value to indicate infinite drift. If this value is returned, the drift should
    // be considered infinite
    static constexpr inline uint32_t INF_DRIFT = std::numeric_limits<uint32_t>::max();

    static constexpr inline uint32_t DRIFT = 30;  // drift rate as parts-per DRIFT_PERIOD
    static constexpr inline uint32_t DRIFT_PERIOD = 1'000'000;

    // get the drift over a particular duration
    // note the possibility for rounding errors here due to integer math -
    //      Accumulating results for small increments is not advisable
    static uint32_t getDriftForDuration(uint64_t durationNanos);

private:
    // obtain gps and steady timestamps
    static uint64_t _gpsNow();
    static uint64_t _steadyNow();
    // Holds the same information as a GPSTimePoint, but in a volatile-friendly data members
    struct _GPSNanos {
        _GPSNanos();
        _GPSNanos(uint64_t steady, uint64_t real, uint32_t error);
        void operator=(const volatile _GPSNanos& o) volatile;
        void operator=(const _GPSNanos& o) volatile;
        void operator=(_GPSNanos&& o) volatile;
        uint64_t steady{0};
        uint64_t real{0};
        uint32_t error{0};
    };
    // the last time we observed a new atomic clock value
    static inline volatile _GPSNanos _lastGPSts;

    // to prevent r-w races when modifying _lastGPSts
    static inline LFMutex _mutex;
};
}  // namespace k2

// Formatting utilities for atomic timestamps
template <>
struct fmt::formatter<k2::GPSTimePoint> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(k2::GPSTimePoint const& ts, FormatContext& ctx) {
        return fmt::format_to(ctx.out(), "steady:{}, real:{}, error:{}", ts.steady, ts.real, ts.error);
    }
};

namespace std {
inline void to_json(nlohmann::json& j, const k2::GPSTimePoint& tp) {
    j = nlohmann::json{
        {"steady", fmt::format("{}", tp.steady)},
        {"real", fmt::format("{}", tp.real)},
        {"error", fmt::format("{}", tp.error)}
    };
}

inline void from_json(const nlohmann::json& j, k2::GPSTimePoint& tp) {
    tp.steady = j[0].get<k2::TimePoint>();
    tp.real = j[1].get<k2::TimePoint>();
    tp.error = j[2].get<k2::Duration>();
}

inline ostream& operator<<(ostream& os, const k2::GPSTimePoint& o) {
    fmt::print(os, "{}", o);
    return os;
}

}  // namespace std
