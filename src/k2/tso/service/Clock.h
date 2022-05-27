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

#include <k2/logging/Chrono.h>
#include <k2/common/SpinMutex.h>

namespace k2::tso {
// Standardize on using GPS timestamps due to no other reason other than the epoch is a nice value
// epoch = 01-01-1980:00:00:00.000000000, and GPS clocks are the most-likely deployment scenario for
// cloud infrastructure. They are easily convertable to other GNSS or atomic standards
// if needed (constant addition/subtraction), or can be smeared to UTC
// For now, add our own implementation here. With c++20 we should migrate to the types defined there

// A GPS timepoint represents a point in time from a time authority(atomic/GNSS clock).
// It specifies a GPS clock value, and what is the error in this observation at the time the call was made
struct GPSTimePoint {
    // the last value read from a GNSS clock as measured since GPS Epoch
    TimePoint real;
    // the total possible error at the time we observed the real value.
    Duration error;
    bool operator==(const GPSTimePoint& o) const;
};

// The GPS clock can be used to obtain GPSTimePoints
class GPSClock {
   private:  // Holds the same information as a GPSTimePoint, but in a volatile-friendly data members
    struct _GPSNanos {
        _GPSNanos();
        _GPSNanos(const volatile _GPSNanos& o);
        _GPSNanos(const _GPSNanos& o);
        _GPSNanos(uint64_t steady, uint64_t real, uint32_t error);
        void operator=(const volatile _GPSNanos& o) volatile;
        void operator=(const _GPSNanos& o) volatile;
        void operator=(_GPSNanos&& o) volatile;
        uint64_t steady{0};
        uint64_t real{0};
        uint32_t error{0};
        K2_DEF_FMT(_GPSNanos, steady, real, error);
    };

public:
    // Generates GPS timepoints. These are guaranteed to be monotonically increasing.
    GPSTimePoint now();

    // helper, factored for testing
    GPSTimePoint _generateGPSNow(const _GPSNanos& nowGPS, uint64_t nowSteady);

    // Should be called often to poll for new GPS clock values.
    // The assumption here is that the GPS hardware will be able to DMA a value in some memory
    // location, which we can poll and associate with a steady clock value ASAP.
    void poll();
    // this helper method (exposed for testing) is called when
    // the real clocked changed to the value indicated by realNow, somewhere between steadyLow and steadyNow
    void _updatePolledTimestamp(uint64_t steadyLow, uint64_t steadyNow, uint64_t realNow);

    // busy-loop poll to detect at least one update to the gps clock value within a reasonable error
    // This method should be called before the application proceeds with its startup (i.e. in main())
    void initialize(uint64_t maxErrorNanos);

    // the base error tolerance for the gps device
    // TODO this should come based on particular manufacturer guarantees and calibrated values
    static constexpr inline uint64_t BASE_ERROR_NANOS = 1'000;
    // sentinel value to indicate infinite error in nanoseconds. If this value is returned, the drift should
    // be considered infinite
    static constexpr inline uint64_t INF_ERROR = std::numeric_limits<uint32_t>::max();

    // TODO, this should be benchmarked and confirmed for the particular motherboard crystal.
    // Standard drift  is ~30 PPM on desktop hardware (or 30us per second). We could achieve better
    // with server-grade components with better quality crystals (e.g. TCXO)
    // Some published numbers
    //
    // 2021 IBM power8/9 servers with 3sec/day which is ~35PPM
    //   https://www.ibm.com/support/pages/time-drift-power8-and-power9-servers
    //
    // TCXO-based oscillators can achieve 1us/day (1.2PPM)
    //  - https://www.gsc-europa.eu/sites/default/files/sites/all/files/Report_on_User_Needs_and_Requirements_Timing_Synchronisation.pdf
    // - https://www.microsemi.com/product-directory/high-reliability-rugged-oscillators/4848-tcxo
    static constexpr inline uint32_t DRIFT = 30;  // drift rate as parts-per DRIFT_PERIOD
    static constexpr inline uint32_t DRIFT_PERIOD = 1'000'000;

    // We require the hardware to be able to generate a valid timestamp within this time
    static constexpr inline uint64_t LIVELINESS_NANOS = 5'000'000'000;

    // get the drift over a particular duration
    // note the possibility for rounding errors here due to integer math -
    //      Accumulating results for small increments is not advisable
    uint32_t getDriftForDuration(uint64_t durationNanos);

private:
    // obtain gps and steady timestamps
    uint64_t _gpsNow();
    uint64_t _steadyNow();

    // the last time we observed a new atomic clock value
    // the following, combined with the spin-lock guard should guarantee coherency across threads on X86/64
    // For other archs, don't compile for now, but we should use an std::atomic instead
#ifdef __x86_64__
    // the current timestamp we use for vending timestamps
    volatile _GPSNanos _useTs;

    // to prevent r-w races when modifying _useTs
    SpinMutex _mutex;
#endif
};

// Global instance, shareable among threads
static inline GPSClock GPSClockInst;
}  // namespace k2

// Formatting utilities for atomic timestamps
template <>
struct fmt::formatter<k2::tso::GPSTimePoint> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(k2::tso::GPSTimePoint const& ts, FormatContext& ctx) {
        return fmt::format_to(ctx.out(), "real:{}, error:{}", ts.real, ts.error);
    }
};

namespace std {
inline ostream& operator<<(ostream& os, const k2::tso::GPSTimePoint& o) {
    fmt::print(os, "{}", o);
    return os;
}

}  // namespace std
