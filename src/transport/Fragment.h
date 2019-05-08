//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#pragma once

// stl
#include <functional>

// third-party
#include <seastar/core/temporary_buffer.hh>

namespace k2tx {
// the binary type to use
typedef uint8_t Binary_t;

// Use the seastar temporary_buffer directly for now.
typedef seastar::temporary_buffer<Binary_t> Fragment;

// The type for a function which can allocate fragments
typedef std::function<Fragment()> Allocator_t;
} // k2tx
