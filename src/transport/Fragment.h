//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#pragma once
#include <vector>

#include <seastar/core/temporary_buffer.hh>

namespace k2tx {
// Use the seastar temporary_buffer directly for now.
typedef seastar::temporary_buffer<char> Fragment;
} // k2tx
