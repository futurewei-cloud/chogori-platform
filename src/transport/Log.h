//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once
#include <iostream>

#include <seastar/core/reactor.hh>

// This file contains some utility macros for logging and tracing, used in the transport
// TODO hook this up into proper logging

#define K2LOG(msg) { std::cerr << "(" << seastar::engine().cpu_id() <<") [" << __FILE__ << ":" <<__LINE__ << " @" << __FUNCTION__ <<"]"  << msg << std::endl; }

#if K2TX_DEBUG == 1
#define K2DEBUG(msg) K2LOG("[DEBUG] " << msg)
#else
#define K2DEBUG(msg)
#endif

// TODO warnings and errors must also emit metrics
#define K2INFO(msg) K2LOG("[INFO] " << msg)
#define K2WARN(msg) K2LOG("[WARN] " << msg)
#define K2ERROR(msg) K2LOG("[ERROR] " << msg)
