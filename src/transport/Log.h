//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once
#include <iostream>

#include <seastar/core/reactor.hh>

#if K2TX_DEBUG == 1
#define K2LOG(msg)                                                               \
  { std::cerr << "(" << seastar::engine().cpu_id() <<") [" << __FILE__ << ":" <<__LINE__ << " @" << __FUNCTION__ <<"]"  << msg << std::endl; }
#else
#define K2LOG(msg)
#endif
