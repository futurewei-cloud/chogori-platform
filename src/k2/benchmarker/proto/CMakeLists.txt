#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

include(FindProtobuf)
find_package(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIR})
protobuf_generate_cpp(PROTO_SRC PROTO_HEADER k2bdto.proto)
add_library(k2bdto ${PROTO_HEADER} ${PROTO_SRC})
set_target_properties(k2bdto PROPERTIES COMPILE_OPTIONS "") # remove protobuf compile warnings
