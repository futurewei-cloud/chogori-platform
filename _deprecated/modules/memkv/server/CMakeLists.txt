file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2memkv_module_server STATIC ${HEADERS} ${SOURCES})
install(FILES ${HEADERS} DESTINATION include/k2/node/module)
set_target_properties(k2memkv_module_server PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries (k2memkv_module_server PRIVATE k2indexer k2node k2common)
