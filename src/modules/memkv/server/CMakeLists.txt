file(GLOB HEADERS "*.h")

add_library(k2memkv_module_server STATIC ${HEADERS})
install(FILES ${HEADERS} DESTINATION include/node/module)
set_target_properties(k2memkv_module_server PROPERTIES LINKER_LANGUAGE CXX)
