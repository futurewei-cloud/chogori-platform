file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(indexer STATIC ${HEADERS} ${SOURCES})
set_target_properties(indexer PROPERTIES LINKER_LANGUAGE CXX)

# export the library in the common k2Targets
install(TARGETS indexer EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/indexer)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS indexer NAMESPACE k2:: FILE indexer-config.cmake)
