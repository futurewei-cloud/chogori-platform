file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2indexer STATIC ${HEADERS} ${SOURCES})
set_target_properties(k2indexer PROPERTIES LINKER_LANGUAGE CXX)

install(TARGETS k2indexer DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/indexer)
