file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")


add_library(k2indexer STATIC ${HEADERS} ${SOURCES})
set_target_properties(k2indexer PROPERTIES LINKER_LANGUAGE CXX)
