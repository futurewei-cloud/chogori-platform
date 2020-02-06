add_subdirectory(scheduler)
add_subdirectory(plog)

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2types STATIC ${HEADERS} ${SOURCES})
target_link_libraries (k2types PRIVATE k2common k2config k2transport)

install(TARGETS k2types DESTINATION lib/k2)
