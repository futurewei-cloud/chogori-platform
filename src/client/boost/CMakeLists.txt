project(k2boostclient VERSION 1.0.0 DESCRIPTION "client library for k2 platform")

add_library (k2boostclient STATIC Client.cpp)
add_definitions(-DPARTITION_MANAGER_USE_OBS_INDEX)

set_target_properties(k2boostclient PROPERTIES
    VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

target_link_libraries (k2boostclient PRIVATE k2common k2transport)

install(TARGETS k2boostclient DESTINATION lib)
install(FILES Client.h DESTINATION include/client/boost)
