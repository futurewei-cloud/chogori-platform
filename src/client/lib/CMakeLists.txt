project(k2client VERSION 1.0.0 DESCRIPTION "client library for k2 platform")

include_directories(..)

add_library (k2client STATIC Client.cpp)

set_target_properties(k2client PROPERTIES 
    VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

target_link_libraries (k2client PRIVATE k2common)

install(TARGETS k2client DESTINATION lib)
install(FILES Client.h DESTINATION include/client)
