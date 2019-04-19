project(_client VERSION 1.0.0 DESCRIPTION "client library for k2 platform")

include_directories(..)

add_library (_client STATIC Client.cpp)

set_target_properties(_client PROPERTIES 
    VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

target_link_libraries (_client PRIVATE common)

install(TARGETS _client DESTINATION lib)
install(FILES Client.h DESTINATION include/client)
