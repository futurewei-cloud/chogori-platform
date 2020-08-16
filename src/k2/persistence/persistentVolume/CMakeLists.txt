file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(persistent_volume STATIC ${HEADERS} ${SOURCES})
target_link_libraries (persistent_volume PRIVATE common plog)

# export the library in the common k2Targets
install(TARGETS persistent_volume EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/persistent_volume/persistentVolume)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS persistent_volume NAMESPACE k2:: FILE persistent_volume-config.cmake)
