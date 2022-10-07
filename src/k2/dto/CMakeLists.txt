file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(dto OBJECT ${HEADERS} ${SOURCES})
target_link_libraries (dto PRIVATE transport common)

# export the library in the common k2Targets
install(TARGETS dto EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/dto)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS dto NAMESPACE k2:: FILE dto-config.cmake)
