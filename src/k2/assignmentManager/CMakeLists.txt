file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(assignment_manager STATIC ${HEADERS} ${SOURCES})
target_link_libraries (assignment_manager PRIVATE common transport Seastar::seastar dto k23si)

# export the library in the common k2Targets
install(TARGETS assignment_manager EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/assignmentManager)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS assignment_manager NAMESPACE k2:: FILE assignment_manager-config.cmake)
