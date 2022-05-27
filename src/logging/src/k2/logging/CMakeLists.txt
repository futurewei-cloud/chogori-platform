file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

set(AUTOGEN_HEADER
  "${CMAKE_CURRENT_SOURCE_DIR}/AutoGenFormattingUtils.h"
)

add_custom_command(
  OUTPUT ${AUTOGEN_HEADER}
  COMMAND python3 ${CMAKE_CURRENT_SOURCE_DIR}/gen_macros.py > ${AUTOGEN_HEADER}
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/gen_macros.py
)

add_library(logging STATIC ${HEADERS} ${SOURCES} ${AUTOGEN_HEADER})

set_target_properties(logging PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
)

set_target_properties(logging PROPERTIES LINKER_LANGUAGE CXX)

target_link_libraries (logging PRIVATE fmt::fmt)

# export the library in the logging yuleTargets
install(TARGETS logging EXPORT yuleTargets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/logging)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS logging NAMESPACE k2:: FILE common-config.cmake)
