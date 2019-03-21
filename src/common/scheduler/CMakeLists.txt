project(scheduler VERSION 1.0.0 DESCRIPTION "scheduler library")

add_library(scheduler STATIC
	RoundrobinScheduler.cpp 
	SeastarScheduler.cpp
)

set_target_properties(scheduler PROPERTIES 
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

install(TARGETS scheduler DESTINATION lib)
install(FILES Scheduler.h SeastarScheduler.h RoundrobinScheduler.h DESTINATION include)
