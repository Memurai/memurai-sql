add_library(multicore "src/server.cpp")
set_target_properties(multicore PROPERTIES POSITION_INDEPENDENT_CODE YES)

target_include_directories(multicore PRIVATE include)
target_include_directories(multicore PRIVATE ${CMAKE_SOURCE_DIR}/include)
