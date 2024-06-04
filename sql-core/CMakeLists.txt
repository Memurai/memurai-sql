add_library(sql-core "src/token.cpp")
set_target_properties(sql-core PROPERTIES POSITION_INDEPENDENT_CODE YES)

target_include_directories(sql-core PRIVATE ${PROJECT_SOURCE_DIR}/multicore/include)
target_include_directories(sql-core PRIVATE ${PROJECT_SOURCE_DIR}/include)
