add_library(sql-fe SHARED
    "src/Buffer.cpp"
    "src/column.cpp"
    "src/database.cpp"
    "src/frontend.cpp"
    "src/idxtable.cpp"
    "src/interface.cpp"
    "src/module.cpp"
    "src/PublisherV2DStrings.cpp"
    "src/QueryCtx.cpp"
    "src/QueryWalker.cpp"
    ${VERSION_FILE}
)

target_include_directories(sql-fe PUBLIC "include")
target_include_directories(sql-fe PRIVATE ${CMAKE_SOURCE_DIR}/multicore/include)
target_include_directories(sql-fe PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(sql-fe SYSTEM PRIVATE ${CMAKE_SOURCE_DIR}/deps/sql-parser/src)

target_link_libraries(sql-fe sql-core multicore sql-parser)
set_target_properties(sql-fe PROPERTIES OUTPUT_NAME "memurai-sql")

install(TARGETS sql-fe DESTINATION lib/redis/modules)
