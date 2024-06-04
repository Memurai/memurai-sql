set(SQL_FE_SOURCES
    ${CMAKE_SOURCE_DIR}/sql-fe/src/Buffer.cpp
    ${CMAKE_SOURCE_DIR}/sql-fe/src/column.cpp
    ${CMAKE_SOURCE_DIR}/sql-fe/src/database.cpp
    ${CMAKE_SOURCE_DIR}/sql-fe/src/frontend.cpp
    ${CMAKE_SOURCE_DIR}/sql-fe/src/idxtable.cpp
    ${CMAKE_SOURCE_DIR}/sql-fe/src/interface.cpp
    ${CMAKE_SOURCE_DIR}/sql-fe/src/module.cpp
    ${CMAKE_SOURCE_DIR}/sql-fe/src/PublisherV2DStrings.cpp
    ${CMAKE_SOURCE_DIR}/sql-fe/src/QueryCtx.cpp
    ${CMAKE_SOURCE_DIR}/sql-fe/src/QueryWalker.cpp
    ${VERSION_FILE}
)

add_executable(unit-tests
    ${SQL_FE_SOURCES}
    "src/main.cpp"
    "src/test_cache_behavior.cpp"
    "src/test_database.cpp"
    "src/test_frontend.cpp"
    "src/test_idxtable.cpp"
    "src/test_query_exec.cpp"
    "src/test_query_walker.cpp"
    "src/test_server.cpp"
    "src/test_spinlock.cpp"
    "src/test_string_metadata.cpp"
    "src/test_syncqueue.cpp"
    "src/test_table_utils.cpp"
    "src/test_thread_pool.cpp"
    "src/test_TokenizedStrings.cpp"
    "src/test_utils.cpp"
)

target_compile_definitions(unit-tests PRIVATE TEST_BUILD)

target_include_directories(unit-tests PRIVATE ${CMAKE_SOURCE_DIR}/include/)
target_include_directories(unit-tests PRIVATE ${CMAKE_SOURCE_DIR}/multicore/include)
target_include_directories(unit-tests PRIVATE ${CMAKE_SOURCE_DIR}/sql-fe/include)
target_include_directories(unit-tests SYSTEM PRIVATE ${CMAKE_SOURCE_DIR}/deps/sql-parser/src)

target_link_libraries(unit-tests PRIVATE sql-core sql-parser hiredis multicore)
if (UNIX)
    target_link_libraries(unit-tests PRIVATE pthread)
endif (UNIX)

# Tests
add_test(NAME unit-tests COMMAND unit-tests)
