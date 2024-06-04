
add_executable(system-tests
            "src/main.cpp"
            "src/test-hash-ops.cpp"
            "src/test-flushdb.cpp"
            "src/test-keyspace-notification.cpp"
)

target_include_directories(system-tests PRIVATE include/)
target_include_directories(system-tests SYSTEM PRIVATE ${CMAKE_SOURCE_DIR}/deps/)

target_link_libraries(system-tests PRIVATE hiredis)
if(WIN32)
    target_link_libraries(system-tests PRIVATE Ws2_32)
endif(WIN32)
