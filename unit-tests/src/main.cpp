/*
 * Copyright (c) 2020-2021, Janea Systems
   by Benedetto Proietti
 */

#include <iostream>
#include <map>
#include <string>

#include "logger.h"
#include "scope/take_time.h"
#include "test/test_utils.h"
#include "util.h"

using namespace std;

int g_errors = 0;
int g_warnings = 0;
int l_errors = 0;
int l_warnings = 0;

string g_test_name{"none"};
string g_addendum{""};

void TEST_NOT_REACHED() {
    g_errors++;
    cout << "Test " << g_test_name << " not supposed to reach here." << endl;
    DEBUG_BREAK;
}

void test_syncqueue(bool quick);

void test_spinlock(bool quick);

void test_idxtable(bool quick);

void test_thread_pool(bool quick);

void test_TokenizedStrings(bool quick);

void test_string_metadata();

void test_sql_query_planner(bool quick);

void test_database(bool quick);

void test_query_exec(bool quick);

void test_server(bool quick);

void test_frontend(bool quick);

void test_tags(bool quick);

int main(int argc, char** argv) {
#ifdef TEST_BUILD
    memurai::Logger::getInstance().setLogLevel(memurai::Logger::LogLevel::Debug);
#else
    // memurai::Logger::getInstance().setLogLevel(memurai::Logger::LogLevel::Verbose);
    memurai::Logger::getInstance().setLogLevel(memurai::Logger::LogLevel::Debug);
#endif

    bool quick = true;

    if (argc > 1) //  && (0 == _stricmp("long", argv[1])))
    {
        string argv1{argv[1]};
        memurai::Util::to_lower(argv1);

        if (argv1 == "long") {
            quick = false;
        }
    }

    map<cchar*, uint64> tests_and_durations = {
        {"idxtable test", (TAKE_TIME { test_idxtable(quick); }).millis()},
        {"query exec test", (TAKE_TIME { test_query_exec(quick); }).millis()},
        {"database test", (TAKE_TIME { test_database(quick); }).millis()},
        {"frontend test", (TAKE_TIME { test_frontend(quick); }).millis()},
        {"syncqueue test", (TAKE_TIME { test_syncqueue(quick); }).millis()},
        {"tokenized strings test", (TAKE_TIME { test_TokenizedStrings(quick); }).millis()},
        {"sql query planner", (TAKE_TIME { test_sql_query_planner(quick); }).millis()},
        {"server test", (TAKE_TIME { test_server(quick); }).millis()},
        {"thread pool test", (TAKE_TIME { test_thread_pool(quick); }).millis()},
        {"string metadataa test", (TAKE_TIME { test_string_metadata(); }).millis()},
        {"spinlock test", (TAKE_TIME { test_spinlock(quick); }).millis()},
    };

    LOG_INFO("Test durations:");

    for (auto test : tests_and_durations) {
        LOG_INFO(test.first << " : " << (test.second / 1000) << " secs");
    }

    memurai::Logger::getInstance().join();

    cout << "\n\nTestsuite finished with:\n " << g_warnings << " warnings" << endl << g_errors << " errors" << endl;

    return g_errors;
}
