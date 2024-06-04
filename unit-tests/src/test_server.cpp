/*
* Copyright (c) 2020-2021, Janea Systems
by Benedetto Proietti
*/

#include <barrier>
#include <string>

#include "logger.h"
#include "server.h"
#include "test/test_utils.h"

using namespace memurai;
using namespace memurai::multicore;
using namespace std;

#undef TEST_START

#define TEST_START                                                                                               \
    g_test_name = string(__func__) + g_addendum;                                                                 \
    g_addendum = "";                                                                                             \
    l_errors = 0;                                                                                                \
    l_warnings = 0;                                                                                              \
    LOG_VERBOSE(g_test_name << " is starting ");                                                                 \
    SCOPE_EXIT {                                                                                                 \
        LOG_VERBOSE(g_test_name << " ended with: " << l_errors << " errors and " << l_warnings << " warnings."); \
    };

extern string g_addendum;

namespace {

enum ErrorCode { NoError };

#define EXPECT_NO_EXCEPTION

void test_ctor(bool quick) {
    TEST_START;

    try {
#if 0
        // Ctor and memurai connection valid IP address
        {
            IServer::Config config{ 1 };
            Server server(config);
//            server.run();
        }
#endif
        // Simple ctor, valid IP address
        {
            ModuleInterface module_interface;
            IServer::Config config{1};
            Server server(config, module_interface);
        }
    } catch (...) {
        // should not throw exception
        TEST_VALUE(1, 0);
    }
}

} // namespace

void test_server(bool quick) {
    test_ctor(quick);
}
