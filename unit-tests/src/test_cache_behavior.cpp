/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "test/test_utils.h"
#include "types.h"

#include "idxtable.h"

#include "scope/take_time.h"
#include "threads/worker_thread.h"

#undef TEST_START

#define TEST_START                                                                                               \
    g_test_name = string(__func__) + g_addendum;                                                                 \
    l_errors = 0;                                                                                                \
    l_warnings = 0;                                                                                              \
    LOG_VERBOSE(g_test_name << " is starting ");                                                                 \
    SCOPE_EXIT {                                                                                                 \
        LOG_VERBOSE(g_test_name << " ended with: " << l_errors << " errors and " << l_warnings << " warnings."); \
        g_addendum = "";                                                                                         \
        std::this_thread::sleep_for(std::chrono::milliseconds(200));                                             \
    };

extern string g_addendum;

using namespace std;
using namespace memurai;
using namespace memurai::multicore;
using namespace memurai::sql;

void test_cache_behavior() {
    TEST_START;

    constexpr uint64 size = 10'000;
    uint64 items[size];

    TEST_VALUE(0, items[rand() % size]);

    atomic_bool s_start{false}, s_finish{false};

    const uint32 num_times_per_thread = 100'000'000;
    const uint32 num_threads = 128;

    uint32 issueIdx = 2000;

    auto lambda = [&](uint32 idx, uint32 num_times) {
        while (!s_start.load(std::memory_order_acquire))
            _mm_pause();

        for (uint32 i = 0; i < num_times; i++) {
            items[idx]++;
        }
    };

    vector<thread> t_threads;

    for (uint32 i = 0; i < num_threads; i++) {
        t_threads.emplace_back(lambda, issueIdx + i, num_times_per_thread);
    }

    s_start.store(true, std::memory_order_release);

    for (uint32 i = 0; i < num_threads; i++) {
        t_threads[i].join();
    }

    for (uint32 i = 0; i < num_threads; i++) {
        uint32 idx = issueIdx + i;
        LOG_INFO("items[" << idx << "] = " << items[idx]);
    }

    this_thread::sleep_for(chrono::seconds(5));
}
