/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "sync/spinlock.h"
#include "test/test_utils.h"
#include "types.h"

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

void test_spinlock_1thread() {
    TEST_START;

    const int N = 100;
    int variable = 0;
    spinlock sl;

    for (int i = 0; i < N; i++) {
        sl.lock();
        variable++;
        sl.unlock();
    }

    TEST_VALUE(N, variable);
}

void test_spinlock_2threads() {
    TEST_START;

    const int Neach = 1000000;
    const int Ntotal = Neach * 2;
    int variable = 0;
    spinlock sl;

    atomic<bool> start{false};

    auto lambda = [&]() {
        while (!start.load(std::memory_order_acquire))
            ;

        for (int i = 0; i < Neach; i++) {
            sl.lock();
            variable++;
            sl.unlock();
        }
    };

    thread t1(lambda);
    thread t2(lambda);

    start.store(true, std::memory_order_release);

    t1.join();
    t2.join();

    TEST_VALUE(Ntotal, variable);
}

void test_spinlock_Nthreads(uint64 num_threads) {
    g_addendum = "_" + to_string(num_threads);
    TEST_START;

    const uint64 Neach = 1000000;
    const uint64 Ntotal = Neach * num_threads;
    uint64 variable = 0;
    spinlock sl;

    atomic<bool> start{false};

    auto lambda = [&]() {
        while (!start.load(std::memory_order_acquire))
            ;

        for (int i = 0; i < Neach; i++) {
            sl.lock();
            variable++;
            sl.unlock();
        }
    };

    vector<thread> threads;

    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back(lambda);
    }

    start.store(true, std::memory_order_release);

    for (int t = 0; t < num_threads; t++) {
        threads[t].join();
    }

    TEST_VALUE(Ntotal, variable);
}

void test_spinlock(bool quick) {
    test_spinlock_1thread();
    const uint64 factor = quick ? 4 : 2;

    for (uint64 num_threads = 2; num_threads <= 2 * std::thread::hardware_concurrency(); num_threads *= factor) {
        test_spinlock_Nthreads(num_threads);
    }
}
