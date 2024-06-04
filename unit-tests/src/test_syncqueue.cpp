/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#include <string>
#include <vector>

#include "sync/syncqueue.h"
#include "test/test_utils.h"
#include "types.h"

using namespace std;
using namespace memurai;

extern string g_addendum;

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

void test_syncqueue_simple_uint64() {
    TEST_START;

    syncqueue<uint64> queue;
    const uint64 N = 100'000;

    for (uint64 i = 0; i < N; i++) {
        queue.enqueue(i);
    }

    TEST_VALUE(N, queue.size());

    for (uint64 i = 0; i < N; i++) {
        auto opt = queue.dequeue();
        TEST_VALUE(true, opt.has_value());
        if (opt.has_value()) {
            TEST_VALUE(i, *opt);
        }
    }
}

void test_syncqueue_simple_string() {
    TEST_START;

    syncqueue<string> queue;
    const uint64 N = 100'000;

    for (uint64 i = 0; i < N; i++) {
        queue.enqueue(to_string(i));
    }

    TEST_VALUE(N, queue.size());

    for (uint64 i = 0; i < N; i++) {
        auto opt = queue.dequeue();
        TEST_VALUE(true, opt.has_value());
        if (opt.has_value()) {
            TEST_VALUE(to_string(i), *opt);
        }
    }
}

void test_syncqueue_Nthreads(uint64 num_threads) {
    g_addendum = "_" + to_string(num_threads);
    TEST_START;

    syncqueue<uint64> queue;

    const uint64 Neach = 100000;
    const uint64 Ntotal = Neach * num_threads;

    atomic<bool> start{false};

    auto lambda = [&](uint64 firstNumber) {
        while (!start.load(std::memory_order_acquire))
            ;

        for (uint64 i = 0; i < Neach; i++) {
            queue.enqueue(firstNumber + i);
        }
    };

    vector<thread> threads;

    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back(lambda, t * Neach);
    }

    start.store(true, std::memory_order_release);

    for (int t = 0; t < num_threads; t++) {
        threads[t].join();
    }

    TEST_VALUE(Ntotal, queue.size());

    while (queue.size() > 0) {
        queue.dequeue();
    }
}

void test_syncqueue_stop() {
    TEST_START;

    syncqueue<uint64> queue;

    atomic<bool> exited{false};
    atomic<bool> start{false};

    auto lambda = [&]() {
        start.store(true);
        auto opt = queue.dequeue();
        exited.store(true);
    };

    std::thread t(lambda);

    while (!start.load())
        ;

    queue.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    TEST_VALUE(true, exited);

    t.join();
}

void test_syncqueue(bool quick) {
    const uint64 factor = quick ? 4 : 2;

    cuint64 ntimes = (quick ? 1 : 20);
    for (int i = 0; i < ntimes; i++) {
        test_syncqueue_stop();
    }

    test_syncqueue_simple_uint64();
    test_syncqueue_simple_string();

    for (uint64 num_threads = 2; num_threads <= 2ull * std::thread::hardware_concurrency(); num_threads *= factor) {
        test_syncqueue_Nthreads(num_threads);
    }
}
