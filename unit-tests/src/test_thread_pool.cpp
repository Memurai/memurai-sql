/*
 * Copyright (c) 2020-2021, Janea Systems
   by Benedetto Proietti
 */

#include <barrier>
#include <emmintrin.h>
#include <string>

#include "sync/syncqueue.h"
#include "threads/cancellation.h"
#include "threads/thread_pool.h"
#include "threads/worker_thread.h"

#include "logger.h"
#include "scope/scope_exit.h"
#include "test/run_test.h"
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

void test_empty() {
    TEST_START;

    cuint32 threadPoolSize = 4;
    typedef WorkerThread<syncqueue<TLambda>, std::exception, memurai::exception> WorkerThread;
    ThreadPool<WorkerThread> thread_pool(g_test_name, threadPoolSize);
}

enum class Behavior { do_not_throw, throw_std_exc, throw_memurai_exc, throw_other };

std::string my_to_string(Behavior b) {
    switch (b) {
    case Behavior::do_not_throw:
        return "do_not_throw";
    case Behavior::throw_std_exc:
        return "throw_std_exc";
    case Behavior::throw_memurai_exc:
        return "throw_memurai_exc";
    case Behavior::throw_other:
        return "throw_other";
    default:
        return "";
    }
}

// true = success
void test_submits(cuint32 threadPoolSize, cuint32 numItems, Behavior behave) {
    g_addendum = string("_") + to_string(numItems) + "_" + my_to_string(behave);
    TEST_START;

    typedef WorkerThread<syncqueue<TLambda>, std::exception, memurai::exception> WorkerThread;
    ThreadPool<WorkerThread> thread_pool(g_test_name, threadPoolSize);

    std::atomic<bool> flag(false);
    Token token("test_token", 1234, 1);

    thread_pool.submit(
        [&flag, behave]() {
            flag.store(true);
            this_thread::sleep_for(chrono::milliseconds(rand() % 100));
            switch (behave) {
            case Behavior::do_not_throw:
                return;
            case Behavior::throw_std_exc:
                throw std::runtime_error("std::exception thrown on purpose!");
            case Behavior::throw_memurai_exc:
                throw MEMURAI_EXCEPTION(NoError, "memurai::exception thrown on purpose!");
            case Behavior::throw_other:
                throw 42;
            }
        },
        &token);

    token.join();
    // this_thread::sleep_for(chrono::milliseconds(100));

    auto ret = token.getState();

    switch (behave) {
    case Behavior::do_not_throw:
        TEST_VALUE(TokenState::DONE_WELL, get<0>(ret));
        TEST_VALUE(false, token.gotMemuraiExceptions());
        TEST_VALUE(false, token.gotUnknownExceptions());
        TEST_VALUE(false, token.gotStdExceptions());
        TEST_VALUE(false, get<1>(ret).has_value());
        break;

    case Behavior::throw_std_exc:
        TEST_VALUE(TokenState::DONE_NOT_WELL, get<0>(ret));
        TEST_VALUE(false, token.gotMemuraiExceptions());
        TEST_VALUE(false, token.gotUnknownExceptions());
        TEST_VALUE(true, token.gotStdExceptions());
        TEST_VALUE(true, get<1>(ret).has_value());
        break;

    case Behavior::throw_memurai_exc:
        TEST_VALUE(TokenState::DONE_NOT_WELL, get<0>(ret));
        TEST_VALUE(true, token.gotMemuraiExceptions());
        TEST_VALUE(false, token.gotStdExceptions());
        TEST_VALUE(false, token.gotUnknownExceptions());
        TEST_VALUE(true, get<1>(ret).has_value());
        break;

    case Behavior::throw_other:
        TEST_VALUE(TokenState::DONE_NOT_WELL, get<0>(ret));
        TEST_VALUE(false, token.gotMemuraiExceptions());
        TEST_VALUE(false, token.gotStdExceptions());
        TEST_VALUE(true, token.gotUnknownExceptions());
        TEST_VALUE(true, get<1>(ret).has_value());
        break;
    default:
        TEST_VALUE(1, UNREACHED);
    }

    TEST_VALUE(true, flag.load());
}

void test_submits(uint32 threadPoolSize, bool quick) {
    const vector<uint32> long_numItems{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024};
    const vector<uint32> short_numItems{1, 8, 128, 1024};
    const vector<uint32>& numItems = quick ? short_numItems : long_numItems;

    for (auto n : numItems) {
        RUN_TEST {
            test_submits(threadPoolSize, n, Behavior::do_not_throw);
        };
        RUN_TEST {
            test_submits(threadPoolSize, n, Behavior::throw_std_exc);
        };
        RUN_TEST {
            test_submits(threadPoolSize, n, Behavior::throw_memurai_exc);
        };
    }
}

void test_cancellations_simple(uint32 threadPoolSize, uint32 num_threads, Behavior strange) {
    g_addendum = string("_") + to_string(threadPoolSize) + "_" + to_string(num_threads);
    TEST_START;

    typedef WorkerThread<syncqueue<TLambda>, std::exception, memurai::exception> WorkerThread;
    ThreadPool<WorkerThread> thread_pool(g_test_name, threadPoolSize);

    // We submit a job with N pieces. They all just wait and intermittently check on the token.
    // After a bit one job throws exception.
    // We expect all of them to cancel quickly and fail.

    // atomic flag to tell threads to start and finish
    atomic<bool> s_finish{false}, s_do_your_thing{false};

    // count of threads that finish normally
    atomic<uint64> s_done{0};

    Token token(g_test_name, 1234, num_threads);

    syncqueue<Behavior> queue;

    // Fill queue with some behaviors for the threads
    for (uint32 i = 0; i < num_threads; i++) {
        if (i == num_threads / 2) {
            queue.enqueue(strange);
        } else {
            queue.enqueue(Behavior::do_not_throw);
        }
    }

    std::barrier start_barrier(num_threads + 1);

    for (uint32 i = 0; i < num_threads; i++) {
        thread_pool.submit(
            [&]() {
                // all threads start together
                start_barrier.arrive_and_wait();

                // get what we gotta do
                Behavior behave = *queue.dequeue();

                // unntil it's time to close, sleep and do something
                while (!s_finish.load()) {
                    _mm_pause();
                    if (s_do_your_thing.load()) {
                        switch (behave) {
                        case Behavior::do_not_throw:
                            break;
                        case Behavior::throw_std_exc:
                            throw std::runtime_error("std::exception thrown on purpose!");
                        case Behavior::throw_memurai_exc:
                            throw MEMURAI_EXCEPTION(NoError, "memurai::exception thrown on purpose!");
                        case Behavior::throw_other:
                            throw 42;
                        }
                    }
                }

                s_done++;
            },
            &token);
    }

    start_barrier.arrive_and_wait();

    s_do_your_thing.store(true);
    this_thread::sleep_for(chrono::milliseconds(2000 + 10 * num_threads));
    s_finish.store(true);

    token.join();

    auto token_state = get<0>(token.getState());

    if (strange != Behavior::do_not_throw) {
        TEST_VALUE(num_threads - 1, s_done);
        TEST_VALUE(TokenState::DONE_NOT_WELL, token_state);
        TEST_VALUE(true, token.isCanceled());
    }

    switch (strange) {
    case Behavior::do_not_throw:
        TEST_VALUE(false, token.gotMemuraiExceptions());
        TEST_VALUE(false, token.gotStdExceptions());
        TEST_VALUE(false, token.gotUnknownExceptions());
        break;
    case Behavior::throw_std_exc:
        TEST_VALUE(false, token.gotMemuraiExceptions());
        TEST_VALUE(true, token.gotStdExceptions());
        TEST_VALUE(false, token.gotUnknownExceptions());
        break;
    case Behavior::throw_memurai_exc:
        TEST_VALUE(true, token.gotMemuraiExceptions());
        TEST_VALUE(false, token.gotStdExceptions());
        TEST_VALUE(false, token.gotUnknownExceptions());
        break;
    case Behavior::throw_other:
        TEST_VALUE(false, token.gotMemuraiExceptions());
        TEST_VALUE(false, token.gotStdExceptions());
        TEST_VALUE(true, token.gotUnknownExceptions());
        break;
    }
}

void test_cancellations_complex(uint32 threadPoolSize, uint32 num_threads) {
    g_addendum = string("_") + to_string(threadPoolSize) + "_" + to_string(num_threads);
    TEST_START;

    typedef WorkerThread<syncqueue<TLambda>, std::exception, memurai::exception> WorkerThread;
    ThreadPool<WorkerThread> thread_pool(g_test_name, threadPoolSize);

    // We submit a job with N pieces. They all just wait and intermittently check on the token.
    // After a bit one job throws exception.
    // We expect all of them to cancel quickly and fail.

    // atomic flag to tell threads to start and finish
    atomic<bool> s_finish{false}, s_do_your_thing{false};

    // count of threads that do not throw
    atomic<uint64> s_not_thrown{0};
    // ...
    atomic<uint32> s_thrown_memurai{0};
    atomic<uint32> s_thrown_std{0};
    atomic<uint32> s_thrown_other{0};

    Token token(g_test_name, 1234, num_threads);

    std::barrier start_barrier(num_threads + 1);

    for (uint32 i = 0; i < num_threads; i++) {
        thread_pool.submit(
            [&]() {
                start_barrier.arrive_and_wait();

                // until it's time to close, sleep and do something
                while (!s_finish.load()) {
                    _mm_pause();
                    if (s_do_your_thing.load()) {
                        switch (rand() % 4) {
                        case 0:
                            break;
                        case 1:
                            s_thrown_std++;
                            throw std::runtime_error("std::exception thrown on purpose!");
                        case 2:
                            s_thrown_memurai++;
                            throw MEMURAI_EXCEPTION(NoError, "memurai::exception thrown on purpose!");
                        default:
                            s_thrown_other++;
                            throw 42;
                        }
                    }
                }

                s_not_thrown++;
            },
            &token);
    }

    start_barrier.arrive_and_wait();

    this_thread::sleep_for(chrono::milliseconds(10));
    s_do_your_thing.store(true);
    this_thread::sleep_for(chrono::milliseconds(2000 + 10 * num_threads));
    s_finish.store(true);

    token.join();

    auto token_state = get<0>(token.getState());

    TEST_VALUE(num_threads, s_not_thrown + s_thrown_memurai + s_thrown_std + s_thrown_other);
    TEST_VALUE(num_threads != s_not_thrown, token.isCanceled());
    TEST_VALUE(num_threads != s_not_thrown, token_state == TokenState::DONE_NOT_WELL);
    TEST_VALUE(s_thrown_memurai.load() != 0, token.gotMemuraiExceptions());
    TEST_VALUE(s_thrown_std.load() != 0, token.gotStdExceptions());
    TEST_VALUE(s_thrown_other.load() != 0, token.gotUnknownExceptions());
}

void test_cancellations(bool quick) {
    const uint32 factor = quick ? 4 : 2;
    const vector<uint32> long_list_num_threads{4, 8, 16, 32};
    const vector<uint32> short_list_num_threads{4, 8};
    const vector<uint32>& list_num_threads = quick ? short_list_num_threads : long_list_num_threads;

    for (auto num_threads : list_num_threads) {
        uint32 threadPoolSize = num_threads + 1;
        test_cancellations_complex(threadPoolSize, num_threads);
    }

    for (uint32 num_threads : list_num_threads) {
        uint32 threadPoolSize = num_threads + 1;

        test_cancellations_simple(threadPoolSize, num_threads, Behavior::throw_memurai_exc);
        test_cancellations_simple(threadPoolSize, num_threads, Behavior::do_not_throw);
        test_cancellations_simple(threadPoolSize, num_threads, Behavior::throw_std_exc);
        test_cancellations_simple(threadPoolSize, num_threads, Behavior::throw_other);
    }
}

} // namespace

void test_thread_pool(bool quick) {
    RUN_TEST {
        test_empty();
    };

    test_cancellations(quick);

    const vector<uint32> long_list_num_threads{4, 8, 16, 32};
    const vector<uint32> short_list_num_threads{4, 8};
    const vector<uint32>& list_num_threads = quick ? short_list_num_threads : long_list_num_threads;

    for (auto num_threads : list_num_threads) {
        test_submits(num_threads, quick);
    }
}

using memurai::multicore::TokenState;

template <> bool TEST_VALUE_IMPL<TokenState, TokenState>(const TokenState& expected, const TokenState& gotten, int linenum) {
    if (expected != gotten) {
        LOG_ERROR(g_test_name << ": at line " << linenum << " Expected " << to_string(expected) << "\n gotten " << to_string(gotten));
        g_errors++;
        l_errors++;
        return true;
    }
    return false;
}
