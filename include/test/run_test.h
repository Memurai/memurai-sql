/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti

Thanks to Andrei Alexandrescu for thinking about this. (CppCon 2015)
*/

#pragma once

#include <chrono>
#include <exception>
#include <string>
#include <utility>

#include "exception.h"
#include "test_utils.h"

//
// Usage:
//
// RUN_TEST{ cout << "hello"; }; // will be called at the scope exit and duration millis will be returned
//

#define RUN_TEST_CAT2(x, y) x##y
#define RUN_TEST_CAT(x, y) RUN_TEST_CAT2(x, y)
#define RUN_TEST const auto RUN_TEST_CAT(takeTime_, __COUNTER__) = RunTest::MakeRunTest(g_test_name) += [&]

namespace RunTest {

template <typename F> class RunTest {
    uint64 ret_ms;
    bool failed;
    const string test_name;

public:
    explicit RunTest(F&& fn, const string& test_name) : m_fn(fn), failed(false), test_name(test_name) {
        auto time_start = std::chrono::high_resolution_clock::now();

        try {
            m_fn();
        } catch (const memurai::exception& ex) {
            cout << "Got memurai::exception:\n" << ex.what();
            failed = true;
        } catch (const std::exception& ex) {
            cout << "Got std::exception:\n" << ex.what();
            failed = true;
        } catch (...) {
            cout << "Got unknown exception\n";
            failed = true;
        }
        auto time_end = std::chrono::high_resolution_clock::now();
        ret_ms = std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count();

        if (failed || (l_errors > 0)) {
            LOG_ERROR(test_name << " : FAIL after " << ret_ms << "ms.  with " << l_errors << " errors.\n");
        } else {
            LOG_INFO(test_name << " : SUCCESS after " << ret_ms << "ms. with " << l_warnings << " warnings \n");
        }
    }

    ~RunTest() = default;

    RunTest(RunTest&& other) noexcept : ret_ms(other.ret_ms), failed(other.failed), m_fn(std::move(other.m_fn)), test_name(other.test_name) {}

    RunTest(const RunTest&) = delete;
    RunTest& operator=(const RunTest&) = delete;

private:
    F m_fn;
};

struct MakeRunTest {
    const string name;

    explicit MakeRunTest(string name) : name(std::move(name)) {}

    template <typename F> RunTest<F> operator+=(F&& fn) {
        return RunTest<F>(std::forward<F>(fn), name);
    }
};

} // namespace RunTest
