/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti

   Thanks to Andrei Alexandrescu for thinking about this. (CppCon 2015)
 */

#pragma once

#include <chrono>

//
// Usage:
//
// TAKE_TIME{ cout << "hello"; }; // will be called at the scope exit and duration millis will be returned
//

#define TAKE_TIME TakeTime::MakeTakeTime() += [&]

namespace TakeTime {

template <typename F> class TakeTime {
public:
    explicit TakeTime(F&& fn) : m_fn(fn) {
        auto time_start = std::chrono::high_resolution_clock::now();
        m_fn();
        auto time_end = std::chrono::high_resolution_clock::now();

        ret = std::chrono::duration_cast<std::chrono::microseconds>(time_end - time_start).count();
    }

    [[nodiscard]] uint64 micros() const {
        return ret;
    }

    [[nodiscard]] uint64 millis() const {
        return ret / 1000;
    }

    ~TakeTime() = default;

    TakeTime(TakeTime&& other) noexcept : ret(0), m_fn(std::move(other.m_fn)) {}

    TakeTime(const TakeTime&) = delete;
    TakeTime& operator=(const TakeTime&) = delete;

private:
    uint64 ret;
    F m_fn;
};

struct MakeTakeTime {
    MakeTakeTime() = default;

    template <typename F> TakeTime<F> operator+=(F&& fn) {
        return TakeTime<F>(std::forward<F>(fn));
    }
};

} // namespace TakeTime
