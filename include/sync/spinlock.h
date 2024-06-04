/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <atomic>

namespace memurai {

class spinlock {
public:
    spinlock() = default;

    void lock() {
        while (flag.test_and_set(std::memory_order_acquire)) // acquire lock
            while (flag.test(std::memory_order_relaxed))     // test lock
                ;                                            // spin
    }

    void unlock() {
        flag.clear(std::memory_order_release); // release lock
    }

private:
    std::atomic_flag flag;
};

} // namespace memurai
