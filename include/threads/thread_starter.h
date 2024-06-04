/*
 * Copyright (c) 2020-2021, Janea Systems
   by Benedetto Proietti
 */

#pragma once

#include <atomic>
#include <string>

#include "logger.h"
#include "types.h"

namespace memurai::multicore {
using std::atomic;
using std::string;

template <typename Queue, typename BadExc, typename GoodExc> //, typename Context>
class WorkerThread;

class ThreadStarter {
    atomic<bool> s_finishing;
    atomic<uint64> num_running, num_constructed, num_exceptions, num_waiting;

    void notify_waiting() {
        num_waiting++;
    }
    void notify_working() {
        num_waiting--;
    }

    void notify_exception() {
        num_exceptions++;
    }

    void notify_running() {
        num_running++;
    }
    void notify_exiting() {
        num_running--;
    }

    void notify_ctor() {
        num_constructed++;
    };
    void notify_dtor() {
        num_constructed--;
    };

public:
    template <typename Queue, typename BadExc, typename GoodExc> friend class WorkerThread;

    ThreadStarter(ThreadStarter&&) = delete;
    ThreadStarter(const ThreadStarter&) = delete;

    ThreadStarter() = default;

    ~ThreadStarter() {
        stop();
    }

    [[nodiscard]] bool isFinishing() const {
        return s_finishing.load();
    }

    virtual void stop() {
        s_finishing.store(true);
    }
};

} // namespace memurai::multicore
