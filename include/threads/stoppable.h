/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#pragma once

#include <atomic>
#include <string>

#include "inc_during_scope.h"
#include "logger.h"
#include "types.h"

namespace memurai::multicore {
using std::atomic;

class Stoppable {
    mutable atomic<bool> s_finishing;

protected:
    mutable atomic<uint64> num_running_commands;

    void long_running_method_start() const {
        num_running_commands.fetch_add(1, std::memory_order_acq_rel);
    }
    void long_running_method_end() const {
        num_running_commands.fetch_sub(1, std::memory_order_acq_rel);
    }

public:
    Stoppable() : s_finishing(false), num_running_commands(0) {}

    bool isFinishing() const {
        return s_finishing.load();
    }

    virtual void stop() {
        s_finishing.store(true);
    }

    virtual void join() = 0;
};

#define CHECK_STOPPING \
    if (isFinishing()) \
        return;
#define CHECK_STOPPING_AND_RETURN(RET) \
    if (isFinishing())                 \
        return (RET);

} // namespace memurai::multicore
