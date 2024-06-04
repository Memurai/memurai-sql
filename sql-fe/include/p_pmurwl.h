/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#pragma once

#include <atomic>
#include <emmintrin.h>
#include <mutex>

#include "lambda.h"
#include "scope/expect.h"
#include "scope/scope_exit.h"
#include "threads/stoppable.h"
#include "types.h"

namespace memurai {
using multicore::Stoppable;
using std::atomic;
using std::atomic_bool;
using std::mutex;

#define __CHECK_STOPPING         \
    if (stoppable.isFinishing()) \
        return;
#define __CHECK_STOPPING_AND_RETURN(RET) \
    if (stoppable.isFinishing())         \
        return (RET);

// Policy
// Poor's man upgradable read-write lock
// Design By Policy' God bless Alexandrescu. Added Policy for 'LockLessAcquire LockFullMutate'.
// Many can add to a datastructure with no locking (just an interlocked access), one can mutate with a lock (forcing all others to block).
//
class P_PMURWL {
    Stoppable& stoppable;

    // Read/Write mutex for compute vs mutate methods
    mutable std::shared_mutex m_mutex_rw;

    // Real mutating is controllled by the mutex, so we can have only one "mutate" at the time.
    // The flag is needed to avoid taking the mutex on addRow when we are not "mutate"ing.
    atomic_bool s_isAttepmtingMutate;

    atomic_bool s_simple_op_upgrading;

    // number of active simple commands (should be 1 or 0)
    atomic<uint64> s_simple_commands;
    atomic<uint64> s_computes_active;
    mutable std::mutex m_mutex_simple_op;

public:
protected:
    // Derived class MUST not use a unique lock on this
    // Shared only
    // Except on termination (ex: IdxTable::join())
    // std::shared_mutex & getSharedMutex() const { return m_mutex_rw; }

    // ctor
    explicit P_PMURWL(Stoppable& stoppable)
        : s_simple_commands(0), s_isAttepmtingMutate(false), stoppable(stoppable), s_simple_op_upgrading(false), s_computes_active(0) {}

    void waitIfMutating() {
        // If mutate is already happening, spinlock away!!!
        while (s_isAttepmtingMutate.load(std::memory_order_acquire))
            [[unlikely]] {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }

        // First, we let everybody know that there's another "add" operation going on.
        // This is needed because some "add" threads could have gotten here, but in
        // the meantime "mutate" tried to start. It cannot really start until
        // the active "add" threads finish.
    }

    // It'd be nice to have an upgradable read-write lock.
    // Boost should have one...
    // For now, we just disable all readers and wait for the active ones to be done.
    //

    void syncMutate(const Lambda& mutate) {
        __CHECK_STOPPING;

        s_isAttepmtingMutate.store(true, std::memory_order_release);
        mutate_wait_non_mutating();

        std::unique_lock lock(m_mutex_rw);
        __CHECK_STOPPING;

        mutate();

        s_isAttepmtingMutate.store(false, std::memory_order_release);
    }

    void syncMutateUpgrade(const Lambda& mutate) {
        s_simple_op_upgrading.store(true);

        syncMutate(mutate);
    }

    bool perform_simple_operation(const Lambda& simple_op) {
        __CHECK_STOPPING_AND_RETURN(false);
        waitIfMutating();
        __CHECK_STOPPING_AND_RETURN(false);

        // write stuff
        // ONE thread at the time...
        std::unique_lock lock(m_mutex_simple_op);
        // Check again if mutating just started
        if (s_isAttepmtingMutate.load())
            return false;

        IncrementDuringScope guard1(s_simple_commands);

        // here we go
        //
        simple_op();
        // simple_op done.

        return true;
    }

public:
    void compute_lock() {
        EXPECT_OR_THROW(!stoppable.isFinishing(), InternalError, "Trying to lock table while it is shutting down.");
        s_computes_active++;
        m_mutex_rw.lock_shared();
    }

    void compute_unlock() {
        m_mutex_rw.unlock_shared();
        s_computes_active--;
    }

    uint64 getActiveComputes() const {
        return s_computes_active.load();
    }

private:
    // Wait all acquirers to finsih or to be blocked in WAIT_MUTATE
    void mutate_wait_non_mutating() {
        uint64 limit = s_simple_op_upgrading.load() ? 1 : 0;

        // Spin until all add commands have terminated.
        // New ones won't start because of the flag set to false above.
        while (s_simple_commands.load(std::memory_order_acquire) > limit) {
            _mm_pause();
        }
    }
};

} // namespace memurai

#undef __CHECK_STOPPING
#undef __CHECK_STOPPING_AND_RETURN
