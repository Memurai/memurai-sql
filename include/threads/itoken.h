/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#pragma once

#include <atomic>
#include <exception>
#include <optional>
#include <sstream>
#include <string>
#include <tuple>

#include "exception.h"
#include "jobid.h"

namespace memurai::multicore {

using std::atomic;
using std::optional;
using std::string;
using std::tuple;

enum class TokenState { CREATED, CANCELED, DONE_NOT_WELL, DONE_WELL };

string to_string(TokenState s);

class IToken {
    atomic<uint32> got_std_exceptions, got_memurai_exceptions, got_unknown_exceptions;

    JOBID jobId;

    std::stringstream fullState;
    mutable std::mutex m_fullState;

protected:
    atomic<int64> numDone, numCancels;

public:
    explicit IToken(JOBID jobId) : jobId(jobId), numDone(0), numCancels(0), got_std_exceptions(0), got_memurai_exceptions(0), got_unknown_exceptions(0) {}

    virtual void notify_exception(const std::exception& ex) {
        got_std_exceptions++;
        numCancels++;
        std::unique_lock<std::mutex> guard(m_fullState);
        fullState << "std::exception:\n" << ex.what() << "\n";
    }

    virtual void notify_exception(const memurai::exception& ex) {
        got_memurai_exceptions++;
        numCancels++;
        std::unique_lock<std::mutex> guard(m_fullState);
        fullState << "memurai::exception:\n" << ex.what() << "\n";
    }

    virtual void notify_exception() {
        got_unknown_exceptions++;
        numCancels++;
        std::unique_lock<std::mutex> guard(m_fullState);
        fullState << "unknown exception.\n";
    }

    virtual void join() = 0;

    virtual void done() {
        numDone++;
    }

    bool isCanceled() const noexcept {
        return (numCancels > 0);
    }

    JOBID getJobId() const {
        return jobId;
    }

    virtual string getStateString() const noexcept {
        std::unique_lock<std::mutex> guard(m_fullState);
        return std::move(fullState.str());
    }

    bool gotStdExceptions() const noexcept {
        return (got_std_exceptions > 0);
    }
    bool gotMemuraiExceptions() const noexcept {
        return (got_memurai_exceptions > 0);
    }
    bool gotUnknownExceptions() const noexcept {
        return (got_unknown_exceptions > 0);
    }
};

} // namespace memurai::multicore
