/*
 * Copyright (c) 2020-2021, Janea Systems
   by Benedetto Proietti
 */

#pragma once

#include <exception>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <tuple>

#include "exception.h"
#include "itoken.h"
#include "lambda.h"
#include "logger.h"
#include "types.h"

namespace memurai::multicore {

using std::optional;
using std::string;
using std::string_view;
using std::tuple;

struct TLambda {
    Lambda lambda;
    IToken* token;
};

/*
    class Token

    Cancellation is the real problem of multithreading computation.
    My advice is: don't do multithreading if you are not very very familiar with it.
    And even if you are, avoid it when possible.
    Nevertheless... here we are.

    Token responsibilities: (soon to be refactored out in Policies probably)
    - collects exceptions info
    - provides "join" synchronization for a PREDETERMINED number of jobs.
    - let other jobs know whether it is canceled or good.

*/

class Token : public IToken {
    typedef TokenState State;

    string name;
    State state;

    uint64 numJobs;

    atomic<uint64> numBusy;

    mutable std::mutex m_join;
    std::condition_variable cv_finished;

    bool merged;
    IToken* parent_token;

public:
    Token(string_view name, JOBID jobId, uint64 numJobs)
        : IToken(jobId), name(name), state(State::CREATED), numBusy(numJobs), numJobs(numJobs), merged(false), parent_token(nullptr) {}

    Token(string_view name, uint64 numJobs, IToken* parent)
        : IToken(parent->getJobId()), name(name), state(State::CREATED), numBusy(numJobs), numJobs(numJobs), merged(true), parent_token(parent) {}

    Token(const Token&) = delete;

    string_view getName() const {
        return name;
    }

    uint64 getNumJobs() const {
        return numJobs;
    }

    ~Token() noexcept = default;

    bool isGood() const noexcept {
        return (state == State::DONE_WELL);
    }

    tuple<State, optional<string>> getState() const noexcept {
        std::string s;

        if (state == State::DONE_WELL)
            return make_tuple(State::DONE_WELL, std::nullopt);
        else {
            s = IToken::getStateString();
        }
        return make_tuple(state, s);
    }

    void notify_exception(const std::exception& ex) final {
        IToken::notify_exception(ex);
        LOG_DEBUG("Token " << name << " received std::exception '" << ex.what() << "'. Canceling.");
        if (parent_token) {
            this->parent_token->notify_exception(ex);
        }

        cancel();
    }
    void notify_exception(const memurai::exception& ex) final {
        IToken::notify_exception(ex);
        LOG_DEBUG("Token " << name << " received memurai::exception '" << ex.what() << "'. Canceling??");
        if (parent_token) {
            this->parent_token->notify_exception(ex);
        }
        cancel();
    }
    void notify_exception() final {
        IToken::notify_exception();
        LOG_DEBUG("Token " << name << " received unkwon exception. Canceling. And aborting.??");
        if (parent_token) {
            this->parent_token->notify_exception();
        }
        cancel();
    }

    void done() final {
        IToken::done();

        if ((numDone + numCancels) == numJobs) {
            state = (numCancels == 0) ? State::DONE_WELL : State::DONE_NOT_WELL;
        }

        // yes, scope.
        {
            std::unique_lock<std::mutex> lock(m_join);
            numBusy--;
        }
        if (numBusy == 0) {
            if (merged)
                parent_token->done();
            cv_finished.notify_all();
        }
    }

private:
    void cancel() {
        // yes, scope.
        {
            std::unique_lock<std::mutex> lock(m_join);
            numBusy--;

            if ((state == State::CREATED) || (state == State::CANCELED)) {
                state = State::CANCELED;
            } else {
                assert((state == State::DONE_NOT_WELL));
            }
            if ((numDone + numCancels) == numJobs) {
                state = State::DONE_NOT_WELL;
            }
        }
        if (numBusy == 0) {
            cv_finished.notify_all();
        }
    }

public:
    void join() final {
        // If not yet started (numJobs==0) or not yet finished (numBusy>0)... wait...
        if (numBusy.load() > 0) {
            std::unique_lock<std::mutex> lock(m_join);
            cv_finished.wait(lock, [this]() {
                return (numBusy.load() == 0);
            });
        }

        LOG_DEBUG("Token " << name << " done with status: " << to_string(state));
    }
};

} // namespace memurai::multicore
