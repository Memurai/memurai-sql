/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

#include "itoken.h"

namespace memurai::multicore {

using std::vector;

class GroupToken : public IToken {
public:
    explicit GroupToken(JOBID jobId) : IToken(jobId) {}

    void join() override {
        assert(UNREACHED);
        EXPECT_OR_THROW(false, InternalError, "GroupTokens should not be 'join'ed.");
    }

    void done() override {
        IToken::done();
    }

    tuple<int64, int64, string> getState() const noexcept {
        return make_tuple(numDone.load(), numCancels.load(), IToken::getStateString());
    }
};

} // namespace memurai::multicore
