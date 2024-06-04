/*
* Copyright (c) 2021, Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <atomic>

#include "jobid.h"
#include "types.h"

namespace memurai::sql {
class QWalker;
using multicore::JOBID;
using std::atomic;

class QueryCtx {
    static atomic<uint64> g_queryId;
    uint64 queryId;
    QWalker* handle_walker;

public:
    explicit QueryCtx(QWalker*);

    [[nodiscard]] uint64 getQueryId() const;

    [[nodiscard]] void* getHandle() const;

    void close();
};

} // namespace memurai::sql
