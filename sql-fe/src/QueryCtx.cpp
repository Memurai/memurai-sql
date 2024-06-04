/*
* Copyright (c) 2021 Janea Systems
by Benedetto Proietti
*/

#include "QueryCtx.h"
#include "QueryWalker.h"

namespace memurai::sql {

using std::atomic;

atomic<uint64> QueryCtx::g_queryId{1};

QueryCtx::QueryCtx(QWalker* w) : handle_walker(w) {
    queryId = g_queryId++;
}

uint64 QueryCtx::getQueryId() const {
    return queryId;
}

void* QueryCtx::getHandle() const {
    return handle_walker;
}

void QueryCtx::close() {
    delete handle_walker;
}

} // namespace memurai::sql
