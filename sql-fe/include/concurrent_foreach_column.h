/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti

   Thanks to Andrei Alexandrescu for thinking about this. (CppCon 2015)
 */

#pragma once

#include "column.h"
#include "memurai-multicore.h"

//
// Usage:
//
// CONCURRENT_FOREACH_COLUMN(int i=0; i<10; i++)
// {
//       do_something_with_i(i);
///  };
//

#define CONCURRENT_FOREACH_COLUMN_CAT2(x, y) x##y
#define CONCURRENT_FOREACH_COLUMN_CAT(x, y) CONCURRENT_FOREACH_COLUMN_CAT2(x, y)
#define CONCURRENT_FOREACH_COLUMN(IDX)                                  \
    const auto CONCURRENT_FOREACH_COLUMN_CAT(scopeExit_, __COUNTER__) = \
        ConcurrentForeachColumn::MakeConcurrentForeachColumn(server, num_all_columns, __LINE__) += [&](ColIdx colIdx)

namespace ConcurrentForeachColumn {
using namespace memurai;
using namespace memurai::multicore;
using namespace memurai::sql;

typedef std::function<void(ColIdx)> ColLambda;

class ConcurrentForeachColumn {
public:
    explicit ConcurrentForeachColumn(ColLambda&& fn, IServer& server, ColIdx num_columns, int linenum) : m_fn(fn), server(server), num_columns(num_columns) {
        Token token(std::string("CONCURRENT_FOREACH_COLUMN_") + std::to_string(linenum), 0, num_columns);

        for (ColIdx colIdx = 0; colIdx < num_columns - 1; colIdx++) {
            // Note that we capture "colIdx" by value, do NOT capture it by ref!
            // Because in a moment it will become one too big :)
            server.schedule_once_with_token(
                [&, colIdx]() {
                    m_fn(colIdx);
                },
                &token);
        }
        m_fn(num_columns - 1);

        token.done();
        token.join();
    }

    ~ConcurrentForeachColumn() = default;

    ConcurrentForeachColumn(ConcurrentForeachColumn&& other) noexcept : m_fn(std::move(other.m_fn)), server(other.server), num_columns(other.num_columns) {}

    ConcurrentForeachColumn(const ConcurrentForeachColumn&) = delete;
    ConcurrentForeachColumn& operator=(const ConcurrentForeachColumn&) = delete;

private:
    ColLambda m_fn;
    IServer& server;
    ColIdx num_columns;
};

struct MakeConcurrentForeachColumn {
    IServer& server;
    ColIdx num_columns;
    int linenum;

    MakeConcurrentForeachColumn(IServer& server, ColIdx num_columns, int linenum) : server(server), num_columns(num_columns), linenum(linenum) {}

    ConcurrentForeachColumn operator+=(ColLambda&& fn) {
        return ConcurrentForeachColumn(std::move(fn), server, num_columns, linenum);
    }
};
} // namespace ConcurrentForeachColumn