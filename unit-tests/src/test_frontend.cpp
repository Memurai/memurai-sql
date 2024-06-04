/*
* Copyright (c) 2021, Janea Systems
by Benedetto Proietti
*/

#include <barrier>
#include <string>
#include <utility>

#include "test/test_utils.h"

#include "error_codes.h"
#include "frontend.h"
#include "logger.h"
#include "server.h"

using namespace memurai;
using namespace memurai::multicore;
using namespace memurai::sql;
using namespace std;

#undef TEST_START

#define TEST_START                                                                                               \
    g_test_name = string(__func__) + g_addendum;                                                                 \
    g_addendum = "";                                                                                             \
    l_errors = 0;                                                                                                \
    l_warnings = 0;                                                                                              \
    LOG_VERBOSE(g_test_name << " is starting ");                                                                 \
    SCOPE_EXIT {                                                                                                 \
        LOG_VERBOSE(g_test_name << " ended with: " << l_errors << " errors and " << l_warnings << " warnings."); \
    };

extern string g_addendum;

#define PREPARE_THREAD_POOL_AND_STUFF                                                               \
    typedef WorkerThread<syncqueue<TLambda>, std::exception, memurai::exception> WorkerThreadSync;  \
    ModuleInterface module_interface;                                                               \
    ThreadPool<WorkerThreadSync> thread_pool(g_test_name, threadPoolSize);                          \
    multicore::IServer server{[&](vector<Lambda> lambdas, IToken* token) {                          \
                                  Token* innertoken = new Token("wg_token", lambdas.size(), token); \
                                  thread_pool.submit(lambdas, innertoken);                          \
                              },                                                                    \
                              [&](Lambda item, IToken* token) {                                     \
                                  thread_pool.submit(item, token);                                  \
                              },                                                                    \
                              [](Lambda) {},                                                        \
                              [](Lambda, uint64, JOBID) {},                                         \
                              [](JOBID) {},                                                         \
                              [](string) {},                                                        \
                              module_interface};

vector<tuple<string, string, JOBID, string, string>> g_replies;

#define STR_VALUE_LATEST_REPLY get<4>(g_replies.back())

class mockTSC : public memurai::TSCInterface {
    std::atomic<bool> s_done;

    std::atomic<bool> s_locked;

public:
    mockTSC() : s_done(false), s_locked(false) {}

    ~mockTSC() {
        finish();
    }

    void lock() final {
        EXPECT_OR_THROW(s_locked == true, InternalError, "mockTSC: s_locked is false in 'unlock'.");
        s_locked = true;
    }

    void unlock() final {
        EXPECT_OR_THROW(s_locked == false, InternalError, "mockTSC: s_locked is false in 'unlock'.");
        s_locked = false;
    }

    void finish() final {
        if (!s_done.load(std::memory_order_acquire)) {
            s_done.store(true, std::memory_order_release);
        }
    }
};

OuterCtx* g_ctx = nullptr;

uint64 get_size(FrontEnd* frontend, string name, int linenum) {
    auto ret = frontend->processMsg_sync({"msql.size", std::move(name)}, JOBID(9), g_ctx);
    TEST_VALUE_IMPL(true, std::holds_alternative<Reply>(ret), linenum);

    if (std::holds_alternative<Reply>(ret)) {
        return std::get<Reply>(ret).retcode;
    }

    return 0;
}

template <typename Reply> void expect_same_replies(const Reply r1, const Reply r2, int linenum) {
    TEST_VALUE_IMPL(get<0>(r1), get<0>(r2), linenum);
    TEST_VALUE_IMPL(get<1>(r1), get<1>(r2), linenum);
    TEST_VALUE_IMPL(get<3>(r1), get<3>(r2), linenum);
    TEST_VALUE_IMPL(get<4>(r1), get<4>(r2), linenum);
}

#define EXPECT_SAME_REPLIES(R1, R2)                       \
    {                                                     \
        auto linenum = __LINE__;                          \
        TEST_VALUE_IMPL(get<0>(R1), get<0>(R2), linenum); \
        TEST_VALUE_IMPL(get<1>(R1), get<1>(R2), linenum); \
        TEST_VALUE_IMPL(get<3>(R1), get<3>(R2), linenum); \
        TEST_VALUE_IMPL(get<4>(R1), get<4>(R2), linenum); \
    }

#define EXPECT_SAME_REPLIES_EXCEPT_KEY(R1, R2)            \
    {                                                     \
        auto linenum = __LINE__;                          \
        TEST_VALUE_IMPL(get<0>(R1), get<0>(R2), linenum); \
        TEST_VALUE_IMPL(get<1>(R1), get<1>(R2), linenum); \
        TEST_VALUE_IMPL(get<4>(R1), get<4>(R2), linenum); \
    }

#define EXPECT_REPLY(DOMAIN, OP, KEY, VALUE) expect_reply(DOMAIN, OP, KEY, VALUE, __LINE__)

void expect_reply(const string& domain, const string& op, const string& key, const string& value, int linenum) {
    auto latest = g_replies.back();

    TEST_VALUE_IMPL(domain, get<0>(latest), linenum);
    TEST_VALUE_IMPL(op, get<1>(latest), linenum);
    TEST_VALUE_IMPL(key, get<3>(latest), linenum);
    TEST_VALUE_IMPL(value, get<4>(latest), linenum);
}

namespace {

#define EXPECT_NO_EXCEPTION

void test_simple(bool quick) {
    uint32 threadPoolSize = 1;
    TEST_START;
    PREPARE_THREAD_POOL_AND_STUFF;

    const string tmp_folder = "temp_folder";
    FrontEnd::Config config = {tmp_folder, true};

    try {
        // Simple ctor
        {
            delete_test_files(tmp_folder);
            FrontEnd frontend(server, config);
        }

        // Create cmd
        {
            delete_test_files(tmp_folder);
            FrontEnd frontend(server, config);

#define TEST_FE_SYNC(EXP, CMD)                                                                              \
    {                                                                                                       \
        auto ret = CMD;                                                                                     \
        TEST_VALUE(true, std::holds_alternative<Reply>(ret) && (std::get<Reply>(ret).retcode == int(EXP))); \
        if (!(std::holds_alternative<Reply>(ret) && (std::get<Reply>(ret).retcode == int(EXP)))) {          \
            LOG_DEBUG("DEBUG: hold_alternative = " << std::holds_alternative<Reply>(ret));                  \
        }                                                                                                   \
    }

            // M.CREATE mymovies ON hash movie: title TEXT release_year NUMERIC rating BIGINT genre TEXT
            // Correct syntax, should pass
            auto ret = frontend.processMsg_sync(
                {"msql.create", "mymovies", "ON", "hash", "movie:", "title", "TEXT", "release_year", "BIGINT", "rating", "FLOAT", "genre", "TEXT"},
                JOBID(1),
                g_ctx);
            TEST_VALUE(true, std::holds_alternative<Reply>(ret) && (std::get<Reply>(ret).retcode == 0));

            // incorrect syntax, should fail
            // msql.create
            TEST_FE_SYNC(ErrorCode::ERROR_SQL_WRONG_FORMAT,
                         frontend.processMsg_sync(
                             {"msql.create", "mymovies", "O", "hash", "movie:", "title", "TEXT", "release_year", "BIGINT", "rating", "FLOAT", "genre", "TAG"},
                             JOBID(1),
                             g_ctx));
            TEST_FE_SYNC(ErrorCode::ERROR_SQL_WRONG_FORMAT,
                         frontend.processMsg_sync(
                             {"msql.create", "mymovies", "ON", "hah", "movie:", "title", "TEXT", "release_year", "BIGINT", "rating", "FLOAT", "genre", "TAG"},
                             JOBID(1),
                             g_ctx));
            TEST_FE_SYNC(ErrorCode::ERROR_SQL_WRONG_COL_TYPE,
                         frontend.processMsg_sync(
                             {"msql.create", "mymovies", "ON", "hash", "movie:", "title", "TEXTo", "release_year", "BIGINT", "rating", "FLOAT", "genre", "TAG"},
                             JOBID(1),
                             g_ctx));
            TEST_FE_SYNC(ErrorCode::ERROR_SQL_WRONG_COL_TYPE,
                         frontend.processMsg_sync(
                             {"msql.create", "mymovies", "ON", "hash", "movie:", "title", "TEXT", "release_year", "NUMERI", "rating", "FLOAT", "genre", "TAG"},
                             JOBID(1),
                             g_ctx));
            TEST_FE_SYNC(ErrorCode::ERROR_SQL_WRONG_COL_TYPE,
                         frontend.processMsg_sync(
                             {"msql.create", "mymovies", "ON", "hash", "movie:", "title", "TEXT", "release_year", "BIGINT", "rating", "FLOAT", "genre", "TAG"},
                             JOBID(1),
                             g_ctx));
            TEST_FE_SYNC(ErrorCode::ERROR_SQL_WRONG_COL_TYPE,
                         frontend.processMsg_sync(
                             {"msql.create", "mymovies", "ON", "hash", "movie:", "title", "TEXT", "release_year", "BIGINT", "rating", "FLOAT", "genre", "TAZ"},
                             JOBID(1),
                             g_ctx));
            TEST_FE_SYNC(ErrorCode::ERROR_SQL_WRONG_ARGC,
                         frontend.processMsg_sync(
                             {"msql.create", "mymovies", "ON", "hash", "movie:", "title", "TEXT", "release_year", "BIGINT", "rating", "FLOAT", "genre"},
                             JOBID(1),
                             g_ctx));
            TEST_FE_SYNC(ErrorCode::ERROR_SQL_WRONG_ARGC,
                         frontend.processMsg_sync(
                             {"msql.create", "mymovies", "hash", "movie:", "title", "TEXT", "release_year", "BIGINT", "rating", "FLOAT", "genre", "TAG"},
                             JOBID(1),
                             g_ctx));
            TEST_FE_SYNC(ErrorCode::ERROR_SQL_WRONG_ARGC,
                         frontend.processMsg_sync(
                             {"msql.create", "mymovies", "ON", "movie:", "title", "TEXT", "release_year", "BIGINT", "rating", "FLOAT", "genre", "TAG"},
                             JOBID(1),
                             g_ctx));
            TEST_FE_SYNC(
                ErrorCode::ERROR_SQL_CMD_NOT_SUPPORTED,
                frontend.processMsg_sync(
                    {"msql.creatooo", "mymovies", "ON", "hash", "movie:", "title", "TEXT", "release_year", "BIGINT", "rating", "FLOAT", "genre", "TAG"},
                    JOBID(1),
                    g_ctx));

            // msql.describe
            TEST_FE_SYNC(ErrorCode::ERROR_SQL_WRONG_ARGC, frontend.processMsg_sync({"msql.describe", "myxxx", "mymovies"}, JOBID(1), g_ctx));
            TEST_FE_SYNC(ErrorCode::ERROR_SQL_TABLE_DOES_NOT_EXIST, frontend.processMsg_sync({"msql.describe", "mymovie"}, JOBID(1), g_ctx));
            TEST_FE_SYNC(ErrorCode::ERROR_SQL_CMD_NOT_SUPPORTED, frontend.processMsg_sync({"msql.describ", "mymovies"}, JOBID(1), g_ctx));

            // msql.show
            TEST_FE_SYNC(ErrorCode::ERROR_SQL_WRONG_ARGC, frontend.processMsg_sync({"msql.show", "mymovies"}, JOBID(1), g_ctx));
            TEST_FE_SYNC(ErrorCode::ERROR_SQL_WRONG_FORMAT, frontend.processMsg_sync({"msql.show", "with", "colmns"}, JOBID(1), g_ctx));
            TEST_FE_SYNC(ErrorCode::ERROR_SQL_CMD_NOT_SUPPORTED, frontend.processMsg_sync({"msql.sho"}, JOBID(1), g_ctx));
        }
    } catch (...) {
        // should not throw exception
        TEST_VALUE(1, 0);
    }
}

void test_double_tables(bool quick) {
    uint32 threadPoolSize = 1;
    TEST_START;
    PREPARE_THREAD_POOL_AND_STUFF;

    try {

        const string tmp_folder = "temp_folder";
        FrontEnd::Config config = {tmp_folder, true};

        // Test 1 - create tables
        {
            delete_test_files(tmp_folder);
            FrontEnd frontend(server, config);

            // Create first table on "movie:" prefix
            TEST_FE_SYNC(SUCCESS,
                         frontend.processMsg_sync(
                             {"msql.create", "mymovies", "ON", "hash", "movie:", "title", "TEXT", "release_year", "BIGINT", "rating", "FLOAT", "genre", "TEXT"},
                             JOBID(1),
                             g_ctx));
            // Create second table on "movie:" prefix, same name. FAIL
            TEST_FE_SYNC(ErrorCode::ERROR_SQL_TABLE_ALREADY_EXISTS,
                         frontend.processMsg_sync({"msql.create", "mymovies", "ON", "hash", "moviexxx:", "title", "TEXT"}, JOBID(1), g_ctx));
            // Create second table on "movie:" prefix, different name. SUCCESS
            TEST_FE_SYNC(SUCCESS, frontend.processMsg_sync({"msql.create", "mymovies2", "ON", "hash", "moviexxx:", "title", "TEXT"}, JOBID(1), g_ctx));
        }

        // Test 2 - restore tables
        {
            FrontEnd frontend(server, config);

            // Tables should have been restored from metadata
            TEST_FE_SYNC(ErrorCode::ERROR_SQL_TABLE_ALREADY_EXISTS,
                         frontend.processMsg_sync(
                             {"msql.create", "mymovies", "ON", "hash", "movie:", "title", "TEXT", "release_year", "BIGINT", "rating", "FLOAT", "genre", "TEXT"},
                             JOBID(1),
                             g_ctx));
            TEST_FE_SYNC(ErrorCode::ERROR_SQL_TABLE_ALREADY_EXISTS,
                         frontend.processMsg_sync({"msql.create", "mymovies2", "ON", "hash", "moviexxx:", "title", "TEXT"}, JOBID(1), g_ctx));
        }
    } catch (...) {
        // should not throw exception
        TEST_VALUE(1, 0);
    }
} // function

} // namespace

void test_frontend(bool quick) {
    test_simple(quick);

    test_double_tables(quick);
}
