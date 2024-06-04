/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#include <barrier>
#include <map>
#include <string>
#include <vector>

#include "IActual.h"
#include "QueryWalker.h"

#include "PublisherCount.h"
#include "database.h"
#include "memurai-multicore.h"
#include "query2ds.h"
#include "scope/take_time.h"
#include "test/test_table_utils.h"
#include "threads/cancellation.h"
#include "threads/thread_pool.h"
#include "threads/worker_thread.h"

using namespace std;
using namespace memurai;
using namespace memurai::multicore;
using namespace memurai::sql;

#define TAG TEXT

#undef TEST_START

#define TEST_START                                                                                               \
    g_test_name = string(__func__) + g_addendum;                                                                 \
    l_errors = 0;                                                                                                \
    l_warnings = 0;                                                                                              \
    LOG_VERBOSE(g_test_name << " is starting ");                                                                 \
    SCOPE_EXIT {                                                                                                 \
        LOG_VERBOSE(g_test_name << " ended with: " << l_errors << " errors and " << l_warnings << " warnings."); \
        g_addendum = "";                                                                                         \
        std::this_thread::sleep_for(std::chrono::milliseconds(200));                                             \
    };

extern string g_addendum;

#define PREPARE_THREAD_POOL_AND_STUFF                                                                 \
    typedef WorkerThread<syncqueue<TLambda>, Token, std::exception, memurai::exception> WorkerThread; \
    ThreadPool<WorkerThread, Token> thread_pool(g_test_name, threadPoolSize);                         \
    multicore::IServer server{[&](vector<Lambda> lambdas, Token* token) {                             \
                                  Token* innertoken = new Token("wg_token", lambdas.size(), token);   \
                                  thread_pool.submit(lambdas, innertoken);                            \
                              },                                                                      \
                              [&](Lambda item, Token* token) {                                        \
                                  thread_pool.submit(item, token);                                    \
                              },                                                                      \
                              [](Lambda) {},                                                          \
                              [](Lambda, uint64, JOBID) {},                                           \
                              [](JOBID) {},                                                           \
                              [](string) {}};

namespace memurai::sql {
class Test_Query_Exec {
public:
    static void hard_delete(IdxTable* table, const vector<RID>& rids) {
        table->hard_delete(rids);
    }
};
} // namespace memurai::sql

namespace {
Database* database{nullptr};

const string tmp_folder = "temp_folder";

Database::Config test_config{Database::default_config.num_max_tables,
                             Database::default_config.idxtable_metadata_prefix,
                             Database::default_config.idxtable_metadata_suffix,
                             Database::default_config.master_tid_path,
                             tmp_folder,
                             Database::default_config.folder_separator};

#define EXPECT_NO_RESULT(QUERY) EXPECT_NO_RESULT_IMPL(QUERY, __LINE__)

void EXPECT_NO_RESULT_IMPL(string query, int linenum) {
    Query2DS query_obj(database, query);

    uint64 start_num_buffers = Buffer::get_num_buffers();

    // run query
    QResult ret = query_obj.execute();

    TEST_VALUE(start_num_buffers, Buffer::get_num_buffers());

    // verify no error
    TEST_VALUE_IMPL(ErrorCode::NoError, EXTRACT_ERROR(ret), linenum);

    TEST_VALUE_IMPL(0, query_obj.get_data().size(), linenum);
}

void TEST_ROW_RESULT(int rownum, vector<optional<string>> expected, const vector<vector<optional<string>>>& gotten, int linenum) {
    TEST_VALUE_IMPL(1, gotten.size(), linenum);

    if (!gotten.empty()) {
        TEST_VALUE_IMPL(expected.size(), gotten[rownum].size(), linenum);
        if (gotten[rownum].size() == expected.size()) {

            for (int idx = 0; idx < gotten[rownum].size(); idx++) {
                TEST_VALUE_IMPL(expected[idx].has_value(), gotten[rownum][idx].has_value(), linenum);
                if (expected[idx].has_value()) {
                    TEST_VALUE_IMPL(*expected[idx], *gotten[rownum][idx], linenum);
                }
            }
        }
    }
}

void EXPECT_QUERY_GOOD_1_ROW(string query, vector<optional<string>> expected, int linenum) {
    Query2DS query_obj(database, query);

    auto start_num_buffers = Buffer::get_num_buffers();

    // run query
    QResult ret = query_obj.execute();

    TEST_VALUE(start_num_buffers, Buffer::get_num_buffers());

    // verify no error
    TEST_VALUE(ErrorCode::NoError, EXTRACT_ERROR(ret));
    // Expect one result
    TEST_ROW_RESULT(0, expected, query_obj.get_data(), linenum);
}

void EXPECT_QUERY_GOOD_1_ROW_WITH_PARAMETERS(string query, vecstring others, vector<optional<string>> expected, int linenum) {
    PublisherV2DStrings publisher;
    // fullname, dob, avgScore, someint
    // run query
    QResult ret = database->parse_and_run_query(&publisher, query.c_str(), others);

    // verify results
    TEST_VALUE(ErrorCode::NoError, EXTRACT_ERROR(ret));
    // Expect one result
    TEST_ROW_RESULT(0, expected, publisher.get_data(), __LINE__);
}

void EXPECT_QUERY_NUM_OF_ROWS(string query_string, int expected) {
    Query2DS query_obj(database, query_string);
    QResult ret = query_obj.execute();
    TEST_VALUE(expected, query_obj.get_data().size());
}

void test_select_simple() {
    TEST_START;

    try {
        IdxTable* t_students = nullptr;
        vector<CID> students_cids;

        // Initial test preparation
        //
        auto actual = database->getActualInterface();

        const vector<QualifiedColumn> columns{
            {"fullname", ColumnType::TEXT}, {"dob", ColumnType::DATE}, {"avgscore", ColumnType::FLOAT}, {"someint", ColumnType::BIGINT}, {"tstmp", ColumnType::TIMESTAMP}};
        t_students = database->createTable("students", "", columns, "students:", IdxTable::default_config);
        SCOPE_EXIT {
            database->dropTable("students", "");
            auto ret = database->getTable("students", "");
            TEST_VALUE(false, ret.has_value());
        };

        students_cids = t_students->getCIDs();

        uint64 add_idx = 1;
#define KEY_NAME to_string(add_idx++)

        {
            // insert some data
            string fullname{"john doe"};
            UINT64 stok = t_students->tokenizeString(fullname).getFullToken();
            optional<UINT64> dob = IdxTable::serialize_date("2007-05-11");
            UINT64 score = IdxTable::serializeDouble(9.9);
            optional<UINT64> tstmp = IdxTable::serialize_timestamp("2021-06-13T14:34:27");
            t_students->upinsertKey(KEY_NAME, students_cids, vector<UINT64>{stok, *dob, score, 12345, *tstmp});

            // Create a Publisher that dumps data into a vector<vector<string>>
            PublisherV2DStrings publisher;

            // run query
            QResult ret = database->parse_and_run_query(&publisher, "SELECT fullname, someint FROM students WHERE dob>'2005-01-01' and avgscore>5.1");

            // verify results
            TEST_VALUE(ErrorCode::NoError, EXTRACT_ERROR(ret));
            // Expect one result
            TEST_ROW_RESULT(0, {"john doe", "12345"}, publisher.get_data(), __LINE__);
        }

        {
            // we do not insert additional data
            EXPECT_QUERY_GOOD_1_ROW(
                "SELECT fullname, dob, avgscore, someint FROM students WHERE dob=='2007-05-11'", {"john doe", "2007-05-11", "9.9", "12345"}, __LINE__);
        }

        // --------------------------------------------------------------------------------------------
        // Unit tests about DATES
        // small table
        // --------------------------------------------------------------------------------------------
        {
            // insert some data
            t_students->upinsertKey(KEY_NAME,
                                    students_cids,
                                    vector<UINT64>{t_students->tokenizeString("Max Plank").getFullToken(),
                                                   (uint64)*IdxTable::serialize_date("2000-02-28"),
                                                   (uint64)IdxTable::serializeDouble(2.2),
                                                   123456,
                                                   (uint64)*IdxTable::serialize_timestamp("2001-03-27T12:45:12")});
            auto res_short = vector<optional<string>>{"Max Plank", "2000-02-28", "2.2", "123456"};

            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM Students WHERE dob=='2000-2-28'", res_short, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM Students WHERE dob=='2000-02-28'", res_short, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM studentS WHERE dob=='2000-Feb-28'", res_short, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM stUdents WHERE dob<='2000-Feb-28'", res_short, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM sTudents WHERE dob<'2000-Feb-29'", res_short, __LINE__);

            EXPECT_NO_RESULT("SELECT fullname, dob, avgscore, someint FROM students WHERE dob=='2000-Feb-29'");
            EXPECT_QUERY_GOOD_1_ROW(
                "SELECT fullname, dob, avgscore, someint FROM students WHERE dob>'2000-Feb-29'", {"john doe", "2007-05-11", "9.9", "12345"}, __LINE__);

            EXPECT_NO_RESULT("SELECT fullname, dob, avgscore, someint FROM students WHERE dob>'2000-Feb-29' AND dob<'2002-1-1'");
        }

        // --------------------------------------------------------------------------------------------
        // Unit tests about DATES
        // small table
        // --------------------------------------------------------------------------------------------
        {
            // insert some data
            t_students->upinsertKey(KEY_NAME,
                                    students_cids,
                                    vector<UINT64>{t_students->tokenizeString("DDDDD").getFullToken(),
                                                   (uint64)*IdxTable::serialize_date("1999-02-03"),
                                                   (uint64)IdxTable::serializeDouble(1.01),
                                                   1234568,
                                                   (uint64)*IdxTable::serialize_timestamp("2001-03-27T12:45:12")});
            vector<optional<string>> short_res{"DDDDD", "1999-02-03", "1.01", "1234568"};

            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE dob=='1999-Feb-03'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE dob=='1999-Feb-3'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE dob=='1999-02-03'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE dob=='1999-2-03'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE dob=='1999-2-3'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE dob=='1999-02-3'", short_res, __LINE__);
            EXPECT_NO_RESULT("SELECT dob, avgscore FROM students WHERE dob=='1999-02-3' AND dob<>'1999-02-3'");
        }

        // --------------------------------------------------------------------------------------------
        // Unit tests about TEXT - Short String size 1
        // small table
        // --------------------------------------------------------------------------------------------
        {
            // insert some data
            t_students->upinsertKey("4",
                                    students_cids,
                                    vector<UINT64>{t_students->tokenizeString("Z").getFullToken(),
                                                   (uint64)*IdxTable::serialize_date("1999-02-03"),
                                                   (uint64)IdxTable::serializeDouble(1.01),
                                                   1234568,
                                                   (uint64)*IdxTable::serialize_timestamp("2001-03-27T12:45:12")});
            vector<optional<string>> short_res{"Z", "1999-02-03", "1.01", "1234568"};

            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Z%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%Z'", short_res, __LINE__);
        }

        // --------------------------------------------------------------------------------------------
        // Unit tests about TEXT - Short String size 2
        // small table
        // --------------------------------------------------------------------------------------------
        {
            // insert some data
            t_students->upinsertKey(KEY_NAME,
                                    students_cids,
                                    vector<UINT64>{t_students->tokenizeString("Xy").getFullToken(),
                                                   (uint64)*IdxTable::serialize_date("1999-02-03"),
                                                   (uint64)IdxTable::serializeDouble(1.01),
                                                   1234568,
                                                   (uint64)*IdxTable::serialize_timestamp("2001-03-27T12:45:12")});
            vector<optional<string>> short_res{"Xy", "1999-02-03", "1.01", "1234568"};

            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'X%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%Xy'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Xy%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%y'", short_res, __LINE__);
        }

        // --------------------------------------------------------------------------------------------
        // Unit tests about TEXT - Short String size 3
        // small table
        // --------------------------------------------------------------------------------------------
        {
            // insert some data
            t_students->upinsertKey(KEY_NAME,
                                    students_cids,
                                    vector<UINT64>{t_students->tokenizeString("Alc").getFullToken(),
                                                   (uint64)*IdxTable::serialize_date("1999-02-03"),
                                                   (uint64)IdxTable::serializeDouble(1.01),
                                                   1234568,
                                                   (uint64)*IdxTable::serialize_timestamp("2001-03-27T12:45:12")});
            vector<optional<string>> short_res{"Alc", "1999-02-03", "1.01", "1234568"};

            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'A%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Al%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Alc%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%Alc'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%lc'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%c'", short_res, __LINE__);
        }

        // --------------------------------------------------------------------------------------------
        // Unit tests about TEXT - Short String size 4
        // small table
        // --------------------------------------------------------------------------------------------
        {
            // insert some data
            t_students->upinsertKey(KEY_NAME,
                                    students_cids,
                                    vector<UINT64>{t_students->tokenizeString("John").getFullToken(),
                                                   (uint64)*IdxTable::serialize_date("1999-02-03"),
                                                   (uint64)IdxTable::serializeDouble(1.01),
                                                   1234568,
                                                   (uint64)*IdxTable::serialize_timestamp("2001-03-27T12:45:12")});
            vector<optional<string>> short_res{"John", "1999-02-03", "1.01", "1234568"};

            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'J%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Jo%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Joh%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'John%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%John'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%ohn'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%hn'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%n'", short_res, __LINE__);
        }

        // --------------------------------------------------------------------------------------------
        // Unit tests about TEXT - Short String size 5
        // small table
        // --------------------------------------------------------------------------------------------
        {
            // insert some data
            t_students->upinsertKey(KEY_NAME,
                                    students_cids,
                                    vector<UINT64>{t_students->tokenizeString("Shorq").getFullToken(),
                                                   (uint64)*IdxTable::serialize_date("1999-02-03"),
                                                   (uint64)IdxTable::serializeDouble(1.01),
                                                   1234568,
                                                   (uint64)*IdxTable::serialize_timestamp("2001-03-27T12:45:12")});
            vector<optional<string>> short_res{"Shorq", "1999-02-03", "1.01", "1234568"};

            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'S%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Sh%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Sho%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Shor%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Shorq%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%Shorq'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%horq'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%orq'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%rq'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%q'", short_res, __LINE__);
        }

        // --------------------------------------------------------------------------------------------
        // Unit tests about TEXT - Short String size 6
        // small table
        // --------------------------------------------------------------------------------------------
        {
            // insert some data
            t_students->upinsertKey(KEY_NAME,
                                    students_cids,
                                    vector<UINT64>{t_students->tokenizeString("Endrew").getFullToken(),
                                                   (uint64)*IdxTable::serialize_date("1999-02-03"),
                                                   (uint64)IdxTable::serializeDouble(1.01),
                                                   1234568,
                                                   (uint64)*IdxTable::serialize_timestamp("2001-03-27T12:45:12")});
            vector<optional<string>> short_res{"Endrew", "1999-02-03", "1.01", "1234568"};

            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'E%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'En%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'End%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Endr%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Endre%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Endrew%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%Endrew'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%ndrew'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%drew'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%rew'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%ew'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%w'", short_res, __LINE__);
        }

        // --------------------------------------------------------------------------------------------
        // Unit tests about TEXT - Short String size 7
        // small table
        // --------------------------------------------------------------------------------------------
        {
            // insert some data
            t_students->upinsertKey(KEY_NAME,
                                    students_cids,
                                    vector<UINT64>{t_students->tokenizeString("Taniele").getFullToken(),
                                                   (uint64)*IdxTable::serialize_date("1999-02-03"),
                                                   (uint64)IdxTable::serializeDouble(1.01),
                                                   1234568,
                                                   (uint64)*IdxTable::serialize_timestamp("2001-03-27T12:45:12")});
            vector<optional<string>> short_res{"Taniele", "1999-02-03", "1.01", "1234568"};

            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'T%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Ta%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Tan%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Tani%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Tanie%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Taniel%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Taniele%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%Taniele'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%aniele'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%niele'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%iele'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%ele'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%le'", short_res, __LINE__);
        }

        // --------------------------------------------------------------------------------------------
        // Unit tests about TEXT - (NOT Short) String size 7
        // small table
        // --------------------------------------------------------------------------------------------
        {
            // insert some data
            t_students->upinsertKey(KEY_NAME,
                                    students_cids,
                                    vector<UINT64>{t_students->tokenizeString("Benedict").getFullToken(),
                                                   (uint64)*IdxTable::serialize_date("1999-02-03"),
                                                   (uint64)IdxTable::serializeDouble(1.01),
                                                   1234568,
                                                   (uint64)*IdxTable::serialize_timestamp("2001-03-27T12:45:12")});
            vector<optional<string>> short_res{"Benedict", "1999-02-03", "1.01", "1234568"};

            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'B%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Be%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Ben%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Bene%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Bened%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Benedi%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Benedic%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE 'Benedict%'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%Benedict'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%enedict'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%nedict'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%edict'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%dict'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%ict'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%ct'", short_res, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE fullname LIKE '%t'", short_res, __LINE__);
        }

        // --------------------------------------------------------------------------------------------
        // Unit tests about TIMESTAMP
        // small table
        // --------------------------------------------------------------------------------------------
        {
            // insert some data
            t_students->upinsertKey(KEY_NAME,
                                    students_cids,
                                    vector<UINT64>{t_students->tokenizeString("Max Plank").getFullToken(),
                                                   (uint64)*IdxTable::serialize_date("2000-02-28"),
                                                   (uint64)IdxTable::serializeDouble(2.2),
                                                   123456,
                                                   (uint64)*IdxTable::serialize_timestamp("2001-03-07T12:45:12")});
            auto res_short = vector<optional<string>>{"Max Plank", "2000-02-28", "2.2", "123456", "2001-03-07T12:45:12"};

            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint, tstmp FROM Students WHERE tstmp=='2001-3-07T12:45:12'", res_short, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint, tstmp FROM Students WHERE tstmp=='2001-03-07T12:45:12'", res_short, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint, tstmp FROM Students WHERE tstmp=='2001-03-7T12:45:12'", res_short, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint, tstmp FROM Students WHERE tstmp=='2001-3-7T12:45:12'", res_short, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint, tstmp FROM Students WHERE tstmp<='2001-03-07T12:45:12'", res_short, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint, tstmp FROM Students WHERE tstmp<'2001-03-08T12:45:12'", res_short, __LINE__);

            EXPECT_NO_RESULT("SELECT fullname, dob, avgscore, someint, tstmp FROM students WHERE tstmp=='2001-03-08T12:45:12'");
            EXPECT_QUERY_GOOD_1_ROW(
                    "SELECT fullname, dob, avgscore, someint, tstmp FROM students WHERE tstmp>'2021-01-08T12:45:12'", {"john doe", "2007-05-11", "9.9", "12345", "2021-06-13T14:34:27"}, __LINE__);

            EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint, tstmp FROM students WHERE tstmp>'2001-03-06T12:45:12' AND tstmp<'2001-03-08T12:45:12'", res_short, __LINE__);
            EXPECT_NO_RESULT("SELECT fullname, dob, avgscore, someint, tstmp FROM students WHERE tstmp>'2001-03-02T12:45:12' AND tstmp<'2001-03-03T12:45:12'");

            EXPECT_NO_RESULT("SELECT tstmp, avgscore FROM students WHERE tstmp=='2001-03-02T12:45:12' AND tstmp<>'2001-03-02T12:45:12'");
        }

        // --------------------------------------------------------------------------------------------
        // Unit tests about NULLs
        // small table - NULL on integer
        // --------------------------------------------------------------------------------------------
        {
            VecCID some_cids(students_cids);
            some_cids.erase(some_cids.begin() + some_cids.size() - 2);
            // insert some data
            t_students->upinsertKey(KEY_NAME,
                                    some_cids,
                                    vector<UINT64>{t_students->tokenizeString("Benedict").getFullToken(),
                                                   (uint64)*IdxTable::serialize_date("1999-02-03"),
                                                   (uint64)IdxTable::serializeDouble(1.01),
                                                   (uint64)*IdxTable::serialize_timestamp("2001-03-27T12:45:12")});

            {
                vector<optional<string>> short_res{"Benedict", "1999-02-03", "1.01"};
                EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore FROM students WHERE someint IS NULL", short_res, __LINE__);
            }
            {
                vector<optional<string>> short_res{"Benedict", "1999-02-03", "1.01", std::nullopt};
                EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE someint IS NULL", short_res, __LINE__);
            }
        }

        // --------------------------------------------------------------------------------------------
        // Unit tests about NULLs
        // small table - NULL on floats
        // --------------------------------------------------------------------------------------------
        {
            VecCID some_cids(students_cids);
            some_cids.erase(some_cids.begin() + some_cids.size() - 3);

            // insert some data
            t_students->upinsertKey(
                KEY_NAME,
                some_cids,
                vector<UINT64>{t_students->tokenizeString("Benedict").getFullToken(), (uint64)*IdxTable::serialize_date("1999-02-03"), 1234,
                               (uint64)*IdxTable::serialize_timestamp("2001-03-27T12:45:12")});

            {
                vector<optional<string>> short_res{"Benedict", "1999-02-03", "1234"};
                EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, someint FROM students WHERE avgscore IS NULL", short_res, __LINE__);
            }
            {
                vector<optional<string>> short_res{"Benedict", "1999-02-03", std::nullopt};
                EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore FROM students WHERE avgscore IS NULL", short_res, __LINE__);
            }
        }

        // --------------------------------------------------------------------------------------------
        // Unit tests about NULLs
        // small table - NULL on dates
        // --------------------------------------------------------------------------------------------
        {
            VecCID some_cids(students_cids);
            some_cids.erase(some_cids.begin() + 1);

            // insert some data
            t_students->upinsertKey(
                KEY_NAME, some_cids, vector<UINT64>{t_students->tokenizeString("Benedict").getFullToken(), (uint64)IdxTable::serializeDouble(1.01), 1234,
                                                    (uint64)*IdxTable::serialize_timestamp("2001-03-27T12:45:12")});

            {
                vector<optional<string>> short_res{"Benedict", "1.01", "1234"};
                EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, avgscore, someint FROM students WHERE dob IS NULL", short_res, __LINE__);
            }
            {
                vector<optional<string>> short_res{"Benedict", "1.01", std::nullopt};
                EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, avgscore, dob FROM students WHERE dob IS NULL", short_res, __LINE__);
            }
        }

        // --------------------------------------------------------------------------------------------
        // Unit tests about NULLs
        // small table - NULL on TEXT
        // --------------------------------------------------------------------------------------------
        {
            VecCID some_cids(students_cids);
            some_cids.erase(some_cids.begin());

            // insert some data
            t_students->upinsertKey(
                KEY_NAME, some_cids, vector<UINT64>{(uint64)*IdxTable::serialize_date("1999-02-03"), (uint64)IdxTable::serializeDouble(1.01), 1234,
                                                    (uint64)*IdxTable::serialize_timestamp("2001-03-27T12:45:12")});

            {
                vector<optional<string>> short_res{"1999-02-03", "1.01", "1234"};
                EXPECT_QUERY_GOOD_1_ROW("SELECT dob, avgscore, someint FROM students WHERE fullname IS NULL", short_res, __LINE__);
            }
            {
                vector<optional<string>> short_res{std::nullopt, "1999-02-03", "1.01"};
                EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore FROM students WHERE fullname IS NULL", short_res, __LINE__);
            }
        }

        // --------------------------------------------------------------------------------------------
        // Unit tests about NULLs
        // small table - NULL on timestamps
        // --------------------------------------------------------------------------------------------
        {
            VecCID some_cids(students_cids);
            some_cids.pop_back();

            // insert some data
            t_students->upinsertKey(
                    KEY_NAME, some_cids, vector<UINT64>{t_students->tokenizeString("Benedict").getFullToken(), (uint64)*IdxTable::serialize_date("1999-02-03"),
                                                        (uint64)IdxTable::serializeDouble(1.01), 1234});

            {
                vector<optional<string>> short_res{"Benedict", "1999-02-03", "1.01", "1234"};
                EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, someint FROM students WHERE tstmp IS NULL", short_res, __LINE__);
            }
            {
                vector<optional<string>> short_res{"Benedict", "1999-02-03", "1.01", std::nullopt};
                EXPECT_QUERY_GOOD_1_ROW("SELECT fullname, dob, avgscore, tstmp FROM students WHERE tstmp IS NULL", short_res, __LINE__);
            }
        }

        // --------------------------------------------------------------------------------------------
        // Unit tests about query parameters
        // --------------------------------------------------------------------------------------------
        {
            // insert some data
            t_students->upinsertKey(KEY_NAME,
                                    students_cids,
                                    vector<UINT64>{t_students->tokenizeString("name1").getFullToken(),
                                                   (uint64)*IdxTable::serialize_date("1999-02-03"),
                                                   (uint64)IdxTable::serializeDouble(1.01),
                                                   1233,
                                                   (uint64)*IdxTable::serialize_timestamp("2001-03-27T12:45:12")});
            t_students->upinsertKey(KEY_NAME,
                                    students_cids,
                                    vector<UINT64>{t_students->tokenizeString("name2").getFullToken(),
                                                   (uint64)*IdxTable::serialize_date("1999-02-02"),
                                                   (uint64)IdxTable::serializeDouble(1.01),
                                                   1233,
                                                   (uint64)*IdxTable::serialize_timestamp("2001-03-27T12:45:12")});
            t_students->upinsertKey(KEY_NAME,
                                    students_cids,
                                    vector<UINT64>{t_students->tokenizeString("name3").getFullToken(),
                                                   (uint64)*IdxTable::serialize_date("1999-02-01"),
                                                   (uint64)IdxTable::serializeDouble(1.01),
                                                   1233,
                                                   (uint64)*IdxTable::serialize_timestamp("2001-03-27T12:45:12")});

            EXPECT_QUERY_GOOD_1_ROW_WITH_PARAMETERS("SELECT fullname, dob FROM students WHERE fullname = ?", {"name1"}, {"name1", "1999-02-03"}, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW_WITH_PARAMETERS(
                "SELECT fullname, dob FROM students WHERE dob > ? and someint = 1233", {"1999-02-02"}, {"name1", "1999-02-03"}, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW_WITH_PARAMETERS(
                "SELECT fullname, dob FROM students WHERE dob > ? and someint = ?", {"1999-02-02", "1233"}, {"name1", "1999-02-03"}, __LINE__);
            EXPECT_QUERY_GOOD_1_ROW_WITH_PARAMETERS("SELECT fullname, dob FROM students WHERE fullname = ? and avgscore > ? and dob = ? and someint = ?",
                                                    {"name1", "0.001", "1999-02-03", "1233"},
                                                    {"name1", "1999-02-03"},
                                                    __LINE__);
        }

    } catch (...) {
        TEST_VALUE(1, 0);
    }
}

void test_select_big(uint32 num_threads) {
    TEST_START;

    try {
        IdxTable* t_employees = nullptr;
        vector<CID> employees_cids;

        // Initial test preparation
        //
        auto actual = database->getActualInterface();
        const vector<QualifiedColumn> columns{{"firstname", ColumnType::TEXT},
                                              {"lastname", ColumnType::TEXT},
                                              {"id", ColumnType::BIGINT},
                                              {"dob", ColumnType::DATE},
                                              {"employeedate", ColumnType::DATE},
                                              {"department", ColumnType::TAG},
                                              {"manager", ColumnType::TAG},
                                              {"height", ColumnType::FLOAT},
                                              {"weight", ColumnType::FLOAT},
                                              {"preferredlang", ColumnType::TAG},
                                              {"numkids", ColumnType::BIGINT}};

        t_employees = database->createTable("employees", "", columns, "employee:", IdxTable::default_config);
        SCOPE_EXIT {
            database->dropTable("employees", "");
            auto ret = database->getTable("employees", "");
            TEST_VALUE(false, ret.has_value());
        };

        employees_cids = t_employees->getCIDs();

        std::barrier start_barrier(num_threads + 1);
        const uint64 ITEMS_PER_THREAD = 1000;

        auto writer = [&](uint64 num_items, uint64 threadIdx) {
            // wait for all threads to be set up
            start_barrier.arrive_and_wait();

            //  adding many rows with random data
            for (uint64 i = 0; i < num_items; i++) {
                string key = to_string(threadIdx) + "_" + to_string(i);
                t_employees->upinsertKey(
                    key,
                    employees_cids,
                    vector<UINT64>{uint64(rand()), uint64(rand()), uint64(rand()), i, 12, uint64(rand()), uint64(rand()), uint64(rand()), i, 12, i + 1});
            }
        };

        vector<thread> t_threads;

        for (uint32 thread = 0; thread < num_threads; thread++) {
            t_threads.emplace_back(writer, ITEMS_PER_THREAD, thread);
        }

        // let all threads go!
        start_barrier.arrive_and_wait();

        for (uint32 idx = 0; idx < num_threads; idx++) {
            t_threads[idx].join();
        }

        {
            // insert some data
            string fullname{"john doe"};
            UINT64 stok = t_employees->tokenizeString(fullname).getFullToken();
            optional<UINT64> dob = IdxTable::serialize_date("2007-05-11");
            UINT64 height = IdxTable::serializeDouble(1.85);
            t_employees->upinsertKey(
                "1", {employees_cids[0], employees_cids[3], employees_cids[7], employees_cids[10]}, vector<UINT64>{stok, *dob, height, 12345});

            {
                // Create a Publisher that dumps data into a vector<vector<string>>
                PublisherCount publisher;
                // run query
                QResult ret = database->parse_and_run_query(&publisher, "SELECT firstname, numkids FROM employees WHERE dob>'2005-01-01' and height>1.70");
                // verify results
                // Expect one result
                TEST_VALUE(ErrorCode::NoError, EXTRACT_ERROR(ret));
                TEST_VALUE(1, publisher.get_count());
            }

            {
                Query2DS query_obj_base(database, "SELECT numkids FROM employees ORDER BY numkids desc");
                // run baseline query
                QResult ret_base = query_obj_base.execute();

                // verify no error
                TEST_VALUE(ErrorCode::NoError, EXTRACT_ERROR(ret_base));
                // Verify at least 10 rows
                TEST_VALUE(true, query_obj_base.get_data().size() >= 10);

                // TEST LIMIT
                {
                    Query2DS query_obj(database, "SELECT numkids FROM employees ORDER BY numkids desc LIMIT 2");
                    // run baseline query
                    QResult ret = query_obj.execute();
                    TEST_VALUE(2, query_obj.get_data().size());
                    TEST_VALUE(true, query_obj_base.get_data()[0][0] == query_obj.get_data()[0][0]);
                    TEST_VALUE(true, query_obj_base.get_data()[1][0] == query_obj.get_data()[1][0]);
                }

                // TEST OFFSET
                {
                    Query2DS query_obj(database, "SELECT numkids FROM employees ORDER BY numkids desc OFFSET 1");
                    // run baseline query
                    QResult ret = query_obj.execute();
                    TEST_VALUE(query_obj_base.get_data().size() - 1, query_obj.get_data().size());
                    TEST_VALUE(true, query_obj_base.get_data()[1][0] == query_obj.get_data()[0][0]);
                    TEST_VALUE(true, query_obj_base.get_data()[2][0] == query_obj.get_data()[1][0]);
                }

                // TEST LIMIT+OFFSET
                {
                    Query2DS query_obj(database, "SELECT numkids FROM employees ORDER BY numkids desc LIMIT 3 OFFSET 1");
                    // run baseline query
                    QResult ret = query_obj.execute();
                    TEST_VALUE(3, query_obj.get_data().size());
                    TEST_VALUE(true, query_obj_base.get_data()[1][0] == query_obj.get_data()[0][0]);
                    TEST_VALUE(true, query_obj_base.get_data()[2][0] == query_obj.get_data()[1][0]);
                    TEST_VALUE(true, query_obj_base.get_data()[3][0] == query_obj.get_data()[2][0]);
                }
            }
        }
    } catch (...) {
        TEST_VALUE(1, 0);
    }
}

void test_multiple_where_clauses(int limit) {
    for (int i = 0; i < limit; ++i) {
        string firstname_condition = "firstname='first_" + string(1, 'a' + i) + "'";
        string lastname_condition = "lastname='last_" + string(1, 'a' + i) + "'";
        string dob_condition = "dob<='200" + string(1, '0' + i) + "-05-11" + "'";
        if (i >= 10) {
            dob_condition = "dob<='20" + to_string(i) + "-05-11" + "'";
        }
        string height_condition = "height<" + to_string(1.051 + (i * 0.10));
        string weight_condition = "weight<" + to_string(100.051 + (i * 0.10));
        string numkids_condition = "numkids<=" + to_string(i);
        string query_args[] = {dob_condition, height_condition, weight_condition, numkids_condition, firstname_condition, lastname_condition};
        string query_string = "SELECT numkids FROM employees WHERE";
        for (int j = 0; j < 6; ++j) {
            if (j > 0) {
                query_string += " and";
            }
            query_string += " " + query_args[j];
            Query2DS query_obj(database, query_string);
            QResult ret = query_obj.execute();
            auto tmp = query_obj.get_data();
            int expected = i + 1;
            if (j >= 4)
                expected = 1;
            TEST_VALUE(expected, query_obj.get_data().size());
        }
    }
}

void test_select_simple2() {
    TEST_START;

    try {
        IdxTable* t_employees = nullptr;
        vector<CID> employees_cids;

        // Initial test preparation
        //
        auto actual = database->getActualInterface();
        const vector<QualifiedColumn> columns{{"firstname", ColumnType::TEXT},
                                              {"lastname", ColumnType::TEXT},
                                              {"id", ColumnType::BIGINT},
                                              {"dob", ColumnType::DATE},
                                              {"employeedate", ColumnType::DATE},
                                              {"department", ColumnType::TAG},
                                              {"manager", ColumnType::TAG},
                                              {"height", ColumnType::FLOAT},
                                              {"weight", ColumnType::FLOAT},
                                              {"preferredlang", ColumnType::TAG},
                                              {"numkids", ColumnType::BIGINT}};
        // with this config, first sort will be on the 10th upinsertKey operation
        t_employees = database->createTable("employees", "", columns, "employee:", {100, 1000, IdxTable::gstrat, IdxTable::sstrat});
        SCOPE_EXIT {
            database->dropTable("employees", "");
            auto ret = database->getTable("employees", "");
            TEST_VALUE(false, ret.has_value());
        };

        employees_cids = t_employees->getCIDs();

        {
            // insert 9 rows, so no sort operations yet
            for (int i = 0; i < 9; ++i) {
                UINT64 firstname = t_employees->tokenizeString("first_" + string(1, 'a' + i)).getFullToken();
                UINT64 lastname = t_employees->tokenizeString("last_" + string(1, 'a' + i)).getFullToken();
                optional<UINT64> dob = IdxTable::serialize_date(("200" + string(1, '0' + i) + "-05-11").c_str());
                UINT64 height = IdxTable::serializeDouble(1.05 + (i * 0.10));
                UINT64 weight = IdxTable::serializeDouble(100.05 + (i * 0.10));
                UINT64 numkids = i;

                t_employees->upinsertKey(to_string(i),
                                         {employees_cids[0], employees_cids[1], employees_cids[3], employees_cids[7], employees_cids[8], employees_cids[10]},
                                         vector<UINT64>{firstname, lastname, *dob, height, weight, numkids});
            }

            TEST_VALUE(t_employees->get_c_sort_events(), 0);

            // TEST NO SORTED
            // table contains 9 rows
            test_multiple_where_clauses(9);

            // insert 10th row
            {
                uint64 firstname = t_employees->tokenizeString("first_" + string(1, 'a' + 9)).getFullToken();
                uint64 lastname = t_employees->tokenizeString("last_" + string(1, 'a' + 9)).getFullToken();
                optional<uint64> dob = IdxTable::serialize_date(("200" + string(1, '0' + 9) + "-05-11").c_str());
                uint64 height = IdxTable::serializeDouble(1.05 + (9 * 0.10));
                uint64 weight = IdxTable::serializeDouble(100.05 + (9 * 0.10));
                uint64 numkids = 9;

                t_employees->upinsertKey("xx",
                                         {employees_cids[0], employees_cids[1], employees_cids[3], employees_cids[7], employees_cids[8], employees_cids[10]},
                                         vector<UINT64>{firstname, lastname, *dob, height, weight, numkids});
            }

            TEST_VALUE(t_employees->get_c_sort_events(), 1);

            // TEST SORTED
            // table contains 10 rows
            test_multiple_where_clauses(10);

            // TEST eq & neq
            {
                EXPECT_QUERY_GOOD_1_ROW("SELECT numkids FROM employees WHERE numkids = 3", {"3"}, __LINE__);
                EXPECT_QUERY_GOOD_1_ROW("SELECT numkids FROM employees WHERE numkids = 6", {"6"}, __LINE__);
                EXPECT_QUERY_GOOD_1_ROW("SELECT numkids FROM employees WHERE numkids = 4", {"4"}, __LINE__);
                EXPECT_QUERY_GOOD_1_ROW("SELECT numkids FROM employees WHERE numkids = 9", {"9"}, __LINE__);

                EXPECT_QUERY_NUM_OF_ROWS("SELECT numkids FROM employees WHERE numkids != 3 and numkids != 5", 8);
                EXPECT_QUERY_NUM_OF_ROWS("SELECT numkids FROM employees WHERE numkids != 0", 9);
            }

            // insert 5 more rows
            {
                for (int i = 10; i < 15; ++i) {
                    UINT64 firstname = t_employees->tokenizeString("first_" + string(1, 'a' + i)).getFullToken();
                    UINT64 lastname = t_employees->tokenizeString("last_" + string(1, 'a' + i)).getFullToken();
                    optional<UINT64> dob = IdxTable::serialize_date(("20" + to_string(i) + "-05-11").c_str());
                    UINT64 height = IdxTable::serializeDouble(1.05 + (i * 0.10));
                    UINT64 weight = IdxTable::serializeDouble(100.05 + (i * 0.10));
                    UINT64 numkids = i;

                    t_employees->upinsertKey(
                        to_string(i * 10),
                        {employees_cids[0], employees_cids[1], employees_cids[3], employees_cids[7], employees_cids[8], employees_cids[10]},
                        vector<UINT64>{firstname, lastname, *dob, height, weight, numkids});
                }
            }

            // TEST SOME SORTED AND SOME UNSORTED
            // table contains 15 rows
            test_multiple_where_clauses(15);
        }
    } catch (...) {
        TEST_VALUE(1, 0);
    }
}

void test_concurrent_selects(uint64 num_threads) {
    TEST_START;

    try {
        IdxTable* t_students = nullptr;
        vector<CID> students_cids;
        cuint64 num_rows_to_add = 10'000;

        // Initial test preparation
        //
        auto actual = database->getActualInterface();
        const vector<QualifiedColumn> columns{
            {"fullname", ColumnType::TEXT}, {"dob", ColumnType::DATE}, {"avgscore", ColumnType::FLOAT}, {"someint", ColumnType::BIGINT}};
        t_students = database->createTable("students", "", columns, "students:", IdxTable::default_config);
        SCOPE_EXIT {
            database->dropTable("students", "");
            auto ret = database->getTable("students", "");
            TEST_VALUE(false, ret.has_value());
        };

        students_cids = t_students->getCIDs();
        {
            // insert lots of data
            string fullname{"john doe"};
            UINT64 stok = t_students->tokenizeString(fullname).getFullToken();
            optional<UINT64> dob = IdxTable::serialize_date("2007-05-11");
            UINT64 score = IdxTable::serializeDouble(9.9);

            t_students->upinsertKey("x", students_cids, vector<UINT64>{stok, *dob, score, 12345});

            //  adding many rows with random data
            for (uint64 i = 0; i < num_rows_to_add; i++) {
                t_students->upinsertKey(to_string(i), students_cids, vector<UINT64>{uint64(rand()), uint64(rand()), uint64(rand()), i});
            }

            LOG_INFO("Table has now " << t_students->getNumRowsApprox() << " rows.");

            const uint64 QUERIES_PER_THREAD = 100;

            std::barrier start_barrier(num_threads + 1);

            atomic<uint64> s_done{0};

            // createa threads, each running a long query

            auto lambda = [&](uint64 num_queries) {
                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                for (int i = 0; i < num_queries; i++) {
                    // Create a Publisher that dumps data into a vector<vector<string>>
                    PublisherV2DStrings publisher;

                    // run query
                    QResult ret = database->parse_and_run_query(&publisher, "SELECT fullname, someint FROM students WHERE dob>'2005-01-01' and avgscore>5.1");
                }

                s_done++;
            };

            vector<thread> t_threads;

            for (uint32 thread = 0; thread < num_threads; thread++) {
                t_threads.emplace_back(lambda, QUERIES_PER_THREAD);
            }

            start_barrier.arrive_and_wait();

            // now verify that queries are running concurrently
            bool success = false;
            for (uint64 times = 0; times < 1000000000; times++) {
                auto metrics = t_students->getMetrics();

                auto active_computes = metrics["active computes"];

                // if we have observe concurrency, test is done.
                if (active_computes > 1) {
                    success = true;
                    break;
                }
            }

            TEST_VALUE(true, success);

            for (uint32 thread = 0; thread < num_threads; thread++) {
                t_threads[thread].join();
            }
        }
    } catch (...) {
        TEST_VALUE(1, 0);
    }
}

void test_concurrent_selects_and_adds(uint64 num_threads) {
    TEST_START;

    try {
        IdxTable* t_students = nullptr;
        vector<CID> students_cids;
        cuint64 num_rows_to_add = 10'000;

        // Initial test preparation
        //
        auto actual = database->getActualInterface();
        const vector<QualifiedColumn> columns{
            {"fullname", ColumnType::TEXT}, {"dob", ColumnType::DATE}, {"avgscore", ColumnType::FLOAT}, {"someint", ColumnType::BIGINT}};
        t_students = database->createTable("students", "", columns, "students:", IdxTable::default_config);
        SCOPE_EXIT {
            database->dropTable("students", "");
            auto ret = database->getTable("students", "");
            TEST_VALUE(false, ret.has_value());
        };

        students_cids = t_students->getCIDs();
        {
            // insert lots of data
            string fullname{"john doe"};
            UINT64 stok = t_students->tokenizeString(fullname).getFullToken();
            optional<UINT64> dob = IdxTable::serialize_date("2007-05-11");
            UINT64 score = IdxTable::serializeDouble(9.9);

            t_students->upinsertKey("0", students_cids, vector<UINT64>{stok, *dob, score, 12345});

            //  adding many rows with random data
            for (uint64 i = 0; i < num_rows_to_add; i++) {
                t_students->upinsertKey(to_string(i), students_cids, vector<UINT64>{uint64(rand()), uint64(rand()), uint64(rand()), 12345 + i});
            }

            const uint64 QUERIES_PER_THREAD = 100;

            std::barrier start_barrier(num_threads + 1);

            atomic<uint64> s_stopping{false};

            // createa threads, each running a long query

            auto lambda_select = [&](uint64 num_queries) {
                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                for (int i = 0; i < num_queries; i++) {
                    if (s_stopping.load(std::memory_order_acquire))
                        return;

                    // Create a Publisher that dumps data into a vector<vector<string>>
                    PublisherV2DStrings publisher;

                    // run query
                    QResult ret = database->parse_and_run_query(&publisher, "SELECT fullname, someint FROM students WHERE dob>'2005-01-01' and avgscore>5.1");
                }
            };

            auto lambda_add = [&](uint64 num_rows_to_add, uint64 threadIdx) {
                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                for (uint64 i = 0; i < num_rows_to_add; i++) {
                    if (s_stopping.load(std::memory_order_acquire))
                        return;

                    string key = to_string(threadIdx) + "_" + to_string(i);
                    t_students->upinsertKey(key, students_cids, vector<UINT64>{uint64(rand()), uint64(rand()), uint64(rand()), i});
                }
            };

            vector<thread> t_threads;

            t_threads.emplace_back(lambda_add, 1'000'000, num_threads);

            for (uint64 thread = 0; thread < num_threads - 1; thread++) {
                t_threads.emplace_back(lambda_select, QUERIES_PER_THREAD);
            }

            start_barrier.arrive_and_wait();

            // now verify that queries are running concurrently
            bool success = false;
            uint64 last_addrow_cnt = t_students->getNumRowsApprox();

            for (uint64 times = 0; times < 1000000000; times++) {
                auto metrics = t_students->getMetrics();

                auto active_computes = metrics["active computes"];
                auto addrow_cnt = metrics["addrow cnt"];

                if ((addrow_cnt > last_addrow_cnt) && (active_computes > 0)) {
                    success = true;
                    s_stopping.store(true, std::memory_order_release);
                    break;
                }

                last_addrow_cnt = addrow_cnt;
            }

            TEST_VALUE(true, success);

            for (uint32 thread = 0; thread < num_threads; thread++) {
                t_threads[thread].join();
            }
        }

        TEST_VALUE(0, Buffer::get_num_buffers());
    } catch (...) {
        TEST_VALUE(1, 0);
    }
}

class DebugPublisherCount : public PublisherCount {
    function<void()> lambda;

public:
    DebugPublisherCount(function<void()> lambda) : lambda(lambda) {}

    void query_started() final {
        lambda();
    }
};

void test_concurrent_selects_and_adds2() {
    TEST_START;

    try {
        IdxTable* t_students = nullptr;
        vector<CID> students_cids;
        cuint64 num_rows_to_add = 10'000;

        // Initial test preparation
        //
        auto actual = database->getActualInterface();
        const vector<QualifiedColumn> columns{
            {"fullname", ColumnType::TEXT}, {"dob", ColumnType::DATE}, {"avgscore", ColumnType::FLOAT}, {"someint", ColumnType::BIGINT}};
        t_students = database->createTable("students", "", columns, "students:", IdxTable::default_config);
        SCOPE_EXIT {
            database->dropTable("students", "");
            auto ret = database->getTable("students", "");
            TEST_VALUE(false, ret.has_value());
        };

        students_cids = t_students->getCIDs();
        {
            // insert lots of data
            string fullname{"john doe"};
            UINT64 stok = t_students->tokenizeString(fullname).getFullToken();
            optional<UINT64> dob = IdxTable::serialize_date("2007-05-11");
            UINT64 score = IdxTable::serializeDouble(9.9);

            t_students->upinsertKey("x", students_cids, vector<UINT64>{stok, *dob, score, 12345});

            //  adding many rows with random data
            for (uint64 i = 0; i < num_rows_to_add; i++) {
                t_students->upinsertKey(to_string(i), students_cids, vector<UINT64>{uint64(rand()), uint64(rand()), uint64(rand()), 12345 + 1 + i});
            }

            LOG_INFO("Table has now " << t_students->getNumRowsApprox() << " rows.");

            {
                PublisherCount publisher;
                database->parse_and_run_query(&publisher, "SELECT fullname, someint FROM students WHERE someint=12345");
                TEST_VALUE(1, publisher.get_count());
            }

            std::barrier barrier1(2 + 1);
            std::barrier barrier2(2);
            atomic<uint64> s_stopping{false};

            auto lambda_select = [&]() {
                // Create a Publisher that dumps data into a vector<vector<string>>
                DebugPublisherCount dpublisher([&]() {
                    barrier1.arrive_and_wait();
                    barrier2.arrive_and_wait();
                });
                // run query
                database->parse_and_run_query(&dpublisher, "SELECT fullname, someint FROM students WHERE someint=12345");
                TEST_VALUE(1, dpublisher.get_count());

                // RUN again, now expect 2 rows
                PublisherCount publisher;
                database->parse_and_run_query(&publisher, "SELECT fullname, someint FROM students WHERE someint=12345");
                TEST_VALUE(2, publisher.get_count());
            };

            auto lambda_add = [&]() {
                // wait for all threads to be set up
                barrier1.arrive_and_wait();
                t_students->upinsertKey("xX", students_cids, vector<UINT64>{uint64(rand()), uint64(rand()), uint64(rand()), 12345});
                barrier2.arrive_and_wait();
            };

            vector<thread> t_threads;

            t_threads.emplace_back(lambda_add);
            t_threads.emplace_back(lambda_select);

            barrier1.arrive_and_wait();

            t_threads[0].join();
            t_threads[1].join();
        }
    } catch (...) {
        TEST_VALUE(1, 0);
    }
}

void test_delete() {
    TEST_START;

    try {
        IdxTable* t_movies = nullptr;
        vector<CID> movies_cids;
        cuint64 num_rows_to_add = 10'000;

        // Initial test preparation
        //
        auto actual = database->getActualInterface();
        const vector<QualifiedColumn> columns{{"title", ColumnType::TEXT}, {"length", ColumnType::BIGINT}};
        t_movies = database->createTable("movies", "", columns, "movie:", IdxTable::default_config);
        SCOPE_EXIT {
            database->dropTable("movies", "");
            auto ret = database->getTable("movies", "");
            TEST_VALUE(false, ret.has_value());
        };

        movies_cids = t_movies->getCIDs();

        const uint64 N = 8000;
        const uint64 D = 3000;

        // insert some data
        for (uint64 i = 0; i < N; i++) {
            string title = "abc";
            title += to_string(i);

            auto stok = t_movies->tokenizeString(title).getFullToken();

            uint64 length = 123 + i;
            t_movies->upinsertKey(to_string(i), movies_cids, vector<UINT64>{stok, length});
        }

        LOG_INFO("Table has now " << t_movies->getNumRowsApprox() << " rows.");

        {
            PublisherCount publisher;
            database->parse_and_run_query(&publisher, "SELECT title, length FROM movies WHERE title IS NOT NULL ORDER BY length");
            TEST_VALUE(N, publisher.get_count());
        }
        {
            // By inserting only cids[1], we set cids[0] to NULL
            //
            cuint64 len_value = 111ull;
            t_movies->upinsertKey("0", {movies_cids[1]}, {len_value});
            PublisherCount publisher1;
            PublisherV2DStrings publisher2;

            database->parse_and_run_query(&publisher1, "SELECT title, length FROM movies WHERE title IS NOT NULL ORDER BY length");
            TEST_VALUE(N - 1, publisher1.get_count());
            database->parse_and_run_query(&publisher2, "SELECT length FROM movies WHERE title IS NULL");
            TEST_VALUE(1, publisher2.get_data().size());
            TEST_VALUE(to_string(len_value), publisher2.get_data()[0][0].value());
        }

        // Delete some data, check results
        for (uint64 i = 0; i < D; i++) {
            memurai::sql::Test_Query_Exec::hard_delete(t_movies, {i});
        }

        {
            PublisherCount publisher;
            database->parse_and_run_query(&publisher, "SELECT title, length FROM movies WHERE title IS NOT NULL ORDER BY length");
            TEST_VALUE(N - D, publisher.get_count());
        }

        {
            PublisherCount publisher;
            database->parse_and_run_query(&publisher, "SELECT title, length FROM movies ORDER BY length");
            TEST_VALUE(N - D, publisher.get_count());
        }
    } catch (...) {
        TEST_VALUE(1, 0);
    }
}

} // namespace

void test_query_exec(bool quick) {
    cuint32 db_thread_pool_size = 16;
    typedef WorkerThread<syncqueue<TLambda>, std::exception, memurai::exception> WorkerThread;
    ThreadPool<WorkerThread> thread_pool("test_db", db_thread_pool_size);
    ModuleInterface module_interface;
    multicore::IServer server{[&](vector<Lambda> lambdas, IToken* token) {
                                  Token* innertoken = new Token("wg_token", lambdas.size(), token);
                                  thread_pool.submit(lambdas, innertoken);
                              },
                              [&](Lambda item, IToken* token) {
                                  thread_pool.submit(item, token);
                              },
                              [](Lambda) {},
                              [](Lambda, uint64, JOBID) {},
                              [](JOBID) {},
                              [](string) {},
                              module_interface};

    delete_test_files(tmp_folder);
    database = Database::createDatabase("testdb", server, test_config);
    SCOPE_EXIT {
        delete database;
    };

    test_delete();

    test_concurrent_selects_and_adds2();

    test_select_simple();
    test_select_simple2();

    const vector<uint32> long_list_num_threads{2, 4, 6, 8, 12, 16, 20, 24, 32};
    const vector<uint32> short_list_num_threads{4, 8};
    const vector<uint32>& list_num_threads = quick ? short_list_num_threads : long_list_num_threads;

    for (auto num_threads : list_num_threads) {
        test_select_big(num_threads);
        test_concurrent_selects_and_adds(num_threads);
        test_concurrent_selects(num_threads);
    }

    TEST_VALUE(0, Buffer::get_num_buffers());
}
