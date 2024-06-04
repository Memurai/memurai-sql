/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#include <map>
#include <optional>
#include <string>
#include <vector>
#include <filesystem>

#include "database.h"
#include "threads/cancellation.h"
#include "threads/thread_pool.h"
#include "threads/worker_thread.h"

#include "test/test_utils.h"

using namespace std;
using namespace memurai;
using namespace memurai::multicore;
using namespace memurai::sql;

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
    };                                                                                                           \
    cuint32 db_thread_pool_size = std::thread::hardware_concurrency();                                           \
    typedef WorkerThread<syncqueue<TLambda>, std::exception, memurai::exception> WorkerThread;                   \
    ThreadPool<WorkerThread> thread_pool("test_db", db_thread_pool_size);                                        \
    ModuleInterface module_interface;                                                                            \
    multicore::IServer server{[&](vector<Lambda> lambdas, IToken* token) {                                       \
                                  Token* innertoken = new Token("wg_token", lambdas.size(), token);              \
                                  thread_pool.submit(lambdas, innertoken);                                       \
                              },                                                                                 \
                              [&](Lambda item, IToken* token) {                                                  \
                                  thread_pool.submit(item, token);                                               \
                              },                                                                                 \
                              [](Lambda) {},                                                                     \
                              [](Lambda, uint64, JOBID) {},                                                      \
                              [](JOBID) {},                                                                      \
                              [](string) {},                                                                     \
                              module_interface};

extern string g_addendum;

namespace {
Database* database{nullptr};

const string tmp_folder = "temp_folder";

Database::Config test_config_persistence_on = {100,
                                               "idxtable_",
                                               ".meta",
                                               "master.tids",
                                               tmp_folder,
#ifndef _MSC_VER
                                               "/"
#else
                                               "\\"
#endif
                                               ,
                                               true};

Database::Config test_config_persistence_off = {100,
                                                "idxtable_",
                                                ".meta",
                                                "master.tids",
                                                tmp_folder,
#ifndef _MSC_VER
                                                "/"
#else
                                                "\\"
#endif
                                                ,
                                                false};

/*
These do not seem to be working in Linux for some reason...

const Database::Config test_config_persistence_on {
    Database::default_config.num_max_tables,
    Database::default_config.idxtable_metadata_prefix,
    Database::default_config.idxtable_metadata_suffix,
    Database::default_config.master_tid_path,
    tmp_folder,
    Database::default_config.folder_separator,
    true
};

const Database::Config test_config_persistence_off {
    Database::default_config.num_max_tables,
    Database::default_config.idxtable_metadata_prefix,
    Database::default_config.idxtable_metadata_suffix,
    Database::default_config.master_tid_path,
    tmp_folder,
    Database::default_config.folder_separator,
    false
};
*/

#define TEST2DATES(A, B, C) Test2Dates(A, B, C, __LINE__)
#define TEST2TIMESTAMPS(A, B, C) Test2Timestamps(A, B, C, __LINE__)

void Test2Dates(cchar* d1, cchar* d2, int64 delta, int linenum) {
    optional<UINT64> date1 = IdxTable::serialize_date(d1);
    optional<UINT64> date2 = IdxTable::serialize_date(d2);
    TEST_VALUE_IMPL(true, date1.has_value(), linenum);
    TEST_VALUE_IMPL(true, date2.has_value(), linenum);
    TEST_VALUE_IMPL(delta, *date2 - *date1, linenum);
}

void Test2Timestamps(cchar* ts1, cchar* ts2, int64 delta, int linenum) {
    optional<UINT64> sts1 = IdxTable::serialize_timestamp(ts1);
    optional<UINT64> sts2 = IdxTable::serialize_timestamp(ts2);
    TEST_VALUE_IMPL(true, sts1.has_value(), linenum);
    TEST_VALUE_IMPL(true, sts2.has_value(), linenum);
    TEST_VALUE_IMPL(delta, *sts2 - *sts1, linenum);
}

void test_timestamps(bool quick) {
    TEST_VALUE(false, IdxTable::serialize_timestamp("crap").has_value());

    TEST_VALUE(true, IdxTable::serialize_timestamp("2000-1-1T23:23:23.012").has_value());
    TEST_VALUE(true, IdxTable::serialize_timestamp("2000-1-1T23:23:23.012+000").has_value());
    TEST_VALUE(false, IdxTable::serialize_timestamp("1600-1-1T23:23:23.012+000").has_value());
    TEST_VALUE(false, IdxTable::serialize_timestamp("2000-13-1T23:23:23.012+000").has_value());
    TEST_VALUE(false, IdxTable::serialize_timestamp("2000-1-44T23:23:23.012+000").has_value());
    TEST_VALUE(false, IdxTable::serialize_timestamp("2000-1-1T25:23:23.012+000").has_value());
    TEST_VALUE(false, IdxTable::serialize_timestamp("2000-1-1T23:71:23.012+000").has_value());
    TEST_VALUE(false, IdxTable::serialize_timestamp("2000-1-1T23:23:88.012+000").has_value());
    TEST_VALUE(false, IdxTable::serialize_timestamp("2000-1-1U23:23:23.012").has_value());
    TEST_VALUE(false, IdxTable::serialize_timestamp("2000-11T23:23:23.012").has_value());

    // Feb 28.. or 29?
    TEST_VALUE(true, IdxTable::serialize_timestamp("2001-02-28T23:23:23.012").has_value());
    TEST_VALUE(true, IdxTable::serialize_timestamp("2000-02-29T23:23:23.012").has_value());
    // TO DO:  fix leap years
#if 0  
    TEST_VALUE(false, IdxTable::serialize_timestamp("2001-02-29T23:23:23.012").has_value());
    TEST_VALUE(false, IdxTable::serialize_timestamp("2002-02-29T23:23:23.012").has_value());
    TEST_VALUE(false, IdxTable::serialize_timestamp("2003-02-29T23:23:23.012").has_value());
    TEST_VALUE(true, IdxTable::serialize_timestamp("2004-02-29T23:23:23.012").has_value());
    TEST_VALUE(false, IdxTable::serialize_timestamp("2005-02-29T23:23:23.012").has_value());
#endif

    TEST2TIMESTAMPS("2000-1-1T23:23:23.012", "2000-1-1T23:23:23.013", 1);
    TEST2TIMESTAMPS("2000-1-1T23:23:23.999", "2000-1-1T23:23:24.000", 1);
    TEST2TIMESTAMPS("2000-1-1T23:23:23.999", "2000-1-2T23:23:23.999", 24 * 3600 * 1000);
    TEST2TIMESTAMPS("2000-1-1T23:23:23.999", "2000-1-2T23:23:24.000", 24 * 3600 * 1000 + 1);

    TEST2TIMESTAMPS("2000-1-1T23:23:23.999", "2000-1-1T23:23:24.000", 1);

    TEST_VALUE(true, IdxTable::serialize_timestamp("2000-1-1T23:23:23.012+000").has_value());
}

void test_dates(bool quick) {
    TEST_VALUE(false, IdxTable::serialize_date("crap").has_value());
    TEST_VALUE(false, IdxTable::serialize_date("1600-1-1").has_value());
    TEST_VALUE(false, IdxTable::serialize_date("1-1-2000").has_value());

    TEST2DATES("2000-01-01", "2000/1/1", 0);
    TEST2DATES("2000-1-01", "2000/Jan/1", 0);
    TEST2DATES("2010-Dec-31", "2010/Dec/31", 0);
    TEST2DATES("2010-Dec-31", "2011-1-1", 1);
    TEST2DATES("2010-Feb-28", "2010-3-1", 1);
}

void test_dates_2(bool quick) {
    const int advance = quick ? 100 : 1;

    for (int day = 0; day < 20000; day += advance) {
        stringstream ss;
        IdxTable::print_date(day, ss);
        string str = ss.str();
        auto ret = IdxTable::serialize_date(str.c_str());

        TEST_VALUE(true, ret.has_value());
        if (ret.has_value()) {
            TEST_VALUE(day, *ret);
        }
    }
}

#define EXPECT_SAME_COLUMNS(A, B) ExpectSameColumns(A, B, __LINE__)

void ExpectSameColumns(const vector<QualifiedColumn>& expected, const vector<FullyQualifiedCID>& fqcids, int linenum) {
    TEST_VALUE_IMPL(expected.size(), fqcids.size(), linenum);
    if (expected.size() != fqcids.size())
        return;

    for (auto exp : expected) {
        auto name = get<0>(exp);
        auto type = get<1>(exp);
        bool found = false;

        for (auto fq : fqcids) {
            auto name2 = get<0>(fq);
            auto type2 = get<2>(fq);

            if (name == name2) {
                TEST_VALUE_IMPL(uint32(type), uint32(type2), linenum);
                if (type == type2) {
                    found = true;
                    break;
                }
            }
        }
        TEST_VALUE_IMPL(true, found, linenum);
    }
}

void test_configuration(bool quick) {
    TEST_START;

    IdxTable* t_students = nullptr;
    const vector<QualifiedColumn> students_qcols{
        {"fullname", ColumnType::TEXT}, {"dob", ColumnType::DATE}, {"avgscore", ColumnType::FLOAT}, {"someint", ColumnType::BIGINT}};

    try {
        delete_test_files(tmp_folder);

        // Create and then stop a DB. Expect a previously created table to be there next time around.
        //
        {
            delete_test_files(tmp_folder);

            database = Database::createDatabase("testdb", server, test_config_persistence_on);
            t_students = database->createTable("students", "", students_qcols, "students:", IdxTable::default_config);
            delete database;
            database = Database::createDatabase("testdb", server, test_config_persistence_on);
            auto opt = database->getTable("students", "");
            TEST_VALUE(true, opt.has_value());
            if (opt.has_value()) {
                t_students = *opt;
                EXPECT_SAME_COLUMNS(students_qcols, t_students->getFullyQualifiedCIDs());
            }
        }

        // Create and then stop a DB.
        // Restart with presistenc OFF and do not expect that table to exist.
        //
        {
            delete_test_files(tmp_folder);

            database = Database::createDatabase("testdb", server, test_config_persistence_off);
            t_students = database->createTable("students", "", students_qcols, "students:", IdxTable::default_config);
            delete database;
            database = Database::createDatabase("testdb", server, test_config_persistence_on);
            auto opt = database->getTable("students", "");
            TEST_VALUE(false, opt.has_value());
        }

        // Create and then stop a DB. Expect a previously created table to be there next time around.
        //
        {
            delete_test_files(tmp_folder);

            database = Database::createDatabase("testdb", server, test_config_persistence_on);
            t_students = database->createTable("students", "", students_qcols, "students:", IdxTable::default_config);
            auto t2 = database->createTable("students2", "", students_qcols, "students:", IdxTable::default_config);

            delete database;
            database = Database::createDatabase("testdb", server, test_config_persistence_on);
            {
                auto opt = database->getTable("students", "");
                TEST_VALUE(true, opt.has_value());
                if (opt.has_value()) {
                    t_students = *opt;
                    EXPECT_SAME_COLUMNS(students_qcols, t_students->getFullyQualifiedCIDs());
                }
            }
            {
                auto opt = database->getTable("students2", "");
                TEST_VALUE(true, opt.has_value());
                if (opt.has_value()) {
                    t2 = *opt;
                    EXPECT_SAME_COLUMNS(students_qcols, t2->getFullyQualifiedCIDs());
                }
            }
        }

        // Create a DB with too many tables.
        //
        {
            delete_test_files(tmp_folder);

            Database::Config test_config{2, // num_max_tables
                                         Database::default_config.idxtable_metadata_prefix,
                                         Database::default_config.idxtable_metadata_suffix,
                                         Database::default_config.master_tid_path,
                                         tmp_folder,
                                         Database::default_config.folder_separator,
                                         true};

            database = Database::createDatabase("testdb", server, test_config);
            auto table1 = database->createTable("students1", "", students_qcols, "students:", IdxTable::default_config);
            auto table2 = database->createTable("students2", "", students_qcols, "students:", IdxTable::default_config);
            auto table3 = database->createTable("students3", "", students_qcols, "students:", IdxTable::default_config);

            TEST_VALUE_NEQ(nullptr, table1);
            TEST_VALUE_NEQ(nullptr, table2);
            TEST_VALUE(nullptr, table3);

            delete database;
            database = Database::createDatabase("testdb", server, test_config);
            TEST_VALUE(true, database->getTable("students1", "").has_value());
            TEST_VALUE(true, database->getTable("students2", "").has_value());
            // SHOULD NOT BE HERE!!
            TEST_VALUE(false, database->getTable("students", "").has_value());
            TEST_VALUE(false, database->getTable("students3", "").has_value());
        }

        // Restore a DB with too many tables.
        //
        {
            delete_test_files(tmp_folder);

            Database::Config test_config1{3, // num_max_tables
                                          Database::default_config.idxtable_metadata_prefix,
                                          Database::default_config.idxtable_metadata_suffix,
                                          Database::default_config.master_tid_path,
                                          tmp_folder,
                                          Database::default_config.folder_separator,
                                          true};

            database = Database::createDatabase("testdb", server, test_config1);
            auto table1 = database->createTable("students1", "", students_qcols, "students:", IdxTable::default_config);
            auto table2 = database->createTable("students2", "", students_qcols, "students:", IdxTable::default_config);
            auto table3 = database->createTable("students3", "", students_qcols, "students:", IdxTable::default_config);
            TEST_VALUE_NEQ(nullptr, table1);
            TEST_VALUE_NEQ(nullptr, table2);
            TEST_VALUE_NEQ(nullptr, table3);

            delete database;
            Database::Config test_config2{2, // num_max_tables
                                          Database::default_config.idxtable_metadata_prefix,
                                          Database::default_config.idxtable_metadata_suffix,
                                          Database::default_config.master_tid_path,
                                          tmp_folder,
                                          Database::default_config.folder_separator,
                                          true};

            database = Database::createDatabase("testdb", server, test_config2);
            // Max tables is 2, metadata has 3.
            // NO TABLE IS LOADED!!!
            TEST_VALUE(false, database->getTable("students1", "").has_value());
            TEST_VALUE(false, database->getTable("students2", "").has_value());
            TEST_VALUE(false, database->getTable("students", "").has_value());
            TEST_VALUE(false, database->getTable("students3", "").has_value());
        }

        // Create and then stop a DB. Corrupt file by changing size and expect no tables.
        //
        {
            delete_test_files(tmp_folder);

            database = Database::createDatabase("testdb", server, test_config_persistence_on);
            t_students = database->createTable("students", "", students_qcols, "students:", IdxTable::default_config);
            delete database;
            auto filename = tmp_folder + std::string("/idxtable_1.meta");
            std::filesystem::resize_file(filename, std::filesystem::file_size(filename)-1);
            database = Database::createDatabase("testdb", server, test_config_persistence_on);
            auto opt = database->getTable("students", "");
            TEST_VALUE(false, opt.has_value());
        }

        // Restore a DB with an incompatible metadata version.
        //
        {
            //  ... hmmhm.... software doesn't allow me to change metadata version right now.
        }

        delete_test_files(tmp_folder);
    } catch (...) {
        delete_test_files(tmp_folder);
        TEST_VALUE(1, 0);
    }
}

void test_simple() {
    TEST_START;
    delete_test_files(tmp_folder);

    const vector<QualifiedColumn> students_qcols{
        {"fullname", ColumnType::TEXT}, {"dob", ColumnType::DATE}, {"avgscore", ColumnType::FLOAT}, {"someint", ColumnType::BIGINT}};

    database = Database::createDatabase("testdb", server, test_config_persistence_on);
    SCOPE_EXIT {
        delete database;
    };

    try {
        auto table1 = database->createTable("students", "", students_qcols, "students:", IdxTable::default_config);
        TEST_VALUE(false, table1 == nullptr);
        auto table2 = database->createTable("students", "", students_qcols, "students:", IdxTable::default_config);
        TEST_VALUE(nullptr, table2);
    } catch (...) {
        TEST_VALUE(1, 0);
    }
}

void test_get_tables_names() {
    TEST_START;

    const vector<tuple<string, vector<QualifiedColumn>>> tables{
        {"st1", {{"fullname", ColumnType::TEXT}, {"dob", ColumnType::DATE}, {"avgscore", ColumnType::FLOAT}, {"someint", ColumnType::BIGINT}}},
        {"st2", {{"fullname", ColumnType::TEXT}, {"dob", ColumnType::DATE}, {"avgscore", ColumnType::FLOAT}}},
        {"st3", {{"fullname", ColumnType::TEXT}, {"dob", ColumnType::DATE}}},
        {"st4", {{"fullname", ColumnType::TEXT}}},
    };

    vector<string> expected;

    database = Database::createDatabase("testdb", server, test_config_persistence_off);
    SCOPE_EXIT {
        delete database;
    };

    for (const auto& table : tables) {
        try {
            delete_test_files(tmp_folder);
            database->createTable(get<0>(table), "", get<1>(table), (get<0>(table)) + ":", IdxTable::default_config);
            expected.push_back(get<0>(table));
            vector<string> gotten = database->getTablesNames();
            TEST_VALUE(gotten.size(), expected.size());
            for (int j = 0; j < gotten.size(); ++j) {
                TEST_VALUE(expected[j], gotten[j]);
            }
        } catch (...) {
            TEST_VALUE(1, 0);
        }
    }
}

void test_get_tables_names_with_columns() {
    TEST_START;

    const vector<tuple<string, vector<QualifiedColumn>>> tables{
        {"st1", {{"fullname", ColumnType::TEXT}, {"dob", ColumnType::DATE}, {"avgscore", ColumnType::FLOAT}, {"someint", ColumnType::BIGINT}}},
        {"st2", {{"fullname", ColumnType::TEXT}, {"dob", ColumnType::DATE}, {"avgscore", ColumnType::FLOAT}}},
        {"st3", {{"fullname", ColumnType::TEXT}, {"dob", ColumnType::DATE}}},
        {"st4", {{"fullname", ColumnType::TEXT}}},
    };

    vector<tuple<string, vector<QualifiedColumn>>> expected;

    database = Database::createDatabase("testdb", server, test_config_persistence_off);
    SCOPE_EXIT {
        delete database;
    };

    int cid = 1;

    for (const auto& table : tables) {
        try {
            delete_test_files(tmp_folder);
            database->createTable(get<0>(table), "", get<1>(table), (get<0>(table)) + ":", IdxTable::default_config);
            vector<QualifiedColumn> expected_qcol;
            auto cur_cols = get<1>(table);
            for (auto& cur_col : cur_cols) {
                expected_qcol.emplace_back(get<0>(cur_col), get<1>(cur_col));
            }
            expected.emplace_back(get<0>(table), expected_qcol);
            vector<tuple<string, vector<FullyQualifiedCID>>> gotten = database->getTablesNamesWithColumns();
            TEST_VALUE(gotten.size(), expected.size());
            for (int j = 0; j < gotten.size(); ++j) {
                TEST_VALUE(get<0>(expected[j]), get<0>(gotten[j]));
                EXPECT_SAME_COLUMNS(get<1>(expected[j]), get<1>(gotten[j]));
            }
        } catch (...) {
            TEST_VALUE(1, 0);
        }
    }
}

void test_prefix(bool quick) {
    TEST_START;

    const vector<QualifiedColumn> students_qcols{
        {"fullname", ColumnType::TEXT}, {"dob", ColumnType::DATE}, {"avgscore", ColumnType::FLOAT}, {"someint", ColumnType::BIGINT}};

    database = Database::createDatabase("testdb", server, test_config_persistence_on);
    SCOPE_EXIT {
        delete database;
    };
    try {
        auto table1 = database->createTable("students1", "", students_qcols, "students:", IdxTable::default_config);
        auto table2 = database->createTable("students2", "", students_qcols, "students:", IdxTable::default_config);
        TEST_VALUE(true, table1 != nullptr);
        TEST_VALUE(true, table2 != nullptr);
    } catch (...) {
        TEST_VALUE(1, 0);
    }
}

} // namespace

void test_database(bool quick) {
    test_simple();

    test_get_tables_names();

    test_get_tables_names_with_columns();

    test_configuration(quick);

    test_prefix(quick);

    test_timestamps(quick);

    test_dates(quick);

    test_dates_2(quick);
}
