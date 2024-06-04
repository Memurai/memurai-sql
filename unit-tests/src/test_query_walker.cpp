/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#include <map>
#include <string>
#include <vector>

#include "IActual.h"
#include "QueryWalker.h"

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

using namespace std;
using namespace memurai::multicore;

namespace {
using namespace memurai;
using namespace memurai::sql;

optional<QWalker*> run_query_no_exception(cchar* query, IActual& actual, const vecstring pars = {""}) {
    optional<QWalker*> ret;

    try {
        ret.emplace(QWalker::walkSQLQueryTree(query, actual, pars));
    } catch (const memurai::exception& ex) {
        LOG_DEBUG("Got expected memurai exception: " << ex.what());
        // should not get here
        TEST_VALUE(1, 0);
    } catch (const std::exception& ex) {
        LOG_ERROR("Not supposed to get std exception: " << ex.what());
        // should not get here
        TEST_VALUE(1, 0);
    } catch (...) {
        // should not get here
        TEST_VALUE(1, 0);
    }

    return ret;
}

optional<QWalker*> test_expect_memurai_exception(cchar* query, IActual& actual, ErrorCode expected) {
    bool got_memurai_exception = false;
    optional<QWalker*> ret;

    try {
        ret.emplace(QWalker::walkSQLQueryTree(query, actual, {""}));
    } catch (const memurai::exception& ex) {
        LOG_DEBUG("Got expected memurai exception: " << ex.what());
        // should get here
        got_memurai_exception = true;
        TEST_VALUE(expected, ErrorCode{ex.code});
    } catch (const std::exception& ex) {
        LOG_ERROR("Not supposed to get std exception: " << ex.what());
        // should not get here
        TEST_VALUE(1, 0);
    } catch (...) {
        // should not get here
        TEST_VALUE(1, 0);
    }
    TEST_VALUE(true, got_memurai_exception);

    return ret;
}

void test_degenerate_cases() {
    IActual actual;

    // Test some degenerate cases
    //  Empty query, nullptr query, random garbage in query
    //
    // Empty query
    test_expect_memurai_exception("", actual, ErrorCode::MalformedInput);

    // nullptr query
    test_expect_memurai_exception(nullptr, actual, ErrorCode::MalformedInput);

    // random garbage in query
    test_expect_memurai_exception("fdhn^4j*8y9(043ywhgnbdfklngk", actual, ErrorCode::MalformedInput);
}

static map<memurai::sql::TableNameType, map<ColumnName, QualifiedCID>> all_col_names = {
    {"table1", {{"col1", {101, ColumnType::BIGINT}}, {"col2", {102, ColumnType::FLOAT}}, {"col3", {103, ColumnType::DATE}}, {"col4", {104, ColumnType::TEXT}}}},
    {"students", {{"name", {201, ColumnType::TEXT}}, {"grade", {203, ColumnType::FLOAT}}, {"country", {203, ColumnType::TEXT}}}},
};

IActual buildActual1() {
    IActual actual;

    actual.translateColumn = [](const string& table_name, const string& col_name) -> optional<QualifiedCID> {
        auto iter1 = all_col_names.find(table_name);
        EXPECT_OR_RETURN(iter1 != all_col_names.end(), std::nullopt);

        auto one_table = iter1->second;
        auto iter2 = one_table.find(col_name);
        EXPECT_OR_RETURN(iter2 != one_table.end(), std::nullopt);
        return iter2->second;
    };
    actual.getStringToken = [&](TID, cchar* cstr) -> StringToken {
        return StringToken(std::atoll(cstr), StringType::Normal);
    };

    actual.getTableTID = [](const TableNameType& table_name, const SchemaName& schema_name) -> optional<TID> {
        if (!schema_name.empty())
            return std::nullopt;

        auto iter1 = all_col_names.find(table_name);
        EXPECT_OR_RETURN(iter1 != all_col_names.end(), std::nullopt);

        return 101;
    };

    return std::move(actual);
}

void test_not_supported() {
    auto actual = buildActual1();

    auto expected = ErrorCode::Unsupported;

    // '*' not permitted!
    test_expect_memurai_exception("SELECT * FROM table1;", actual, expected);

    // These are not supported.. for now.
    test_expect_memurai_exception("INSERT INTO test_table (id, value, name) VALUES (1, 2, 'test');", actual, expected);
    test_expect_memurai_exception("CREATE TABLE students (name TEXT, student_number INTEGER, city TEXT, grade DOUBLE);", actual, expected);
    test_expect_memurai_exception("DELETE FROM students;", actual, expected);
    test_expect_memurai_exception("TRUNCATE students;", actual, expected);
    test_expect_memurai_exception("UPDATE students SET grade = 1.0;", actual, expected);
    test_expect_memurai_exception("DROP TABLE students;", actual, expected);
    test_expect_memurai_exception("PREPARE prep_inst FROM 'INSERT INTO test VALUES (?, ?, ?)';", actual, expected);
    test_expect_memurai_exception("EXECUTE prep;", actual, expected);
    test_expect_memurai_exception("DEALLOCATE PREPARE prep;", actual, expected);
    test_expect_memurai_exception("COPY students FROM 'student.tbl';", actual, expected);
    test_expect_memurai_exception("SELECT name FROM students WITH HINT(NO_CACHE);", actual, expected);
    test_expect_memurai_exception("SELECT name FROM students JOIN table1 ON country = 'italy';", actual, expected);
    test_expect_memurai_exception("SELECT col1, MAX(col2), MAX(col3, col2), CUSTOM(col1, UP(col2)) AS f FROM table1;", actual, expected);
    test_expect_memurai_exception("SELECT SUBSTR(name, 3, 5) FROM students;", actual, expected);
    test_expect_memurai_exception("SELECT col1 FROM(SELECT col1 FROM table1);", actual, expected);
    test_expect_memurai_exception("SELECT 2*grade as blah FROM students;", actual, expected);
    test_expect_memurai_exception("SELECT name FROM students GROUP BY city;", actual, expected);

    run_query_no_exception("SELECT name FROM students WHERE grade = ? AND country = ?", actual, {"1", "a"});
    run_query_no_exception("SELECT name FROM students OFFSET 10;", actual);
    run_query_no_exception("SELECT name FROM students LIMIT 2;", actual);
    run_query_no_exception("SELECT name FROM students LIMIT 2 OFFSET 10 ;", actual);
    // WE DO SUPPORT ORDER BY!!!
}

void test_simple_1() {
    auto actual = buildActual1();

    // SELECT:  1 column, no WHERE
    {
        auto opt = run_query_no_exception("SELECT col1 from table1;", actual);
        TEST_VALUE(true, opt.has_value());
        if (!opt.has_value())
            return;

        auto walker = *opt;

        auto projs = walker->getProjectionsCID();
        TEST_VALUE(1, projs.size());
        if (projs.size() != 1)
            return;

        TEST_VALUE(101, projs[0]);
        TEST_VALUE(0, strcmp("table1", walker->getSourceTable()));
    }

    // SELECT:  2 columns, no WHERE
    {
        auto opt = run_query_no_exception("SELECT col1, col2 from table1;", actual);
        TEST_VALUE(true, opt.has_value());
        if (!opt.has_value())
            return;

        auto walker = *opt;

        auto projs = walker->getProjectionsCID();
        TEST_VALUE(2, projs.size());
        if (projs.size() != 2)
            return;

        TEST_VALUE(101, projs[0]);
        TEST_VALUE(102, projs[1]);
        TEST_VALUE(0, strcmp("table1", walker->getSourceTable()));
    }

    // SELECT:  2 columns different order, no WHERE
    {
        auto opt = run_query_no_exception("SELECT col2, col1 from table1;", actual);
        TEST_VALUE(true, opt.has_value());
        if (!opt.has_value())
            return;

        auto walker = *opt;

        auto projs = walker->getProjectionsCID();
        TEST_VALUE(2, projs.size());
        if (projs.size() != 2)
            return;

        TEST_VALUE(102, projs[0]);
        TEST_VALUE(101, projs[1]);
        TEST_VALUE(0, strcmp("table1", walker->getSourceTable()));
    }
}
} // namespace

namespace memurai::sql {

class Test_QWalker {
public:
    static void test_where_simple() {
        auto actual = buildActual1();

        // SELECT:  2 columns, WHERE EQ 1 column not in projection
        {
            auto opt = run_query_no_exception("SELECT col2, col1 from table1 WHERE col1 = 4;", actual);
            TEST_VALUE(true, opt.has_value());
            if (!opt.has_value())
                return;

            auto walker = *opt;

            auto projs = walker->getProjectionsCID();
            TEST_VALUE(2, projs.size());
            if (projs.size() != 2)
                return;

            TEST_VALUE(102, projs[0]);
            TEST_VALUE(101, projs[1]);
            TEST_VALUE(0, strcmp("table1", walker->getSourceTable()));

            TEST_VALUE(NodeType::nOperator, walker->whereRootNode->type);
            TEST_VALUE(hsql::kOpEquals, get<hsql::OperatorType>(walker->whereRootNode->value));
            TEST_VALUE(2, walker->whereRootNode->kids.size());
            if (walker->whereRootNode->kids.size() != 2)
                return;

            auto left = walker->whereRootNode->kids[0];
            auto right = walker->whereRootNode->kids[1];
            TEST_VALUE(true, (left->type == nColumnRef) || (right->type == nColumnRef));
            TEST_VALUE(NodeType::nLiteralInt, right->type);
            TEST_VALUE(101, get<CID>(left->value));
            TEST_VALUE(4, get<int64>(right->value));
        }

        // SELECT:  2 columns, WHERE EQ 1 column IN projection
        {
            auto opt = run_query_no_exception("SELECT col2, col1 from table1 WHERE col2 = 4.1;", actual);
            TEST_VALUE(true, opt.has_value());
            if (!opt.has_value())
                return;

            auto walker = *opt;

            auto projs = walker->getProjectionsCID();
            TEST_VALUE(2, projs.size());
            if (projs.size() != 2)
                return;

            TEST_VALUE(102, projs[0]);
            TEST_VALUE(101, projs[1]);
            TEST_VALUE(0, strcmp("table1", walker->getSourceTable()));

            TEST_VALUE(NodeType::nOperator, walker->whereRootNode->type);
            TEST_VALUE(hsql::kOpEquals, get<hsql::OperatorType>(walker->whereRootNode->value));
            TEST_VALUE(2, walker->whereRootNode->kids.size());
            if (walker->whereRootNode->kids.size() != 2)
                return;

            auto left = walker->whereRootNode->kids[0];
            auto right = walker->whereRootNode->kids[1];
            TEST_VALUE(true, (left->type == nColumnRef) || (right->type == nColumnRef));
            TEST_VALUE(nLiteralFloat, right->type);
            TEST_VALUE(102, get<CID>(left->value));
            TEST_VALUE(4.1, get<double>(right->value));
        }

        // SELECT: WHERE with 2 conditions: > <
        {
            auto opt = run_query_no_exception("SELECT col2 from table1 WHERE col2 > 4 AND col1 < -10;", actual);
            TEST_VALUE(true, opt.has_value());
            if (!opt.has_value())
                return;

            auto walker = *opt;
            auto projs = walker->getProjectionsCID();
            TEST_VALUE(1, projs.size());
            if (projs.size() != 1)
                return;

            TEST_VALUE(102, projs[0]);
            TEST_VALUE(0, strcmp("table1", walker->getSourceTable()));

            TEST_VALUE(NodeType::nOperator, walker->whereRootNode->type);
            TEST_VALUE(hsql::kOpAnd, get<hsql::OperatorType>(walker->whereRootNode->value));
            TEST_VALUE(2, walker->whereRootNode->kids.size());
            if (walker->whereRootNode->kids.size() != 2)
                return;

            auto left = walker->whereRootNode->kids[0];
            auto right = walker->whereRootNode->kids[1];
            TEST_VALUE(nOperator, left->type);
            TEST_VALUE(nOperator, right->type);
            TEST_VALUE(hsql::kOpGreater, get<hsql::OperatorType>(left->value));
            TEST_VALUE(hsql::kOpLess, get<hsql::OperatorType>(right->value));

            TEST_VALUE(2, left->kids.size());
            TEST_VALUE(2, right->kids.size());
            if (left->kids.size() != 2)
                return;
            if (right->kids.size() != 2)
                return;

            auto leftleft = left->kids[0];
            auto leftright = left->kids[1];
            TEST_VALUE(nColumnRef, leftleft->type);
            TEST_VALUE(nLiteralFloat, leftright->type);
            TEST_VALUE(102, get<CID>(leftleft->value));
            TEST_VALUE(4.0, get<double>(leftright->value));

            auto rightleft = right->kids[0];
            auto rightright = right->kids[1];
            TEST_VALUE(nColumnRef, rightleft->type);
            TEST_VALUE(nLiteralInt, rightright->type);
            TEST_VALUE(101, get<CID>(rightleft->value));
            TEST_VALUE(-10, get<int64>(rightright->value));
        }
    }
};

} // namespace memurai::sql

void test_sql_query_planner(bool quick) {
    test_degenerate_cases();

    test_not_supported();

    test_simple_1();

    Test_QWalker::test_where_simple();

    const vector<uint32> long_list_num_threads{1, 2, 4, 6, 8, 12, 16, 20, 24, 32};
    const vector<uint32> short_list_num_threads{4, 8};
    const vector<uint32>& list_num_threads = quick ? short_list_num_threads : long_list_num_threads;

    for (auto num_threads : list_num_threads) {
    }
}
