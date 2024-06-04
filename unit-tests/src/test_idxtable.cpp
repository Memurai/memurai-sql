/*
 * Copyright (c) 2020-2021, Janea Systems
   by Benedetto Proietti
 */

#include <atomic>
#include <barrier>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "types.h"

#include "jobid.h"
#include "memurai-multicore.h"
#include "scope/take_time.h"
#include "threads/cancellation.h"
#include "threads/thread_pool.h"
#include "threads/worker_thread.h"

#include "PublisherV2DStrings.h"
#include "shared_table_lock.h"
#include "test/test_table_utils.h"
#include "test/test_utils.h"

#undef TEST_START

extern string g_addendum;

#define PREPARE_THREADPOOL_SERVER                                                                   \
    cuint32 server_threadPoolSize = 16;                                                             \
    typedef WorkerThread<syncqueue<TLambda>, std::exception, memurai::exception> WorkerThread;      \
    ThreadPool<WorkerThread> thread_pool(g_test_name, server_threadPoolSize);                       \
    ModuleInterface module_interface;                                                               \
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

#define TEST_START                                                                                               \
    g_test_name = string(__func__) + g_addendum;                                                                 \
    l_errors = 0;                                                                                                \
    l_warnings = 0;                                                                                              \
    LOG_VERBOSE(g_test_name << " is starting ");                                                                 \
    SCOPE_EXIT {                                                                                                 \
        LOG_VERBOSE(g_test_name << " ended with: " << l_errors << " errors and " << l_warnings << " warnings."); \
        g_addendum = "";                                                                                         \
        std::this_thread::sleep_for(std::chrono::milliseconds(10));                                              \
    };                                                                                                           \
    PREPARE_THREADPOOL_SERVER;

#define ALLRIDS table.getNextRid()

namespace memurai::sql {

class DtorBHandle {
    BHandle handle;

public:
    DtorBHandle() = delete;
    DtorBHandle(const DtorBHandle&) = delete;
    DtorBHandle(DtorBHandle&&) = delete;

    DtorBHandle(BHandle h) : handle(h) {}

    operator BHandle() const {
        return handle;
    }

    ~DtorBHandle() {
        Buffer::destroy(handle);
    }
};

class Test_IdxTable {
    template <typename T> static T SumAllEmemsInCID_core(IdxTable& table, CID cid) {
        // we are processing inside table UNDER A ReadWrite Read lock
        T sum{0};

        Column& col = table.getColumn(cid);
        auto all_elems = col.get_all_elems_by_rid();

        //
        // This DOES consider deleted elements.
        for (uint64 idx = 0; idx < table.columns_num_elems; idx++) {
            if ((table.metadata[idx] >> 63) == 0) {
                INT64 value = all_elems[idx];
                sum += value;
            }
        }

        return sum;
    }

public:
    static void delete_keys(IdxTable& table, const vector<string>& keys) {
        table.delete_keys(keys);
    }

    static uint64 SumAllEmemsInCID(IdxTable& table, CID cid) {
        uint64 ret = 0;
        table.test_compute([&]() {
            ret = SumAllEmemsInCID_core<uint64>(table, cid);
        });

        return ret;
    }

    static vector<CID> projectCIDs(const vector<FullyQualifiedCID>& qcids) {
        return Column::projectCIDs(qcids);
    }

    static vector<CID> projectCIDs(const vector<QCID>& qcids) {
        return Column::projectCIDs(qcids);
    }

    static void sort_and_grow(IdxTable& table) {
        table.no_asserts_on_sort = true;
        table.sort_all_columns();
        table.grow();
        table.no_asserts_on_sort = false;
    }

    static vector<RID> nleaf_AND(const vector<RID>& a, const vector<RID>& b) {
        JOBID jobId = 999;

        DtorBHandle handle_a = Buffer::createFromContainer(a);
        DtorBHandle handle_b = Buffer::createFromContainer(b);

        VecConstHandles srcs;
        srcs.push_back(handle_a);
        srcs.push_back(handle_b);

        DtorBHandle handle = IdxTable::nleaf_AND(srcs, jobId);

        vector<RID> ret;
        auto ptr = Buffer::const_data(handle);
        for (uint64 idx = 0; idx < Buffer::num_elems(handle); idx++) {
            ret.push_back(ptr[idx]);
        }

        return std::move(ret);
    }

    static uint64 wslf_string_pattern(IdxTable& table, CID cid, BHandle dest, StringToken stok, uint64 num_rids) {
        auto iter = table.columns_cids_to_idx.find(cid);
        TEST_VALUE(true, iter != table.columns_cids_to_idx.end());
        return table.wslf_string_pattern(iter->second, dest, stok, num_rids);
    }
};
} // namespace memurai::sql

#define ADD_ROWS_MT(TABLE, NUM_ROWS, TH) AddRows(TABLE, NUM_ROWS, TH, __LINE__)

#define ADD_ROWS_MT_MIN(TABLE, NUM_ROWS, TH, ROWIDX) AddRows(TABLE, NUM_ROWS, TH, __LINE__, ROWIDX)

#define ADD_ROWS(TABLE, NUM_ROWS) AddRows(TABLE, NUM_ROWS, 1, __LINE__)

#define ADD_ROWS_W_BASELINE(TABLE, NUM_ROWS) AddRows(TABLE, NUM_ROWS, 1, __LINE__, sumRowIdx, &baseline)

#define ADD_ROWS_MT_W_BASELINE(TABLE, NUM_ROWS, TH) AddRows(TABLE, NUM_ROWS, TH, __LINE__, sumRowIdx, &baseline)

using namespace std;
using namespace memurai;
using namespace memurai::multicore;
using namespace memurai::sql;
using namespace memurai::sql_test;

namespace {

// --------------------------------------------------------------------
// --------------------------------------------------------------------

const string prefix{"unused"};

void test_creation_good() {
    uint32 threadPoolSize = 1;
    // g_addendum = "_" + to_string(num_threads);

    TEST_START;

    // 1 column
    {
        bool got_exception = false;
        const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}};

        try {
            IdxTable temp("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);
        } catch (...) {
            got_exception = true;
        }
        TEST_VALUE(false, got_exception);
    }

    // 2 columns
    {
        bool got_exception = false;
        const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"col2", 102, ColumnType::BIGINT}};

        try {
            IdxTable temp("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);
        } catch (...) {
            got_exception = true;
        }
        TEST_VALUE(false, got_exception);
    }

    // 3 columns
    {
        bool got_exception = false;
        const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

        try {
            IdxTable temp("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);
        } catch (...) {
            got_exception = true;
        }
        TEST_VALUE(false, got_exception);
    }

    // 4 columns
    {
        bool got_exception = false;
        const vector<FullyQualifiedCID> fqcids{
            {"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}, {"104", 104, ColumnType::TEXT}};
        try {
            IdxTable temp("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);
        } catch (...) {
            got_exception = true;
        }
        TEST_VALUE(false, got_exception);
    }

    // 5 columns
    {
        bool got_exception = false;
        const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT},
                                               {"102", 102, ColumnType::BIGINT},
                                               {"103", 103, ColumnType::DATE},
                                               {"104", 104, ColumnType::TEXT},
                                               {"105", 105, ColumnType::TIMESTAMP}};
        try {
            IdxTable temp("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);
        } catch (...) {
            got_exception = true;
        }
        TEST_VALUE(false, got_exception);
    }
}

void test_add_simple() {
    uint32 threadPoolSize = 1;

    TEST_START;

    bool got_exception = false;
    const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

    try {
        IdxTable table("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);

        vector<CID> cids = table.getCIDs();

        // Valid CID
        TEST_VALUE(true, table.addKey("1", vector<CID>{cids[0]}, vector<UINT64>{0x2343453}));
        TEST_VALUE(true, table.addKey("2", vector<CID>{cids[1]}, vector<UINT64>{46}));
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

void test_add_multithreaded(uint32 num_threads) {
    g_addendum = "_" + to_string(num_threads);
    TEST_START;

    uint32 threadPoolSize = 4;

    string tput_unit_measure("Kput/sec");

    bool got_exception = false;
    const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

    try {
        IdxTable table("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);

        vector<CID> cids = table.getCIDs();

        const int elems_per_thread = 1'000'000;

        vector<thread> t_threads;
        std::barrier start_barrier(num_threads + 1);

        atomic<uint64> total_tputK = 0;

        auto adder_lambda = [&](uint64 threadIdx) {
            try {
                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                auto dur = TAKE_TIME {
                    // go!!
                    for (int i = 0; i < elems_per_thread; i++) {
                        string key = to_string(threadIdx) + "_" + to_string(i);
                        table.upinsertKey(key, vector<CID>{cids[0]}, vector<UINT64>{threadIdx});
                    }
                };

                auto durationMS = dur.millis();
                auto tputK = (elems_per_thread / durationMS);
                total_tputK += tputK;
                DEBUG_LOG_PERF(g_test_name << " (thread " << threadIdx << "): " << durationMS << "ms   or throughput: " << tputK << " Krows/sec");
            } catch (...) {
                got_exception = true;
                TEST_VALUE(false, got_exception);
            }
        };

        // create all threads (they will wait on the barrier)
        for (uint32 i = 0; i < num_threads; i++) {
            t_threads.emplace_back(adder_lambda, i);
        }

        // let all threads go!
        start_barrier.arrive_and_wait();

        // Wait for all threads to be done.
        for (uint32 i = 0; i < num_threads; i++) {
            t_threads[i].join();
        }

        LOG_PERF(g_test_name << " aggregate throughput = " << total_tputK << " " << tput_unit_measure);

        // expect the right number of rows in the table
        //
        TEST_VALUE(elems_per_thread * num_threads, table.getNumRowsApprox());
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

void test_delete_simple() {
    uint32 threadPoolSize = 1;
    // g_addendum = "_" + to_string(num_threads);

    TEST_START;

    bool got_exception = false;
    const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

    try {
        IdxTable table("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);

        vector<CID> cids = table.getCIDs();

        {
            table.upinsertKey("1", vector<CID>{cids[0]}, vector<UINT64>{1});
            table.upinsertKey("2", vector<CID>{cids[1]}, vector<UINT64>{46});
            // strings become "0" numbers...
            table.upinsertKey("3", vector<CID>{cids[1]}, vector<UINT64>{0});
            TEST_VALUE(3, table.getNumRowsApprox());
        }

        // Now delete an existing rid
        {
            Test_IdxTable::delete_keys(table, {"2"});
            TEST_VALUE(2, table.getNumRowsApprox());
        }

        // Now delete a NON-existing rid
        {
            Test_IdxTable::delete_keys(table, {"11111"});
            TEST_VALUE(2, table.getNumRowsApprox());
        }
        // Delete remaining rids
        {
            Test_IdxTable::delete_keys(table, {"1", "3"});
            TEST_VALUE(0, table.getNumRowsApprox());
        }
        // Delete more
        {
            Test_IdxTable::delete_keys(table, {"7"});
            TEST_VALUE(0, table.getNumRowsApprox());
        }

        // Add back
        { table.upinsertKey("7", vector<CID>{cids[0]}, vector<UINT64>{143}); }
        { table.upinsertKey("1", vector<CID>{cids[1]}, vector<UINT64>{46546546}); }
        TEST_VALUE(2, table.getNumRowsApprox());
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

void test_delete_multithreaded(uint32 num_threads) {
    g_addendum = "_" + to_string(num_threads);
    TEST_START;

    const uint64 initialCapacity = 100'000;
    ///  ThresholdStrategy: here threshold kicks in at 10K _absolute_ distance from boundary.
    const ThresholdStrategy thresholdStrat = {ThresholdStrategy::StrategyType::Absolute, 10'000};

    ///  When we reach the threshold, then we grow linearly by 10x
    const GrowingStrategy gstrat = {GrowingStrategy::StrategyType::Linear, 10};

    /// This calculates the unsorted capacity related to the sorted one.
    const ThresholdStrategy sstrat = {ThresholdStrategy::StrategyType::Percentage, 10};

    // Default initial capacity of 1M, unsorted buffer of 100K.
    const IdxTable::Config config = {initialCapacity, 1'000, gstrat, sstrat};

    vector<thread> t_threads;
    std::barrier start_barrier(num_threads + 1);

    bool got_exception = false;
    const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

    try {
        IdxTable table("simpletable", prefix, fqcids, config, server, 1000);
        vector<CID> cids = table.getCIDs();

        const uint64 elems_per_thread = 100'000;

        ADD_ROWS_MT(table, elems_per_thread, num_threads);
        TEST_VALUE(elems_per_thread * num_threads, table.getNumRowsApprox());

        const uint64 deletion_per_thread = elems_per_thread;

        auto deleter_lambda = [&](uint64 threadIdx) {
            try {
                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                // go!!
                for (int i = 0; i < deletion_per_thread; i++) {
                    string key = to_string(threadIdx) + "_" + to_string(i);
                    Test_IdxTable::delete_keys(table, {key});
                }
            } catch (...) {
                got_exception = true;
                TEST_VALUE(false, got_exception);
            }
        };

        // create all threads (they will wait on s_start)
        for (uint32 i = 0; i < num_threads; i++) {
            t_threads.emplace_back(deleter_lambda, i);
        }

        start_barrier.arrive_and_wait();

        // Wait for all threads to be done.
        for (uint32 i = 0; i < num_threads; i++) {
            t_threads[i].join();
        }

        // expect the right number of rows in the table
        //
        TEST_VALUE(0, table.getNumRowsApprox());
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

void test_sort_0() {
    TEST_START;

    const uint64 initialCapacity = 100'000;
    ///  ThresholdStrategy: here threshold kicks in at 10K _absolute_ distance from boundary.
    const ThresholdStrategy thresholdStrat = {ThresholdStrategy::StrategyType::Absolute, 10'000};

    ///  When we reach the threshold, then we grow linearly by 10x
    const GrowingStrategy gstrat = {GrowingStrategy::StrategyType::Linear, 10};

    /// This calculates the unsorted capacity related to the sorted one.
    const ThresholdStrategy sstrat = {ThresholdStrategy::StrategyType::Percentage, 10};

    // Default initial capacity of 1M, unsorted buffer of 100K.
    const IdxTable::Config config = {initialCapacity, 1'000, gstrat, sstrat};

    cuint32 sumRowIdx = 1;
    bool got_exception = false;
    const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

    try {
        const uint64 elems_to_add = 1;

        IdxTable table("simpletable", prefix, fqcids, config, server, 1000);
        vector<CID> cids = table.getCIDs();
        vector<uint64> baseline(elems_to_add);

        ADD_ROWS_W_BASELINE(table, elems_to_add);
        TEST_VALUE(elems_to_add, table.getNumRowsApprox());

        Test_IdxTable::sort_and_grow(table);
        TEST_VALUE(elems_to_add, table.getNumRowsApprox());

        auto my_result = Test_IdxTable::SumAllEmemsInCID(table, cids[sumRowIdx]);
        auto baseline_result = std::accumulate(baseline.begin(), baseline.end(), 0ull, std::plus<uint64>());
        TEST_VALUE(baseline_result, my_result);
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

void test_grow_1() {
    TEST_START;

    const uint64 initialCapacity = 1'000'000;
    ///  ThresholdStrategy: here threshold kicks in at 10K _absolute_ distance from boundary.
    const ThresholdStrategy thresholdStrat = {ThresholdStrategy::StrategyType::Absolute, 10'000};

    ///  When we reach the threshold, then we grow linearly by 10x
    const GrowingStrategy gstrat = {GrowingStrategy::StrategyType::Linear, 10};

    /// This calculates the unsorted capacity related to the sorted one.
    const ThresholdStrategy sstrat = {ThresholdStrategy::StrategyType::Literal, 2};

    // Default initial capacity of 1M, unsorted buffer of 100K.
    const IdxTable::Config config = {initialCapacity, 1'000, gstrat, sstrat};

    cuint32 sumRowIdx = 1;
    bool got_exception = false;
    const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

    try {
        IdxTable table("simpletable", prefix, fqcids, config, server, 1000);
        vector<CID> cids = table.getCIDs();
        const uint64 elems_to_add = 2;
        vector<uint64> baseline(elems_to_add);
        ADD_ROWS_W_BASELINE(table, elems_to_add);
        TEST_VALUE(elems_to_add, table.getNumRowsApprox());

        // SUM all elements in both table and baseline, and check
        auto my_result = Test_IdxTable::SumAllEmemsInCID(table, cids[sumRowIdx]);
        auto baseline_result = std::accumulate(baseline.begin(), baseline.end(), 0ull, std::plus<uint64>());
        TEST_VALUE(baseline_result, my_result);

        table.upinsertKey("1", vector<CID>{cids[sumRowIdx]}, vector<UINT64>{0});
        auto my_result2 = Test_IdxTable::SumAllEmemsInCID(table, cids[sumRowIdx]);
        TEST_VALUE(my_result, my_result2);

        table.upinsertKey("2", vector<CID>{cids[sumRowIdx]}, vector<UINT64>{12});
        auto my_result3 = Test_IdxTable::SumAllEmemsInCID(table, cids[sumRowIdx]);
        TEST_VALUE(my_result + 12, my_result3);

        table.upinsertKey("3", vector<CID>{cids[sumRowIdx]}, vector<UINT64>{0});
        auto my_result4 = Test_IdxTable::SumAllEmemsInCID(table, cids[sumRowIdx]);
        TEST_VALUE(my_result + 12, my_result4);
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

// This test sets the linear increment at 2X, but we add 3X of initial capacity in one shot.
//
void test_grow_2() {
    TEST_START;

    const uint64 initialCapacity = 1'000'000;
    ///  ThresholdStrategy: here threshold kicks in at 10K _absolute_ distance from boundary.
    const ThresholdStrategy thresholdStrat = {ThresholdStrategy::StrategyType::Absolute, 10'000};

    ///  When we reach the threshold, then we grow linearly by 10x
    const GrowingStrategy gstrat = {GrowingStrategy::StrategyType::Linear, 10};

    /// This calculates the unsorted capacity related to the sorted one.
    const ThresholdStrategy sstrat = {ThresholdStrategy::StrategyType::Percentage, 10};

    // Default initial capacity of 1M, unsorted buffer of 100K.
    const IdxTable::Config config = {initialCapacity, 1'000, gstrat, sstrat};

    bool got_exception = false;
    cuint32 sumRowIdx = 1;
    const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

    try {
        IdxTable table("simpletable", prefix, fqcids, config, server, 1000);
        vector<CID> cids = table.getCIDs();
        const uint64 elems_to_add = initialCapacity * 3;
        vector<uint64> baseline(elems_to_add);
        ADD_ROWS_W_BASELINE(table, elems_to_add);
        TEST_VALUE(elems_to_add, table.getNumRowsApprox());

        auto my_result = Test_IdxTable::SumAllEmemsInCID(table, cids[sumRowIdx]);
        auto baseline_result = std::accumulate(baseline.begin(), baseline.end(), 0ull, std::plus<uint64>());
        TEST_VALUE(baseline_result, my_result);
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

void test_grow_1_multithreaded(uint32 num_threads) {
    g_addendum = "_" + to_string(num_threads);
    TEST_START;

    const uint64 initialCapacity = 100'000;
    ///  ThresholdStrategy: here threshold kicks in at 10K _absolute_ distance from boundary.
    const ThresholdStrategy thresholdStrat = {ThresholdStrategy::StrategyType::Absolute, 10'000};

    ///  When we reach the threshold, then we grow linearly by 10x
    const GrowingStrategy gstrat = {GrowingStrategy::StrategyType::Linear, 10};

    /// This calculates the unsorted capacity related to the sorted one.
    const ThresholdStrategy sstrat = {ThresholdStrategy::StrategyType::Percentage, 10};

    // Default initial capacity of 1M, unsorted buffer of 100K.
    const IdxTable::Config config = {initialCapacity, 1'000, gstrat, sstrat};

    uint32 threadPoolSize = num_threads;

    bool got_exception = false;
    cuint32 sumRowIdx = 1;
    const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

    try {
        IdxTable table("simpletable", prefix, fqcids, config, server, 1000);
        vector<CID> cids = table.getCIDs();
        const uint64 elems_to_add = initialCapacity * 2;
        vector<uint64> baseline(elems_to_add * num_threads);
        ADD_ROWS_MT_W_BASELINE(table, elems_to_add, num_threads);
        TEST_VALUE(elems_to_add * num_threads, table.getNumRowsApprox());

        uint64 baseline_result;
        auto my_result = Test_IdxTable::SumAllEmemsInCID(table, cids[sumRowIdx]);
        thread t([&]() {
            baseline_result = std::accumulate(baseline.begin(), baseline.end(), 0ull, std::plus<uint64>());
        });
        t.join();

        TEST_VALUE(baseline_result, my_result);
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

// This is like delete_multithreaded, just adds and delete happen concurrently
//
void test_mixed_1(uint32 num_threads) {
    g_addendum = "_" + to_string(num_threads);
    TEST_START;

    const uint64 initialCapacity = 1'000'000;
    ///  ThresholdStrategy: here threshold kicks in at 10K _absolute_ distance from boundary.
    const ThresholdStrategy thresholdStrat = {ThresholdStrategy::StrategyType::Absolute, 10'000};

    ///  When we reach the threshold, then we grow linearly by 10x
    const GrowingStrategy gstrat = {GrowingStrategy::StrategyType::Linear, 10};

    /// This calculates the unsorted capacity related to the sorted one.
    const ThresholdStrategy sstrat = {ThresholdStrategy::StrategyType::Percentage, 10};

    // Default initial capacity of 1M, unsorted buffer of 100K.
    const IdxTable::Config config = {initialCapacity, 1'000, gstrat, sstrat};

    uint32 threadPoolSize = 4;

    vector<thread> t_threads;
    std::barrier start_barrier(num_threads + 2);

    bool got_exception = false;
    const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

    try {
        IdxTable table("simpletable", prefix, fqcids, config, server, 1000);
        vector<CID> cids = table.getCIDs();

        const uint64 elems_per_thread = 200'000;

        auto adder_driver = [&]() {
            // wait for all threads to be set up
            start_barrier.arrive_and_wait();

            ADD_ROWS_MT(table, elems_per_thread, num_threads);
        };

        const uint64 deletion_per_thread = elems_per_thread;

        auto one_modify_delete_lambda = [&](uint64 threadIdx) {
            try {
                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                // go!!
                for (int i = 0; i < deletion_per_thread; i++) {
                    string key = to_string(threadIdx) + "_" + to_string(i);

                    if (i % 2) {

                        // We cannot expect this to go through, since the element might not be there yet.
                        // That said, it should not crash
                        Test_IdxTable::delete_keys(table, {key});
                    } else {
                        table.upinsertKey(key, vector<CID>{cids[0]}, vector<UINT64>{UINT64(i)});
                    }
                }
            } catch (...) {
                got_exception = true;
                TEST_VALUE(false, got_exception);
            }
        };

        // create all threads (they will wait on s_start)
        for (uint32 i = 0; i < num_threads; i++) {
            t_threads.emplace_back(one_modify_delete_lambda, i);
        }
        t_threads.emplace_back(adder_driver);

        start_barrier.arrive_and_wait();

        // Wait for all threads to be done.
        for (auto& t : t_threads) {
            t.join();
        }

        // Test done... we cannot expect the number of elements at this point.
        // We just want to make sure that we do not throw.
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

typedef IdxTable::QueryInterface QueryInterface;
typedef WhereNumericLeafFilter WhereLeafFilter;
typedef IdxTable::WhereValue WhereValue;

typedef function<bool(VecCID, IdxTable&)> TestProducerType;

enum TableTestType { AllUnsorted, AllSorted, AllSortedTwice, SomeSortedSomeUnsorted };

void test_one_test(const TestProducerType& testProducer, TableTestType ttype) {
    TEST_START;
    const uint64 initialCapacity = 100'000;
    ///  ThresholdStrategy: here threshold kicks in at 10K _absolute_ distance from boundary.
    const ThresholdStrategy thresholdStrat = {ThresholdStrategy::StrategyType::Absolute, 10'000};

    ///  When we reach the threshold, then we grow linearly by 10x
    const GrowingStrategy gstrat = {GrowingStrategy::StrategyType::Linear, 10};

    /// This calculates the unsorted capacity related to the sorted one.
    const ThresholdStrategy sstrat = {ThresholdStrategy::StrategyType::Percentage, 10};

    // Default initial capacity of 1M, unsorted buffer of 100K.
    const IdxTable::Config config = {initialCapacity, 1'000, gstrat, sstrat};

    vector<thread> t_threads;
    atomic<bool> s_finish{false};

    bool got_exception = false;
    const vector<FullyQualifiedCID> fqcids{{"colname", 110, ColumnType::BIGINT},
                                           {"102", 102, ColumnType::BIGINT},
                                           {"101", 101, ColumnType::TEXT},
                                           {"103", 103, ColumnType::DATE},
                                           {"105", 105, ColumnType::TEXT}};

    try {
        IdxTable table("simpletable", prefix, fqcids, config, server, 1000);
        VecCID cids = Test_IdxTable::projectCIDs(fqcids);
        cuint32 num_adders = 5;
        cuint64 each_time = config.columnCapacity - config.sortStrategy.parameter;
        cuint64 elems_per_adder_all_unsorted = each_time / num_adders - 10;
        cuint64 elems_per_adder_all_sorted = (each_time / num_adders);
        cuint64 elems_per_adder_all_sorted_twice = 2 * (each_time / num_adders) + 1;
        cuint64 elems_per_adder_mixed = 2 * (each_time / num_adders) + config.columnCapacity / (num_adders / 2);
        uint64 elems_to_add;
        if (ttype == TableTestType::AllUnsorted) {
            elems_to_add = elems_per_adder_all_unsorted;
        } else if (ttype == TableTestType::AllSorted) {
            elems_to_add = elems_per_adder_all_sorted;
        } else if (ttype == TableTestType::AllSortedTwice) {
            elems_to_add = elems_per_adder_all_sorted_twice;
        } else {
            elems_to_add = elems_per_adder_mixed;
        }
        ADD_ROWS_MT_MIN(table, elems_to_add, num_adders, 100'000);

        auto ret = testProducer(cids, table);
        TEST_VALUE(true, ret);
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

bool SomeRawTests(VecCID cids, IdxTable& table) {
    bool got_exception = false;
    bool ret = false;
    WhereLeafFilter op = WhereLeafFilter::EQ;
    JOBID jobId = 1;
    CID cidProj = cids[1];
    CID cidFilter = cids[0];

    try {
        QueryInterface queryInterface = table.getQueryInterface();

        // test EQ, no data expected
        {
            // Create a Publisher that dumps data into a vector<vector<string>>
            PublisherV2DStrings publisher;

            Token token("idxtable_test", 123, 1);
            DtorBHandle filtered = queryInterface.leafFilterInt(WhereLeafFilter::EQ, 123, cidFilter, jobId, ALLRIDS);
            queryInterface.project(filtered, VecCID{cidProj}, &token, &publisher);
            token.join();

            // Expect no exception raised
            TEST_VALUE(true, token.isGood());
            // Expect no value found
            TEST_VALUE(0, Buffer::num_elems(filtered));
            TEST_VALUE(0, publisher.get_data().size());
        }

        // test EQ, we expect something
        {
            // Create a Publisher that dumps data into a vector<vector<string>>
            PublisherV2DStrings publisher;

            Token token("idxtable_test", 234, 1);
            table.upinsertKey("1", VecCID{cids[0], cids[1]}, vector<UINT64>{234000000ull, 0xFF1});
            cuint64 num_rids = table.getNumRowsApprox();

            DtorBHandle filtered = queryInterface.leafFilterInt(WhereLeafFilter::EQ, 234000000ull, cids[0], jobId, num_rids);
            // Expect one value found
            TEST_VALUE(1, Buffer::num_elems(filtered));

            table.upinsertKey("2", VecCID{cids[0], cids[1]}, vector<UINT64>{234000000ull, 0xFF1});
            // Note that we are still passing num_rows, mimicing the behavior of the Database api
            // This query should NOT catch key "2" because it is *after* num_rids.
            DtorBHandle filtered2 = queryInterface.leafFilterInt(WhereLeafFilter::EQ, 234000000ull, cids[0], jobId, num_rids);
            // Expect one value found
            TEST_VALUE(1, Buffer::num_elems(filtered2));

            queryInterface.project(filtered, VecCID{cids[1]}, &token, &publisher);
            token.join();

            // Expect no exception raised
            TEST_VALUE(true, token.isGood());
            // Expect one value found
            TEST_VALUE(1, publisher.get_data().size());
            if (publisher.get_data().size() == 1) {
                TEST_VALUE(1, publisher.get_data()[0].size());
                if (publisher.get_data()[0].size() == 1) {
                    TEST_VALUE("4081", *publisher.get_data()[0][0]);
                }
            }
        }

        // test EQ, we expect something but first we delete some
        {
            // Create a Publisher that dumps data into a vector<vector<string>>
            PublisherV2DStrings publisher;

            Token token("idxtable_test", 234, 1);
            cuint64 num_rows_before = table.getNumRowsApprox();
            table.delete_keys({"1"});
            TEST_VALUE(num_rows_before, table.getNumRowsApprox() + 1);

            DtorBHandle filtered = queryInterface.leafFilterInt(WhereLeafFilter::EQ, 234000000ull, cids[0], jobId, ALLRIDS);
            // Expect one value found
            TEST_VALUE(1, Buffer::num_elems(filtered));

            table.delete_keys({"2"});

            // Note that we are still passing num_rows, mimicing the behavior of the Database api
            DtorBHandle filtered2 = queryInterface.leafFilterInt(WhereLeafFilter::EQ, 234000000ull, cids[0], jobId, ALLRIDS);
            // Expect one value found
            TEST_VALUE(0, Buffer::num_elems(filtered2));

            // put things back as they were
            table.upinsertKey("1", VecCID{cids[0], cids[1]}, vector<UINT64>{234000000ull, 0xFF1});
            table.upinsertKey("2", VecCID{cids[0], cids[1]}, vector<UINT64>{234000000ull, 0xFF1});
        }

        // test GT and LTE, we expect something
        try {
            auto num_elems = table.getNumRowsApprox();
            DtorBHandle filtered_gt = queryInterface.leafFilterInt(WhereLeafFilter::GT, 50'000, cids[0], jobId, ALLRIDS);
            DtorBHandle filtered_lte = queryInterface.leafFilterInt(WhereLeafFilter::LE, 50'000, cids[0], jobId, ALLRIDS);
            auto num_gt = Buffer::num_elems(filtered_gt);
            auto num_lte = Buffer::num_elems(filtered_lte);
            TEST_VALUE(num_elems, num_gt + num_lte);
        } catch (...) {
            // Expect no exception raised
            TEST_VALUE(0, 1);
        }

        // test GTE and LT, we expect something
        try {
            auto num_elems = table.getNumRowsApprox();
            DtorBHandle filtered_gte = queryInterface.leafFilterInt(WhereLeafFilter::GE, 50'000, cids[0], jobId, ALLRIDS);
            DtorBHandle filtered_lt = queryInterface.leafFilterInt(WhereLeafFilter::LT, 50'000, cids[0], jobId, ALLRIDS);
            auto num_gte = Buffer::num_elems(filtered_gte);
            auto num_lt = Buffer::num_elems(filtered_lt);
            TEST_VALUE(num_elems, num_gte + num_lt);
        } catch (...) {
            // Expect no exception raised
            TEST_VALUE(0, 1);
        }

        // ******************************************************************************************************************
        // STRING TESTS
        // ******************************************************************************************************************

        // Small String : not found
        {
            // Create a Publisher that dumps data into a vector<vector<string>>
            PublisherV2DStrings publisher;

            WhereStringFilter op = WhereStringFilter::EQ_CASE_SENSITIVE;
            Token token("idxtable_test", 234, 1);
            DtorBHandle filtered = queryInterface.leafFilterString(op, table.tokenizeString("betto"), cids[2], jobId, ALLRIDS);
            // Expect no value found
            TEST_VALUE(0, Buffer::num_elems(filtered));
            queryInterface.project(filtered, VecCID{cids[1]}, &token, &publisher);
            token.join();

            // Expect no exception raised
            TEST_VALUE(true, token.isGood());
            // Expect no value found
            TEST_VALUE(0, publisher.get_data().size());
        }

        // Small String : found
        {
            // Create a Publisher that dumps data into a vector<vector<string>>
            PublisherV2DStrings publisher;

            Token token("idxtable_test", 234, 1);
            string mystring("betto");
            StringToken betto_token = table.tokenizeString(mystring);
            WhereStringFilter op = WhereStringFilter::EQ_CASE_SENSITIVE;

            auto betto_cid = cids[2];
            auto proj_cid = cids[1];

            table.upinsertKey("11", vector<CID>{betto_cid, proj_cid}, vector<UINT64>{betto_token.getFullToken(), 0xFF1});
            DtorBHandle filtered = queryInterface.leafFilterString(op, betto_token, cids[2], jobId, ALLRIDS);
            queryInterface.project(filtered, VecCID{cidProj}, &token, &publisher);
            token.join();
            // Expect one value found
            TEST_VALUE(1, Buffer::num_elems(filtered));
            TEST_VALUE(1, publisher.get_data().size());
            if (publisher.get_data().size() == 1) {
                TEST_VALUE(1, publisher.get_data()[0].size());
                if (publisher.get_data()[0].size() == 1) {
                    TEST_VALUE("4081", *publisher.get_data()[0][0]);
                }
            }
        }

        // Normal String : not found
        {
            // Create a Publisher that dumps data into a vector<vector<string>>
            PublisherV2DStrings publisher;

            WhereStringFilter op = WhereStringFilter::EQ_CASE_SENSITIVE;
            Token token("idxtable_test", 234, 1);
            DtorBHandle filtered = queryInterface.leafFilterString(op, table.tokenizeString("1234567betto"), cids[2], jobId, ALLRIDS);
            queryInterface.project(filtered, vector<CID>{cidProj}, &token, &publisher);
            token.join();

            // Expect no exception raised
            TEST_VALUE(true, token.isGood());
            // Expect no value found
            TEST_VALUE(0, Buffer::num_elems(filtered));
            TEST_VALUE(0, publisher.get_data().size());
        }

        // Normal String : found
        {
            // Create a Publisher that dumps data into a vector<vector<string>>
            PublisherV2DStrings publisher;

            Token token("idxtable_test", 234, 1);
            string mystring("1234567betto");
            StringToken betto_token = table.tokenizeString(mystring);
            WhereStringFilter op = WhereStringFilter::EQ_CASE_SENSITIVE;

            auto betto_cid = cids[2];
            auto proj_cid = cids[1];

            table.upinsertKey("12", vector<CID>{betto_cid, proj_cid}, vector<UINT64>{betto_token.getFullToken(), 0xFF1});
            DtorBHandle filtered = queryInterface.leafFilterString(op, betto_token, cids[2], jobId, ALLRIDS);
            queryInterface.project(filtered, vector<CID>{cidProj}, &token, &publisher);
            token.join();
            // Expect one value found
            TEST_VALUE(1, Buffer::num_elems(filtered));
            if (publisher.get_data().size() == 1) {
                TEST_VALUE(1, publisher.get_data()[0].size());
                if (publisher.get_data()[0].size() == 1) {
                    TEST_VALUE("4081", *publisher.get_data()[0][0]);
                }
            }
        }

        // Normal String : not found
        {
            // Create a Publisher that dumps data into a vector<vector<string>>
            PublisherV2DStrings publisher;

            WhereStringFilter op = WhereStringFilter::EQ_CASE_SENSITIVE;
            Token token("idxtable_test", 234, 1);
            string mystring(5000, '1');
            DtorBHandle filtered = queryInterface.leafFilterString(op, table.tokenizeString(mystring), cids[2], jobId, ALLRIDS);
            queryInterface.project(filtered, vector<CID>{cidProj}, &token, &publisher);
            token.join();

            // Expect no exception raised
            TEST_VALUE(true, token.isGood());
            // Expect no value found
            TEST_VALUE(0, Buffer::num_elems(filtered));
            TEST_VALUE(0, publisher.get_data().size());
        }

        // Big String : found
        {
            // Create a Publisher that dumps data into a vector<vector<string>>
            PublisherV2DStrings publisher;

            Token token("idxtable_test", 234, 1);
            string mystring(5000, '1');
            StringToken betto_token = table.tokenizeString(mystring);
            WhereStringFilter op = WhereStringFilter::EQ_CASE_SENSITIVE;

            auto betto_cid = cids[2];
            auto proj_cid = cids[1];

            table.upinsertKey("21", vector<CID>{betto_cid, proj_cid}, vector<UINT64>{betto_token.getFullToken(), 0xFF1});
            DtorBHandle filtered = queryInterface.leafFilterString(op, betto_token, cids[2], jobId, ALLRIDS);
            queryInterface.project(filtered, vector<CID>{cidProj}, &token, &publisher);
            token.join();
            // Expect one value found
            TEST_VALUE(1, Buffer::num_elems(filtered));
            if (publisher.get_data().size() == 1) {
                TEST_VALUE(1, publisher.get_data()[0].size());
                if (publisher.get_data()[0].size() == 1) {
                    TEST_VALUE("4081", *publisher.get_data()[0][0]);
                }
            }
        }
    } catch (...) {
        got_exception = true;
    }

    TEST_VALUE(0, Buffer::get_num_buffers());

    return true;
}

void test_raw_filters() {
    test_one_test(SomeRawTests, TableTestType::AllSortedTwice);
    test_one_test(SomeRawTests, TableTestType::AllSorted);
    test_one_test(SomeRawTests, TableTestType::AllUnsorted);
    test_one_test(SomeRawTests, TableTestType::SomeSortedSomeUnsorted);
}

bool SomeBoolTests(VecCID cids, IdxTable& table) {
    bool got_exception = false;
    bool ret = false;
    CID cidFilter = cids[0];
    CID cidProj = cids[1];
    WhereLeafFilter op = WhereLeafFilter::EQ;
    JOBID jobId = 1;
    QueryInterface queryInterface = table.getQueryInterface();

    try {
        table.upinsertKey("34", vector<CID>{cids[0], cids[1]}, vector<UINT64>{234000000ull, 0xFF1});

        {
            // Create a Publisher that dumps data into a vector<vector<string>>
            PublisherV2DStrings publisher;

            DtorBHandle filteredA = queryInterface.leafFilterInt(op, 234000000ull, cids[0], jobId, ALLRIDS);
            // Expect one value found
            TEST_VALUE(1, Buffer::num_elems(filteredA));

            DtorBHandle filteredB = queryInterface.leafFilterInt(op, 234000001ull, cids[0], jobId, ALLRIDS);
            // Expect no value found
            TEST_VALUE(0, Buffer::num_elems(filteredB));

            DtorBHandle filteredAND = queryInterface.ridFilter(WhereRIDFilter::AND, VecConstHandles{filteredA, filteredB}, 2);
            // Expect no value found
            TEST_VALUE(0, Buffer::num_elems(filteredAND));

            DtorBHandle filteredOR = queryInterface.ridFilter(WhereRIDFilter::OR, VecConstHandles{filteredA, filteredB}, 2);
            // Expect one value found
            TEST_VALUE(1, Buffer::num_elems(filteredOR));

            Token token("idxtable_test", 123, 1);
            queryInterface.project(filteredOR, vector<CID>{cidProj}, &token, &publisher);
            token.join();

            // Expect no exception raised
            TEST_VALUE(true, token.isGood());
            // Expect no value found
            if (publisher.get_data().size() == 1) {
                TEST_VALUE(1, publisher.get_data()[0].size());
                if (publisher.get_data()[0].size() == 1) {
                    TEST_VALUE("4081", *publisher.get_data()[0][0]);
                }
            }
        }

    } catch (...) {
        got_exception = true;
    }

    TEST_VALUE(0, Buffer::get_num_buffers());

    return true;
}

void test_bool_filters() {
    test_one_test(SomeBoolTests, TableTestType::AllSortedTwice);
    test_one_test(SomeBoolTests, TableTestType::AllSorted);
    test_one_test(SomeBoolTests, TableTestType::AllUnsorted);
    test_one_test(SomeBoolTests, TableTestType::SomeSortedSomeUnsorted);
}

void test_nleaf_and() {
    {
        const vector<RID> left = {0, 1, 4, 9, 25, 50, 1000};
        const vector<RID> right = {1};
        auto ret = Test_IdxTable::nleaf_AND(left, right);
        TEST_VALUE(1, ret.size());
        TEST_VALUE(true, ((1 == ret.size()) && (ret[0] == 1)));
    }
    {
        const vector<RID> left = {0, 1, 4, 9, 25, 50, 1000};
        const vector<RID> right = {};
        auto ret = Test_IdxTable::nleaf_AND(left, right);
        TEST_VALUE(0, ret.size());
    }
    {
        const vector<RID> left = {0, 1, 4, 9, 25, 50, 1000};
        const vector<RID> right = {24};
        auto ret = Test_IdxTable::nleaf_AND(left, right);
        TEST_VALUE(0, ret.size());
    }
    {
        const vector<RID> left = {0, 1, 4, 9, 25, 50, 1000};
        const vector<RID> right = {25, 98, 98};
        auto ret = Test_IdxTable::nleaf_AND(left, right);
        TEST_VALUE(1, ret.size());
        TEST_VALUE(true, ((1 == ret.size()) && (ret[0] == 25)));
    }
    {
        const vector<RID> left = {0, 1, 4, 9, 25, 50, 1000};
        const vector<RID> right = {4};
        auto ret = Test_IdxTable::nleaf_AND(left, right);
        TEST_VALUE(1, ret.size());
        TEST_VALUE(true, ((1 == ret.size()) && (ret[0] == 4)));
    }
    {
        const vector<RID> left = {1};
        const vector<RID> right = {0, 1, 4, 9, 25, 50, 1000};
        auto ret = Test_IdxTable::nleaf_AND(left, right);
        TEST_VALUE(1, ret.size());
        TEST_VALUE(true, ((1 == ret.size()) && (ret[0] == 1)));
    }
    {
        const vector<RID> left = {4};
        const vector<RID> right = {0, 1, 4, 9, 25, 50, 1000};
        auto ret = Test_IdxTable::nleaf_AND(left, right);
        TEST_VALUE(1, ret.size());
        TEST_VALUE(true, ((1 == ret.size()) && (ret[0] == 4)));
    }
}

void test_get_info() {
    uint32 threadPoolSize = 1;

    TEST_START;

    // 1 column
    {
        bool got_exception = false;
        const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}};

        tuple<string, vector<FullyQualifiedCID>> expected_response = {"1000", fqcids};

        try {
            IdxTable temp("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);
            TEST_VALUE(get<0>(expected_response), get<0>(temp.getInfo()));
            TEST_VALUE(get<1>(expected_response).size(), get<1>(temp.getInfo()).size());
        } catch (...) {
            got_exception = true;
        }
        TEST_VALUE(false, got_exception);
    }

    // 2 columns
    {
        bool got_exception = false;
        const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"col2", 102, ColumnType::BIGINT}};

        tuple<string, vector<FullyQualifiedCID>> expected_response = {"1000", fqcids};

        try {
            IdxTable temp("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);
            TEST_VALUE(get<0>(expected_response), get<0>(temp.getInfo()));
            TEST_VALUE(get<1>(expected_response).size(), get<1>(temp.getInfo()).size());
        } catch (...) {
            got_exception = true;
        }
        TEST_VALUE(false, got_exception);
    }

    // 3 columns
    {
        bool got_exception = false;
        const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

        tuple<string, vector<FullyQualifiedCID>> expected_response = {"1000", fqcids};

        try {
            IdxTable temp("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);
            TEST_VALUE(get<0>(expected_response), get<0>(temp.getInfo()));
            TEST_VALUE(get<1>(expected_response).size(), get<1>(temp.getInfo()).size());
        } catch (...) {
            got_exception = true;
        }
        TEST_VALUE(false, got_exception);
    }

    // 4 columns
    {
        bool got_exception = false;
        const vector<FullyQualifiedCID> fqcids{
            {"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}, {"104", 104, ColumnType::TEXT}};

        tuple<string, vector<FullyQualifiedCID>> expected_response = {"1000", fqcids};

        try {
            IdxTable temp("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);
            TEST_VALUE(get<0>(expected_response), get<0>(temp.getInfo()));
            TEST_VALUE(get<1>(expected_response).size(), get<1>(temp.getInfo()).size());
        } catch (...) {
            got_exception = true;
        }
        TEST_VALUE(false, got_exception);
    }

    // 5 columns
    {
        bool got_exception = false;
        const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT},
                                               {"102", 102, ColumnType::BIGINT},
                                               {"103", 103, ColumnType::DATE},
                                               {"104", 104, ColumnType::TEXT},
                                               {"105", 105, ColumnType::TIMESTAMP}};

        tuple<string, vector<FullyQualifiedCID>> expected_response = {"1000", fqcids};

        try {
            IdxTable temp("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);
            TEST_VALUE(get<0>(expected_response), get<0>(temp.getInfo()));
            TEST_VALUE(get<1>(expected_response).size(), get<1>(temp.getInfo()).size());
        } catch (...) {
            got_exception = true;
        }
        TEST_VALUE(false, got_exception);
    }

    // 1 column - vary column name, cid and type
    for (int type = 0; type <= static_cast<int>(ColumnType::LAST); type++) {
        bool got_exception = false;
        const vector<FullyQualifiedCID> fqcids{{"colname" + std::to_string(type), 100 + type, static_cast<ColumnType>(type)}};

        tuple<string, vector<FullyQualifiedCID>> expected_response = {"1000", fqcids};

        try {
            IdxTable temp("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);
            TEST_VALUE(get<0>(expected_response), get<0>(temp.getInfo()));
            auto col_info = get<1>(temp.getInfo());
            TEST_VALUE(get<1>(expected_response).size(), col_info.size());
            TEST_VALUE(get<0>(fqcids[0]), get<0>(col_info[0]));
            TEST_VALUE(get<1>(fqcids[0]), get<1>(col_info[0]));
            TEST_VALUE(static_cast<int>(get<2>(fqcids[0])), static_cast<int>(get<2>(col_info[0])));
        } catch (...) {
            got_exception = true;
        }
        TEST_VALUE(false, got_exception);
    }
}

void test_stop() {
    TEST_START;
    const uint64 initialCapacity = 100'000;
    ///  ThresholdStrategy: here threshold kicks in at 10K _absolute_ distance from boundary.
    const ThresholdStrategy thresholdStrat = {ThresholdStrategy::StrategyType::Absolute, 10'000};

    ///  When we reach the threshold, then we grow linearly by 10x
    const GrowingStrategy gstrat = {GrowingStrategy::StrategyType::Linear, 10};

    /// This calculates the unsorted capacity related to the sorted one.
    const ThresholdStrategy sstrat = {ThresholdStrategy::StrategyType::Percentage, 10};

    // Default initial capacity of 1M, unsorted buffer of 100K.
    const IdxTable::Config config = {initialCapacity, 1'000, gstrat, sstrat};
    const vector<FullyQualifiedCID> fqcids{{"colname", 110, ColumnType::BIGINT},
                                           {"102", 102, ColumnType::BIGINT},
                                           {"101", 101, ColumnType::TEXT},
                                           {"103", 103, ColumnType::DATE},
                                           {"105", 105, ColumnType::TEXT}};

    // Test table stop while adding rows
    //
    try {
        IdxTable table("simpletable", prefix, fqcids, config, server, 1000);
        cuint64 elems_to_add = 100'000;
        std::barrier start_barrier(2);

        thread t([&]() {
            // give it a little time to stop
            start_barrier.arrive_and_wait();
            this_thread::sleep_for(chrono::milliseconds(100));

            try {
                for (uint64 i = 0; i < elems_to_add; i++) {
                    string key = to_string(i);
                    TEST_VALUE(false, table.addKey(key, {110}, {i}));
                }
            } catch (memurai::exception& ex) {
                // it's ok to get an internal error adding rows while it's shutting down
                TEST_VALUE(uint64(ErrorCode::InternalError), ex.code);
            } catch (...) {
                TEST_VALUE(0, 1);
            }
        });

        start_barrier.arrive_and_wait();
        table.stop();
        table.join();
        t.join();

        bool gotException = false;

        try {
            TEST_VALUE(false, table.addKey("110", {110}, {1}));
        } catch (memurai::exception& ex) {
            // it's ok to get an internal error adding rows while it's shutting down
            TEST_VALUE(uint64(ErrorCode::InternalError), ex.code);
        } catch (...) {
            TEST_VALUE(0, 1);
        }
        TEST_VALUE(0, Buffer::get_num_buffers());
    } catch (...) {
        TEST_VALUE(false, true);
    }
}

void test_concurrent_adds_and_queries_and_modifies(uint32 num_threads) {
    g_addendum = "_" + to_string(num_threads);
    TEST_START;

    uint32 threadPoolSize = 4;

    string tput_unit_measure("Kput/sec");

    bool got_exception = false;
    const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

    try {
        IdxTable table("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);

        vector<CID> cids = table.getCIDs();
        cuint64 init_elems_per_thread = 100'000;
        ADD_ROWS_MT(table, init_elems_per_thread, num_threads);

        const uint64 elems_per_thread = 1'000'000;
        const uint64 total_elems = elems_per_thread * num_threads;

        vector<thread> t_threads;
        std::barrier start_barrier(2 * num_threads + 2);

        atomic<bool> s_stopping{false};

        auto adder_lambda = [&](uint64 idx) {
            try {
                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                // go!!
                for (int i = 0; i < elems_per_thread; i++) {
                    table.upinsertKey(to_string(i), vector<CID>{cids[0]}, vector<UINT64>{idx});

                    if (s_stopping.load())
                        return;
                }
            } catch (...) {
                got_exception = true;
                TEST_VALUE(false, got_exception);
            }
        };

        auto modifier_lambda = [&](uint64 idx) {
            try {
                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                // go!!
                for (int i = 0; i < elems_per_thread; i++) {
                    auto threadIdx = std::rand() % num_threads;
                    string key = to_string(threadIdx) + "_" + to_string(i);
                    table.upinsertKey(key, vector<CID>{cids[0]}, vector<UINT64>{idx + i});

                    if (s_stopping.load())
                        return;
                }
            } catch (...) {
                got_exception = true;
                TEST_VALUE(false, got_exception);
            }
        };

        auto select_lambda = [&]() {
            try {

                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                for (int i = 0; i < 100; i++) {
                    table.compute_lock();
                    std::this_thread::sleep_for(chrono::milliseconds(2));
                    table.compute_unlock();
                }
            } catch (...) {
                got_exception = true;
                TEST_VALUE(false, got_exception);
            }
        };

        t_threads.emplace_back(select_lambda);

        // create all threads (they will wait on the barrier)
        for (uint32 i = 0; i < num_threads; i++) {
            t_threads.emplace_back(adder_lambda, i);
        }

        for (uint32 i = 0; i < num_threads; i++) {
            t_threads.emplace_back(modifier_lambda, i);
        }

        // wait for all threads to be set up
        start_barrier.arrive_and_wait();

        std::this_thread::sleep_for(std::chrono::seconds(10));
        s_stopping.store(true);

        // Wait for all threads to be done.
        for (uint32 i = 0; i < 2 * num_threads + 1; i++) {
            t_threads[i].join();
        }

        // expect the right number of rows in the table
        //
        TEST_VALUE(0, Buffer::get_num_buffers());
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

void test_concurrent_adds_and_queries(uint32 num_threads) {
    g_addendum = "_" + to_string(num_threads);
    TEST_START;

    uint32 threadPoolSize = 4;

    string tput_unit_measure("Kput/sec");

    bool got_exception = false;
    const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

    try {
        IdxTable table("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);

        vector<CID> cids = table.getCIDs();

        const int elems_per_thread = 1'000'000;

        vector<thread> t_threads;
        std::barrier start_barrier(num_threads + 1);

        atomic<bool> s_stopping{false};

        auto adder_lambda = [&](uint64 idx) {
            try {
                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                // go!!
                for (int i = 0; i < elems_per_thread; i++) {
                    string key = to_string(idx) + "_" + to_string(i);
                    table.upinsertKey(key, vector<CID>{cids[0]}, vector<UINT64>{idx});

                    if (s_stopping.load())
                        return;
                }
            } catch (...) {
                got_exception = true;
                TEST_VALUE(false, got_exception);
            }
        };

        auto select_lambda = [&]() {
            try {
                table.compute_lock();

                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                while (!s_stopping.load())
                    ;

                table.compute_lock();
            } catch (...) {
                got_exception = true;
                TEST_VALUE(false, got_exception);
            }
        };

        t_threads.emplace_back(select_lambda);

        // create all threads (they will wait on the barrier)
        for (uint32 i = 0; i < num_threads - 1; i++) {
            t_threads.emplace_back(adder_lambda, 0);
        }

        // wait for all threads to be set up
        start_barrier.arrive_and_wait();

        // now verify that queries are running concurrently
        bool success = false;
        uint64 last_addrow_cnt = table.getNextRid();

        for (uint64 times = 0; times < 1000000000; times++) {
            auto metrics = table.getMetrics();

            auto active_computes = metrics["active computes"];
            auto addrow_cnt = metrics["addrow cnt"];

            if ((times != 0) && (addrow_cnt > last_addrow_cnt) && (active_computes > 0)) {
                success = true;
                s_stopping.store(true, std::memory_order_release);
                break;
            }

            last_addrow_cnt = addrow_cnt;
        }

        // Wait for all threads to be done.
        for (uint32 i = 0; i < num_threads; i++) {
            t_threads[i].join();
        }

        // expect the right number of rows in the table
        //
        TEST_VALUE(true, success);
        TEST_VALUE(0, Buffer::get_num_buffers());
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

void test_table_lock() {
    TEST_START;

    const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

    try {
        IdxTable table("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);
        {
            shared_table_lock<IdxTable> guard{&table};

            TEST_VALUE(1, table.getMetrics()["active computes"]);
        }

        TEST_VALUE(0, table.getMetrics()["active computes"]);
    } catch (...) {
        TEST_VALUE(1, 0);
    }
}

// This is testing IdxTable::wslf_string_pattern

void test_pattern_small_strings() {
    uint32 threadPoolSize = 1;
    TEST_START;

    const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

    try {
        IdxTable table("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);

        vector<CID> cids = table.getCIDs();

        // Add some string
        TEST_VALUE(true, table.addKey("1", vector<CID>{101}, vector<UINT64>{table.tokenizeString("1234567xetto").getFullToken()}));
        TEST_VALUE(true, table.addKey("2", vector<CID>{101}, vector<UINT64>{table.tokenizeString("betto").getFullToken()}));
        TEST_VALUE(true, table.addKey("3", vector<CID>{101}, vector<UINT64>{table.tokenizeString("1betto").getFullToken()}));
        TEST_VALUE(true, table.addKey("4", vector<CID>{101}, vector<UINT64>{table.tokenizeString("betto1").getFullToken()}));
        TEST_VALUE(true, table.addKey("5", vector<CID>{101}, vector<UINT64>{table.tokenizeString("1betto1").getFullToken()}));
        TEST_VALUE(true, table.addKey("6", vector<CID>{101}, vector<UINT64>{table.tokenizeString("12betto").getFullToken()}));
        TEST_VALUE(true, table.addKey("7", vector<CID>{101}, vector<UINT64>{table.tokenizeString("betto12").getFullToken()}));
        TEST_VALUE(true, table.addKey("8", vector<CID>{101}, vector<UINT64>{table.tokenizeString("123betto").getFullToken()}));
        TEST_VALUE(true, table.addKey("9", vector<CID>{101}, vector<UINT64>{table.tokenizeString("betto123").getFullToken()}));
        TEST_VALUE(true, table.addKey("10", vector<CID>{101, 103}, vector<UINT64>{table.tokenizeString("1234betto").getFullToken(), 99999}));
        TEST_VALUE(true, table.addKey("11", vector<CID>{101}, vector<UINT64>{table.tokenizeString("betto1234").getFullToken()}));

        BHandle handle = Buffer::create(10);
        SCOPE_EXIT {
            Buffer::destroy(handle);
        };

        // Search pattern:  len=1  start
        TEST_VALUE(5, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("b%"), ALLRIDS));
        // Search pattern:  len=1  middle
        TEST_VALUE(10, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("%b%"), ALLRIDS));
        // Search pattern:  len=1  end
        TEST_VALUE(1, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("%4"), ALLRIDS));

        // Search pattern:  len=2  start
        TEST_VALUE(5, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("be%"), ALLRIDS));
        // Search pattern:  len=2  middle
        TEST_VALUE(7, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("%12%"), ALLRIDS));
        // Search pattern:  len=2  end
        TEST_VALUE(1, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("%23"), ALLRIDS));

        // Search pattern:  len=3  start
        TEST_VALUE(5, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("bet%"), ALLRIDS));
        // Search pattern:  len=3  middle
        TEST_VALUE(5, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("%123%"), ALLRIDS));
        // Search pattern:  len=3  end
        TEST_VALUE(1, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("%123"), ALLRIDS));

        // now delete something and try again
        table.delete_keys({"10"});
        // Search pattern:  len=1  start
        TEST_VALUE(5, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("b%"), ALLRIDS));
        // Search pattern:  len=1  middle
        TEST_VALUE(9, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("%b%"), ALLRIDS));
        // Search pattern:  len=1  end
        TEST_VALUE(1, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("%4"), ALLRIDS));

        // Search pattern:  len=2  start
        TEST_VALUE(5, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("be%"), ALLRIDS));
        // Search pattern:  len=2  middle
        TEST_VALUE(6, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("%12%"), ALLRIDS));
        // Search pattern:  len=2  end
        TEST_VALUE(1, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("%23"), ALLRIDS));

        // Search pattern:  len=3  start
        TEST_VALUE(5, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("bet%"), ALLRIDS));
        // Search pattern:  len=3  middle
        TEST_VALUE(4, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("%123%"), ALLRIDS));
        // Search pattern:  len=3  end
        TEST_VALUE(1, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("%123"), ALLRIDS));

        // now delete something and try again
        table.delete_keys({"11"});
        // Search pattern:  len=1  start
        TEST_VALUE(4, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("b%"), ALLRIDS));
        // Search pattern:  len=1  middle
        TEST_VALUE(8, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("%b%"), ALLRIDS));
        // Search pattern:  len=1  end
        TEST_VALUE(0, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("%4"), ALLRIDS));

        // Search pattern:  len=2  start
        TEST_VALUE(4, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("be%"), ALLRIDS));
        // Search pattern:  len=2  middle
        TEST_VALUE(5, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("%12%"), ALLRIDS));
        // Search pattern:  len=2  end
        TEST_VALUE(1, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("%23"), ALLRIDS));

        // Search pattern:  len=3  start
        TEST_VALUE(4, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("bet%"), ALLRIDS));
        // Search pattern:  len=3  middle
        TEST_VALUE(3, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("%123%"), ALLRIDS));
        // Search pattern:  len=3  end
        TEST_VALUE(1, Test_IdxTable::wslf_string_pattern(table, 101, handle, table.tokenizeString("%123"), ALLRIDS));
    } catch (...) {
        TEST_VALUE(1, 0);
    }
}

void test_modify_row_simple() {
    TEST_START;

    bool got_exception = false;
    const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

    try {
        IdxTable table("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);
        vector<CID> cids = table.getCIDs();

        TEST_VALUE(true, table.addKey("1", vector<CID>{cids[0]}, vector<UINT64>{0x2343453}));

        {
            auto ret = table.upinsertKey("1", vector<CID>{cids[0]}, vector<UINT64>{0xABC});
            TEST_VALUE(1, ret.size());
            if (ret.size() == 1) {
                TEST_VALUE(cids[0], ret[0]);
            }
        }

        // Modify again but with same value... expect no changes.
        {
            auto ret = table.upinsertKey("1", vector<CID>{cids[0]}, vector<UINT64>{0xABC});
            TEST_VALUE(0, ret.size());
        }

        // Modify a RID that does not exist
        {
            RID rid = table.getNextRid() + 100;
            auto ret = table.upinsertKey("1111", vector<CID>{cids[0]}, vector<UINT64>{0xABC});
            TEST_VALUE(fqcids.size() + 1, ret.size());
        }
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

void test_modify_row_same_rid(uint32 num_threads) {
    g_addendum = "_" + to_string(num_threads);
    TEST_START;

    string tput_unit_measure("Kput/sec");

    bool got_exception = false;
    const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

    try {
        IdxTable table("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);
        const uint64 elems_per_thread = 1'000;
        ADD_ROWS_MT(table, elems_per_thread, num_threads);

        vector<CID> cids = table.getCIDs();

        const int modifies_per_thread = 1'000'000;
        const uint64 total_elems = modifies_per_thread * num_threads;

        RID rid = 53;

        vector<thread> t_threads;
        std::barrier start_barrier(num_threads + 1);

        auto modifier_lambda = [&](uint64 idx) {
            try {
                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                // go!!
                for (int i = 0; i < modifies_per_thread; i++) {
                    string key = to_string(idx) + "_" + to_string(i);
                    table.upinsertKey(key, vector<CID>{cids[0]}, vector<UINT64>{idx % 2});
                }
            } catch (...) {
                got_exception = true;
                TEST_VALUE(false, got_exception);
            }
        };

        // create all threads (they will wait on the barrier)
        for (uint32 i = 0; i < num_threads; i++) {
            t_threads.emplace_back(modifier_lambda, i);
        }

        // let all threads go!
        start_barrier.arrive_and_wait();

        // Wait for all threads to be done.
        for (uint32 i = 0; i < num_threads; i++) {
            t_threads[i].join();
        }

        // expect the right number of rows in the table
        //
        TEST_VALUE(total_elems, table.getNumRowsApprox());
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

void test_modify_row(uint32 num_threads) {
    g_addendum = "_" + to_string(num_threads);
    TEST_START;

    uint32 threadPoolSize = 4;

    string tput_unit_measure("Kput/sec");

    bool got_exception = false;
    const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

    try {
        IdxTable table("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);
        const uint64 elems_per_thread = 1'000'000;
        const uint64 total_elems = elems_per_thread * num_threads;
        ADD_ROWS_MT(table, elems_per_thread, num_threads);

        vector<CID> cids = table.getCIDs();

        const int modifies_per_thread = 1'000'000;

        vector<thread> t_threads;
        std::barrier start_barrier(num_threads + 1);

        atomic<uint64> total_tputK = 0;

        auto modifier_lambda = [&](uint64 idx) {
            try {
                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                auto dur = TAKE_TIME {
                    // go!!
                    for (int i = 0; i < modifies_per_thread; i++) {
                        string key = to_string(idx) + "_" + to_string(i);
                        table.upinsertKey(key, vector<CID>{cids[0]}, vector<UINT64>{idx});
                    }
                };

                auto durationMS = dur.millis();
                auto tputK = (modifies_per_thread / durationMS);
                total_tputK += tputK;
                DEBUG_LOG_PERF(g_test_name << " (thread " << idx << "): " << durationMS << "ms   or throughput: " << tputK << " Krows/sec");
            } catch (...) {
                got_exception = true;
                TEST_VALUE(false, got_exception);
            }
        };

        // create all threads (they will wait on the barrier)
        for (uint32 i = 0; i < num_threads; i++) {
            t_threads.emplace_back(modifier_lambda, i);
        }

        // let all threads go!
        start_barrier.arrive_and_wait();

        // Wait for all threads to be done.
        for (uint32 i = 0; i < num_threads; i++) {
            t_threads[i].join();
        }

        LOG_PERF(g_test_name << " aggregate throughput = " << total_tputK << " " << tput_unit_measure);

        // expect the right number of rows in the table
        //
        TEST_VALUE(total_elems, table.getNumRowsApprox());
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

void test_clear() {
    uint32 threadPoolSize = 1;

    TEST_START;

    bool got_exception = false;
    const vector<FullyQualifiedCID> fqcids{{"colname", 101, ColumnType::TEXT}, {"102", 102, ColumnType::BIGINT}, {"103", 103, ColumnType::DATE}};

    try {
        IdxTable table("simpletable", prefix, fqcids, IdxTable::default_config, server, 1000);

        vector<CID> cids = table.getCIDs();

        {
            table.upinsertKey("1", vector<CID>{cids[0]}, vector<UINT64>{1});
            table.upinsertKey("2", vector<CID>{cids[1]}, vector<UINT64>{46});
            table.upinsertKey("3", vector<CID>{cids[1]}, vector<UINT64>{0});
            TEST_VALUE(3, table.getNumRowsApprox());
        }

        // Now clear table
        {
            table.clear();
            TEST_VALUE(0, table.getNumRowsApprox());
        }
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

} // namespace

void test_idxtable(bool quick) {
    const vector<uint32> long_list_num_threads{4, 8, 16, 32};
    const vector<uint32> short_list_num_threads{4, 8};
    const vector<uint32>& list_num_threads = quick ? short_list_num_threads : long_list_num_threads;

    // delete after debugging
    test_add_multithreaded(4);

    test_pattern_small_strings();

    test_raw_filters();
    TEST_VALUE(0, Buffer::get_num_buffers());

    for (auto num_threads : list_num_threads) {
        test_mixed_1(num_threads);
    }

    test_stop();
    TEST_VALUE(0, Buffer::get_num_buffers());

    test_modify_row_simple();

    test_table_lock();
    TEST_VALUE(0, Buffer::get_num_buffers());

    test_nleaf_and();
    TEST_VALUE(0, Buffer::get_num_buffers());

    test_creation_good();
    TEST_VALUE(0, Buffer::get_num_buffers());

    test_grow_1();
    TEST_VALUE(0, Buffer::get_num_buffers());

    test_bool_filters();
    TEST_VALUE(0, Buffer::get_num_buffers());

    test_add_simple();
    TEST_VALUE(0, Buffer::get_num_buffers());

    test_get_info();

    test_sort_0();
    TEST_VALUE(0, Buffer::get_num_buffers());

    test_grow_2();
    TEST_VALUE(0, Buffer::get_num_buffers());

    test_delete_simple();
    TEST_VALUE(0, Buffer::get_num_buffers());

    test_clear();
    TEST_VALUE(0, Buffer::get_num_buffers());

    for (auto num_threads : list_num_threads) {
        test_modify_row(num_threads);

        test_modify_row_same_rid(num_threads);

        if (!quick) {
            test_concurrent_adds_and_queries(num_threads);
        }

        test_concurrent_adds_and_queries_and_modifies(num_threads);

        test_grow_1_multithreaded(num_threads);

        test_mixed_1(num_threads);

        test_delete_multithreaded(num_threads);

        test_add_multithreaded(num_threads);
    }

    TEST_VALUE(0, Buffer::get_num_buffers());
}
