/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#include <atomic>
#include <barrier>
#include <string>
#include <thread>
#include <vector>

#include "test/test_utils.h"
#include "types.h"

#include "idxtable.h"

#include "scope/take_time.h"
#include "threads/worker_thread.h"

#include "TokenizedStrings.h"

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

#define ADD_STRINGS_MT(TABLE, NUM_ROWS, TH) AddStrings(TABLE, NUM_ROWS, TH, __LINE__)

#define ADD_STRINGS(TABLE, NUM_ROWS) AddStrings(TABLE, NUM_ROWS, 1, __LINE__)

#define ADD_STRINGS_MT_NUMBERS(TABLE, START, TO, NUM_ROWS, TH) AddStringsFromNumbers(TABLE, START, TO, NUM_ROWS, TH, __LINE__)

using namespace std;
using namespace memurai;
using namespace memurai::multicore;
using namespace memurai::sql;

namespace memurai::sql {
class Test_TokenizedStrings {
public:
    static void remove_random_string(TokenizedStrings& htable) {
        decltype(htable.pos2token)::iterator iter;

        {
            std::shared_lock<std::shared_mutex> mlock(htable.mutex_general);

            auto rnd_pos = htable.n_sizeSoFar / 2;

            iter = htable.pos2token.lower_bound(rnd_pos);

            if (iter == htable.pos2token.end())
                return;
        }

        htable.removeString(iter->second);
    }

    static vector<StringToken> getAllTokens(TokenizedStrings& htable) {
        std::unique_lock<std::shared_mutex> mlock(htable.mutex_general);

        vector<StringToken> ret;
        ret.reserve(htable.pos2token.size());

        for (auto kv : htable.pos2token) {
            ret.push_back(kv.second);
        }

        return std::move(ret);
    }

    static uint64 getTrapped(TokenizedStrings& htable) {
        return htable.c_trapped;
    }
};
} // namespace memurai::sql

namespace {

void AddStringsFromNumbers(TokenizedStrings& htable, uint64 from, uint64 to, uint64 num_strings_per_thread, uint64 num_threads, int linenum) {
    uint64 delta = to - from;
    vector<thread> t_threads;
    bool got_exception = false;

    DEBUG_LOG_DEBUG("AddStringsFromNumbers: starting with " << num_threads << " threads.");

    auto adder_lambda = [&](uint64 idx) {
        auto ms = std::chrono::high_resolution_clock::now().time_since_epoch().count();

        srand((uint32)ms);
        try {
            // go!!
            for (int i = 0; i < num_strings_per_thread; i++) {
                auto number = (rand() % delta) + from;
                assert(number >= from);
                assert(number <= to);
                // Making string bigger than 7
                auto str = string("_abcde_") + to_string(number);
                htable.addString(str.c_str(), str.length());
            }
        } catch (...) {
            got_exception = true;
            TEST_VALUE_INSIDE(false, got_exception);
        }

        DEBUG_LOG_DEBUG("AddStringsFromNumbers: thread " << idx << " done.");
    };

    // create all threads (they will wait on barrier)
    for (uint32 i = 0; i < num_threads; i++) {
        t_threads.emplace_back(adder_lambda, i);
    }

    // Wait for all threads to be done.
    for (uint32 i = 0; i < num_threads; i++) {
        t_threads[i].join();
    }

    LOG_DEBUG("AddStrings done. Added " << num_strings_per_thread * num_threads << " items with " << num_threads << " threads.");
}

void AddStrings(TokenizedStrings& htable, uint64 num_strings_per_thread, uint64 num_threads, int linenum) {
    vector<thread> t_threads;
    bool got_exception = false;

    DEBUG_LOG_DEBUG("AddStrings: starting with " << num_threads << " threads.");

    atomic<uint64> total_tputK{0};
    string tput_unit_measure("Kput/sec");

    auto adder_lambda = [&](uint64 idx) {
        auto ms = std::chrono::high_resolution_clock::now().time_since_epoch().count();

        try {
            auto dur = TAKE_TIME {

                // go!!
                for (int i = 0; i < num_strings_per_thread; i++) {
                    // Making string longer than 7 bytes
                    auto str = string("xyzw_") + to_string(idx) + "_" + to_string(i);
                    htable.addString(str.c_str(), str.length());
                }
            };

            auto durationMS = dur.millis();
            auto tputK = (durationMS == 0) ? (0) : (num_strings_per_thread / durationMS);
            total_tputK += tputK;
            DEBUG_LOG_PERF(g_test_name << " (thread " << idx << "): " << durationMS << "ms   or throughput: " << tputK << " Kitems/sec");
        } catch (...) {
            got_exception = true;
            TEST_VALUE_INSIDE(false, got_exception);
        }

        DEBUG_LOG_DEBUG("AddStrings: thread " << idx << " done.");
    };

    // create all threads (they will wait on s_start)
    for (uint32 i = 0; i < num_threads; i++) {
        t_threads.emplace_back(adder_lambda, i);
    }

    // Wait for all threads to be done.
    for (uint32 i = 0; i < num_threads; i++) {
        t_threads[i].join();
    }

    LOG_DEBUG("AddStrings done. Added " << num_strings_per_thread * num_threads << " rows with " << num_threads << " threads.");
    LOG_PERF(g_test_name << " aggregate throughput = " << total_tputK << " " << tput_unit_measure);
}

// --------------------------------------------------------------------
// --------------------------------------------------------------------

void test_trivial() {
    // g_addendum = "_" + to_string(num_threads);
    TEST_START;
    bool got_exception = false;

    try {
        TokenizedStrings htable;
        {
            // Small strings
            string mystring("hello!");
            htable.addString(mystring);
            TEST_VALUE(1, htable.getNumStringsApprox());
            TEST_VALUE(true, htable.contains(mystring));
        }

        string mystring("1234566hello!");
        htable.addString(mystring);
        TEST_VALUE(2, htable.getNumStringsApprox());
        TEST_VALUE(true, htable.contains(mystring));

        htable.removeString(mystring);
        TEST_VALUE(1, htable.getNumStringsApprox());
        TEST_VALUE(false, htable.contains(mystring));
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

void test_grow_1(uint32 num_threads, uint64 num_strings_to_add) {
    const auto num_strings_per_thread = num_strings_to_add / num_threads;
    g_addendum = "_" + to_string(num_threads) + "_" + to_string(num_strings_per_thread);
    TEST_START;
    bool got_exception = false;

    ///  ThresholdStrategy: here threshold kicks in at 2K _absolute_ distance from boundary.
    static constexpr ThresholdStrategy thresholdStrat = {ThresholdStrategy::StrategyType::Absolute, 1'000};
    ///  We grow when threshold (above) kicks in, and we grow linearly by 10x
    static constexpr GrowingStrategy gstrat = {GrowingStrategy::StrategyType::Linear, 10};
    // Default initial capacity of 1M, minimum search result size 1K.
    static constexpr TokenizedStrings::Config config = {10'000, 1'000, 1'000, gstrat, gstrat};

    try {
        TokenizedStrings htable(config);

        ADD_STRINGS_MT(htable, num_strings_per_thread, num_threads);

        // Small string!
        htable.addString(string("hello!"));
        TEST_VALUE(true, htable.contains("hello!"));

        string str = "hellojmgfhngkhngfknhgklnhkhgnjk!";
        htable.addString(str.c_str(), str.length());
        TEST_VALUE(true, htable.contains(str));

        auto collisions = htable.getNumCollisions();
        auto num_strings = htable.getNumStringsApprox();
        auto total = num_strings + collisions;

        TEST_VALUE_WITH_EPSILON(1ull + num_strings_per_thread * num_threads, total, 2ull);
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

void test_concurrent_mixed(uint32 num_threads) {
    g_addendum = "_" + to_string(num_threads);
    TEST_START;
    bool got_exception = false;
    uint64 num_strings_per_thread = 100'000;

    try {
        TokenizedStrings htable;

        ADD_STRINGS_MT(htable, num_strings_per_thread, num_threads);

        cchar* ptr = "hellojmgfhngkhngfknhgklnhkhgnjk!";
        htable.addString(ptr);

        auto collisions = htable.getNumCollisions();
        auto num_strings = htable.getNumStringsApprox();
        auto total = num_strings + collisions;

        TEST_VALUE_WITH_EPSILON(1ull + num_strings_per_thread * num_threads, total, 2ull);

        TEST_VALUE(true, htable.contains(ptr));
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

// Adds and delete happen concurrently
//
void test_concurrent_add_delete(uint32 num_threads) {
    g_addendum = "_" + to_string(num_threads);
    TEST_START;

    vector<thread> t_threads;
    std::barrier start_barrier(num_threads + 2);

    bool got_exception = false;
    const vector<QualifiedColumn> columns{{"name", ColumnType::TEXT}, {"age", ColumnType::BIGINT}, {"dob", ColumnType::DATE}};

    try {
        TokenizedStrings htable;
        uint64 num_strings_per_thread = 100'000;

        auto adder_driver = [&]() {
            // wait for all threads to be set up
            start_barrier.arrive_and_wait();

            ADD_STRINGS_MT_NUMBERS(htable, 0, 1'000'000, num_strings_per_thread, num_threads);
        };

        const uint64 deletion_per_thread = num_strings_per_thread;

        auto one_deleter_lambda = [&](uint64 threadIdx) {
            try {
                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                // go!!
                for (int i = 0; i < deletion_per_thread; i++) {
                    Test_TokenizedStrings::remove_random_string(htable);
                }
            } catch (...) {
                got_exception = true;
                TEST_VALUE(false, got_exception);
            }
        };

        // create all threads (they will wait on s_start)
        for (uint32 i = 0; i < num_threads; i++) {
            t_threads.emplace_back(one_deleter_lambda, i);
        }
        t_threads.emplace_back(adder_driver);

        // wait for all threads to be set up
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

//
void test_concurrent_add_delete_searches(uint32 num_threads) {
    g_addendum = "_" + to_string(num_threads);
    TEST_START;

    vector<thread> t_threads;
    std::barrier start_barrier(num_threads + 2);

    bool got_exception = false;
    const vector<QualifiedColumn> columns{{"name", ColumnType::TEXT}, {"age", ColumnType::BIGINT}, {"dob", ColumnType::DATE}};

    try {
        static constexpr ThresholdStrategy thresholdStrat = {ThresholdStrategy::StrategyType::Absolute, 2'000};
        static constexpr GrowingStrategy gstrat = {GrowingStrategy::StrategyType::Linear, 10};
        TokenizedStrings::Config config{100'000, 100'000, 1'000, gstrat, gstrat};
        TokenizedStrings htable(config);
        uint64 num_strings_per_thread = 200'000;
        const uint64 maximum_value = 1'000'000;

        auto adder_driver = [&]() {
            // wait for all threads to be set up
            start_barrier.arrive_and_wait();

            ADD_STRINGS_MT_NUMBERS(htable, 0, maximum_value, num_strings_per_thread, num_threads);
        };

        const uint64 deletion_per_thread = num_strings_per_thread;

        auto one_deleter_lambda = [&](uint64 threadIdx) {
            try {
                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                // go!!
                for (int i = 0; i < deletion_per_thread; i++) {
                    auto str = to_string(rand() % maximum_value);
                    htable.removeString(str.c_str(), str.length());
                }
            } catch (...) {
                got_exception = true;
                TEST_VALUE(false, got_exception);
            }
        };

        auto adder_lambda = [&]() {
            // wait for all threads to be set up
            start_barrier.arrive_and_wait();

            ADD_STRINGS_MT_NUMBERS(htable, 0, maximum_value, num_strings_per_thread, num_threads);
        };

        auto one_search_lambda = [&](uint64 threadIdx) {
            try {
                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                // go!!
                for (int i = 0; i < deletion_per_thread; i++) {
                    TEST_VALUE(true, htable.contains(to_string(rand() % maximum_value)));
                }
            } catch (...) {
                got_exception = true;
                TEST_VALUE(false, got_exception);
            }
        };

        // create all threads (they will wait on s_start)
        for (uint32 i = 0; i < num_threads; i++) {
            t_threads.emplace_back(one_deleter_lambda, i);
        }
        t_threads.emplace_back(adder_lambda);

        // wait for all threads to be set up
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

void test_reads_vs_puts_core(uint32 writes_Hz, uint32 num_readers) {
    g_addendum = "_" + to_string(writes_Hz) + "_" + to_string(num_readers);
    TEST_START;

    string tput_unit_measure("Ksearches/sec");

    vector<thread> t_threads;
    atomic<bool> s_finish{false};
    std::barrier start_barrier(num_readers + 2);

    bool got_exception = false;
    const vector<QualifiedColumn> columns{{"name", ColumnType::TEXT}, {"age", ColumnType::BIGINT}, {"dob", ColumnType::DATE}};

    try {
        static constexpr ThresholdStrategy thresholdStrat = {ThresholdStrategy::StrategyType::Absolute, 2'000};
        static constexpr GrowingStrategy gstrat = {GrowingStrategy::StrategyType::Linear, 10};
        TokenizedStrings::Config config{100'000, 100'000, 1'000, gstrat, gstrat};
        TokenizedStrings htable(config);
        uint64 num_strings_per_thread = 41'000;
        const uint64 maximum_value = 1'000'000;

        // First add a million strings
        ADD_STRINGS_MT_NUMBERS(htable, 0, maximum_value, num_strings_per_thread, 5);

        auto stokens = Test_TokenizedStrings::getAllTokens(htable);

        atomic<uint64> total_tputK{0};

        auto adder_lambda = [&]() {
            // wait for all threads to be set up
            start_barrier.arrive_and_wait();

            uint64 inc = 1000;
            while (!s_finish.load(std::memory_order_acquire)) {
                htable.addString(to_string(inc++));

                this_thread::sleep_for(chrono::milliseconds(1000 / writes_Hz));
            }
        };

        auto one_search_lambda = [&](uint64 threadIdx) {
            try {
                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                auto size = stokens.size();
                uint64 searches = 0;

                auto dur = TAKE_TIME {
                    while (!s_finish.load(std::memory_order_acquire)) {
                        auto idx = rand() % size;
                        htable.getString(stokens[idx]);
                        searches++;
                    }
                };

                auto durationMS = dur.millis();
                auto tputK = (durationMS == 0) ? (0) : (searches / durationMS);
                DEBUG_LOG_PERF(g_test_name << " (thread " << threadIdx << "): in " << durationMS / 1000 << "s done " << searches
                                           << " searches, or throughput: " << tputK << " " << tput_unit_measure);
                total_tputK += tputK;
            } catch (...) {
                got_exception = true;
                TEST_VALUE(false, got_exception);
            }
        };

        // create all threads (they will wait on s_start)
        for (uint32 i = 0; i < num_readers; i++) {
            t_threads.emplace_back(one_search_lambda, i);
        }
        t_threads.emplace_back(adder_lambda);

        // wait for all threads to be set up
        start_barrier.arrive_and_wait();

        this_thread::sleep_for(chrono::seconds(5));

        s_finish.store(true, std::memory_order_release);

        // Wait for all threads to be done.
        for (auto& t : t_threads) {
            t.join();
        }

        LOG_PERF(g_test_name << " aggregate throughput = " << total_tputK << " " << tput_unit_measure);
        LOG_INFO(g_test_name << " trapped = " << Test_TokenizedStrings::getTrapped(htable));

        // Test done... we cannot expect the number of elements at this point.
        // We just want to make sure that we do not throw.
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

void test_reads_vs_puts(bool quick) {
    const vector<uint32> long_list_num_readers{1, 2, 4, 10, 15, 20, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200};
    const vector<uint32> long_list_writes_Hz{10, 20, 50, 100};
    const vector<uint32> short_list_num_readers{20, 100};
    const vector<uint32> short_list_writes_Hz{20, 100};

    const vector<uint32>& list_num_readers = quick ? short_list_num_readers : long_list_num_readers;
    const vector<uint32>& list_writes_Hz = quick ? short_list_writes_Hz : long_list_writes_Hz;

    for (auto num_readers : list_num_readers) {
        for (auto writes_Hz : list_writes_Hz) {
            test_reads_vs_puts_core(writes_Hz, num_readers);
        }
    }
}

void test_grow_while_reads_core(uint32 num_readers) {
    // g_addendum = "_" + to_string(writes_Hz) + "_" + to_string(num_readers);
    TEST_START;

    vector<thread> t_threads;
    atomic<bool> s_finish{false};
    std::barrier start_barrier(num_readers + 2);
    string tput_unit_measure("Ksearches/sec");

    bool got_exception = false;
    const vector<QualifiedColumn> columns{{"name", ColumnType::TEXT}, {"age", ColumnType::BIGINT}, {"dob", ColumnType::DATE}};

    try {
        static constexpr ThresholdStrategy thresholdStrat = {ThresholdStrategy::StrategyType::Absolute, 2'000};
        static constexpr GrowingStrategy gstrat = {GrowingStrategy::StrategyType::Linear, 10};
        TokenizedStrings::Config config{100'000, 100'000, 1'000, gstrat, gstrat};
        TokenizedStrings htable(config);
        uint64 num_strings_per_thread = 1'000'000;
        const uint64 maximum_value = 1'000'000;

        ADD_STRINGS_MT_NUMBERS(htable, 0, maximum_value, 1'000'000, 1);

        auto stokens = Test_TokenizedStrings::getAllTokens(htable);

        auto adder_lambda = [&]() {
            // wait for all threads to be set up
            start_barrier.arrive_and_wait();

            while (!s_finish.load(std::memory_order_acquire)) {
                ADD_STRINGS_MT_NUMBERS(htable, 0, maximum_value, 1000, 1);
                this_thread::sleep_for(chrono::milliseconds(100));
            }
        };

        atomic<uint64> total_tputK{0};

        auto one_search_lambda = [&](uint64 threadIdx) {
            try {
                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                auto size = stokens.size();
                uint64 searches = 0;

                auto dur = TAKE_TIME {
                    while (!s_finish.load(std::memory_order_acquire)) {
                        auto idx = rand() % size;
                        htable.getString(stokens[idx]);
                        searches++;
                    }
                };

                auto durationMS = dur.millis();
                auto tputK = (durationMS == 0) ? (0) : (searches / durationMS);
                LOG_PERF(g_test_name << " (thread " << threadIdx << "): in " << durationMS / 1000 << "s done " << searches
                                     << " searches, or throughput: " << tputK << " Ksearches/sec");
                total_tputK += tputK;
            } catch (...) {
                got_exception = true;
                TEST_VALUE(false, got_exception);
            }
        };

        // create all threads (they will wait on s_start)
        for (uint32 i = 0; i < num_readers; i++) {
            t_threads.emplace_back(one_search_lambda, i);
        }
        t_threads.emplace_back(adder_lambda);

        // wait for all threads to be set up
        start_barrier.arrive_and_wait();

        this_thread::sleep_for(chrono::seconds(5));

        s_finish.store(true, std::memory_order_release);

        // Wait for all threads to be done.
        for (auto& t : t_threads) {
            t.join();
        }

        LOG_PERF(g_test_name << " aggregate throughput = " << total_tputK << " " << tput_unit_measure);
        LOG_INFO(g_test_name << " trapped = " << Test_TokenizedStrings::getTrapped(htable));

        // Test done... we cannot expect the number of elements at this point.
        // We just want to make sure that we do not throw.
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

void test_grow_while_reads() {
    auto hw_threads = std::thread::hardware_concurrency();

    for (uint32 num_readers = 1; num_readers <= hw_threads; num_readers *= 2) {
        test_grow_while_reads_core(num_readers);
    }
}

#define TEST_LONG_STRINGS(B) test_long_strings(htable, B)

void test_long_strings(TokenizedStrings& htable, uint32 num) {
    auto begin_size = htable.getNumStringsApprox();
    string str_num = to_string(num);

    string sxx(num, 'x');
    htable.addString(sxx);

    TEST_VALUE(begin_size + 1, htable.getNumStringsApprox());
    TEST_VALUE(0, htable.findPattern(str_num + "x%").size());
    TEST_VALUE(0, htable.findPattern(string("%x") + str_num).size());
    TEST_VALUE(0, htable.findPattern(string("%x") + str_num + "%").size());

    TEST_VALUE(0, htable.findPattern("%zzzz").size());
    TEST_VALUE(1, htable.findPattern("hell%").size());
    string s2(110 + num - 233, 'x'), s3(119, 'x');
    string sxyy = s2 + str_num + s3;
    htable.addString(sxyy);
    TEST_VALUE(begin_size + 2, htable.getNumStringsApprox());

    TEST_VALUE(0, htable.findPattern("%zzzz").size());
    TEST_VALUE(0, htable.findPattern(str_num + "x%").size());
    TEST_VALUE(0, htable.findPattern(string("%x") + str_num).size());
    TEST_VALUE(1, htable.findPattern(string("%x") + str_num + "%").size());
    htable.removeString(sxx);
    TEST_VALUE(begin_size + 1, htable.getNumStringsApprox());
    htable.removeString(sxyy);
    TEST_VALUE(begin_size, htable.getNumStringsApprox());

    auto end_size = htable.getNumStringsApprox();
    TEST_VALUE(begin_size, end_size);
}

void test_wildcards_1(bool quick) {
    TEST_START;
    bool got_exception = false;
    const uint32 num_strings = 1'000;

    try {
        TokenizedStrings htable;
        ADD_STRINGS_MT(htable, num_strings, 1);

        string mystring = "hellofgfdhfghfghfghfg!";
        htable.addString(mystring);
        // TEST_VALUE(1 + num_strings, htable.getNumStringsApprox());

        auto ret = htable.findPattern("%he");
        TEST_VALUE(0, ret.size());

        ret = htable.findPattern("%ll");
        TEST_VALUE(0, ret.size());

        ret = htable.findPattern("ll%");
        TEST_VALUE(0, ret.size());

        ret = htable.findPattern("he%");
        TEST_VALUE(1, ret.size());
        TEST_VALUE(0, memcmp(mystring.c_str(), htable.getString(ret[0]).c_str(), mystring.length()));

        ret = htable.findPattern("%ll%");
        TEST_VALUE(1, ret.size());
        TEST_VALUE(0, memcmp(mystring.c_str(), htable.getString(ret[0]).c_str(), mystring.length()));

        ret = htable.findPattern("%zzzz");
        TEST_VALUE(0, ret.size());
        ret = htable.findPattern("zzzz%");
        TEST_VALUE(0, ret.size());
        ret = htable.findPattern("%zzzz%");
        TEST_VALUE(0, ret.size());

        // long strings now
        {
            string sxx(232, 'x');
            htable.addString(sxx);
            TEST_VALUE(0, htable.findPattern("%zzzz").size());
            TEST_VALUE(1, htable.findPattern("hell%").size());

            string s2(110, 'x'), s3(119, 'x');
            string sxyy = s2 + string("hello") + s3;
            htable.addString(sxyy);
            TEST_VALUE(0, htable.findPattern("%zzzz").size());
            TEST_VALUE(1, htable.findPattern("hell%").size());
            TEST_VALUE(0, htable.findPattern("%hell").size());
            TEST_VALUE(2, htable.findPattern("%hell%").size());
            htable.removeString(sxx);
            htable.removeString(sxyy);
        }

        {
            string sxx(233, 'x');
            cchar* s = sxx.c_str();
            htable.addString(s);
            TEST_VALUE(0, htable.findPattern("%zzzz").size());
            TEST_VALUE(1, htable.findPattern("hell%").size());

            string s2(110, 'x'), s3(119, 'x');
            string sxyy = s2 + string("hello") + s3;
            cchar* sx = sxyy.c_str();
            htable.addString(sx);
            TEST_VALUE(0, htable.findPattern("%zzzz").size());
            TEST_VALUE(1, htable.findPattern("hell%").size());
            TEST_VALUE(0, htable.findPattern("%hell").size());
            TEST_VALUE(2, htable.findPattern("%hell%").size());
            htable.removeString(s);
            htable.removeString(sx);
        }

        TEST_LONG_STRINGS(232);
        TEST_LONG_STRINGS(233);
        TEST_LONG_STRINGS(234);
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);

    try {
#ifdef _DEBUG
        // auto size_limit = 10*1024*1024;
#else
        // auto size_limit = TokenizedStrings::STRING_SIZELIMIT;
#endif
        auto size_limit = 20 * 1024 * 1024;

        const int factor = quick ? 4 : 2;

        for (int num = 256; num < size_limit; num *= factor) {
            TokenizedStrings htable;
            auto mystring = "hello345363456565436453!";
            htable.addString(mystring);
            TEST_LONG_STRINGS(num - 2);
            TEST_LONG_STRINGS(num - 1);
            TEST_LONG_STRINGS(num);
            TEST_LONG_STRINGS(num + 1);
            TEST_LONG_STRINGS(num + 2);
        }
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

void test_wildcards_mt(uint32 num_threads, bool adder_thread, uint32 writes_Hz) {
    g_addendum = "_" + to_string(num_threads) + "_" + to_string(writes_Hz);
    TEST_START;

    string tput_unit_measure("Ksearches/sec");

    vector<thread> t_threads;
    atomic<bool> s_finish{false};
    std::barrier start_barrier(num_threads + 1 + (adder_thread ? 1 : 0));

    bool got_exception = false;
    const vector<QualifiedColumn> columns{{"name", ColumnType::TEXT}, {"age", ColumnType::BIGINT}, {"dob", ColumnType::DATE}};

    try {
        static constexpr ThresholdStrategy thresholdStrat = {ThresholdStrategy::StrategyType::Absolute, 2'000};
        static constexpr GrowingStrategy gstrat = {GrowingStrategy::StrategyType::Linear, 10};
        TokenizedStrings::Config config{100'000, 100'000, 1'000, gstrat, gstrat};
        TokenizedStrings htable(config);
        uint64 num_strings_total = 220'000;
        uint64 num_strings_per_thread = num_strings_total / num_threads;
        const uint64 maximum_value = 1'000'000;

        // First add a few strings
        ADD_STRINGS_MT_NUMBERS(htable, 0, maximum_value, num_strings_per_thread, 5);

        atomic<uint64> total_tputK{0};

        auto adder_lambda = [&]() {
            // wait for all threads to be set up
            start_barrier.arrive_and_wait();

            uint64 inc = 1000;
            while (!s_finish.load(std::memory_order_acquire)) {
                htable.addString(to_string(inc++));

                this_thread::sleep_for(chrono::milliseconds(1000 / writes_Hz));
            }
        };

        string perc("%");
        auto one_search_lambda = [&](uint64 threadIdx) {
            try {
                // wait for all threads to be set up
                start_barrier.arrive_and_wait();

                uint64 searches = 0;

                auto dur = TAKE_TIME {
                    while (!s_finish.load(std::memory_order_acquire)) {
                        auto idx = rand() % 100;
                        string str = to_string(idx);
                        htable.findPattern(string(perc + str));
                        htable.findPattern(string(perc + str + perc));
                        htable.findPattern(string(str + perc));
                        searches += 3;
                    }
                };

                auto durationMS = dur.millis();
                auto tputK = (durationMS == 0) ? (0) : (searches / durationMS);
                LOG_PERF(g_test_name << " (thread " << threadIdx << "): in " << durationMS / 1000 << "s done " << searches
                                     << " searches, or throughput: " << tputK << " " << tput_unit_measure);
                total_tputK += tputK;
            } catch (...) {
                got_exception = true;
                TEST_VALUE(false, got_exception);
            }
        };

        // create all threads (they will wait on s_start)
        for (uint32 i = 0; i < num_threads; i++) {
            t_threads.emplace_back(one_search_lambda, i);
        }

        if (adder_thread) {
            t_threads.emplace_back(adder_lambda);
        }

        // wait for all threads to be set up
        start_barrier.arrive_and_wait();

        this_thread::sleep_for(chrono::seconds(5));

        s_finish.store(true, std::memory_order_release);

        // Wait for all threads to be done.
        for (auto& t : t_threads) {
            t.join();
        }

        LOG_PERF(g_test_name << " aggregate throughput = " << total_tputK << " " << tput_unit_measure);
        LOG_INFO(g_test_name << " trapped = " << Test_TokenizedStrings::getTrapped(htable));

        // Test done... we cannot expect the number of elements at this point.
        // We just want to make sure that we do not throw.
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

void test_get_token(bool quick) {
    bool got_exception = false;

    try {
        {
            // test small string...
            TokenizedStrings htable;
            string mystring("hello!");
            StringToken stoken = htable.addString(mystring);
            TEST_VALUE(1, htable.getNumStringsApprox());
            TEST_VALUE(true, htable.contains(stoken));

            const char* mystringnormal("12343hello!");
            auto stoken2 = htable.addString(mystringnormal);
            TEST_VALUE(2, htable.getNumStringsApprox());
            TEST_VALUE(true, htable.contains(stoken2));

            htable.removeString(mystring);
            TEST_VALUE(1, htable.getNumStringsApprox());
            TEST_VALUE(false, htable.contains(stoken));
        }

        {
            // test XLstring - remove by cstr
            string sxx(TokenizedStrings::XLSTRING_MINIMUM_SIZE + 100, 'x');

            TokenizedStrings htable;
            auto stoken = htable.addString(sxx);

            TEST_VALUE(1, htable.getNumStringsApprox());
            TEST_VALUE(true, htable.contains(stoken));

            htable.removeString(sxx);
            TEST_VALUE(0, htable.getNumStringsApprox());
            TEST_VALUE(false, htable.contains(stoken));
        }

        {
            // test XLstring - remove by stoken
            string sxx(TokenizedStrings::XLSTRING_MINIMUM_SIZE + 100, 'x');

            TokenizedStrings htable;
            auto stoken = htable.addString(sxx);

            TEST_VALUE(1, htable.getNumStringsApprox());
            TEST_VALUE(true, htable.contains(stoken));

            htable.removeString(stoken);
            TEST_VALUE(0, htable.getNumStringsApprox());
            TEST_VALUE(false, htable.contains(stoken));
        }
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

void test_regressions() {
    bool got_exception = false;

    try {
        {
            // test small string...
            TokenizedStrings htable;
            string mystring("ababa!");
            StringToken stoken = htable.addString(mystring);
            TEST_VALUE(1, htable.getNumStringsApprox());
            TEST_VALUE(true, htable.contains(stoken));

            auto ret = htable.findPattern("%a%");
            TEST_VALUE(1, ret.size());
        }
    } catch (...) {
        got_exception = true;
    }
    TEST_VALUE(false, got_exception);
}

} // namespace

void test_TokenizedStrings(bool quick) {
    auto hw_threads = std::thread::hardware_concurrency();
    const uint32 factor = quick ? 4 : 2;

    test_regressions();

    test_get_token(quick);

    test_wildcards_1(quick);

    test_trivial();

    const vector<uint32> strings_to_add{50'000, 500'000};
    const vector<uint32> long_list_num_threads{4, 8, 16, 32};
    const vector<uint32> short_list_num_threads{4, 8};
    const vector<uint32>& list_num_threads = quick ? short_list_num_threads : long_list_num_threads;

    for (auto num_threads : list_num_threads) {
        test_concurrent_add_delete(num_threads);

        test_concurrent_add_delete_searches(num_threads);

        test_wildcards_mt(num_threads, true, 10);
        test_wildcards_mt(num_threads, true, 100);

        for (auto num_strings_to_add : strings_to_add) {
            test_grow_1(num_threads, num_strings_to_add);
        }
    }

    test_reads_vs_puts(quick);
}
