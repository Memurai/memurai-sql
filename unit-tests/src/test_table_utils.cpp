/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#include "test/test_table_utils.h"

namespace memurai {
namespace sql_test {
using namespace std;
using namespace memurai::sql;

string randomString() {
    return "ciaociao";

    static const vector<uint32> base_lenghts{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 24, 32, 48, 64, 128, 256, 512, 1024, 2048, 4096, 8192};

    cuint32 base_rand = rand();
    cuint32 base_elem = base_lenghts[base_rand % base_lenghts.size()];

    // jitter
    cint32 jitter = ((base_elem <= 16) ? 1 : (rand() % (base_elem / 2))) * (base_rand % 2 == 0 ? +1 : -1);

    cuint32 final_length = base_elem + jitter;

    // now build the string of this length

    static const char characters[] = "0123456789"
                                     "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                     "abcdefghijklmnopqrstuvwxyz";
    static const char punctuation[] = " _:.,;'~!@&()[]{}?";

    static cuint32 space_freq_one_every = 7;
    static cuint32 punct_freq_one_every = 15;

    string ret(final_length, ' ');

    for (uint32 idx = 0; idx < final_length; idx++) {
        const uint32 myrand = rand();
        const bool do_space = (myrand % space_freq_one_every) == 0;

        if (do_space) {
            ret.at(idx) = ' ';
        } else {
            const bool do_punct = (myrand % punct_freq_one_every) == 0;
            if (do_punct) {
                ret.at(idx) = punctuation[myrand % sizeof(punctuation)];
            } else {
                ret.at(idx) = characters[myrand % sizeof(characters)];
            }
        }
    }

    return std::move(ret);
}

UINT64 generateRandomValue(ColumnType type, IdxTable& table) {
    static vector<string> tags{"open", "closed", "active", "resolved", "nevertheless", "1235", "12345678"};

    switch (type) {
    case ColumnType::TEXT: {
        StringToken tok = table.tokenizeString(randomString());
        return tok.getFullToken();
    }
#if 0
    case ColumnType::TAG:
    {
        StringToken tok = table.tokenizeString(tags[rand() % tags.size()]);
        return tok.getFullToken();
    }
#endif

    case ColumnType::BIGINT:
    case ColumnType::DATE:
    case ColumnType::TIMESTAMP:
        return (rand() + 1000) * rand();

    default:
        assert(UNREACHED);
        throw 123456;
    }
}

vector<UINT64> generateRandomRow(const VecQCID& qcids, IdxTable& table) {
    vector<UINT64> ret(qcids.size());
    uint64 idx = 0;

    for (auto qc : qcids) {
        ColumnType type = get<1>(qc);

        UINT64 value = generateRandomValue(type, table);

        ret[idx++] = value;
    }

    return std::move(ret);
}

void AddRows(IdxTable& table, uint64 num_rows_per_thread, uint64 num_threads, int linenum, uint32 rowIdx, vector<UINT64>* baseline) {
    vector<thread> t_threads;
    bool got_exception = false;

    DEBUG_LOG_DEBUG("AddRow: starting with " << num_threads << " threads.");

    auto adder_lambda = [&](uint64 idx) {
        try {
            VecCID cids = table.getCIDs();
            VecQCID qcids = table.getQualifiedCIDs();

            // go!!
            for (int i = 0; i < num_rows_per_thread; i++) {
                vector<UINT64> row = generateRandomRow(qcids, table);

                string key = to_string(idx) + "_" + to_string(i);
                table.upinsertKey(key, cids, row);
                if (baseline) {
                    (*baseline)[i + idx * num_rows_per_thread] = row[rowIdx];
                }
            }
        } catch (...) {
            got_exception = true;
            TEST_VALUE_INSIDE(false, got_exception);
        }

        DEBUG_LOG_DEBUG("AddRow: thread " << idx << " done.");
    };

    for (uint32 i = 0; i < num_threads; i++) {
        t_threads.push_back(thread(adder_lambda, i));
    }

    // Wait for all threads to be done.
    for (uint32 i = 0; i < num_threads; i++) {
        t_threads[i].join();
    }

    LOG_DEBUG("AddRows done. Added " << num_rows_per_thread * num_threads << " rows with " << num_threads << " threads.");
}

string generate_random_string_for_column_type(ColumnType type, const string& colname) {
    const vector<string> the_tags = {"1", "2", "3", "ABC", "DEC", "HEX", "OCT", "CIAO", "BYEBYE", "NOTHERE", "MEMURAi"};

    const uint64 stable1 = 11111111111;
    const uint64 stable2 = 22222222222;

    const double stable1d = 3.1415;
    const double stable2d = 9.12345;

    auto rand1 = rand();
    auto rand2 = rand();
    auto sign = ((rand1 % 2) == 0) ? +1 : -1;

    switch (type) {
    case ColumnType::TEXT:
        return randomString();

    case ColumnType::BIGINT:
        if (strncmp(colname.c_str(), "stable1", 7) == 0) {
            return to_string(stable1 + sign * rand1);
        } else if (strncmp(colname.c_str(), "stable2", 7) == 0) {
            return to_string(stable2 + sign * rand2);
        }
        return to_string(rand());

    case ColumnType::TIMESTAMP:
        return to_string(rand());

    case ColumnType::FLOAT:
        if (strncmp(colname.c_str(), "stable1d", 7) == 0) {
            return to_string(stable1d + sign * rand1);
        } else if (strncmp(colname.c_str(), "stable2d", 7) == 0) {
            return to_string(stable2d + sign * rand2);
        }
        return to_string(double(rand1) * 13.4);

#if 0
    case ColumnType::TAG:
        return the_tags[rand() % the_tags.size()];
#endif
    case ColumnType::DATE: {
        stringstream ss;
        IdxTable::print_date(rand() % 20000, ss);
        return ss.str();
    }

    default:
        assert(UNREACHED);
        return "";
    } // switch

} // function

} // namespace sql_test

} //  namespace memurai
