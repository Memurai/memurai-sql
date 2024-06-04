/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#pragma once

#include <algorithm>
#include <atomic>
#include <emmintrin.h>
#include <functional>
#include <map>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "Buffer.h"
#include "ColumnTypes.h"
#include "IPublisher.h"
#include "TokenizedStrings.h"
#include "ascdesc.h"
#include "column.h"
#include "fe-types.h"
#include "growingStrategy.h"
#include "inc_during_scope.h"
#include "memurai-multicore.h"
#include "p_pmurwl.h"
#include "rid.h"
#include "scope/expect.h"
#include "scope/scope_exit.h"
#include "shared_table_lock.h"
#include "threads/itoken.h"
#include "threads/stoppable.h"

namespace memurai::sql {

using multicore::IServer;
using multicore::IToken;
using multicore::JOBID;
using multicore::Stoppable;
using multicore::Token;
using std::atomic;
using std::atomic_bool;
using std::function;
using std::make_pair;
using std::map;
using std::optional;
using std::string;
using std::tuple;
using std::vector;

class Test_IdxTable;
class Database;

#define DELETED_BIT (1ull << 63)
#define DELETE_RID(RID) metadata[RID] |= DELETED_BIT;
#define IS_RID_DELETED(RID) ((metadata[RID] >> 63) != 0)
#define IS_RID_NOT_DELETED(RID) ((metadata[RID] >> 63) == 0)
#define IS_COLUMN_IDX_NULL(RID, IDX) (((metadata[RID] >> (IDX)) & 0x1) == 1)

class IdxTable : public Stoppable, public P_PMURWL {
public:
    friend class Test_IdxTable;
    friend class Test_IdxTable_Tags;
    friend class Test_Query_Exec;
    friend class Database;

    struct Config {
        cuint64 columnCapacity;
        cuint64 minResultCapacity;
        GrowingStrategy growthStrategy;
        ThresholdStrategy sortStrategy;
    };

    ///  When we reach the threshold, then we grow linearly by 10x
    static constexpr GrowingStrategy gstrat = {GrowingStrategy::StrategyType::Linear, 10};

    /// This calculates the unsorted capacity related to the sorted one.
    static constexpr ThresholdStrategy sstrat = {ThresholdStrategy::StrategyType::Percentage, 10};

    // Default initial capacity of 1M, unsorted buffer of 100K.
    static constexpr Config default_config = {1'000'000, 1'000, gstrat, sstrat};

    typedef tuple<bool, int64> WhereValue;

    // Where String Leaf Filter
    typedef function<uint64(ColIdx, BHandle, StringToken, uint64)> WSLF;

    // Where Int Leaf Filter
    typedef function<uint64(uint64, ColIdx, BHandle, INT64)> LIF;

    // Where Int Float Filter
    typedef function<uint64(uint64, ColIdx, BHandle, double)> LDF;

    // Not Leaf Filter
    typedef function<uint64(VecConstHandles, JOBID)> RIDFilter;

    struct QueryInterface {
        function<BHandle(WhereStringFilter, StringToken, CID, JOBID, uint64)> leafFilterString;
        function<BHandle(WhereNumericLeafFilter, INT64, CID, JOBID, uint64)> leafFilterInt;
        function<BHandle(WhereNumericLeafFilter, double, CID, JOBID, uint64)> leafFilterFloat;
        function<BHandle(JOBID, uint64)> allRIDs;
        function<BHandle(CID, JOBID, uint64)> leafIsNull;
        function<BHandle(CID, JOBID, uint64)> leafIsNotNull;
        function<BHandle(WhereRIDFilter, VecConstHandles, JOBID)> ridFilter;
        function<void(BHandle, VecCID, IToken*, IPublisher*)> project;
    };

    ValuePrinter value_printer;

private:
    // QueryInterface : to serve queries
    QueryInterface queryInterface;

    static const string NULL_STRING;

    uint64 wlf_or_eq(uint64 num_rids, ColIdx colIdx, BHandle dest, uint64 mask, uint64 value, uint64 offset = 0) noexcept {
        uint64* dest_data = Buffer::data(dest);
        uint64* dest_ptr = dest_data + offset;
        cuint64 num_sorted_elems = columns_num_sorted_elems;
        Column& my_col = the_columns[colIdx];
        uint64 c_found = 0;

        {
            QElem* src_data = my_col.get_sorted();
            QElem* src_data_end = src_data + columns_num_sorted_elems;

            // first looks for "value" in sorted elements
            //
            if (num_sorted_elems > 0) {
                QElem* sorted_data = my_col.get_sorted();

                // then look in unsorted ones
                for (uint64 idx = 0; idx < columns_num_sorted_elems; idx++) {
                    auto val = reinterpret<uint64>(sorted_data[idx].value);

                    if ((val & mask) == value) {
                        cRID rid = sorted_data[idx].rid;

                        if (!IS_COLUMN_IDX_NULL(rid, colIdx)) {
                            *dest_ptr++ = rid;
                            c_found++;
                        }
                    }
                }
            }
        }

        INT64* src_data = my_col.get_all_elems_by_rid();

        // then look in unsorted ones
        for (RID rid = columns_num_sorted_elems; rid < num_rids; rid++) {
            auto val = reinterpret<uint64>(src_data[rid]);

            if ((val & mask) == value) {
                *dest_ptr++ = rid;
                c_found++;
            }
        }

        return c_found;
    }

    uint64 wlf_eq(uint64 num_rids, ColIdx colIdx, BHandle dest, INT64 value, uint64 offset = 0) noexcept {
        uint64* dest_data = Buffer::data(dest);
        uint64* dest_ptr = dest_data + offset;
        cuint64 num_sorted_elems = columns_num_sorted_elems;
        Column& my_col = the_columns[colIdx];
        uint64 c_found = 0;
        // ................

        QElem* src_data = my_col.get_sorted();
        QElem* src_data_end = src_data + columns_num_sorted_elems;

        // first looks for "value" in sorted elements
        //
        if (num_sorted_elems > 0) {
            auto iter = std::lower_bound(src_data, src_data_end, QElem(value, 0), compare_QElem_by_value);

            while ((iter != src_data_end) && (iter->value == value)) {
                const RID rid = iter->rid;
                if (!IS_COLUMN_IDX_NULL(rid, colIdx) && IS_RID_NOT_DELETED(rid)) {
                    *dest_ptr++ = rid;
                    iter++;
                    c_found++;
                }
            }
        }

        {
            INT64* allElemsByRid = my_col.get_all_elems_by_rid();

            // then look in unsorted ones
            for (RID rid = columns_num_sorted_elems; rid < num_rids; rid++) {
                INT64 val = allElemsByRid[rid];

                if (val == value) {
                    if (!IS_COLUMN_IDX_NULL(rid, colIdx) && IS_RID_NOT_DELETED(rid)) {
                        *dest_ptr++ = rid;
                        c_found++;
                    }
                }
            }
        }

        return c_found;
    }

    template <typename T> uint64 wlf_neq(uint64 num_rids, ColIdx colIdx, BHandle dest, T value, uint64 offset = 0) noexcept {
        uint64* dest_data = Buffer::data(dest);
        uint64* dest_ptr = dest_data + offset;
        cuint64 num_sorted_elems = columns_num_sorted_elems;
        Column& my_col = the_columns[colIdx];
        uint64 c_found = 0;
        // ................

        {
            QElem* src_data = my_col.get_sorted();
            QElem* src_data_end = src_data + columns_num_sorted_elems;

            // first looks for "value" in sorted elements
            //
            if (num_sorted_elems > 0) {
                auto lower = std::lower_bound(src_data, src_data_end, QElem(value, 0), compare_QElem_by_value);
                auto upper = std::upper_bound(src_data, src_data_end, QElem(value, std::numeric_limits<uint64>::max()), compare_QElem_by_value);

                auto iter = src_data;
                const RID rid = iter->rid;
                while (iter != lower) {
                    assert(iter->value != value);
                    if (!IS_COLUMN_IDX_NULL(rid, colIdx) && IS_RID_NOT_DELETED(rid)) {
                        *dest_ptr++ = iter->rid;
                        c_found++;
                    }
                    iter++;
                }

                iter = upper;
                while (iter != src_data_end) {
                    assert(iter->value != value);
                    if (!IS_COLUMN_IDX_NULL(rid, colIdx) && IS_RID_NOT_DELETED(rid)) {
                        *dest_ptr++ = iter->rid;
                        c_found++;
                    }
                    iter++;
                }
            }
        }

        INT64* src_data = my_col.get_all_elems_by_rid();

        // then look in unsorted ones
        for (RID rid = columns_num_sorted_elems; rid < num_rids; rid++) {
            INT64 val = src_data[rid];

            if (val != value) {
                if (!IS_COLUMN_IDX_NULL(rid, colIdx) && IS_RID_NOT_DELETED(rid)) {
                    *dest_ptr++ = rid;
                    c_found++;
                }
            }
        }

        return c_found;
    }

    template <typename T> uint64 wlf_gte(uint64 num_rids, ColIdx colIdx, BHandle dest, T value) noexcept {
        uint64* dest_data = Buffer::data(dest);
        uint64* dest_ptr = dest_data;
        cuint64 num_sorted_elems = columns_num_sorted_elems;
        Column& my_col = the_columns[colIdx];
        uint64 c_found = 0;

        {
            QElem* src_data = my_col.get_sorted();
            QElem* src_data_end = src_data + columns_num_sorted_elems;

            // frst looks for "value" in sorted elements
            //
            if (num_sorted_elems > 0) {
                auto iter = std::lower_bound(src_data, src_data_end, QElem(value, 0), compare_QElem_by_value);

                while (iter != src_data_end) {
                    cRID rid = iter->rid;
                    if (!IS_COLUMN_IDX_NULL(rid, colIdx) && IS_RID_NOT_DELETED(rid)) {
                        *dest_ptr++ = rid;
                        c_found++;
                    }
                    iter++;
                }
            }
        }

        INT64* src_data = my_col.get_all_elems_by_rid();

        // then look in unsorted ones
        for (RID rid = columns_num_sorted_elems; rid < num_rids; rid++) {
            T val = reinterpret<T>(src_data[rid]);

            if ((val >= value) && (!IS_COLUMN_IDX_NULL(rid, colIdx)) && IS_RID_NOT_DELETED(rid)) {
                *dest_ptr++ = rid;
                c_found++;
            }
        }

        return c_found;
    }

    template <typename T> uint64 wlf_gt(uint64 num_rids, ColIdx colIdx, BHandle dest, T value) noexcept {
        uint64* dest_data = Buffer::data(dest);
        uint64* dest_ptr = dest_data;
        cuint64 num_sorted_elems = columns_num_sorted_elems;
        Column& my_col = the_columns[colIdx];
        uint64 c_found = 0;

        {
            QElem* src_data = my_col.get_sorted();
            QElem* src_data_end = src_data + columns_num_sorted_elems;

            // first looks for "value" in sorted elements
            //
            if (num_sorted_elems > 0) {
                auto iter = std::upper_bound(src_data, src_data_end, QElem(value, std::numeric_limits<uint64>::max()), compare_QElem_by_value);

                while (iter != src_data_end) {
                    assert(iter->value > value);
                    cRID rid = iter->rid;

                    if (!IS_COLUMN_IDX_NULL(rid, colIdx) && IS_RID_NOT_DELETED(rid)) {
                        *dest_ptr++ = rid;
                        c_found++;
                    }
                    iter++;
                }
            }
        }

        INT64* src_data = my_col.get_all_elems_by_rid();

        // then look in unsorted ones
        for (RID rid = columns_num_sorted_elems; rid < num_rids; rid++) {
            T val = reinterpret<T>(src_data[rid]);

            if ((val > value) && (!IS_COLUMN_IDX_NULL(rid, colIdx)) && IS_RID_NOT_DELETED(rid)) {
                *dest_ptr++ = rid;
                c_found++;
            }
        }

        return c_found;
    }

    template <typename T> uint64 wlf_lt(uint64 num_rids, ColIdx colIdx, BHandle dest, T value) noexcept {
        uint64* dest_data = Buffer::data(dest);
        uint64* dest_ptr = dest_data;
        cuint64 num_sorted_elems = columns_num_sorted_elems;
        Column& my_col = the_columns[colIdx];
        uint64 c_found = 0;

        {
            QElem* src_data = my_col.get_sorted();
            QElem* src_data_end = src_data + columns_num_sorted_elems;

            // first look in sorted elements
            //
            if (num_sorted_elems > 0) {
                auto iter = std::lower_bound(src_data, src_data_end, QElem(value, 0), compare_QElem_by_value);

                for (auto cur = src_data; cur != iter; cur++) {
                    cRID rid = cur->rid;

                    if (!IS_COLUMN_IDX_NULL(rid, colIdx) && IS_RID_NOT_DELETED(rid)) {
                        *dest_ptr++ = rid;
                        c_found++;
                    }
                }
            }
        }

        INT64* src_data = my_col.get_all_elems_by_rid();

        // then look in unsorted ones
        for (RID rid = columns_num_sorted_elems; rid < num_rids; rid++) {
            T val = reinterpret<T>(src_data[rid]);

            if ((val < value) && (!IS_COLUMN_IDX_NULL(rid, colIdx)) && IS_RID_NOT_DELETED(rid)) {
                *dest_ptr++ = rid;
                c_found++;
            }
        }

        return c_found;
    }

    template <typename T> uint64 wlf_lte(uint64 num_rids, ColIdx colIdx, BHandle dest, T value) noexcept {
        uint64* dest_data = Buffer::data(dest);
        uint64* dest_ptr = dest_data;
        cuint64 num_sorted_elems = columns_num_sorted_elems;
        Column& my_col = the_columns[colIdx];
        uint64 c_found = 0;

        {
            QElem* src_data = my_col.get_sorted();
            QElem* src_data_end = src_data + columns_num_sorted_elems;

            // first look in sorted elements
            //
            if (num_sorted_elems > 0) {
                auto iter = std::upper_bound(src_data, src_data_end, QElem(value, std::numeric_limits<uint64>::max()), compare_QElem_by_value);

                for (auto cur = src_data; cur != iter; cur++) {
                    cRID rid = cur->rid;

                    if (!IS_COLUMN_IDX_NULL(rid, colIdx) && IS_RID_NOT_DELETED(rid)) {
                        *dest_ptr++ = rid;
                        c_found++;
                    }
                }
            }
        }

        INT64* src_data = my_col.get_all_elems_by_rid();

        // then look in unsorted ones
        for (RID rid = columns_num_sorted_elems; rid < num_rids; rid++) {
            T val = reinterpret<T>(src_data[rid]);

            if ((val <= value) && (!IS_COLUMN_IDX_NULL(rid, colIdx)) && IS_RID_NOT_DELETED(rid)) {
                *dest_ptr++ = rid;
                c_found++;
            }
        }

        return c_found;
    }

    string remove_percent(const string& str) {
        cchar* ptr = str.c_str();
        uint64 len = str.length();

        bool start_perc = (ptr[0] == '%');
        bool end_perc = (ptr[len - 1] == '%');

        uint64 real_len = len - (start_perc ? 1 : 0) - (end_perc ? 1 : 0);

        return str.substr(start_perc ? 1 : 0, real_len);
    }

    uint64 wslf_string_pattern(ColIdx colIdx, BHandle dest, const StringToken& stok, uint64 num_rids) {
        auto pattern = tokenizedStrings.getString(stok);
        vector<StringToken> searchValues = tokenizedStrings.findPattern(pattern);

        uint64 c_found = 0;

        for (auto& searchValue : searchValues) {
            uint64 value = searchValue.getFullToken();
            uint64 found_now = wlf_eq(num_rids, colIdx, dest, value, c_found);
            c_found += found_now;
        }

        return c_found;
    }

    uint64 wslf_string_eq_case_sensitive(ColIdx colIdx, BHandle dest, const StringToken& stok, uint64 num_rids) noexcept {
        uint64 searchValue = stok.getFullToken();

        return wlf_eq(num_rids, colIdx, dest, reinterpret<INT64>(searchValue));
    }

    LIF getIntFilterFromOp(WhereNumericLeafFilter op) {
        switch (op) {
        case EQ:
            return [&](uint64 num_rids, ColIdx colIdx, BHandle dest, INT64 value) {
                return wlf_eq(num_rids, colIdx, dest, value);
            };
        case NEQ:
            return [&](uint64 num_rids, ColIdx colIdx, BHandle dest, INT64 value) {
                return wlf_neq<INT64>(num_rids, colIdx, dest, value);
            };
        case GT:
            return [&](uint64 num_rids, ColIdx colIdx, BHandle dest, INT64 value) {
                return wlf_gt<INT64>(num_rids, colIdx, dest, value);
            };
        case GE:
            return [&](uint64 num_rids, ColIdx colIdx, BHandle dest, INT64 value) {
                return wlf_gte<INT64>(num_rids, colIdx, dest, value);
            };
        case LT:
            return [&](uint64 num_rids, ColIdx colIdx, BHandle dest, INT64 value) {
                return wlf_lt<INT64>(num_rids, colIdx, dest, value);
            };
        case LE:
            return [&](uint64 num_rids, ColIdx colIdx, BHandle dest, INT64 value) {
                return wlf_lte<INT64>(num_rids, colIdx, dest, value);
            };
        default:
            assert(UNREACHED);
            throw MEMURAI_EXCEPTION(InternalError, "Invalid integer leaf filter operation.");
        }
    }

    LDF getFloatFilterFromOp(WhereNumericLeafFilter op) {
        switch (op) {
        case EQ:
            return [&](uint64 num_rids, ColIdx colIdx, BHandle dest, double value) {
                auto value_int = reinterpret<INT64>(value);
                return wlf_eq(num_rids, colIdx, dest, value_int);
            };
        case GT:
            return [&](uint64 num_rids, ColIdx colIdx, BHandle dest, double value) {
                return wlf_gt<double>(num_rids, colIdx, dest, value);
            };
        case GE:
            return [&](uint64 num_rids, ColIdx colIdx, BHandle dest, double value) {
                return wlf_gte<double>(num_rids, colIdx, dest, value);
            };
        case LT:
            return [&](uint64 num_rids, ColIdx colIdx, BHandle dest, double value) {
                return wlf_lt<double>(num_rids, colIdx, dest, value);
            };
        case LE:
            return [&](uint64 num_rids, ColIdx colIdx, BHandle dest, double value) {
                return wlf_lte<double>(num_rids, colIdx, dest, value);
            };
        default:
            assert(UNREACHED);
            throw MEMURAI_EXCEPTION(InternalError, "Invalid float leaf filter operation.");
        }
    }

    WSLF getStringFilterFromOp(WhereStringFilter op) {
        switch (op) {
        case EQ_CASE_SENSITIVE:
            return [&](ColIdx colIdx, BHandle dest, StringToken stok, uint64 num_rids) {
                return wslf_string_eq_case_sensitive(colIdx, dest, stok, num_rids);
            };
            break;
        case PATTERN:
            return [&](ColIdx colIdx, BHandle dest, StringToken stok, uint64 num_rids) {
                return wslf_string_pattern(colIdx, dest, stok, num_rids);
            };
            break;
        default:
            assert(UNREACHED);
            throw MEMURAI_EXCEPTION(InternalError, "Invalid string leaf filter operation.");
        }
    }

    // Non-filter all ANDs
    //
    // assume each src is a sorted list of RIDs!!
    static BHandle nleaf_AND(VecConstHandles srcs, JOBID jobId);

    static BHandle nleaf_OR(VecConstHandles srcs, JOBID jobId);

    RIDFilter getNFilterFromOp(WhereRIDFilter op, JOBID jobId) {
        switch (op) {
        case AND:
            return [this](VecConstHandles srcs, JOBID jobId) -> BHandle {
                return nleaf_AND(std::move(srcs), jobId);
            };
        case OR:
            return [this](VecConstHandles srcs, JOBID jobId) -> BHandle {
                return nleaf_OR(std::move(srcs), jobId);
            };
        default:
            assert(UNREACHED);
            throw MEMURAI_EXCEPTION(InternalError, "Invalid N filter from operation.");
        }
    }

    void addRedisPrimaryKeyIfNeeded(const vector<FullyQualifiedCID>& fqcids);

private:
    static const string REDIS_PRIMARY_KEY;
    static const string DATE_PARSING_ISO_8601_MSG;
    static constexpr CID REDIS_PRIMARY_KEY_CID = 0;

    // Configuration
    const Config config;
    uint64 sort_threshold;

    IServer& server;

    // Table ID and name
    TID tid;
    const string table_name;
    const string prefix;

    // Here are columns names, types and CIDs
    // I guess names are only needed for serialization.
    vector<FullyQualifiedCID> colsInfo;
    VecCID colCids;
    map<CID, uint32> columns_cids_to_idx;
    const vector<string> colNamesVec;
    vector<string> colNamesNoPKeyVec;
    map<string, CID> colNames2CID;

    // Number of columns. Convenience member for columns.size()
    // uint64              num_proper_columns;
    uint64 num_all_columns;

#define NUM_PROPER_COLUMNS (num_all_columns - 1)

    // This is important, it needs to change when "grow"ing.
    uint64 columns_capacity;
    uint64 columns_num_elems;
    // sorted elems are a fraction of unsorted ones
    uint64 columns_num_sorted_elems;

    // Here's the data, in columns
    vector<Column> the_columns;

    // Used at ingestion, to search whether a key is present or not
    map<string, RID> primary_keys;

    // here's the metadata.
    // Bit 63 for each entry tells whether the RID is present (0) or deleted (1).
    // Other 63 bits tell you whether the various columns for each entry are present (0) or deleted (1).
    vector<uint64> metadata;

    // Here are the strings
    TokenizedStrings tokenizedStrings;

    // Next write goes here.
    uint64 n_next_rid;

    // Keeping track of currente deletions to speed up approx size
    uint64 n_completed_deletes;

    // epoch value
    static std::chrono::system_clock::time_point tp_epoch;
    static std::locale my_locale;

    mutable atomic<uint64> s_active_select_queries;
    mutable atomic<uint64> s_active_add_commands;

    // Metrics!
    mutable atomic<uint64> c_select_stmts;
    mutable atomic<uint64> c_update_stmts;
    mutable atomic<uint64> c_delete_stmts;
    mutable atomic<uint64> c_add_row;
    mutable atomic<uint64> c_modify_row;
    mutable atomic<uint64> c_grow_event, c_sort_events;

protected:
    // Atomic values holding number of threads currently engaged in these operations
    atomic<uint64> s_non_mutating_commands;

#ifdef TEST_BUILD
    bool no_asserts_on_sort;
#endif

    Column& getColumn(CID cid) {
        auto iter = columns_cids_to_idx.find(cid);
        EXPECT_OR_THROW(iter != columns_cids_to_idx.end(), InternalError, "Bad CID in IdxTable::getColumn.");
        EXPECT_OR_THROW(iter->second < num_all_columns, InternalError, "Bad CID in IdxTable::getColumn..");
        uint64 idx = iter->second;
        return the_columns[idx];
    }

    // Doesn't get more clear than this
    //
    INT64 slow_access(Column& col, RID rid) const {
        assert(rid < columns_num_sorted_elems + columns_num_elems);
        // TO DO: use direct L1 load
        return col.get_all_elems_by_rid()[rid];
    }

    bool acceptableCIDs(const vector<CID>& cids_orig) const {
        if (cids_orig.empty())
            return true;

        auto cids = cids_orig;
        std::sort(cids.begin(), cids.end());

        for (auto c : cids) {
            if (columns_cids_to_idx.end() == columns_cids_to_idx.find(c)) {
                return false;
            }
        }

        return true;
    }

    // returns number of RIDs actually deleted
    uint64 hard_delete(const vector<RID>& rids) {
        // TBD: sort rids in case of size greater than some threshold (5?)
        // To be a bit more cache friendly
        uint64 localDeleted = 0;

        for (auto rid : rids) {
            if (rid < n_next_rid) {
                if (!IS_RID_DELETED(rid)) {
                    DELETE_RID(rid);
                    localDeleted++;
                }
            }
        }

        n_completed_deletes += localDeleted;

        return localDeleted;
    }

    void resetThresholds() {
        sort_threshold = config.sortStrategy.calculateThreshold(columns_capacity, columns_num_sorted_elems);
    }
#ifdef TEST_BUILD

    void test_compute(const Lambda& lambda) {
        LOG_DEBUG("IdxTable::test_compute starting.");

        shared_table_lock<P_PMURWL> mlock((P_PMURWL*)this);
        CHECK_STOPPING;

        LOG_DEBUG("IdxTable::test_compute about to start lambda.");
        lambda();
        LOG_DEBUG("IdxTable::test_compute about to lambda done.");
    }

#endif

    JOBID getProjectJOBID() const {
        return 1;
    }

    VecHandles prepareBufferVec(JOBID jobId, uint64 numcols, uint64 num_elems_per_col) {
        // Create 'numcols' Buffers
        vector<BHandle> results(numcols, 0ull);

        for (uint64 i = 0; i < numcols; i++) {
            results[i] = Buffer::create(num_elems_per_col);
        }

        return results;
    }

    // Merge and sort unsorted data into sorted array.
    // If needed, grows the columns datastores according to the chosen strategy
    //
    // void sort_and_grow();

    vector<uint64> vec_cids_to_idxs(VecCID cids);

    void sort_all_columns();
    void sort_columns(VecCID modifiedCids, bool allColumns);
    void grow();

    void projectOneColumn(BHandle results, ColIdx colIdx, BHandle rids) noexcept;

    uint64 calculate_max_num_rids(WhereRIDFilter op, vector<uint64> tmp) {
        if (op == WhereRIDFilter::AND) {
            return *std::max_element(tmp.begin(), tmp.end());
        } else if (op == WhereRIDFilter::OR) {
            uint64 ret = 0;
            for (auto v : tmp) {
                ret += v;
            }
            return ret;
        }

        assert(0);
        EXPECT_OR_THROW(false, InternalError, "RID filter operator not supported in where node " << op);
    }

    static int64 create_millis_from_epoch(std::tm& timeinfo, int millis);
    static tuple<int64, int64> get_timeinfo_from_millis(INT64 value);

public:
    // Returns 64 bit representaton of a double. No big change actually for now.
    static INT64 serializeDouble(double val) noexcept {
        return reinterpret<INT64>(val);
    }

    // Serialize a string date into a 64 bit number
    // Expecting date as "YYY-MM-DD". Dash in between.
    // Where "day" is a number, possibly zero leaded.
    // "month" is either a number, possibly zero leaded, or a month string, possibly abbreviated.
    // Good dates examples:
    //  "2010-12-18"
    //
    static optional<INT64> serialize_date(cchar* date) noexcept;

    static optional<INT64> serialize_timestamp(cchar* timestamp) noexcept;

    static void print_date(INT64 value, std::stringstream& ss) noexcept;

    static void print_timestamp(INT64 value, std::stringstream& ss) noexcept;

private:
    vector<uint64> nulls_per_column;

    optional<UINT64> serialize_from_string(const string& value, ColumnType coltype);

    static bool get_time(std::istringstream& ss, const string& format, std::tm* timeinfo);

    VecCID core_modify_row(RID rid, const vector<CID>& cids, const vector<UINT64>& values) {
        uint64 nulls = 0;
        uint64 bit = 1;
        const uint64 num_row_cids = cids.size();
        VecCID modifiedCids;

        for (auto cid : colCids) {
            uint32 idx = 0;
            while ((idx < num_row_cids) && (cids[idx] != cid))
                idx++;

            uint64 value = 0;
            Column& col = getColumn(cid);

            if (idx >= num_row_cids) {
                nulls |= bit;
            } else {
                value = values[idx];
            }

            uint64 prev_val = col.get_all_elems_by_rid()[rid];
            if (prev_val != value) {
                modifiedCids.push_back(cid);
            }
            col.get_all_elems_by_rid()[rid] = value;
            bit <<= 1;
        }
        metadata[rid] = nulls;

        return modifiedCids;
    }

    void notification_add_modify(void* ctx, const char* key);

    // re-entrant method
    //
    // Add one row to ALL columns. If not present, adds null.
    // Returns success
    bool addRow(const vector<CID>& cids, const vector<UINT64>& values, RID& rid) {
        // Acquire a rid.
        rid = n_next_rid++;
        columns_num_elems++;

        core_modify_row(rid, cids, values);

        if (columns_num_elems == columns_capacity) {
            syncMutateUpgrade([&]() {
                assert(s_active_add_commands.load() == 1);
                assert(s_active_select_queries.load() == 0);
                EXPECT_OR_THROW(s_active_select_queries.load() == 0, InternalError, "Select queries still running while trying to grow table. " << table_name);
                EXPECT_OR_THROW(s_active_add_commands.load() == 1, InternalError, "addRow commands still running while trying to grow table. " << table_name);
                sort_all_columns();
                grow();
            });
        } else if (columns_num_elems == sort_threshold) {
            syncMutateUpgrade([&]() {
                assert(s_active_add_commands.load() == 1);
                assert(s_active_select_queries.load() == 0);
                EXPECT_OR_THROW(s_active_select_queries.load() == 0, InternalError, "Select queries still running while trying to sort table. " << table_name);
                EXPECT_OR_THROW(s_active_add_commands.load() == 1, InternalError, "addRow commands still running while trying to sort table. " << table_name);
                sort_all_columns();
            });
        }

        // metric
        c_add_row++;

        return true;
    }

public:
    // ctor
    //
    IdxTable(const string& table_name, const string& prefix, const vector<FullyQualifiedCID>& fqcids, const Config& config, IServer& server, TID tid);

    // ctor - for deserialization
    // TO DO

    ~IdxTable() = default;

#define FOR_EACH_PROPER_COLUMN(I) for (uint32 I = 0; I < num_all_columns - 1; I++)

#define FOR_EACH_COLUMN(I) for (uint32 I = 0; I < num_all_columns; I++)

    // Return QCIDs - WITHOUT Primary Key
    //
    vector<QualifiedCID> getQualifiedCIDs() const {
        shared_table_lock<P_PMURWL> mlock((P_PMURWL*)this);

        vector<QualifiedCID> qualifieds_cids;
        for (auto c : columns_cids_to_idx) {
            if (c.first != REDIS_PRIMARY_KEY_CID) {
                qualifieds_cids.emplace_back(c.first, get<2>(colsInfo[c.second]));
            }
        }
        return std::move(qualifieds_cids);
    }

    // Return QCIDs - WITHOUT Primary Key
    //
    vector<FullyQualifiedCID> getFullyQualifiedCIDs() const {
        shared_table_lock<P_PMURWL> mlock((P_PMURWL*)this);

        vector<FullyQualifiedCID> fqcids;
        for (auto c : columns_cids_to_idx) {
            if (c.first != REDIS_PRIMARY_KEY_CID) {
                fqcids.push_back(colsInfo[c.second]);
            }
        }
        return std::move(fqcids);
    }

    vector<CID> getCIDs() const {
        shared_table_lock<P_PMURWL> mlock((P_PMURWL*)this);

        vector<CID> columns_cids;

        for (auto c : columns_cids_to_idx) {
            if (c.first != REDIS_PRIMARY_KEY_CID) {
                columns_cids.push_back(c.first);
            }
        }
        return std::move(columns_cids);
    }

    uint64 getNumRowsApprox() const {
        return n_next_rid - n_completed_deletes;
    }

    uint64 getNextRid() const {
        return n_next_rid;
    }

    uint64 get_c_sort_events() const {
        return c_sort_events;
    }

    /*
        Table can be:
         - working
         - shutting down


       If working.
       - State 1:
           - adds permitted.
           - queries permitted.
       - State 2:
           - adds not permitted.
           - queries not permitted.
           - growing or sorting going on.
    */

    // re-entrant method
    //
    void delete_keys(const vector<string>& keys);

private:
    VecCID modifyKeyCore(RID rid, const string& key, const vector<CID>& cids, const vector<UINT64>& values) {
        VecCID modifiedCids;

        assert(s_active_select_queries.load() == 0);
        EXPECT_OR_THROW(s_active_select_queries.load() == 0, InternalError, "Select queries still running while modifying data in table " << table_name);
        if (rid < n_next_rid) {
            modifiedCids = core_modify_row(rid, cids, values);
        } else {
            LOG_VERBOSE("modifyKeyCore ignored becasue bad RID.");
        }

        // metric
        c_modify_row++;

        return modifiedCids;
    }

public:
    // re-entrant method
    //
    VecCID upinsertKey(const string& key, const vector<CID>& cids, const vector<UINT64>& values) {
        assert(cids.size() == values.size());
        assert(acceptableCIDs(cids));
        VecCID modifiedCids;
        bool doAdd = false;

        perform_simple_operation([&]() {
            auto iter = primary_keys.find(key);
            if (iter == primary_keys.end()) {
                doAdd = true;
                return;
            }

            RID rid = iter->second;

            modifiedCids = modifyKeyCore(rid, key, cids, values);
        });

        if (doAdd) {
            if (addKey(key, cids, values)) {
                return colCids;
            }
        }

        return modifiedCids;
    }

    // re-entrant!
    //
    bool addKey(string key, const vector<CID>& cids, const vector<UINT64>& values) {
        assert(cids.size() == values.size());
        assert(acceptableCIDs(cids));

        bool ret;

        bool done = perform_simple_operation([&]() {
            IncrementDuringScope guard2(s_active_add_commands);
            RID rid;
            ret = addRow(cids, values, rid);
            if (ret) {
                primary_keys.insert(make_pair(key, rid));
            } else {
                LOG_ERROR("Internal Error: Could not add row to table " << table_name << " !!");
            }
        });

        return (done && ret);
    }

    map<string, uint64> getMetrics() const {
        map<string, uint64> ret;

        ret.insert(make_pair("active computes", getActiveComputes()));
        ret.insert(make_pair("addrow cnt", c_add_row.load()));
        ret.insert(make_pair("modrow cnt", c_modify_row.load()));

        return std::move(ret);
    }

    StringToken tokenizeString(const string& str) {
        return tokenizedStrings.addString(str.c_str(), str.length());
    }

    string token2string(const StringToken& stok) const {
        return tokenizedStrings.getString(stok);
    }

    TID getTID() const {
        return tid;
    }

    QueryInterface getQueryInterface() {
        return {queryInterface};
    }

    void keyspace_notification(OuterCtx ctx, const char* key, const char* event);

    struct ScanRet {
        uint64 num_keys;
        vector<string> msgs;
    };

    // Scans main Redis for keys with given prefix, and add them to the table
    //
    // Returns number of keys found.
    ScanRet scan(OuterCtx ctx, const char* prefix);

    vector<tuple<string, string>> getDebugInfo() const;

    tuple<string, vector<FullyQualifiedCID>> getInfo() const;

    void sortRidsByCids(BHandle handle, const vector<tuple<QCID, ASCDESC>>& sorting_cols);

    // Stoppable::join
    // Must be called after "Stoppable::stop"
    void join() final;

    static bool isValidTableName(const string& key);

    optional<QCID> translateColumnName(const string& colname);

    Config getConfig() const {
        return config;
    }

    static vector<uint64> SerializeConfig(const Config&);

    static Config DeserializeConfig(const vector<uint64>&);

    string get_table_name() const {
        return table_name;
    }

    void clear();
};

} // namespace memurai::sql

#ifndef IDXTABLE_CPP

#undef WAIT_GROWTH
#undef COMPUTING_METHOD
#undef MUTATING_METHOD
#undef CHECK_EXIT
#undef FOR_EACH_COLUMN
#undef DELETE_RID
#undef IS_RID_DELETED
#undef NUM_PROPER_COLUMNS

#endif