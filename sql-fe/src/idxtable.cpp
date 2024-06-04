/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#include <chrono>
#include <set>
#include <sstream>
#include <tuple>

#define IDXTABLE_CPP
#include "idxtable.h"

#include "concurrent_foreach_column.h"
#include "gmtime.h"
#include "scope/take_time.h"
#include "threads/cancellation.h"

namespace memurai::sql {

using std::set;
using std::stringstream;
using std::tie;

const string IdxTable::NULL_STRING = "";
std::chrono::system_clock::time_point IdxTable::tp_epoch;
std::locale IdxTable::my_locale;
const string IdxTable::REDIS_PRIMARY_KEY = "redis_key";

const string IdxTable::DATE_PARSING_ISO_8601_MSG = "Please use YYYY-MM-DD ISO 8601 format.";

void IdxTable::addRedisPrimaryKeyIfNeeded(const vector<FullyQualifiedCID>& fqcids) {
    bool present = false;

    for (auto ci : colsInfo) {
        if (get<0>(ci) == REDIS_PRIMARY_KEY) {
            present = true;
            break;
        }
    }

    if (present) {
        bool other = false;
        // check other data too
        for (auto cid : colCids) {
            if (cid == REDIS_PRIMARY_KEY_CID) {
                other = true;
                break;
            }
        }
        EXPECT_OR_THROW(other, InternalError, "RedisPrimaryKey column not found in colCids during restoring.");
    } else {
        num_all_columns++;
        colsInfo.insert(colsInfo.begin(), FQCID{REDIS_PRIMARY_KEY, REDIS_PRIMARY_KEY_CID, ColumnType::TEXT});
        colCids.insert(colCids.begin(), REDIS_PRIMARY_KEY_CID);
    }
}

// ctor - for first creation
//
IdxTable::IdxTable(const string& table_name, const string& prefix, const vector<FullyQualifiedCID>& fqcids, const Config& config, IServer& server, TID tid)
    : server(server),
      prefix(prefix),
      config(config),
      table_name(table_name),
      n_next_rid(0),
      tid(tid),
      colsInfo(fqcids),
      num_all_columns(fqcids.size()),
      colCids(Column::projectCIDs(fqcids)),
      colNamesVec(Column::projectColNames(fqcids)),
      colNamesNoPKeyVec(Column::projectColNames(fqcids)),
      columns_capacity(config.columnCapacity),
      columns_num_elems(0),
      columns_num_sorted_elems(0),
      n_completed_deletes(0),
      c_grow_event(0),
      c_sort_events(0),
      s_active_add_commands(0),
      s_active_select_queries(0),
      P_PMURWL(*(Stoppable*)this) {
    resetThresholds();

    addRedisPrimaryKeyIfNeeded(fqcids);

    colNames2CID.insert(make_pair(REDIS_PRIMARY_KEY, REDIS_PRIMARY_KEY_CID));
    for (auto fq : fqcids) {
        colNames2CID.insert(make_pair(get<0>(fq), get<1>(fq)));
    }

    nulls_per_column.resize(num_all_columns);

    metadata.resize(columns_capacity);
    std::fill(metadata.begin(), metadata.end(), 0);

    the_columns.resize(num_all_columns);

    FOR_EACH_COLUMN(idx) {
        CID cid = get<1>(colsInfo[idx]);
        columns_cids_to_idx.insert(make_pair(cid, idx));
        auto col_name = get<0>(colsInfo[idx]);
        colNames2CID.insert(make_pair(col_name, cid));

        the_columns[idx].grow(columns_capacity, columns_num_sorted_elems);
    }

    queryInterface.ridFilter = [this](WhereRIDFilter op, VecConstHandles rids, JOBID jobId) -> BHandle {
        RIDFilter filter = this->getNFilterFromOp(op, jobId);

        vector<uint64> tmp;

        std::transform(rids.begin(), rids.end(), std::back_inserter(tmp), [](ConstBHandle h) {
            return Buffer::num_elems(h);
        });

        std::sort(rids.begin(), rids.end(), [](BHandle l, BHandle r) {
            return Buffer::num_elems(l) < Buffer::num_elems(r);
        });

        cuint64 max_num_rids = calculate_max_num_rids(op, tmp);

        return filter(rids, jobId);
    };

    queryInterface.leafFilterString = [this](WhereStringFilter op, StringToken stok, CID cid, JOBID jobId, uint64 num_rids) -> BHandle {
        auto iter = columns_cids_to_idx.find(cid);
        assert(iter != columns_cids_to_idx.end());
        EXPECT_OR_THROW(iter != columns_cids_to_idx.end(), InternalError, "CID " << cid << " not found when executing query (leafStringFilter).");
        uint32 idx = iter->second;
        assert(get<2>(colsInfo[idx]) == ColumnType::TEXT);
        EXPECT_OR_THROW(get<2>(colsInfo[idx]) == ColumnType::TEXT, InternalError, "leafStringFilter called on non-text column.");
        BHandle dest = Buffer::create(num_rids);
        WSLF filter = getStringFilterFromOp(op);

        uint64 num_elems = 0;

        // auto dur_filter_us = TAKE_TIME_US{
        num_elems = filter(idx, dest, stok, num_rids);
        Buffer::set_num_elems(dest, num_elems);
        //};

        // auto dur_sort_us = TAKE_TIME_US{
        auto ptr_start = Buffer::data(dest);
        auto ptr_end = ptr_start + num_elems;
        std::sort(ptr_start, ptr_end);
        //};

        // LOG_PERF("leafFilterString  " << op << " filtering took: " << dur_filter_us << "us.");
        // LOG_PERF("leafFilterString  " << op << " sorting took:   "   << dur_sort_us << "us.");

        return dest;
    };

    queryInterface.leafFilterFloat = [this](WhereNumericLeafFilter op, double value, CID cid, JOBID jobId, uint64 num_rids) -> BHandle {
        auto iter = columns_cids_to_idx.find(cid);
        assert(iter != columns_cids_to_idx.end());
        EXPECT_OR_THROW(iter != columns_cids_to_idx.end(), InternalError, "CID " << cid << " not found when executing query (leafFilterFloat).");
        uint32 idx = iter->second;
        assert(get<2>(colsInfo[idx]) != ColumnType::TEXT);
        EXPECT_OR_THROW(get<2>(colsInfo[idx]) != ColumnType::TEXT, InternalError, "leafFilterFloat called for text column.");
        BHandle dest = Buffer::create(num_rids);
        LDF filter = this->getFloatFilterFromOp(op);

        uint64 num_elems = 0;
        // TO DO!!! Insert sorting while filtering

        // auto dur_filter_us = TAKE_TIME_US{
        num_elems = filter(num_rids, idx, dest, value);
        Buffer::set_num_elems(dest, num_elems);
        //};

        // auto dur_sort_us = TAKE_TIME_US{
        auto ptr_start = Buffer::data(dest);
        auto ptr_end = ptr_start + num_elems;
        std::sort(ptr_start, ptr_end);
        //};

        // LOG_PERF("leafFilterFloat  " << op << " filtering took: " << dur_filter_us << "us.");
        // LOG_PERF("leafFilterFloat  " << op << " sorting took:   " << dur_sort_us << "us.");

        return dest;
    };

    queryInterface.leafIsNull = [this](CID cid, JOBID jobId, uint64 num_rids) -> BHandle {
        auto iter = columns_cids_to_idx.find(cid);
        assert(iter != columns_cids_to_idx.end());
        EXPECT_OR_THROW(iter != columns_cids_to_idx.end(), InternalError, "CID " << cid << " not found when executing query (leafIsNull).");
        uint32 idx = iter->second;
        BHandle dest = Buffer::create(num_rids);

        uint64* dest_data = Buffer::data(dest);
        uint64* dest_ptr = dest_data;
        uint64 num_found = 0;

        for (RID rid = 0; rid < num_rids; rid++) {
            if (IS_COLUMN_IDX_NULL(rid, idx)) {
                *dest_ptr++ = rid;
                num_found++;
            }
        }

        Buffer::set_num_elems(dest, num_found);
        return dest;
    };

    queryInterface.leafIsNotNull = [this](CID cid, JOBID jobId, uint64 num_rids) -> BHandle {
        auto iter = columns_cids_to_idx.find(cid);
        assert(iter != columns_cids_to_idx.end());
        EXPECT_OR_THROW(iter != columns_cids_to_idx.end(), InternalError, "CID " << cid << " not found when executing query (leafIsNotNull).");
        uint32 idx = iter->second;
        BHandle dest = Buffer::create(num_rids);

        uint64* dest_data = Buffer::data(dest);
        uint64* dest_ptr = dest_data;
        uint64 num_found = 0;

        for (RID rid = 0; rid < num_rids; rid++) {
            if (!IS_COLUMN_IDX_NULL(rid, idx)) {
                *dest_ptr++ = rid;
                num_found++;
            }
        }

        Buffer::set_num_elems(dest, num_found);
        return dest;
    };

    queryInterface.leafFilterInt = [this](WhereNumericLeafFilter op, INT64 value, CID cid, JOBID jobId, uint64 num_rids) -> BHandle {
        auto iter = columns_cids_to_idx.find(cid);
        assert(iter != columns_cids_to_idx.end());
        EXPECT_OR_THROW(iter != columns_cids_to_idx.end(), InternalError, "CID " << cid << " not found when executing query (leafFilterInt).");
        uint32 idx = iter->second;
        assert(get<2>(colsInfo[idx]) != ColumnType::TEXT);
        EXPECT_OR_THROW(get<2>(colsInfo[idx]) != ColumnType::TEXT, InternalError, "leafFilterInt called for text column.");
        BHandle dest = Buffer::create(num_rids);

        LIF filter = this->getIntFilterFromOp(op);

        uint64 num_elems = 0;

        // auto dur_filter_us = TAKE_TIME_US{
        num_elems = filter(num_rids, idx, dest, value);
        Buffer::set_num_elems(dest, num_elems);
        //};

        // auto dur_sort_us = TAKE_TIME_US{
        auto ptr_start = Buffer::data(dest);
        auto ptr_end = ptr_start + num_elems;
        std::sort(ptr_start, ptr_end);
        //};

        // LOG_PERF("leafFilterInt  " << op << " filtering took: " << dur_filter_us << "us.");
        // LOG_PERF("leafFilterInt  " << op << " sorting took:   " << dur_sort_us << "us.");

        return dest;
    };

    value_printer = [this](INT64 value, ColumnType ctype, stringstream& ss) {
        switch (ctype) {
        case ColumnType::TEXT:
            ss << token2string(StringToken(value));
            break;
        case ColumnType::BIGINT:
            ss << value;
            break;
        case ColumnType::FLOAT:
            ss << reinterpret<double>(value);
            break;
        case ColumnType::DATE:
            print_date(value, ss);
            break;
        case ColumnType::TIMESTAMP:
            print_timestamp(value, ss);
            break;
        case ColumnType::NULLTYPE:
            ss << NULL_STRING;
            break;
        case ColumnType::RIDCOL:
            assert(UNREACHED);
            EXPECT_OR_THROW(false, InternalError, "Unexpected type RIDCOL in serializer.");
        default:
            assert(UNREACHED);
            EXPECT_OR_THROW(false, InternalError, "Unknown type in serializer.");
        }
    };

    queryInterface.allRIDs = [this](JOBID jobId, uint64 num_rids) -> BHandle {
        BHandle dest = Buffer::create(num_rids);
        uint64 numElems = 0;

        for (uint64 i = 0; i < num_rids; i++) {
            if (IS_RID_NOT_DELETED(i)) {
                Buffer::data(dest)[numElems++] = i;
            }
        }

        Buffer::set_num_elems(dest, numElems);

        return dest;
    };

    queryInterface.project = [this](BHandle rids, VecCID cids, IToken* token, IPublisher* pub) -> void {
        const uint64 num_projs = cids.size();
        auto num_rids = Buffer::num_elems(rids);

        // now RID found? Bail out
        if (num_rids == 0) {
            token->done();
            pub->done();
            return;
        }

        // FQcids needed by publisher
        vector<FullyQualifiedCID> fqcids;

        // convert cids to index to the column array
        //
        vector<uint64> proj_idxs;
        for (CID cid : cids) {
            auto iter = columns_cids_to_idx.find(cid);
            EXPECT_OR_THROW(iter != columns_cids_to_idx.end(), InternalError, "CID " << cid << " not found when executing projection.");
            auto idx = iter->second;
            proj_idxs.push_back(idx);
            fqcids.push_back(colsInfo[idx]);
        }

        // project and send/store, row by row
        //
        vector<optional<INT64>> row(num_projs);
        uint64 row_idx = 1;

        pub->publish_header(Column::projectColNames(fqcids));

        //
        for (RID rid : Buffer::BufferRange<RID>(rids)) {
            // TO DO: check every 1'000 (??) for token cancelation
            //
            if (IS_RID_NOT_DELETED(rid)) {
                for (uint64 proj_idx = 0; proj_idx < num_projs; proj_idx++) {
                    uint64 col_idx = proj_idxs[proj_idx];

                    if (IS_COLUMN_IDX_NULL(rid, col_idx)) {
                        row[proj_idx] = std::nullopt;
                    } else {
                        // where is null field?
                        // Where is null RID?
                        row[proj_idx] = slow_access(the_columns[col_idx], rid);
                    }
                }

                pub->publish_row(row, fqcids, value_printer, row_idx++);
            }
        }

        token->done();
        pub->done();
    };
}

vector<uint64> IdxTable::vec_cids_to_idxs(VecCID cids) {
    vector<uint64> ret;
    for (auto item : columns_cids_to_idx) {
        if (find(cids.begin(), cids.end(), item.first) == cids.end()) {
            continue;
        }
        ret.push_back(item.second);
    }
    return ret;
}

void IdxTable::sort_all_columns() {
    return sort_columns(vec_cids_to_idxs(colCids), true);
}

void IdxTable::sort_columns(vector<uint64> modifiedIdxs, bool allColumns) {
    CHECK_STOPPING;

    uint64 num_elems_to_sort = (allColumns) ? columns_num_elems : columns_num_sorted_elems;

    // assert that we have the rw lock?

    auto dur = TAKE_TIME {
        DEBUG_LOG_DEBUG("IdxTable::sort_and_grow starting.");

        // Sorting works in 2 steps:
        // 1- copy unsorted elems at the end of the sorted ones.
        // 2- sort'em all!

        CONCURRENT_FOREACH_COLUMN(colIdx)
        // for (ColIdx colIdx = 0; colIdx < num_columns; colIdx++)
        {

            auto tmp = find(modifiedIdxs.begin(), modifiedIdxs.end(), colIdx);
            if (tmp != modifiedIdxs.end()) {
                the_columns[colIdx].sort(columns_num_sorted_elems, num_elems_to_sort, (num_elems_to_sort > columns_num_sorted_elems));
#ifdef TEST_BUILD
                // check if sorted
                auto sorted_data = the_columns[colIdx].get_sorted();
                bool ret = std::is_sorted(sorted_data, sorted_data + num_elems_to_sort, compare_QElem_by_value);
                assert(ret);
#endif
            }
        };

        columns_num_sorted_elems = num_elems_to_sort;

        c_sort_events++;

        DEBUG_LOG_DEBUG("IdxTable::sort_and_grow. Sort done. " << c_grow_event << " growths, " << c_sort_events << " sorts.");
    };

    LOG_PERF("IdxTable::sort took " << dur.millis() << "ms for " << num_elems_to_sort << " elems.");
}

void IdxTable::grow() {
    // reason for this assertion: "grow" expects to happen after a sort
    assert(columns_num_elems == columns_num_sorted_elems);

    auto dur = TAKE_TIME {
        DEBUG_LOG_DEBUG("IdxTable::grow starting.");

        uint64 old_capacity = columns_capacity;
        columns_capacity = this->config.growthStrategy.grow(old_capacity);
        assert(columns_capacity > old_capacity);

        Token token("idxtable_grow", 0, num_all_columns);

        FOR_EACH_COLUMN(colIdx) {
            // Note that we capture "colIdx" by value, do NOT capture it by ref!
            // Because in a moment it will become one too big :)
            server.schedule_once_with_token(
                [this, colIdx]() {
                    the_columns[colIdx].grow(columns_capacity, columns_num_sorted_elems);
                },
                &token);
        }

        // do the metadata work in this thread
        metadata.resize(columns_capacity, 0);

        DEBUG_LOG_DEBUG("IdxTable::grow waiting for token.");
        token.join();

        resetThresholds();

        c_grow_event++;
        DEBUG_LOG_DEBUG("IdxTable::grow done. " << c_grow_event << " growths, " << c_sort_events << " sorts.");
    };

    LOG_PERF("IdxTable::grow took " << dur.millis() << "ms for " << columns_num_sorted_elems << " elems.");
}

void IdxTable::projectOneColumn(BHandle results, ColIdx colIdx, BHandle rids) noexcept {
    uint64 rids_num_elems = Buffer::num_elems(rids);
    assert(Buffer::num_elems(results) >= rids_num_elems);
    const uint64* rids_data = Buffer::const_data(rids);
    auto results_data = Buffer::data(results);

    assert(std::is_sorted(rids_data, rids_data + rids_num_elems));

    Column& mycol = the_columns[colIdx];
    auto unsorted_elems = mycol.get_all_elems_by_rid();

    // Critical loop!!!
    for (uint64 idx = 0; idx < rids_num_elems; idx++) {
        RID rid = rids_data[idx];
        auto value = unsorted_elems[rid];
        results_data[idx] = value;
    }
}

// Non-filter all ANDs
//
// assume each src is a sorted list of RIDs!!
BHandle IdxTable::nleaf_AND(VecConstHandles srcs, JOBID jobId) {
    auto R = rand();
    auto first_src_size = Buffer::num_elems(srcs[0]);

    // check corner case
    if (srcs.size() == 1) {
        assert(UNREACHED);
        return srcs[0];
    }

    BHandle results = Buffer::create(first_src_size);
    RID* results_ptr = Buffer::data(results);

    uint64 durTotal = 0;
    uint64 cntTotal = 0;

    vector<ColIdx> srcs_idxs(srcs.size());
    vector<uint64> srcs_sizes(srcs.size());
    vector<RID*> srcs_ptrs(srcs.size());

    for (uint64 i = 0; i < srcs.size(); i++) {
        srcs_idxs[i] = 0;
        srcs_sizes[i] = Buffer::num_elems(srcs[i]);
        srcs_ptrs[i] = Buffer::data(srcs[i]);

        LOG_DEBUG("==== nleaf_AND:  size of buffer " << i << " is " << srcs_sizes[i]);

        assert(std::is_sorted(srcs_ptrs[i], srcs_ptrs[i] + srcs_sizes[i]));
    }

    auto ptr_src0 = srcs_ptrs[0];
    uint64 c_found = 0;

    LOG_DEBUG("==== nleaf_AND:  iterating on buffer with size" << first_src_size);

    auto dur = TAKE_TIME {
        for (uint64 idx = 0; idx < first_src_size; idx++) {
            RID rid = *ptr_src0++;

            for (uint64 src = 1; src < srcs.size(); src++) {
                auto the_end = &srcs_ptrs[src][srcs_sizes[src]];
                auto iter = std::lower_bound(srcs_ptrs[src], the_end, rid);

                if ((iter != the_end) && (*iter == rid)) {
                    // found it? Put it in the results
                    *results_ptr++ = rid;
                    c_found++;
                }
            }
        }
    };

    Buffer::set_num_elems(results, c_found);
    LOG_PERF("++++ nleaf_AND " << R << " core loop took: " << dur.micros() << "us.");
    LOG_VERBOSE("++++ nleaf_AND: " << R << " rids: " << Buffer::num_elems(srcs[0]) << ", " << Buffer::num_elems(srcs[1]) << " --> " << c_found);

    return results;
}

BHandle IdxTable::nleaf_OR(VecConstHandles srcs, JOBID jobId) {
    auto first_src_size = Buffer::num_elems(srcs[0]);

    // check corner case
    if (srcs.size() == 1) {
        Buffer::create(0);
        return first_src_size;
    }

    set<RID> results;

    // auto dur_us = TAKE_TIME_US{
    for (auto src : srcs) {
        for (uint64 idx = 0; idx < Buffer::num_elems(src); idx++) {
            RID rid = Buffer::const_data(src)[idx];
            results.insert(rid);
        }
    }
    //};

    // LOG_PERF("nleaf_OR  core loop took: " << dur_us << "us.");
    return Buffer::createFromContainer(results);
}

using namespace std::chrono;

using days = duration<int, std::ratio<86400>>;

tuple<int64, int64> IdxTable::get_timeinfo_from_millis(INT64 value) {
    int64 secs_from_epoch = value / 1000;
    int64 millis = value % 1000;
    return make_tuple(secs_from_epoch, millis);
}

void IdxTable::print_timestamp(INT64 value, stringstream& ss) noexcept {
    try {
        using namespace std::chrono;

        int64 secs_from_epoch, millis;
        std::tie(secs_from_epoch, millis) = get_timeinfo_from_millis(value);

        auto x = tp_epoch + seconds(secs_from_epoch);
        std::time_t x_c = std::chrono::system_clock::to_time_t(x);
        std::tm my_time{};

        auto ret = GMTIME(&my_time, &x_c);
        if (ret != 0)
            [[unlikely]] {
                ss << "error";
            }
        else {
            stringstream ss_local;
            ss_local << std::put_time(&my_time, "%Y-%m-%dT%H:%M:%S");
            if (millis > 0)
                [[unlikely]] {
                    ss_local << "." << millis;
                }

            ss << ss_local.str();
        }
    } catch (...) {
        ss << "exception";
    }
}

// Timezone default is UTC
//
void IdxTable::print_date(INT64 value, stringstream& ss) noexcept {
    try {
        using namespace std::chrono;
        auto x = tp_epoch + days(value);
        std::time_t x_c = std::chrono::system_clock::to_time_t(x);
        std::tm my_time{};
        auto ret = GMTIME(&my_time, &x_c);
        if (ret != 0)
            [[unlikely]] {
                ss << "error";
            }
        else {
            ss << std::put_time(&my_time, "%Y-%m-%d");
        }
    } catch (...) {
        ss << "exception";
    }
}

int64 IdxTable::create_millis_from_epoch(std::tm& timeinfo, int millis) {
    constexpr uint64 SECONDS_PER_DAY = 3600ull * 24ull;
    assert(millis < 1000);

#ifdef _MSC_VER
    std::time_t tt = _mkgmtime(&timeinfo);
#else
    std::time_t tt = timegm(&timeinfo);
#endif

    // Epoch = January 1st 1970
    system_clock::time_point tp = system_clock::from_time_t(tt);

    int64 days_since_epoch = std::chrono::duration_cast<days>(tp.time_since_epoch()).count();
    int64 millis_since_epoch =
        ((days_since_epoch * SECONDS_PER_DAY + ((((int64(timeinfo.tm_hour) * 60) + int64(timeinfo.tm_min)) * 60ull) + int64(timeinfo.tm_sec))) * 1000) +
        int64(millis % 1000);

    return millis_since_epoch;
}

optional<INT64> IdxTable::serialize_timestamp(cchar* date) noexcept {
    try {
        // TO DO:  fix leap years
        constexpr uint64 SECONDS_PER_DAY = 3600ull * 24ull;

        std::tm t = {};
        std::istringstream ss(date);
        ss.imbue(my_locale);
        auto fail = get_time(ss, "%Y-%m-%dT%H:%M:%S", &t);

        if (fail) {
            return std::nullopt;
        }

        if (t.tm_year < 0)
            return std::nullopt;
        if ((t.tm_mon < 0) || (t.tm_mon > 12))
            return std::nullopt;
        // Months...
        if ((t.tm_mday < 0) || (t.tm_mday > 31))
            return std::nullopt;
        if ((t.tm_hour < 0) || (t.tm_hour > 23))
            return std::nullopt;
        if ((t.tm_min < 0) || (t.tm_min > 59))
            return std::nullopt;
        if ((t.tm_sec < 0) || (t.tm_sec > 59))
            return std::nullopt;

        int millis = 0;
        auto idx = ss.str().find('.');
        if (idx != string::npos) {
            string sub = ss.str().substr(idx + 1);
            millis = std::stoi(sub);
        }

        if ((millis < 0) || (millis > 1000))
            return std::nullopt;

        int64 millis_since_epoch = create_millis_from_epoch(t, millis);

        return millis_since_epoch;
    } catch (...) {
        return std::nullopt;
    }
}

bool IdxTable::get_time(std::istringstream& ss, const string& format, std::tm* timeinfo) {
#ifdef _MSC_VER
    ss >> std::get_time(timeinfo, format.c_str());
    if (ss.fail()) {
        return true;
    }
#else
    // With gcc, due to https://gcc.gnu.org/bugzilla/show_bug.cgi?id=45896, leading zeros in month are required for std::get_time
    // Also, Calendar and Timezones from C++20 is only partially implemented in gcc and clang (std::chrono::parse not available)
    // Workaround using a POSIX function
    auto curLocale = std::setlocale(LC_TIME, nullptr);
    std::setlocale(LC_TIME, my_locale.name().c_str());
    auto ret = strptime(ss.str().c_str(), format.c_str(), timeinfo);
    std::setlocale(LC_TIME, curLocale);
    if (ret == nullptr || isdigit(ret[0])) {
        return true;
    }
#endif
    return false;
}

optional<INT64> IdxTable::serialize_date(cchar* date) noexcept {
    try {
        std::tm timeinfo = {};
        std::istringstream ss(date);
        ss.imbue(my_locale);

        // TO DO: why not check directly date[1] and date[2] for '/' or '-' ???

        if (date[4] == '/') {
            auto fail = get_time(ss, "%Y/%m/%d", &timeinfo);

            if (fail || (timeinfo.tm_year < 0) || (timeinfo.tm_mon < 0)) {
                std::istringstream ssdate(date);
                ssdate.imbue(my_locale);
                fail = get_time(ssdate, "%Y/%b/%d", &timeinfo);
                if (fail || (timeinfo.tm_year < 0) || (timeinfo.tm_mon < 0)) {
                    LOG_ERROR("Cannot parse date string: " << date << DATE_PARSING_ISO_8601_MSG);
                    return std::nullopt;
                }
            }
        } else if (date[4] == '-') {
            auto fail = get_time(ss, "%Y-%m-%d", &timeinfo);

            if (fail || (timeinfo.tm_year < 0) || (timeinfo.tm_mon < 0)) {
                std::istringstream ssdate(date);
                ssdate.imbue(my_locale);
                fail = get_time(ssdate, "%Y-%b-%d", &timeinfo);
                if (fail || (timeinfo.tm_year < 0) || (timeinfo.tm_mon < 0)) {
                    LOG_ERROR("Cannot parse date string: " << date << DATE_PARSING_ISO_8601_MSG);
                    return std::nullopt;
                }
            }
        } else {
            LOG_ERROR("Cannot parse date string: " << date << DATE_PARSING_ISO_8601_MSG);
            return std::nullopt;
        }

        timeinfo.tm_hour = 12;
        std::time_t tt = std::mktime(&timeinfo);

        // Epoch = January 1st 1970

        system_clock::time_point tp = system_clock::from_time_t(tt);

        int64 days_since_epoch = std::chrono::duration_cast<days>(tp.time_since_epoch()).count();

        return days_since_epoch;
    } catch (...) {
        return std::nullopt;
    }
}

optional<UINT64> IdxTable::serialize_from_string(const string& value, ColumnType coltype) {
    optional<UINT64> ret = std::nullopt;
    bool unknown_type = false;

    try {
        switch (coltype) {
        case ColumnType::BIGINT:
            ret = std::stoll(value);
            break;

        case ColumnType::FLOAT:
            ret = serializeDouble(std::stod(value));
            break;

        case ColumnType::DATE:
            ret = serialize_date(value.c_str());
            break;

        case ColumnType::TIMESTAMP:
            ret = serialize_timestamp(value.c_str());
            break;

        case ColumnType::TEXT: {
            StringToken stok = tokenizedStrings.addString(value);
            ret = stok.getFullToken();
            break;
        }

        default:
            assert(UNREACHED);
            unknown_type = true;
        }
    } catch (...) {
        LOG_ERROR("Exception in serialize from string. Value = '" << value << "'  coltype = " << uint64(coltype));
        if (unknown_type) {
            EXPECT_OR_THROW(false, InternalError, "Unexpected ColumnType in serialize_from_string.");
        }
    }

    return ret;
}

void IdxTable::delete_keys(const vector<string>& keys) {
    syncMutate([&]() {
        assert(s_active_select_queries.load() == 0);
        EXPECT_OR_THROW(s_active_select_queries.load() == 0, InternalError, "Select queries still running while modifying data in table " << table_name);

        for (const auto& key : keys) {

            auto iter = primary_keys.find(key);

            // not here? Bail out.
            //
            if (iter == primary_keys.end())
                return;

            RID rid = iter->second;

            hard_delete({rid});

            primary_keys.erase(iter);
        }
    });
}

void IdxTable::notification_add_modify(void* ctx, const char* key) {
    auto values = server.module_interface.hget_me(ctx, key, colNamesNoPKeyVec);

    assert(values.size() == colNamesNoPKeyVec.size());

    // Convert each value into proper format
    vector<CID> cids;
    vector<UINT64> vals;

    vals.push_back(tokenizedStrings.addString(key).getFullToken());
    cids.push_back(REDIS_PRIMARY_KEY_CID);

    for (ColIdx cidx = 0; cidx < NUM_PROPER_COLUMNS; cidx++) {
        auto opt = values[cidx];
        if (opt.has_value()) {
            const CID cid = colCids[cidx + 1];

            if (std::holds_alternative<long long>(*opt)) {
                vals.push_back(get<long long>(*opt));
                cids.push_back(cid);
            } else if (std::holds_alternative<string>(*opt)) {
                ColumnType coltype = get<2>(colsInfo[cidx + 1]);
                optional<UINT64> str_opt = serialize_from_string(get<string>(*opt), coltype);
                if (str_opt.has_value()) {
                    vals.push_back(*str_opt);
                    cids.push_back(cid);
                }
            }
        } else {
            nulls_per_column[cidx]++;
        }
    }

    assert(vals.size() == cids.size());

    auto iter = primary_keys.find(key);

    if (iter == primary_keys.end()) {
        upinsertKey(key, cids, vals);
    } else {
        RID rid = iter->second;
        VecCID modifiedCids = modifyKeyCore(rid, key, cids, vals);

        if (rid + 1 < columns_num_sorted_elems) {
            sort_columns(vec_cids_to_idxs(modifiedCids), false);
        }
    }
}

void IdxTable::keyspace_notification(OuterCtx ctx, const char* key, const char* event) {
    DEBUG_LOG_DEBUG("Table " << table_name << " got notification of event '" << event << "'");

    if ((strcmp(event, "hset") != 0) && (strcmp(event, "hdel") != 0) && (strcmp(event, "del") != 0) && (strcmp(event, "rename_from") != 0) &&
        (strcmp(event, "rename_to") != 0) && (strcmp(event, "hincrby") != 0) && (strcmp(event, "hincrbyfloat") != 0) && (strcmp(event, "restore") != 0))
        return;

    if ((strcmp(event, "hset") == 0) || (strcmp(event, "hdel") == 0) || (strcmp(event, "rename_to") == 0) || (strcmp(event, "hincrby") == 0) ||
        (strcmp(event, "hincrbyfloat") == 0) || (strcmp(event, "restore") == 0)) {
        notification_add_modify(ctx, key);
        return;
    }

    if ((strcmp(event, "del") == 0) || (strcmp(event, "rename_from") == 0)) {
        delete_keys({key});
        return;
    }
}

// Scans main Redis for keys with given prefix, and add them to the table
//
// Returns number of keys found.
//
IdxTable::ScanRet IdxTable::scan(OuterCtx ctx, const char* prefix) {
    uint64 keys_found = 0;
    ;
    string str_prefix = string(prefix) + "*";

    auto dur = TAKE_TIME {
        server.module_interface.key_me(ctx, str_prefix.c_str(), [this, ctx, &keys_found](cchar* str, int len) {
            string key(str, len);
            keyspace_notification(ctx, key.c_str(), "hset");
            keys_found++;
        });
    };

    LOG_PERF("IdxTable::scan " << str_prefix << " took:          " << dur.millis() << "ms.");

    vector<string> msgs;

    FOR_EACH_PROPER_COLUMN(cidx) {
        stringstream ss;
        ss << nulls_per_column[cidx] << " NULLs in column " << colNamesVec[cidx];
        LOG_INFO(ss.str());
        msgs.push_back(ss.str());
    }

    return ScanRet{keys_found, msgs};
}

tuple<string, vector<FullyQualifiedCID>> IdxTable::getInfo() const {
    tuple<string, vector<FullyQualifiedCID>> ret;

    get<0>(ret) = std::to_string(tid);

    FOR_EACH_PROPER_COLUMN(cidx) {
        get<1>(ret).push_back(make_tuple(colNamesVec[cidx], colCids[cidx + 1], get<2>(colsInfo[cidx + 1])));
    }

    return std::move(ret);
}

vector<tuple<string, string>> IdxTable::getDebugInfo() const {
    vector<tuple<string, string>> ret;
    auto tmp = this->getInfo();

    ret.emplace_back("TID", get<0>(tmp));

    for (auto item : get<1>(tmp)) {
        ret.emplace_back("column name", get<0>(item));
        ret.emplace_back("column id", std::to_string(get<1>(item)));
        ret.emplace_back("column type", coltypeToString(get<2>(item)));
    }

    ret.emplace_back("num_columns", std::to_string(num_all_columns));
    ret.emplace_back("columnCapacity", std::to_string(config.columnCapacity));
    ret.emplace_back("minResultCapacity", std::to_string(config.minResultCapacity));
    ret.emplace_back("growthStrategyType", std::to_string((int)config.growthStrategy.growth));
    ret.emplace_back("growthStrategyParemeter", std::to_string(config.growthStrategy.parameter));
    ret.emplace_back("sortStrategyType", std::to_string((int)config.sortStrategy.type));
    ret.emplace_back("sortStrategyParameter", std::to_string(config.sortStrategy.parameter));
    ret.emplace_back("sort_threshold", std::to_string(sort_threshold));
    ret.emplace_back("columns_capacity", std::to_string(columns_capacity));
    ret.emplace_back("columns_num_elems", std::to_string(columns_num_elems));
    ret.emplace_back("columns_num_sorted_elems", std::to_string(columns_num_sorted_elems));
    ret.emplace_back("metadata_size", std::to_string(metadata.size()));

    auto ts = tokenizedStrings.getInfo();
    ret.insert(ret.end(), ts.begin(), ts.end());

    ret.emplace_back("next_rid", std::to_string(n_next_rid));
    ret.emplace_back("completed_deletes", std::to_string(n_completed_deletes));
    ret.emplace_back("num_computes_active", std::to_string(getActiveComputes()));

    ret.emplace_back("c_add_row", std::to_string(c_add_row));
    ret.emplace_back("c_grow_event", std::to_string(c_grow_event));
    ret.emplace_back("c_sort_events", std::to_string(c_sort_events));

    return std::move(ret);
}

void IdxTable::sortRidsByCids(BHandle handle, const vector<tuple<QCID, ASCDESC>>& sorting_cols) {
    uint64 num_sorting_cols = sorting_cols.size();

    auto sort_lambda = [num_sorting_cols, sorting_cols, this](RID l, RID r) -> bool {
        uint64 sort_col_idx = 0;

        for (;;) {
            const CID cid = get<0>(get<0>(sorting_cols[sort_col_idx]));
            auto col_idx = this->columns_cids_to_idx[cid];
            ColumnType col_type = get<1>(get<0>(sorting_cols[sort_col_idx]));
            INT64 l_value = slow_access(the_columns[col_idx], l);
            INT64 r_value = slow_access(the_columns[col_idx], r);
            ASCDESC ascdesc = get<1>(sorting_cols[sort_col_idx]);

            if (ascdesc == ASCDESC::DESC) {
                std::swap(l_value, r_value);
            }

            switch (col_type) {
            case ColumnType::BIGINT:
            case ColumnType::DATE:
            case ColumnType::TIMESTAMP:
                if (l_value < r_value)
                    return true;
                if (l_value > r_value)
                    return false;
                // values are the same....
                // We either are last column... or there's another one
                if (++sort_col_idx == num_sorting_cols)
                    return (l < r);

                // move on to next sorting col
                break;

            case ColumnType::FLOAT: {
                double l_double = reinterpret<double>(l_value);
                double r_double = reinterpret<double>(r_value);
                if (l_double < r_double)
                    return true;
                if (l_double > r_double)
                    return false;
                // values are the same....
                // We either are last column... or there's another one
                if (++sort_col_idx == num_sorting_cols)
                    return (l < r);

                // move on to next sorting col
                break;
            }

            case ColumnType::TEXT: {
                uint64 l_u = reinterpret<uint64>(l_value);
                uint64 r_u = reinterpret<uint64>(r_value);

                StringToken l_stok(l_u);
                StringToken r_stok(r_u);

                string l_string = token2string(l_stok);
                string r_string = token2string(r_stok);

                auto cmp_ret = l_string.compare(r_string);

                if (cmp_ret != 0)
                    return (cmp_ret < 0);

                // values are the same....
                // We either are last column... or there's another one
                if (++sort_col_idx == num_sorting_cols)
                    return (l < r);

                // move on to next sorting col
                break;
            }

            default:
                assert(UNREACHED);
                EXPECT_OR_THROW(false, InternalError, "Unexpected type in sortRidsByCids: " << uint32(col_type));
            }
        }
    };

    uint64 num_elems = Buffer::num_elems(handle);
    RID* start = Buffer::data(handle);
    RID* end = Buffer::data(handle) + num_elems;

    std::sort(start, end, sort_lambda);
}

// Stoppable::join
// Must be called after "Stoppable::stop"
void IdxTable::join() {
    assert(Stoppable::isFinishing());
    EXPECT_OR_THROW(Stoppable::isFinishing(), InternalError, "IdxTable::join called before calling 'stop'.");

    syncMutate([&]() {
        assert(s_active_select_queries.load() == 0);
        assert(s_active_add_commands.load() == 0);
        assert(num_running_commands.load() == 0);
    });
}

optional<QCID> IdxTable::translateColumnName(const string& colname) {
    auto iter = colNames2CID.find(colname);
    if (iter == colNames2CID.end()) {
        return std::nullopt;
    }

    CID cid = iter->second;
    auto iter2 = columns_cids_to_idx.find(cid);
    if (iter2 == columns_cids_to_idx.end()) {
        return std::nullopt;
    }

    return QCID{cid, get<2>(colsInfo[iter2->second])};
}

bool IdxTable::isValidTableName(const string& key) {
    if (std::isdigit(key.at(0))) {
        return false;
    }

    for (auto ch : key) {
    }

    return true;
}

vector<uint64> IdxTable::SerializeConfig(const Config& config) {
    vector<uint64> ret;
    ret.push_back(config.columnCapacity);
    ret.push_back(config.minResultCapacity);
    ret.push_back(uint64(config.growthStrategy.growth));
    ret.push_back(uint64(config.growthStrategy.parameter));
    ret.push_back(uint64(config.sortStrategy.type));
    ret.push_back(uint64(config.sortStrategy.parameter));

    return std::move(ret);
}

IdxTable::Config IdxTable::DeserializeConfig(const vector<uint64>& ser_config) {
    auto columnCapacity = ser_config[0];
    auto minResultCapacity = ser_config[1];
    uint32 growthStrategy_grow = uint32(ser_config[2]);
    uint32 growthStrategy_parameter = uint32(ser_config[3]);
    uint32 sortStrategy_type = uint32(ser_config[4]);
    uint32 sortStrategy_parameter = uint32(ser_config[5]);

    GrowingStrategy gstrat = {GrowingStrategy::StrategyType(growthStrategy_grow), growthStrategy_parameter};

    /// This calculates the unsorted capacity related to the sorted one.
    ThresholdStrategy sstrat = {ThresholdStrategy::StrategyType(sortStrategy_type), sortStrategy_parameter};

    IdxTable::Config ret{columnCapacity, minResultCapacity, gstrat, sstrat};

    return ret;
}

void IdxTable::clear() {
    syncMutate([&]() {
        assert(s_active_select_queries.load() == 0);
        EXPECT_OR_THROW(s_active_select_queries.load() == 0, InternalError, "Select queries still running while modifying data in table " << table_name);

        for (const auto& key : primary_keys) {
            hard_delete({key.second});
        }

        primary_keys.clear();
    });
}

} // namespace memurai::sql
