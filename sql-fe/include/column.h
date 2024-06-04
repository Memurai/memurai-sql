/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <cassert>
#include <cstring>
#include <string>
#include <vector>

#include "ColumnTypes.h"
#include "aligned_malloc.h"
#include "cache.h"
#include "rid.h"
#include "tidcid.h"
#include "types.h"

namespace memurai::sql {

using std::get;
using std::string;
using std::vector;

// Qualified Element:  value and RID
struct QElem {
    INT64 value;
    RID rid;

    QElem(INT64 v, RID rid) : value(v), rid(rid) {}

    QElem(double v, RID rid) : value(reinterpret<INT64>(v)), rid(rid) {}
};

inline bool compare_QElem_by_value(const QElem& a, const QElem& b) {
    return ((a.value < b.value) || ((a.value == b.value) && (a.rid < b.rid)));
}

class Column {
    uint64 capacity;
    QElem* sorted_elems = nullptr;
    INT64* all_elems = nullptr;

    template <typename T> static T* allocate(uint64 elems) {
        auto bytes = elems * sizeof(T);
        auto size = (bytes - (bytes % ALIGNED_PAGE)) + (((bytes % ALIGNED_PAGE) == 0) ? 0 : ALIGNED_PAGE);
        assert(size >= bytes);
        assert(size <= bytes + ALIGNED_PAGE);
        assert(size % ALIGNED_PAGE == 0);
        return (T*)ALIGNED_MALLOC(ALIGNED_PAGE, size);
    }

    static void deallocate(void* ptr) {
        return ALIGNED_FREE(ptr);
    }
#if defined(TEST_BUILD)
    // std::fill(the_columns[idx].unsorted_elems, columns_raw_data[idx].end(), 0);
    // std::fill(the_columns[idx].begin(), columns_sorted_data[idx].end(), 0);
    // std::fill(the_columns[idx].begin(), columns_sorted_rids[idx].end(), 0);
#endif

    void copy_unsorted_to_sorted(uint64 columns_num_sorted_elems, uint64 columns_num_unsorted_elems);

public:
    Column() : capacity(0) {}

    ~Column() {
        if (capacity > 0) {
            deallocate(sorted_elems);
            deallocate(all_elems);
        }
    }

    [[nodiscard]] uint64 get_capacity() const {
        return capacity;
    }

    void grow(uint64 new_capacity_sorted, uint64 new_capacity_unsorted);

    void sort(uint64 columns_num_sorted_elems, uint64 columns_num_elems, bool merge);

    [[nodiscard]] QElem* get_sorted() const {
        return sorted_elems;
    }
    [[nodiscard]] INT64* get_all_elems_by_rid() const {
        return all_elems;
    }

    static vector<string> projectColNames(const vector<FullyQualifiedCID>& fqcids);

    static vector<CID> projectCIDs(const vector<FullyQualifiedCID>& fqcids);

    static vector<CID> projectCIDs(const vector<QualifiedCID>& fqcids);

    static vector<ColumnType> projectTypes(const vector<FullyQualifiedCID>& fqcids);

    static vector<ColumnType> projectTypes(const vector<QCID>& qcids);

    static vector<string> projectNames(const vector<FullyQualifiedCID>& fqcids);
};

} // namespace memurai::sql
