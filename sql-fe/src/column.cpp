/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#include <algorithm>

#include "column.h"
#include <cassert>

namespace memurai::sql {

void Column::sort(uint64 columns_num_sorted_elems, uint64 columns_num_elems, bool merge) {
    // TO DO:   Perfomance improvement opportunity
    // Instead of appending unsorted to sorted, and then sort everything...
    // we could sort "unsorted" only first, and then merge sort.

    assert(columns_num_elems >= columns_num_sorted_elems);

    // 1- copy unsorted elems at the end of the sorted ones.
    if (merge) {
        copy_unsorted_to_sorted(columns_num_sorted_elems, columns_num_elems);
    }
    // 2- sort'em all!
    QElem* const ptr_sorted_start = get_sorted();
    QElem* const ptr_sorted_end = ptr_sorted_start + columns_num_elems;
    std::sort(ptr_sorted_start, ptr_sorted_end, compare_QElem_by_value);
}

void Column::grow(uint64 new_capacity, uint64 columns_num_sorted_elems) {
    uint64 old_capacity = capacity;
    QElem* const old_sorted_data = sorted_elems;
    INT64* const old_all_data = all_elems;

    // allocate
    sorted_elems = allocate<QElem>(new_capacity);
    all_elems = allocate<INT64>(new_capacity);
    capacity = new_capacity;

    // First time, no data to move or deallocate
    if (old_capacity == 0)
        return;

    // copy sorted data
    memcpy(sorted_elems, old_sorted_data, old_capacity * sizeof(QElem));

    // copy unsorted data
    memcpy(all_elems, old_all_data, old_capacity * sizeof(INT64));

    // deallocate old
    deallocate(old_sorted_data);
    deallocate(old_all_data);
}

void Column::copy_unsorted_to_sorted(uint64 columns_num_sorted_elems, uint64 columns_num_all_elems) {
    QElem* const ptr_sorted_start = get_sorted();
    INT64* const ptr_unsorted_start = get_all_elems_by_rid();
    QElem* transform_ptr = ptr_sorted_start;

    uint64 dest_idx = columns_num_sorted_elems;
    for (RID rid = columns_num_sorted_elems; rid < columns_num_all_elems; rid++) {
        transform_ptr[dest_idx].value = ptr_unsorted_start[rid];
        transform_ptr[dest_idx].rid = rid;
        ++dest_idx;
    }
}

vector<string> Column::projectColNames(const vector<FullyQualifiedCID>& fqcids) {
    vector<string> ret;
    for (auto c : fqcids) {
        ret.push_back(get<0>(c));
    }
    return std::move(ret);
}

vector<CID> Column::projectCIDs(const vector<FullyQualifiedCID>& fqcids) {
    vector<CID> ret;
    for (auto c : fqcids) {
        ret.push_back(get<1>(c));
    }
    return std::move(ret);
}

vector<CID> Column::projectCIDs(const vector<QualifiedCID>& fqcids) {
    vector<CID> ret;
    for (auto c : fqcids) {
        ret.push_back(get<0>(c));
    }
    return std::move(ret);
}

vector<ColumnType> Column::projectTypes(const vector<FullyQualifiedCID>& fqcids) {
    vector<ColumnType> ret;
    for (auto c : fqcids) {
        ret.push_back(get<2>(c));
    }
    return std::move(ret);
}

vector<ColumnType> Column::projectTypes(const vector<QCID>& qcids) {
    vector<ColumnType> ret;
    for (auto c : qcids) {
        ret.push_back(get<1>(c));
    }
    return std::move(ret);
}

vector<string> Column::projectNames(const vector<FullyQualifiedCID>& fqcids) {
    vector<string> ret;
    for (auto c : fqcids) {
        ret.push_back(get<0>(c));
    }
    return std::move(ret);
}

} // namespace memurai::sql
