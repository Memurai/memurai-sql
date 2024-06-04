/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <cassert>
#include <string>
#include <vector>

#include "idxtable.h"
#include "test_utils.h"
#include "tidcid.h"
#include "types.h"

namespace memurai::sql_test {
using namespace std;
using namespace memurai::sql;

string randomString();

UINT64 generateRandomValue(ColumnType type, IdxTable& table);

vector<UINT64> generateRandomRow(const VecQCID& qcids, IdxTable& table);

void AddRows(IdxTable& table, uint64 num_rows_per_thread, uint64 num_threads, int linenum, uint32 rowIdx = 0, vector<UINT64>* baseline = nullptr);

string generate_random_string_for_column_type(ColumnType type, const string& colname);

} // namespace memurai::sql_test