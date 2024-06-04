/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#include <filesystem>

#include "test/test_utils.h"

namespace fs = std::filesystem;

void delete_test_files(const string& temp_folder) {
    LOG_VERBOSE("Deleting temp files in: " << temp_folder);
    fs::remove_all(temp_folder.c_str());
    LOG_VERBOSE("Files deleted. (" << temp_folder << ")");
}

template <> void TEST_VALUE_WITH_EPSILON_IMPL<uint64, uint64>(const uint64& expected, const uint64& gotten, const uint64 epsilon, int linenum) {
    if ((std::max(expected, gotten) - std::min(expected, gotten)) > epsilon) {
        LOG_ERROR(g_test_name << ": at line " << linenum << " Expected " << expected << ", gotten " << gotten);
        DEBUG_BREAK;
        g_errors++;
        l_errors++;
    }
}
