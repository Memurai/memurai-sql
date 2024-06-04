/*
 * Copyright (c) 2020-2021, Janea Systems
   by Benedetto Proietti
 */

#pragma once

#include <cmath> // std::abs
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "error_codes.h"
#include "logger.h"
#include "scope/scope_exit.h"

using namespace std;

extern int g_errors, g_warnings;
extern int l_errors, l_warnings;
extern string g_test_name;

void TEST_NOT_REACHED();

enum Test { Error, Warning };

#ifdef _MSC_VER
#define DEBUG_BREAK __debugbreak()
#else
#define DEBUG_BREAK __asm__ volatile("int $0x03");
//__builtin_debugtrap()
#endif

// returns TRUE when error!
//
template <typename T, typename U> bool TEST_VALUE_IMPL(const T& expected, const U& gotten, int linenum) {
    if (expected != gotten) {
        LOG_ERROR(g_test_name << ": at line " << linenum << " Expected " << expected << "\n gotten " << gotten);
        g_errors++;
        l_errors++;
        DEBUG_BREAK;
        return true;
    }
    return false;
}

namespace memurai::multicore {
enum class TokenState;
}
using memurai::multicore::TokenState;

template <> bool TEST_VALUE_IMPL<TokenState, TokenState>(const TokenState& expected, const TokenState& gotten, int linenum);

template <typename T, typename U> bool TEST_SAME_STRINGS_IMPL(const T* expected, const U* gotten, int linenum) {
    if (expected != gotten) {
        LOG_ERROR(g_test_name << ": at line " << linenum << " Expected " << expected << " gotten " << gotten);
        g_errors++;
        l_errors++;
        DEBUG_BREAK;
        return true;
    }
    return false;
}

template <> bool TEST_SAME_STRINGS_IMPL<char, char>(const char* expected, const char* gotten, int linenum);

template <typename T1, typename T2>
void TEST_VALUE_IF_THEN_IF_IMPL(const T1& expected1, const T1& gotten1, const T2& expected2, const T2& gotten2, int linenum) {
    if (expected1 != gotten1) {
        LOG_ERROR(g_test_name << ": at line " << linenum << " Expected (first couple) " << expected1 << ", gotten " << gotten1);
        g_errors++;
        l_errors++;
        DEBUG_BREAK;
        return;
    }
    if (expected2 != gotten2) {
        LOG_ERROR(g_test_name << ": at line " << linenum << " Expected (2nd couple) " << expected2 << ", gotten " << gotten2);
        g_errors++;
        l_errors++;
        DEBUG_BREAK;
    }
}

template <typename T> void TEST_VALUE_WARNING(const T& expected, const T& gotten) {
    if (expected != gotten) {
        LOG_WARNING(g_test_name << ": at line " << __LINE__ << " Expected " << expected << ", gotten " << gotten);
        g_warnings++;
        l_warnings++;
        DEBUG_BREAK;
    }
}

template <typename T, typename U> bool TEST_VALUE_NEQ(const T& expected, const U& gotten) {
    if (expected == gotten) {
        LOG_ERROR(g_test_name << ": at line " << __LINE__ << " Expected a different value, gotten same value: " << gotten);
        g_errors++;
        l_errors++;
        DEBUG_BREAK;
        return true;
    }
    return false;
}

template <typename T, typename U> void TEST_VALUE_NEQ_WARNING(const T& expected, const U& gotten) {
    if (expected == gotten) {
        LOG_WARNING(g_test_name << ": at line " << __LINE__ << " Expected a different value, gotten same value: " << gotten);
        g_warnings++;
        l_warnings++;
        DEBUG_BREAK;
    }
}

#define TEST_VALUE_WITH_EPSILON(EXPECTED, GOTTEN, EPS) TEST_VALUE_WITH_EPSILON_IMPL(EXPECTED, GOTTEN, EPS, __LINE__)

template <typename T, typename U> void TEST_VALUE_WITH_EPSILON_IMPL(const T& expected, const U& gotten, const T epsilon, int linenum) {
    if (std::abs(expected - gotten) > epsilon) {
        LOG_ERROR(g_test_name << ": at line " << __LINE__ << " Expected " << expected << ", gotten " << gotten);
        DEBUG_BREAK;
        g_errors++;
        l_errors++;
    }
}
template <> void TEST_VALUE_WITH_EPSILON_IMPL<uint64, uint64>(const uint64& expected, const uint64& gotten, const uint64 epsilon, int linenum);

#define TEST_VALUE(A, B) TEST_VALUE_IMPL(A, B, __LINE__)

#define TEST_VALUE_INSIDE(A, B) TEST_VALUE_IMPL(A, B, linenum)

#define TEST_SAME_STRINGS(A, B) TEST_SAME_STRINGS_IMPL(A, B, __LINE__)

#define TEST_VALUE_IF_THEN_IF(A1, B1, A2, B2) TEST_VALUE_IF_THEN_IF_IMPL(A1, B1, A2, B2, __LINE__)

#define TEST_VALUE_SUCCESS(expected, SUC, VALUE) (expected ? TEST_VALUE(SUC, VALUE) : TEST_VALUE_NEQ(SUC, VALUE))

#define TEST_START                                                                                               \
    g_test_name = string(__func__) + "-" + to_string(capacity);                                                  \
    l_errors = 0;                                                                                                \
    l_warnings = 0;                                                                                              \
    LOG_VERBOSE(g_test_name << " is starting ");                                                                 \
    int step = 0;                                                                                                \
    g_skipBaseline = false;                                                                                      \
    SCOPE_EXIT {                                                                                                 \
        LOG_VERBOSE(g_test_name << " ended with: " << l_errors << " errors and " << l_warnings << " warnings."); \
        std::this_thread::sleep_for(std::chrono::milliseconds(200));                                             \
    };

void delete_test_files(const string& temp_folder);
