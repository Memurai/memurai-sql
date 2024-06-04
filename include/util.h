/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>

#include "types.h"

#ifdef _MSC_VER
#define time_type __time64_t
#define time_secs_from_epoc _time64
#define my_localtime(A, B) localtime_s(A, B)
#define GOOD_LOCALTIME_RET(RET) (RET == 0)
#else
#include <ctime>
#define time_type time_t
#define time_secs_from_epoc std::time
#define my_localtime(A, B) localtime_r(B, A)
#define GOOD_LOCALTIME_RET(RET) (RET != nullptr)
#endif

namespace memurai {
using std::string;
using std::vector;

class Util {
public:
    static uint64 timeSinceEpochMillisec() {
        using std::chrono::duration_cast;
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    template <typename TimePoint> static std::string TimePointToString(const TimePoint& timePoint) {
        auto seconds = double(timePoint.time_since_epoch().count()) * TimePoint::period::num / TimePoint::period::den;
        auto const zeconds = std::modf(seconds, &seconds);
        std::ostringstream oss;

        struct tm newtime {};

        time_type long_time;
        time_secs_from_epoc(&long_time);

        auto ret_ptr = my_localtime(&newtime, &long_time);

        if (!GOOD_LOCALTIME_RET(ret_ptr)) {
            oss << "Error";
        } else {
            oss << std::put_time(&newtime, "%Y-%b-%d %H:%M:") << std::setw(6) << std::setfill('0') << std::fixed << std::setprecision(3)
                << newtime.tm_sec + zeconds;
        }
        return oss.str();
    }

    template <typename TimePoint> static std::string TimePointToString(const TimePoint& timePoint, const char* fmt) {
        auto seconds = double(timePoint.time_since_epoch().count()) * TimePoint::period::num / TimePoint::period::den;
        std::ostringstream oss;

        struct tm newtime {};

        time_type long_time;
        time_secs_from_epoc(&long_time);
        auto ret = my_localtime(&newtime, &long_time);

        if (!GOOD_LOCALTIME_RET(ret)) {
            oss << "Error";
        } else {
            oss << std::put_time(&newtime, fmt);
        }
        return oss.str();
    }

    template <typename TimePoint> static std::wstring TimePointToWString(const TimePoint& timePoint, const wchar_t* fmt) {
        auto seconds = double(timePoint.time_since_epoch().count()) * TimePoint::period::num / TimePoint::period::den;
        std::wstringstream oss;

        struct tm newtime {};

        time_type long_time;
        time_secs_from_epoc(&long_time);
        auto ret = my_localtime(&newtime, &long_time);

        if (GOOD_LOCALTIME_RET(ret)) {
            oss << std::put_time<wchar_t>(&newtime, fmt);
        }
        return oss.str();
    }

    static void split(vector<string>& slice, const string& str, char sep) {
        const char* ptr = str.c_str();
        const char* lastbegin = str.c_str();

        while (*ptr != 0) {
            while (*ptr != 0 && *ptr != sep) {
                ptr++;
            }

            auto len = ptr - lastbegin;
            slice.emplace_back(lastbegin, len);
            if (*ptr++ == 0)
                return;
            lastbegin = ptr;
        }
    }

    static string trim(const string& str, char ch) {
        uint64 size = str.size();
        uint64 idx = 0;

        if (size == 0) {
            return str;
        }

        while ((idx < size) && (str.at(idx) == ch)) {
            idx++;
        }

        if (idx == size) {
            return "";
        }

        cuint64 start = idx;

        idx = 1;
        while ((size - idx >= 0) && (str.at(size - idx) == ch)) {
            idx++;
        }

        if (size - idx <= start) {
            return "";
        }

        cuint64 cnt = size - idx - start + 1;

        return str.substr(start, cnt);
    }

    static string trim_left(const string& str, char ch) {
        uint64 size = str.size();
        uint64 idx = 0;

        if (size == 0) {
            return str;
        }

        while ((idx < size) && (str.at(idx) == ch)) {
            idx++;
        }

        if (idx == size) {
            return "";
        }

        return str.substr(idx);
    }

    static void to_lower(string& data) {
        std::transform(data.begin(), data.end(), data.begin(), [](unsigned char c) {
            return std::tolower(c);
        });
    }

    static bool isPow2(const uint64 num) {
        return !!num & !(num & (num - 1));
    }

    static uint64 next_pow2(uint64 x) {
        if (isPow2(x))
            return x;

        uint64 bit = 1;
        while (x > 1) {
            ++bit;
            x >>= 1;
        }
        return (1ull << bit);
    }
};

} // namespace memurai
