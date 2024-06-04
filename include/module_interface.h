/*
 * Copyright (c) 2021, Janea Systems
   by Benedetto Proietti
 */

#pragma once

#include <functional>
#include <iterator>
#include <optional>
#include <string>
#include <tuple>
#include <variant>
#include <vector>

using std::function;
using std::optional;
using std::string;
using std::variant;
using std::vector;
typedef std::vector<std::string> vecstring;

struct ModuleInterface {
    // log to Memurai
    function<void(const char* level, const char* msg)> log_me;

    // HGET with list of fields
    function<vector<optional<variant<long long, string>>>(void* ctx, const char* prefix, const vecstring& fields)> hget_me;

    // KEYS
    function<void(void* ctx, const char* prefix, function<void(const char*, int)> consume)> key_me;
};
