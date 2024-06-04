/*
* Copyright (c) 2020-2021, Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <string>
#include <tuple>

#include "scope/expect.h"

namespace memurai {
using std::string;
using std::tuple;

typedef tuple<ErrorCode, string> QResult;

} // namespace memurai

template <class Traits> std::basic_ostream<char, Traits>& operator<<(std::basic_ostream<char, Traits>& bs, ErrorCode code) {
    switch (code) {
    case ErrorCode::NoError:
        bs << "NoError";
        break;
    case ErrorCode::MalformedInput:
        bs << "MalformedInput";
        break;
    case ErrorCode::ERROR_SQL_TABLE_DOES_NOT_EXIST:
        bs << "TableNotFound";
        break;
    case ErrorCode::ERROR_SQL_COLUMN_NOT_FOUND:
        bs << "ColumnNotFound";
        break;
    case ErrorCode::InternalError:
        bs << "InternalError";
        break;
    case ErrorCode::Unsupported:
        bs << "Unsupported";
        break;
    case ErrorCode::AlreadyCanceled:
        bs << "AlreadyCanceled";
        break;
    case ErrorCode::ERROR_TYPE_MISMATCH:
        bs << "TypeMismatch";
        break;
    default:
        EXPECT_OR_THROW(false, InternalError, "Unrecognized ErrorCode " << code);
    }

    return bs;
}
