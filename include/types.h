/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#pragma once

#include <cstring>
#include <type_traits>

// timestamp type
typedef unsigned long long Timestamp;

//
typedef const char cchar;

namespace memurai {
typedef unsigned char byte;
}
typedef unsigned char uint8;
typedef unsigned int uint32;
typedef int int32;
typedef const int cint32;
typedef const uint32 cuint32;
typedef unsigned int uint;
typedef signed long long int64;
typedef unsigned long long uint64;
typedef const uint64 cuint64;
typedef const int64 cint64;
typedef unsigned short uint16;
typedef const uint16 cuint16;

enum class ErrorCode : uint64 {
    NoError = 0,
    ERROR_SQL_WRONG_ARGC,
    ERROR_SQL_INVALID_TIMESTAMP,
    ERROR_SQL_CREATE_DIFFERENT_LABELS,
    ERROR_SQL_TABLE_DOES_NOT_EXIST,
    ERROR_SQL_TABLE_ALREADY_EXISTS,
    ERROR_SQL_BRIDGE_FULL,
    ERROR_SQL_CMD_NOT_SUPPORTED,
    ERROR_SQL_UNSOPPORTED_PAR,
    ERROR_SQL_WRONG_FORMAT,
    ERROR_SQL_TIMESERIES_FULL,
    ERROR_SQL_INVALID_CADENCEMS,
    ERROR_SQL_INVALID_WINDOW_TYPE_LABEL,
    ERROR_SQL_INVALID_WINDOW_TYPE,
    ERROR_SQL_INVALID_WINDOW_HOPPING_TIME_LABEL,
    ERROR_SQL_INVALID_WINDOW_SIZE_LABEL,
    ERROR_SQL_INVALID_WINDOW_SIZE,
    ERROR_SQL_GROUPBY_LABEL_NOT_FOUND,
    ERROR_SQL_LABEL_NOT_FOUND,
    ERROR_SQL_FILE_ERROR,
    ERROR_SQL_WRONG_COL_TYPE,
    ERROR_SQL_COLUMN_NOT_FOUND,
    ERROR_SQL_TABLE_NAME_INVALID,
    ERROR_SQL_TABLES_ALREADY_RESTORED,
    AlreadyCanceled,
    ERROR_TYPE_MISMATCH,
    CannotConnectToRedis,
    InternalError,
    MalformedInput,
    TableNameTooLong,
    PrefixTooLong,
    Unsupported,
    StringNotValid,
    FilesystemError,
    TooManyTables,
    IncompatibleMetadataVersion
};

#define UNREACHED 0
#define SUCCESS ErrorCode::NoError

typedef void* OuterCtx;

template <class Out, class In> static Out reinterpret(In val) {
    static_assert(sizeof(In) == sizeof(Out));
    static_assert(std::is_trivially_copyable<In>::value);
    static_assert(std::is_trivially_copyable<Out>::value);
    Out result;
    memcpy(&result, &val, sizeof(Out));
    return result;
}
