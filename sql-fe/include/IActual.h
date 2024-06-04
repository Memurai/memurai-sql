/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <functional>
#include <optional>
#include <string>

#include "ColumnTypes.h"
#include "string_token.h"
#include "tidcid.h"
#include "types.h"

namespace memurai::sql {
using std::function;
using std::optional;
using std::string;

typedef string TableNameType;
typedef string ColumnName;
typedef string SchemaName;

struct IActual {
    function<optional<QualifiedCID>(TableNameType, ColumnName)> translateColumn;
    function<StringToken(TID, cchar*)> getStringToken;
    function<optional<TID>(TableNameType, SchemaName)> getTableTID;
    function<optional<int64>(TID, StringToken)> serialize_date;
    function<optional<int64>(TID, StringToken)> serialize_timestamp;
    function<uint64(TID, cchar*)> getTag;
};

} // namespace memurai::sql
