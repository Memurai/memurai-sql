/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <string>

#include "tidcid.h"
#include "types.h"

namespace memurai::sql {

enum class ColumnType : uint32 { TEXT, BIGINT, FLOAT, DATE, TIMESTAMP, NULLTYPE, RIDCOL, LAST = RIDCOL };

typedef std::tuple<std::string, ColumnType> QualifiedColumn;

typedef std::tuple<CID, ColumnType> QualifiedCID;

typedef QualifiedCID QCID;

typedef std::tuple<std::string, CID, ColumnType> FullyQualifiedCID;

typedef std::vector<QualifiedCID> VecQCID;

typedef FullyQualifiedCID FQCID;

std::string coltypeToString(ColumnType);

typedef uint64 ColIdx;

} // namespace memurai::sql
