/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#pragma once

namespace memurai::sql {

enum NodeType { nLiteralFloat, nLiteralString, nLiteralInt, nLiteralNull, nLiteralDate, nLiteralTimestamp, nColumnRef, nOperator, nCast };

enum WhereNumericLeafFilter { EQ, NEQ, GE, GT, LE, LT };
enum WhereRIDFilter { AND, OR };
enum WhereStringFilter { EQ_CASE_SENSITIVE, EQ_CASE_INSENSITIVE, PATTERN };

} // namespace memurai::sql
