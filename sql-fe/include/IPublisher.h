/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <functional>
#include <optional>
#include <sstream>
#include <string>

#include "ColumnTypes.h"
#include "string_token.h"
#include "tidcid.h"
#include "types.h"

namespace memurai::sql {
using std::function;
using std::string;
using std::vector;

// Only Stream supported right now
//
enum class PublisherType { Stream, Count, Vec2DString, S3, LocalFile, Kafka, Other };

typedef function<void(INT64, ColumnType, std::stringstream&)> ValuePrinter;

class IPublisher {
public:
    virtual void query_started() {}
    virtual void publish_header(const vector<string>&) = 0;
    virtual void publish_row(vector<std::optional<INT64>>, vector<FullyQualifiedCID>, ValuePrinter, uint64 row_idx) = 0;
    virtual void done() = 0;
};

} // namespace memurai::sql
