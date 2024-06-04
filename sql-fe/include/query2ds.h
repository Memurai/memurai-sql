/*
* Copyright (c) 2021, Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <utility>

#include "PublisherV2DStrings.h"
#include "database.h"

namespace memurai::sql {

class Query2DS {
    Database* db;
    string query;
    PublisherV2DStrings publisher;

public:
    Query2DS(Database* db, string query)
        : db(db),
          query(std::move(query))
          // Create a Publisher that dumps data into a vector<vector<string>>
          ,
          publisher() {}

    ~Query2DS() = default;

    // run query
    QResult execute() {
        QResult ret = db->parse_and_run_query(&publisher, query.c_str());
        return ret;
    }

    [[nodiscard]] const vector<vector<optional<string>>>& get_data() const {
        return publisher.get_data();
    }
};

} // namespace memurai::sql
