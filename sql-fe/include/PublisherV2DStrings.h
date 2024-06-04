/*
* Copyright (c) 2021, Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "IPublisher.h"
#include "memurai-multicore.h"

typedef std::vector<std::string> vecstring;

namespace memurai::sql {
using memurai::multicore::IServer;

class PublisherV2DStrings : public IPublisher {
    bool is_done;
    vector<vector<optional<string>>> data;

    vecstring header;

public:
    PublisherV2DStrings() : is_done(false) {}

    [[nodiscard]] vecstring get_header() const {
        return header;
    }

    void publish_header(const vecstring& names) final;

    void publish_row(vector<std::optional<INT64>> values, vector<FullyQualifiedCID> cols, ValuePrinter, uint64 row_idx) final;
    void done() final;

    [[nodiscard]] const vector<vector<optional<string>>>& get_data() const {
        return data;
    }
};

} // namespace memurai::sql
