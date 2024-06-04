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

namespace memurai::sql {
using memurai::multicore::IServer;

class PublisherCount : public IPublisher {
    bool is_done;

    uint64 count;

public:
    PublisherCount() : is_done(false), count(0) {}

    void publish_header(const vecstring&) final {}

    void publish_row(vector<std::optional<INT64>> values, vector<FullyQualifiedCID> cols, ValuePrinter, uint64) final {
        count++;
    }

    void done() final {
        is_done = true;
    }

    [[nodiscard]] uint64 get_count() const {
        return count;
    }
};

} // namespace memurai::sql
