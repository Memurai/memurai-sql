/*
* Copyright (c) 2021, Janea Systems
by Benedetto Proietti
*/

#include <sstream>

#include "PublisherV2DStrings.h"
#include "error_codes.h"
#include "scope/expect.h"

namespace memurai::sql {
void PublisherV2DStrings::publish_header(const vecstring& names) {
    header = names;
}

void PublisherV2DStrings::publish_row(vector<optional<INT64>> values, vector<FullyQualifiedCID> cols, ValuePrinter printer, uint64 row_idx) {
    EXPECT_OR_THROW(!is_done, InternalError, "PublisherStream publish_row invoked when stream already closed.");

    assert(values.size() == cols.size());
    auto num_cols = values.size();

    //  "colname0", "colname1", "colname2", ...
    //  "row0 col0 value", "row0 col1 value", "row0 col2 value", ...
    //  "row1 col0 value", "row1 col1 value", "row1 col2 value", ...

    data.emplace_back(num_cols);

    for (auto idx = 0; idx < num_cols; idx++) {
        std::stringstream ss;
        auto coltype = get<2>(cols[idx]);
        if (values[idx].has_value()) {
            printer(*values[idx], coltype, ss);
            data.back()[idx] = ss.str();
        } else {
            data.back()[idx] = std::nullopt;
        }
    }
}

void PublisherV2DStrings::done() {
    is_done = true;
}

} // namespace memurai::sql
