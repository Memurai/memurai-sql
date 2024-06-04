/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <functional>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "ColumnTypes.h"
#include "aligned_malloc.h"
#include "cache.h"
#include "exception.h"
#include "scope/expect.h"
#include "tidcid.h"
#include "types.h"

namespace memurai::sql {

using std::optional;
using std::string;
using std::tuple;
using std::vector;

class Body {
    void* data_;
    uint64 numBytes_;

public:
    Body(const Body&) = delete;
    Body(Body&& other) noexcept : data_(other.data_), numBytes_(other.numBytes_) {
        other.data_ = nullptr;
        other.numBytes_ = 0;
    }

    explicit Body(uint64 numBytes_, const std::function<void(void*)>& fill) : numBytes_(numBytes_) {
        data_ = ALIGNED_MALLOC(ALIGNED_PAGE, numBytes_);
        fill(data_);
    }

    ~Body() {
        if (data_ != nullptr) {
            ALIGNED_FREE(data_);
        }
    }

    [[nodiscard]] const void* data() const {
        return data_;
    }

    [[nodiscard]] uint64 sizeBytes() const {
        return numBytes_;
    }
};

} // namespace memurai::sql
