/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <cassert>

#include "exception.h"
#include "types.h"

namespace memurai::sql {

enum StringType : uint64 { Normal = 0, /*Small = 1, */ XL = 2, Reserved = 3 };

//  2 bits for string type
// 46 bits for posIdx/ptr
// 16 bits for RefCount

class StringToken {
    uint64 data;

public:
    static constexpr uint64 STRING_TYPE_BITPOS = 62;
    static constexpr uint64 PLAIN_STOKEN_BITMASK = ~(0b11ull << STRING_TYPE_BITPOS);

    enum ErrorCode { InternalError };

    StringToken() = delete;

    StringToken(uint64 data, StringType st) : data(data | (st << STRING_TYPE_BITPOS)) {}

    explicit StringToken(uint64 data) : data(data) {}

    StringToken(const StringToken& o) = default;

    bool operator==(const StringToken& o) const {
        return data == o.data;
    }

    [[nodiscard]] uint64 getFullToken() const {
        return (data);
    }

    [[nodiscard]] uint64 getPlainToken() const {
        return (data & PLAIN_STOKEN_BITMASK);
    }

    [[nodiscard]] StringType getStringType() const {
        return StringType(data >> STRING_TYPE_BITPOS);
    }
};

static_assert(sizeof(StringToken) == 8, "SToken should be 64bits.");

} // namespace memurai::sql
