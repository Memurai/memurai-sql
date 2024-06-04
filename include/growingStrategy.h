/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#pragma once

#include "scope/expect.h"
#include "strategy.h"
#include "types.h"

namespace memurai {

struct GrowingStrategy {
    enum ErrorCode { NotImplemented };

    enum class StrategyType { Linear, Exponential };

    StrategyType growth;

    // Growth algo is used with this parameter.
    cuint32 parameter;

    [[nodiscard]] uint64 grow(uint64 currentSize) const {
        uint64 ret = 0;
        assert(parameter != 0);

        switch (growth) {
        case StrategyType::Linear:
            ret = currentSize * parameter;
            break;
        default:
            assert(UNREACHED);
            EXPECT_OR_THROW(false, NotImplemented, "GrowingStrategy type not implemented.");
        }

        return ret;
    }
};

} // namespace memurai
