/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

#include "scope/expect.h"
#include "types.h"

namespace memurai {

struct ThresholdStrategy {
    enum ErrorCode { MisConfiguration };

    enum class StrategyType { Absolute, Percentage, Literal };

    const StrategyType type;
    cuint32 parameter;

    [[nodiscard]] uint64 calculateThreshold(uint64 capacity, uint64 num_already_sorted) const {
        switch (type) {
        case StrategyType::Absolute:
            assert(UNREACHED);
            return parameter;

        case StrategyType::Literal:
            return parameter;

        case StrategyType::Percentage:
            return num_already_sorted + ((parameter * capacity) / 100);
        }

        return false;
    }
};

} // namespace memurai
