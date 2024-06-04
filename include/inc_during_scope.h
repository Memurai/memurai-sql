/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

#include "types.h"

namespace memurai {

template <typename T> class IncrementDuringScope {
    T& obj;

public:
    IncrementDuringScope(const T&) = delete;
    IncrementDuringScope(T&&) = delete;

    explicit IncrementDuringScope(T& obj_) : obj(obj_) {
        obj.fetch_add(1, std::memory_order_acq_rel);
    }
    ~IncrementDuringScope() {
        obj.fetch_sub(1, std::memory_order_acq_rel);
    }
};

} // namespace memurai
