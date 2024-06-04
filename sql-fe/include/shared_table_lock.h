/*
* Copyright (c) 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

namespace memurai::sql {

template <typename T> class shared_table_lock {
    T* ptr;

public:
    explicit shared_table_lock(T* ptr) : ptr(ptr) {
        ptr->compute_lock();
    }

    shared_table_lock() = delete;
    shared_table_lock(const shared_table_lock&) = delete;
    shared_table_lock(shared_table_lock&&) = delete;

    ~shared_table_lock() {
        ptr->compute_unlock();
    }
};

} // namespace memurai::sql
