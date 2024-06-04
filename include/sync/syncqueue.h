/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

//
// Copyright (c) 2014-2019 Benedetto Proietti, MemFusion
//
// Based on the syncqueue by Benedetto Proietti in MemFusion prototype:
// https://github.com/MemFusion/memfusion_prototype/blob/master/include/MemFusion/syncqueue.h
//

#pragma once

#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>

namespace memurai {
template <typename T, typename Lock = std::mutex> class syncqueue {
public:
    typedef T value_type;

    std::optional<T> try_dequeue() {
        std::unique_lock<Lock> lock_guard(lock_);

        if (queue_.empty() || isStopping.load(std::memory_order_acquire))
            return std::nullopt;

        auto item = queue_.front();
        queue_.pop();
        return (item);
    }

    std::optional<T> dequeue() {
        std::unique_lock<Lock> lock_guard(lock_);
        while (queue_.empty()) {
            if (isStopping.load(std::memory_order_acquire))
                return std::nullopt;

            cond_.wait(lock_guard);
        }
        auto item = queue_.front();
        queue_.pop();
        return (item);
    }

    size_t size() {
        std::unique_lock<Lock> lock_guard(lock_);
        auto ret = queue_.size();
        lock_guard.unlock();
        cond_.notify_all();
        return (ret);
    }

    void enqueue(const T& item) {
        std::unique_lock<Lock> lock_guard(lock_);
        queue_.push(item);
        lock_guard.unlock();
        cond_.notify_all();
    }

    void enqueue(T&& item) {
        std::unique_lock<Lock> lock_guard(lock_);
        queue_.push(std::move(item));
        lock_guard.unlock();
        cond_.notify_all();
    }

    size_t enqueueAndGetSize(const T& item) {
        std::unique_lock<Lock> lock_guard(lock_);
        queue_.push(item);
        auto ret = queue_.size();
        lock_guard.unlock();
        cond_.notify_all();
        return ret;
    }

    void stop() {
        std::unique_lock<Lock> lock_guard(lock_);
        isStopping.store(true, std::memory_order_release);
        cond_.notify_all();
    }

private:
    std::queue<T> queue_;
    Lock lock_;
    std::condition_variable cond_;
    std::atomic_bool isStopping{false};
};
} // namespace memurai
