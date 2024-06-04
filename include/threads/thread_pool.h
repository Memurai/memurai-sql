/*
 * Copyright (c) 2020-2021, Janea Systems
   by Benedetto Proietti
 */

#pragma once

#include <functional>
#include <string>
#include <vector>

#include "cancellation.h"
#include "logger.h"
#include "thread_starter.h"
#include "types.h"

namespace memurai::multicore {

using std::string;
using std::vector;

class IToken;

template <typename WorkerThread> class ThreadPool : public ThreadStarter {
public:
    typedef ThreadPool<WorkerThread> self_type;
    typedef typename WorkerThread::Queue Queue;
    typedef typename WorkerThread::WorkItem WorkItem;

private:
    Queue queue;
    const string name;
    uint32 threadPoolSize;

    vector<WorkerThread*> workers;

public:
    ThreadPool(self_type&&) = delete;
    ThreadPool(const self_type&) = delete;

    ThreadPool(string name, uint32 threadPoolSize) : threadPoolSize(threadPoolSize), name(name), ThreadStarter() {
        for (uint32 i = 0; i < threadPoolSize; i++) {
            workers.push_back(new WorkerThread(this, name, queue, i));
        }

        DEBUG_LOG_DEBUG("ThreadPool " << name << " started.");
    }

    void join() {
        for (uint32 i = 0; i < threadPoolSize; i++) {
            workers[i]->join();
        }
    }

    void stop() override {
        ThreadStarter::stop();

        // Token sometoken(string("temp_token"), 0, threadPoolSize);

        for (uint32 i = 0; i < threadPoolSize; i++) {
            submit([]() {}, nullptr);
        }
    }

    ~ThreadPool() {
        DEBUG_LOG_DEBUG("ThreadPool " << name << " dtor-ing.");

        stop();

        join();

        for (uint32 i = 0; i < threadPoolSize; i++) {
            auto worker = workers[i];
            delete worker;
        }

        DEBUG_LOG_DEBUG("ThreadPool " << name << " dtor-ed.");
    }

    void wait_end() {
        DEBUG_LOG_DEBUG("ThreadPool " << name << " joining.");

        for (uint32 i = 0; i < threadPoolSize; i++) {
            workers[i]->join();
        }

        DEBUG_LOG_DEBUG("ThreadPool " << name << " joined.");
    }

    void submit(std::function<void()> lambda, IToken* token) {
        queue.enqueue(WorkItem{lambda, token});
    }

    void submit(const vector<std::function<void()>>& items, IToken* token) {
        for (auto item : items) {
            queue.enqueue(WorkItem{item, token});
        }
    }
};

} // namespace memurai::multicore
