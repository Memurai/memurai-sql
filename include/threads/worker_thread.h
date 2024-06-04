/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#pragma once

#include <functional>
#include <optional>
#include <thread>
#include <tuple>
#include <utility>

#include "itoken.h"
#include "thread_starter.h"

namespace memurai::multicore {

using std::optional;
using std::string;

template <typename Queue_, typename BadExc, typename GoodExc> class WorkerThread {
public:
    typedef Queue_ Queue;
    typedef typename Queue::value_type WorkItem;

private:
    Queue& queue;
    string name;
    ThreadStarter* thread_starter;

    thread* t_thread;

    std::atomic<bool> s_done;
    cuint32 workerId;
    std::mutex m_join;
    std::condition_variable cv_finished;

public:
    WorkerThread(ThreadStarter* thread_starter, string str, Queue& queue, cuint32 workerId)
        : thread_starter(thread_starter), name(std::move(str)), queue(queue), t_thread(nullptr), s_done(false), workerId(workerId) {
        t_thread = new thread([this]() {
            run();
        });
        thread_starter->notify_ctor();
    }

    ~WorkerThread() {
        assert(s_done.load() == true);

        thread_starter->notify_dtor();

        delete t_thread;
    }

    void run() {
        IToken* inner_token = nullptr;

        thread_starter->notify_running();

        DEBUG_LOG_DEBUG("WorkerThread " << workerId << " is starting run loop.");

        while (!thread_starter->isFinishing()) {
            try {
                // DEBUG_LOG_DEBUG("WorkerThread " << workerId << " is blocked on the queue.");

                thread_starter->notify_waiting();
                // Sync call to client's queue.
                auto opt = queue.dequeue();

                if (opt.has_value()) {
                    auto item = *opt;
                    thread_starter->notify_working();

                    // DEBUG_LOG_DEBUG("WorkerThread " << workerId << " is doing some work.");

                    // Item might have (or not) a token. If it does, use it.
                    inner_token = item.token;

                    item.lambda();

                    if (inner_token) {
                        inner_token->done();
                        DEBUG_LOG_DEBUG("WorkerThread " << workerId << " notifying token of 'done'.");
                    } else {
                        DEBUG_LOG_DEBUG("WorkerThread " << workerId << " done. No token to notify.");
                    }
                }
            } catch (GoodExc& ex) {
                thread_starter->notify_exception();
                if (inner_token) {
                    LOG_ERROR("WorkerThread " << workerId << " notifying token of resumable exception.");
                    inner_token->notify_exception(ex);
                }
            } catch (BadExc& ex) {
                thread_starter->notify_exception();
                if (inner_token) {
                    LOG_ERROR("WorkerThread " << workerId << " notifying token of BAD exception.");
                    inner_token->notify_exception(ex);
                }
            } catch (...) {
                thread_starter->notify_exception();
                if (inner_token) {
                    LOG_ERROR("WorkerThread " << workerId << " notifying token of unknown exception.");
                    inner_token->notify_exception();
                }
            }
        }

        DEBUG_LOG_DEBUG("WorkerThread " << workerId << " is exiting run loop.");
        thread_starter->notify_exiting();

        {
            std::unique_lock<std::mutex> lock(m_join);
            s_done.store(true);
        }
        cv_finished.notify_one();
        DEBUG_LOG_DEBUG("WorkerThread " << workerId << " really exiting.");
    }

    void join() {
        DEBUG_LOG_DEBUG("WorkerThread " << workerId << " is joining.");
        {
            std::unique_lock<std::mutex> lock(m_join);
            cv_finished.wait(lock, [this]() {
                return s_done.load();
            });
        }
        t_thread->join();
        DEBUG_LOG_DEBUG("WorkerThread " << workerId << " has joined.");
    }
};

} // namespace memurai::multicore
