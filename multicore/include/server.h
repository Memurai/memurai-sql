/*
 * Copyright (c) 2020-2021, Janea Systems
   by Benedetto Proietti
 */

#include <atomic>
#include <map>
#include <string>
#include <thread>
#include <tuple>
#include <utility>

#include "exception.h"
#include "jobid.h"
#include "memurai-multicore.h"
#include "threads/thread_pool.h"
#include "threads/thread_starter.h"
#include "threads/worker_thread.h"
#include "types.h"

#pragma once

struct redisContext;

namespace memurai::multicore {

using std::atomic;
using std::map;
using std::string;
using std::thread;
using std::tuple;
using std::vector;

class Server {
private:
    ModuleInterface module_interface;

    typedef WorkerThread<syncqueue<TLambda>, std::exception, memurai::exception> WorkerThreadSync;
    ThreadPool<WorkerThreadSync> thread_pool;

    // Atomic flag to set to 'true' when we want the server to shut down
    atomic<bool> s_finishing;

    // Info on scheduler threads that are running
    map<uint32, tuple<atomic<bool>*, thread*>> timerThreads;
    std::mutex m_timerThreads;

    void stop();

public:
    ~Server();

    Server(IServer::Config config, ModuleInterface m_i);

    void run();

    // Put the given lambda in the syncqueue and, eventually, run it once
    // in the thread pool.
    void schedule_once(Lambda exec, IToken* token) {
        this->thread_pool.submit(std::move(exec), token);
    }
    // Put the given lambda in the syncqueue and, eventually, run it once
    // in the thread pool.
    void schedule_once(Lambda exec) {
        // cmdQueue.enqueue({ exec, nullptr });
        this->thread_pool.submit(std::move(exec), nullptr);
    }

    void schedule_recurring(Lambda exec, uint64 ms, JOBID jobId);

    void stop_command(JOBID jobId);

    void parallel_exec(vector<Lambda> items, IToken* token);
};

} // namespace memurai::multicore
