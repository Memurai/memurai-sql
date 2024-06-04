/*
 * Copyright (c) 2020-2021, Janea Systems
   by Benedetto Proietti
 */

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <utility>

#include "logger.h"
#include "server.h"
#include "types.h"

namespace memurai::multicore {

using std::atomic;
using std::string;
using std::to_string;
using std::this_thread::sleep_for;
using namespace std::chrono;
using std::get;

Server::Server(IServer::Config config, ModuleInterface m_i) : thread_pool("Server", config.compute_pool_size), module_interface(std::move(m_i)) {
    LOG_INFO("Server is starting.");
#ifdef WIN32
    LOG_DEBUG("QPF=" << to_string(_Query_perf_frequency()));
#endif
}

Server::~Server() {
    LOG_INFO("Server is destructing.");
    this->stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    LOG_INFO("Server is destructed.");
}

void Server::stop_command(JOBID jobId) {
    decltype(timerThreads)::mapped_type right;

    {
        std::unique_lock<std::mutex> mlock(this->m_timerThreads);
        auto iter = timerThreads.find(jobId);
        if (iter == timerThreads.end()) {
            return;
        }

        right = iter->second;
        // setting the flag to true, so the job will stop within 1 second.
        get<0>(right)->store(true);

        get<1>(right)->join();

        delete get<1>(right);

        timerThreads.erase(iter);
    }
}

void Server::stop() {
    LOG_INFO("Server is stopping.");

    // this atomic flag will shutdown everything
    s_finishing.store(true);

    // ------------------------------------------------------------------------
    // Communicate scheduled jobs to stop and...
    //
    for (auto const& [jobId, t_stuff] : timerThreads) {
        get<0>(t_stuff)->store(false);
    }
    LOG_INFO("All scheduler threads signaled to stop.");
    // ... and wait for them
    //
    for (auto const& [jobId, t_stuff] : timerThreads) {
        auto t_thread = get<1>(t_stuff);
        t_thread->join();
        delete t_thread;
    }
    LOG_INFO("All scheduler threads joined and deleted.");
    // ------------------------------------------------------------------------

    // Tell thread pool to stop
    thread_pool.stop();

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

// create a new thread for each scheduled job (for now)
//
void Server::schedule_recurring(Lambda exec, uint64 cadence_ms, JOBID jobId) {
    auto flag = new atomic<bool>(false);

    auto t = new thread([this, cadence_ms, exec, jobId, flag]() {
        LOG_VERBOSE("Scheduled jobId " << jobId << " starting.");

        while (!this->s_finishing.load() && !flag->load()) {
            LOG_VERBOSE("Scheduled jobId " << jobId << " executing.");
            exec();
            LOG_VERBOSE("Scheduled jobId " << jobId << " executED.");

            for (auto times = 0; times < cadence_ms; times += 1000) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                if (this->s_finishing.load() || flag->load()) {
                    // double break avoidance: I know .. I know...
                    goto hack_end;
                }
            }
        }

    hack_end:
        LOG_VERBOSE("Scheduled jobId " << jobId << " ending.");
        delete flag;
    });

    std::unique_lock<std::mutex> mlock(this->m_timerThreads);
    this->timerThreads.insert(make_pair(jobId, make_tuple(flag, t)));
}

void Server::run() {
    thread_pool.join();

    LOG_INFO("Server exits.");
}

IServer IServer::getInterface(IServer::Config config, const ModuleInterface& m_i) {
    static std::mutex my_mutex;
    static atomic<uint64> s_called(0ull);

    if (s_called++ == 0) {
        server_ = new Server(config, m_i);
    }

    auto server = server_;

    IServer ret;
    ret.schedule_once = [server](Lambda exec) {
        server->schedule_once(std::move(exec));
    };
    ret.schedule_once_with_token = [server](Lambda exec, IToken* token) {
        server->schedule_once(std::move(exec), token);
    };
    ret.schedule_recurring = [server](Lambda exec, uint64 ms, JOBID jobId) {
        server->schedule_recurring(std::move(exec), ms, jobId);
    };
    ret.stop_command = [server](JOBID jobId) {
        server->stop_command(jobId);
    };

    ret.schedule_group = [server](vector<Lambda> items, IToken* token) {
        server->parallel_exec(std::move(items), token);
    };
    ret.module_interface = m_i;

    return ret;
}

void IServer::startServer() {
    // this blocks! till the end
    server_->run();
}

void Server::parallel_exec(vector<Lambda> items, IToken* token) {
    // DO WE EVER GET HERE???
#ifdef WIN32
    __debugbreak();
#endif

    for (auto item : items) {
        schedule_once(item, token);
    }
}

Server* IServer::server_ = nullptr;

} // namespace memurai::multicore
