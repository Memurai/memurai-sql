/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#include <array>
#include <barrier>
#include <filesystem>
#include <fstream>
#include <string>
#include <utility>

#include "database.h"
#include "frontend.h"
#include "interface.h"
#include "logger.h"
#include "version.h"

namespace memurai::multicore {

using sql::Database;
using sql::FrontEnd;
using std::array;
using std::string;
using std::thread;
using std::to_string;
using std::vector;
namespace fs = std::filesystem;

Interface* ts_client = nullptr;

cchar* CONFIG_FILE_NUMBER_OF_THREADS_KEY = "num_threads";
cchar* CONFIG_FILE_METADATA_PATH_KEY = "metadata_path";
cchar* CONFIG_FILE_METADATA_PERSISTENCE = "metadata_persistence";

Interface::Config Interface::default_config = {string(""), false};

map<string, string> parseConfigFile(ifstream& ifsconfig) {
    std::map<string, string> ret;
    string line;
    while (std::getline(ifsconfig, line)) {
        size_t found = line.find('#');
        string line2;

        if (found != std::string::npos) {
            line2 = line.substr(0, found);
        } else {
            line2 = line;
        }

        auto line3 = Util::trim(line2, ' ');

        if (line3.empty()) {
            continue;
        }

        auto space_pos = line3.find(' ');

        if (space_pos != std::string::npos) {
            string key = line3.substr(0, space_pos);
            string val = Util::trim_left(line3.substr(space_pos + 1), ' ');
            ret.insert(make_pair(key, val));
        }
    }

    return std::move(ret);
}

bool initialize_module(vecstring args, ModuleInterface module_interface) {
#ifdef TEST_BUILD
    memurai::Logger::getInstance().setLogLevel(memurai::Logger::LogLevel::Debug);
#else
    // memurai::Logger::getInstance().setLogLevel(memurai::Logger::LogLevel::Verbose);
    memurai::Logger::getInstance().setLogLevel(memurai::Logger::LogLevel::Debug);
#endif

    auto log_me = module_interface.log_me;
    log_me("notice", (string("Memurai-SQL version=") + APP_VERSION + string(", is starting.")).c_str());
#ifdef MSC_VER
    log_me("notice", (string("QPF=") + to_string(_Query_perf_frequency()).c_str()).c_str());
#endif

    const uint32 MINIMUM_NUMBER_OF_THREADS = 4;
    const uint32 DEFAULT_COMPUTE_THREAD_POOL_SIZE = std::thread::hardware_concurrency();
    uint32 compute_pool_size = DEFAULT_COMPUTE_THREAD_POOL_SIZE;
    string metadata_path = ".";
    bool persistence_on = false;

    if (!args.empty()) {
        auto cfg_file_path = args[0];
        std::ifstream ifsconfig(cfg_file_path);

        if (ifsconfig.fail()) {
            log_me("warning", "Cannot open configuration file");
            return false;
        }

        auto data = parseConfigFile(ifsconfig);
        ifsconfig.close();

        // Number of threads
        {
            auto iter = data.find(CONFIG_FILE_NUMBER_OF_THREADS_KEY);
            if (iter != data.end()) {
                uint32 read_value = std::atol(iter->second.c_str());
                if (read_value > MINIMUM_NUMBER_OF_THREADS) {
                    compute_pool_size = read_value;
                }
            }
        }

        // Metadata Path
        {
            auto iter = data.find(CONFIG_FILE_METADATA_PATH_KEY);
            if (iter != data.end()) {
                metadata_path = iter->second;
            }
        }

        // Persistence On
        {
            auto iter = data.find(CONFIG_FILE_METADATA_PERSISTENCE);
            if (iter != data.end()) {
                if (iter->second == "on" || iter->second == "ON") {
                    persistence_on = true;
                }
            }
        }
    }

    if (!fs::exists(metadata_path)) {
        log_me("warning", (string("Cannot access metadata path: ") + metadata_path).c_str());
        return false;
    }

    string str_compute_pool_size = to_string(compute_pool_size);
    log_me("notice", (string("Memurai-sql is using num_threads: ") + str_compute_pool_size).c_str());
    log_me("notice", (string("Memurai-sql is using metadata_path: ") + metadata_path).c_str());

    std::barrier start_barrier(2);

    std::thread* t = new std::thread([=, &t, &start_barrier]() {
        IServer::Config config{compute_pool_size};
        auto serverInterface = IServer::getInterface(config, module_interface);

        ts_client = new Interface(serverInterface, t, Interface::Config{metadata_path, persistence_on});

        start_barrier.arrive_and_wait();

        // this blocks
        IServer::startServer();
    });

    start_barrier.arrive_and_wait();

    return true;
}

FE_ClientReply Interface::syncCommand(ARGS args, OuterCtx ctx) {
    auto jobId = s_jobId++;

    auto ret = frontend->processMsg_sync(args, jobId, ctx);

    return ret;
}

// This runs in Redis thread!!
// Sending command to server to run in a thread pool
// Returning immediately to the Redis single thread.
//
unsigned int Interface::enqueueCommand(ARGS args, TSCInterface* tsc) {
    auto jobId = s_jobId++;

    // add this command to a bounded circular buffer, unless it's full
    this->cmd_queue.enqueue(make_tuple(args, jobId, tsc));

    return jobId;
}

// This runs in its own thread. We left Redis thread.
//
void Interface::cmd_forwarder() {
    server.module_interface.log_me("Info", "Memurai-SQL: Interface forwarder thread is starting.");

    auto opt = cmd_queue.dequeue();

    if (opt.has_value()) {
        server.schedule_once([this, opt]() {
            auto args = std::get<0>(*opt);
            auto jobId = std::get<1>(*opt);
            auto tsc = std::get<2>(*opt);

            frontend->processMsg_async(FrontEnd::FrontEndInterface{args, jobId, tsc});
            // delete tsc;
        });
    }
}

Interface::Interface(const IServer& serverInterface, thread* t, const Config& config)
    : server(serverInterface), s_jobId(0), frontend(new FrontEnd(serverInterface, FrontEnd::Config{config.metadata_path, config.persistence_on})) {
    RunningLoop::start([this]() {
        cmd_forwarder();
    });
}

void Interface::stop() {
    RunningLoop::stop();
    cmd_queue.stop();
}

bool Interface::keyspace_notification(OuterCtx ctx, const string& key, int type, const char* event) {
    frontend->keyspace_notification(ctx, key, type, event);

    return true;
}

void Interface::server_notification(OuterCtx ctx, DatabaseServerEvent event) {
    frontend->server_notification(ctx, event);
}

} // namespace memurai::multicore
