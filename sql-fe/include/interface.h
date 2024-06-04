/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#pragma once

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "DatabaseServerEvent.h"
#include "FE_ClientReply.h"
#include "memurai-multicore.h"
#include "sync/syncqueue.h"
#include "threads/runningloop.h"
#include "tsc_interface.h"
#include "types.h"

namespace memurai {

namespace sql {
class FrontEnd;
class Database;
} // namespace sql

namespace multicore {

using std::function;
using std::ifstream;
using std::map;
using std::string;
using std::tuple;
using std::vector;

class Interface : public RunningLoop {
    IServer server;
    sql::FrontEnd* frontend;

    std::atomic<JOBID> s_jobId;

    syncqueue<tuple<ARGS, JOBID, TSCInterface*>> cmd_queue;
    void cmd_forwarder();

public:
    struct Config {
        const string metadata_path;
        const bool persistence_on;
    };

    static Config default_config;

    Interface(const IServer&, std::thread*, const Config& config = default_config);

    FE_ClientReply syncCommand(ARGS args, OuterCtx ctx);

    unsigned int enqueueCommand(ARGS args, TSCInterface* tsc);

    bool keyspace_notification(OuterCtx ctx, const string& key, int type, const char* event);

    void server_notification(OuterCtx ctx, DatabaseServerEvent event);

    virtual void stop() override;
};

bool initialize_module(vecstring, ModuleInterface);

extern Interface* ts_client;

} // namespace multicore

} // namespace memurai
