/*
 * Copyright (c) 2020-2021, Janea Systems
   by Benedetto Proietti
 */

#pragma once

#include <chrono>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <tuple>

#include "types.h"

#include "DatabaseServerEvent.h"
#include "memurai-multicore.h"
#include "serializeTable.h"
#include "tsc_interface.h"

namespace memurai::sql {
using multicore::ARGS;
using multicore::IServer;
using multicore::JOBID;
using std::function;
using std::get;
using std::map;
using std::optional;
using std::set;
using std::string;
using std::stringstream;
using std::tuple;
using std::vector;
using std::chrono::milliseconds;

class IdxTable;
class Database;

typedef FE_ClientReply FE_Ret;

class FrontEnd {
public:
    struct FrontEndInterface {
        ARGS args;
        JOBID jobId;
        TSCInterface* tsc;
    };

private:
    static const string DEFAULT_SCHEMA;

public:
    struct Config {
        string metadata_path;
        bool persistence_on;
    };

    static Config default_config;

protected:
    IServer server;
    bool restored;
    Database* database;
    Config config;

    static const vector<string> available_column_types;

    map<string, set<JOBID>> tablesJobs;
    std::mutex m_tablesJobs;

    map<string, function<FE_Ret(FrontEndInterface)>> map_async_cmds;

    map<string, function<FE_Ret(ARGS, JOBID, OuterCtx)>> map_sync_cmds;

    // Synchronous commands
    //
    FE_ClientReply cmd_Create(ARGS args, JOBID jobId, OuterCtx ctx);
    FE_ClientReply cmd_Size(ARGS args, JOBID jobId, OuterCtx ctx);
    FE_ClientReply cmd_Describe(ARGS args, JOBID jobId, OuterCtx ctx);
    FE_ClientReply cmd_Show(ARGS args, JOBID jobId, OuterCtx ctx);
    FE_ClientReply cmd_Debug_Describe(ARGS args, JOBID jobId, OuterCtx ctx);
    FE_ClientReply cmd_Restore(ARGS args, JOBID jobId, OuterCtx ctx);

    // Asynchronous commands
    //
    FE_ClientReply cmd_Query(FrontEndInterface inter);
    FE_ClientReply cmd_Destroy(FrontEndInterface inter);

    void addJobForTable(const string& key, JOBID jobId);

public:
    explicit FrontEnd(const IServer& server, const Config& config = default_config);

    ~FrontEnd();

    // These runs in the server thread pool.
    //
    void processMsg_async(FrontEndInterface);

    FE_Ret processMsg_sync(ARGS args, JOBID jobId, OuterCtx ctx);

    //
    void keyspace_notification(OuterCtx ctx, const string& key, int type, const char* event);

    void server_notification(OuterCtx ctx, DatabaseServerEvent event);
};

} // namespace memurai::sql
