/*
* Copyright (c) 2020-2021, Janea Systems
by Benedetto Proietti
*/

#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <utility>

#include "frontend.h"
#include "lambda.h"

#include "util.h"

#include "PublisherV2DStrings.h"
#include "concats.h"
#include "database.h"
#include "idxtable.h"
#include "scope/take_time.h"

namespace memurai::sql {
using multicore::ARGS;
using multicore::JOBID;
using std::make_pair;
using std::make_tuple;
using std::string;
using std::stringstream;
using std::to_string;

const map<string, ColumnType> colTypeMap = {{"text", ColumnType::TEXT},
                                            {"bigint", ColumnType::BIGINT},
                                            {"float", ColumnType::FLOAT},
                                            {"date", ColumnType::DATE},
                                            {"null", ColumnType::NULLTYPE},
                                            {"timestamp", ColumnType::TIMESTAMP}};

const string FrontEnd::DEFAULT_SCHEMA = "";
FrontEnd::Config FrontEnd::default_config = {"msql_meta"};

optional<ColumnType> decodeType(const string& t) {
    auto iter = colTypeMap.find(t);
    if (iter == colTypeMap.end())
        return std::nullopt;

    return iter->second;
}

bool decodeTimestamp(cchar* value, Timestamp& ret) {
    ret = std::atoll(value);

    return (ret != 0);
}

// ctor
FrontEnd::FrontEnd(const IServer& server_, const Config& config) : server(server_), restored(false), config(config), database(nullptr) {
    this->server.module_interface = server_.module_interface;

    Database::Config dbcfg = Database::default_config;
    dbcfg.metadata_path = config.metadata_path;
    dbcfg.persistence_on = config.persistence_on;

    database = Database::createDatabase("db0", server, dbcfg);

    map_sync_cmds.insert(make_pair("msql.create", [this](ARGS args, JOBID jobId, OuterCtx ctx) -> FE_Ret {
        return this->cmd_Create(std::move(args), jobId, ctx);
    }));
    map_sync_cmds.insert(make_pair("msql.size", [this](ARGS args, JOBID jobId, OuterCtx ctx) {
        return this->cmd_Size(std::move(args), jobId, ctx);
    }));
    map_sync_cmds.insert(make_pair("msql.restore", [this](ARGS args, JOBID jobId, OuterCtx ctx) {
        return this->cmd_Restore(std::move(args), jobId, ctx);
    }));
    map_sync_cmds.insert(make_pair("msql.describe", [this](ARGS args, JOBID jobId, OuterCtx ctx) {
        return this->cmd_Describe(std::move(args), jobId, ctx);
    }));
    map_sync_cmds.insert(make_pair("msql.show", [this](ARGS args, JOBID jobId, OuterCtx ctx) {
        return this->cmd_Show(std::move(args), jobId, ctx);
    }));
    map_sync_cmds.insert(make_pair("msql.debug.describe", [this](ARGS args, JOBID jobId, OuterCtx ctx) {
        return this->cmd_Debug_Describe(std::move(args), jobId, ctx);
    }));

    map_async_cmds.insert(make_pair("msql.query", [this](FrontEndInterface inter) {
        return this->cmd_Query(std::move(inter));
    }));
    map_async_cmds.insert(make_pair("msql.destroy", [this](FrontEndInterface inter) {
        return this->cmd_Destroy(std::move(inter));
    }));
}

FrontEnd::~FrontEnd() {
    LOG_INFO("FrontEnd dtor.");

    delete database;
}

// DIRECT CALL!!!
// Must be re-entrant
//
void FrontEnd::keyspace_notification(OuterCtx ctx, const string& key, int type, const char* event) {
    database->keyspace_notification(ctx, key, type, event);
}

void FrontEnd::server_notification(OuterCtx ctx, DatabaseServerEvent event) {
    database->server_notification(ctx, event);
}

#define CONSUME1(args)                 \
    if (args.size() == 0) {            \
        return ERROR_SQL_WRONG_FORMAT; \
    } else {                           \
        Consume1(args);                \
    }
#define READ1(DEST)                    \
    if (args.size() == 0) {            \
        return ERROR_SQL_WRONG_FORMAT; \
    } else {                           \
        Read1(args, DEST);             \
    }
#define EXPECT1(VAL)                       \
    {                                      \
        string temp;                       \
        READ1(temp);                       \
        if (temp.compare(VAL) != 0) {      \
            return ERROR_SQL_WRONG_FORMAT; \
        }                                  \
    }

#define EXPECT_LABEL_AT_POS(LABEL, POS)                                          \
    Util::to_lower(args[POS]);                                                   \
    if (args[POS].compare(LABEL) != 0) {                                         \
        SYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_WRONG_FORMAT, "Wrong SQL format."); \
    }

void Read1(ARGS& args, string& ret) {
    ret = args[0];
    args.erase(args.begin());
}

void Consume1(ARGS& args) {
    args.erase(args.begin());
}

#define CHECK_ARGS_AND_SERIES                                                                   \
    auto argc = args.size();                                                                    \
    if (argc < MINIMUM_ARGC) {                                                                  \
        LOG_INFO("Command " << command << " ignored because of wrong arg count.");              \
        SEND_ERROR_REPLY(int(ERROR_SQL_WRONG_ARGC), "Wrong number of arguments : " << argc);    \
        return ERROR_SQL_WRONG_ARGC;                                                            \
    }                                                                                           \
    string key = args[KEY_POSITION_IDX];                                                        \
    IdxTable* table = nullptr;                                                                  \
                                                                                                \
    bool found = safe_tablesByName_find(key, table);                                            \
    if (!found) {                                                                               \
        LOG_INFO("Command " << command << " ignored because table " << key << " not found.");   \
        SEND_ERROR_REPLY(int(ERROR_SQL_TABLE_DOES_NOT_EXIST), "Table does not exist: " << key); \
        return ERROR_SQL_TABLE_DOES_NOT_EXIST;                                                  \
    }                                                                                           \
    uint32 Larity = table->getLArity();                                                         \
    vector<string> labels = table->getLabels();

#define NEW_CHECK_ARGS_AND_SERIES                                                               \
    auto argc = args.size();                                                                    \
    if (argc < MINIMUM_ARGC) {                                                                  \
        LOG_INFO("Command " << command << " ignored because of wrong arg count.");              \
        SEND_ERROR_REPLY(int(ERROR_SQL_WRONG_ARGC), "Wrong number of arguments : " << argc);    \
        return ERROR_SQL_WRONG_ARGC;                                                            \
    }                                                                                           \
    CONSUME1(args);                                                                             \
    string key;                                                                                 \
    READ1(key);                                                                                 \
    IdxTable* table = nullptr;                                                                  \
                                                                                                \
    auto opt = database->getTable(key, DEFAULT_SCHEMA);                                         \
    if (!opt.has_value()) {                                                                     \
        LOG_INFO("Command " << command << " ignored because table " << key << " not found.");   \
        SEND_ERROR_REPLY(int(ERROR_SQL_TABLE_DOES_NOT_EXIST), "Table does not exist: " << key); \
        return ERROR_SQL_TABLE_DOES_NOT_EXIST;                                                  \
    }

#define SYNC_REPLY_NUMBER_TO_CLIENT(EC, NUM) \
    { return FE_ClientReply{int(NUM)}; }

#define SYNC_REPLY_ERROR_TO_CLIENT(EC, MSG)                                                     \
    {                                                                                           \
        std::stringstream ssx;                                                                  \
        ssx << "ERR in " << command << ": " << MSG;                                             \
        LOG_ERROR(ssx.str());                                                                   \
        return FE_ClientReply{Reply{int(ErrorCode COLONCOLON(EC)), vector<string>{ssx.str()}}}; \
    }

#define SYNC_REPLY_STRING_VECTOR_TO_CLIENT(EC, V) \
    { return FE_ClientReply{Reply{int(ErrorCode COLONCOLON(EC)), V}}; }

#define SYNC_REPLY_TO_CLIENT(MSG)                                              \
    {                                                                          \
        std::stringstream ssx;                                                 \
        ssx << command << ": " << MSG;                                         \
        LOG_INFO(ssx.str());                                                   \
        return FE_ClientReply{Reply{int(SUCCESS), vector<string>{ssx.str()}}}; \
    }

#define SYNC_REPLY_ERROR_TO_CLIENT2(EC, MSG)              \
    {                                                     \
        std::stringstream ssx;                            \
        ssx << "ERR in " << command << ": " << MSG;       \
        LOG_ERROR(ssx.str());                             \
        return Reply{int(EC), vector<string>{ssx.str()}}; \
    }

#define ASYNC_REPLY_ERROR_TO_CLIENT(EC, MSG)                                                         \
    {                                                                                                \
        std::stringstream ssx;                                                                       \
        ssx << "ERR in " << command << ": " << MSG;                                                  \
        LOG_ERROR(ssx.str());                                                                        \
        inter.tsc->reply_to_client(Reply{int(ErrorCode COLONCOLON(EC)), vector<string>{ssx.str()}}); \
    }

// Create a IdxTable capped with maximum capacity
//
// MSQL.CREATE mymovies ON hash "movie:" title TEXT release_year NUMERIC rating NUMERIC genre TAG
//
// MSQL.CREATE {INDEXESNAME} on hash {PREFIX} {COL1NAME} {COL1TYPE} [ {COL2NAME} {COL2TYPE} ,.... ] ]
// {INDEXESNAME} : string
// {PREFIX} : string
// {COL?NAME} : string field present in hash object with given prefix.
// {COL?TYPE} : one of:    TEXT, BIGINT, FLOAT, DATE, TIMESTAMP
//
// For now all elements of the IdxTablees are int64
//

FE_Ret FrontEnd::cmd_Create(ARGS args, JOBID jobId, OuterCtx ctx) {
    static const string command("msql.create");

    static const int KEY_POSITION_IDX = 1;
    static const int LABEL_ON_POSITION_IDX = 2;
    static const int LABEL_HASH_POSITION_IDX = 3;
    static const int PREFIX_VALUE_POSITION_IDX = 4;
    static const int FIRST_COLNAME_POSITION_IDX = 5;
    static const int FIRST_COLTYPE_POSITION_IDX = 6;
    static const int MINIMUM_ARGC = 7;

    auto key = args[KEY_POSITION_IDX];
    auto argc = args.size();

    Util::to_lower(key);

    if (!IdxTable::isValidTableName(key)) {
        SYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_TABLE_NAME_INVALID, "Table name not following valid syntax: " << key);
    }

    // expecting 8 or more args. Even number.
    if ((argc < MINIMUM_ARGC) || (2 * (argc >> 1) == argc)) {
        SYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_WRONG_ARGC, "Wrong number of arguments: " << argc);
    }

    EXPECT_LABEL_AT_POS("on", LABEL_ON_POSITION_IDX);
    EXPECT_LABEL_AT_POS("hash", LABEL_HASH_POSITION_IDX);

    auto prefix = args[PREFIX_VALUE_POSITION_IDX];

    vector<QualifiedColumn> qcols;
    for (int i = FIRST_COLNAME_POSITION_IDX; i < argc; i += 2) {
        auto& colname = args[i];
        auto type_str = args[i + 1];
        Util::to_lower(type_str);
        auto verified_type = decodeType(type_str);
        if (!verified_type.has_value())
            SYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_WRONG_COL_TYPE, "SQL wrong column type: " << colname);

        qcols.emplace_back(colname, *verified_type);
    }

    IdxTable* table = nullptr;

    // if key already present no error is given
    auto opt = database->getTable(key, DEFAULT_SCHEMA);

    if (opt.has_value()) {
        SYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_TABLE_ALREADY_EXISTS, "Table already exists: " << key);
    }

    if (database->get_num_tables() >= database->config.num_max_tables) {
        SYNC_REPLY_ERROR_TO_CLIENT(TooManyTables, "Database has exceed number of allowed tables.");
    }

    vector<string> ret_msgs;
    auto dur_ms = TAKE_TIME {
        // Table does NOT exist
        // Let's create it.
        //
        ///  When we reach the threshold, then we grow linearly by 10x
        static constexpr GrowingStrategy gstrat = {GrowingStrategy::StrategyType::Linear, 10};
        /// This calculates the unsorted capacity related to the sorted one.
        static constexpr ThresholdStrategy sstrat = {ThresholdStrategy::StrategyType::Percentage, 10};
        static constexpr IdxTable::Config config = {50'000, 1'000, gstrat, sstrat};
        table = database->createTable(key, DEFAULT_SCHEMA, qcols, prefix, config);

        if (ctx != nullptr) {
            ret_msgs.push_back("Table created: '" + key + "'");

            auto ret = table->scan(ctx, prefix.c_str());
            ret_msgs.push_back("scanned " + std::to_string(ret.num_keys) + " keys.");
            ret_msgs.insert(ret_msgs.end(), ret.msgs.begin(), ret.msgs.end());
        }
    };

    LOG_INFO("Table created: " << key);
    return std::move(FE_ClientReply(Reply{int(SUCCESS), ret_msgs}));
}

//
// MSQL.QUERY  "SELECT TITLE, RATING FROM mymovies WHERE RELEASED < 1/1/1980 AND VIEWS > 1000000"
//
// MSQL.QUERY  [QUERY TEXT]
//
//
FE_Ret FrontEnd::cmd_Query(FrontEndInterface inter) {
    static const string command("msql.query");

    static const int QUERY_POSITION_IDX = 1;
    static const int MINIMUM_ARGC = 2;

    auto args = inter.args;
    auto argc = args.size();

    if (argc < MINIMUM_ARGC) {
        SYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_WRONG_ARGC, "Wrong number of arguments: " << argc);
    }

    auto query_text = args[QUERY_POSITION_IDX];

    // Create a Publisher that dumps data into a stream
    shared_ptr<PublisherV2DStrings> publisher(new PublisherV2DStrings());

    // run query - in 2 parts
    // First parse it and return possible error to client
    // Then execute it.

    auto other_args = args;
    other_args.erase(other_args.begin(), other_args.begin() + 2);
    tuple<QResult, QueryCtx> ret1 = database->parse_query(query_text.c_str(), other_args);
    QResult& qr = get<0>(ret1);
    ErrorCode ec = get<0>(qr);

    // Send the reply in any case before executing the query
    //
    if (ec != SUCCESS) {
        LOG_ERROR("FrontEnd query failed: " << get<1>(qr));
        SYNC_REPLY_ERROR_TO_CLIENT2(ec, get<1>(qr));
    }

    Token token("name", inter.jobId, 1);
    QResult qres;

    server.schedule_once_with_token(
        [=, &token, &qres, this]() {
            qres = database->execute_query(publisher.get(), get<1>(ret1));
        },
        &token);

    token.join();

    if (get<0>(qres) == SUCCESS) {
        return FE_ClientReply{publisher};
    }

    SYNC_REPLY_ERROR_TO_CLIENT2(get<0>(qres), get<1>(qres));
}

void FrontEnd::addJobForTable(const string& key, JOBID jobId) {
    std::unique_lock<std::mutex> lock(m_tablesJobs);
    tablesJobs[key].insert(jobId);
}

// We are in Server thread pool now.
//
void FrontEnd::processMsg_async(FrontEndInterface inter) {
    auto args = inter.args;
    auto command = args[0];
    Util::to_lower(command);

    // slice key into dots "."
    vector<string> slice;
    Util::split(slice, command, '.');

    if (slice[0] != "msql") {
        LOG_INFO("Message ignored. Expecting command starting with 'msql.' " << command);
        ASYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_CMD_NOT_SUPPORTED, "Command not supported.");
        return;
    }

    auto iter = map_async_cmds.find(command);
    if (iter != map_async_cmds.end()) {
        auto time_start = std::chrono::high_resolution_clock::now();
        auto reply = iter->second(inter);
        auto time_end = std::chrono::high_resolution_clock::now();
        auto elapsedMS = std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count();

        stringstream ssx;
        ssx << "Command " << command << ' ' << command << " took " << elapsedMS << "ms";
        if (elapsedMS > 0) {
            LOG_VERBOSE(ssx.str());
        }
#ifdef _DEBUG
        else {
            LOG_DEBUG(ssx.str());
        }
#endif

        inter.tsc->reply_to_client(reply);
    } else {
        LOG_INFO("FrontEnd: command ignored because no action is registered for it: " << command);
        ASYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_CMD_NOT_SUPPORTED, "Command not supported.");
    }

    // SPECIAL COMMANDS also handled by server.

    if ((slice.size() > 2) && (slice[3] == "stop")) {
        if (!args.empty()) {
            JOBID jobId = atol(args[1].c_str());
            server.stop_command(jobId);
        } else {
            LOG_INFO("FrontEnd: ignoring stop command because jobId not provided.");
        }
    }
}

FE_Ret FrontEnd::processMsg_sync(ARGS args, JOBID jobId, OuterCtx ctx) {
    auto command = args[0];
    Util::to_lower(command);

    auto iter = map_sync_cmds.find(command);
    if (iter != map_sync_cmds.end()) {
        // auto time_start = std::chrono::high_resolution_clock::now();
        auto ret = iter->second(args, jobId, ctx);
        // auto time_end = std::chrono::high_resolution_clock::now();
        // auto dur_ms = std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count();

        return ret;
    }

    LOG_INFO("FrontEnd: command ignored because no action is registered for it: " << command);
    SYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_CMD_NOT_SUPPORTED, "Command not supported");
}

// Create a IdxTable capped with maximum capacity
//
// MSQL.SIZE mymovies
//
// MSQL.SIZE {INDEXESNAME}
// {INDEXESNAME} : string
//
//

FE_Ret FrontEnd::cmd_Size(ARGS args, JOBID jobId, OuterCtx ctx) {
    static const string command("msql.size");

    static const int INDEXES_POSITION_IDX = 1;
    static const int MINIMUM_ARGC = 2;

    auto argc = args.size();

    if (argc < MINIMUM_ARGC) {
        SYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_WRONG_ARGC, "Wrong number of arguments: " << argc);
    }

    auto table_name = args[INDEXES_POSITION_IDX];
    Util::to_lower(table_name);

    auto opt = database->getTable(table_name, DEFAULT_SCHEMA);

    if (!opt.has_value()) {
        SYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_TABLE_DOES_NOT_EXIST, "Table does not exist: " << table_name);
    }

    auto num_rows = (*opt)->getNumRowsApprox();

    return std::move(FE_ClientReply{Reply{int(num_rows)}});
}

// Destroy a IdxTable
//
// MSQL.DESTROY mymovies
//
// MSQL.DESTROY {INDEXESNAME}
// {INDEXESNAME} : string
//
//

FE_Ret FrontEnd::cmd_Destroy(FrontEndInterface inter) {
    static const string command("msql.destroy");

    static const int TABLENAME_POSITION_IDX = 1;

    auto args = inter.args;
    auto argc = args.size();

    if (argc != 2) {
        SYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_WRONG_ARGC, "Wrong number of argument for command.");
    }

    auto table_name = args[TABLENAME_POSITION_IDX];
    Util::to_lower(table_name);
    auto opt = database->getTable(table_name, DEFAULT_SCHEMA);

    if (!opt.has_value()) {
        SYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_TABLE_DOES_NOT_EXIST, "Table does not exist: " << table_name);
    }

    IdxTable* table_ptr = *opt;

    database->dropTable(table_name, DEFAULT_SCHEMA);

    SYNC_REPLY_TO_CLIENT("Table " << table_name << " dropped.");
}

FE_Ret FrontEnd::cmd_Debug_Describe(ARGS args, JOBID jobId, OuterCtx ctx) {
    static const string command("msql.debug.describe");

    static const int TABLENAME_POSITION_IDX = 1;
    auto argc = args.size();

    if (argc != 2) {
        SYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_WRONG_ARGC, "Wrong number of argument for command.");
    }

    auto table_name = args[TABLENAME_POSITION_IDX];
    Util::to_lower(table_name);
    auto opt = database->getTable(table_name, DEFAULT_SCHEMA);

    if (!opt.has_value()) {
        SYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_TABLE_DOES_NOT_EXIST, "Table does not exist: " << table_name);
    }

    auto table_info = (*opt)->getDebugInfo();

    vector<string> processed_info;
    for (auto s : table_info) {
        processed_info.push_back(get<0>(s) + " " + get<1>(s));
    }

    SYNC_REPLY_STRING_VECTOR_TO_CLIENT(NoError, processed_info);
}

FE_Ret FrontEnd::cmd_Describe(ARGS args, JOBID jobId, OuterCtx ctx) {
    static const string command("msql.describe");

    static const int TABLENAME_POSITION_IDX = 1;
    auto argc = args.size();

    if (argc != 2) {
        SYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_WRONG_ARGC, "Wrong number of argument for command.");
    }

    auto table_name = args[TABLENAME_POSITION_IDX];
    Util::to_lower(table_name);
    auto opt = database->getTable(table_name, DEFAULT_SCHEMA);

    if (!opt.has_value()) {
        SYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_TABLE_DOES_NOT_EXIST, "Table does not exist: " << table_name);
    }

    auto table_info = (*opt)->getInfo();

    auto table_info_tid = get<0>(table_info);

    auto col_info = get<1>(table_info);

    vector<string> processed_info;

    processed_info.push_back("TID " + table_info_tid);

    for (auto item : col_info) {
        processed_info.push_back(get<0>(item) + ' ' + coltypeToString(get<2>(item)));
    }

    SYNC_REPLY_STRING_VECTOR_TO_CLIENT(NoError, processed_info);
}

FE_Ret FrontEnd::cmd_Show(ARGS args, JOBID jobId, OuterCtx ctx) {
    static const string command("msql.show");

    static const int LABEL_WITH_POSITION_IDX = 1;
    static const int LABEL_COLUMNS_POSITION_IDX = 2;

    auto argc = args.size();

    //

    if ((argc != 1) && (argc != 3)) {
        SYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_WRONG_ARGC, "Wrong number of argument for command.");
    }

    if (argc == 1) {
        SYNC_REPLY_STRING_VECTOR_TO_CLIENT(NoError, database->getTablesNames());
    }

    EXPECT_LABEL_AT_POS("with", LABEL_WITH_POSITION_IDX);
    EXPECT_LABEL_AT_POS("columns", LABEL_COLUMNS_POSITION_IDX);

    auto names_with_cols = database->getTablesNamesWithColumns();

    vector<string> ret;

    for (auto table : names_with_cols) {
        ret.push_back(get<0>(table));
        int cnt = 1;
        for (auto col : get<1>(table)) {
            ret.push_back(to_string(cnt++) + ") " + get<0>(col) + ", " + coltypeToString(get<2>(col)));
        }
    }

    return FE_ClientReply{Reply{int(ErrorCode::NoError), ret}};
}

FE_ClientReply FrontEnd::cmd_Restore(ARGS args, JOBID jobId, OuterCtx ctx) {
    static const string command("msql.restore");

    if (restored) {
        LOG_ERROR("Can restore only once!");
        SYNC_REPLY_ERROR_TO_CLIENT(ERROR_SQL_TABLES_ALREADY_RESTORED, "Tables already restored!");
    }

    restored = true;

    LOG_VERBOSE("FrontEnd: Restore starting.");
    database->restore(ctx);
    LOG_VERBOSE("FrontEnd: Restore done.");

    SYNC_REPLY_TO_CLIENT("Restore done.");
}

} // namespace memurai::sql
