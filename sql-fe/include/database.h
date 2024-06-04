/*
* Copyright (c) 2020-2021, Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <sstream>
#include <utility>
#include <variant>
#include <vector>

#include "Buffer.h"
#include "DatabaseServerEvent.h"
#include "IActual.h"
#include "QueryCtx.h"
#include "QueryWalker.h"
#include "TokenizedStrings.h"
#include "column.h"
#include "error_codes.h"
#include "idxtable.h"
#include "memurai-multicore.h"
#include "rid.h"
#include "threads/grouptoken.h"

namespace memurai::sql {
using multicore::GroupToken;
using multicore::IServer;
using multicore::IToken;
using std::atomic;
using std::map;
using std::tuple;
using std::variant;
using std::vector;

class IdxTable;

class QueryExecCtx {
private:
    VecHandles handles;
    std::mutex m_handles;

public:
    IdxTable::QueryInterface& queryInterface;
    const JOBID jobId;
    GroupToken gtoken;
    IServer server;
    uint64 num_rids;

    QueryExecCtx(IPublisher* publisher, IdxTable::QueryInterface& queryInterface, JOBID jobId, IServer server, uint64 num_rids)
        : jobId(jobId), queryInterface(queryInterface), gtoken(jobId), server(std::move(server)), num_rids(num_rids) {
        publisher->query_started();
    }

    ~QueryExecCtx() {
        for (auto handle : handles) {
            Buffer::destroy(handle);
        }
    }

    void addHandle(BHandle handle) {
        std::unique_lock<std::mutex> guard(m_handles);
        handles.push_back(handle);
    }
};

class Database {
    static constexpr uint64 METADATA_VERSION = 0x1;

    // table name ->   (TID, ptr, prefix)
    map<string, tuple<TID, IdxTable*, string>> tables_names2info;

    set<TID> activetids;

    // map from TID to:
    map<TID,
        IdxTable* //  table pointer
        >
        tid2table;

    // Map:  prefix -> set<table>
    //
    map<string, set<IdxTable*>> prefixes;

    // Mutex for above 4 entities
    mutable std::mutex m_tables;

    IActual actual;

    // Self-explanatory I guess
    //
    atomic<TID> s_global_TID;
    atomic<CID> s_global_CID;

    mutable atomic<JOBID> s_jobId;

    mutable atomic<uint64> c_notification_not_taken, c_notification_taken;

public:
    struct Config {
        uint32 num_max_tables;
        string idxtable_metadata_prefix;
        string idxtable_metadata_suffix;
        string master_tid_path;
        string metadata_path;
        string folder_separator;
        bool persistence_on;
    };
    static Config default_config;

    typedef string DateAsString;
    typedef bool NullValue;
    typedef uint64 TSU64;
    typedef variant<string, int64, double, TSU64, DateAsString, NullValue> OneValueImpl;
    typedef tuple<ColumnType, OneValueImpl> OneValue;

    friend class Test_Database;

    // ctor
    Database(const string& name, IServer& server, const Config& config = default_config);

    // public but immutable!!
    const Config config;

    uint64 get_num_tables() const {
        std::unique_lock<std::mutex> guard(m_tables);
        return activetids.size();
    }

private:
    const string name;

    IServer& server;

    void addToPrefix(const string& prefix, IdxTable* ptr);
    void removeFromPrefix(const string& prefix, IdxTable* ptr);

    string assembleTableMetadataPath(TID tid);

    tuple<QWalker*, QResult> getWalker(cchar* query, vecstring& others);

    BHandle executeLeafOp_numeric(QueryExecCtx& queryCtx, hsql::OperatorType opType, const QWalker::WhereNode* left, const QWalker::WhereNode* right);
    BHandle executeLeafOp_string(QueryExecCtx& queryCtx, hsql::OperatorType opType, const QWalker::WhereNode* left, const QWalker::WhereNode* right);
    BHandle executeLeafOp(QueryExecCtx& queryCtx, hsql::OperatorType opType, const QWalker::WhereNode* left, const QWalker::WhereNode* right);
    BHandle executeLeafOpUnary(QueryExecCtx& queryCtx, hsql::OperatorType opType, const QWalker::WhereNode* node);
    BHandle executeOpNot(QueryExecCtx& queryCtx, const QWalker::WhereNode* node);

    BHandle projectAll(QueryExecCtx& queryCtx, const QWalker::WhereNode* node);

    BHandle executeFilters(QueryExecCtx& queryCtx, const QWalker::WhereNode* node);
    BHandle executeFilterNode(QueryExecCtx& queryCtx, const QWalker::WhereNode* node);
    void executeSorts(BHandle filtered, QueryExecCtx& queryCtx, IdxTable* table, const vector<tuple<QCID, ASCDESC>>&);
    BHandle apply_limit_offset(QWalker* walker, BHandle filtered, QueryExecCtx& queryCtx);

    JOBID getJobId() const {
        return s_jobId++;
    }

    void persistTableMetadata(TID tid, const string& schema);
    void restoreTableMetadata(TID tid);
    void populatedTable(TID tid, OuterCtx ctx);

    //
    void restoreMasterTableIndex();
    void persistMasterTableIndex();

    static bool isLeafNode(hsql::OperatorType optype) {
        switch (optype) {
        case hsql::kOpAnd:
        case hsql::kOpOr:
            return false;

        default:
            return true;
        }
    }

    IdxTable* createTableCore(const string& table_name,
                              const string& schema,
                              const vector<FullyQualifiedCID>& fqcids,
                              const string& prefix,
                              TID tid,
                              const IdxTable::Config& config = IdxTable::default_config);

    void initialize();

    void handlePersistence(TID tid, const string& schema);

    void dropAllTables();

public:
    ~Database();

    IActual getActualInterface() {
        return actual;
    }

    optional<IdxTable*> getTable(const string& table_name, const string& schema) const noexcept;

    optional<IdxTable*> getTable(TID tid) const noexcept;

    vector<string> getTablesNames() const noexcept;

    vector<tuple<string, vector<FullyQualifiedCID>>> getTablesNamesWithColumns() const noexcept;

    void dropTable(const string& table_name, const string& schema);

    IdxTable* createTable(const string& table_name,
                          const string& schema,
                          const vector<QualifiedColumn>& columns,
                          const string& prefix,
                          const IdxTable::Config& config = IdxTable::default_config);

    void keyspace_notification(OuterCtx ctx, const string& key, int type, const char* event);

    void server_notification(OuterCtx ctx, DatabaseServerEvent event);

    // Just parses the query. Usually done in Redis thread
    //
    tuple<QResult, QueryCtx> parse_query(cchar* query, vecstring others);

    // Executes a query previously parsed.
    // Could be slow. Should NOT be done in Redis thread.
    //
    QResult execute_query(IPublisher* publisher, QueryCtx qctx);

    // Parses AND executes a query.
    // Could be slow. Should NOT be done in Redis thread.
    QResult parse_and_run_query(IPublisher* publisher, cchar* query, vecstring others = {""});

    void restore(OuterCtx ctx);

    static Database* createDatabase(const string& name, IServer& server, const Config& config) {
        return new Database(name, server, config);
    }

    static WhereStringFilter translateOpToFilter_String(hsql::OperatorType opType) {
        switch (opType) {
        case hsql::kOpEquals:
            return WhereStringFilter::EQ_CASE_SENSITIVE;
        case hsql::kOpLike:
            return WhereStringFilter::PATTERN;
        default:
            EXPECT_OR_THROW(false, InternalError, "Cannot decode OperatorType " << opType);
        }
    }

    static WhereNumericLeafFilter translateOpToFilter_Numeric(hsql::OperatorType opType) {
        switch (opType) {
        case hsql::kOpEquals:
            return WhereNumericLeafFilter::EQ;
        case hsql::kOpNotEquals:
            return WhereNumericLeafFilter::NEQ;
        case hsql::kOpGreater:
            return WhereNumericLeafFilter::GT;
        case hsql::kOpGreaterEq:
            return WhereNumericLeafFilter::GE;
        case hsql::kOpLess:
            return WhereNumericLeafFilter::LT;
        case hsql::kOpLessEq:
            return WhereNumericLeafFilter::LE;
        default:
            EXPECT_OR_THROW(false, InternalError, "Cannot decode OperatorType " << opType);
        }
    }
};

#define EXTRACT_ERROR(RESPONSE) get<0>(RESPONSE)

} // namespace memurai::sql
