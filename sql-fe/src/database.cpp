/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#include <filesystem>
#include <fstream>
#include <iostream>
#include <locale>
#include <utility>
namespace fs = std::filesystem;

#include "PublisherV2DStrings.h"
#include "database.h"
#include "scope/take_time.h"
#include "shared_table_lock.h"
#include "table_metadata.h"

namespace memurai::sql {
QResult NO_ERROR{ErrorCode::NoError, string("")};

Database::Config Database::default_config = {100,
                                             "idxtable_",
                                             ".meta",
                                             "master.tids",
                                             ".",
#ifndef _MSC_VER
                                             "/"
#else
                                             "\\"
#endif
                                             ,
                                             false};

// ctor
//
Database::Database(const string& name, IServer& server, const Config& config)
    : name(name), server(server), config(config), s_jobId(1), c_notification_taken(0), c_notification_not_taken(0), s_global_TID(1), s_global_CID(1) {
    actual.serialize_date = [this](TID tid, StringToken stok) -> optional<int64> {
        auto iter = tid2table.find(tid);
        EXPECT_OR_THROW(iter != tid2table.end(), InternalError, "TID not found in getStringToken.");

        auto cstr = iter->second->token2string(stok);
        return IdxTable::serialize_date(cstr.c_str());
    };

    actual.serialize_timestamp = [this](TID tid, StringToken stok) -> optional<int64> {
        auto iter = tid2table.find(tid);
        EXPECT_OR_THROW(iter != tid2table.end(), InternalError, "TID not found in getStringToken.");

        auto cstr = iter->second->token2string(stok);
        return IdxTable::serialize_timestamp(cstr.c_str());
    };

    actual.getStringToken = [this](TID tid, cchar* cstr) -> StringToken {
        auto iter = tid2table.find(tid);
        EXPECT_OR_THROW(iter != tid2table.end(), InternalError, "TID not found in getStringToken.");
        return iter->second->tokenizeString(cstr);
    };

    actual.translateColumn = [this](const TableNameType& table_name, const ColumnName& col_name) -> optional<QualifiedCID> {
        // see if we find table by name
        auto iter = tables_names2info.find(table_name);
        EXPECT_OR_RETURN(iter != tables_names2info.end(), std::nullopt);
        auto tid = get<0>(iter->second);

        // Table found, we should have its TID
        auto iter2 = tid2table.find(tid);
        EXPECT_OR_THROW(iter2 != tid2table.end(), InternalError, "TID not found in translateColumn.");

        return iter2->second->translateColumnName(col_name);
    };

    actual.getTableTID = [this](const TableNameType& table_name, const SchemaName& schema) -> optional<TID> {
        EXPECT_OR_THROW(schema.empty(), InternalError, "Schema not supported.");

        // see if we find table by name
        TableNameType table_name_lc = table_name;
        Util::to_lower(table_name_lc);
        auto iter = tables_names2info.find(table_name_lc);
        EXPECT_OR_RETURN(iter != tables_names2info.end(), std::nullopt);
        auto tid = get<0>(iter->second);

        return tid;
    };

    LOG_INFO("Creating database '" << name << "' with:");
    LOG_INFO(" persistence = " << config.persistence_on);
    LOG_INFO(" num_max_tables = " << config.num_max_tables);
    LOG_INFO(" idxtable_metadata_prefix = " << config.idxtable_metadata_prefix);
    LOG_INFO(" idxtable_metadata_suffix = " << config.idxtable_metadata_suffix);
    LOG_INFO(" master_tid_path = " << config.master_tid_path);
    LOG_INFO(" metadata_path = " << config.metadata_path);
    LOG_INFO(" folder_separator = " << config.folder_separator);

    initialize();
}

void Database::initialize() {
    LOG_INFO("Initializing database '" << name << "' with:");

    if (!config.persistence_on) {
        return;
    }

    try {
        // This reads a file and populates activetids
        restoreMasterTableIndex();

        for (auto tid : activetids) {
            this->restoreTableMetadata(tid);
        }
    } catch (const std::exception& ex) {
        LOG_ERROR("Unexpected exception in Database initialization. Not using persisted metadata.");
        LOG_ERROR("Message: " << ex.what());

        dropAllTables();

        std::unique_lock<std::mutex> guard(m_tables);
        tid2table.clear();
        activetids.clear();
        tables_names2info.clear();
        prefixes.clear();
    }
}

void Database::restore(OuterCtx ctx) {
    if (!config.persistence_on) {
        return;
    }

    for (auto tid : activetids) {
        this->populatedTable(tid, ctx);
    }
}

void Database::handlePersistence(TID tid, const string& schema) {
    if (!config.persistence_on) {
        return;
    }

    if (!fs::exists(config.metadata_path)) {
        auto ret = fs::create_directory(config.metadata_path);
        EXPECT_OR_THROW(ret, FilesystemError, "Cannot create provided folder for persistence: " << config.metadata_path);
    }

    std::unique_lock<std::mutex> guard(m_tables);

    persistTableMetadata(tid, schema);
    activetids.insert(tid);
    persistMasterTableIndex();
}

IdxTable* Database::createTable(
    const string& table_name, const string& schema, const vector<QualifiedColumn>& columns, const string& prefix, const IdxTable::Config& config) {
    // Check if table exists
    auto iter = tables_names2info.find(table_name);
    if (iter == tables_names2info.end()) {

        if (activetids.size() >= this->config.num_max_tables) {
            LOG_INFO("Database exceeds the number of allowed tables (" << activetids.size() << " vs " << this->config.num_max_tables
                                                                       << "). Cannot create table " << table_name);
            return nullptr;
        }

        // acquire unique TID
        TID tid = s_global_TID++;

        // Scanning the columns to build qualified cids, and the map name->cid
        vector<FullyQualifiedCID> fqcids;

        for (auto xc : columns) {
            CID cid = s_global_CID++;
            string col_name = get<0>(xc);
            auto tup = make_tuple(col_name, cid, get<1>(xc));
            fqcids.push_back(tup);
        }

        auto ptr = createTableCore(table_name, schema, fqcids, prefix, tid, config);
        handlePersistence(tid, schema);
        return ptr;
    }

    LOG_INFO("Database asked to create a table that already exists: " << table_name);
    return nullptr;
}

void Database::addToPrefix(const string& prefix, IdxTable* ptr) {
    auto iter = prefixes.find(prefix);

    if (iter == prefixes.end()) {
        prefixes.insert(make_pair(prefix, set<IdxTable*>{ptr}));
    } else {
        iter->second.insert(ptr);
    }
}

IdxTable* Database::createTableCore(
    const string& table_name, const string& schema, const vector<FullyQualifiedCID>& fqcids, const string& prefix, TID tid, const IdxTable::Config& config) {
    IdxTable* ptr = nullptr;

    try {

        // actually create the table
        ptr = new IdxTable(table_name, prefix, fqcids, config, server, tid);

        // Now adding new table to the global structures. With mutex!!
        {
            std::unique_lock<std::mutex> guard(m_tables);
            tid2table.insert(make_pair(tid, ptr));
            tables_names2info.insert(make_pair(table_name, make_tuple(tid, ptr, prefix)));
            addToPrefix(prefix, ptr);
        }

        // need to scan all hash in Redis with this prefix!!!
#if SSCAN_MODULE_API_AVAILABLE_ONLY_IN_V6
        ptr->scan(ctx, prefix.c_str());
#endif
    } catch (...) {
        LOG_ERROR("Exception in Create table '" << table_name << "'!! ");
        if (ptr) {
            delete ptr;
            ptr = nullptr;
        }
    }

    return ptr;
}

void Database::keyspace_notification(OuterCtx ctx, const string& key, int type, const char* event) {
    // assert(type == ? ? ? );
    (void)type;
    (void)event;

    for (const auto& p_t : prefixes) {
        const string& prefix = p_t.first;
        size_t len = prefix.length();

        if (strncmp(prefix.c_str(), key.c_str(), len) == 0) {
            auto set_of_tables = p_t.second;
            for (auto table_ptr : set_of_tables) {
                table_ptr->keyspace_notification(ctx, key.c_str(), event);
            }
            ++c_notification_taken;
            return;
        }
    }

    ++c_notification_not_taken;
}

void Database::server_notification(OuterCtx ctx, DatabaseServerEvent event) {
    if (event == DatabaseServerEvent::FLUSHDB) {
        for (auto table_ptr : tid2table) {
            table_ptr.second->clear();
        }
    }
}

optional<IdxTable*> Database::getTable(const string& t_name, const string& schema) const noexcept {
    std::unique_lock<std::mutex> guard(m_tables);

    (void)schema;
    auto iter = tables_names2info.find(t_name);
    EXPECT_OR_RETURN(iter != tables_names2info.end(), std::nullopt);
    return get<1>(iter->second);
}

optional<IdxTable*> Database::getTable(TID tid) const noexcept {
    std::unique_lock<std::mutex> guard(m_tables);

    auto iter = tid2table.find(tid);
    return ((iter != tid2table.end()) ? optional<IdxTable*>(iter->second) : std::nullopt);
}

vector<string> Database::getTablesNames() const noexcept {
    std::unique_lock<std::mutex> guard(m_tables);

    vector<string> tables_names;

    for (const auto& table : tables_names2info) {
        tables_names.push_back(table.first);
    }

    return move(tables_names);
}

vector<tuple<string, vector<FullyQualifiedCID>>> Database::getTablesNamesWithColumns() const noexcept {
    std::unique_lock<std::mutex> guard(m_tables);

    vector<tuple<string, vector<FullyQualifiedCID>>> tables_names_with_columns;

    for (auto table : tables_names2info) {
        IdxTable* table_ptr = get<1>(table.second);
        auto cols_fqcid = table_ptr->getFullyQualifiedCIDs();
        tables_names_with_columns.emplace_back(table.first, vector<FullyQualifiedCID>(cols_fqcid.begin(), cols_fqcid.end()));
    }

    return move(tables_names_with_columns);
}

void Database::removeFromPrefix(const string& prefix, IdxTable* table_ptr) {
    auto p_iter = prefixes.find(prefix);
    if (p_iter == prefixes.end()) {
        LOG_ERROR("Internal error: cannot find prefix in internal list: " << prefix);
    }

    auto iter = p_iter->second.find(table_ptr);
    if (iter == p_iter->second.end()) {
        LOG_ERROR("Internal error: prefix pointing to wrong table: " << prefix);
    }

    p_iter->second.erase(table_ptr);
    if (p_iter->second.empty()) {
        prefixes.erase(p_iter);
    }
}

// Unlinks the table from the Database
// It is not destroyed yet???
//
void Database::dropTable(const string& table_name, const string& schema) {
    LOG_INFO("Dropping table " << table_name);

    // Check whether a table with that name exists
    //
    auto opt = getTable(table_name, schema);
    if (!opt.has_value()) {
        LOG_ERROR("Unlinking a table that does not exist in current DB:  tid = " << table_name);
        return;
    }

    IdxTable* table_ptr = *opt;

    // Command the table to initiate shutdown
    // Incoming commands will not be taken.
    // Running commands will eventually finish.
    //
    table_ptr->stop();

    std::unique_lock<std::mutex> guard(m_tables);
    // Check table names list and remove it. Log if info is not consistent
    //
    auto iter = tables_names2info.find(table_name);
    if (iter == tables_names2info.end()) {
        LOG_ERROR("Internal error: cannot find info about table " << table_name);
        return;
    }

    // Check tid list and remove it. Log if info is not consistent
    //
    TID tid = get<0>(iter->second);
    auto t_iter = tid2table.find(tid);
    if (t_iter == tid2table.end()) {
        LOG_ERROR("Internal error: cannot find table tid in internal list: " << tid);
    }
    if (t_iter->second != table_ptr) {
        LOG_ERROR("Internal error: tid pointing to wrong table: " << tid);
    }
    tid2table.erase(t_iter);

    // Check prefix list and remove it. Log if info is not consistent
    //
    string prefix = get<2>(iter->second);
    removeFromPrefix(prefix, table_ptr);

    tables_names2info.erase(iter);

    table_ptr->join();

    // Remove persistent metadata
    activetids.erase(tid);
    // remove idxtable persistent metadata
    string path = assembleTableMetadataPath(tid);
    fs::remove(path);

    // BOOM!
    try {
        delete table_ptr;
    } catch (...) {
        LOG_ERROR("Unexpected exception while deleting table " << table_name);
    }

    LOG_INFO("Dropping table " << table_name << " done.");
}

tuple<QWalker*, QResult> Database::getWalker(cchar* query, vecstring& others) {
    try {
        auto walker = QWalker::walkSQLQueryTree(query, actual, others);

        return make_tuple(walker, NO_ERROR);
    } catch (const memurai::exception& ex) {
        LOG_ERROR("Exception during parsing: " << ex.what());

        return make_tuple(nullptr, QResult{ErrorCode{ex.code}, string(ex.what())});
    }
}

BHandle Database::executeLeafOp_numeric(QueryExecCtx& queryCtx, hsql::OperatorType opType, const QWalker::WhereNode* left, const QWalker::WhereNode* right) {
    WhereNumericLeafFilter filterOp = translateOpToFilter_Numeric(opType);

    assert(left->type == NodeType::nColumnRef);

    CID cid = get<CID>(left->value);
    cuint64 num_rids = queryCtx.num_rids;

    switch (right->type) {
    case nLiteralNull:
        if (filterOp == EQ) {
            return queryCtx.queryInterface.leafIsNull(cid, queryCtx.jobId, num_rids);
        } else if (filterOp == NEQ) {
            return queryCtx.queryInterface.leafIsNotNull(cid, queryCtx.jobId, num_rids);
        }
        EXPECT_OR_THROW(false, InternalError, "Unrecognized op among NULL types " << filterOp);
        break;
    case nLiteralInt: {
        int64 litValue = get<int64>(right->value);
        return queryCtx.queryInterface.leafFilterInt(filterOp, litValue, cid, queryCtx.jobId, num_rids);
    }
    case nLiteralFloat: {
        double litValue = get<double>(right->value);
        return queryCtx.queryInterface.leafFilterFloat(filterOp, litValue, cid, queryCtx.jobId, num_rids);
    }
    case nLiteralDate:
    case nLiteralTimestamp: {
        int64 litValue = get<int64>(right->value);
        return queryCtx.queryInterface.leafFilterInt(filterOp, litValue, cid, queryCtx.jobId, num_rids);
    }
    default:
        EXPECT_OR_THROW(false, InternalError, "Unrecognized type in executeLeafOp " << right->type);
        break;
    }

    assert(UNREACHED);
    EXPECT_OR_THROW(false, InternalError, "Unrecognized type in executeLeafOp " << right->type);
    return 0;
}

BHandle Database::executeLeafOp_string(QueryExecCtx& queryCtx, hsql::OperatorType opType, const QWalker::WhereNode* left, const QWalker::WhereNode* right) {
    WhereStringFilter filterOp = translateOpToFilter_String(opType);

    assert(left->type == NodeType::nColumnRef);

    CID cid = get<CID>(left->value);
    cuint64 num_rids = queryCtx.num_rids;

    switch (right->type) {
    case nLiteralString: {
        StringToken stok = get<StringToken>(right->value);
        return queryCtx.queryInterface.leafFilterString(filterOp, stok, cid, queryCtx.jobId, num_rids);
    }
    default:
        EXPECT_OR_THROW(false, InternalError, "Unrecognized type in executeLeafOp " << right->type);
        break;
    }

    assert(UNREACHED);
    EXPECT_OR_THROW(false, InternalError, "Unrecognized type in executeLeafOp " << right->type);
    return 0;
}

BHandle Database::executeLeafOp(QueryExecCtx& queryCtx, hsql::OperatorType opType, const QWalker::WhereNode* left, const QWalker::WhereNode* right) {
    BHandle handle;

    if (right->type == nLiteralString) {
        handle = executeLeafOp_string(queryCtx, opType, left, right);
    } else {
        handle = executeLeafOp_numeric(queryCtx, opType, left, right);
    }

    queryCtx.addHandle(handle);

    return handle;
}

void Database::executeSorts(BHandle rids, QueryExecCtx& queryCtx, IdxTable* table, const vector<tuple<QCID, ASCDESC>>& sorting_cols) {
    if (sorting_cols.empty())
        return;

    table->sortRidsByCids(rids, sorting_cols);
}

BHandle Database::projectAll(QueryExecCtx& queryCtx, const QWalker::WhereNode* node) {
    BHandle handle = queryCtx.queryInterface.allRIDs(queryCtx.jobId, queryCtx.num_rids);
    queryCtx.addHandle(handle);
    return handle;
}

BHandle Database::executeFilters(QueryExecCtx& queryCtx, const QWalker::WhereNode* node) {
    if (node == nullptr) {
        return projectAll(queryCtx, node);
    }

    return executeFilterNode(queryCtx, node);
}

BHandle Database::executeFilterNode(QueryExecCtx& queryCtx, const QWalker::WhereNode* node) {
    assert(node);
    assert(node->kids.size() == 2);
    auto opType = get<hsql::OperatorType>(node->value);
    BHandle ret;

    decltype(std::chrono::high_resolution_clock::now()) time1, time2, time3, time4, timeB2, timeB3, timeB4, timeB5, timeB6;

    auto time_start = std::chrono::high_resolution_clock::now();
    EXPECT_OR_THROW(node->kids.size() == 2, InternalError, "Expecting 2 leaf nodes when executing node.");

    JOBID jobId = queryCtx.jobId;

    if (isLeafNode(opType)) {
        // gotten to the leaf: execute in this thread
        return executeLeafOp(queryCtx, opType, node->kids[0], node->kids[1]);
    } else {
        assert((opType == hsql::OperatorType::kOpOr) || (opType == hsql::OperatorType::kOpAnd));

        // NON-leaf operation (either AND or OR at the moment)
        // run left here (in this thread), give right to the server thread pool
        BHandle left, right;

        // yes, scope
        {
            auto lambda = [=, &time4, &time3, &right, &queryCtx, this]() {
                time3 = std::chrono::high_resolution_clock::now();
                right = executeFilterNode(queryCtx, node->kids[1]);
                time4 = std::chrono::high_resolution_clock::now();
            };
            Token token_child("name", 1, &queryCtx.gtoken);
            time1 = std::chrono::high_resolution_clock::now();
            server.schedule_once_with_token(lambda, &token_child);
            time2 = std::chrono::high_resolution_clock::now();

            // run here
            left = executeFilterNode(queryCtx, node->kids[0]);

            timeB3 = std::chrono::high_resolution_clock::now();
            token_child.join();
            timeB4 = std::chrono::high_resolution_clock::now();
        }

        if (queryCtx.gtoken.isCanceled())
            throw MEMURAI_EXCEPTION(AlreadyCanceled, "Canceling this step because token already canceled.");

        timeB5 = std::chrono::high_resolution_clock::now();

        // both child nodes are done. Let's find result now.
        hsql::OperatorType operation = get<hsql::OperatorType>(node->value);
        switch (operation) {
        case hsql::kOpAnd:
            ret = queryCtx.queryInterface.ridFilter(WhereRIDFilter::AND, {left, right}, jobId);
            break;
        case hsql::kOpOr:
            ret = queryCtx.queryInterface.ridFilter(WhereRIDFilter::OR, {left, right}, jobId);
            break;
        default:
            // always throw. I do this to be able to use the shift operator for string concatenation.
            //
            EXPECT_OR_THROW(false, InternalError, "Operator not supported in query: " << operation);
        }

        queryCtx.addHandle(ret);

        timeB6 = std::chrono::high_resolution_clock::now();
    }

    auto time_end = std::chrono::high_resolution_clock::now();
    auto dur_us = std::chrono::duration_cast<std::chrono::microseconds>(time_end - time_start).count();
    LOG_PERF("Execute node " << node->type << " TOTAL took: " << dur_us << "us.");

    return ret;
}

QResult Database::parse_and_run_query(IPublisher* publisher, cchar* query, vecstring others) {
    auto ret1 = parse_query(query, std::move(others));
    QResult res = get<0>(ret1);
    QueryCtx qctx = get<1>(ret1);
    void* handle = qctx.getHandle();

    if (get<0>(res) == ErrorCode::NoError) {
        QResult ret = execute_query(publisher, qctx);
        return ret;
    }

    QWalker* walker = (QWalker*)handle;
    delete walker;

    return res;
}

tuple<QResult, QueryCtx> Database::parse_query(cchar* query, vecstring others) {
    tuple<QWalker*, QResult> tup;

    LOG_VERBOSE("Memurai SQL Query' " << query << "'");

    auto parse_dur = TAKE_TIME {
        tup = getWalker(query, others);
    };

    QueryCtx qctx(get<0>(tup));

    LOG_PERF("Memurai SQL Query " << qctx.getQueryId() << " parsing took: " << parse_dur.micros() << "us.");
    return make_tuple(get<1>(tup), qctx);
}

BHandle Database::apply_limit_offset(QWalker* walker, BHandle filtered, QueryExecCtx& queryCtx) {
    auto limit = *(walker->get_limit());
    auto offset = *(walker->get_offset());

    auto size = Buffer::num_elems(filtered);

    limit = min(size, limit);
    offset = min(size, offset);

    if ((limit == size) && (offset == 0))
        return filtered;

    uint64 final_size = std::min(size - offset, limit);

    auto ret = Buffer::create_from(filtered, offset, final_size);

    queryCtx.addHandle(ret);
    queryCtx.num_rids = final_size;

    return ret;
}

QResult Database::execute_query(IPublisher* publisher, QueryCtx qctx) {
    QWalker* walker = (QWalker*)(qctx.getHandle());
    SCOPE_EXIT {
        qctx.close();
    };

    TID tid = walker->getSourceTableTID();
    auto opt = getTable(tid);
    EXPECT_OR_THROW(opt.has_value(), InternalError, "TID not found when executing node: " << tid);

    // This locks the table for compute till the end of this scope
    //
    shared_table_lock<IdxTable> guard{*opt};

    IdxTable::QueryInterface queryInterface = (*opt)->getQueryInterface();

    JOBID jobId = getJobId();
    QueryExecCtx queryCtx(publisher, queryInterface, jobId, server, (*opt)->n_next_rid);
    BHandle filtered;

    auto filtering_dur = TAKE_TIME {
        filtered = executeFilters(queryCtx, walker->getRootNode());
    };
    LOG_PERF("Memurai SQL Query " << qctx.getQueryId() << " filtering took: " << filtering_dur.micros() << "us.");

    if (!walker->getSortingCols().empty()) {
        auto sorting_dur = TAKE_TIME {
            executeSorts(filtered, queryCtx, *opt, walker->getSortingCols());
        };
        LOG_PERF("Memurai SQL Query " << qctx.getQueryId() << " sorting took: " << sorting_dur.micros() << "us.");
    }

    LOG_VERBOSE("Select is returning " << Buffer::num_elems(filtered) << " rids.");

    // project returned RIDs into given CIDS,
    // and returns them according to the Query instructions (stream, table?, file? s3?)
    //
    auto projection_dur = TAKE_TIME {
        BHandle handle = apply_limit_offset(walker, filtered, queryCtx);
        queryCtx.queryInterface.project(handle, walker->getProjectionsCID(), &queryCtx.gtoken, publisher);
    };
    LOG_PERF("Memurai SQL Query " << qctx.getQueryId() << " projection took: " << projection_dur.millis() << "ms.");

    if (queryCtx.gtoken.isCanceled()) {
        // more details please
        return QResult{ErrorCode::InternalError, string("Error ")};
    }

    return NO_ERROR;
}

void Database::dropAllTables() {
    std::unique_lock<std::mutex> guard(m_tables);

    for (auto t : tid2table) {
        IdxTable* table_ptr = t.second;
        auto table_name = table_ptr->get_table_name();
        LOG_INFO("Stopping table " << table_name);
        table_ptr->stop();
        LOG_INFO("Joining table " << table_name);
        table_ptr->join();
        LOG_INFO("Deleting table " << table_name);
        delete table_ptr;
    }
}

Database::~Database() {

    LOG_INFO("Database dtor.");

    dropAllTables();

    LOG_INFO("Database dtor done.");
}

string coltypeToString(ColumnType type) {
    switch (type) {
    case ColumnType::BIGINT:
        return {"BIGINT"};
    case ColumnType::FLOAT:
        return {"FLOAT"};
    case ColumnType::TEXT:
        return {"TEXT"};
    // case ColumnType::TAG:   return {"TAG"};
    case ColumnType::DATE:
        return {"DATE"};
    case ColumnType::TIMESTAMP:
        return {"TIMESTAMP"};
    case ColumnType::NULLTYPE:
        return {"NULLTYPE"};
    case ColumnType::RIDCOL:
        return {"RIDCOL"};

    default:
        EXPECT_OR_THROW(false, InternalError, "ColumnType " << int(type) << " not recognized in coltypeToString.");
    }
}

string Database::assembleTableMetadataPath(TID tid) {
    return config.metadata_path + config.folder_separator + config.idxtable_metadata_prefix + to_string(tid) + config.idxtable_metadata_suffix;
}

void Database::persistTableMetadata(TID tid, const string& schema) {
    auto iter = tid2table.find(tid);
    if (iter == tid2table.end()) {
        LOG_ERROR("Database class cannot persist metadata for TID " << tid << ". TID not found");
        return;
    }

    auto table_ptr = iter->second;
    auto table_name = table_ptr->get_table_name();

    auto iter2 = tables_names2info.find(table_name);
    if (iter2 == tables_names2info.end()) {
        LOG_ERROR("Database class cannot persist metadata for TID " << tid << ". TID not found");
        return;
    }

    auto prefix = get<2>(iter2->second);
    auto fqcdis = table_ptr->getFullyQualifiedCIDs();
    auto serialized_config = IdxTable::SerializeConfig(table_ptr->getConfig());
    PersistedTableMetadata_Write writer(tid, table_name, prefix, fqcdis, schema, serialized_config);

    cuint64 serialized_size = 16 * 1024;
    std::unique_ptr<char[]> buffer(new char[serialized_size]);
    cuint64 actual_size = writer.serialize_to(buffer.get(), serialized_size);

    string path = assembleTableMetadataPath(tid);

    std::ofstream wf(path, std::ios::out | std::ios::binary);
    if (!wf) {
        LOG_ERROR("IdxTable Metadata saving: error opening file " << path);
        return;
    }
    SCOPE_EXIT {
        wf.close();
    };

    wf.write(buffer.get(), actual_size);

    DEBUG_LOG_DEBUG("Metadata save completed for TID " << tid << ", table name " << table_name);
}

void Database::populatedTable(TID tid, OuterCtx ctx) {
    auto iter = tid2table.find(tid);

    EXPECT_OR_THROW(iter != tid2table.end(), InternalError, "Cannot find TID " << tid << " in restore.");

    auto table_ptr = iter->second;
    auto table_name = table_ptr->get_table_name();
    auto iter2 = this->tables_names2info.find(table_name);

    EXPECT_OR_THROW(iter2 != tables_names2info.end(), InternalError, "Cannot find table_name " << table_name << " in restore.");
    auto prefix = get<2>(iter2->second);

    // populate!
    auto ret = table_ptr->scan(ctx, prefix.c_str());
    LOG_INFO("Table " << table_name << ": got " << ret.num_keys << " keys restored.");
}

void Database::restoreTableMetadata(TID tid) {
    LOG_VERBOSE("Database: Restoring table TID " << tid);

    string path = assembleTableMetadataPath(tid);

    std::ifstream rf(path, std::ios::in | std::ios::binary);
    if (!rf) {
        LOG_ERROR("IdxTable Metadata saving: error opening file " << path);
        return;
    }
    SCOPE_EXIT {
        rf.close();
    };

    cuint64 serialized_size = 16 * 1024;
    std::unique_ptr<char[]> buffer(new char[serialized_size]);
    rf.read(buffer.get(), serialized_size);

    PersistedTableMetadata_Read reader(buffer.get(), rf.gcount());

    if (!reader.isValid()) {
        LOG_ERROR("Cannot retrieve metadata for index table TID " << tid << "!!");
        return;
    }

    auto num_columns = reader.getNumColumns();
    auto colnames = reader.getColumnNames();
    auto coltypes = reader.getColumnTypes();
    auto prefix = reader.getPrefix();
    auto cids = reader.getCIDs();
    auto table_name = reader.getTableName();
    auto tid_read = reader.getTID();
    auto schema = reader.getSchema();

    auto serialized_config = reader.getConfig();

    vector<FullyQualifiedCID> fqcids;

    for (uint64 cidx = 0; cidx < num_columns; cidx++) {
        fqcids.emplace_back(colnames[cidx], cids[cidx], coltypes[cidx]);
    }

    auto deserializeConfig = IdxTable::DeserializeConfig(serialized_config);

    LOG_VERBOSE("Database: metadata restored for table TID " << tid);

    auto table_ptr = createTableCore(table_name, schema, fqcids, prefix, tid, deserializeConfig);
    if (table_ptr == nullptr) {
        LOG_ERROR("Table " << table_name << " could not be created from metadata.");
        return;
    }

    LOG_INFO("Table " << table_name << " created from metadata.");

    auto local_max_cid = *max_element(cids.begin(), cids.end());

    s_global_CID = std::max(s_global_CID.load(), local_max_cid + 1);
}

void Database::persistMasterTableIndex() {
    const string path = config.metadata_path + config.folder_separator + config.master_tid_path;

    std::ofstream wf(path, std::ios::out | std::ios::binary);
    if (!wf) {
        LOG_ERROR("Database MasterTableIndex saving: error opening file " << config.master_tid_path);
        return;
    }
    SCOPE_EXIT {
        wf.close();
    };

    uint64 temp;

    temp = METADATA_VERSION;
    wf.write((const char*)&temp, sizeof(temp));

    temp = activetids.size();
    wf.write((const char*)&temp, sizeof(temp));

    for (auto tid : activetids) {
        uint32 temp32 = tid;
        wf.write((const char*)&temp32, sizeof(temp32));
    }
}

void Database::restoreMasterTableIndex() {
    cuint64 serialized_size = 16 * 1024;
    std::unique_ptr<char[]> buffer(new char[serialized_size]);

    LOG_VERBOSE("Database: Restoring MasterTableIndex.");

    uint64 version = 0;
    uint64 num_tids;

    const string path = config.metadata_path + config.folder_separator + config.master_tid_path;

    std::ifstream rf(path, std::ios::in | std::ios::binary);
    if (!rf) {
        LOG_ERROR("MasterTableIndex file not found: " << config.master_tid_path);
        return;
    }
    SCOPE_EXIT {
        rf.close();
    };

    TID maxtid = 0;
    rf.read((char*)&version, sizeof(version));
    rf.read((char*)&num_tids, sizeof(num_tids));

    EXPECT_OR_THROW(version == METADATA_VERSION, IncompatibleMetadataVersion, "Cannot load metadata written with another version.");

    // throw??
    EXPECT_OR_THROW(num_tids <= config.num_max_tables,
                    TooManyTables,
                    "Number of tables to restore exceeding allowed maximum. (" << num_tids << " vs " << config.num_max_tables << ")");

    for (uint64 idx = 0; idx < num_tids; idx++) {
        uint32 tid;
        rf.read((char*)&tid, sizeof(tid));
        activetids.insert(tid);

        maxtid = std::max(maxtid, tid);

        if (!rf) {
            activetids.clear();
            EXPECT_OR_THROW(false, InternalError, "Exception during MasterTableIndex reading. Path = " << config.master_tid_path);
        }
    }

    s_global_TID = maxtid + 1;

    LOG_VERBOSE("Database: Restoring MasterTableIndex done.");
}

} // namespace memurai::sql
