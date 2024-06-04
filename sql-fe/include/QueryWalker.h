/*
 * Copyright (c) 2020-2021, Janea Systems
   by Benedetto Proietti
 */

#include <variant>
#include <vector>

#include "column.h"
#include "scope/expect.h"
#include "string_token.h"
#include "test/test_utils.h"
#include "tidcid.h"
#include "types.h"

// include the sql parser
#include "SQLParser.h"

#include "IActual.h"
#include "ascdesc.h"
#include "error_codes.h"
#include "fe-types.h"

#pragma once

namespace memurai::sql {

class Test_QWalker;
class IdxTable;

typedef vector<string> vecstring;

class QWalker {

public:
    typedef QWalker self_type;

    friend class Test_QWalker;

    struct WhereNode {
        NodeType type;

        // It's either an operation (OperatorType), a column CID (64bit),
        // a ...
        std::variant<hsql::OperatorType, CID, int64, double, StringToken, ColumnType> value;
        uint32 value2;

        vector<WhereNode*> kids;
    };

private:
    enum LiteralStringType { Normal, Parameter };

    bool IsValidDBString(cchar* str);

    void resolveStringParameter(WhereNode* node);

    ColumnType getColumnType(WhereNode* node);

    bool typeConvertibleTo(WhereNode* src, WhereNode* dest);

    WhereNode* createNodeLiteralDATE(StringToken stok);

    WhereNode* createNodeLiteralTIMESTAMP(StringToken stok);

    WhereNode* upgradeColumn(WhereNode* node, ColumnType newtype);

    WhereNode* createNode(hsql::OperatorType op, WhereNode* left, WhereNode* right);

    QWalker::WhereNode* handleOpNull(WhereNode* node);
    QWalker::WhereNode* handleOpNot(WhereNode* node);

    template <NodeType type> WhereNode* createNodeLeaf(uint64 value) {
        switch (type) {
        case NodeType::nColumnRef:
            throw MEMURAI_EXCEPTION(InternalError, "Unexpected node in WHERE create node.");
            // return new WhereNode{ kExprColumnRef, {CID{value}} };

        case NodeType::nLiteralFloat:
            return new WhereNode{nLiteralFloat, {double{*((double*)&value)}}, 0};

        case NodeType::nLiteralInt:
            return new WhereNode{nLiteralInt, {int64{*((int64*)&value)}}, 0};

        case NodeType::nLiteralString:
            throw MEMURAI_EXCEPTION(InternalError, "Unexpected node in WHERE create node.");
            // return new WhereNode{ nLiteralString, {value} };

        default:
            throw MEMURAI_EXCEPTION(InternalError, "Unexpected node in WHERE create node.");
        }
    }

    WhereNode* createNodeLeafStringToken(StringToken stok);

    WhereNode* createNodeLeafColumn(QualifiedCID qcid);

    WhereNode* createStringParameterNode();

    uint32 parameterCnt;

    const vecstring others;

    IActual actual;

    vector<QCID> source_columns;

    // Ordered List of projections columns;
    vector<QCID> projections;

    // Ordered List of columns in GROUP BY;
    vector<tuple<QCID, ASCDESC>> sort_columns;

    WhereNode* whereRootNode;

    // just one for now
    string default_table_name, default_table_alias, default_schema;
    TID default_table_tid;

    uint64 return_row_limit, return_row_offset;

    QualifiedCID getColumnOrThrow(cchar* exprtable, cchar* colname);

    QCID decodeColumn(hsql::Expr* col);

    void acquireSource(cchar* table_name, cchar* table_schema);

    void walkSources(hsql::TableRef* table);

    void walkProjections(const std::vector<hsql::Expr*>& projs);

    void walkWhereClause(hsql::Expr* expr);

    WhereNode* walkWhereImpl(hsql::Expr* expr);

    void walkGroupBy(hsql::GroupByDescription* groupBy);

    void walkOrderBy(const std::vector<hsql::OrderDescription*>* order);

    // JUST FOR DUMMY!
    QWalker();

    QWalker(IActual& actual, const vecstring& others);

public:
    ~QWalker();

    [[nodiscard]] decltype(sort_columns) getSortingCols() const {
        return sort_columns;
    }

    [[nodiscard]] vector<CID> getProjectionsCID() const {
        return Column::projectCIDs(projections);
    }

    [[nodiscard]] const vector<QCID>& getProjectionsQCID() const {
        return projections;
    }

    [[nodiscard]] cchar* getSourceTable() const {
        return default_table_name.c_str();
    }

    [[nodiscard]] TID getSourceTableTID() const {
        return default_table_tid;
    }

    [[nodiscard]] optional<uint64> get_limit() const {
        return {return_row_limit};
    }

    [[nodiscard]] optional<uint64> get_offset() const {
        return {return_row_offset};
    }

    void walkSelectStatement(const hsql::SelectStatement* stmt);

    void walkUpdateStatement(const hsql::UpdateStatement*);
    void walkInsertStatement(const hsql::InsertStatement*);
    void walkCreateStatement(const hsql::CreateStatement*);
    void walkImportStatement(const hsql::ImportStatement*);
    void walkExportStatement(const hsql::ExportStatement*);
    void walkTransactionStatement(const hsql::TransactionStatement*);
    void walkDeleteStatement(const hsql::DeleteStatement*);
    void walkDropStatement(const hsql::DropStatement*);
    void walkPrepareStatement(const hsql::PrepareStatement*);
    void walkExecuteStatement(const hsql::ExecuteStatement*);

    static QWalker* walkSQLQueryTree(cchar* query, IActual& actual, const vecstring& others);

    [[nodiscard]] const WhereNode* getRootNode() const {
        return this->whereRootNode;
    }
};

} // namespace memurai::sql
