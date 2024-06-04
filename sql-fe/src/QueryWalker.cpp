/*
* Copyright (c) 2021, Janea Systems
by Benedetto Proietti
*/

#include "QueryWalker.h"

namespace memurai::sql {
QWalker::QWalker() : parameterCnt(0), whereRootNode(nullptr), return_row_offset(0) {}

QWalker::QWalker(IActual& actual, const vecstring& others) : actual(actual), parameterCnt(0), others(others), whereRootNode(nullptr), return_row_offset(0) {}

QWalker::~QWalker() {
    // delete stuff
}

QWalker::WhereNode* QWalker::createNodeLeafStringToken(StringToken stok) {
    return new WhereNode{nLiteralString, {stok}, LiteralStringType::Normal};
}

QWalker::WhereNode* QWalker::createNodeLeafColumn(QualifiedCID qcid) {
    return new WhereNode{nColumnRef, get<0>(qcid), uint32(get<1>(qcid))};
}

QWalker::WhereNode* QWalker::createStringParameterNode() {
    EXPECT_OR_THROW(parameterCnt < others.size(), MalformedInput, "Query missing actual parameters.");
    return new WhereNode{nLiteralString, int64(parameterCnt++), LiteralStringType::Parameter};
}

ColumnType QWalker::getColumnType(WhereNode* node) {
    EXPECT_OR_THROW(node, InternalError, "Null node pointer in getColumnType.");

    switch (node->type) {
    case nOperator:
        return ColumnType::RIDCOL;
    case nColumnRef:
        return ColumnType(node->value2);
    case nLiteralString:
        return ColumnType::TEXT;
    case nLiteralInt:
        return ColumnType::BIGINT;
    case nLiteralDate:
        return ColumnType::DATE;
    case nLiteralTimestamp:
        return ColumnType::TIMESTAMP;
    case nLiteralFloat:
        return ColumnType::FLOAT;
    case nLiteralNull:
        return ColumnType::NULLTYPE;
    case nCast:
        return get<ColumnType>(node->value);

    default:
        EXPECT_OR_THROW(false, InternalError, "Unexpected node type " << node->type << " in where clause node.");
    }
}

bool QWalker::typeConvertibleTo(WhereNode* src, WhereNode* dest) {
    ColumnType srcType = getColumnType(src);
    ColumnType destType = getColumnType(dest);

    if (srcType == destType)
        return true;

    if ((src->type == nLiteralString) && (src->value2 == LiteralStringType::Parameter)) {
        return true;
    }

    if ((srcType == ColumnType::BIGINT) && (destType == ColumnType::FLOAT))
        return true;

    if ((srcType == ColumnType::DATE) && (destType == ColumnType::TIMESTAMP))
        return true;

#if 0
    if ((srcType == ColumnType::TEXT) && (destType == ColumnType::TAG))
        return true;
#endif

    if ((srcType == ColumnType::TEXT) && (destType == ColumnType::DATE))
        return true;

    if ((srcType == ColumnType::TEXT) && (destType == ColumnType::TIMESTAMP))
        return true;

    if ((srcType == ColumnType::NULLTYPE) || (destType == ColumnType::NULLTYPE))
        return true;

    if ((srcType == ColumnType::RIDCOL) && (destType == ColumnType::RIDCOL))
        return true;

    return false;
}

QWalker::WhereNode* QWalker::createNodeLiteralDATE(StringToken stok) {
    auto opt = actual.serialize_date(default_table_tid, stok);
    EXPECT_OR_THROW(opt.has_value(), MalformedInput, "Cannot convert literal string into DATE.");
    return new WhereNode{nLiteralDate, int64(*opt), 0, {}};
}

QWalker::WhereNode* QWalker::createNodeLiteralTIMESTAMP(StringToken stok) {
    auto opt = actual.serialize_timestamp(default_table_tid, stok);
    EXPECT_OR_THROW(opt.has_value(), MalformedInput, "Cannot convert literal string into TIMESTAMP.");
    return new WhereNode{nLiteralTimestamp, int64(*opt), 0, {}};
}

QWalker::WhereNode* QWalker::upgradeColumn(WhereNode* node, ColumnType newtype) {
    WhereNode* ret;

    switch (node->type) {
    case NodeType::nLiteralString:
        switch (newtype) {
        case ColumnType::DATE: {
            bool isPar = (node->value2 == LiteralStringType::Parameter);
            if (isPar) {
                int64 parIdx = get<int64>(node->value);
                auto stok = actual.getStringToken(default_table_tid, others[parIdx].c_str());
                ret = createNodeLiteralDATE(stok);
            } else {
                // create literal date.... delete original node
                ret = createNodeLiteralDATE(get<StringToken>(node->value));
            }
            delete node;
            return ret;
        }
        case ColumnType::TIMESTAMP: {
            bool isPar = (node->value2 == LiteralStringType::Parameter);
            if (isPar) {
                int64 parIdx = get<int64>(node->value);
                auto stok = actual.getStringToken(default_table_tid, others[parIdx].c_str());
                ret = createNodeLiteralTIMESTAMP(stok);
            } else {
                // create literal timestamp.... delete original node
                ret = createNodeLiteralTIMESTAMP(get<StringToken>(node->value));
            }
            delete node;
            return ret;
        }
        case ColumnType::BIGINT: {
            bool isPar = (node->value2 == LiteralStringType::Parameter);
            assert(isPar);
            EXPECT_OR_THROW(isPar, InternalError, "Unable to convert TEXT to BIGINT.");
            int64 parIdx = get<int64>(node->value);
            ret = createNodeLeaf<nLiteralInt>(std::atol(others[parIdx].c_str()));
            delete node;
            return ret;
        }
        case ColumnType::FLOAT: {
            bool isPar = (node->value2 == LiteralStringType::Parameter);
            assert(isPar);
            EXPECT_OR_THROW(isPar, InternalError, "Unable to convert TEXT to FLOAT.");
            int64 parIdx = get<int64>(node->value);
            double fval = std::atof(others[parIdx].c_str());
            ret = new WhereNode{nLiteralFloat, {fval}, 0};
            delete node;
            return ret;
        }
        default:
            EXPECT_OR_THROW(false, InternalError, "Cannot cast from 'literal string' to " << coltypeToString(newtype));
            break;
        }
        break;

    case NodeType::nLiteralInt:
        switch (newtype) {
        case ColumnType::FLOAT:
            return new WhereNode{nLiteralFloat, double(get<int64>(node->value)), 0, {}};
        default:
            EXPECT_OR_THROW(false, InternalError, "Cannot cast from 'literal int' to " << coltypeToString(newtype));
            break;
        }
        break;

    default: {
        string tmp1 = coltypeToString(get<ColumnType>(node->value));
        string tmp2 = coltypeToString(newtype);
        EXPECT_OR_THROW(false, InternalError, "Cannot cast from " << tmp1 << " to " << tmp2);
    }
    }
}

QWalker::WhereNode* QWalker::handleOpNot(WhereNode* node) {
    if ((node->type == nOperator) && (get<hsql::OperatorType>(node->value) == hsql::kOpEquals) && (node->kids[1]->type == nLiteralNull)) {

        node->value = hsql::kOpNotEquals;
        return node;
    }

    EXPECT_OR_THROW(false, InternalError, "Unexpected operator sequence during parsing.");
}

QWalker::WhereNode* QWalker::handleOpNull(WhereNode* node) {
    EXPECT_OR_THROW(node->type == nColumnRef, InternalError, "Expecting columnref with OpIsNull.");

    WhereNode* litnull = new WhereNode{nLiteralNull};
    return new WhereNode{nOperator, hsql::kOpEquals, 0, {node, litnull}};
}

#define IS_PARAMETER(NODE) ((NODE->type == nLiteralString) && (NODE->value2 == LiteralStringType::Parameter))

void QWalker::resolveStringParameter(WhereNode* node) {
    EXPECT_OR_THROW(IS_PARAMETER(node), InternalError, "Parameter upgrade allowed only for TEXT.");

    auto parIdx = get<int64>(node->value);
    auto stok = actual.getStringToken(default_table_tid, others[parIdx].c_str());
    node->value = stok;
}

QWalker::WhereNode* QWalker::createNode(hsql::OperatorType op, WhereNode* left, WhereNode* right) {
    if (op == hsql::kOpIsNull) {
        return handleOpNull(left);
    }
    if (op == hsql::kOpNot) {
        return handleOpNot(left);
    }

    EXPECT_OR_THROW(left, InternalError, "Null left pointer in getColumnType op = " << op);
    EXPECT_OR_THROW(right, InternalError, "Null right pointer in getColumnType op = " << op);

    ColumnType leftType = getColumnType(left);
    ColumnType rightType = getColumnType(right);
    WhereNode* newRight = right;

    if (leftType != rightType) {
#if 0
        if (typeConvertibleTo(leftType, rightType)) {
            newLeft = upgradeColumn(left, rightType);
            EXPECT_OR_THROW(typeConvertibleTo(getColumnType(newLeft), rightType), InternalError, "Incompatible types: " << coltypeToString(getColumnType(newLeft)) << ", " << coltypeToString(rightType));
        }
        else
#endif
        if (typeConvertibleTo(right, left)) {
            newRight = upgradeColumn(right, leftType);
            EXPECT_OR_THROW(typeConvertibleTo(newRight, left),
                            InternalError,
                            "Incompatible types:  " << coltypeToString(getColumnType(newRight)) << ", " << coltypeToString(leftType));
        } else {
            // TODO: types to string
            EXPECT_OR_THROW(false,
                            ERROR_TYPE_MISMATCH,
                            "Column type mismatch in where clause: " << coltypeToString(leftType) << " cannot be used with " << coltypeToString(rightType));
        }
    } else {
        if (IS_PARAMETER(right)) {
            resolveStringParameter(right);
        }
    }

    return new WhereNode{nOperator, op, 0, {left, newRight}};
}

QualifiedCID QWalker::getColumnOrThrow(cchar* exprtable, cchar* col_name) {
    string colname{col_name};
    string tablename{(exprtable ? exprtable : default_table_name.c_str())};
    Util::to_lower(tablename);
    auto opt_cid = actual.translateColumn(tablename, colname);

    if (opt_cid.has_value()) {
        return *opt_cid;
    } else {
        string _ = string("Not found: Table '") + tablename + "' and column '" + colname + "'.";
        throw MEMURAI_EXCEPTION(ERROR_SQL_COLUMN_NOT_FOUND, _.c_str());
    }
}

QCID QWalker::decodeColumn(hsql::Expr* proj) {
    assert(proj->type == hsql::kExprColumnRef);
    EXPECT_OR_THROW(proj->type == hsql::kExprColumnRef, InternalError, "Expecting only column type in decodeProjectionColumn.");

    cchar* colname = proj->name;
    return getColumnOrThrow(proj->table, colname);
}

void QWalker::acquireSource(cchar* table_name, cchar* table_schema) {
    default_table_name = table_name;
    default_schema = (table_schema ? table_schema : "");

    auto opt = actual.getTableTID(string(default_table_name), string(default_schema));
    EXPECT_OR_THROW(opt.has_value(), ERROR_SQL_TABLE_DOES_NOT_EXIST, "Table '" << table_name << "' not found.");

    default_table_tid = *opt;
}

void QWalker::walkSources(hsql::TableRef* table) {
    EXPECT_OR_THROW(table, ERROR_SQL_TABLE_DOES_NOT_EXIST, "Table not found.");

    switch (table->type) {
    case hsql::kTableName:
        acquireSource(table->name, table->schema);
        break;
    case hsql::kTableSelect:
        EXPECT_OR_THROW(false, Unsupported, "Inner SELECT not currently supported.");
        break;
    case hsql::kTableJoin:
        EXPECT_OR_THROW(false, Unsupported, "JOIN operatons not currently supported.");
        break;
    case hsql::kTableCrossProduct:
        EXPECT_OR_THROW(false, Unsupported, "Cross product not currently supported.");
        break;
    }

    if (table->alias) {
        assert(UNREACHED);
        EXPECT_OR_THROW(false, Unsupported, "Aliases not currently supported.");
    }
}

void QWalker::walkProjections(const std::vector<hsql::Expr*>& projs) {
    for (auto proj : projs) {
        switch (proj->type) {
        case hsql::kExprColumnRef:
            projections.push_back(decodeColumn(proj));
            break;

        case hsql::kExprStar:
            EXPECT_OR_THROW(false, Unsupported, "'*' not currently supported in SELECT projections.");
        case hsql::kExprLiteralFloat:
            EXPECT_OR_THROW(false, Unsupported, "Literals not currently supported in SELECT projections.");
            break;
        case hsql::kExprLiteralInt:
            EXPECT_OR_THROW(false, Unsupported, "Literals not currently supported in SELECT projections.");
            break;
        case hsql::kExprLiteralString:
            EXPECT_OR_THROW(false, Unsupported, "Literals not currently supported in SELECT projections.");
            break;
        case hsql::kExprFunctionRef:
            EXPECT_OR_THROW(false, Unsupported, "Functions not currently supported in SELECT projections.");
            break;
        case hsql::kExprExtract:
            EXPECT_OR_THROW(false, Unsupported, "Extract not currently supported in SELECT projections.");
            break;
        case hsql::kExprCast:
            EXPECT_OR_THROW(false, Unsupported, "Cast not currently supported in SELECT projections.");
            break;
        case hsql::kExprOperator:
            EXPECT_OR_THROW(false, Unsupported, "Operators not currently supported in SELECT projections.");
            break;
        case hsql::kExprSelect:
            EXPECT_OR_THROW(false, Unsupported, "Inner SELECT not currently supported in SELECT projections.");
            break;
        case hsql::kExprParameter:
            EXPECT_OR_THROW(false, Unsupported, "Operators not currently supported in SELECT projections.");
            break;
        case hsql::kExprArray:
            EXPECT_OR_THROW(false, Unsupported, "Arrays not currently supported in SELECT projections.");
            break;
        case hsql::kExprArrayIndex:
            EXPECT_OR_THROW(false, Unsupported, "Arrays not currently supported in SELECT projections.");
            break;
        default:
            EXPECT_OR_THROW(false, Unsupported, "Unknown expression element in SELECT.");
            break;
        }
    }
}

void QWalker::walkWhereClause(hsql::Expr* expr) {
    if (expr == nullptr) {
        // no where filters
        // nothing to do??
        // whereFilter.setNoFilter();
        return;
    }

    // expect operation, create operation node
    EXPECT_OR_THROW(expr->type == hsql::kExprOperator, MalformedInput, "Expected simple filters in WHERE clause.");
    EXPECT_OR_THROW(expr->opType != hsql::kOpBetween, MalformedInput, "BETWEEN not supported in WHERE clause.");

    auto left = walkWhereImpl(expr->expr);
    auto right = walkWhereImpl(expr->expr2);
    whereRootNode = createNode(expr->opType, left, right);
}

bool QWalker::IsValidDBString(cchar* str) {
    while (*str > 0) {
        str++;
    }
    return (*str == 0);
}

QWalker::WhereNode* QWalker::walkWhereImpl(hsql::Expr* expr) {
    if (expr == nullptr)
        return nullptr;

    WhereNode *left, *right;

    // Okay, we got filters
    switch (expr->type) {
    case hsql::kExprOperator:
        if (expr->opType == hsql::kOpNotLike)
            throw MEMURAI_EXCEPTION(Unsupported, "NOT LIKE not currently supported in WHERE clause.");

        if (expr->opType == hsql::kOpUnaryMinus) {
            assert(expr->expr->type == hsql::kExprLiteralFloat || expr->expr->type == hsql::kExprLiteralInt);
            expr->expr->fval = -expr->expr->fval;
            expr->expr->ival = -expr->expr->ival;
            return walkWhereImpl(expr->expr);
        } else {
            left = walkWhereImpl(expr->expr);
            right = walkWhereImpl(expr->expr2);

            return createNode(expr->opType, left, right);
        }
    case hsql::kExprColumnRef:
        return createNodeLeafColumn(getColumnOrThrow(expr->table, expr->name));
    case hsql::kExprLiteralFloat:
        return createNodeLeaf<nLiteralFloat>(reinterpret<uint64>(expr->fval));
    case hsql::kExprLiteralInt:
        return createNodeLeaf<nLiteralInt>(expr->ival);
    case hsql::kExprLiteralString:
        return createNodeLeafStringToken(actual.getStringToken(default_table_tid, expr->name));

        // UNSUPPORTED cases here
    case hsql::kExprStar:
        throw MEMURAI_EXCEPTION(MalformedInput, "'*' cannot be in WHERE clause.");
    case hsql::kExprFunctionRef:
        throw MEMURAI_EXCEPTION(Unsupported, "Functions not currently supported in WHERE clause.");
        break;
    case hsql::kExprExtract:
        throw MEMURAI_EXCEPTION(Unsupported, "Extract not currently supported in WHERE clause.");
        break;
    case hsql::kExprCast:
        throw MEMURAI_EXCEPTION(Unsupported, "Cast not currently supported in WHERE clause.");
        break;
    case hsql::kExprSelect:
        throw MEMURAI_EXCEPTION(Unsupported, "SELECT not currently supported in WHERE clause.");
        break;
    case hsql::kExprParameter:
        return createStringParameterNode();
    case hsql::kExprArray:
        throw MEMURAI_EXCEPTION(Unsupported, "Arrays not currently supported in WHERE clause.");
        break;
    case hsql::kExprArrayIndex:
        throw MEMURAI_EXCEPTION(Unsupported, "Arrays not currently supported in WHERE clause.");
        break;
    default:
        throw MEMURAI_EXCEPTION(MalformedInput, "Expression not recognized in WHERE clause.");
    }

    throw MEMURAI_EXCEPTION(InternalError, "You should never see this.");
}

void QWalker::walkGroupBy(hsql::GroupByDescription* groupBy) {
    if (groupBy == nullptr)
        return;

    EXPECT_OR_THROW(false, Unsupported, "GROUP BY not currently supported.");
}

void QWalker::walkOrderBy(const std::vector<hsql::OrderDescription*>* order) {
    if (order == nullptr)
        return;

    for (const auto& ord : *order) {
        const auto& desc = *ord;
        QCID qcid = decodeColumn(desc.expr);
        ASCDESC ascdesc = (desc.type == hsql::kOrderAsc) ? ASCDESC::ASC : ASCDESC::DESC;
        sort_columns.emplace_back(qcid, ascdesc);
    }
}

void QWalker::walkSelectStatement(const hsql::SelectStatement* stmt) {
    if (stmt->hints != nullptr)
        throw MEMURAI_EXCEPTION(Unsupported, "HINTS not currently supported.");

    walkSources(stmt->fromTable);

    walkProjections(*stmt->selectList);

    walkWhereClause(stmt->whereClause);

    walkGroupBy(stmt->groupBy);

    walkOrderBy(stmt->order);

    if (stmt->limit != nullptr && stmt->limit->limit != nullptr) {
        if (stmt->limit->limit->ival < 0) {
            return_row_limit = numeric_limits<uint64>::max();
        } else {
            return_row_limit = stmt->limit->limit->ival;
        }
    } else {
        return_row_limit = numeric_limits<uint64>::max();
    }

    if (stmt->limit != nullptr && stmt->limit->offset != nullptr) {
        if (stmt->limit->offset->ival < 0) {
            return_row_offset = 0;
        } else {
            return_row_offset = stmt->limit->offset->ival;
        }
    }
}

void QWalker::walkUpdateStatement(const hsql::UpdateStatement*) {
    throw MEMURAI_EXCEPTION(Unsupported, "UPDATE statements not currently supported.");
}
void QWalker::walkInsertStatement(const hsql::InsertStatement*) {
    throw MEMURAI_EXCEPTION(Unsupported, "INSERT statements not currently supported.");
}
void QWalker::walkCreateStatement(const hsql::CreateStatement*) {
    throw MEMURAI_EXCEPTION(Unsupported, "CREATE statements not currently supported.");
}
void QWalker::walkImportStatement(const hsql::ImportStatement*) {
    throw MEMURAI_EXCEPTION(Unsupported, "Import statements not currently supported.");
}
void QWalker::walkExportStatement(const hsql::ExportStatement*) {
    throw MEMURAI_EXCEPTION(Unsupported, "Export statements not currently supported.");
}
void QWalker::walkTransactionStatement(const hsql::TransactionStatement*) {
    throw MEMURAI_EXCEPTION(Unsupported, "Transaction statements not currently supported.");
}
void QWalker::walkDeleteStatement(const hsql::DeleteStatement*) {
    throw MEMURAI_EXCEPTION(Unsupported, "DELETE statements not currently supported.");
}
void QWalker::walkDropStatement(const hsql::DropStatement*) {
    throw MEMURAI_EXCEPTION(Unsupported, "DROP statements not currently supported.");
}
void QWalker::walkPrepareStatement(const hsql::PrepareStatement*) {
    throw MEMURAI_EXCEPTION(Unsupported, "PREPARE statements not currently supported.");
}
void QWalker::walkExecuteStatement(const hsql::ExecuteStatement*) {
    throw MEMURAI_EXCEPTION(Unsupported, "EXECUTE statements not currently supported.");
}

QWalker* QWalker::walkSQLQueryTree(cchar* query, IActual& actual, const vecstring& others) {
    if (query != nullptr) {
        hsql::SQLParserResult result;
        hsql::SQLParser::parse(query, &result);

        self_type* pwalker = new self_type(actual, others);
        self_type& walker = *pwalker;

        if (result.isValid()) {
            if (result.size() > 0) {
                const hsql::SQLStatement* stmt = result.getStatement(0);
                switch (stmt->type()) {
                case hsql::kStmtSelect:
                    walker.walkSelectStatement((const hsql::SelectStatement*)stmt);
                    break;
                case hsql::kStmtInsert:
                    walker.walkInsertStatement((const hsql::InsertStatement*)stmt);
                    break;
                case hsql::kStmtCreate:
                    walker.walkCreateStatement((const hsql::CreateStatement*)stmt);
                    break;
                case hsql::kStmtUpdate:
                    walker.walkUpdateStatement((const hsql::UpdateStatement*)stmt);
                    break;
                case hsql::kStmtImport:
                    walker.walkImportStatement((const hsql::ImportStatement*)stmt);
                    break;
                case hsql::kStmtExport:
                    walker.walkExportStatement((const hsql::ExportStatement*)stmt);
                    break;
                case hsql::kStmtTransaction:
                    walker.walkTransactionStatement((const hsql::TransactionStatement*)stmt);
                    break;
                case hsql::kStmtDelete:
                    walker.walkDeleteStatement((const hsql::DeleteStatement*)stmt);
                    break;
                case hsql::kStmtDrop:
                    walker.walkDropStatement((const hsql::DropStatement*)stmt);
                    break;
                case hsql::kStmtPrepare:
                    walker.walkPrepareStatement((const hsql::PrepareStatement*)stmt);
                    break;
                case hsql::kStmtExecute:
                    walker.walkExecuteStatement((const hsql::ExecuteStatement*)stmt);
                    break;
                default:
                    throw MEMURAI_EXCEPTION(MalformedInput, "Unknown query.");
                    break;
                }
            }

            return pwalker;
        } else {
        } // fallthrough
    }

    throw MEMURAI_EXCEPTION(MalformedInput, "Invalid query.");
}

} // namespace memurai::sql
