/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <cstdlib>
#include <functional>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "cache.h"
#include "column.h"
#include "exception.h"
#include "scope/expect.h"
#include "tidcid.h"
#include "types.h"

namespace memurai::sql {

using std::optional;
using std::string;
using std::tuple;
using std::vector;

#define SERIALIZE(PTR, TYPE, VALUE) \
    *((TYPE*)PTR) = VALUE;          \
    PTR += sizeof(TYPE);

// Takes enough IdxTable info to be able to repopulate it from Redis (not from file for now)
// after a restart
//
class PersistedTableMetadataCore {
protected:
    constexpr static uint64 VERSION = 0x0000100ull;
    constexpr static uint64 CHECK1_UINT64 = 0x01FF03FD05FB00FFull;
    constexpr static uint64 CHECK2_UINT64 = 0xBADBADBADBADBADull;

    uint64 tid{};
    uint64 version{};
    uint64 num_columns{};

    vector<uint64> config;

    constexpr static uint64 MAX_TABLE_SIZE = 256;
    constexpr static uint64 COLUMNS_NAMES_BUFFER_SIZE = 4096;
    constexpr static uint64 COLUMNS_TYPES_BUFFER_SIZE = 1024;
    constexpr static uint64 COLUMNS_TYPES_ENTRIES = 1024 / sizeof(ColumnType);
    constexpr static uint64 COLUMNS_CIDS_ENTRIES = 1024 / sizeof(CID);

    char table_name[MAX_TABLE_SIZE] = {0};
    char prefix_ser[MAX_TABLE_SIZE] = {0};
    char schema[MAX_TABLE_SIZE] = {0};
    char colNamesBuffer[COLUMNS_NAMES_BUFFER_SIZE] = {0};
    ColumnType colTypesBuffer[COLUMNS_TYPES_ENTRIES]{};
    CID colCIDsBuffer[COLUMNS_CIDS_ENTRIES]{};
};

// Takes enough IdxTable info to be able to repopulate it from Redis (not from file for now)
// after a restart
//
class PersistedTableMetadata_Write : private PersistedTableMetadataCore {
    void serializeNames(const vector<string>& names) {
        char* ptr = colNamesBuffer;
        uint64 cumulative_len = 0;

        for (const auto& l : names) {
            auto label_ptr = l.c_str();
            auto len = strlen(label_ptr) + 1;
            memcpy(ptr, label_ptr, len);
            cumulative_len += len;
            ptr += len;
            if (cumulative_len >= COLUMNS_NAMES_BUFFER_SIZE - 2) {
                throw MEMURAI_EXCEPTION(InternalError, "Header buffer too small for column names or types.");
            }
        }
        *ptr++ = 0;
    }

    void serializeCIDs(const vector<CID>& cids) {
        if (cids.size() > COLUMNS_CIDS_ENTRIES) {
            throw MEMURAI_EXCEPTION(InternalError, "Header buffer too small for CID entries.");
        }

        int idx = 0;
        for (auto cid : cids) {
            colCIDsBuffer[idx++] = cid;
        }
    }

    void serializeTypes(const vector<ColumnType>& types) {
        if (types.size() > COLUMNS_TYPES_ENTRIES) {
            throw MEMURAI_EXCEPTION(InternalError, "Header buffer too small for ColumnType entries.");
        }

        int idx = 0;
        for (auto cid : types) {
            colTypesBuffer[idx++] = cid;
        }
    }

public:
    PersistedTableMetadata_Write(
        TID tid, const string& tname, const string& prefix, const vector<FullyQualifiedCID>& fqcids, const string& schema_, std::vector<uint64>& config) {
        this->tid = tid;
        this->version = VERSION;
        this->num_columns = fqcids.size();

        EXPECT_OR_THROW(tname.size() <= MAX_TABLE_SIZE, TableNameTooLong, "Table name should be less than " << MAX_TABLE_SIZE);
        EXPECT_OR_THROW(prefix.size() <= MAX_TABLE_SIZE, PrefixTooLong, "Prefix name should be less than " << MAX_TABLE_SIZE);

        memcpy(table_name, tname.c_str(), tname.size());
        memcpy(prefix_ser, prefix.c_str(), prefix.size());
        memcpy(schema, schema_.c_str(), schema_.size());
        serializeCIDs(Column::projectCIDs(fqcids));
        serializeTypes(Column::projectTypes(fqcids));
        serializeNames(Column::projectColNames(fqcids));
        this->config = config;
    }

    uint64 serialize_to(void* dest, uint64 max_size) const {
        byte* ptr = (byte*)dest;

        SERIALIZE(ptr, uint64, tid);
        SERIALIZE(ptr, uint64, version);
        SERIALIZE(ptr, uint64, num_columns);

        cuint64 expected_size = 3 * sizeof(uint64) + MAX_TABLE_SIZE * 3 + sizeof(uint64) + config.size() * sizeof(uint64) +
                                COLUMNS_TYPES_ENTRIES * sizeof(ColumnType) + COLUMNS_TYPES_ENTRIES * sizeof(ColumnType) + COLUMNS_CIDS_ENTRIES * sizeof(CID);

        EXPECT_OR_THROW(expected_size < max_size, InternalError, "Buffer too small when serializing table metadata for table " << table_name);

        memcpy(ptr, table_name, MAX_TABLE_SIZE);
        ptr += MAX_TABLE_SIZE;
        memcpy(ptr, prefix_ser, MAX_TABLE_SIZE);
        ptr += MAX_TABLE_SIZE;
        memcpy(ptr, schema, MAX_TABLE_SIZE);
        ptr += MAX_TABLE_SIZE;
        SERIALIZE(ptr, uint64, config.size());
        SERIALIZE(ptr, uint64, CHECK1_UINT64);
        memcpy(ptr, config.data(), config.size() * sizeof(uint64));
        ptr += config.size() * sizeof(uint64);
        SERIALIZE(ptr, uint64, CHECK2_UINT64);

        memcpy(ptr, colNamesBuffer, MAX_TABLE_SIZE);
        ptr += MAX_TABLE_SIZE;
        memcpy(ptr, colTypesBuffer, COLUMNS_TYPES_ENTRIES * sizeof(ColumnType));
        ptr += COLUMNS_TYPES_ENTRIES * sizeof(ColumnType);
        memcpy(ptr, colCIDsBuffer, COLUMNS_CIDS_ENTRIES * sizeof(CID));
        ptr += COLUMNS_CIDS_ENTRIES * sizeof(CID);

        return (ptr - (byte*)dest);
    }
};

class PersistedTableMetadata_Read : private PersistedTableMetadataCore {
    enum STATUS { GOOD, BAD_CHECK1, BAD_CHECK2, BAD_SIZE };

    // TODO: Fix dangerous unaligned memory reads

    STATUS status;

public:
    PersistedTableMetadata_Read(void* file, uint64 xsize) : status(GOOD) {
        const byte* ptr = (const byte*)file;
        tid = *(uint64*)ptr;
        ptr += sizeof(uint64);
        version = *(uint64*)ptr;
        ptr += sizeof(uint64);
        num_columns = *(uint64*)ptr;
        ptr += sizeof(uint64);

        memcpy(table_name, ptr, MAX_TABLE_SIZE);
        ptr += MAX_TABLE_SIZE;
        memcpy(prefix_ser, ptr, MAX_TABLE_SIZE);
        ptr += MAX_TABLE_SIZE;
        memcpy(schema, ptr, MAX_TABLE_SIZE);
        ptr += MAX_TABLE_SIZE;
        auto config_size = *(uint64*)ptr;
        ptr += sizeof(uint64);

        uint64 check1 = *(uint64*)ptr;
        ptr += sizeof(uint64);
        if (check1 != CHECK1_UINT64) {
            status = STATUS::BAD_CHECK1;
            return;
        }

        cuint64 expected_size = 3 * sizeof(uint64) + MAX_TABLE_SIZE * 3 + sizeof(uint64) * 2 + config_size * sizeof(uint64) + sizeof(uint64) + MAX_TABLE_SIZE +
                                COLUMNS_TYPES_ENTRIES * sizeof(ColumnType) + COLUMNS_CIDS_ENTRIES * sizeof(CID);
        if (xsize != expected_size) {
            status = STATUS::BAD_SIZE;
            return;
        }

        config.resize(config_size);
        memcpy(config.data(), ptr, config_size * sizeof(uint64));
        ptr += config_size * sizeof(uint64);
        uint64 check2 = *(uint64*)ptr;
        if (check2 != CHECK2_UINT64) {
            status = STATUS::BAD_CHECK2;
            return;
        }
        ptr += sizeof(uint64);
        memcpy(colNamesBuffer, ptr, MAX_TABLE_SIZE);
        ptr += MAX_TABLE_SIZE;
        memcpy(colTypesBuffer, ptr, COLUMNS_TYPES_ENTRIES * sizeof(ColumnType));
        ptr += COLUMNS_TYPES_ENTRIES * sizeof(ColumnType);
        memcpy(colCIDsBuffer, ptr, COLUMNS_CIDS_ENTRIES * sizeof(CID));
        ptr += COLUMNS_CIDS_ENTRIES * sizeof(CID);
    }

    [[nodiscard]] bool isValid() const {
        return status == STATUS::GOOD;
    }

    [[nodiscard]] vector<string> getColumnNames() const {
        vector<string> ret;

        const char* ptr = colNamesBuffer;

        while (*ptr != 0) {
            ret.emplace_back(ptr);
            ptr += strlen(ptr) + 1;
        }

        return std::move(ret);
    }

    [[nodiscard]] vector<ColumnType> getColumnTypes() const {
        vector<ColumnType> ret;
        for (uint64 idx = 0; idx < num_columns; idx++) {
            ret.push_back(colTypesBuffer[idx]);
        }

        return std::move(ret);
    }

    [[nodiscard]] vector<CID> getCIDs() const {
        vector<CID> ret;
        for (uint64 idx = 0; idx < num_columns; idx++) {
            ret.push_back(colCIDsBuffer[idx]);
        }

        return std::move(ret);
    }

    [[nodiscard]] uint64 getTID() const {
        return tid;
    }

    [[nodiscard]] uint64 getVersion() const {
        return version;
    }

    [[nodiscard]] uint64 getNumColumns() const {
        return num_columns;
    }

    [[nodiscard]] string getTableName() const {
        return {table_name};
    }

    [[nodiscard]] string getPrefix() const {
        return {prefix_ser};
    }

    [[nodiscard]] string getSchema() const {
        return {schema};
    }

    [[nodiscard]] vector<uint64> getConfig() const {
        return config;
    }
};

} // namespace memurai::sql
