/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#include <cstring>
#include <variant>

#define VALKEYMODULE_EXPERIMENTAL_API
#include "valkey/valkeymodule.h"

#include "TSC.h"
#include "interface.h"
#include "module_interface.h"
#include "scope/scope_exit.h"

using memurai::FE_ClientReply;
using memurai::TSC;
using memurai::sql::ColumnType;
using memurai::sql::PublisherV2DStrings;
using std::function;
using std::optional;
using std::variant;
using std::vector;

#define GET_ARGS                                           \
    std::vector<std::string> args(argc);                   \
    for (int i = 0; i < argc; i++) {                       \
        size_t len;                                        \
        args[i] = ValkeyModule_StringPtrLen(argv[i], &len); \
    }

vector<optional<variant<long long, string>>> redis_module_hmget(ValkeyModuleCtx* ctx, const char* key_name, const vecstring& fields) {
    ValkeyModuleString* key = ValkeyModule_CreateString(ctx, key_name, strlen(key_name));

    std::vector<ValkeyModuleString*> redis_strings;
    redis_strings.push_back(key);

    for (const auto& field : fields) {
        auto str = field.c_str();
        ValkeyModuleString* temp = ValkeyModule_CreateString(ctx, str, strlen(str));
        redis_strings.push_back(temp);
    }

    ValkeyModuleCallReply* reply = ValkeyModule_Call(ctx, "hmget", "v", redis_strings.data(), redis_strings.size());

    auto size = ValkeyModule_CallReplyLength(reply);

    vector<optional<variant<long long, string>>> ret;

    for (uint64 idx = 0; idx < size; idx++) {
        ValkeyModuleCallReply* r = ValkeyModule_CallReplyArrayElement(reply, idx);

        if (r == nullptr) {
            ret.emplace_back(std::nullopt);
        } else {
            auto replytype = ValkeyModule_CallReplyType(r);

            switch (replytype) {
            case VALKEYMODULE_REPLY_STRING: {
                size_t len;
                const char* ptr = ValkeyModule_CallReplyStringPtr(r, &len);
                if (ptr != nullptr) {
                    ret.emplace_back(std::string(ptr, len));
                } else {
                    ret.emplace_back(std::nullopt);
                }
                break;
            }
            case VALKEYMODULE_REPLY_INTEGER: {
                long long myval = ValkeyModule_CallReplyInteger(r);
                ret.emplace_back(myval);
                break;
            }
            case VALKEYMODULE_REPLY_NULL:
                ret.emplace_back(std::nullopt);
                break;

            default:
                assert(UNREACHED);
                ret.emplace_back(std::nullopt);
                break;
            }
        }
    }

    for (auto s : redis_strings) {
        ValkeyModule_FreeString(ctx, s);
    }

    return std::move(ret);
}

extern "C" {

// Reply callback for blocking command MSQL.QUERY (and others?)
//
int replyCallback(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    FE_ClientReply** preply = (FE_ClientReply**)ValkeyModule_GetBlockedClientPrivateData(ctx);
    if (preply == nullptr) {
        ValkeyModule_ReplyWithError(ctx, "ERR: Internal error.");
        return VALKEYMODULE_ERR;
    }

    FE_ClientReply* reply = *preply;
    if (reply == nullptr) {
        ValkeyModule_ReplyWithError(ctx, "ERR: Internal error.");
        return VALKEYMODULE_ERR;
    }

    SCOPE_EXIT {
        delete reply;
    };

    if (std::holds_alternative<memurai::Reply>(*reply)) {
        // Response WITHOUT data
        //

        auto reply2 = std::get<memurai::Reply>(*reply);
        const auto& msgs = reply2.msgs;

        const bool isError = (reply2.retcode != 0);

        if (isError) {
            if (!msgs.empty()) {
                ValkeyModule_ReplyWithError(ctx, msgs[0].c_str());
                return VALKEYMODULE_ERR;
            } else {
                ValkeyModule_ReplyWithError(ctx, "ERR: Internal error.");
                return VALKEYMODULE_ERR;
            }
        } else {
            ValkeyModule_ReplyWithArray(ctx, msgs.size());
            for (auto& msg : msgs) {
                ValkeyModule_ReplyWithCString(ctx, msg.c_str());
            }
        }

        return VALKEYMODULE_OK;
    }

    if (std::holds_alternative<std::shared_ptr<PublisherV2DStrings>>(*reply)) {
        // Response WITH data...
        auto publisher = std::get<std::shared_ptr<PublisherV2DStrings>>(*reply);
        PublisherV2DStrings* ptr = publisher.get();

        if (ptr == nullptr) {
            ValkeyModule_ReplyWithError(ctx, "ERR: Internal error.");
            return VALKEYMODULE_ERR;
        }

        auto data = ptr->get_data();

        if (data.empty()) {
            ValkeyModule_ReplyWithArray(ctx, 0);
            return VALKEYMODULE_OK;
        }

        const auto& header = ptr->get_header();

        assert(header.size() == data[0].size());
        if (header.size() != data[0].size()) {
            ValkeyModule_ReplyWithError(ctx, "ERR: Internal error.");
            return VALKEYMODULE_ERR;
        }

        cuint64 num_rows = data.size();
        cuint64 num_cols = data[0].size();
        cuint64 num_total = num_rows * (num_cols + 1);

        ValkeyModule_ReplyWithArray(ctx, num_rows + 1);

        // First row... column names
        ValkeyModule_ReplyWithArray(ctx, num_cols);
        for (uint64 cidx = 0; cidx < num_cols; cidx++) {
            ValkeyModule_ReplyWithCString(ctx, header[cidx].c_str());
        }

        // then, data rows.
        for (uint64 row_idx = 0; row_idx < data.size(); row_idx++) {
            ValkeyModule_ReplyWithArray(ctx, num_cols);
            for (uint64 cidx = 0; cidx < num_cols; cidx++) {
                auto elem = data[row_idx][cidx];
                if (elem.has_value()) {
                    ValkeyModule_ReplyWithCString(ctx, elem->c_str());
                } else {
                    ValkeyModule_ReplyWithNull(ctx);
                }
            }
        }

        return VALKEYMODULE_OK;
    }

    // what is thisRedisModule_ReplyWithError
    ValkeyModule_ReplyWithSimpleString(ctx, "ERR: Internal error.");
    return VALKEYMODULE_ERR;
}

/* Timeout callback for blocking command HELLO.BLOCK */
int timeoutCallback(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);
    return ValkeyModule_ReplyWithSimpleString(ctx, "ERR: Request timedout");
}

/* Private data freeing callback for HELLO.BLOCK command. */
void freeDataCallback(ValkeyModuleCtx* ctx, void* privdata) {
    VALKEYMODULE_NOT_USED(ctx);
    ValkeyModule_Free(privdata);
}

void disconnectedCallback(ValkeyModuleCtx* ctx, ValkeyModuleBlockedClient* bc) {
    ValkeyModule_Log(ctx, "warning", "Blocked client %p disconnected!", (void*)bc);

    /* Here you should cleanup your state / threads, and if possible
     * call ValkeyModule_UnblockClient(), or notify the thread that will
     * call the function ASAP. */
}

void redis_module_keys(ValkeyModuleCtx* ctx, const char* prefix, std::function<void(const char*, int)> consume) {
    ValkeyModuleCallReply* reply = ValkeyModule_Call(ctx, "keys", "c", prefix);

    auto size = ValkeyModule_CallReplyLength(reply);

    for (uint64 idx = 0; idx < size; idx++) {
        ValkeyModuleCallReply* r = ValkeyModule_CallReplyArrayElement(reply, idx);

        if (r != nullptr) {
            auto replytype = ValkeyModule_CallReplyType(r);

            switch (replytype) {
            case VALKEYMODULE_REPLY_STRING: {
                size_t len;
                const char* ptr = ValkeyModule_CallReplyStringPtr(r, &len);
                if (ptr != nullptr) {
                    int i = 0;
                    while ((ptr[i] != 0) && (ptr[i] != '\r')) {
                        i++;
                    }

                    consume(ptr, i);
                } else {
                    // ???
                }
                break;
            }

            default:
                assert(UNREACHED);
                break;
            }
        } else {
        }
    }
}

void test_redis_module_hget(ValkeyModuleCtx* ctx, const char* key_name_str, const char* field_str) {
    ValkeyModuleString* key_name = ValkeyModule_CreateString(ctx, key_name_str, strlen(key_name_str));

    ValkeyModuleKey* key_obj = (ValkeyModuleKey*)ValkeyModule_OpenKey(ctx, key_name, VALKEYMODULE_READ);
    if (key_obj != nullptr) {
        ValkeyModuleString* oldval;
        int ret = ValkeyModule_HashGet(key_obj, VALKEYMODULE_HASH_CFIELDS, field_str, &oldval, NULL);

        if (ret == VALKEYMODULE_OK) {
            size_t len;
            auto value = ValkeyModule_StringPtrLen(oldval, &len);
            // ValkeyModule_Log(ctx, "notice", "DEBUG!!   hash %s has field %s of value '%s'.", key_name_str, field_str, value);

            // This booms
            // ValkeyModule_FreeString(ctx, oldval);
        }

        ValkeyModule_CloseKey(key_obj);
    }
    ValkeyModule_FreeString(ctx, key_name);
}

/* Any MemuraiSQL ASYNC command
 */
int MemuraiSQL_Command_Async(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc) {
    GET_ARGS;
    PORT_LONGLONG timeout = 1000000;

    ValkeyModuleBlockedClient* bc = ValkeyModule_BlockClient(ctx, replyCallback, timeoutCallback, freeDataCallback, timeout);

    ValkeyModule_SetDisconnectCallback(bc, disconnectedCallback);

    ValkeyModule_Log(ctx, "notice", "Async command");

    auto ret = memurai::multicore::ts_client->enqueueCommand(args, new TSC(bc));

    return VALKEYMODULE_OK;
}

/* Any MemuraiSQL SYNC command
 */
int MemuraiSQL_Command_Sync(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc) {
    GET_ARGS;

    ValkeyModule_Log(ctx, "notice", "Sync command");

    // Actually issues the command
    auto reply = memurai::multicore::ts_client->syncCommand(args, (OuterCtx)ctx);

    // Process reply send it to client
    //
    if (std::holds_alternative<memurai::Reply>(reply)) {
        auto reply2 = std::get<memurai::Reply>(reply);
        const auto& msgs = reply2.msgs;

        if (msgs.empty()) {
            ValkeyModule_ReplyWithLongLong(ctx, std::get<memurai::Reply>(reply).retcode);
            return VALKEYMODULE_OK;
        }

        ValkeyModule_ReplyWithArray(ctx, msgs.size());
        for (auto& msg : msgs) {
            ValkeyModule_ReplyWithCString(ctx, msg.c_str());
        }

        return VALKEYMODULE_OK;
    }

    ValkeyModule_ReplyWithSimpleString(ctx, "Internal errors.");
    return VALKEYMODULE_ERR;
}

int keyspace_callback(ValkeyModuleCtx* ctx, int type, const char* event, ValkeyModuleString* key) {
    size_t len;
    std::string key_str(ValkeyModule_StringPtrLen(key, &len));
    bool ret = memurai::multicore::ts_client->keyspace_notification((OuterCtx)(ctx), key_str, type, event);
    return (ret ? VALKEYMODULE_OK : VALKEYMODULE_ERR);
}

void server_event_callback(ValkeyModuleCtx* ctx, ValkeyModuleEvent eid, uint64_t subevent, void* data) {
    if (eid.id == VALKEYMODULE_EVENT_FLUSHDB) {
        memurai::multicore::ts_client->server_notification((OuterCtx)(ctx), DatabaseServerEvent::FLUSHDB);
    }
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int ValkeyModule_OnLoad(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc) {
    if (argc > 1) {
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_Init(ctx, "memurai-sql", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (!ValkeyModule_GetServerInfo) // API only present on version 6 and up
    {
        ValkeyModule_Log(ctx, "warning", "Requires version 6 or higher");
        return VALKEYMODULE_ERR;
    }

    GET_ARGS;

    auto log_me = [ctx](const char* level, const char* msg) {
        ValkeyModule_Log(ctx, level, "%s", msg);
    };

    auto hget_me = [](void* ctx, const char* key, const vecstring& fields) -> vector<optional<variant<long long, string>>> {
        return redis_module_hmget((ValkeyModuleCtx*)ctx, key, fields);
    };

    auto keys_me = [](void* ctx, const char* prefix, std::function<void(const char*, int)> consume) {
        redis_module_keys((ValkeyModuleCtx*)ctx, prefix, consume);
    };

    bool ret = memurai::multicore::initialize_module(args, ModuleInterface{log_me, hget_me, keys_me});
    if (!ret) {
        return VALKEYMODULE_ERR;
    }

    // Subscribe to ALL hash keyspace events
    //
    if (ValkeyModule_SubscribeToKeyspaceEvents(ctx, VALKEYMODULE_NOTIFY_GENERIC | VALKEYMODULE_NOTIFY_HASH, keyspace_callback) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_SubscribeToServerEvent(ctx, ValkeyModuleEvent_FlushDB, server_event_callback) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "msql.size", MemuraiSQL_Command_Sync, "readonly no-cluster deny-oom", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "msql.create", MemuraiSQL_Command_Sync, "readonly no-cluster deny-oom", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "msql.destroy", MemuraiSQL_Command_Async, "readonly no-cluster deny-oom", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "msql.query", MemuraiSQL_Command_Async, "readonly no-cluster deny-oom", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "msql.describe", MemuraiSQL_Command_Sync, "readonly no-cluster deny-oom", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "msql.show", MemuraiSQL_Command_Sync, "readonly no-cluster deny-oom", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "msql.debug.describe", MemuraiSQL_Command_Sync, "readonly no-cluster deny-oom", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    FE_ClientReply reply = memurai::multicore::ts_client->syncCommand({"msql.restore"}, ctx);
    if ((get<0>(reply)).retcode != 0) {
        return VALKEYMODULE_ERR;
    }

    return VALKEYMODULE_OK;
}

} // extern "C"
