/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#include <atomic>
#include <mutex>

#include "FE_ClientReply.h"
#include "tsc_interface.h"
#include "types.h"

#pragma once

namespace memurai {

class TSC : public memurai::TSCInterface {
    ValkeyModuleBlockedClient* bc;
    std::mutex m_done;
    std::atomic<bool> s_done;

protected:
    void lock() final {
        auto ctx = ValkeyModule_GetThreadSafeContext(bc);
        ValkeyModule_ThreadSafeContextLock(ctx);
    }
    void unlock() final {
        auto ctx = ValkeyModule_GetThreadSafeContext(bc);
        ValkeyModule_ThreadSafeContextUnlock(ctx);
    }

    struct ReplyInternal {
        int ret_code;
        std::string* msg;
    };

public:
    explicit TSC(ValkeyModuleBlockedClient* bc) : bc(bc), s_done(false) {}

    ~TSC() {
        finish();
    }

    // ???
    void reply_to_client(const FE_ClientReply& reply) final {
        void* temp = ValkeyModule_Alloc(sizeof(FE_ClientReply*));
        FE_ClientReply* new_obj = new FE_ClientReply(reply);
        *(FE_ClientReply**)temp = new_obj;
        ValkeyModule_UnblockClient(bc, temp);
    }

    void finish() final {
        std::lock_guard<std::mutex> guard(m_done);

        if (!s_done.load(std::memory_order_acquire)) {
            s_done.store(true, std::memory_order_release);
            auto ctx = ValkeyModule_GetThreadSafeContext(bc);
            ValkeyModule_FreeThreadSafeContext(ctx);
            ValkeyModule_UnblockClient(bc, nullptr);
        }
    }
};

} // namespace memurai
