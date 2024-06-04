/*
* Copyright (c) 2020-2021, Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <functional>

#include "FE_ClientReply.h"

namespace memurai {
class TSCInterface {
public:
    virtual void lock() = 0;
    virtual void unlock() = 0;
    virtual void finish() = 0;

    virtual void reply_to_client(const FE_ClientReply& reply) = 0;
};

} // namespace memurai
