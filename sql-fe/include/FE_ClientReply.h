/*
* Copyright (c) 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <functional>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "PublisherV2DStrings.h"
#include "types.h"

namespace memurai {

using std::map;
using std::optional;
using std::string;
using std::variant;
using std::vector;

// We can return to the client
// - a number               (int)
// - a list of messages     (vector<string>)
// - a query result         (PublisherV2DStrings)
//
// We can return either sync'ly o async'ly
//

struct Reply {
    int retcode;
    vector<string> msgs;
};

typedef variant<Reply, std::shared_ptr<sql::PublisherV2DStrings>> FE_ClientReply;

} // namespace memurai