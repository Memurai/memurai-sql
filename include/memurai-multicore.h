/*
 * Copyright (c) 2020, Janea Systems
   by Benedetto Proietti
 */

#pragma once

#include <atomic>
#include <functional>
#include <string>
#include <vector>

#include "module_interface.h"
#include "sync/syncqueue.h"
#include "threads/cancellation.h"
#include "types.h"

namespace memurai::multicore {
using std::atomic;
using std::function;
using std::pair;
using std::string;
using std::vector;

typedef vector<string> ARGS;

class Server;
struct IServer;

/*
    ThreadPoolInterface

    A class to give an interface to compute engines.
    An instance of this class is setup by some entity (ex: TimeSeries) and is given to some compute provider (ex: bvecN).
    The compute provider has 2 public methods:
     - submit: to submit work items.
     - join:  to wait for the work items to be done.

    This object does:
     - forwards computation to the server.
     - Owns the cancellation token for the various work items.
     - waits on the join.

    We don't actually know how big the thread pool is, the server takes care of that.
    This makes me thin that maybe the name of this class is wrong. :)
*/

/*
    struct ServerInterface

    To be given to server clients instead of real pointers to the server.

*/
struct IServer {
    typedef function<void(vector<Lambda>, IToken*)> asyncrun_group;
    typedef function<void(Lambda)> asyncrun_type;
    typedef function<void(Lambda, IToken*)> asyncrun_type_token;
    typedef function<void(Lambda, uint64 ms, JOBID)> scheduler_type;
    typedef function<void(JOBID)> stop_command_type;
    typedef function<void(syncqueue<Lambda>, atomic<bool>)> streamExec_type;
    typedef function<void(string)> send_to_redis_type;

    asyncrun_group schedule_group;
    asyncrun_type_token schedule_once_with_token;
    asyncrun_type schedule_once;
    scheduler_type schedule_recurring;
    stop_command_type stop_command;
    send_to_redis_type send_to_redis;

    struct Config {
        uint32 compute_pool_size;
    };

    ModuleInterface module_interface;

    static IServer getInterface(Config config, const ModuleInterface&);
    static void startServer();

private:
    static Server* server_;
};

} // namespace memurai::multicore
