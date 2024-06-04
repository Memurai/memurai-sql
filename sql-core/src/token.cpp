/*
 * Copyright (c) 2020-2021, Janea Systems
   by Benedetto Proietti
 */

#include "threads/cancellation.h"

namespace memurai::multicore {

std::string to_string(memurai::multicore::TokenState s) {
    typedef memurai::multicore::TokenState State;

    switch (s) {
    case State::CREATED:
        return "CREATED";
    case State::CANCELED:
        return "CANCELED";
    case State::DONE_NOT_WELL:
        return "DONE_NOT_WELL";
    case State::DONE_WELL:
        return "DONE_WELL";
    default:
        return "n/a";
    }
}

} // namespace memurai::multicore
