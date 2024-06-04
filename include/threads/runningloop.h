/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#pragma once

#include <thread>
#include <utility>

#include "lambda.h"

namespace memurai {

class RunningLoop {
    Lambda loopLambda;

    std::thread* t_thread;

    std::atomic_bool isFinishing, gotException;

    void run_loop() {
        try {
            while (!isFinishing.load(std::memory_order_acquire)) {
                loopLambda();
            }
        } catch (...) {
            gotException = true;
        }
    }

public:
    RunningLoop() : t_thread(nullptr), isFinishing(false), gotException(false) {}

    ~RunningLoop() {
        if (t_thread) {
            stop();
            t_thread->join();
            delete t_thread;
        }
    }

    virtual void stop() {
        isFinishing.store(true, std::memory_order_release);
    }

    [[nodiscard]] bool exceptionFound() const {
        return gotException.load(std::memory_order_acquire);
    }

    void start(Lambda lambda) {
        loopLambda = std::move(lambda);
        t_thread = new std::thread([this]() {
            run_loop();
        });
    }
};

} // namespace memurai
