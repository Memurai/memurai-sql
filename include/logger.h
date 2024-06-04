/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <atomic>
#include <cassert>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>

#include "sync/syncqueue.h"
#include "util.h"

namespace memurai {

using memurai::syncqueue;
using std::atomic;
using std::get;
using std::make_tuple;
using std::string;
using std::thread;
using std::tuple;

using time_point = std::chrono::system_clock::time_point;

class Logger {
public:
    enum class LogLevel { Debug, Verbose, Info, Warning, Error, Critical, Fatal, Perf };

private:
    syncqueue<tuple<LogLevel, string*, time_point>> queue;
    atomic<bool> s_stop, s_stop_ingest;
    ;

    thread* logger_thread;
    LogLevel minLogLevel;

    static const char* nameOf(LogLevel l) {
        switch (l) {
        case LogLevel::Debug:
            return "DEBUG";
        case LogLevel::Verbose:
            return "VERBOSE";
        case LogLevel::Info:
            return "INFO";
        case LogLevel::Warning:
            return "WARNING";
        case LogLevel::Error:
            return "ERROR";
        case LogLevel::Critical:
            return "CRITICAL!";
        case LogLevel::Fatal:
            return "FATAL!";
        case LogLevel::Perf:
            return "[perf]";
        default:
            assert(UNREACHED);
            return "none";
        }
    }

    void printMsg(tuple<LogLevel, string*, time_point> tup) {
        LogLevel level = get<0>(tup);

        if (level >= minLogLevel) {
            string* s = get<1>(tup);
            std::cout << Util::TimePointToString(get<2>(tup)) << "-" << nameOf(level) << ": " << *s << std::endl;
        }
    }

    void run_loop() {
        std::cout << "Logger run_loop starting.\n";

        while (!s_stop.load(std::memory_order_acquire)) {
            auto ret = queue.dequeue();
            if (ret.has_value()) {
                printMsg(*ret);
                delete std::get<1>(*ret);
            }
        }

        std::cout << "Logger run_loop exiting.\n";
    }

    static Logger* static_logger;

    explicit Logger(LogLevel minLogLevel = LogLevel::Info) : minLogLevel(minLogLevel), s_stop_ingest(false), s_stop(false) {
        logger_thread = new std::thread([this]() {
            this->run_loop();
        });
    }

public:
    Logger(Logger const&) = delete;
    void operator=(Logger const&) = delete;

    static Logger& getInstance() {
        static Logger static_logger;

        return static_logger;
    }

    ~Logger() {
        stop();
        if (logger_thread) {
            logger_thread->join();
            delete logger_thread;
        }
    }

    void setLogLevel(LogLevel l) {
        minLogLevel = l;
    }

    void join() {
        s_stop_ingest.store(true);
        while (queue.size() > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        s_stop.store(true);
        queue.stop();
    }

    void stop() {
        s_stop.store(true);
        queue.stop();
    }

    void log(LogLevel level, string s) {
        // stop adding if stopping!
        if (!s_stop_ingest.load(std::memory_order_acquire)) {
            queue.enqueue(make_tuple(level, new string(std::move(s)), std::chrono::system_clock::now()));
        }
    }

    void log_fatal(string s) {
        printMsg(make_tuple(LogLevel::Fatal, new string(std::move(s)), std::chrono::system_clock::now()));
    }

}; // class Logger

} // namespace memurai

#if !defined(NDEBUG) && defined(_DEBUG)
#define DEBUG_LOG_DEBUG(XX) LOG_DEBUG(XX)
#define DEBUG_LOG_PERF(XX) LOG_PERF(XX)
#else
#define DEBUG_LOG_DEBUG(XX)
#define DEBUG_LOG_PERF(XX)
#endif

#define LOG_INFO(XX)                                                                          \
    {                                                                                         \
        std::stringstream ss____xx_;                                                          \
        ss____xx_ << XX;                                                                      \
        memurai::Logger::getInstance().log(memurai::Logger::LogLevel::Info, ss____xx_.str()); \
    }

#define LOG_PERF(XX)                                                                          \
    {                                                                                         \
        std::stringstream ss____xx_;                                                          \
        ss____xx_ << XX;                                                                      \
        memurai::Logger::getInstance().log(memurai::Logger::LogLevel::Perf, ss____xx_.str()); \
    }

#define LOG_DEBUG(XX)                                                                          \
    {                                                                                          \
        std::stringstream ss____xx_;                                                           \
        ss____xx_ << XX;                                                                       \
        memurai::Logger::getInstance().log(memurai::Logger::LogLevel::Debug, ss____xx_.str()); \
    }

#define LOG_VERBOSE(XX)                                                                          \
    {                                                                                            \
        std::stringstream ss____xx_;                                                             \
        ss____xx_ << XX;                                                                         \
        memurai::Logger::getInstance().log(memurai::Logger::LogLevel::Verbose, ss____xx_.str()); \
    }

#define LOG_ERROR(XX)                                                                          \
    {                                                                                          \
        std::stringstream ss____xx_;                                                           \
        ss____xx_ << XX;                                                                       \
        memurai::Logger::getInstance().log(memurai::Logger::LogLevel::Error, ss____xx_.str()); \
    }

#define LOG_WARNING(XX)                                                                          \
    {                                                                                            \
        std::stringstream ss____xx_;                                                             \
        ss____xx_ << XX;                                                                         \
        memurai::Logger::getInstance().log(memurai::Logger::LogLevel::Warning, ss____xx_.str()); \
    }

#define LOG_CRITICAL(XX)                                                                          \
    {                                                                                             \
        std::stringstream ss____xx_;                                                              \
        ss____xx_ << XX;                                                                          \
        memurai::Logger::getInstance().log(memurai::Logger::LogLevel::Critical, ss____xx_.str()); \
    }

#define LOG_FATAL(XX)                                              \
    {                                                              \
        std::stringstream ss____xx_;                               \
        ss____xx_ << XX;                                           \
        memurai::Logger::getInstance().log_fatal(ss____xx_.str()); \
    }
