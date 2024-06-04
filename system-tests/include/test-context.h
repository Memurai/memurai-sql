#pragma once

#include <iostream>
#include <random>
#include <string>

#include "hiredis/hiredis.h"

class reply_wrapper {
    void* m_ptr;
    bool m_weak;

public:
    void clear() {
        if (!m_weak) {
            freeReplyObject(m_ptr);
        }
        m_weak = false;
        m_ptr = nullptr;
    }

    reply_wrapper() : m_ptr(nullptr), m_weak(true) {}

    // When reply_wrapper is constructed from a void*, it's assumed to come from redisCommand, we must free it.
    reply_wrapper(void* reply) : m_ptr(reply), m_weak(false) {}

    // When reply_wrapper is constructed from redisReply*, it's assumed to be a subobject of a redisCommand, do not free it.
    reply_wrapper(redisReply* reply) : m_ptr(reply), m_weak(true) {}

    reply_wrapper(reply_wrapper&& obj) noexcept : m_ptr(obj.m_ptr), m_weak(obj.m_weak) {
        obj.m_ptr = nullptr;
    }

    reply_wrapper(const reply_wrapper& obj) = delete;
    reply_wrapper& operator=(const reply_wrapper& obj) = delete;

    reply_wrapper& operator=(reply_wrapper&& obj) noexcept {
        clear();
        m_ptr = obj.m_ptr;
        obj.m_ptr = nullptr;
        return *this;
    }

    ~reply_wrapper() {
        clear();
    }

    redisReply* operator->() const {
        return (redisReply*)m_ptr;
    }

    operator bool() const {
        return m_ptr != nullptr;
    }
};

struct test_context {
    redisContext* redis;
    std::mt19937 rng;
    size_t num_failed;

    int fail_test() {
#if !defined(NDEBUG) || defined(_DEBUG)
#ifdef _MSC_VER
        __debugbreak();
#else
        __asm__ volatile("int $0x03");
#endif
#endif
        num_failed++;
        return 0;
    }

    template <class... Args> void* command(Args... args) {
#ifdef _DEBUG
        ((std::cout << args << ' '), ...);
        std::cout << '\n';
#endif
        return redisCommand(redis, args...);
    }

    test_context() {
        num_failed = 0;
        redis = redisConnect("127.0.0.1", 6379);
        if (redis->err) {
            std::cout << "Connection failed, please run Memurai first\n";
            return;
        }
    }

    ~test_context() {
        if (redis) {
            redisFree(redis);
        }
    }

    test_context(test_context&& obj) noexcept : redis(obj.redis), rng(obj.rng), num_failed(obj.num_failed) {
        obj.redis = nullptr;
    }

    test_context& operator=(test_context&& obj) noexcept {
        redis = obj.redis;
        rng = obj.rng;
        num_failed = obj.num_failed;
        obj.redis = nullptr;
        return *this;
    }

    test_context(const test_context& obj) = delete;
    test_context& operator=(const test_context& obj) = delete;

    operator bool() {
        return redis != nullptr && redis->err == 0;
    }

    int expect_integer(long long expected, const reply_wrapper& reply) {
        if (!reply) {
            return fail_test();
        }

        if (reply->type != REDIS_REPLY_INTEGER || reply->integer != expected) {
            return fail_test();
        }

        return 1;
    }

    int expect_status(std::string_view expected, const reply_wrapper& reply) {
        if (!reply) {
            return fail_test();
        }

        if (reply->type != REDIS_REPLY_STATUS || std::string_view(reply->str, reply->len) != expected) {
            return fail_test();
        }

        return 1;
    }

    int expect_string(std::string_view expected, const reply_wrapper& reply) {
        if (!reply) {
            return fail_test();
        }

        if (reply->type != REDIS_REPLY_STRING || std::string_view(reply->str, reply->len) != expected) {
            return fail_test();
        }

        return 1;
    }

    int expect_array_size(std::size_t expected, const reply_wrapper& reply) {
        if (!reply) {
            return fail_test();
        }

        if (reply->type != REDIS_REPLY_ARRAY || reply->elements != expected) {
            return fail_test();
        }

        return 1;
    }

    int expect_string_matrix(const std::vector<std::vector<std::string>> data, const reply_wrapper& reply) {
        if (!reply) {
            return fail_test();
        }

        if (!expect_array_size(data.size(), reply)) {
            return 0;
        }

        for (size_t row = 0; row < data.size(); row++) {
            if (!expect_array_size(data[row].size(), reply->element[0])) {
                return 0;
            }

            for (size_t col = 0; col < data[row].size(); col++) {
                if (!expect_string(data[row][col], reply->element[row]->element[col])) {
                    return 0;
                }
            }
        }

        return 1;
    }
};
