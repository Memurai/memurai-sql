#include <chrono>
#include <map>
#include <random>
#include <string>
#include <vector>

using namespace std;

#include "test-context.h"

struct ab_pair {
    int a, b;
};

void test_hash_ops(test_context& ctx) {
    std::random_device device;
    mt19937 eng(device());

#if !defined(NDEBUG) || defined(_DEBUG)
    const int range = 4;
#else
    const int range = 200;
#endif
    reply_wrapper reply = ctx.command("msql.create tbl ON hash t: a BIGINT b BIGINT");
    ctx.expect_array_size(4, reply);
    uniform_int_distribution<int> uid(0, range - 1);

    map<int, ab_pair> hashes;

    for (int i = 0; i < 100000; i++) {
        int key = uid(eng);
        int value = uid(eng);

        if (key == 0) {
            vector<ab_pair> int_data(hashes.size());
            size_t idx = 0;
            for (auto [k, v] : hashes) {
                int_data[idx++] = v;
            }

            sort(int_data.begin(), int_data.end(), [&](auto u, auto v) {
                return u.a < v.a;
            });

            vector<vector<std::string>> data(hashes.size() + 1, vector<std::string>(2));
            data[0][0] = "a";
            data[0][1] = "b";
            for (size_t j = 0; j < hashes.size(); j++) {
                data[j + 1][0] = to_string(int_data[j].a);
                data[j + 1][1] = to_string(int_data[j].b);
            }

            std::string query = "select a, b from tbl order by a";
            auto replyCmd = ctx.command("msql.query %b", query.data(), query.size());

            if (hashes.empty()) {
                // No headers are emitted when the result set is empty
                data = {};
            }

            ctx.expect_string_matrix(data, replyCmd);
        } else {
            if (uid(eng) < range / 2) {
                // add/delete
                if (hashes.count(key)) {
                    auto replyCmd = ctx.command("hdel t:%d a b", key);
                    ctx.expect_integer(2, replyCmd);
                    hashes.erase(key);
                } else {
                    auto replyCmd = ctx.command("hset t:%d a %d b %d", key, i, value);
                    ctx.expect_integer(2, replyCmd);
                    hashes[key] = {i, value};
                }
            } else {
                // add/replace
                auto replyCmd = ctx.command("hset t:%d a %d b %d", key, i, value);
                ctx.expect_integer(hashes.count(key) ? 0 : 2, replyCmd);
                hashes[key] = {i, value};
            }
        }
    }

    ctx.command("flushdb");
    ctx.command("msql.destroy tbl");
}
