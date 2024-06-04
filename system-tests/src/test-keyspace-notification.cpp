#include "test-context.h"

void test_keyspace_notification(test_context& ctx) {
    auto reply = ctx.command("msql.create tbl_notification ON hash item: a BIGINT b FLOAT");
    ctx.expect_array_size(4, reply);

    // HSET
    {
        reply = ctx.command("hset item:0 a 0 b 9.54");
        ctx.expect_integer(2, reply);
        reply = ctx.command("hset item:1 a 1 b 8.23");
        ctx.expect_integer(2, reply);
        reply = ctx.command("hset item:2 a 2 b 7.89");
        ctx.expect_integer(2, reply);

        reply = ctx.command("msql.size tbl_notification");
        ctx.expect_integer(3, reply);
    }

    // HDEL
    {
        reply = ctx.command("hdel item:1 a");
        ctx.expect_integer(1, reply);
    }

    // DEL
    {
        reply = ctx.command("del item:2");
        ctx.expect_integer(1, reply);

        reply = ctx.command("msql.size tbl_notification");
        ctx.expect_integer(2, reply);
    }

    // RENAME
    {
        reply = ctx.command("rename item:0 other");
        ctx.expect_status("OK", reply);

        reply = ctx.command("msql.size tbl_notification");
        ctx.expect_integer(1, reply);

        reply = ctx.command("rename other item:0");
        ctx.expect_status("OK", reply);

        reply = ctx.command("msql.size tbl_notification");
        ctx.expect_integer(2, reply);
    }

    auto check_msql_query_int = [&ctx](long long value) {
        std::string query = "select a from tbl_notification";
        std::vector<std::vector<std::string>> data{{"a"}, {std::to_string(value)}};
        auto reply = ctx.command("msql.query %b", query.data(), query.size());
        ctx.expect_string_matrix(data, reply);
    };

    // HINCRBY
    {
        reply = ctx.command("del item:1");
        ctx.expect_integer(1, reply);

        reply = ctx.command("hincrby item:0 a 1");
        ctx.expect_integer(1, reply);
        check_msql_query_int(1);

        reply = ctx.command("hincrby item:0 a 3");
        ctx.expect_integer(4, reply);
        check_msql_query_int(4);

        reply = ctx.command("hincrby item:0 a -7");
        ctx.expect_integer(-3, reply);
        check_msql_query_int(-3);

        reply = ctx.command("hincrby item:0 a 3");
        ctx.expect_integer(0, reply);
        check_msql_query_int(0);

        auto value = std::numeric_limits<long long>::max() - 1;
        reply = ctx.command("hincrby item:0 a %lld", value);
        ctx.expect_integer(value, reply);
        check_msql_query_int(value);

        reply = ctx.command("hincrby item:0 a %lld", -value);
        ctx.expect_integer(0, reply);
        check_msql_query_int(0);

        value = std::numeric_limits<long long>::min();
        reply = ctx.command("hincrby item:0 a %lld", value);
        ctx.expect_integer(value, reply);
        check_msql_query_int(value);
    }

    auto check_msql_query_float = [&ctx](float value) {
        std::string query = "select b from tbl_notification";
        reply_wrapper reply = ctx.command("msql.query %b", query.data(), query.size());
        auto result = std::stof(std::string(reply->element[1]->element[0]->str, reply->element[1]->element[0]->len));
        if (std::abs(value - result) > std::numeric_limits<float>::epsilon()) {
            ctx.fail_test();
        }
    };

    // HINCRBYFLOAT
    {
        ctx.command("hincrbyfloat item:0 b 0.3");
        check_msql_query_float(9.84);

        ctx.command("hincrbyfloat item:0 b -10.44");
        check_msql_query_float(-0.6);

        ctx.command("hincrbyfloat item:0 b 0.6");
        check_msql_query_float(0);
    }

    // RESTORE
    {
        reply = ctx.command("del item:0");
        ctx.expect_integer(1, reply);

        reply = ctx.command("hset item:1 a 1 b 8.23");
        ctx.expect_integer(2, reply);

        reply_wrapper dump = ctx.command("dump item:1");

        reply = ctx.command("del item:1");
        ctx.expect_integer(1, reply);

        reply = ctx.command("restore item:1 0 %b", dump->str, dump->len);
        ctx.expect_status("OK", reply);
        check_msql_query_int(1);
        check_msql_query_float(8.23);
    }

    ctx.command("flushdb");
    ctx.command("msql.destroy tbl_notification");
}
