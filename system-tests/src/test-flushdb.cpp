#include "test-context.h"

void test_flushdb(test_context& ctx) {
    const int number_elements = 100;

    auto reply = ctx.command("msql.create tbl-flushdb ON hash t: a BIGINT");
    ctx.expect_array_size(3, reply);

    for (int i = 0; i < number_elements; i++) {
        ctx.command("hset t:%d a %d", i, i);
    }

    // Check size and flushdb
    reply = ctx.command("dbsize");
    ctx.expect_integer(number_elements, reply);

    reply = ctx.command("msql.size tbl-flushdb");
    ctx.expect_integer(number_elements, reply);

    ctx.command("flushdb");

    reply = ctx.command("dbsize");
    ctx.expect_integer(0, reply);

    reply = ctx.command("msql.size tbl-flushdb");
    ctx.expect_integer(0, reply);

    ctx.command("msql.destroy tbl-flushdb");
}
