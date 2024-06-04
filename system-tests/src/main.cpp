#include "test-context.h"

#include <iostream>

using namespace std;

void test_hash_ops(test_context& ctx);
void test_flushdb(test_context& ctx);
void test_keyspace_notification(test_context& ctx);

int main() {
    test_context ctx;

    if (!ctx) {
        return 1;
    }

    test_hash_ops(ctx);
    test_flushdb(ctx);
    test_keyspace_notification(ctx);
    cout << "Errors: " << ctx.num_failed << '\n';
    if (ctx.num_failed != 0) {
        return 1;
    }
    return 0;
}
