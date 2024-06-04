/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#include <cassert>

#include "Buffer.h"
#include "scope/expect.h"

namespace memurai::sql {
using std::make_pair;
using std::make_tuple;

map<BHandle, tuple<Buffer::NumElemsType, uint64*>> Buffer::handle2ptr;
std::mutex Buffer::the_mutex;
uint64 Buffer::next_handle{0};
uint64 Buffer::c_handles_created{0};
uint64 Buffer::c_handles_destroyed{0};

void Buffer::destroy(BHandle handle) {
    std::unique_lock<std::mutex> guard(the_mutex);

    auto iter = handle2ptr.find(handle);
    assert(iter != handle2ptr.end());
    EXPECT_OR_THROW(iter != handle2ptr.end(), InternalError, "Invalid BHandle in destroy.");

    handle2ptr.erase(iter);
}

BHandle Buffer::create_from(BHandle handle, uint64 offset, uint64 size) {
    BHandle ret = create(size);

    auto dest_ptr = Buffer::data(ret);
    auto src_ptr = Buffer::const_data(handle);

    for (uint64 idx = 0; idx < size; idx++) {
        dest_ptr[idx] = src_ptr[offset + idx];
    }

    return ret;
}

BHandle Buffer::create(uint64 num_elems) {
    uint64* ptr = allocate(num_elems);

    std::unique_lock<std::mutex> guard(the_mutex);

    BHandle handle = next_handle++;

    handle2ptr.insert(make_pair(handle, make_tuple(num_elems, ptr)));

    c_handles_created++;

    return handle;
}

void Buffer::set_num_elems(BHandle handle, uint64 new_num_elems) {
    {
        std::unique_lock<std::mutex> guard(the_mutex);
        auto iter = handle2ptr.find(handle);
        EXPECT_OR_THROW(iter != handle2ptr.end(), InternalError, "Invalid BHandle in set_num_elems.");
        get<0>(iter->second) = new_num_elems;
    }
    assert(new_num_elems == num_elems(handle));
}

uint64 Buffer::num_elems(BHandle handle) {
    std::unique_lock<std::mutex> guard(the_mutex);
    auto iter = handle2ptr.find(handle);
    EXPECT_OR_THROW(iter != handle2ptr.end(), InternalError, "Invalid BHandle in set_num_elems.");
    return get<0>(iter->second);
}

uint64* Buffer::data(BHandle handle) {
    std::unique_lock<std::mutex> guard(the_mutex);
    auto iter = handle2ptr.find(handle);
    EXPECT_OR_THROW(iter != handle2ptr.end(), InternalError, "Invalid BHandle in set_num_elems.");
    return get<1>(iter->second);
}

cuint64* Buffer::const_data(BHandle handle) {
    return data(handle);
}

uint64 Buffer::get_num_buffers() {
    std::unique_lock<std::mutex> guard(the_mutex);
    return handle2ptr.size();
}

} // namespace memurai::sql
