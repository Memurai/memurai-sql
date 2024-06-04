/*
* Copyright (c) 2020-2021, Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <map>
#include <mutex>
#include <set>
#include <tuple>
#include <vector>

#include "aligned_malloc.h"
#include "cache.h"
#include "jobid.h"
#include "types.h"

namespace memurai::sql {
using multicore::JOBID;
using std::atomic;
using std::map;
using std::set;
using std::tuple;
using std::vector;

typedef uint64 BHandle;
typedef uint64 ConstBHandle;

class Buffer {
    typedef uint64 NumElemsType;

    static map<BHandle, tuple<NumElemsType, uint64*>> handle2ptr;
    static std::mutex the_mutex;
    static uint64 next_handle;

    // Metrics
    static uint64 c_handles_created;
    static uint64 c_handles_destroyed;

    static uint64* allocate(uint64 elems) {
        auto bytes = elems * sizeof(uint64);
        auto size = (bytes - (bytes % ALIGNED_PAGE)) + (((bytes % ALIGNED_PAGE) == 0) ? 0 : ALIGNED_PAGE);
        assert(size >= bytes);
        assert(size <= bytes + ALIGNED_PAGE);
        assert(size % ALIGNED_PAGE == 0);
        return (uint64*)ALIGNED_MALLOC(ALIGNED_PAGE, size);
    }

    static void deallocate(void* ptr) {
        return ALIGNED_FREE(ptr);
    }

public:
    enum ErrorCode { InternalError };

    ~Buffer() = delete;

    static BHandle create(uint64 num_elems);

    template <typename T> static BHandle createFromContainer(const T& vec) {
        auto handle = create(vec.size());

        uint64* ptr = Buffer::data(handle);

        for (auto value : vec) {
            *ptr++ = value;
        }

        return handle;
    }

    static void destroy(BHandle);

    static uint64* data(BHandle);
    static cuint64* const_data(BHandle);
    static uint64 num_elems(BHandle);
    static void set_num_elems(BHandle, uint64);

    static BHandle create_from(BHandle handle, uint64 offset, uint64 size);

    static uint64 get_num_buffers();

    template <typename T> class BufferRange {
        BHandle handle;

    public:
        explicit BufferRange(BHandle handle) : handle(handle) {}

        const T* begin() {
            return Buffer::const_data(handle);
        }
        const T* end() {
            return &Buffer::const_data(handle)[Buffer::num_elems(handle)];
        }
    };

    template <typename T> static BufferRange<T> range(BHandle h) {
        return BufferRange<T>(h);
    }
};

typedef vector<BHandle> VecHandles;
typedef vector<ConstBHandle> VecConstHandles;

} // namespace memurai::sql
