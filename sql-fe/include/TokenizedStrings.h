/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <condition_variable>
#include <cstring>
#include <list>
#include <map>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <emmintrin.h>

#include "aligned_malloc.h"
#include "cache.h"
#include "growingStrategy.h"
#include "logger.h"
#include "scope/expect.h"
#include "scope/scope_exit.h"
#include "string_token.h"
#include "strings_metadata.h"
#include "types.h"

namespace memurai::sql {

using std::atomic;
using std::atomic_bool;
using std::list;
using std::make_pair;
using std::map;
using std::optional;
using std::string;
using std::tuple;
using std::unordered_map;
using std::vector;

class Test_HashedStrings;

// Strings are transformed into hashes (and put in the columnar data), and live
// in an hashtable   (hash -> list_of strings)
// Trying to minimize collisions with MD5 64bit.
//
class TokenizedStrings {

public:
    enum ErrorCode { InternalError };

    friend class Test_TokenizedStrings;

    struct Config {
        uint64 initialStringBufferCapacity;
        uint64 initialTokenInfoCapacity;
        uint32 minSearchResultSize;
        GrowingStrategy growthStringBufferStrategy;
        GrowingStrategy growthTokenInfoStrategy;
    };

    ///  ThresholdStrategy: here threshold kicks in at 2K _absolute_ distance from boundary.
    static constexpr ThresholdStrategy thresholdStrat = {ThresholdStrategy::StrategyType::Absolute, 2'000};

    ///  We grow when threshold (above) kicks in, and we grow linearly by 10x
    static constexpr GrowingStrategy gstrat = {GrowingStrategy::StrategyType::Linear, 10};

    // Default initial capacity of 1M, minimum search result size 1K.
    static constexpr Config default_config = {1'000'000, 1'000'000, 1'000, gstrat, gstrat};

private:
    const Config config;

#define WAIT_FOR_GETS_ENABLED                                                                \
    while (false == s_gets_enabled.load(std::memory_order_acquire))                          \
        [[unlikely]] {                                                                       \
            c_trapped++;                                                                     \
            std::unique_lock<std::shared_mutex> lock_guard(mutex_general);                   \
            cv_mutating_done.wait_for(lock_guard, std::chrono::milliseconds(100), [this]() { \
                return s_gets_enabled.load(std::memory_order_acquire);                       \
            });                                                                              \
        }

    // We busy wait here because gets are super fast
#define WAIT_FOR_ZERO_GETS                                      \
    while (0 != s_active_get.load(std::memory_order_acquire)) { \
        _mm_pause();                                            \
    }
#define DISABLE_GETS s_gets_enabled.store(false, std::memory_order_release)
#define ENABLE_GETS s_gets_enabled.store(true, std::memory_order_release)

    // in reality this is 48bit!!
    typedef uint64 PosIdx;
    typedef uint32 RefCount;
    typedef uint64 meta_uint64;

    static constexpr uint64 STOKEN_POSPTR_BITPOS = 16;
    static constexpr uint64 REFCOUNT_BITMASK = 0xFFFF;

public:
    static constexpr uint64 XLSTRING_MINIMUM_SIZE = 4096;

    static StringType StringTypeOfString(const string& str) {
        cuint64 len = str.size();

        if (len >= XLSTRING_MINIMUM_SIZE)
            return StringType::XL;

        return StringType::Normal;
    }

    // 48 bits PosIdx, 16bit RefCount
    typedef uint64 PosIdxRefCnt;

private:
    uint64 n_tokenIdx;

    struct StringInfo {
        PosIdxRefCnt pos_ptr_refcnt;
        uint64 metadata;
    };

    static PosIdx StringInfo2PosIdx(const StringInfo& sinfo) {
        return PosIdx((sinfo.pos_ptr_refcnt) >> STOKEN_POSPTR_BITPOS);
    }

    StringToken generate_new_token(StringType st) {
        uint64 ret = n_tokenIdx++;
        if (ret == token2info.size()) {
            growTokenInfo();
        }

        return {ret, st};
    }

    static RefCount StringInfo2RefCount(const StringInfo& sinfo) {
        return ((sinfo.pos_ptr_refcnt) & 0xFFFF);
    }

    const StringInfo& getSInfoFromSToken(const StringToken& tok) const {
        uint64 temp = tok.getPlainToken();
        return token2info[temp];
    }

    StringInfo& getSInfoFromSToken(const StringToken& tok) {
        uint64 temp = tok.getPlainToken();
        assert(temp < token2info.size());
        EXPECT_OR_THROW(temp < token2info.size(), InternalError, "Wrong Token: plain too big.");
        return token2info[temp];
    }

    RefCount DecreaseRefCount(const StringToken& tok) {
        StringInfo& sinfo = getSInfoFromSToken(tok);
        RefCount refcnt = StringInfo2RefCount(sinfo);
        assert(refcnt > 0);
        EXPECT_OR_THROW(refcnt > 0, InternalError, "RefCount cannot be zero when decrementing.");
        refcnt = (refcnt - 1) & REFCOUNT_BITMASK;
        sinfo.pos_ptr_refcnt &= ~REFCOUNT_BITMASK;
        sinfo.pos_ptr_refcnt |= refcnt;
        return refcnt;
    }

    RefCount getRefCount(const StringToken& tok) {
        StringInfo& sinfo = getSInfoFromSToken(tok);
        return StringInfo2RefCount(sinfo);
    }

    RefCount IncreaseRefCount(const StringToken& tok) {
        StringInfo& sinfo = getSInfoFromSToken(tok);
        RefCount refcnt = StringInfo2RefCount(sinfo);
        RefCount orig_refcnt = refcnt;
        refcnt = (refcnt + 1) & REFCOUNT_BITMASK;
        sinfo.pos_ptr_refcnt &= ~REFCOUNT_BITMASK;
        sinfo.pos_ptr_refcnt |= refcnt;

        return orig_refcnt;
    }

    // Indexed by (almost) raw tokens
    vector<StringInfo> token2info;

    //   Metadata -> list<STokens>
    // map<meta_uint64, list<StringToken>*>  str2token;
    unordered_map<string, StringToken> str2token;

    //
    map<PosIdx, StringToken> pos2token;

    // "The" buffer for "normal" sized strings
    char* string_buffer;
    uint64 stringBufferCapacity;
    uint64 growth_string_buffer_threshold;
    uint32 n_sizeSoFar, n_numStringSoFar;
    mutable std::shared_mutex mutex_general;
    // Using this CV to break into mutex above.
    mutable std::condition_variable_any cv_mutating_done;

    mutable atomic<bool> s_gets_enabled;
    mutable atomic<uint64> s_active_get;

    mutable atomic_bool s_commands_enabled;
    mutable atomic<uint64> s_active_commands;
    mutable atomic<uint64> s_growth_decider;

    // Metrics
    atomic<uint64> c_collisions;
    atomic<uint64> c_growths_string_buffer, c_growths_token_info;
    mutable atomic<uint64> c_trapped;

    // XL Strings!!!!!!!!!
    // Special treatment for strings greater than 4KB
    typedef uint64 xlstring_len;
    typedef uint64 xls_string_length;
    unordered_map<xlstring_len, list<StringToken>*> xlstrings;
    typedef decltype(xlstrings)::iterator xlstring_iterator;
    //
    map<cchar*, StringToken> ptr2XLstoken;

    static char* allocate(uint64 bytes) {
        auto size = (bytes - (bytes % ALIGNED_PAGE)) + (((bytes % ALIGNED_PAGE) == 0) ? 0 : ALIGNED_PAGE);
        assert(size >= bytes);
        assert(size <= bytes + ALIGNED_PAGE);
        assert(size % ALIGNED_PAGE == 0);

        return (char*)ALIGNED_MALLOC(ALIGNED_PAGE, size);
    }

    static void deallocate(char* ptr) {
        return ALIGNED_FREE(ptr);
    }

    uint64 GetLengthFromSToken(const StringToken& stok) const {
        cchar* ptr = nullptr;
        StringType stype = stok.getStringType();
        uint64 temp;

        auto sinfo = getSInfoFromSToken(stok);
        temp = (sinfo.pos_ptr_refcnt >> STOKEN_POSPTR_BITPOS);
        uint32 stoklen;

        switch (stype) {
        case StringType::Normal:
            ptr = &string_buffer[temp];
            stoklen = *(uint32*)ptr;
            ptr += 4; // SHould be replaced with a MACRO
            break;
        case StringType::XL:
            ptr = ((char*)temp);
            stoklen = *(uint32*)ptr;
            ptr += 4; // SHould be replaced with a MACRO
            break;
        default:
            EXPECT_OR_THROW(false, InternalError, "StringType not expected in GetLengthFromSToken.");
            break;
        }

        return stoklen;
    }

    bool middle_of_token_equal_to(StringToken stok, cchar* cstr, uint64 len) {
        cchar* ptr = nullptr;
        uint64 fulltoken = stok.getFullToken();
        StringType stype = stok.getStringType();

        auto sinfo = getSInfoFromSToken(stok);
        uint64 temp;
        temp = (sinfo.pos_ptr_refcnt >> STOKEN_POSPTR_BITPOS);
        uint32 stoklen;

        switch (stype) {
        case StringType::Normal:
            ptr = &string_buffer[temp];
            stoklen = *(uint32*)ptr;
            ptr += 4; // SHould be replaced with a MACRO
            break;
        case StringType::XL:
            ptr = ((char*)temp);
            stoklen = *(uint32*)ptr;
            ptr += 4; // SHould be replaced with a MACRO
            break;
        default:
            EXPECT_OR_THROW(false, InternalError, "StringType not expected in identical_string.");
            break;
        }

        if (stoklen < len)
            return false;

        for (uint64 idx = 0; idx < stoklen - len; idx++) {
            if (memcmp((void*)&ptr[idx], cstr, len) == 0) {
                return true;
            }
        }

        return false;
    }

    bool end_of_token_equal_to(StringToken stok, cchar* cstr, uint64 len) {
        cchar* ptr = nullptr;
        uint64 fulltoken = stok.getFullToken();
        StringType stype = stok.getStringType();

        auto sinfo = getSInfoFromSToken(stok);
        uint64 temp;
        temp = (sinfo.pos_ptr_refcnt >> STOKEN_POSPTR_BITPOS);
        uint32 stoklen;

        switch (stype) {
        case StringType::Normal:
            ptr = &string_buffer[temp];
            stoklen = *(uint32*)ptr;
            ptr += 4; // SHould be replaced with a MACRO
            break;
        case StringType::XL:
            ptr = ((char*)temp);
            stoklen = *(uint32*)ptr;
            ptr += 4; // SHould be replaced with a MACRO
            break;
        default:
            EXPECT_OR_THROW(false, InternalError, "StringType not expected in identical_string.");
            break;
        }

        if (stoklen < len)
            return false;

        return (memcmp(&ptr[stoklen - len], cstr, len) == 0);
    }

    //
    //
    bool identical_strings(StringToken stok, cchar* cstr, cuint64 len) {
        cchar* ptr = nullptr;
        uint64 fulltoken = stok.getFullToken();
        StringType stype = stok.getStringType();

        auto sinfo = getSInfoFromSToken(stok);
        uint64 temp;
        temp = (sinfo.pos_ptr_refcnt >> STOKEN_POSPTR_BITPOS);
        uint32 length;

        switch (stype) {
        case StringType::Normal:
            ptr = &string_buffer[temp];
            length = *(uint32*)ptr;
            ptr += 4; // SHould be replaced with a MACRO
            break;
        case StringType::XL:
            ptr = ((char*)temp);
            length = *(uint32*)ptr;
            ptr += 4; // SHould be replaced with a MACRO
            break;
        default:
            EXPECT_OR_THROW(false, InternalError, "StringType not expected in identical_string.");
            break;
        }

        return (memcmp(ptr, cstr, length) == 0);
    }

    void resetStringBufferGrowthThreshold() {
        // TO DO:  TO BE CONFIGURED!!
        growth_string_buffer_threshold = stringBufferCapacity - 1000;
    }

    void growStringBuffer() {
        DISABLE_GETS;
        WAIT_FOR_ZERO_GETS;

        uint64 newCapacity = config.growthStringBufferStrategy.grow(stringBufferCapacity);
        char* new_data = allocate(newCapacity);
        memcpy(new_data, string_buffer, n_sizeSoFar);
        deallocate(string_buffer);
        string_buffer = new_data;
        stringBufferCapacity = newCapacity;
        ENABLE_GETS;
        c_growths_string_buffer++;
        resetStringBufferGrowthThreshold();
        cv_mutating_done.notify_all();

        LOG_DEBUG("StringBuffer grew to " << newCapacity);
    }

    void growTokenInfo() {
        DISABLE_GETS;
        WAIT_FOR_ZERO_GETS;

        uint64 newCapacity = config.growthTokenInfoStrategy.grow(token2info.size());
        assert(newCapacity > token2info.size());
        token2info.resize(newCapacity);
        ENABLE_GETS;
        c_growths_token_info++;
        cv_mutating_done.notify_all();

        LOG_DEBUG("token2info grew to " << newCapacity);
    }

    uint64 getStringBufferSizeApprox() const noexcept {
        return n_sizeSoFar;
    }

    uint64 getHashTableSize() const noexcept {
        std::shared_lock<std::shared_mutex> mlock(mutex_general);
        assert(UNREACHED);
        return 0;
    }

    template <typename T1, typename T2> class HandleImpl {
        const bool t1_found_, t2_found_;
        T1 t1;
        T2 t2;

    public:
        HandleImpl(const T1 t1) : t1(t1), t1_found_(true), t2_found_(false) {}

        HandleImpl(const T2 t2) : t2(t2), t2_found_(true), t1_found_(false) {}

        HandleImpl() : t1_found_(false), t2_found_(false) {}

        [[nodiscard]] bool t1_found() const {
            return t1_found_;
        }
        [[nodiscard]] bool t2_found() const {
            return t2_found_;
        }

        [[nodiscard]] T1 getT1() const {
            assert(t1_found_);
            return t1;
        }

        [[nodiscard]] T2 getT2() const {
            assert(t2_found_);
            return t2;
        }
    };

    typedef HandleImpl<list<StringToken>*, StringToken*> Handle;

    // Find a "Normal" string. Assuming XL and SMall have already been ruled out
    //
    optional<StringToken> findStringCore(uint64 meta, cchar* cstr, uint64 len) {
        // We assume the lock has already been taken.

        // is the hash present in the hashmap?
        auto iter = str2token.find(cstr);
        if (iter != str2token.end()) {
            return iter->second;
        }

        return std::nullopt;
    }

    void removeInternal(PosIdx posIdx, StringToken stoken) {
        auto iter = pos2token.find(posIdx);
        assert(iter != pos2token.end());
        assert(iter->second == stoken);

        auto sinfo = getSInfoFromSToken(stoken);
        RefCount refcnt = StringInfo2RefCount(sinfo);

        if (refcnt > 0) {
            auto after = DecreaseRefCount(stoken);
            if (after == 0) {
                n_numStringSoFar--;

                // We keep it here, but with refcnt == 0??

                // token2info[stoken.getPlainToken()] = StringInfo{ 0ull, 0ull };
                // pos2token.erase(iter);
                // We do not phyisicall remove the string from the buffer !!
            }
        } else {
            /// ???
        }
    }

    Handle findXLStringCore(cuint64 len, cchar* cstr) {
        auto iter = xlstrings.find(len);

        if (iter != xlstrings.end()) {
            list<StringToken>* str_list = iter->second;

            for (StringToken& stoken : *str_list) {
                auto sinfo = getSInfoFromSToken(stoken);
                RefCount refcnt = StringInfo2RefCount(sinfo);

                if ((refcnt > 0) && identical_strings(stoken, cstr, len)) {
                    return {&stoken};
                }
            }

            return {str_list};
        }

        return {};
    }

    char* allocate_xlstring(cuint64 len) {
        return allocate(len);
    }

    StringToken addXLString(cchar* cstr, cuint64 len) {
        std::unique_lock<std::shared_mutex> mlock(mutex_general);
        s_active_commands++;
        SCOPE_EXIT {
            s_active_commands--;
        };

        auto handle = findXLStringCore(len, cstr);

        if (handle.t2_found()) {
            // found it!
            // increase ref count
            StringToken& stoken = *handle.getT2();
            IncreaseRefCount(stoken);

            return stoken;
        } else {
            // Token not found
            // Either found the list for this len, or found nothing
            //
            auto ptr = allocate_xlstring(len + 4); // TO DO: replace 4 with a MACRO
            *(uint32*)ptr = uint32(len);
            memcpy(ptr + 4, cstr, len);
            StringToken stoken = generate_new_token(StringType::XL);
            n_numStringSoFar++;

            if (handle.t1_found()) {
                // found list but not token.
                auto s_list = handle.getT1();
                s_list->push_back(stoken);
            } else {
                // found nothing
                xlstrings.insert(make_pair(len, new list<StringToken>{stoken}));
            }

            ptr2XLstoken.insert(make_pair(ptr, stoken));

            Metadata meta(cstr);
            PosIdxRefCnt temp = (reinterpret_cast<uint64>(ptr) << STOKEN_POSPTR_BITPOS) | RefCount(1);
            token2info[stoken.getPlainToken()] = StringInfo{temp, meta.get()};

            return stoken;
        }
    }

    void removeXLStringInternal(const StringToken& stok) {
        // decrease ref count, and delete if needed

        RefCount refcnt = DecreaseRefCount(stok);

        if (refcnt == 0) {
            n_numStringSoFar--;

            auto len = GetLengthFromSToken(stok);

            //
            auto sinfo = getSInfoFromSToken(stok);
            uint64 temp = (sinfo.pos_ptr_refcnt >> STOKEN_POSPTR_BITPOS);

            cchar* ptr = (cchar*)temp;
            auto handle2 = findXLStringCore(len, ptr);
            assert(handle2.t2_found() == false);
            assert(handle2.t1_found() == true);
            EXPECT_OR_THROW(handle2.t2_found() == false, InternalError, "Internal error in removeXLString. (" << __LINE__ << ")");
            EXPECT_OR_THROW(handle2.t1_found() == true, InternalError, "Internal error in removeXLString. (" << __LINE__ << ")");

            auto str_list = handle2.getT1();

            auto iter = std::find_if(str_list->begin(), str_list->end(), [this, ptr](StringToken elem) {
                auto sinfo2 = getSInfoFromSToken(elem);
                uint64 temp2 = (sinfo2.pos_ptr_refcnt >> STOKEN_POSPTR_BITPOS);
                cchar* ptr_elem = (cchar*)temp2;

                // Yes, comparing pointers!!
                return (ptr_elem == ptr);
            });

            assert(iter != str_list->end());
            EXPECT_OR_THROW(iter != str_list->end(), InternalError, "Internal error in removeXLString. (" << __LINE__ << ")");

            // fo this before the following erase
            token2info[stok.getPlainToken()] = StringInfo{0ull, 0ull};

            // finally remove this tuple from the list
            str_list->erase(iter);

            // If we just emptied the list...
            if (str_list->empty()) {
                // remove it...
                delete str_list;
                xlstrings.erase(len);
            }

            ptr2XLstoken.erase(ptr);
            // deallocate it

            deallocate(const_cast<char*>(ptr));
        }
    }

    void removeXLString(cchar* cstr, uint64 len) {
        std::unique_lock<std::shared_mutex> mlock(mutex_general);
        s_active_commands++;
        SCOPE_EXIT {
            s_active_commands--;
        };

        auto handle = findXLStringCore(len, cstr);

        if (handle.t2_found()) {
            // found it!

            StringToken& stoken = *handle.getT2();
            removeXLStringInternal(stoken);
        } else {
            // not found???
            // metric??
        }
    }

    void findXLSPatternStart(vector<StringToken>& ret, cchar* pattern, uint64 len) {
        for (auto xls : xlstrings) {
            xlstring_len str_len = xls.first;
            if (str_len >= len) {
                auto str_list = *xls.second;

                for (StringToken& stok : str_list) {
                    if (identical_strings(stok, pattern, len)) {
                        ret.push_back(stok);
                    }
                }
            }
        }
    }

    void findXLSPatternEnd(vector<StringToken>& ret, cchar* pattern, uint64 len) {
        for (auto xls : xlstrings) {
            xlstring_len str_len = xls.first;
            if (str_len >= len) {
                auto str_list = *xls.second;

                for (auto stok : str_list) {
                    auto str_len_from_token = GetLengthFromSToken(stok);

                    if ((str_len_from_token >= len) && end_of_token_equal_to(stok, pattern, len)) {
                        ret.push_back(stok);
                    }
                }
            }
        }
    }

    void findXLSPatternMiddle(vector<StringToken>& ret, cchar* pattern, uint64 len) {
        const char firstCh = pattern[0];

        for (auto xls : xlstrings) {
            xlstring_len str_len = xls.first;
            if (str_len >= len) {
                auto str_list = *xls.second;

                for (auto stok : str_list) {
                    if (middle_of_token_equal_to(stok, pattern, len)) {
                        ret.push_back(stok);
                    }
                }
            }
        }
    }

public:
    // ctor
    explicit TokenizedStrings(const Config& config = default_config)
        : config(config),
          n_sizeSoFar(0ull),
          stringBufferCapacity(config.initialStringBufferCapacity),
          growth_string_buffer_threshold(0),
          n_numStringSoFar(0),
          s_active_commands(0),
          s_growth_decider(0),
          s_commands_enabled(true),
          c_collisions(0),
          s_active_get(0),
          s_gets_enabled(true),
          c_trapped(0),
          c_growths_string_buffer(0),
          c_growths_token_info(0),
          n_tokenIdx(0) {
        string_buffer = allocate(stringBufferCapacity);

        token2info.resize(config.initialTokenInfoCapacity);

        resetStringBufferGrowthThreshold();
    }

    // dtor
    ~TokenizedStrings() {
        deallocate(string_buffer);
    }

    [[nodiscard]] uint64 getNumCollisions() const noexcept {
        return c_collisions;
    }

    tuple<uint64, uint64, uint64> startSerialization() const {
        mutex_general.lock();
        return std::move(make_tuple(getNumStringsApprox(), getStringBufferSizeApprox(), getHashTableSize()));
    }

    [[nodiscard]] uint64 getNumStringsApprox() const noexcept {
        return n_numStringSoFar;
    }

    optional<StringToken> findNormalString(const string& str) {
        Metadata meta(str);

        std::shared_lock<std::shared_mutex> mlock(mutex_general);
        s_active_commands++;
        SCOPE_EXIT {
            s_active_commands--;
        };

        return findStringCore(meta.get(), str.c_str(), str.length());
    }

    optional<StringToken> findXLString(const string& str) {
        std::shared_lock<std::shared_mutex> mlock(mutex_general);
        s_active_commands++;
        SCOPE_EXIT {
            s_active_commands--;
        };

        auto handle = findXLStringCore(str.size(), str.c_str());

        if (!handle.t2_found())
            return std::nullopt;

        return *handle.getT2();
    }

    StringToken addString(const string& str) {
        return addString(str.c_str(), str.length());
    }

    StringToken addString(cchar* cstr, cuint64 len0) {
        cuint64 len = len0 + 1;
        Metadata meta(cstr, len);

        if (len >= XLSTRING_MINIMUM_SIZE) {
            return addXLString(cstr, len);
        }

        uint64 meta1 = meta.get();

        std::unique_lock<std::shared_mutex> mlock(mutex_general);
        s_active_commands++;
        SCOPE_EXIT {
            s_active_commands--;
        };

        auto opt = findStringCore(meta.get(), cstr, len);

        if (opt.has_value()) {
            auto stoken = *opt;
            auto sinfo = getSInfoFromSToken(stoken);
            auto posIdx = StringInfo2PosIdx(sinfo);
            auto iter = pos2token.find(posIdx);

            if (iter != pos2token.end()) {
                c_collisions++;
                auto prev_refcnt = IncreaseRefCount(stoken);
                if (prev_refcnt == 0) {
                    n_numStringSoFar++;
                }
            } else {
                StringToken new_stoken = generate_new_token(StringType::Normal);
                pos2token.insert(make_pair(posIdx, new_stoken));
                n_numStringSoFar++;

                PosIdxRefCnt temp = (posIdx << STOKEN_POSPTR_BITPOS) | RefCount(1);
                token2info[new_stoken.getPlainToken()] = StringInfo{temp, meta1};
            }
            return stoken;
        } else {
            // Not found

            // check first if we have enough room for the string
            uint32 slen = uint32(len) + 4;
            PosIdx posIdx = n_sizeSoFar;

            if (posIdx + slen > growth_string_buffer_threshold) {
                growStringBuffer();
            }

            n_sizeSoFar += slen;
            StringToken stoken = generate_new_token(StringType::Normal);
            pos2token.insert(make_pair(posIdx, stoken));

            str2token.insert(make_pair(cstr, stoken));

            auto destPtr = &string_buffer[posIdx];
            *(uint32*)destPtr = uint32(len);
            memcpy(destPtr + 4, cstr, len);
            n_numStringSoFar++;

            PosIdxRefCnt temp = (posIdx << STOKEN_POSPTR_BITPOS) | RefCount(1);
            token2info[stoken.getPlainToken()] = StringInfo{temp, meta1};

            return stoken;
        }
    }

    void removeString(const string& str) {
        removeString(str.c_str(), str.length());
    }

    void removeString(cchar* cstr, cuint64 len0) {
        cuint64 len = len0 + 1;
        Metadata meta(cstr, len);

        if (len >= XLSTRING_MINIMUM_SIZE) {
            removeXLString(cstr, len);
            return;
        }

        std::unique_lock<std::shared_mutex> mlock(mutex_general);
        s_active_commands++;
        SCOPE_EXIT {
            s_active_commands--;
        };

        auto opt = findStringCore(meta.get(), cstr, len);

        if (opt.has_value()) {
            auto stoken = *opt;
            auto sinfo = getSInfoFromSToken(stoken);
            PosIdx posIdx = StringInfo2PosIdx(sinfo);
            removeInternal(posIdx, stoken);
        }
    }

    void removeString(StringToken stoken) {
        std::unique_lock<std::shared_mutex> mlock(mutex_general);
        s_active_commands++;
        SCOPE_EXIT {
            s_active_commands--;
        };

        if (stoken.getStringType() == StringType::XL) {
            removeXLStringInternal(stoken);
        } else {
            auto sinfo = getSInfoFromSToken(stoken);
            PosIdx posIdx = StringInfo2PosIdx(sinfo);
            removeInternal(posIdx, stoken);
        }
    }

    [[nodiscard]] bool contains(const string& str) {
        return contains(str.c_str(), str.length());
    }

    [[nodiscard]] bool contains(cchar* cstr, uint64 len0) {
        cuint64 len = len0 + 1;
        Metadata meta(cstr, len);

        std::shared_lock<std::shared_mutex> mlock(mutex_general);
        s_active_commands++;
        SCOPE_EXIT {
            s_active_commands--;
        };

        auto opt = findStringCore(meta.get(), cstr, len);

        return (opt.has_value() && (getRefCount(*opt) > 0));
    }

    [[nodiscard]] bool contains(const StringToken& stoken) {
        std::shared_lock<std::shared_mutex> mlock(mutex_general);
        s_active_commands++;
        SCOPE_EXIT {
            s_active_commands--;
        };

        // token
        auto sinfo = getSInfoFromSToken(stoken);
        return (StringInfo2RefCount(sinfo) > 0);
    }

    void serializeAndUnlock(void*) const {
        assert(UNREACHED);
        mutex_general.unlock();
    }

    void cleanup() {
        std::unique_lock<std::shared_mutex> mlock(mutex_general);
        assert(UNREACHED);
    }

    // This method achieves 59MHz in either single thread or multithreaded aggregated bandwidth!
    // It means that we achieved in making this super fast! :)
    //
    // This blocks only if another thread is attempting to grow (or mutate)
    // The check is just an interlocked access to verify that,
    // and another interlocked to increment the s_active_get.
    string getString(StringToken stok) const {
        WAIT_FOR_GETS_ENABLED;
        s_active_get++;
        SCOPE_EXIT {
            s_active_get--;
        };

        cchar* ptr = nullptr;
        StringType stype = stok.getStringType();

        EXPECT_OR_THROW(stype != StringType::Reserved, InternalError, "StringType not expected in getString.");

        auto sinfo = getSInfoFromSToken(stok);
        uint64 temp = (sinfo.pos_ptr_refcnt >> STOKEN_POSPTR_BITPOS);
        uint32 length = 0;

        switch (stype) {
        case StringType::Normal:
            ptr = &string_buffer[temp];
            length = *(uint32*)ptr;
            ptr += 4; // SHould be replaced with a MACRO
            break;
        case StringType::XL:
            ptr = ((char*)&temp);
            length = *(uint32*)ptr;
            ptr += 4; // SHould be replaced with a MACRO
            break;
        default:
            EXPECT_OR_THROW(false, InternalError, "StringType not expected in getString.");
            break;
        }

        // do not include final binary zero?!
        return {ptr, length - 1};
    }

    vector<StringToken> findPattern(const string& str) {
        vector<StringToken> ret;
        cchar* pattern = str.c_str();
        cuint64 slen = str.length();
        if ((slen == 0) || (*pattern == 0))
            return std::move(ret);

        ret.reserve(config.minSearchResultSize);

        bool ends = pattern[0] == '%';
        bool start = pattern[slen - 1] == '%';

        assert(start || ends);

        WAIT_FOR_GETS_ENABLED;
        s_active_get++;
        SCOPE_EXIT {
            s_active_get--;
        };

        vector<PosIdx> poses;
        if (start && !ends) {
            poses = std::move(findPatternStart(pattern, slen - 1));
            findXLSPatternStart(ret, pattern, slen - 1);
        } else if (ends && !start) {
            poses = std::move(findPatternEnd(pattern + 1, slen - 1));
            findXLSPatternEnd(ret, pattern + 1, slen - 1);
        } else {
            poses = std::move(findPatternMiddle(pattern + 1, slen - 2));
            findXLSPatternMiddle(ret, pattern + 1, slen - 2);
        }

        // now convert poses into tokens
        std::shared_lock<std::shared_mutex> mlock(mutex_general);

        for (auto posIdx : poses) {
            auto iter = pos2token.find(posIdx);
            assert(iter != pos2token.end());
            EXPECT_OR_THROW((iter != pos2token.end()), InternalError, "Fatal: posIdx not found in map in findPattern.");

            auto& stoken = iter->second;
            auto sinfo = getSInfoFromSToken(stoken);
            auto refCnt = StringInfo2RefCount(sinfo);
            if (refCnt != 0) {
                ret.push_back(stoken);
            }
        }

        return std::move(ret);
    }

private:
    vector<PosIdx> findPatternStart(cchar* pattern, uint64 size) {
        vector<PosIdx> ret;
        ret.reserve(config.minSearchResultSize);
        cchar* ptr = string_buffer;
        cchar* const end_ptr = &string_buffer[n_sizeSoFar];
        PosIdx posIdx = 0;

        while (ptr < end_ptr) {
            cuint32 cur_len = *(uint32*)ptr;
            ptr += 4; // 4...

            if ((cur_len >= size) && (memcmp(pattern, ptr, size) == 0)) {
                ret.push_back(posIdx);
            }

            ptr += cur_len;
            posIdx += cur_len + 4; // 4...
        }

        return std::move(ret);
    }

    vector<PosIdx> findPatternEnd(cchar* pattern, uint64 size) {
        vector<PosIdx> ret;
        ret.reserve(config.minSearchResultSize);
        cchar* ptr = string_buffer;
        cchar* const end_ptr = &string_buffer[n_sizeSoFar];
        PosIdx posIdx = 0;

        while (ptr < end_ptr) {
            cuint32 cur_len = *(uint32*)ptr;
            ptr += 4 + cur_len; // 4...

            if ((cur_len >= size) && (memcmp(pattern, ptr - size - 1, size) == 0)) {
                ret.push_back(posIdx);
            }

            posIdx += cur_len + 4;
        }

        return std::move(ret);
    }

    vector<PosIdx> findPatternMiddle(cchar* pattern, uint64 size) {
        vector<PosIdx> ret;
        ret.reserve(config.minSearchResultSize);
        cchar* ptr = string_buffer;
        cchar* const end_ptr = &string_buffer[n_sizeSoFar];
        PosIdx posIdx = 0, lastPosIdx = n_sizeSoFar + 1;

        char firstCh = pattern[0];

        while (ptr < end_ptr) {
            cuint32 cur_len = *(uint32*)ptr;
            auto base_ptr = ptr + 4; // 4...

            if (cur_len >= size) {
                for (uint64 idx = 0; idx < cur_len - size; idx++) {
                    if ((*base_ptr == firstCh) && (memcmp(pattern, base_ptr, size) == 0)) {
                        ret.push_back(posIdx);
                        break;
                    }
                    base_ptr++;
                }
            }

            ptr += cur_len + 4;
            posIdx += cur_len + 4;
        }

        return std::move(ret);
    }

public:
    vector<tuple<string, string>> getInfo() const {
        vector<tuple<string, string>> ret;

        ret.emplace_back("tokenizedStrings.tokenIdx", std::to_string(n_tokenIdx));
        ret.emplace_back("tokenizedStrings.stringBufferCapacity", std::to_string(stringBufferCapacity));
        ret.emplace_back("tokenizedStrings.growth_string_buffer_threshold", std::to_string(growth_string_buffer_threshold));
        ret.emplace_back("tokenizedStrings.n_sizeSoFar", std::to_string(n_sizeSoFar));
        ret.emplace_back("tokenizedStrings.n_numStringSoFar", std::to_string(n_numStringSoFar));
        ret.emplace_back("tokenizedStrings.s_active_get", std::to_string(s_active_get));
        ret.emplace_back("tokenizedStrings.s_active_commands", std::to_string(s_active_commands));
        ret.emplace_back("tokenizedStrings.s_growth_decider", std::to_string(s_growth_decider));

        ret.emplace_back("tokenizedStrings.c_collisions", std::to_string(c_collisions));
        ret.emplace_back("tokenizedStrings.c_growths_string_buffer", std::to_string(c_growths_string_buffer));
        ret.emplace_back("tokenizedStrings.c_growths_token_info", std::to_string(c_growths_token_info));
        ret.emplace_back("tokenizedStrings.c_trapped", std::to_string(c_trapped));

        return std::move(ret);
    }
};

#undef WAIT_FOR_GETS_ENABLED
#undef WAIT_FOR_ZERO_GETS
#undef DISABLE_GETS
#undef ENABLE_GETS

} // namespace memurai::sql
