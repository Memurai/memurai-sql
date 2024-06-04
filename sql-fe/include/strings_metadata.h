/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

#include <mutex>
#include <optional>
#include <string>

#include "scope/expect.h"
#include "types.h"

namespace memurai::sql {

using std::string;

class Test_TokenizedStrings;

//  Poor's man Bloom Filter
//  Plus string size
//
class Metadata {
    static constexpr uint64 mask32bitLO = 0xFFFFFFFF;
    static constexpr uint64 mask32bitHI = 0xFFFFFFFF00000000;

    uint64 meta;

    /*
    struct
    {
        unsigned size               : 32;
        unsigned num_digits         : 4;
        unsigned num_bit3set        : 4;
        unsigned num_diff_chars     : 8;
        unsigned num_diff_chars     : 8;
        unsigned num_spaces_class   : 4;
        unsigned num_commas_class   : 4;
        unsigned has_alpha          : 1;
        unsigned has_numeric        : 1;
        unsigned contains_curly     : 1;
        unsigned contains_single_quotes: 1;
        unsigned contains_double_quotes : 1;
    };
    */

    static constexpr byte SIZE_OFFSET = 32;
    static constexpr uint32 MAXIMUM_DIGITS_COUNT = 15;
    static constexpr uint32 MAXIMUM_SPACES_COUNT = 15;
    static constexpr uint32 MAXIMUM_COMMAS_COUNT = 15;
    static constexpr uint32 MAXIMUM_BIT3SET_COUNT = 15;

    static constexpr byte NUM_DIGITS_OFFSET = 0;
    static constexpr byte NUM_BIT3_OFFSET = 4;
    static constexpr byte NUM_DIFF_CHARS_OFFSET = 8;
    static constexpr byte NUM_SPACES_CLASS_OFFSET = 16;
    static constexpr byte NUM_COMMAS_CLASS_OFFSET = 20;
    static constexpr byte HAS_ALPHA_OFFSET = 24;
    static constexpr byte HAS_NUMERIC_OFFSET = 25;
    static constexpr byte CONTAINS_CURLY_OFFSET = 26;
    static constexpr byte CONTAINS_SINGLE_QUOTES_OFFSET = 27;
    static constexpr byte CONTAINS_DOUBLE_QUOTES_OFFSET = 28;

    static constexpr uint64 XLSTRING_MINIMUM_SIZE = 4096;

    // high 32bit: size
    // low 32bit: metadata

public:
    friend class Test_StringsMetadata;

    // ctor
    explicit Metadata(const string& str) : meta(0) {
        compute(str.c_str(), str.length());
    }

    // ctor
    Metadata(cchar* str, cuint64 len) : meta(0) {
        compute(str, len);
    }

    explicit Metadata(uint64 m) : meta(m) {}

    [[nodiscard]] uint32 getSize() const {
        return (meta >> SIZE_OFFSET);
    }

    [[nodiscard]] uint32 getNumDigits() const {
        return (((meta & mask32bitLO) >> NUM_DIGITS_OFFSET) & 0xF);
    }

    [[nodiscard]] uint32 getNumBit3sets() const {
        return (((meta & mask32bitLO) >> NUM_BIT3_OFFSET) & 0xF);
    }

    [[nodiscard]] uint32 getDiffCharsClass() const {
        return (((meta & mask32bitLO) >> NUM_DIFF_CHARS_OFFSET) & 0xFF);
    }

    [[nodiscard]] uint32 getNumSpacesClass() const {
        return (((meta & mask32bitLO) >> NUM_SPACES_CLASS_OFFSET) & 0xF);
    }

    [[nodiscard]] uint32 getNumCommasClass() const {
        return (((meta & mask32bitLO) >> NUM_COMMAS_CLASS_OFFSET) & 0xF);
    }

    [[nodiscard]] bool getHasAlpha() const {
        return (((meta & mask32bitLO) >> HAS_ALPHA_OFFSET) & 0x1) == 1;
    }

    [[nodiscard]] bool getHasNumeric() const {
        return (((meta & mask32bitLO) >> HAS_NUMERIC_OFFSET) & 0x1) == 1;
    }

    [[nodiscard]] bool getContainsCurly() const {
        return (((meta & mask32bitLO) >> CONTAINS_CURLY_OFFSET) & 0x1) == 1;
    }

    [[nodiscard]] bool getContainsSingleQuotes() const {
        return (((meta & mask32bitLO) >> CONTAINS_SINGLE_QUOTES_OFFSET) & 0x1) == 1;
    }

    [[nodiscard]] bool getContainsDoubleQuotes() const {
        return (((meta & mask32bitLO) >> CONTAINS_DOUBLE_QUOTES_OFFSET) & 0x1) == 1;
    }

    explicit operator uint64() const {
        return meta;
    }

    [[nodiscard]] uint64 get() const {
        return meta;
    }

#ifdef TEST_BUILD
    [[nodiscard]] string getDebugString() const {
        std::stringstream ss;

        ss << "size = " << getSize();
        ss << "num digits =" << getNumDigits();
        ss << "num bit3 set =" << getNumBit3sets();
        ss << "num diff chars =" << getDiffCharsClass();
        ss << "num spaces =" << getNumSpacesClass();
        ss << "num commas =" << getNumCommasClass();
        ss << "has alpha =" << getHasAlpha();
        ss << "has alphanum =" << getHasNumeric();
        ss << "has curly =" << getContainsCurly();
        ss << "num single quotes =" << getContainsSingleQuotes();
        ss << "num double quotes =" << getContainsDoubleQuotes();

        return ss.str();
    }
#endif

private:
    void compute(cchar* str, cuint64 len) {
        cchar* ptr = str;
        uint32 num_diffs = 0;
        uint32 num_spaces = 0;
        uint32 num_commas = 0;
        uint32 num_digits = 0;
        uint32 num_bit_3_set = 0;
        vector<bool> founds(256, false);
        bool contains_alpha = false;
        bool contains_non_alphanum = false;
        bool contains_curly = false;
        bool contains_single_quotes = false;
        bool contains_double_quotes = false;

        cuint64 to_be_analyzed = std::min(XLSTRING_MINIMUM_SIZE, len);

        for (uint64 i = 0; i < to_be_analyzed; i++) {
            const char ch = *ptr;
            const int idx = (ch >= 0) ? ch : ch + 128;

            if (!founds[idx]) {
                founds[idx] = true;
                num_diffs++;
            }
            if (ch == ' ')
                num_spaces++;
            if ((ch == '{') || (ch == '}'))
                contains_curly = true;
            if (ch == ',')
                num_commas++;
            if (ch > 0) {
                if (isalpha(ch))
                    contains_alpha = true;
                if (isdigit(ch))
                    num_digits++;
                if (!isalnum(ch))
                    contains_non_alphanum = true;
                if (ch == '\'')
                    contains_single_quotes = true;
                if (ch == '"')
                    contains_double_quotes = true;
                if ((ch & 0b100) != 0)
                    num_bit_3_set++;
            }

            ptr++;
        }

        // compute meta
        meta = 0;
        // strlen goes to upper 32bits
        meta |= len << SIZE_OFFSET;

        // Lower bits... poor's man bloom filter
        meta |= ((num_diffs > 255) ? 255 : num_diffs) << NUM_DIFF_CHARS_OFFSET;
        meta |= std::min(num_spaces, MAXIMUM_SPACES_COUNT) << NUM_SPACES_CLASS_OFFSET;
        meta |= std::min(num_commas, MAXIMUM_COMMAS_COUNT) << NUM_COMMAS_CLASS_OFFSET;
        meta |= (contains_alpha ? 1ull : 0ull) << HAS_ALPHA_OFFSET;
        meta |= (num_digits ? 1ull : 0ull) << HAS_NUMERIC_OFFSET;
        meta |= contains_curly << CONTAINS_CURLY_OFFSET;
        meta |= contains_single_quotes << CONTAINS_SINGLE_QUOTES_OFFSET;
        meta |= contains_double_quotes << CONTAINS_DOUBLE_QUOTES_OFFSET;
        meta |= std::min(num_digits, MAXIMUM_DIGITS_COUNT) << NUM_DIGITS_OFFSET;
        meta |= std::min(num_bit_3_set, MAXIMUM_BIT3SET_COUNT) << NUM_BIT3_OFFSET;
    }
};

static_assert(sizeof(Metadata) == 8, "Metadata should be 64bit in size.");

} // namespace memurai::sql
