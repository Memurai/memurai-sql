/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti
 */

#include <string>

#include "test/test_utils.h"
#include "types.h"

#include "scope/take_time.h"

#include "strings_metadata.h"

#undef TEST_START

#define TEST_START                                                                                               \
    g_test_name = string("StringsMetadata_") + string(__func__) + g_addendum;                                    \
    l_errors = 0;                                                                                                \
    l_warnings = 0;                                                                                              \
    LOG_VERBOSE(g_test_name << " is starting ");                                                                 \
    SCOPE_EXIT {                                                                                                 \
        LOG_VERBOSE(g_test_name << " ended with: " << l_errors << " errors and " << l_warnings << " warnings."); \
        g_addendum = "";                                                                                         \
        std::this_thread::sleep_for(std::chrono::milliseconds(200));                                             \
    };

extern string g_addendum;

using namespace std;
using namespace memurai;
using namespace memurai::multicore;
using namespace memurai::sql;

namespace memurai::sql {

class Test_StringsMetadata {

public:
#define MAKE_TEST_WITH_LENGTH_AND_EXPECTED_CLASS(LEN, EXPECTED_CLASS) make_test_with_length_and_expected_class(LEN, EXPECTED_CLASS, __LINE__)

    static void test_simple() {
        TEST_START;

        {
            Metadata one("a");
            // when unset, should be zero.
            TEST_VALUE(1, one.getSize());
            TEST_VALUE(1, one.getDiffCharsClass());
            TEST_VALUE(0, one.getNumSpacesClass());
            TEST_VALUE(0, one.getNumCommasClass());
            TEST_VALUE(true, one.getHasAlpha());
            TEST_VALUE(false, one.getHasNumeric());
            TEST_VALUE(false, one.getContainsCurly());
            TEST_VALUE(false, one.getContainsSingleQuotes());
            TEST_VALUE(false, one.getContainsDoubleQuotes());
        }

        {
            Metadata one("");
            // when unset, should be zero.
            TEST_VALUE(0, one.getSize());
            TEST_VALUE(0, one.getDiffCharsClass());
            TEST_VALUE(0, one.getNumSpacesClass());
            TEST_VALUE(0, one.getNumCommasClass());
            TEST_VALUE(false, one.getHasAlpha());
            TEST_VALUE(false, one.getHasNumeric());
            TEST_VALUE(false, one.getContainsCurly());
            TEST_VALUE(false, one.getContainsSingleQuotes());
            TEST_VALUE(false, one.getContainsDoubleQuotes());
        }

        {
            Metadata one("ab");
            // when unset, should be zero.
            TEST_VALUE(2, one.getSize());
            TEST_VALUE(2, one.getDiffCharsClass());
            TEST_VALUE(0, one.getNumSpacesClass());
            TEST_VALUE(0, one.getNumCommasClass());
            TEST_VALUE(true, one.getHasAlpha());
            TEST_VALUE(false, one.getHasNumeric());
            TEST_VALUE(false, one.getContainsCurly());
            TEST_VALUE(false, one.getContainsSingleQuotes());
            TEST_VALUE(false, one.getContainsDoubleQuotes());
        }

        {
            Metadata one("abc");
            // when unset, should be zero.
            TEST_VALUE(3, one.getSize());
            TEST_VALUE(3, one.getDiffCharsClass());
            TEST_VALUE(0, one.getNumSpacesClass());
            TEST_VALUE(0, one.getNumCommasClass());
            TEST_VALUE(true, one.getHasAlpha());
            TEST_VALUE(false, one.getHasNumeric());
            TEST_VALUE(false, one.getContainsCurly());
            TEST_VALUE(false, one.getContainsSingleQuotes());
            TEST_VALUE(false, one.getContainsDoubleQuotes());
        }

        {
            Metadata one("abb");
            // when unset, should be zero.
            TEST_VALUE(3, one.getSize());
            TEST_VALUE(2, one.getDiffCharsClass());
            TEST_VALUE(0, one.getNumSpacesClass());
            TEST_VALUE(0, one.getNumCommasClass());
            TEST_VALUE(true, one.getHasAlpha());
            TEST_VALUE(false, one.getHasNumeric());
            TEST_VALUE(false, one.getContainsCurly());
            TEST_VALUE(false, one.getContainsSingleQuotes());
            TEST_VALUE(false, one.getContainsDoubleQuotes());
        }

        {
            Metadata one("1");
            // when unset, should be zero.
            TEST_VALUE(1, one.getSize());
            TEST_VALUE(1, one.getDiffCharsClass());
            TEST_VALUE(0, one.getNumSpacesClass());
            TEST_VALUE(0, one.getNumCommasClass());
            TEST_VALUE(false, one.getHasAlpha());
            TEST_VALUE(true, one.getHasNumeric());
            TEST_VALUE(false, one.getContainsCurly());
            TEST_VALUE(false, one.getContainsSingleQuotes());
            TEST_VALUE(false, one.getContainsDoubleQuotes());
        }

        {
            Metadata one("1a");
            // when unset, should be zero.
            TEST_VALUE(2, one.getSize());
            TEST_VALUE(2, one.getDiffCharsClass());
            TEST_VALUE(0, one.getNumSpacesClass());
            TEST_VALUE(0, one.getNumCommasClass());
            TEST_VALUE(true, one.getHasAlpha());
            TEST_VALUE(true, one.getHasNumeric());
            TEST_VALUE(false, one.getContainsCurly());
            TEST_VALUE(false, one.getContainsSingleQuotes());
            TEST_VALUE(false, one.getContainsDoubleQuotes());
        }

        {
            Metadata one("ciao {");
            // when unset, should be zero.
            TEST_VALUE(6, one.getSize());
            TEST_VALUE(6, one.getDiffCharsClass());
            TEST_VALUE(1, one.getNumSpacesClass());
            TEST_VALUE(0, one.getNumCommasClass());
            TEST_VALUE(true, one.getHasAlpha());
            TEST_VALUE(false, one.getHasNumeric());
            TEST_VALUE(true, one.getContainsCurly());
            TEST_VALUE(false, one.getContainsSingleQuotes());
            TEST_VALUE(false, one.getContainsDoubleQuotes());
        }

        {
            Metadata one("ciao '");
            // when unset, should be zero.
            TEST_VALUE(6, one.getSize());
            TEST_VALUE(6, one.getDiffCharsClass());
            TEST_VALUE(1, one.getNumSpacesClass());
            TEST_VALUE(0, one.getNumCommasClass());
            TEST_VALUE(true, one.getHasAlpha());
            TEST_VALUE(false, one.getHasNumeric());
            TEST_VALUE(false, one.getContainsCurly());
            TEST_VALUE(true, one.getContainsSingleQuotes());
            TEST_VALUE(false, one.getContainsDoubleQuotes());
        }

        {
            Metadata one("ciao \"");
            // when unset, should be zero.
            TEST_VALUE(6, one.getSize());
            TEST_VALUE(6, one.getDiffCharsClass());
            TEST_VALUE(1, one.getNumSpacesClass());
            TEST_VALUE(0, one.getNumCommasClass());
            TEST_VALUE(true, one.getHasAlpha());
            TEST_VALUE(false, one.getHasNumeric());
            TEST_VALUE(false, one.getContainsCurly());
            TEST_VALUE(false, one.getContainsSingleQuotes());
            TEST_VALUE(true, one.getContainsDoubleQuotes());
        }
    }

    static void test_spaces_commas() {
        TEST_START;

        {
            const uint32 num_spaces = 14;
            string str = string("fgfgfsdfd") + string(num_spaces, ' ');
            Metadata one(str);
            // when unset, should be zero.
            TEST_VALUE(num_spaces, one.getNumSpacesClass());
            TEST_VALUE(str.length(), one.getSize());
        }
        {
            const uint32 num_spaces = 15;
            string str = string("fgfgfsdfd") + string(num_spaces, ' ');
            Metadata one(str);
            // when unset, should be zero.
            TEST_VALUE(num_spaces, one.getNumSpacesClass());
            TEST_VALUE(str.length(), one.getSize());
        }
        {
            const uint32 num_spaces = 16;
            string str = string("fgfgfsdfd") + string(num_spaces, ' ');
            Metadata one(str);
            // when unset, should be zero.
            TEST_VALUE(Metadata::MAXIMUM_SPACES_COUNT, one.getNumSpacesClass());
            TEST_VALUE(str.length(), one.getSize());
        }
        {
            const uint32 num_spaces = 32;
            string str = string("fgfgfsdfd") + string(num_spaces, ' ');
            Metadata one(str);
            // when unset, should be zero.
            TEST_VALUE(Metadata::MAXIMUM_SPACES_COUNT, one.getNumSpacesClass());
            TEST_VALUE(str.length(), one.getSize());
        }

        // now commas
        {
            const uint32 num_commas = 14;
            string str = string("fgfgfsdfd") + string(num_commas, ',');
            Metadata one(str);
            // when unset, should be zero.
            TEST_VALUE(num_commas, one.getNumCommasClass());
            TEST_VALUE(str.length(), one.getSize());
        }
        {
            const uint32 num_commas = 15;
            string str = string("fgfgfsdfd") + string(num_commas, ',');
            Metadata one(str);
            // when unset, should be zero.
            TEST_VALUE(num_commas, one.getNumCommasClass());
            TEST_VALUE(str.length(), one.getSize());
        }
        {
            const uint32 num_commas = 16;
            string str = string("fgfgfsdfd") + string(num_commas, ',');
            Metadata one(str);
            // when unset, should be zero.
            TEST_VALUE(Metadata::MAXIMUM_SPACES_COUNT, one.getNumCommasClass());
            TEST_VALUE(str.length(), one.getSize());
        }
        {
            const uint32 num_commas = 32;
            string str = string("fgfgfsdfd") + string(num_commas, ',');
            Metadata one(str);
            // when unset, should be zero.
            TEST_VALUE(Metadata::MAXIMUM_SPACES_COUNT, one.getNumCommasClass());
            TEST_VALUE(str.length(), one.getSize());
        }
    }
};

} // namespace memurai::sql

void test_string_metadata() {
    Test_StringsMetadata::test_simple();

    Test_StringsMetadata::test_spaces_commas();
}
