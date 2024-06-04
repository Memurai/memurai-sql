/*
 * Copyright (c) 2020, Janea Systems
   by Benedetto Proietti
 */

#pragma once

#include <string>

#include "concats.h"
#include "types.h"

namespace memurai {
using std::string;

class exception : public std::exception {
public:
    typedef uint64 exception_code;

    const string msg;
    const string filename, func;
    const int linenum;
    const exception_code code;

public:
    explicit exception(exception_code code, const char* msg, const char* filename, const char* func, int linenum)
        : msg(string(msg)
#if TEST_BUILD
              + "\n" + filename + "_" + func + "_" + std::to_string(linenum))
#else
                  )
#endif
          ,
          filename(filename),
          func(func),
          linenum(linenum),
          code(code) {
    }

    [[nodiscard]] const char* what() const noexcept override {
        return msg.c_str();
    }
};

#define MEMURAI_EXCEPTION(CODE, MSG) memurai::exception(memurai::exception::exception_code(ErrorCode COLONCOLON(CODE)), MSG, __FILE__, __func__, __LINE__)

} // namespace memurai

#undef CONCAT
