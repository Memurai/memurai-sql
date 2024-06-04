/*
 * Copyright (c) 2020 - 2021 Janea Systems
   by Benedetto Proietti

   Thanks to Andrei Alexandrescu for thinking about this. (CppCon 2015)
 */

#pragma once

#include <sstream>

#include "exception.h"

#define EXPECT_OR_THROW(COND, TYPE, MSG)                        \
    if (!(COND)) {                                              \
        std::stringstream ss____xx_;                            \
        ss____xx_ << MSG;                                       \
        throw MEMURAI_EXCEPTION(TYPE, ss____xx_.str().c_str()); \
    }

#define EXPECT_OR_RETURN(COND, RET) \
    if (!(COND)) {                  \
        return RET;                 \
    }
