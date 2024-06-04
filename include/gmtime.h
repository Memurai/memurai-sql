/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

#ifdef _MSC_VER

#define GMTIME(A, B) _gmtime64_s(A, B)

#else

#define GMTIME(A, B)                   \
    [&]() {                            \
        errno = 0;                     \
        gmtime_r(B, A);                \
        return errno == 0 ? 0 : errno; \
    }()

#endif
