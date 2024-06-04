// Defines types and other macros so that redis can still compile on original platforms and _WIN32.
#ifndef XPLAT_PORTABILITY_FIXES_H
#define XPLAT_PORTABILITY_FIXES_H

// Suppress 'warning Cxxx: conversion from XXX to YYY possible loss of data'
#ifdef _WIN32
#pragma warning (disable : 4267 4244)
#endif

#ifdef __cplusplus
extern "C"
{
#endif

/*  Sometimes in the Windows port we make changes from:
        antirez_redis_statement();
    to:
        #ifdef _WIN32
            windows_redis_statement();
        #else
            antirez_redis_statement();
        #endif

    If subsequently antirez changed that code, we might not detect the change during the next merge.
    The INDUCE_MERGE_CONFLICT macro expands to nothing, but it is used to make sure that the original line
    is modified with respect to the antirez version, so that any subsequent modifications will trigger a conflict
    during the next merge.

    Sample usage:
        #ifdef _WIN32
            windows_redis_statement();
        #else
            antirez_redis_statement();          INDUCE_MERGE_CONFLICT
        #endif

    Don't use any parenthesis or semi-colon after INDUCE_MERGE_CONFLICT.
    Use it at the end of a line to preserve the original indentation.
*/
#define INDUCE_MERGE_CONFLICT

/*  Use WIN_PORT_FIX at the end of a line to mark places where we make changes to the code
    without using #ifdefs. Useful to keep the code more legible. Mainly intended for replacing
    the use of long (which is 64-bit on 64-bit Unix and 32-bit on 64-bit Windows) to portable types.
    In order to be eligible for an inline fix (without #ifdef), the change should be portable back to the Posix version.
*/
#define WIN_PORT_FIX

#ifdef _WIN32
#define IF_WIN32(x, y) x
#define WIN32_ONLY(x) x
#define POSIX_ONLY(x)
#define DISABLE(x)
#else
#define IF_WIN32(x, y) y
#define WIN32_ONLY(x)
#define POSIX_ONLY(x) x
#endif

#ifdef __cplusplus
}
#endif


/* On 64-bit *nix and Windows use different data type models: LP64 and LLP64 respectively.
 * The main difference is that 'long' is 64-bit on 64-bit *nix and 32-bit on 64-bit Windows.
 * The Posix version of Redis makes many assumptions about long being 64-bit and the same size
 * as pointers.
 * To deal with this issue, we replace all occurrences of 'long' in antirez code with our own typedefs,
 * and make those definitions 64-bit to match antirez' assumptions.
 * This enables us to have merge check script to verify that no new instances of 'long' go unnoticed.
*/
#ifdef _WIN32
#define PORT_LONGLONG     __int64
#define PORT_ULONGLONG    unsigned __int64
#define PORT_LONGDOUBLE   double

#ifdef _WIN64
#define ssize_t           __int64
#define PORT_LONG         __int64
#define PORT_ULONG        unsigned __int64
#else
#define ssize_t           long
#define PORT_LONG         long
#define PORT_ULONG        unsigned long
#endif

#ifdef _WIN64
#define PORT_LONG_MAX     _I64_MAX
#define PORT_LONG_MIN     _I64_MIN
#define PORT_ULONG_MAX    _UI64_MAX
#else
#define PORT_LONG_MAX     LONG_MAX
#define PORT_LONG_MIN     LONG_MIN
#define PORT_ULONG_MAX    ULONG_MAX
#endif

/* The maximum possible size_t value has all bits set */
#define MAX_SIZE_T        (~(size_t)0)

typedef int               pid_t;

#ifndef mode_t
#define mode_t            unsigned __int32
#endif

/* sha1 */
#ifndef u_int32_t
typedef unsigned __int32  u_int32_t;
#endif

#else // _WIN32

#define PORT_LONGLONG     long long
#define PORT_ULONGLONG    unsigned long long
#define PORT_LONGDOUBLE   long double
#define PORT_LONG         long
#define PORT_ULONG        unsigned long
#define PORT_LONG_MAX     LONG_MAX
#define PORT_LONG_MIN     LONG_MIN
#define PORT_ULONG_MAX    ULONG_MAX

#define POSIX_BUILD
#endif // _WIN32

#endif // XPLAT_PORTABILITY_FIXES_H