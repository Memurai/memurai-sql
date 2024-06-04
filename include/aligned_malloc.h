/*
* Copyright (c) 2020 - 2021 Janea Systems
by Benedetto Proietti
*/

#pragma once

#ifdef _MSC_VER

#define ALIGNED_MALLOC(ALIGNMENT, SIZE) _aligned_malloc(SIZE, ALIGNMENT)

#define ALIGNED_FREE _aligned_free

#else

#include <cstdlib>

#define ALIGNED_MALLOC(ALIGNMENT, SIZE) std::aligned_alloc(ALIGNMENT, SIZE)

#define ALIGNED_FREE std::free

#endif
