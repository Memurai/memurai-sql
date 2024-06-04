cmake_minimum_required(VERSION 3.11)

if (NOT EXISTS ${CMAKE_BINARY_DIR}/CMakeCache.txt)
  if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "" FORCE)
  endif()
endif()

## Get tag version from git
include(cmake/GetGitRevisionDescription.cmake)
git_describe(VERSION --tags --always)

if(VERSION MATCHES "^v[0-9]+\\.[0-9]+\\.[0-9]+(.*)")
    #parse the version information into pieces.
    string(REGEX REPLACE "^v([0-9]+)\\..*" "\\1" VERSION_MAJOR "${VERSION}")
    string(REGEX REPLACE "^v[0-9]+\\.([0-9]+).*" "\\1" VERSION_MINOR "${VERSION}")
    string(REGEX REPLACE "^v[0-9]+\\.[0-9]+\\.([0-9]+).*" "\\1" VERSION_PATCH "${VERSION}")
    string(REGEX REPLACE "^v[0-9]+\\.[0-9]+\\.[0-9]+(.*)" "\\1" VERSION_SHA1 "${VERSION}")

    set(VERSION_SHORT "${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}")
else()
    set(VERSION_SHORT "0.0.0") # Default when no tag is available
endif()

configure_file(cmake/version.cpp.in
        ${CMAKE_CURRENT_BINARY_DIR}/version.cpp)
set(VERSION_FILE "${CMAKE_CURRENT_BINARY_DIR}/version.cpp")

## Memurai SQL project
project(MSQL VERSION ${VERSION_SHORT})

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED YES)
set(CMAKE_CXX_EXTENSIONS OFF)

if(WIN32)
    set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS 1)
endif(WIN32)

# Testing
enable_testing()

add_subdirectory("multicore")
add_subdirectory("datacreator/datacreator")
add_subdirectory("sql-core")
add_subdirectory("sql-fe")
add_subdirectory("unit-tests")
add_subdirectory("system-tests")
add_subdirectory("deps")

# Set default prefix for redis modules
if (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    set (CMAKE_INSTALL_PREFIX "/usr"
            CACHE PATH "default install path" FORCE)
endif()

# Packaging
include(cmake/PackagingDeb.cmake)
include(CPack)
