
## CPack definitions to create a Debian package
set(CPACK_GENERATOR "DEB")

# these are cache variables, so they could be overwritten with -D,
set(CPACK_PACKAGE_NAME ${PROJECT_NAME}
    CACHE STRING "The resulting package name"
)

# which is useful in case of packing only selected components instead of the whole thing
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Memurai SQL is the indexing and querying tool for Memurai and Redis"
        CACHE STRING "Package description for the package metadata"
)
set(CPACK_PACKAGE_VENDOR "Memurai")

set(CPACK_DEBIAN_PACKAGE_MAINTAINER "memurai-sql <memurai-sql@memurai.com>")

# autogenerate dependency information
set (CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)

set(CPACK_PACKAGE_VERSION_MAJOR ${PROJECT_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PROJECT_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${PROJECT_VERSION_PATCH})

# package name for deb to get _amd64.deb
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
