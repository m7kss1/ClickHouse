set(VORTEX_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/vortex")
set(VORTEX_FFI_SOURCE_DIR "${VORTEX_SOURCE_DIR}/vortex-ffi")

set(ENABLE_VORTEX_DEFAULT ${ENABLE_LIBRARIES})

if (NOT ENABLE_RUST
    OR (NOT OS_LINUX AND NOT OS_DARWIN)
    OR (NOT ARCH_AMD64 AND NOT ARCH_AARCH64)
    OR (SANITIZE STREQUAL "memory")
    OR NO_ARMV81_OR_HIGHER)
    message(STATUS "Environment is not compatible with Vortex. Disabling.")
    set(ENABLE_VORTEX_DEFAULT OFF)
endif()

option(ENABLE_VORTEX "Enable Vortex table engine" ${ENABLE_VORTEX_DEFAULT})

if (NOT ENABLE_VORTEX OR NOT ENABLE_RUST)
    message(STATUS "Not using Vortex")
    return()
endif()

clickhouse_import_crate(
    MANIFEST_PATH "${VORTEX_FFI_SOURCE_DIR}/Cargo.toml"
)

clickhouse_config_crate_flags(vortex_ffi)

target_include_directories(vortex_ffi
    INTERFACE "${VORTEX_FFI_SOURCE_DIR}/cinclude"
)

add_library(ch_rust::vortex ALIAS vortex_ffi)
