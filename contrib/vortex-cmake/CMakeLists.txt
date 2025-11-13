# SPDX-License-Identifier: Apache-2.0
# Vortex format integration for ClickHouse

option(ENABLE_VORTEX "Enable vortex format support" ${ENABLE_LIBRARIES})

if (NOT ENABLE_VORTEX)
    message(STATUS "Not using vortex")
    return()
endif()

set(VORTEX_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/vortex/vortex-cxx")
set(VORTEX_INCLUDE_DIR "${VORTEX_SOURCE_DIR}/cpp/include")
set(VORTEX_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/vortex")

# Check if vortex-cxx directory exists
if(NOT EXISTS "${VORTEX_SOURCE_DIR}/Cargo.toml")
    message(WARNING "Vortex submodule not initialized. Run: git submodule update --init contrib/vortex")
    return()
endif()

message(STATUS "Building vortex from ${VORTEX_SOURCE_DIR}")

# Ensure Rust is enabled
if(NOT ENABLE_RUST)
    message(STATUS "Not using vortex (requires Rust)")
    return()
endif()

# Import the Rust crate using ClickHouse's wrapper function
# Use workspace root Cargo.toml but only build vortex-cxx package
clickhouse_import_crate(
    MANIFEST_PATH "${VORTEX_SOURCE_DIR}/Cargo.toml"
    PACKAGES vortex-cxx
)
clickhouse_config_crate_flags(vortex_cxx)

# Add cxxbridge for Rust-C++ interop
corrosion_add_cxxbridge(vortex_cxx_bridge CRATE vortex_cxx FILES lib.rs)

# Add include directories for cxxbridge target
target_include_directories(vortex_cxx_bridge PRIVATE ${VORTEX_INCLUDE_DIR})

# Fetch nanoarrow dependency (not available in ClickHouse)
include(FetchContent)
FetchContent_Declare(nanoarrow
    URL "https://www.apache.org/dyn/closer.lua?action=download&filename=arrow/apache-arrow-nanoarrow-0.7.0/apache-arrow-nanoarrow-0.7.0.tar.gz"
)

# Disable install rules for nanoarrow to avoid CMake export errors
set(_CMAKE_SKIP_INSTALL_RULES_SAVE "${CMAKE_SKIP_INSTALL_RULES}")
set(CMAKE_SKIP_INSTALL_RULES ON)
FetchContent_MakeAvailable(nanoarrow)
set(CMAKE_SKIP_INSTALL_RULES "${_CMAKE_SKIP_INSTALL_RULES_SAVE}")

# Get C++ source files
file(GLOB_RECURSE CPP_SOURCE_FILES CONFIGURE_DEPENDS
    "${VORTEX_SOURCE_DIR}/cpp/src/*.cpp"
)

# Create the main library combining C++ wrapper and Rust code
add_library(vortex STATIC ${CPP_SOURCE_FILES})

# Include directories
target_include_directories(vortex SYSTEM PUBLIC
    ${VORTEX_INCLUDE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}/corrosion_generated/cxxbridge/vortex_cxx_bridge/include
)
target_include_directories(vortex PRIVATE
    ${VORTEX_SOURCE_DIR}/cpp/src
)

# Link with nanoarrow and the Rust bridge
target_link_libraries(vortex
    PUBLIC nanoarrow_static
    PRIVATE vortex_cxx_bridge
)

# Mark nanoarrow's include directories as SYSTEM to suppress warnings
if (TARGET nanoarrow_static)
    get_target_property(NANOARROW_INCLUDES nanoarrow_static INTERFACE_INCLUDE_DIRECTORIES)
    if (NANOARROW_INCLUDES)
        target_include_directories(nanoarrow_static SYSTEM INTERFACE ${NANOARROW_INCLUDES})
    endif()
endif()

# Create an alias for ClickHouse's naming convention
add_library(ch_contrib::vortex ALIAS vortex)

# The library is large - avoid bloat in debug builds
if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(vortex PRIVATE -g0)
endif()

message(STATUS "Vortex library target: vortex")
message(STATUS "Vortex include dir: ${VORTEX_INCLUDE_DIR}")
