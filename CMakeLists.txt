cmake_minimum_required(VERSION 3.14)
project(bptree-db
    VERSION 1.0.0
    DESCRIPTION "A high-performance disk-based B+ tree storage engine"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Export compile_commands.json for IDE support
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Compiler warnings
add_compile_options(-Wall -Wextra -Wpedantic)

# Release build by default
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-O3 -march=native)
endif()

# Core library
add_subdirectory(src)

# Tools
add_subdirectory(tools)

# Tests
option(BPTREE_BUILD_TESTS "Build unit tests" ON)
if(BPTREE_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
