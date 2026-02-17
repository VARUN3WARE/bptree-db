include(FetchContent)

FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.14.0
)
# Prevent GTest from overriding our compiler/linker options on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include(GoogleTest)

# -------------------------------------------------------------------
# B+ Tree tests
# -------------------------------------------------------------------
add_executable(bptree_test
    bplus_tree_test.cpp
)
target_link_libraries(bptree_test PRIVATE bptree GTest::gtest_main)
gtest_discover_tests(bptree_test)

# -------------------------------------------------------------------
# Disk Manager tests
# -------------------------------------------------------------------
add_executable(disk_manager_test
    disk_manager_test.cpp
)
target_link_libraries(disk_manager_test PRIVATE bptree GTest::gtest_main)
gtest_discover_tests(disk_manager_test)

# -------------------------------------------------------------------
# Buffer Pool tests
# -------------------------------------------------------------------
add_executable(buffer_pool_test
    buffer_pool_test.cpp
)
target_link_libraries(buffer_pool_test PRIVATE bptree GTest::gtest_main)
gtest_discover_tests(buffer_pool_test)

# -------------------------------------------------------------------
# WAL tests
# -------------------------------------------------------------------
add_executable(wal_test
    wal_test.cpp
)
target_link_libraries(wal_test PRIVATE bptree GTest::gtest_main)
gtest_discover_tests(wal_test)
