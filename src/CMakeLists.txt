add_library(bptree
    disk_manager.cpp
    buffer_pool.cpp
    wal.cpp
    bplus_tree.cpp
)

target_include_directories(bptree
    PUBLIC  ${PROJECT_SOURCE_DIR}/include
)

target_compile_features(bptree PUBLIC cxx_std_17)
