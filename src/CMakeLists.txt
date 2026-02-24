add_library(bptree
    disk_manager.cpp
    buffer_pool.cpp
    wal.cpp
    bplus_tree.cpp
    visualizer.cpp
)

target_include_directories(bptree
    PUBLIC  ${PROJECT_SOURCE_DIR}/include
)

target_compile_features(bptree PUBLIC cxx_std_17)

# -------------------------------------------------------------------
# SQL library — lexer, parser, AST, catalog, row, executor
# -------------------------------------------------------------------
add_library(bptree_sql
    sql/ast.cpp
    sql/lexer.cpp
    sql/parser.cpp
    sql/catalog.cpp
    sql/row.cpp
    sql/executor.cpp
)

target_include_directories(bptree_sql
    PUBLIC  ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(bptree_sql PUBLIC bptree)
target_compile_features(bptree_sql PUBLIC cxx_std_17)

# -------------------------------------------------------------------
# Networking library — protocol, server, client
# -------------------------------------------------------------------
add_library(bptree_net
    net/message.cpp
    net/server.cpp
    net/client.cpp
)

target_include_directories(bptree_net
    PUBLIC  ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(bptree_net PUBLIC bptree_sql Threads::Threads)
target_compile_features(bptree_net PUBLIC cxx_std_17)
