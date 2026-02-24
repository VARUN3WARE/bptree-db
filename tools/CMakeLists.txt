add_executable(shell shell.cpp)
target_link_libraries(shell PRIVATE bptree)

add_executable(bench bench.cpp)
target_link_libraries(bench PRIVATE bptree)

add_executable(visualize visualize.cpp)
target_link_libraries(visualize PRIVATE bptree)

# -------------------------------------------------------------------
# SQL interactive shell
# -------------------------------------------------------------------
add_executable(sql_shell sql_shell.cpp)
target_link_libraries(sql_shell PRIVATE bptree_sql)

# -------------------------------------------------------------------
# TCP server and CLI client
# -------------------------------------------------------------------
add_executable(bptree_server bptree_server.cpp)
target_link_libraries(bptree_server PRIVATE bptree_net)

add_executable(bptree_cli bptree_cli.cpp)
target_link_libraries(bptree_cli PRIVATE bptree_net)
