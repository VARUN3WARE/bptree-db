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
