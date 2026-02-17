add_executable(shell shell.cpp)
target_link_libraries(shell PRIVATE bptree)

add_executable(bench bench.cpp)
target_link_libraries(bench PRIVATE bptree)
