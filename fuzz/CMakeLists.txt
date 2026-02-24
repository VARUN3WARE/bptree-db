# -------------------------------------------------------------------
# libFuzzer target for SQL layer
# -------------------------------------------------------------------
add_executable(sql_fuzzer
    sql_fuzzer.cpp
)

target_link_libraries(sql_fuzzer PRIVATE bptree_sql)

# Enable coverage and libFuzzer
target_compile_options(sql_fuzzer PRIVATE -fsanitize=fuzzer,address)
target_link_options(sql_fuzzer PRIVATE -fsanitize=fuzzer,address)
