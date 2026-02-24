/// @file sql_fuzzer.cpp
/// @brief libFuzzer target for the SQL layer.
///        Throws random bytes at the parser and executor to catch crashes.

#include "bptree/sql/executor.h"

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>

using namespace bptree::sql;
namespace fs = std::filesystem;

static Executor* g_exec = nullptr;

extern "C" int LLVMFuzzerInitialize(int* /*argc*/, char*** /*argv*/) {
    std::string dir = fs::temp_directory_path() / "bptree_fuzz";
    fs::create_directories(dir);
    g_exec = new Executor(dir + "/catalog.dat", dir);
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0) return 0;
    std::string sql(reinterpret_cast<const char*>(data), size);

    try {
        g_exec->ExecSQL(sql);
    } catch (const std::exception&) {
        // We only care about unhandled crashes/segfaults/UB.
        // Expected parser or execution exceptions are normal here.
    }

    return 0;
}
