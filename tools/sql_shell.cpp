/// @file sql_shell.cpp
/// @brief Interactive SQL shell on top of the B+ tree engine.
///        Type SQL, press Enter, watch it work. No GraphQL here :)

#include "bptree/sql/executor.h"
#include "bptree/sql/row.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <string>
#include <vector>

using namespace bptree::sql;

// ---------------------------------------------------------------------------
// ASCII table printer
// ---------------------------------------------------------------------------

static void print_result(const ResultSet& rs) {
    if (rs.columns.empty()) return;

    // Compute column widths
    std::vector<size_t> widths;
    for (auto& h : rs.columns) widths.push_back(h.size());

    std::vector<std::vector<std::string>> cells;
    for (auto& row : rs.rows) {
        std::vector<std::string> cell_row;
        for (size_t i = 0; i < row.size(); ++i) {
            std::string s = value_to_string(row[i]);
            widths[i] = std::max(widths[i], s.size());
            cell_row.push_back(std::move(s));
        }
        cells.push_back(std::move(cell_row));
    }

    // Separator line
    auto sep = [&]() {
        std::cout << "+";
        for (size_t w : widths) std::cout << std::string(w + 2, '-') << "+";
        std::cout << "\n";
    };

    sep();
    // Header
    std::cout << "|";
    for (size_t i = 0; i < rs.columns.size(); ++i)
        std::cout << " " << std::setw(static_cast<int>(widths[i]))
                  << std::left << rs.columns[i] << " |";
    std::cout << "\n";
    sep();

    // Rows
    for (auto& cell_row : cells) {
        std::cout << "|";
        for (size_t i = 0; i < cell_row.size(); ++i)
            std::cout << " " << std::setw(static_cast<int>(widths[i]))
                      << std::left << cell_row[i] << " |";
        std::cout << "\n";
    }
    sep();

    std::cout << rs.rows.size() << " row(s)\n";
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main() {
    std::cout << "bptree-sql  (type 'exit' or Ctrl-D to quit)\n";

    Executor exec;
    std::string line;

    while (true) {
        std::cout << "sql> " << std::flush;
        if (!std::getline(std::cin, line)) break;  // EOF

        // Trim
        size_t start = line.find_first_not_of(" \t\r\n");
        if (start == std::string::npos) continue;
        line = line.substr(start);

        if (line == "exit" || line == "\\q") break;
        if (line.empty() || line[0] == '-') continue;

        try {
            ResultSet rs = exec.ExecSQL(line);
            print_result(rs);
        } catch (const std::exception& ex) {
            std::cerr << "ERROR: " << ex.what() << "\n";
        }
    }

    std::cout << "\nBye.\n";
    return 0;
}
