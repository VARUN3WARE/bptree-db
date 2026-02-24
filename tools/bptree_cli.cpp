/// @file bptree_cli.cpp
/// @brief Interactive SQL client over TCP.
///        Usage: bptree_cli [--host H] [--port N]
///        Parses CSV responses into a pretty ASCII table -- same shell, different side. :)

#include "bptree/net/client.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

using namespace bptree::net;

// ---------------------------------------------------------------------------
// CSV parser (single-line simplified)
// ---------------------------------------------------------------------------

static std::vector<std::string> split_csv_line(const std::string& line) {
    std::vector<std::string> cols;
    std::string cur;
    bool in_quote = false;
    for (char c : line) {
        if (c == '"') { in_quote = !in_quote; }
        else if (c == ',' && !in_quote) { cols.push_back(cur); cur.clear(); }
        else cur += c;
    }
    cols.push_back(cur);
    return cols;
}

// ---------------------------------------------------------------------------
// ASCII table printer
// ---------------------------------------------------------------------------

static void print_table(const std::string& csv) {
    if (csv.empty()) return;
    std::vector<std::vector<std::string>> rows;
    std::istringstream ss(csv);
    std::string line;
    while (std::getline(ss, line)) {
        if (!line.empty()) rows.push_back(split_csv_line(line));
    }
    if (rows.empty()) return;

    size_t ncols = rows[0].size();
    std::vector<size_t> widths(ncols, 0);
    for (auto& r : rows)
        for (size_t i = 0; i < std::min(r.size(), ncols); ++i)
            widths[i] = std::max(widths[i], r[i].size());

    auto sep = [&]() {
        std::cout << "+";
        for (size_t w : widths) std::cout << std::string(w + 2, '-') << "+";
        std::cout << "\n";
    };

    sep();
    for (size_t ri = 0; ri < rows.size(); ++ri) {
        std::cout << "|";
        for (size_t i = 0; i < ncols; ++i)
            std::cout << " " << std::setw(static_cast<int>(widths[i]))
                      << std::left << (i < rows[ri].size() ? rows[ri][i] : "") << " |";
        std::cout << "\n";
        if (ri == 0) sep();
    }
    sep();
    std::cout << (rows.size() > 1 ? rows.size() - 1 : 0) << " row(s)\n";
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
    std::string host = "127.0.0.1";
    int         port = 5432;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--host" && i + 1 < argc) host = argv[++i];
        if (arg == "--port" && i + 1 < argc) port = std::stoi(argv[++i]);
    }

    Client client;
    try {
        client.Connect(host, port);
    } catch (const std::exception& ex) {
        std::cerr << "Connection failed: " << ex.what() << "\n";
        return 1;
    }

    std::cout << "Connected to " << host << ":" << port
              << "  (type 'exit' to quit)\n";

    std::string line;
    while (true) {
        std::cout << "bptree> " << std::flush;
        if (!std::getline(std::cin, line)) break;

        size_t s = line.find_first_not_of(" \t\r\n");
        if (s == std::string::npos) continue;
        line = line.substr(s);
        if (line == "exit" || line == "\\q") break;
        if (line.empty()) continue;

        try {
            std::string resp = client.Query(line);
            // Heuristic: if response contains newlines it's CSV data, else status text.
            if (resp.find('\n') != std::string::npos) print_table(resp);
            else std::cout << resp << "\n";
        } catch (const std::exception& ex) {
            std::cerr << "ERROR: " << ex.what() << "\n";
        }
    }

    client.Disconnect();
    std::cout << "\nBye.\n";
    return 0;
}
