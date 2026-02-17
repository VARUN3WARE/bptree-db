/// @file shell.cpp
/// @brief Interactive CLI shell for the B+ tree storage engine.

#include "bptree/bplus_tree.h"

#include <iostream>
#include <limits>
#include <string>
#include <vector>

using namespace bptree;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static void ClearInput() {
    std::cin.clear();
    std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
}

static void PrintBanner() {
    std::cout << R"(
 ┌──────────────────────────────────────┐
 │   B+ Tree Storage Engine — Shell     │
 └──────────────────────────────────────┘
)" << std::flush;
}

static void PrintMenu() {
    std::cout << "\n"
        "  [1] Insert / Update       [5] Bulk Insert\n"
        "  [2] Search by Key         [6] Display Records\n"
        "  [3] Range Query           [7] Statistics\n"
        "  [4] Delete                [8] Checkpoint\n"
        "                            [0] Exit\n"
        "\n  > ";
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

static void CmdInsert(BPlusTree& tree) {
    std::cout << "\n  key (int): ";
    int key;
    if (!(std::cin >> key)) { ClearInput(); std::cout << "  ✗ invalid key\n"; return; }
    ClearInput();

    std::cout << "  data (max 99 chars): ";
    std::string data;
    std::getline(std::cin, data);
    if (data.empty()) { std::cout << "  ✗ data cannot be empty\n"; return; }

    Status s = tree.Insert(key, data.c_str());
    if (s.ok()) std::cout << "  ✓ key " << key << " written\n";
    else        std::cout << "  ✗ " << s.ToString() << "\n";
}

static void CmdSearch(BPlusTree& tree) {
    std::cout << "\n  key: ";
    int key;
    if (!(std::cin >> key)) { ClearInput(); std::cout << "  ✗ invalid key\n"; return; }
    ClearInput();

    std::string val;
    Status s = tree.Search(key, val);
    if (s.ok())           std::cout << "  → " << val << "\n";
    else if (s.IsNotFound()) std::cout << "  (not found)\n";
    else                     std::cout << "  ✗ " << s.ToString() << "\n";
}

static void CmdRangeQuery(BPlusTree& tree) {
    std::cout << "\n  lower bound: ";
    int lo; if (!(std::cin >> lo)) { ClearInput(); return; }
    std::cout << "  upper bound: ";
    int hi; if (!(std::cin >> hi)) { ClearInput(); return; }
    ClearInput();

    std::vector<std::pair<key_t, std::string>> results;
    Status s = tree.RangeQuery(lo, hi, results);
    if (!s.ok()) { std::cout << "  ✗ " << s.ToString() << "\n"; return; }

    std::cout << "  " << results.size() << " record(s) in ["
              << lo << ", " << hi << "]:\n";
    const int kLimit = 50;
    int shown = 0;
    for (auto& [k, v] : results) {
        if (shown >= kLimit) {
            std::cout << "  ... (" << results.size() - kLimit << " more)\n";
            break;
        }
        std::cout << "    [" << k << "] " << v << "\n";
        ++shown;
    }
}

static void CmdDelete(BPlusTree& tree) {
    std::cout << "\n  key to delete: ";
    int key;
    if (!(std::cin >> key)) { ClearInput(); return; }
    ClearInput();

    // Show existing record first.
    std::string val;
    if (tree.Search(key, val).ok()) {
        std::cout << "  current value: " << val << "\n";
        std::cout << "  confirm delete? (y/n): ";
        char c; std::cin >> c; ClearInput();
        if (c != 'y' && c != 'Y') { std::cout << "  cancelled\n"; return; }
    }

    Status s = tree.Delete(key);
    if (s.ok())              std::cout << "  ✓ deleted\n";
    else if (s.IsNotFound()) std::cout << "  (not found)\n";
    else                     std::cout << "  ✗ " << s.ToString() << "\n";
}

static void CmdBulkInsert(BPlusTree& tree) {
    int start, count;
    std::cout << "\n  starting key: ";
    if (!(std::cin >> start)) { ClearInput(); return; }
    std::cout << "  count: ";
    if (!(std::cin >> count) || count <= 0) { ClearInput(); return; }
    ClearInput();

    std::cout << "  data pattern (%d = key): ";
    std::string pattern;
    std::getline(std::cin, pattern);
    if (pattern.empty()) pattern = "record_%d";

    int ok = 0;
    for (int i = 0; i < count; ++i) {
        char buf[DATA_SIZE]{};
        std::snprintf(buf, DATA_SIZE, pattern.c_str(), start + i);
        if (tree.Insert(start + i, buf).ok()) ++ok;
        if ((i + 1) % 1000 == 0)
            std::cout << "    " << (i + 1) << " / " << count << "\r" << std::flush;
    }
    std::cout << "  ✓ inserted " << ok << " / " << count << " records\n";
}

static void CmdDisplay(BPlusTree& tree) {
    int lo, hi;
    std::cout << "\n  lower bound (-999999 for all): ";
    if (!(std::cin >> lo)) { ClearInput(); return; }
    std::cout << "  upper bound ( 999999 for all): ";
    if (!(std::cin >> hi)) { ClearInput(); return; }
    ClearInput();

    std::vector<std::pair<key_t, std::string>> results;
    tree.RangeQuery(lo, hi, results);

    std::cout << "  " << results.size() << " record(s):\n";
    for (auto& [k, v] : results) {
        std::cout << "    [" << k << "] " << v << "\n";
    }
}

static void CmdStats(BPlusTree& tree) {
    std::vector<std::pair<key_t, std::string>> all;
    tree.RangeQuery(-999999, 999999, all);

    std::cout << "\n  records:           " << all.size()
              << "\n  index file:        " << tree.FilePath()
              << "\n  page size:         " << PAGE_SIZE << " B"
              << "\n  data size:         " << DATA_SIZE << " B"
              << "\n  leaf capacity:     " << LEAF_MAX_KEYS
              << "\n  internal capacity: " << INTERNAL_MAX_KEYS
              << "\n  buffer pool hits:  " << tree.BufferPoolHits()
              << "\n  buffer pool miss:  " << tree.BufferPoolMisses()
              << "\n  buffer pool rate:  " << (tree.BufferPoolHitRate() * 100) << "%"
              << "\n  WAL enabled:       " << (tree.WALEnabled() ? "yes" : "no")
              << "\n  WAL bytes written: " << tree.WALBytesWritten()
              << "\n  WAL records:       " << tree.WALRecordsWritten()
              << "\n";
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main() {
    PrintBanner();

    BPlusTree tree;
    std::cout << "  Index file: " << tree.FilePath() << "\n";

    bool running = true;
    while (running) {
        PrintMenu();

        int choice;
        if (!(std::cin >> choice)) { ClearInput(); continue; }
        ClearInput();

        switch (choice) {
            case 1: CmdInsert(tree);      break;
            case 2: CmdSearch(tree);       break;
            case 3: CmdRangeQuery(tree);   break;
            case 4: CmdDelete(tree);       break;
            case 5: CmdBulkInsert(tree);   break;
            case 6: CmdDisplay(tree);      break;
            case 7: CmdStats(tree);        break;
            case 8:
                tree.Checkpoint();
                std::cout << "  \xe2\x9c\x93 checkpoint complete\n";
                break;
            case 0:
                std::cout << "\n  Closing B+ tree and flushing to disk...\n";
                running = false;
                break;
            default:
                std::cout << "  unknown option\n";
        }
    }

    return 0;
}
