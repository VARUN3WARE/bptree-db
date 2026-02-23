/// @file visualize.cpp
/// @brief Standalone tool to visualize B+ tree structure.

#include "bptree/bplus_tree.h"
#include "bptree/visualizer.h"

#include <iostream>
#include <cstring>

using namespace bptree;

static void PrintUsage(const char* prog) {
    std::cout << "Usage: " << prog << " <index_file> [options]\n"
              << "\n"
              << "Options:\n"
              << "  --dot <file>    Generate DOT file\n"
              << "  --svg <file>    Generate SVG file (requires graphviz)\n"
              << "  --ascii         Print ASCII tree to console\n"
              << "  --all           Generate all formats (default)\n"
              << "\n"
              << "Example:\n"
              << "  " << prog << " bptree.idx --svg tree.svg\n"
              << "  " << prog << " bptree.idx --ascii\n"
              << "\n";
}

int main(int argc, char** argv) {
    if (argc < 2) {
        PrintUsage(argv[0]);
        return 1;
    }
    
    std::string index_file = argv[1];
    
    // Parse options
    bool gen_dot = false;
    bool gen_svg = false;
    bool gen_ascii = false;
    std::string dot_file;
    std::string svg_file;
    
    for (int i = 2; i < argc; ++i) {
        if (std::strcmp(argv[i], "--dot") == 0 && i + 1 < argc) {
            gen_dot = true;
            dot_file = argv[++i];
        } else if (std::strcmp(argv[i], "--svg") == 0 && i + 1 < argc) {
            gen_svg = true;
            svg_file = argv[++i];
        } else if (std::strcmp(argv[i], "--ascii") == 0) {
            gen_ascii = true;
        } else if (std::strcmp(argv[i], "--all") == 0) {
            gen_dot = gen_svg = gen_ascii = true;
            dot_file = "tree.dot";
            svg_file = "tree.svg";
        }
    }
    
    // Default: ASCII output
    if (!gen_dot && !gen_svg && !gen_ascii) {
        gen_ascii = true;
    }
    
    try {
        // Open tree (read-only, no WAL needed for visualization)
        BPlusTree tree(index_file, DEFAULT_POOL_SIZE, false);
        TreeVisualizer viz(tree);
        
        std::cout << "Visualizing B+ tree: " << index_file << "\n";
        std::cout << "──────────────────────────────────────\n\n";
        
        if (gen_ascii) {
            viz.PrintASCII(std::cout);
            std::cout << "\n";
        }
        
        if (gen_dot) {
            viz.GenerateDOT(dot_file);
            std::cout << "✓ DOT file written to: " << dot_file << "\n";
            std::cout << "  Render with: dot -Tpng " << dot_file << " -o tree.png\n\n";
        }
        
        if (gen_svg) {
            if (viz.GenerateSVG(svg_file)) {
                std::cout << "✓ SVG file written to: " << svg_file << "\n\n";
            } else {
                std::cerr << "✗ Failed to generate SVG (is graphviz installed?)\n\n";
            }
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    
    return 0;
}
