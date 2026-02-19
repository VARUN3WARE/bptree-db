#pragma once

/// @file visualizer.h
/// @brief Tree visualization utilities for generating DOT/Graphviz output.

#include "config.h"
#include "page.h"
#include <string>
#include <sstream>
#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>

namespace bptree {

// Forward declaration
class BPlusTree;

/// Visualizer for B+ tree structure.
///
/// Generates DOT format output that can be rendered with Graphviz:
/// @code
///   BPlusTree tree("my_index.idx");
///   // ... insert some data ...
///   TreeVisualizer viz(tree);
///   viz.GenerateDOT("tree.dot");
///   // Then: dot -Tpng tree.dot -o tree.png
/// @endcode
class TreeVisualizer {
public:
    explicit TreeVisualizer(const BPlusTree& tree);

    /// Generate DOT format representation of the tree.
    /// @param output_path  Path to write .dot file (optional, returns string if empty)
    /// @return DOT format string
    std::string GenerateDOT(const std::string& output_path = "");

    /// Generate SVG directly (requires graphviz installed)
    /// @param output_path  Path to write .svg file
    /// @return true on success
    bool GenerateSVG(const std::string& output_path);

    /// Print ASCII art tree structure to console
    void PrintASCII(std::ostream& os = std::cout) const;

private:
    const BPlusTree& tree_;
    
    // Internal helpers for DOT generation
    void GenerateDOTRecursive(std::ostringstream& out, int64_t node_offset, 
                              int& node_id_counter,
                              std::unordered_map<int64_t, int>& offset_to_id) const;
    
    std::string GetNodeLabel(const char* node_data, bool is_leaf, int num_keys) const;
    
    // ASCII tree helpers
    void PrintASCIIRecursive(std::ostream& os, int64_t node_offset, 
                             const std::string& prefix, bool is_tail) const;
};

} // namespace bptree
