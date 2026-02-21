/// @file visualizer.cpp
/// @brief Implementation of tree visualization utilities.

#include "bptree/visualizer.h"
#include "bptree/bplus_tree.h"
#include "bptree/page.h"

#include <fstream>
#include <sstream>
#include <iomanip>
#include <cstdlib>

namespace bptree {

TreeVisualizer::TreeVisualizer(const BPlusTree& tree) : tree_(tree) {}

std::string TreeVisualizer::GenerateDOT(const std::string& output_path) {
    std::ostringstream out;
    
    out << "digraph BPlusTree {\n";
    out << "  node [shape=record, fontname=\"Courier\", fontsize=10];\n";
    out << "  edge [fontsize=8];\n";
    out << "  rankdir=TB;\n\n";
    
    if (tree_.IsEmpty()) {
        out << "  empty [label=\"Empty Tree\", shape=box];\n";
    } else {
        int node_id_counter = 0;
        std::unordered_map<int64_t, int> offset_to_id;
        GenerateDOTRecursive(out, tree_.root_offset_, node_id_counter, offset_to_id);
        
        // Add leaf chain edges
        out << "\n  // Leaf chain (dashed)\n";
        out << "  edge [style=dashed, color=blue, constraint=false];\n";
        
        // Find all leaf nodes and their next pointers
        for (const auto& [offset, id] : offset_to_id) {
            char* page = tree_.pool_->FetchPage(offset);
            if (page && PageIsLeaf(page)) {
                LeafPage leaf(page);
                int64_t next = leaf.NextLeaf();
                if (next != INVALID_PAGE_ID && offset_to_id.count(next)) {
                    out << "  node" << id << " -> node" << offset_to_id[next] 
                        << " [label=\"next\"];\n";
                }
                tree_.pool_->UnpinPage(offset, false);
            }
        }
    }
    
    out << "}\n";
    
    std::string dot_content = out.str();
    
    // Write to file if path is provided
    if (!output_path.empty()) {
        std::ofstream file(output_path);
        if (file) {
            file << dot_content;
            file.close();
        }
    }
    
    return dot_content;
}

void TreeVisualizer::GenerateDOTRecursive(
    std::ostringstream& out, 
    int64_t node_offset,
    int& node_id_counter,
    std::unordered_map<int64_t, int>& offset_to_id) const {
    
    if (node_offset == INVALID_PAGE_ID) return;
    
    // Pin the page
    char* page = tree_.pool_->FetchPage(node_offset);
    if (!page) return;
    
    int current_id = node_id_counter++;
    offset_to_id[node_offset] = current_id;
    
    bool is_leaf = PageIsLeaf(page);
    
    if (is_leaf) {
        LeafPage leaf(page);
        int num_keys = leaf.NumKeys();
        
        // Create label with keys
        out << "  node" << current_id << " [label=\"";
        if (num_keys == 0) {
            out << "LEAF (empty)";
        } else {
            out << "{LEAF|{";
            for (int i = 0; i < num_keys; ++i) {
                if (i > 0) out << "|";
                out << leaf.KeyAt(i);
                // Show first few chars of data
                char data[DATA_SIZE];
                leaf.GetData(i, data);
                std::string data_str(data, std::min<size_t>(10, DATA_SIZE));
                // Escape special chars
                for (char& c : data_str) {
                    if (c == '\0') break;
                    if (!std::isprint(c)) c = '?';
                }
                if (!data_str.empty()) {
                    out << "\\n" << data_str.substr(0, 8) << "...";
                }
            }
            out << "}}";
        }
        out << "\", style=filled, fillcolor=lightgreen];\n";
        
    } else {
        InternalPage internal(page);
        int num_keys = internal.NumKeys();
        
        // Create label with keys
        out << "  node" << current_id << " [label=\"";
        if (num_keys == 0) {
            out << "INTERNAL (empty)";
        } else {
            out << "{INTERNAL|{";
            for (int i = 0; i < num_keys; ++i) {
                if (i > 0) out << "|";
                out << internal.KeyAt(i);
            }
            out << "}}";
        }
        out << "\", style=filled, fillcolor=lightblue];\n";
        
        // Recursively process children
        for (int i = 0; i <= num_keys; ++i) {
            int64_t child_offset = internal.ChildAt(i);
            if (child_offset != INVALID_PAGE_ID) {
                GenerateDOTRecursive(out, child_offset, node_id_counter, offset_to_id);
                
                // Add edge from parent to child
                int child_id = offset_to_id[child_offset];
                out << "  node" << current_id << " -> node" << child_id;
                if (i < num_keys) {
                    out << " [label=\"< " << internal.KeyAt(i) << "\"]";
                } else {
                    out << " [label=\"≥ " << internal.KeyAt(num_keys - 1) << "\"]";
                }
                out << ";\n";
            }
        }
    }
    
    tree_.pool_->UnpinPage(node_offset, false);
}

bool TreeVisualizer::GenerateSVG(const std::string& output_path) {
    // Generate DOT file first
    std::string dot_file = output_path + ".tmp.dot";
    GenerateDOT(dot_file);
    
    // Call graphviz
    std::string cmd = "dot -Tsvg " + dot_file + " -o " + output_path;
    int result = std::system(cmd.c_str());
    
    // Clean up temp file
    std::remove(dot_file.c_str());
    
    return result == 0;
}

void TreeVisualizer::PrintASCII(std::ostream& os) const {
    os << "B+ Tree Structure (ASCII):\n";
    os << "==========================\n\n";
    
    if (tree_.IsEmpty()) {
        os << "(empty tree)\n";
        return;
    }
    
    PrintASCIIRecursive(os, tree_.root_offset_, "", true);
}

void TreeVisualizer::PrintASCIIRecursive(
    std::ostream& os, 
    int64_t node_offset,
    const std::string& prefix, 
    bool is_tail) const {
    
    if (node_offset == INVALID_PAGE_ID) return;
    
    char* page = tree_.pool_->FetchPage(node_offset);
    if (!page) return;
    
    bool is_leaf = PageIsLeaf(page);
    
    // Print current node
    os << prefix;
    os << (is_tail ? "└── " : "├── ");
    
    if (is_leaf) {
        LeafPage leaf(page);
        int num_keys = leaf.NumKeys();
        os << "[LEAF] Keys: ";
        for (int i = 0; i < std::min(num_keys, 5); ++i) {
            if (i > 0) os << ", ";
            os << leaf.KeyAt(i);
        }
        if (num_keys > 5) os << ", ... (" << num_keys << " total)";
        os << "\n";
    } else {
        InternalPage internal(page);
        int num_keys = internal.NumKeys();
        os << "[INTERNAL] Keys: ";
        for (int i = 0; i < std::min(num_keys, 5); ++i) {
            if (i > 0) os << ", ";
            os << internal.KeyAt(i);
        }
        if (num_keys > 5) os << ", ... (" << num_keys << " total)";
        os << "\n";
        
        // Recursively print children
        std::string child_prefix = prefix + (is_tail ? "    " : "│   ");
        for (int i = 0; i <= num_keys; ++i) {
            int64_t child_offset = internal.ChildAt(i);
            PrintASCIIRecursive(os, child_offset, child_prefix, i == num_keys);
        }
    }
    
    tree_.pool_->UnpinPage(node_offset, false);
}

} // namespace bptree
