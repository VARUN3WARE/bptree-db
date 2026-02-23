#pragma once

/// @file comparator.h
/// @brief KeyComparator trait and built-in specializations.
///
/// A comparator tells the B+ tree how to order keys.  Any type that satisfies
/// the KeyComparator concept can be used as a key -- int, int64_t, std::string,
/// or your own type if you write a comparator for it. :)
///
/// Design follows std::less semantics: Compare(a, b) returns true if a < b.
///
/// Usage:
/// @code
///   bptree::BPlusTree<int>          tree1("ints.idx");       // default: int
///   bptree::BPlusTree<std::string>  tree2("strings.idx");    // string keys
///   bptree::BPlusTree<int64_t>      tree3("bigints.idx");    // 64-bit keys
/// @endcode

#include <cstring>
#include <string>
#include <type_traits>

namespace bptree {

// ============================================================================
// KeyTraits -- per-key-type metadata the tree needs at compile time
// ============================================================================

/// Provides the on-disk size of a key type and helpers to read/write it
/// from raw page bytes.
///
/// Specialize this for custom key types if needed.
template <typename K>
struct KeyTraits {
    // Byte size of this key type when stored in a page.
    static constexpr size_t kSize = sizeof(K);

    /// Read a key from raw page data at byte offset @p off.
    static K ReadFrom(const char* page, size_t off) {
        K v{};
        std::memcpy(&v, page + off, sizeof(K));
        return v;
    }

    /// Write a key into raw page data at byte offset @p off.
    static void WriteTo(char* page, size_t off, const K& key) {
        std::memcpy(page + off, &key, sizeof(K));
    }
};

// ============================================================================
// Comparators -- one per key type
// ============================================================================

/// Generic default comparator (works for any type with operator<).
/// Mimics std::less<K>.
template <typename K>
struct DefaultComparator {
    /// Returns true if a < b.
    bool operator()(const K& a, const K& b) const {
        return a < b;
    }

    /// Returns true if a == b.
    bool Equal(const K& a, const K& b) const {
        return !(a < b) && !(b < a);
    }
};

// std::string -- uses lexicographic order (same as operator<, so the
// default works).  But we specialise KeyTraits because the on-disk rep
// is fixed-width (null-padded, capped at kMaxStringLen). :(
// Variable-length string keys are a Phase 3 stretch goal.

/// Maximum on-disk width of a std::string key (null-padded).
/// Changing this changes the disk format -- do it before first use.
static constexpr size_t kMaxStringKeyLen = 64;

template <>
struct KeyTraits<std::string> {
    static constexpr size_t kSize = kMaxStringKeyLen;

    static std::string ReadFrom(const char* page, size_t off) {
        // Read up to kMaxStringKeyLen bytes, stopping at the first null.
        return std::string(page + off,
                           ::strnlen(page + off, kMaxStringKeyLen));
    }

    static void WriteTo(char* page, size_t off, const std::string& key) {
        size_t n = std::min(key.size(), kMaxStringKeyLen - 1);
        std::memset(page + off, 0, kMaxStringKeyLen);  // zero-pad
        std::memcpy(page + off, key.data(), n);
    }
};

}  // namespace bptree
