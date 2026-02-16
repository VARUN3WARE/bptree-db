#pragma once

/// @file page.h
/// @brief Typed wrappers over raw 4 KB page buffers.
///
/// These provide a clean interface for accessing leaf and internal B+ tree
/// nodes stored in a flat char* buffer, hiding all the byte-level arithmetic.

#include "config.h"
#include <cstring>
#include <cassert>

namespace bptree {

// ============================================================================
// Helper: read / write POD at arbitrary byte offset
// ============================================================================
namespace detail {

template <typename T>
inline T ReadAt(const char* base, size_t off) {
    T v;
    std::memcpy(&v, base + off, sizeof(T));
    return v;
}

template <typename T>
inline void WriteAt(char* base, size_t off, T v) {
    std::memcpy(base + off, &v, sizeof(T));
}

}  // namespace detail

// ============================================================================
// PageType detector  (works on any raw page)
// ============================================================================

/// Check the is_leaf flag at byte 4 of any page.
inline bool PageIsLeaf(const char* data) {
    return detail::ReadAt<int>(data, 4) == 1;
}

// ============================================================================
// LeafPage
// ============================================================================
///
/// Layout (all multi-byte values little-endian on x86):
///
///   Offset  Size   Field
///   ------  -----  --------------------------------
///   0       4      num_keys       (int)
///   4       4      is_leaf = 1    (int)
///   8       8      next_leaf      (int64_t, offset or -1)
///   16      N×104  records[]      — each record is [key(4) | data(100)]
///
///   Max records per page: LEAF_MAX_KEYS (35)
///   Total used: 16 + 35 × 104 = 3656 bytes  (fits in 4096)
///
class LeafPage {
public:
    explicit LeafPage(char* raw) : d_(raw) { assert(raw); }

    // -- Static factory ------------------------------------------------------

    /// Zero-initialise a raw page as a leaf.
    static void Init(char* raw) {
        std::memset(raw, 0, PAGE_SIZE);
        detail::WriteAt<int>(raw, 4, 1);                  // is_leaf = 1
        detail::WriteAt<int64_t>(raw, 8, INVALID_PAGE_ID); // next = -1
    }

    // -- Accessors -----------------------------------------------------------

    [[nodiscard]] int      NumKeys()  const { return detail::ReadAt<int>(d_, 0); }
    void                   SetNumKeys(int n){ detail::WriteAt<int>(d_, 0, n); }

    [[nodiscard]] int64_t  NextLeaf() const { return detail::ReadAt<int64_t>(d_, 8); }
    void                   SetNextLeaf(int64_t v) { detail::WriteAt<int64_t>(d_, 8, v); }

    // -- Per-record access ---------------------------------------------------

    [[nodiscard]] int KeyAt(int idx) const {
        return detail::ReadAt<int>(d_, RecordOffset(idx));
    }

    void SetKeyAt(int idx, int key) {
        detail::WriteAt<int>(d_, RecordOffset(idx), key);
    }

    void GetData(int idx, char* out) const {
        std::memcpy(out, d_ + RecordOffset(idx) + 4, DATA_SIZE);
    }

    void SetData(int idx, const char* data) {
        std::memcpy(d_ + RecordOffset(idx) + 4, data, DATA_SIZE);
    }

    void SetRecord(int idx, int key, const char* data) {
        SetKeyAt(idx, key);
        SetData(idx, data);
    }

    void GetRecord(int idx, int& key, char* data) const {
        key = KeyAt(idx);
        GetData(idx, data);
    }

private:
    char* d_;

    static constexpr size_t kHeaderSize  = 16;  // 4 + 4 + 8
    static constexpr size_t kRecordSize  = 4 + DATA_SIZE;  // key + payload

    static constexpr size_t RecordOffset(int idx) {
        return kHeaderSize + static_cast<size_t>(idx) * kRecordSize;
    }
};

// ============================================================================
// InternalPage
// ============================================================================
///
/// Layout:
///
///   Offset  Size   Field
///   ------  -----  --------------------------------
///   0       4      num_keys       (int)
///   4       4      is_leaf = 0    (int)
///   8       N×12   slots[]        — each slot is [child(8) | key(4)]
///
///   For N keys there are N+1 children.  child[i] < key[i] <= child[i+1].
///   The last child occupies the `child` part of slot N (its `key` part is unused).
///
///   Max keys per page: INTERNAL_MAX_KEYS (100)
///   Total used: 8 + 101 × 12 = 1220 bytes  (fits in 4096)
///
class InternalPage {
public:
    explicit InternalPage(char* raw) : d_(raw) { assert(raw); }

    // -- Static factory ------------------------------------------------------

    static void Init(char* raw) {
        std::memset(raw, 0, PAGE_SIZE);
        detail::WriteAt<int>(raw, 4, 0);  // is_leaf = 0
    }

    // -- Accessors -----------------------------------------------------------

    [[nodiscard]] int NumKeys()    const { return detail::ReadAt<int>(d_, 0); }
    void              SetNumKeys(int n)  { detail::WriteAt<int>(d_, 0, n); }

    // -- Child / key access --------------------------------------------------

    [[nodiscard]] int64_t ChildAt(int idx) const {
        return detail::ReadAt<int64_t>(d_, SlotOffset(idx));
    }

    void SetChildAt(int idx, int64_t child) {
        detail::WriteAt<int64_t>(d_, SlotOffset(idx), child);
    }

    [[nodiscard]] int KeyAt(int idx) const {
        return detail::ReadAt<int>(d_, SlotOffset(idx) + 8);
    }

    void SetKeyAt(int idx, int key) {
        detail::WriteAt<int>(d_, SlotOffset(idx) + 8, key);
    }

private:
    char* d_;

    static constexpr size_t kHeaderSize = 8;   // 4 + 4
    static constexpr size_t kSlotSize   = 12;  // child(8) + key(4)

    static constexpr size_t SlotOffset(int idx) {
        return kHeaderSize + static_cast<size_t>(idx) * kSlotSize;
    }
};

}  // namespace bptree
