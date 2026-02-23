#pragma once

/// @file page.h
/// @brief Typed wrappers over raw 4 KB page buffers.
///
/// These provide a clean interface for accessing leaf and internal B+ tree
/// nodes stored in a flat char* buffer, hiding all the byte-level arithmetic.
///
/// Both page wrappers are templated on the key type K.  Key serialisation
/// is handled by KeyTraits<K> (see comparator.h).  If you change K you must
/// also rebuild a fresh index file -- the on-disk format is not self-describing.

#include "config.h"
#include "comparator.h"

#include <cstring>
#include <cassert>

namespace bptree {

// ============================================================================
// Helper: read / write POD at arbitrary byte offset
// ============================================================================
namespace detail {

template <typename T>
inline T ReadAt(const char* base, size_t off) {
    T v{};
    std::memcpy(&v, base + off, sizeof(T));
    return v;
}

template <typename T>
inline void WriteAt(char* base, size_t off, T v) {
    std::memcpy(base + off, &v, sizeof(T));
}

}  // namespace detail

// ============================================================================
// PageType detector  (works on any raw page regardless of key type)
// ============================================================================

/// Check the is_leaf flag at byte 4 of any page.
inline bool PageIsLeaf(const char* data) {
    return detail::ReadAt<int>(data, 4) == 1;
}

// ============================================================================
// LeafPage<K>
// ============================================================================
///
/// Layout (header is fixed; record size depends on key type K):
///
///   Offset  Size         Field
///   ------  -----------  --------------------------------
///   0       4            num_keys          (int)
///   4       4            is_leaf = 1       (int)
///   8       8            next_leaf         (int64_t, offset or -1)
///   16      N × recsize  records[]         key(KeyTraits<K>::kSize) | data(DATA_SIZE)
///
///   For K = int:  recsize = 4+100 = 104  ->  35 records/page (LEAF_MAX_KEYS)
///
template <typename K = int>
class LeafPage {
public:
    using Traits = KeyTraits<K>;

    explicit LeafPage(char* raw) : d_(raw) { assert(raw); }

    // -- Static factory ------------------------------------------------------

    /// Zero-initialise @p raw as a fresh leaf page.
    static void Init(char* raw) {
        std::memset(raw, 0, PAGE_SIZE);
        detail::WriteAt<int>(raw, 4, 1);                  // is_leaf = 1
        detail::WriteAt<int64_t>(raw, 8, INVALID_PAGE_ID); // next = -1
    }

    // -- Accessors -----------------------------------------------------------

    [[nodiscard]] int     NumKeys()  const { return detail::ReadAt<int>(d_, 0); }
    void                  SetNumKeys(int n){ detail::WriteAt<int>(d_, 0, n); }

    [[nodiscard]] int64_t NextLeaf() const { return detail::ReadAt<int64_t>(d_, 8); }
    void                  SetNextLeaf(int64_t v) { detail::WriteAt<int64_t>(d_, 8, v); }

    // -- Capacity (key-size-aware, computed at compile time) ----------------
    // header = 16, record = Traits::kSize + DATA_SIZE
    static constexpr int kMaxKeys =
        static_cast<int>((PAGE_SIZE - 16) / (Traits::kSize + DATA_SIZE));
    static constexpr int kMinKeys = (kMaxKeys + 1) / 2;

    // -- Per-record access ---------------------------------------------------

    [[nodiscard]] K KeyAt(int idx) const {
        return Traits::ReadFrom(d_, RecordOffset(idx));
    }

    void SetKeyAt(int idx, const K& key) {
        Traits::WriteTo(d_, RecordOffset(idx), key);
    }

    void GetData(int idx, char* out) const {
        std::memcpy(out, d_ + RecordOffset(idx) + Traits::kSize, DATA_SIZE);
    }

    void SetData(int idx, const char* data) {
        std::memcpy(d_ + RecordOffset(idx) + Traits::kSize, data, DATA_SIZE);
    }

    void SetRecord(int idx, const K& key, const char* data) {
        SetKeyAt(idx, key);
        SetData(idx, data);
    }

    void GetRecord(int idx, K& key, char* data) const {
        key = KeyAt(idx);
        GetData(idx, data);
    }

private:
    char* d_;

    static constexpr size_t kHeaderSize  = 16;                      // 4+4+8
    static constexpr size_t kRecordSize  = Traits::kSize + DATA_SIZE;

    static constexpr size_t RecordOffset(int idx) {
        return kHeaderSize + static_cast<size_t>(idx) * kRecordSize;
    }
};

// ============================================================================
// InternalPage<K>
// ============================================================================
///
/// Layout (slot size depends on K):
///
///   Offset  Size         Field
///   ------  -----------  --------------------------------
///   0       4            num_keys          (int)
///   4       4            is_leaf = 0       (int)
///   8       N × slotsize slots[]           child(8) | key(KeyTraits<K>::kSize)
///
///   For N keys there are N+1 children stored in slots 0..N.
///   The last child uses the child field of slot N (its key field is unused).
///
///   For K = int: slotsize = 8+4 = 12  ->  100 keys/page (INTERNAL_MAX_KEYS)
///
template <typename K = int>
class InternalPage {
public:
    using Traits = KeyTraits<K>;

    explicit InternalPage(char* raw) : d_(raw) { assert(raw); }

    // -- Static factory ------------------------------------------------------

    static void Init(char* raw) {
        std::memset(raw, 0, PAGE_SIZE);
        detail::WriteAt<int>(raw, 4, 0);  // is_leaf = 0
    }

    // -- Accessors -----------------------------------------------------------

    [[nodiscard]] int NumKeys()    const { return detail::ReadAt<int>(d_, 0); }
    void              SetNumKeys(int n)  { detail::WriteAt<int>(d_, 0, n); }

    // -- Capacity (key-size-aware) -----------------------------------------
    // header = 8, slot = 8 (child pointer) + Traits::kSize (key)
    // (kMaxKeys+1) slots <= (PAGE_SIZE - 8) / slotSize
    static constexpr int kMaxKeys =
        static_cast<int>((PAGE_SIZE - 8) / (8 + Traits::kSize)) - 1;
    static constexpr int kMinKeys = (kMaxKeys + 1) / 2;

    // -- Child / key access --------------------------------------------------

    [[nodiscard]] int64_t ChildAt(int idx) const {
        return detail::ReadAt<int64_t>(d_, SlotOffset(idx));
    }

    void SetChildAt(int idx, int64_t child) {
        detail::WriteAt<int64_t>(d_, SlotOffset(idx), child);
    }

    [[nodiscard]] K KeyAt(int idx) const {
        return Traits::ReadFrom(d_, SlotOffset(idx) + 8);
    }

    void SetKeyAt(int idx, const K& key) {
        Traits::WriteTo(d_, SlotOffset(idx) + 8, key);
    }

private:
    char* d_;

    static constexpr size_t kHeaderSize = 8;                       // 4+4
    static constexpr size_t kSlotSize   = 8 + Traits::kSize;      // child + key

    static constexpr size_t SlotOffset(int idx) {
        return kHeaderSize + static_cast<size_t>(idx) * kSlotSize;
    }
};

}  // namespace bptree
