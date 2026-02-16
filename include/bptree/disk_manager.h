#pragma once

/// @file disk_manager.h
/// @brief Manages a memory-mapped index file.  Provides page-level access and
///        allocation over a single backing file.

#include "config.h"
#include "status.h"
#include <string>

namespace bptree {

/// Manages a single index file via mmap.
///
/// Responsibilities:
///   - Open / create the backing file
///   - Grow (ftruncate + remap) as needed
///   - Allocate new zeroed pages
///   - Expose raw pointers into the mapped region
///   - Sync dirty pages to disk
///
/// Thread safety: **not thread-safe**.  External synchronisation is required.
class DiskManager {
public:
    /// Open (or create) the index file at @p path.
    explicit DiskManager(const std::string& path = DEFAULT_INDEX_FILE);

    ~DiskManager();

    // Non-copyable, non-movable (owns fd + mmap)
    DiskManager(const DiskManager&)            = delete;
    DiskManager& operator=(const DiskManager&) = delete;
    DiskManager(DiskManager&&)                 = delete;
    DiskManager& operator=(DiskManager&&)      = delete;

    // -- Page access ---------------------------------------------------------

    /// Return a writable pointer to the page at byte @p offset.
    /// @pre offset is page-aligned and within allocated range.
    [[nodiscard]] char*       PageData(int64_t offset);
    [[nodiscard]] const char* PageData(int64_t offset) const;

    /// Allocate a fresh zeroed page.  Returns its byte offset.
    int64_t AllocatePage();

    // -- Metadata helpers (page 0) -------------------------------------------

    /// Read root_offset from the metadata page.
    [[nodiscard]] int64_t RootOffset() const;
    void SetRootOffset(int64_t offset);

    /// Read / write the next-page pointer.
    [[nodiscard]] int64_t NextPageOffset() const;
    void SetNextPageOffset(int64_t offset);

    /// Persist the metadata page synchronously.
    void FlushMetadata();

    // -- Synchronisation -----------------------------------------------------

    /// Flush all dirty pages to disk (synchronous).
    void Sync();

    /// Schedule a background flush (asynchronous).
    void SyncAsync();

    // -- Queries -------------------------------------------------------------

    [[nodiscard]] size_t      FileSize()  const { return file_size_; }
    [[nodiscard]] bool        IsValid()   const { return fd_ >= 0 && mapped_ != nullptr; }
    [[nodiscard]] std::string FilePath()  const { return path_; }

private:
    /// Ensure the mapped region is at least @p required bytes.
    void EnsureCapacity(int64_t required);

    std::string path_;
    int         fd_        = -1;
    char*       mapped_    = nullptr;
    size_t      file_size_ = 0;
};

}  // namespace bptree
