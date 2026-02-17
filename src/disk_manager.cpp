/// @file disk_manager.cpp
/// @brief DiskManager implementation — mmap-based page storage.

#include "bptree/disk_manager.h"

#include <cerrno>
#include <cstring>
#include <stdexcept>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace bptree {

// ============================================================================
// Construction / destruction
// ============================================================================

DiskManager::DiskManager(const std::string& path) : path_(path) {
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
    if (fd_ < 0) {
        throw std::runtime_error("DiskManager: cannot open " + path_ +
                                 ": " + std::strerror(errno));
    }

    struct stat sb{};
    if (::fstat(fd_, &sb) != 0) {
        ::close(fd_);
        throw std::runtime_error("DiskManager: fstat failed: " +
                                 std::string(std::strerror(errno)));
    }

    file_size_ = static_cast<size_t>(sb.st_size);

    // Brand-new file — allocate the metadata page.
    if (file_size_ == 0) {
        file_size_ = PAGE_SIZE;
        if (::ftruncate(fd_, static_cast<off_t>(file_size_)) != 0) {
            ::close(fd_);
            throw std::runtime_error("DiskManager: ftruncate failed");
        }
    }

    mapped_ = static_cast<char*>(
        ::mmap(nullptr, file_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));

    if (mapped_ == MAP_FAILED) {
        mapped_ = nullptr;
        ::close(fd_);
        throw std::runtime_error("DiskManager: mmap failed: " +
                                 std::string(std::strerror(errno)));
    }

    // If new file, initialise metadata page with defaults.
    if (sb.st_size == 0) {
        SetRootOffset(INVALID_PAGE_ID);
        SetNextPageOffset(PAGE_SIZE);
        SetFreeListHead(INVALID_PAGE_ID);
        FlushMetadata();
    }
}

DiskManager::~DiskManager() {
    if (mapped_) {
        Sync();
        ::munmap(mapped_, file_size_);
    }
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

// ============================================================================
// Page access
// ============================================================================

char* DiskManager::PageData(int64_t offset) {
    if (offset < 0 || static_cast<size_t>(offset + PAGE_SIZE) > file_size_) {
        throw std::out_of_range("DiskManager::PageData: offset out of range");
    }
    return mapped_ + offset;
}

const char* DiskManager::PageData(int64_t offset) const {
    if (offset < 0 || static_cast<size_t>(offset + PAGE_SIZE) > file_size_) {
        throw std::out_of_range("DiskManager::PageData: offset out of range");
    }
    return mapped_ + offset;
}

int64_t DiskManager::AllocatePage() {
    // Try to reuse a freed page first.
    int64_t reclaimed = ReclaimPage();
    if (reclaimed != INVALID_PAGE_ID) {
        std::memset(mapped_ + reclaimed, 0, PAGE_SIZE);
        return reclaimed;
    }

    int64_t next = NextPageOffset();
    int64_t new_next = next + static_cast<int64_t>(PAGE_SIZE);
    EnsureCapacity(new_next);

    // Zero out the fresh page.
    std::memset(mapped_ + next, 0, PAGE_SIZE);

    SetNextPageOffset(new_next);
    return next;
}

// ============================================================================
// Metadata helpers (page 0)
// ============================================================================

int64_t DiskManager::RootOffset() const {
    int64_t v;
    std::memcpy(&v, mapped_ + META_ROOT_OFFSET, sizeof(v));
    return v;
}

void DiskManager::SetRootOffset(int64_t offset) {
    std::memcpy(mapped_ + META_ROOT_OFFSET, &offset, sizeof(offset));
}

int64_t DiskManager::NextPageOffset() const {
    int64_t v;
    std::memcpy(&v, mapped_ + META_NEXT_PAGE, sizeof(v));
    return v;
}

void DiskManager::SetNextPageOffset(int64_t offset) {
    std::memcpy(mapped_ + META_NEXT_PAGE, &offset, sizeof(offset));
}

void DiskManager::FlushMetadata() {
    ::msync(mapped_, PAGE_SIZE, MS_SYNC);
}

// ============================================================================
// Free-page list
// ============================================================================

int64_t DiskManager::FreeListHead() const {
    int64_t v;
    std::memcpy(&v, mapped_ + META_FREE_LIST_HEAD, sizeof(v));
    return v;
}

void DiskManager::SetFreeListHead(int64_t offset) {
    std::memcpy(mapped_ + META_FREE_LIST_HEAD, &offset, sizeof(offset));
}

void DiskManager::FreePage(int64_t page_offset) {
    if (page_offset < static_cast<int64_t>(PAGE_SIZE)) return;  // don't free metadata page

    // Push onto the free-list: store current head as this page's "next".
    int64_t old_head = FreeListHead();
    std::memcpy(mapped_ + page_offset + FREE_PAGE_NEXT_OFFSET, &old_head, sizeof(old_head));
    SetFreeListHead(page_offset);
}

int64_t DiskManager::ReclaimPage() {
    int64_t head = FreeListHead();
    if (head == INVALID_PAGE_ID) return INVALID_PAGE_ID;

    // Pop from the free-list.
    int64_t next;
    std::memcpy(&next, mapped_ + head + FREE_PAGE_NEXT_OFFSET, sizeof(next));
    SetFreeListHead(next);
    return head;
}

// ============================================================================
// Sync
// ============================================================================

void DiskManager::Sync() {
    if (mapped_) {
        ::msync(mapped_, file_size_, MS_SYNC);
    }
}

void DiskManager::SyncAsync() {
    if (mapped_) {
        ::msync(mapped_, file_size_, MS_ASYNC);
    }
}

// ============================================================================
// Internal
// ============================================================================

void DiskManager::EnsureCapacity(int64_t required) {
    if (required <= static_cast<int64_t>(file_size_)) return;

    // Grow geometrically (at least double, minimum 1 MB) to avoid
    // frequent ftruncate + mmap cycles during bulk inserts.
    size_t min_size = ((static_cast<size_t>(required) + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;
    size_t new_size = std::max(min_size, std::max(file_size_ * 2, static_cast<size_t>(1 << 20)));
    new_size = ((new_size + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;

    // Flush and unmap current region.
    if (mapped_) {
        ::msync(mapped_, file_size_, MS_ASYNC);
        ::munmap(mapped_, file_size_);
        mapped_ = nullptr;
    }

    // Grow the file.
    if (::ftruncate(fd_, static_cast<off_t>(new_size)) != 0) {
        throw std::runtime_error("DiskManager::EnsureCapacity: ftruncate failed");
    }

    // Re-map at the new size.
    mapped_ = static_cast<char*>(
        ::mmap(nullptr, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));

    if (mapped_ == MAP_FAILED) {
        mapped_ = nullptr;
        throw std::runtime_error("DiskManager::EnsureCapacity: mmap failed");
    }

    file_size_ = new_size;
}

}  // namespace bptree
