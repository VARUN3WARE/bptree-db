#pragma once

/// @file latch.h
/// @brief Reader-writer latch for page-level concurrency control.
///
/// Each page frame in the buffer pool carries one of these.
/// Multiple readers can hold the latch at the same time; a writer
/// needs exclusive access -- just like a shared_mutex, but wrapped
/// in a tiny RAII type so we don't forget to release it. :)
///
/// Usage:
/// @code
///   PageLatch latch;
///   latch.RLock();    // shared -- many readers OK
///   latch.RUnlock();
///
///   latch.WLock();    // exclusive -- one writer at a time
///   latch.WUnlock();
/// @endcode

#include <shared_mutex>

namespace bptree {

/// Thin wrapper around std::shared_mutex with readable names.
///
/// Why not use shared_mutex directly?  Because "RLock / WLock"
/// communicates intent better than "lock_shared / lock". :(
/// Small thing, but it helps when reading the tree code.
class PageLatch {
public:
    PageLatch()  = default;
    ~PageLatch() = default;

    // Not copyable or movable -- latches live inside PageFrame structs.
    PageLatch(const PageLatch&)            = delete;
    PageLatch& operator=(const PageLatch&) = delete;

    /// Acquire shared (read) access.  Blocks until no writer holds the latch.
    void RLock()   { mtx_.lock_shared(); }

    /// Release shared (read) access.
    void RUnlock() { mtx_.unlock_shared(); }

    /// Acquire exclusive (write) access.  Blocks until all readers/writers done.
    void WLock()   { mtx_.lock(); }

    /// Release exclusive (write) access.
    void WUnlock() { mtx_.unlock(); }

    /// Try to upgrade from read -> write (non-blocking).
    /// Returns true if the upgrade succeeded.
    /// NOTE: caller must call RUnlock() first to avoid deadlock -- there is
    /// no atomic upgrade in std::shared_mutex. This helper is intentionally
    /// not provided to keep things explicit. See latch crabbing in bplus_tree.
    bool TryWLock() { return mtx_.try_lock(); }

private:
    std::shared_mutex mtx_;
};

/// RAII guard for read access -- releases on scope exit.
/// Use this so you never forget RUnlock. :)
class ReadGuard {
public:
    explicit ReadGuard(PageLatch& latch) : latch_(latch) { latch_.RLock(); }
    ~ReadGuard() { latch_.RUnlock(); }

    ReadGuard(const ReadGuard&)            = delete;
    ReadGuard& operator=(const ReadGuard&) = delete;

private:
    PageLatch& latch_;
};

/// RAII guard for write access -- releases on scope exit.
class WriteGuard {
public:
    explicit WriteGuard(PageLatch& latch) : latch_(latch) { latch_.WLock(); }
    ~WriteGuard() { latch_.WUnlock(); }

    WriteGuard(const WriteGuard&)            = delete;
    WriteGuard& operator=(const WriteGuard&) = delete;

private:
    PageLatch& latch_;
};

}  // namespace bptree
