#pragma once

/// @file wal.h
/// @brief Write-Ahead Log (WAL) for crash recovery.
///
/// Design:
///   - Append-only log file storing full page after-images.
///   - Redo-only recovery: on crash, replay logged page writes to restore
///     the data file to a consistent state.
///   - Checkpoint support: flush all dirty pages, then truncate the log.
///   - CRC32 checksum per record for integrity verification.
///
/// WAL protocol (enforced by BufferPool):
///   Before a dirty page is written back to disk, its after-image must first
///   be appended to the WAL and fsynced.  This guarantees that on crash we
///   can always redo any page write that reached the data file, plus any that
///   did not.
///
/// File format:
/// @code
///   [FileHeader: magic(4) | version(4) | checkpoint_lsn(8)]
///   [Record 0: lsn(8) | type(4) | page_id(8) | data_len(4) | checksum(4) | data...]
///   [Record 1: ...]
///   ...
/// @endcode
///
/// Recovery:
///   1. Open WAL file.
///   2. Read all valid records.
///   3. Apply all PAGE_WRITE records after the last completed checkpoint.
///   4. Truncate the log.

#include "config.h"

#include <cstdint>
#include <string>
#include <vector>

namespace bptree {

// Forward-declare DiskManager so WAL can apply recovered pages.
class DiskManager;

// ============================================================================
// Log record types
// ============================================================================

enum class LogRecordType : uint32_t {
    kInvalid         = 0,
    kPageWrite       = 1,  ///< Full page after-image (page_id + PAGE_SIZE bytes)
    kCheckpointBegin = 2,  ///< Marks the start of a checkpoint
    kCheckpointEnd   = 3,  ///< Marks the end of a checkpoint (all pages flushed)
};

// ============================================================================
// On-disk structures
// ============================================================================

/// WAL file header (written at offset 0).
struct WALFileHeader {
    uint32_t magic    = 0x57414C31;  ///< "WAL1" in ASCII
    uint32_t version  = 1;
    uint64_t checkpoint_lsn = 0;     ///< LSN of the last completed checkpoint
};

static_assert(sizeof(WALFileHeader) == 16);

/// Header for each log record (immediately followed by `data_len` bytes).
struct LogRecordHeader {
    uint64_t        lsn       = 0;
    LogRecordType   type      = LogRecordType::kInvalid;
    int64_t         page_id   = INVALID_PAGE_ID;
    uint32_t        data_len  = 0;      ///< Bytes of payload following this header
    uint32_t        checksum  = 0;      ///< CRC32 of (header fields + data)
};

static_assert(sizeof(LogRecordHeader) == 32);

// ============================================================================
// WriteAheadLog
// ============================================================================

/// Append-only write-ahead log with redo-only crash recovery.
///
/// Typical lifecycle:
/// @code
///   // On open:
///   WriteAheadLog wal("index.wal");
///   wal.Recover(disk);           // replay after crash
///
///   // During operation (called by BufferPool before flush):
///   wal.LogPageWrite(page_id, data);
///   wal.Flush();
///
///   // Periodic checkpoint:
///   wal.BeginCheckpoint();
///   pool.FlushAllPages();        // flushes dirty pages (which logs them)
///   wal.EndCheckpoint();
/// @endcode
class WriteAheadLog {
public:
    /// Open (or create) a WAL file at @p path.
    explicit WriteAheadLog(const std::string& path);

    ~WriteAheadLog();

    // Non-copyable
    WriteAheadLog(const WriteAheadLog&)            = delete;
    WriteAheadLog& operator=(const WriteAheadLog&) = delete;

    // -- Logging -------------------------------------------------------------

    /// Append a full page after-image to the log.
    /// @return The LSN assigned to this record.
    uint64_t LogPageWrite(int64_t page_id, const char* page_data);

    /// Mark the beginning of a checkpoint.
    uint64_t BeginCheckpoint();

    /// Mark the end of a checkpoint, then truncate the log.
    uint64_t EndCheckpoint();

    /// Force-flush (fsync) the WAL file to stable storage.
    void Flush();

    // -- Recovery ------------------------------------------------------------

    /// Replay logged page writes to @p disk to restore consistency.
    /// Should be called once on startup before normal operations.
    /// @return Number of pages recovered.
    size_t Recover(DiskManager& disk);

    // -- Queries -------------------------------------------------------------

    [[nodiscard]] uint64_t    CurrentLSN()         const { return next_lsn_; }
    [[nodiscard]] uint64_t    CheckpointLSN()      const { return checkpoint_lsn_; }
    [[nodiscard]] size_t      BytesWritten()        const { return bytes_written_; }
    [[nodiscard]] size_t      RecordsWritten()      const { return records_written_; }
    [[nodiscard]] std::string FilePath()            const { return path_; }
    [[nodiscard]] bool        IsEnabled()           const { return fd_ >= 0; }

    // -- Utility -------------------------------------------------------------

    /// Compute CRC32 of a buffer.
    static uint32_t CRC32(const void* data, size_t len);

private:
    /// Append a raw log record (header + optional data).
    void AppendRecord(LogRecordType type, int64_t page_id,
                      const char* data, uint32_t data_len);

    /// Read all valid records from the WAL file.
    struct RecoveryRecord {
        LogRecordHeader header;
        std::vector<char> data;
    };
    std::vector<RecoveryRecord> ReadAllRecords();

    /// Truncate the WAL file (reset to just the file header).
    void Truncate();

    std::string path_;
    int         fd_         = -1;
    uint64_t    next_lsn_   = 1;
    uint64_t    checkpoint_lsn_ = 0;

    // Stats
    size_t bytes_written_   = 0;
    size_t records_written_ = 0;
};

}  // namespace bptree
