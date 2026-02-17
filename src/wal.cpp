/// @file wal.cpp
/// @brief Write-Ahead Log implementation — append-only redo log with
///        CRC32 integrity checks and checkpoint/truncate support.

#include "bptree/wal.h"
#include "bptree/disk_manager.h"

#include <cerrno>
#include <cstring>
#include <stdexcept>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace bptree {

// ============================================================================
// CRC32 (ISO 3309 polynomial, lookup-table based)
// ============================================================================

static const uint32_t kCRC32Table[256] = {
    0x00000000, 0x77073096, 0xEE0E612C, 0x990951BA, 0x076DC419, 0x706AF48F,
    0xE963A535, 0x9E6495A3, 0x0EDB8832, 0x79DCB8A4, 0xE0D5E91B, 0x97D2D988,
    0x09B64C2B, 0x7EB17CBE, 0xE7B82D09, 0x90BF1D91, 0x1DB71064, 0x6AB020F2,
    0xF3B97148, 0x84BE41DE, 0x1ADAD47D, 0x6DDDE4EB, 0xF4D4B551, 0x83D385C7,
    0x136C9856, 0x646BA8C0, 0xFD62F97A, 0x8A65C9EC, 0x14015C4F, 0x63066CD9,
    0xFA0F3D63, 0x8D080DF5, 0x3B6E20C8, 0x4C69105E, 0xD56041E4, 0xA2677172,
    0x3C03E4D4, 0x4B04D442, 0xD20D85FD, 0xA50AB56B, 0x35B5A8FA, 0x42B2986C,
    0xDBBBB9D6, 0xACBCB9C6, 0x32D86CE3, 0x45DF5C75, 0xDCD60DCF, 0xABD13D59,
    0x26D930AC, 0x51DE003A, 0xC8D75180, 0xBFD06116, 0x21B4F6B5, 0x56B3C423,
    0xCFBA9599, 0xB8BDA50F, 0x2802B89E, 0x5F058808, 0xC60CD9B2, 0xB10BE924,
    0x2F6F7C87, 0x58684C11, 0xC1611DAB, 0xB6662D3D, 0x76DC4190, 0x01DB7106,
    0x98D220BC, 0xEFD5102A, 0x71B18589, 0x06B6B51F, 0x9FBFE4A5, 0xE8B8D433,
    0x7807C9A2, 0x0F00F934, 0x9609A88E, 0xE10E9818, 0x7F6A0D69, 0x086D3D2D,
    0x91646C97, 0xE6635C01, 0x6B6B51F4, 0x1C6C6162, 0x856530D8, 0xF262004E,
    0x6C0695ED, 0x1B01A57B, 0x8208F4C1, 0xF50FC457, 0x65B0D9C6, 0x12B7E950,
    0x8BBEB8EA, 0xFCB9887C, 0x62DD1DDF, 0x15DA2D49, 0x8CD37CF3, 0xFBD44C65,
    0x4DB26158, 0x3AB551CE, 0xA3BC0074, 0xD4BB30E2, 0x4ADFA541, 0x3DD895D7,
    0xA4D1C46D, 0xD3D6F4FB, 0x4369E96A, 0x346ED9FC, 0xAD678846, 0xDA60B8D0,
    0x44042D73, 0x33031DE5, 0xAA0A4C5F, 0xDD0D7822, 0x3B75BD5A, 0x4C72B68C,
    0xD5DB596A, 0xA2D85A5C, 0x570D3F9E, 0x2000A7C8, 0xB966D409, 0xCE61E49F,
    0x5EDEF90E, 0x29D9C998, 0xB0D09822, 0xC7D7A8B4, 0x59B33D17, 0x2EB40D81,
    0xB7BD5C3B, 0xC0BA6CAD, 0xEDB88320, 0x9ABFB3B6, 0x03B6E20C, 0x74B1D29A,
    0xEAD54739, 0x9DD277AF, 0x04DB2615, 0x73DC1683, 0xE3630B12, 0x94643B84,
    0x0D6D6A3E, 0x7A6A5AA8, 0xE40ECF0B, 0x9309FF9D, 0x0A00AE27, 0x7D079EB1,
    0xF00F9344, 0x8708A3D2, 0x1E01F268, 0x6906C2FE, 0xF762575D, 0x806567CB,
    0x196C3671, 0x6E6B06E7, 0xFED41B76, 0x89D32BE0, 0x10DA7A5A, 0x67DD4ACC,
    0xF9B9DF6F, 0x8EBEEFF9, 0x17B7BE43, 0x60B08ED5, 0xD6D6A3E8, 0xA1D1937E,
    0x38D8C2C4, 0x4FDFF252, 0xD1BB67F1, 0xA6BC5767, 0x3FB506DD, 0x48B2364B,
    0xD80D2BDA, 0xAF0A1B4C, 0x36034AF6, 0x41047A60, 0xDF60EFC3, 0xA867DF55,
    0x316E8EEF, 0x4669BE79, 0xCB61B38C, 0xBC66831A, 0x256FD2A0, 0x5268E236,
    0xCC0C7795, 0xBB0B4703, 0x220216B9, 0x5505262F, 0xC5BA3BBE, 0xB2BD0B28,
    0x2BB45A92, 0x5CB36A04, 0xC2D7FFA7, 0xB5D0CF31, 0x2CD99E8B, 0x5BDEAE1D,
    0x9B64C2B0, 0xEC63F226, 0x756AA39C, 0x026D930A, 0x9C0906A9, 0xEB0E363F,
    0x72076785, 0x05005713, 0x95BF4A82, 0xE2B87A14, 0x7BB12BAE, 0x0CB61B38,
    0x92D28E9B, 0xE5D5BE0D, 0x7CDCEFB8, 0x0BDBDF21, 0x86D3D2D4, 0xF1D4E242,
    0x68DDB3F6, 0x1FDA836E, 0x81BE16CD, 0xF6B9265B, 0x6FB077E1, 0x18B74777,
    0x88085AE6, 0xFF0F6B70, 0x66063BCA, 0x11010B5C, 0x8F659EFF, 0xF862AE69,
    0x616BFFD3, 0x166CCF45, 0xA00AE278, 0xD70DD2EE, 0x4E048354, 0x3903B3C2,
    0xA7672661, 0xD06016F7, 0x4969474D, 0x3E6E77DB, 0xAED16A4A, 0xD9D65ADC,
    0x40DF0B66, 0x37D83BF0, 0xA9BCAE53, 0xDEBB9EC5, 0x47B2CF7F, 0x30B5FFE9,
    0xBDBDF21C, 0xCABAC28A, 0x53B39330, 0x24B4A3A6, 0xBAD03605, 0xCDD706FF,
    0x54DE5729, 0x23D967BF, 0xB3667A2E, 0xC4614AB8, 0x5D681B02, 0x2A6F2B94,
    0xB40BBE37, 0xC30C8EA1, 0x5A05DF1B, 0x2D02EF8D,
};

uint32_t WriteAheadLog::CRC32(const void* data, size_t len) {
    auto* buf = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; ++i) {
        crc = kCRC32Table[(crc ^ buf[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

// ============================================================================
// Helper: full write (handles partial writes / EINTR)
// ============================================================================

static bool FullWrite(int fd, const void* buf, size_t count) {
    auto* p = static_cast<const char*>(buf);
    size_t remaining = count;
    while (remaining > 0) {
        ssize_t n = ::write(fd, p, remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        p += n;
        remaining -= static_cast<size_t>(n);
    }
    return true;
}

static bool FullRead(int fd, void* buf, size_t count) {
    auto* p = static_cast<char*>(buf);
    size_t remaining = count;
    while (remaining > 0) {
        ssize_t n = ::read(fd, p, remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;  // EOF
        p += n;
        remaining -= static_cast<size_t>(n);
    }
    return true;
}

// ============================================================================
// Construction / destruction
// ============================================================================

WriteAheadLog::WriteAheadLog(const std::string& path) : path_(path) {
    // Open or create the WAL file.
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
    if (fd_ < 0) {
        throw std::runtime_error("WriteAheadLog: cannot open " + path_ +
                                 ": " + std::strerror(errno));
    }

    struct stat sb{};
    if (::fstat(fd_, &sb) != 0) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("WriteAheadLog: fstat failed");
    }

    if (sb.st_size == 0) {
        // New WAL -- write the file header.
        WALFileHeader hdr{};
        if (!FullWrite(fd_, &hdr, sizeof(hdr))) {
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error("WriteAheadLog: cannot write header");
        }
        ::fsync(fd_);
        next_lsn_ = 1;
        checkpoint_lsn_ = 0;
    } else {
        // Existing WAL -- read the file header.
        WALFileHeader hdr{};
        ::lseek(fd_, 0, SEEK_SET);
        if (!FullRead(fd_, &hdr, sizeof(hdr)) || hdr.magic != 0x57414C31) {
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error("WriteAheadLog: invalid WAL file");
        }
        checkpoint_lsn_ = hdr.checkpoint_lsn;

        // Scan to find the highest LSN.
        auto records = ReadAllRecords();
        if (!records.empty()) {
            next_lsn_ = records.back().header.lsn + 1;
        } else {
            next_lsn_ = 1;
        }

        // Seek to end for appending.
        ::lseek(fd_, 0, SEEK_END);
    }
}

WriteAheadLog::~WriteAheadLog() {
    if (fd_ >= 0) {
        ::fsync(fd_);
        ::close(fd_);
    }
}

// ============================================================================
// Logging
// ============================================================================

void WriteAheadLog::AppendRecord(LogRecordType type, int64_t page_id,
                                 const char* data, uint32_t data_len) {
    LogRecordHeader hdr{};
    hdr.lsn      = next_lsn_++;
    hdr.type     = type;
    hdr.page_id  = page_id;
    hdr.data_len = data_len;
    hdr.checksum = 0;  // compute below

    // Checksum covers the header (with checksum field zeroed) + data.
    uint32_t crc = CRC32(&hdr, sizeof(hdr));
    if (data && data_len > 0) {
        // Chain the CRC: continue from where we left off.
        // Simple approach: hash header || data together.
        // We'll concatenate logically by computing combined CRC.
        // For simplicity, compute CRC of full payload separately and XOR.
        uint32_t data_crc = CRC32(data, data_len);
        crc ^= data_crc;
    }
    hdr.checksum = crc;

    // Write header.
    if (!FullWrite(fd_, &hdr, sizeof(hdr))) {
        throw std::runtime_error("WriteAheadLog: write failed (header)");
    }

    // Write data payload.
    if (data && data_len > 0) {
        if (!FullWrite(fd_, data, data_len)) {
            throw std::runtime_error("WriteAheadLog: write failed (data)");
        }
    }

    bytes_written_ += sizeof(hdr) + data_len;
    ++records_written_;
}

uint64_t WriteAheadLog::LogPageWrite(int64_t page_id, const char* page_data) {
    uint64_t lsn = next_lsn_;
    AppendRecord(LogRecordType::kPageWrite, page_id,
                 page_data, static_cast<uint32_t>(PAGE_SIZE));
    return lsn;
}

uint64_t WriteAheadLog::BeginCheckpoint() {
    uint64_t lsn = next_lsn_;
    AppendRecord(LogRecordType::kCheckpointBegin, INVALID_PAGE_ID, nullptr, 0);
    Flush();
    return lsn;
}

uint64_t WriteAheadLog::EndCheckpoint() {
    uint64_t lsn = next_lsn_;
    AppendRecord(LogRecordType::kCheckpointEnd, INVALID_PAGE_ID, nullptr, 0);
    Flush();

    // Update the file header with the new checkpoint LSN.
    checkpoint_lsn_ = lsn;
    WALFileHeader hdr{};
    hdr.checkpoint_lsn = checkpoint_lsn_;
    ::lseek(fd_, 0, SEEK_SET);
    FullWrite(fd_, &hdr, sizeof(hdr));
    ::fsync(fd_);

    // Seek back to end.
    ::lseek(fd_, 0, SEEK_END);

    // Truncate -- all committed changes are now on disk.
    Truncate();
    return lsn;
}

void WriteAheadLog::Flush() {
    if (fd_ >= 0) {
        ::fsync(fd_);
    }
}

// ============================================================================
// Recovery
// ============================================================================

std::vector<WriteAheadLog::RecoveryRecord> WriteAheadLog::ReadAllRecords() {
    std::vector<RecoveryRecord> records;

    ::lseek(fd_, static_cast<off_t>(sizeof(WALFileHeader)), SEEK_SET);

    while (true) {
        LogRecordHeader hdr{};
        if (!FullRead(fd_, &hdr, sizeof(hdr))) break;  // EOF or error

        // Validate: LSN should be > 0, type should be valid.
        if (hdr.lsn == 0 ||
            static_cast<uint32_t>(hdr.type) > static_cast<uint32_t>(LogRecordType::kCheckpointEnd)) {
            break;  // corrupt or end of valid records
        }

        RecoveryRecord rec;
        rec.header = hdr;

        if (hdr.data_len > 0) {
            rec.data.resize(hdr.data_len);
            if (!FullRead(fd_, rec.data.data(), hdr.data_len)) break;  // truncated

            // Verify checksum.
            LogRecordHeader check_hdr = hdr;
            check_hdr.checksum = 0;
            uint32_t expected_crc = CRC32(&check_hdr, sizeof(check_hdr));
            expected_crc ^= CRC32(rec.data.data(), rec.data.size());

            if (expected_crc != hdr.checksum) {
                break;  // checksum mismatch -- treat as end of valid log
            }
        } else {
            // Verify header-only checksum.
            LogRecordHeader check_hdr = hdr;
            check_hdr.checksum = 0;
            uint32_t expected_crc = CRC32(&check_hdr, sizeof(check_hdr));
            if (expected_crc != hdr.checksum) {
                break;
            }
        }

        records.push_back(std::move(rec));
    }

    return records;
}

size_t WriteAheadLog::Recover(DiskManager& disk) {
    auto records = ReadAllRecords();

    // Find the last completed checkpoint.
    uint64_t redo_after_lsn = checkpoint_lsn_;
    for (auto it = records.rbegin(); it != records.rend(); ++it) {
        if (it->header.type == LogRecordType::kCheckpointEnd) {
            redo_after_lsn = it->header.lsn;
            break;
        }
    }

    // Redo all PAGE_WRITE records after the checkpoint.
    size_t pages_recovered = 0;
    for (const auto& rec : records) {
        if (rec.header.lsn <= redo_after_lsn) continue;
        if (rec.header.type != LogRecordType::kPageWrite) continue;
        if (rec.header.page_id == INVALID_PAGE_ID) continue;
        if (rec.data.size() != PAGE_SIZE) continue;

        // Ensure the data file is large enough.
        int64_t required = rec.header.page_id + static_cast<int64_t>(PAGE_SIZE);
        if (required > static_cast<int64_t>(disk.FileSize())) {
            // The data file may not have grown yet -- the WAL has the truth.
            // We need to allocate pages up to this point.
            while (disk.NextPageOffset() <= rec.header.page_id) {
                disk.AllocatePage();
            }
        }

        // Apply the after-image.
        char* page = disk.PageData(rec.header.page_id);
        std::memcpy(page, rec.data.data(), PAGE_SIZE);
        ++pages_recovered;
    }

    if (pages_recovered > 0) {
        disk.Sync();
    }

    // Update next_lsn_ to be past all recovered records.
    if (!records.empty()) {
        next_lsn_ = records.back().header.lsn + 1;
    }

    // Seek to end for future appends.
    ::lseek(fd_, 0, SEEK_END);

    // If we recovered anything, truncate the WAL since we've applied everything.
    if (pages_recovered > 0) {
        Truncate();
    }

    return pages_recovered;
}

// ============================================================================
// Truncate
// ============================================================================

void WriteAheadLog::Truncate() {
    // Reset to just the file header.
    WALFileHeader hdr{};
    hdr.checkpoint_lsn = checkpoint_lsn_;

    if (::ftruncate(fd_, sizeof(WALFileHeader)) != 0) {
        throw std::runtime_error("WriteAheadLog::Truncate: ftruncate failed");
    }

    ::lseek(fd_, 0, SEEK_SET);
    FullWrite(fd_, &hdr, sizeof(hdr));
    ::fsync(fd_);

    // Seek to end (= just past header).
    ::lseek(fd_, 0, SEEK_END);
}

}  // namespace bptree
