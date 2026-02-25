/// @file protocol.h
/// @brief Wire protocol constants for BPlusSQL.
///        Simple framing: [4B length LE][1B type][payload]
///        Think of it as a walkie-talkie, but for databases. :)

#pragma once

#include <cstddef>
#include <cstdint>

namespace bptree::net {

// ---------------------------------------------------------------------------
// Message type byte
// ---------------------------------------------------------------------------

enum class MsgType : uint8_t {
    Query  = 0x01,   ///< client → server: SQL text
    OkRows = 0x02,   ///< server → client: "N rows affected"
    Data   = 0x03,   ///< server → client: CSV result (header + rows)
    Error  = 0x04,   ///< server → client: error message
};

// ---------------------------------------------------------------------------
// Frame layout
// ---------------------------------------------------------------------------
// offset 0 : uint32_t payload_len  (little-endian, does NOT include header)
// offset 4 : uint8_t  msg_type
// offset 5 : payload_len bytes of UTF-8 text
// ---------------------------------------------------------------------------

constexpr int kHeaderSize = 5;  // 4 (len) + 1 (type)

} // namespace bptree::net
