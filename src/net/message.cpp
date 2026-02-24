/// @file message.cpp
/// @brief Framed message read/write over a TCP socket fd.

#include "bptree/net/message.h"

#include <cerrno>
#include <cstring>
#include <unistd.h>

namespace bptree::net {

// ---------------------------------------------------------------------------
// Low-level helpers: read/write exact N bytes
// ---------------------------------------------------------------------------

static bool read_exact(int fd, char* buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = ::read(fd, buf + done, n - done);
        if (r <= 0) return false;  // disconnect or error
        done += static_cast<size_t>(r);
    }
    return true;
}

static bool write_exact(int fd, const char* buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t w = ::write(fd, buf + done, n - done);
        if (w <= 0) return false;
        done += static_cast<size_t>(w);
    }
    return true;
}

// ---------------------------------------------------------------------------
// SendMsg / RecvMsg
// ---------------------------------------------------------------------------

bool SendMsg(int fd, const Message& msg) {
    // Build the 5-byte header.
    uint32_t len = static_cast<uint32_t>(msg.payload.size());
    char header[kHeaderSize];

    // Little-endian length
    header[0] = static_cast<char>( len        & 0xFF);
    header[1] = static_cast<char>((len >>  8) & 0xFF);
    header[2] = static_cast<char>((len >> 16) & 0xFF);
    header[3] = static_cast<char>((len >> 24) & 0xFF);
    header[4] = static_cast<char>(msg.type);

    if (!write_exact(fd, header, kHeaderSize)) return false;
    if (len > 0 && !write_exact(fd, msg.payload.data(), len)) return false;
    return true;
}

bool RecvMsg(int fd, Message& msg) {
    char header[kHeaderSize];
    if (!read_exact(fd, header, kHeaderSize)) return false;

    uint32_t len =
        (static_cast<uint32_t>(static_cast<unsigned char>(header[0]))      ) |
        (static_cast<uint32_t>(static_cast<unsigned char>(header[1])) <<  8) |
        (static_cast<uint32_t>(static_cast<unsigned char>(header[2])) << 16) |
        (static_cast<uint32_t>(static_cast<unsigned char>(header[3])) << 24);
    msg.type = static_cast<MsgType>(static_cast<unsigned char>(header[4]));

    msg.payload.resize(len);
    if (len > 0 && !read_exact(fd, msg.payload.data(), len)) return false;
    return true;
}

} // namespace bptree::net
