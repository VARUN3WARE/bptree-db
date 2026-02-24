/// @file client.h
/// @brief C++ client library for bptree-db over the wire protocol.

#pragma once

#include "bptree/net/message.h"

#include <string>

namespace bptree::net {

struct ClientError : std::runtime_error {
    explicit ClientError(const std::string& m) : std::runtime_error(m) {}
};

/// Synchronous TCP client. Connect once, call Query() N times, Disconnect.
class Client {
public:
    Client() = default;
    ~Client();

    /// Open a TCP connection to host:port.
    /// @throws ClientError on failure.
    void Connect(const std::string& host, int port);

    /// Send a SQL statement and return the server's text response.
    /// For SELECT → CSV text.  For DML/DDL → "N rows affected" or result string.
    /// @throws ClientError on network or server error.
    std::string Query(const std::string& sql);

    void Disconnect();

    bool IsConnected() const { return fd_ >= 0; }

private:
    int fd_ = -1;
};

} // namespace bptree::net
