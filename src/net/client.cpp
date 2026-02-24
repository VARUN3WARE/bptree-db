/// @file client.cpp
/// @brief TCP client implementation.

#include "bptree/net/client.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

namespace bptree::net {

Client::~Client() { Disconnect(); }

void Client::Connect(const std::string& host, int port) {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0)
        throw ClientError(std::string("socket(): ") + std::strerror(errno));

    // Resolve host (accepts both IP and hostname).
    struct hostent* he = ::gethostbyname(host.c_str());
    if (!he) {
        ::close(fd_); fd_ = -1;
        throw ClientError("Cannot resolve host: " + host);
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(port));
    std::memcpy(&addr.sin_addr, he->h_addr, static_cast<size_t>(he->h_length));

    if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd_); fd_ = -1;
        throw ClientError(std::string("connect(): ") + std::strerror(errno));
    }
}

std::string Client::Query(const std::string& sql) {
    if (fd_ < 0) throw ClientError("not connected");

    if (!SendMsg(fd_, Message::Query(sql)))
        throw ClientError("send failed");

    Message resp;
    if (!RecvMsg(fd_, resp))
        throw ClientError("connection closed by server");

    if (resp.type == MsgType::Error)
        throw ClientError("server error: " + resp.payload);

    return resp.payload;
}

void Client::Disconnect() {
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
}

} // namespace bptree::net
