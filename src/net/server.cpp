/// @file server.cpp
/// @brief TCP server implementation.
///        One thread per client -- simple and it works. :)

#include "bptree/net/server.h"
#include "bptree/sql/executor.h"
#include "bptree/sql/row.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace bptree::net {

// ---------------------------------------------------------------------------
// Helpers: result -> CSV text
// ---------------------------------------------------------------------------

static std::string result_to_csv(const sql::ResultSet& rs) {
    std::string out;

    // Header row
    for (size_t i = 0; i < rs.columns.size(); ++i) {
        if (i) out += ',';
        out += rs.columns[i];
    }
    out += '\n';

    // Data rows
    for (auto& row : rs.rows) {
        for (size_t i = 0; i < row.size(); ++i) {
            if (i) out += ',';
            // Quote values containing commas or newlines.
            std::string v = sql::value_to_string(row[i]);
            bool need_quote = v.find(',') != std::string::npos ||
                              v.find('\n') != std::string::npos;
            if (need_quote) { out += '"'; out += v; out += '"'; }
            else out += v;
        }
        out += '\n';
    }
    return out;
}

// ---------------------------------------------------------------------------
// Server ctor / dtor
// ---------------------------------------------------------------------------

Server::Server(const std::string& data_dir, const std::string& catalog)
    : data_dir_(data_dir), catalog_path_(catalog) {}

Server::~Server() { Stop(); }

// ---------------------------------------------------------------------------
// Start / Stop / Wait
// ---------------------------------------------------------------------------

bool Server::Start(int port) {
    server_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        std::cerr << "socket(): " << std::strerror(errno) << "\n";
        return false;
    }

    int opt = 1;
    ::setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(static_cast<uint16_t>(port));

    if (::bind(server_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "bind(): " << std::strerror(errno) << "\n";
        ::close(server_fd_);
        server_fd_ = -1;
        return false;
    }

    if (::listen(server_fd_, 64) < 0) {
        std::cerr << "listen(): " << std::strerror(errno) << "\n";
        ::close(server_fd_);
        server_fd_ = -1;
        return false;
    }

    running_ = true;
    accept_thread_ = std::thread([this] { accept_loop(server_fd_); });
    return true;
}

void Server::Stop() {
    running_ = false;
    if (server_fd_ >= 0) {
        ::shutdown(server_fd_, SHUT_RDWR);
        ::close(server_fd_);
        server_fd_ = -1;
    }
    if (accept_thread_.joinable()) accept_thread_.join();
}

void Server::Wait() {
    if (accept_thread_.joinable()) accept_thread_.join();
}

// ---------------------------------------------------------------------------
// Accept loop
// ---------------------------------------------------------------------------

void Server::accept_loop(int srv_fd) {
    while (running_) {
        sockaddr_in client_addr{};
        socklen_t   len = sizeof(client_addr);
        int cfd = ::accept(srv_fd, reinterpret_cast<sockaddr*>(&client_addr), &len);
        if (cfd < 0) {
            if (running_) std::cerr << "accept(): " << std::strerror(errno) << "\n";
            break;
        }

        // Detach a thread per connection.
        std::thread([this, cfd]{ handle_client(cfd); }).detach();
    }
}

// ---------------------------------------------------------------------------
// Per-connection handler
// ---------------------------------------------------------------------------

void Server::handle_client(int cfd) {
    // Each connection gets its own executor (catalog is shared on disk).
    sql::Executor exec(catalog_path_, data_dir_);

    Message req, resp;
    while (RecvMsg(cfd, req)) {
        if (req.type != MsgType::Query) {
            SendMsg(cfd, Message::Error("expected Query message"));
            continue;
        }

        try {
            sql::ResultSet rs = exec.ExecSQL(req.payload);

            if (rs.rows.empty() && !rs.columns.empty() &&
                rs.columns[0] == "result") {
                // DDL or DML with text result
                SendMsg(cfd, Message::OkRows(
                    !rs.rows.empty() ? sql::value_to_string(rs.rows[0][0])
                                     : std::to_string(rs.affected) + " rows"));
            } else if (rs.rows.empty()) {
                SendMsg(cfd, Message::OkRows(
                    std::to_string(rs.affected) + " rows affected"));
            } else {
                SendMsg(cfd, Message::Data(result_to_csv(rs)));
            }
        } catch (const std::exception& ex) {
            SendMsg(cfd, Message::Error(ex.what()));
        }
    }

    ::close(cfd);
}

} // namespace bptree::net
