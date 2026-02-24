/// @file server.h
/// @brief TCP server that accepts SQL queries over the wire protocol.

#pragma once

#include "bptree/net/message.h"

#include <atomic>
#include <string>
#include <thread>

namespace bptree::net {

/// Listens on a TCP port, accepts multiple clients (one thread each),
/// dispatches SQL to a per-connection Executor, sends back results.
class Server {
public:
    explicit Server(const std::string& data_dir  = ".",
                    const std::string& catalog    = "catalog.dat");
    ~Server();

    /// Start listening on the given port (non-blocking — spawns accept loop).
    bool Start(int port);

    /// Stop the accept loop and close all connections.
    void Stop();

    /// Block until Stop() is called. Good for main() usage.
    void Wait();

    bool IsRunning() const { return running_.load(); }

private:
    void accept_loop(int server_fd);
    void handle_client(int client_fd);

    std::string   data_dir_;
    std::string   catalog_path_;
    std::thread   accept_thread_;
    std::atomic<bool> running_{false};
    int           server_fd_ = -1;
};

} // namespace bptree::net
