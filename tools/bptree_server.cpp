/// @file bptree_server.cpp
/// @brief bptree-db TCP server launcher.
///        Usage: bptree_server [--port N] [--data-dir PATH]

#include "bptree/net/server.h"

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <string>

static bptree::net::Server* g_server = nullptr;

static void on_signal(int) {
    if (g_server) g_server->Stop();
}

int main(int argc, char** argv) {
    int         port     = 5432;
    std::string data_dir = ".";
    std::string catalog  = "catalog.dat";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--port"     && i + 1 < argc) port     = std::stoi(argv[++i]);
        if (arg == "--data-dir" && i + 1 < argc) data_dir = argv[++i];
        if (arg == "--catalog"  && i + 1 < argc) catalog  = argv[++i];
    }

    bptree::net::Server server(data_dir, catalog);
    g_server = &server;

    std::signal(SIGINT,  on_signal);
    std::signal(SIGTERM, on_signal);

    if (!server.Start(port)) return 1;

    std::cout << "bptree-db listening on port " << port
              << "  (data-dir=" << data_dir << ")\n"
              << "Press Ctrl-C to stop.\n";

    server.Wait();
    std::cout << "\nServer stopped.\n";
    return 0;
}
