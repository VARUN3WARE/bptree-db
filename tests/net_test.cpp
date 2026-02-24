/// @file net_test.cpp
/// @brief Integration tests: spin up Server, connect Client, run SQL CRUD.
///        Real sockets, same process. Quick but honest. :)

#include <gtest/gtest.h>

#include "bptree/net/server.h"
#include "bptree/net/client.h"

#include <chrono>
#include <filesystem>
#include <thread>

namespace fs = std::filesystem;
using namespace bptree::net;

// ---------------------------------------------------------------------------
// Test fixture — server on a random-ish high port, client connects
// ---------------------------------------------------------------------------

static constexpr int kPort = 15432;

class NetTest : public ::testing::Test {
protected:
    void SetUp() override {
        CleanUp();
        ASSERT_TRUE(server_.Start(kPort)) << "Failed to start server";
        // Give accept loop a moment to start up.
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        client_.Connect("127.0.0.1", kPort);
    }

    void TearDown() override {
        client_.Disconnect();
        server_.Stop();
        CleanUp();
    }

    void CleanUp() {
        fs::remove("catalog.dat");
        for (auto& f : {"users.idx", "users.idx.wal",
                         "products.idx", "products.idx.wal"}) {
            fs::remove(std::string("./") + f);
        }
    }

    std::string Q(const std::string& sql) { return client_.Query(sql); }

    Server server_{".", "catalog.dat"};
    Client client_;
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

TEST_F(NetTest, CreateTable) {
    std::string r = Q("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT);");
    EXPECT_FALSE(r.empty());
}

TEST_F(NetTest, InsertAndSelect) {
    Q("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT);");
    Q("INSERT INTO users VALUES (1, 'Alice', 30);");
    Q("INSERT INTO users VALUES (2, 'Bob', 25);");

    std::string csv = Q("SELECT * FROM users;");
    EXPECT_NE(csv.find("Alice"), std::string::npos);
    EXPECT_NE(csv.find("Bob"),   std::string::npos);
}

TEST_F(NetTest, SelectWithWhere) {
    Q("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT);");
    Q("INSERT INTO users VALUES (1, 'Alice', 30);");
    Q("INSERT INTO users VALUES (2, 'Bob', 25);");

    std::string csv = Q("SELECT * FROM users WHERE age > 28;");
    EXPECT_NE(csv.find("Alice"), std::string::npos);
    EXPECT_EQ(csv.find("Bob"),   std::string::npos);
}

TEST_F(NetTest, UpdateAndVerify) {
    Q("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT);");
    Q("INSERT INTO users VALUES (1, 'Alice', 30);");
    Q("UPDATE users SET age = 99 WHERE id = 1;");

    std::string csv = Q("SELECT age FROM users WHERE id = 1;");
    EXPECT_NE(csv.find("99"), std::string::npos);
}

TEST_F(NetTest, DeleteAndVerify) {
    Q("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT);");
    Q("INSERT INTO users VALUES (1, 'Alice', 30);");
    Q("INSERT INTO users VALUES (2, 'Bob', 25);");
    Q("DELETE FROM users WHERE id = 1;");

    std::string csv = Q("SELECT * FROM users;");
    EXPECT_EQ(csv.find("Alice"), std::string::npos);
    EXPECT_NE(csv.find("Bob"),   std::string::npos);
}

TEST_F(NetTest, BadSQLReturnsNoException) {
    // Server should send back an Error msg; client throws ClientError.
    EXPECT_THROW(Q("SELECTT garbage table;"), ClientError);
}
