// Copyright (c) 2025 bptree-db
// Logger tests

#include "bptree/logger.h"
#include <gtest/gtest.h>
#include <fstream>
#include <sstream>

using namespace bptree;

TEST(LoggerTest, BasicLogging) {
    auto& logger = Logger::Instance();
    logger.SetLevel(LogLevel::INFO);
    logger.SetConsoleOutput(false);
    
    // Should not crash
    LOG_INFO("Test message");
    LOG_WARN("Warning message");
    LOG_ERROR("Error message");
}

TEST(LoggerTest, LogLevel) {
    auto& logger = Logger::Instance();
    
    logger.SetLevel(LogLevel::ERROR);
    EXPECT_EQ(logger.GetLevel(), LogLevel::ERROR);
    
    logger.SetLevel(LogLevel::DEBUG);
    EXPECT_EQ(logger.GetLevel(), LogLevel::DEBUG);
}

TEST(LoggerTest, FileLogging) {
    auto& logger = Logger::Instance();
    logger.SetLevel(LogLevel::INFO);
    
    std::string log_file = "test_log.txt";
    logger.SetLogFile(log_file);
    
    LOG_INFO("File log test");
    LOG_WARN("Warning in file");
    
    // Close the file by setting empty path
    logger.SetLogFile("");
    
    // Check file exists and has content
    std::ifstream file(log_file);
    ASSERT_TRUE(file.is_open());
    
    std::string content((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());
    
    EXPECT_TRUE(content.find("File log test") != std::string::npos);
    EXPECT_TRUE(content.find("Warning in file") != std::string::npos);
    
    file.close();
    std::remove(log_file.c_str());
}

TEST(LoggerTest, LogLevelFiltering) {
    auto& logger = Logger::Instance();
    logger.SetLevel(LogLevel::WARN);
    logger.SetConsoleOutput(false);
    
    std::string log_file = "test_filter.txt";
    logger.SetLogFile(log_file);
    
    LOG_DEBUG("Should not appear");
    LOG_INFO("Should not appear");
    LOG_WARN("Should appear");
    LOG_ERROR("Should appear");
    
    logger.SetLogFile("");
    
    std::ifstream file(log_file);
    std::string content((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());
    
    EXPECT_TRUE(content.find("Should not appear") == std::string::npos);
    EXPECT_TRUE(content.find("Should appear") != std::string::npos);
    
    file.close();
    std::remove(log_file.c_str());
}

TEST(LoggerTest, LogLevelToString) {
    EXPECT_STREQ(LogLevelToString(LogLevel::TRACE), "TRACE");
    EXPECT_STREQ(LogLevelToString(LogLevel::DEBUG), "DEBUG");
    EXPECT_STREQ(LogLevelToString(LogLevel::INFO), "INFO");
    EXPECT_STREQ(LogLevelToString(LogLevel::WARN), "WARN");
    EXPECT_STREQ(LogLevelToString(LogLevel::ERROR), "ERROR");
    EXPECT_STREQ(LogLevelToString(LogLevel::FATAL), "FATAL");
}

TEST(LoggerTest, ConsoleToggle) {
    auto& logger = Logger::Instance();
    
    logger.SetConsoleOutput(true);
    LOG_INFO("Console enabled");
    
    logger.SetConsoleOutput(false);
    LOG_INFO("Console disabled");
    
    // Re-enable for other tests
    logger.SetConsoleOutput(true);
}
