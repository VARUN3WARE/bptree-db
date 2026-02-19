#pragma once

#include <chrono>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>

namespace bptree {

/// Log severity levels
enum class LogLevel {
    TRACE = 0,
    DEBUG = 1,
    INFO  = 2,
    WARN  = 3,
    ERROR = 4,
    FATAL = 5
};

/// Convert log level to string
inline const char* LogLevelToString(LogLevel level) {
    switch (level) {
        case LogLevel::TRACE: return "TRACE";
        case LogLevel::DEBUG: return "DEBUG";
        case LogLevel::INFO:  return "INFO";
        case LogLevel::WARN:  return "WARN";
        case LogLevel::ERROR: return "ERROR";
        case LogLevel::FATAL: return "FATAL";
        default: return "UNKNOWN";
    }
}

/// Thread-safe structured logger
class Logger {
public:
    /// Get the global logger instance
    static Logger& Instance() {
        static Logger instance;
        return instance;
    }

    /// Set the minimum log level (messages below this are ignored)
    void SetLevel(LogLevel level) {
        std::lock_guard<std::mutex> lock(mutex_);
        min_level_ = level;
    }

    /// Get the current log level
    LogLevel GetLevel() const {
        return min_level_;
    }

    /// Enable/disable file logging
    void SetLogFile(const std::string& path) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (file_.is_open()) {
            file_.close();
        }
        if (!path.empty()) {
            file_.open(path, std::ios::app);
            if (!file_.is_open()) {
                std::cerr << "Failed to open log file: " << path << std::endl;
            }
        }
    }

    /// Enable/disable console output
    void SetConsoleOutput(bool enabled) {
        std::lock_guard<std::mutex> lock(mutex_);
        console_enabled_ = enabled;
    }

    /// Log a message with context
    void Log(LogLevel level, const std::string& file, int line,
             const std::string& function, const std::string& message) {
        if (level < min_level_) {
            return;
        }

        std::lock_guard<std::mutex> lock(mutex_);

        // Build log entry
        std::ostringstream oss;
        
        // Timestamp
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()) % 1000;
        
        struct tm tm_buf;
        localtime_r(&time_t, &tm_buf);
        
        oss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S")
            << '.' << std::setfill('0') << std::setw(3) << ms.count()
            << " [" << LogLevelToString(level) << "] "
            << "[" << file << ":" << line << " " << function << "] "
            << message << std::endl;

        std::string entry = oss.str();

        // Write to console
        if (console_enabled_) {
            if (level >= LogLevel::ERROR) {
                std::cerr << entry;
            } else {
                std::cout << entry;
            }
        }

        // Write to file
        if (file_.is_open()) {
            file_ << entry;
            file_.flush();
        }
    }

private:
    Logger() : min_level_(LogLevel::INFO), console_enabled_(true) {}
    ~Logger() {
        if (file_.is_open()) {
            file_.close();
        }
    }

    // Disable copy and move
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    std::mutex mutex_;
    LogLevel min_level_;
    bool console_enabled_;
    std::ofstream file_;
};

} // namespace bptree

// Convenience macros
#define LOG_TRACE(msg) \
    bptree::Logger::Instance().Log(bptree::LogLevel::TRACE, __FILE__, __LINE__, __func__, msg)

#define LOG_DEBUG(msg) \
    bptree::Logger::Instance().Log(bptree::LogLevel::DEBUG, __FILE__, __LINE__, __func__, msg)

#define LOG_INFO(msg) \
    bptree::Logger::Instance().Log(bptree::LogLevel::INFO, __FILE__, __LINE__, __func__, msg)

#define LOG_WARN(msg) \
    bptree::Logger::Instance().Log(bptree::LogLevel::WARN, __FILE__, __LINE__, __func__, msg)

#define LOG_ERROR(msg) \
    bptree::Logger::Instance().Log(bptree::LogLevel::ERROR, __FILE__, __LINE__, __func__, msg)

#define LOG_FATAL(msg) \
    bptree::Logger::Instance().Log(bptree::LogLevel::FATAL, __FILE__, __LINE__, __func__, msg)
