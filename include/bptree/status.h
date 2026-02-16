#pragma once

/// @file status.h
/// @brief Lightweight error-handling type inspired by LevelDB / RocksDB.

#include <string>
#include <utility>

namespace bptree {

/// Represents the outcome of an operation.
///
/// Usage:
/// @code
///   Status s = tree.Insert(42, "hello");
///   if (!s.ok()) { std::cerr << s.ToString() << std::endl; }
/// @endcode
class Status {
public:
    /// Construct an OK status (default).
    Status() = default;

    // -- Named constructors --------------------------------------------------
    static Status OK()                              { return {}; }
    static Status NotFound(const std::string& msg)  { return {Code::kNotFound, msg}; }
    static Status IOError(const std::string& msg)   { return {Code::kIOError, msg}; }
    static Status Corruption(const std::string& msg){ return {Code::kCorruption, msg}; }
    static Status InvalidArg(const std::string& msg){ return {Code::kInvalidArg, msg}; }
    static Status Full(const std::string& msg)      { return {Code::kFull, msg}; }

    // -- Queries -------------------------------------------------------------
    [[nodiscard]] bool ok()          const { return code_ == Code::kOk; }
    [[nodiscard]] bool IsNotFound()  const { return code_ == Code::kNotFound; }
    [[nodiscard]] bool IsIOError()   const { return code_ == Code::kIOError; }
    [[nodiscard]] bool IsCorruption()const { return code_ == Code::kCorruption; }

    [[nodiscard]] std::string ToString() const {
        switch (code_) {
            case Code::kOk:          return "OK";
            case Code::kNotFound:    return "NotFound: "    + message_;
            case Code::kIOError:     return "IOError: "     + message_;
            case Code::kCorruption:  return "Corruption: "  + message_;
            case Code::kInvalidArg:  return "InvalidArg: "  + message_;
            case Code::kFull:        return "Full: "        + message_;
        }
        return "Unknown";
    }

private:
    enum class Code { kOk, kNotFound, kIOError, kCorruption, kInvalidArg, kFull };

    Code        code_ = Code::kOk;
    std::string message_;

    Status(Code c, std::string msg) : code_(c), message_(std::move(msg)) {}
};

}  // namespace bptree
