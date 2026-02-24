/// @file message.h
/// @brief Message struct and send/recv helpers.

#pragma once

#include "bptree/net/protocol.h"

#include <string>

namespace bptree::net {

struct Message {
    MsgType     type    = MsgType::Query;
    std::string payload;

    // -- Factory helpers --
    static Message Query (const std::string& sql)  { return {MsgType::Query,  sql};  }
    static Message OkRows(const std::string& n)    { return {MsgType::OkRows, n};    }
    static Message Data  (const std::string& csv)  { return {MsgType::Data,   csv};  }
    static Message Error (const std::string& msg)  { return {MsgType::Error,  msg};  }
};

/// Write a complete framed message to fd.  Returns false on I/O error.
bool SendMsg(int fd, const Message& msg);

/// Read a complete framed message from fd.  Returns false on disconnect/error.
bool RecvMsg(int fd, Message& msg);

} // namespace bptree::net
